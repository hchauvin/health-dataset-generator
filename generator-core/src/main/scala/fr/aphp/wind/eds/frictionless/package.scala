// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import fr.aphp.wind.eds.data.GenericDataBundle
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType

import scala.io.Source

/**
  * Implementation of the frictionlessdata spec for a custom profile tailored to
  * Spark-based data bundles.
  *
  * @see http://specs.frictionlessdata.io/data-package
  */
package object frictionless {

  /**
    * Writes a [[GenericDataBundle]] as a frictionless package located on the local filesystem.
    * There is a more generic function that writes directly to HDFS.
    *
    * @param descriptor Descriptor for the data package.
    * @param bundle The bundle to dump.
    * @param outputPath Where to write the data package.
    * @param moveSinglePartitions FIXME
    */
  def writeDataBundle(
      descriptor: DataPackage.Descriptor,
      bundle: GenericDataBundle,
      outputPath: File,
      moveSinglePartitions: Boolean
  ): Unit = {
    writeDataBundle(
      descriptor,
      bundle,
      new HadoopPath("file://" + outputPath.getAbsolutePath),
      moveSinglePartitions
    )

    Files
      .find(
        Paths.get(outputPath.getAbsolutePath),
        999,
        (p, bfa) =>
          bfa.isRegularFile && (p.toString.endsWith(".crc") || p
            .endsWith("_SUCCESS"))
      )
      .forEach { Files.delete _ }

    Files
      .find(
        Paths.get(outputPath.getAbsolutePath),
        999,
        (p, bfa) => bfa.isDirectory && p.toString.endsWith("_temp")
      )
      .forEach { Files.delete _ }
  }

  /**
    * Write a [[GenericDataBundle]] as a frictionless package using HDFS.
    * There is a more specific function tailored to the local filesystem.
    *
    * @param descriptor Descriptor for the data package.
    * @param bundle The bundle to dump.
    * @param outputPath Where to write the data package.
    * @param moveSinglePartitions FIXME
    */
  def writeDataBundle(
      descriptor: DataPackage.Descriptor,
      bundle: GenericDataBundle,
      outputPath: HadoopPath,
      moveSinglePartitions: Boolean
  ): Unit = {
    val fs =
      FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)

    val descriptorResources = descriptor.resources.map { _.name }.toSet
    val bundleResources = bundle.dataFrames.keySet
    val resourceDiscrepancy =
      (descriptorResources diff bundleResources) union (bundleResources diff descriptorResources)
    require(
      resourceDiscrepancy.isEmpty,
      s"the resources in the descriptor (${descriptorResources.mkString(", ")}) do not " +
        s"match the resources in the bundle (${bundleResources.mkString(", ")})"
    )

    val nextResources = descriptor.resources.par
      .map(resource => {
        require(
          resource.path.size == 1,
          s"expected one and only one path for resource ${resource.name}"
        )
        require(
          resource.format.isDefined,
          s"expected a format to be set for resource ${resource.name}"
        )
        var format = resource.format.get

        val resourcePath = outputPath + "/" + resource.path.head
        val df = bundle(resource.name)

        val paths = if (df.rdd.getNumPartitions == 1 && moveSinglePartitions) {
          val finalResourcePath =
            if (df.rdd.getNumPartitions == 1 && moveSinglePartitions)
              resourcePath + "_temp"
            else resourcePath
          df.write.format(format).save(finalResourcePath)

          moveHadoopFile(
            SparkSession.active.sparkContext.hadoopConfiguration,
            fs,
            fs.listFiles(new HadoopPath(finalResourcePath), true)
              .next()
              .getPath,
            new HadoopPath(resourcePath)
          )

          resource.path
        } else {
          df.write.format(format).save(resourcePath)

          val pathBuilder = Seq.newBuilder[String]
          val it = fs.listFiles(new HadoopPath(resourcePath), true)
          while (it.hasNext) {
            val path =
              outputPath.toUri.relativize(it.next().getPath.toUri).getPath
            if (path != "_SUCCESS" && !path.endsWith("/_SUCCESS")) {
              pathBuilder += path
            }
          }

          pathBuilder.result
        }

        resource.schema.foreach(schemaPath => {
          val schemaJSON = SchemaConverters.toAvroType(df.schema).toString(true)
          val out = fs.create(new HadoopPath(outputPath + "/" + schemaPath))
          out.write(schemaJSON.getBytes(StandardCharsets.UTF_8))
          out.close()
        })

        resource.copy(
          path = paths
        )
      })
      .seq

    val nextDescriptor = descriptor.copy(resources = nextResources)

    {
      import org.json4s.DefaultFormats
      import org.json4s.jackson.Serialization.writePretty

      implicit val formats: DefaultFormats.type = DefaultFormats

      val out = fs.create(new HadoopPath(outputPath + "/datapackage.json"))
      out.write(writePretty(nextDescriptor).getBytes(StandardCharsets.UTF_8))
      out.close()
    }

    fs.close()
  }

  case class PackagedDataBundle(
      descriptor: DataPackage.Descriptor,
      bundle: GenericDataBundle
  )

  def readDataBundle(path: File): PackagedDataBundle = {
    readDataBundle(new HadoopPath("file://" + path.getAbsolutePath))
  }

  def readDataBundle(path: HadoopPath): PackagedDataBundle = {
    val fs =
      FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)

    val descriptor = {
      import org.json4s.DefaultFormats
      import org.json4s.jackson.Serialization.read

      implicit val formats: DefaultFormats.type = DefaultFormats

      val in = fs.open(new HadoopPath(path + "/datapackage.json"))
      val descriptorJSON = Source.fromInputStream(in).mkString
      in.close()

      read[DataPackage.Descriptor](descriptorJSON)
    }

    val dataFrames = descriptor.resources
      .map(resource => {
        require(
          resource.format.isDefined,
          s"expected a format to be set for resource ${resource.name}"
        )

        val spark = SparkSession.active
        val format = resource.format.get

        val dfReader = spark.read.format(format)
        val dfReaderWithSchema = resource.schema
          .map(schemaPath => {
            val schemaJSON = {
              val in = fs.open(new HadoopPath(path + "/" + resource.schema.get))
              val json = Source.fromInputStream(in).mkString
              in.close()
              json
            }

            val parser = new Schema.Parser
            val schema = SchemaConverters
              .toSqlType(parser.parse(schemaJSON))
              .dataType
              .asInstanceOf[StructType]

            dfReader.schema(schema)
          })
          .getOrElse(dfReader)
        (resource.name, dfReaderWithSchema.load(resource.path.map { path + "/" + _ }: _*))
      })
      .toMap

    PackagedDataBundle(descriptor, GenericDataBundle(dataFrames))
  }

  private def moveHadoopFile(
      conf: Configuration,
      fs: FileSystem,
      from: HadoopPath,
      to: HadoopPath
  ): Unit = {
    val in = fs.open(from)
    val out = fs.create(to)
    IOUtils.copyBytes(in, out, conf, true)
    fs.delete(from, true)
  }
}
