package fr.aphp.wind.eds

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import fr.aphp.wind.eds.data.GenericDataBundle
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path => HadoopPath}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.SchemaConverters

package object frictionless {
  def fromDataBundle(descriptor: DataPackage.Descriptor, bundle: GenericDataBundle, outputPath: File, moveSinglePartitions: Boolean): Unit = {
    fromDataBundle(descriptor, bundle, new HadoopPath("file://" + outputPath.getAbsolutePath), moveSinglePartitions)

    Files.find(Paths.get(outputPath.getAbsolutePath),
      999, (p, bfa) =>
        bfa.isRegularFile && (p.toString.endsWith(".crc") || p.endsWith("_SUCCESS")))
      .forEach { Files.delete _ }

    Files.find(Paths.get(outputPath.getAbsolutePath),
      999, (p, bfa) =>
        bfa.isDirectory && p.toString.endsWith("_temp")).forEach { Files.delete _ }
  }

  def fromDataBundle(descriptor: DataPackage.Descriptor, bundle: GenericDataBundle, outputPath: HadoopPath,
                     moveSinglePartitions: Boolean): Unit = {
    val fs = FileSystem.get(SparkSession.active.sparkContext.hadoopConfiguration)

    val descriptorResources = descriptor.resources.map { _.name }.toSet
    val bundleResources = bundle.dataFrames.keySet
    val resourceDiscrepancy =
      (descriptorResources diff bundleResources) union (bundleResources diff descriptorResources)
    require(resourceDiscrepancy.isEmpty,
      s"the resources in the descriptor (${descriptorResources.mkString(", ")}) do not " +
      s"match the resources in the bundle (${bundleResources.mkString(", ")})")

    val nextResources = descriptor.resources.par.map(resource => {
      require(resource.path.size == 1)

      val resourcePath = outputPath + "/" + resource.path.head
      val df = bundle(resource.name)

      val paths = if (df.rdd.getNumPartitions == 1 && moveSinglePartitions) {
        val finalResourcePath =
          if (df.rdd.getNumPartitions == 1 && moveSinglePartitions) resourcePath + "_temp"
          else resourcePath
        df.write.format("csv").save(finalResourcePath)

        moveHadoopFile(SparkSession.active.sparkContext.hadoopConfiguration,
          fs, fs.listFiles(new HadoopPath(finalResourcePath), true).next().getPath,
          new HadoopPath(resourcePath))

        resource.path
      } else {
        df.write.format("csv").save(resourcePath)

        val pathBuilder = Seq.newBuilder[String]
        val it = fs.listFiles(new HadoopPath(resourcePath), true)
        while (it.hasNext) {
          val path = outputPath.toUri.relativize(it.next().getPath.toUri).getPath
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
    }).seq

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

  private def moveHadoopFile(conf: Configuration, fs: FileSystem, from: HadoopPath, to: HadoopPath): Unit = {
    val in = fs.open(from)
    val out = fs.create(to)
    IOUtils.copyBytes(in, out, conf, true)
    fs.delete(from, true)
  }
}
