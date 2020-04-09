package fr.aphp.wind.eds.data

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Caches the operations used to produce dataframes.
  *
  * The dataframes are cached on HDFS.  If the path for the cache of a particular dataframe
  * does not exist, the operation to produce it is executed.  Otherwise, it is skipped, and
  * the dataframe is read from cache.
  *
  * The cache lies by default locally, in the ".cache" directory of the current
  * working directory.
  *
  * @param cachePath The path to the cache on HDFS.  All the dataframes will be stored
  *                  under this root path.
  * @param format Format used to store the dataframes.
  */
class DFSupplyCache(
    cachePath: HadoopPath = new HadoopPath(
      "file://" + Paths.get(".cache").toAbsolutePath
    ),
    format: String = "parquet"
) {

  /**
    * Constructs a cache that uses the local file system.
    *
    * @param cachePath Path to the root directory of the cache, on the local file system.
    * @param format Format used to store the dataframes.
    */
  def this(cachePath: File, format: String) {
    this(new HadoopPath("file://" + cachePath.getAbsolutePath), format)
  }

  /**
    * Caches the operations necessary to produce a dataframe.
    *
    * @param name The name of the dataframe.  This is used as a path relative to the
    *             root cache path.
    * @param supply A function that supplies a dataframe.
    * @return The dataframe, either produced by `supply`, or read from cache.
    */
  def cache(name: String, supply: () => DataFrame): DataFrame = {
    val spark = SparkSession.active
    val fs =
      FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val dfCachePath = new HadoopPath(cachePath, name)
    if (fs.exists(dfCachePath)) {
      spark.read.format(format).load(dfCachePath.toString)
    } else {
      val df = supply()
      df.write.format(format).save(dfCachePath.toString)
      df
    }
  }

  /**
    * Caches the operations used to produce a whole data bundle.
    *
    * @param name The name of the bundle.  This is used as a path relative to the
    *             root cache path.
    * @param dfNames The names of the dataframes in the bundle.
    * @param supply A function that supplies a bundle.
    * @return The bundle, either produced by `supply`, or read from cache.
    */
  def cache(
      name: String,
      dfNames: Seq[String],
      supply: () => GenericDataBundle
  ): GenericDataBundle = {
    // TODO: Do not ask for dfNames

    val spark = SparkSession.active
    val fs =
      FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val bundleCachePath = new HadoopPath(cachePath, name)
    if (fs.exists(bundleCachePath)) {
      GenericDataBundle(
        dfNames
          .map(name =>
            (
              name,
              spark.read
                .format(format)
                .load(bundleCachePath.toString + '/' + name)
            )
          )
          .toMap
      )
    } else {
      val bundle = supply()
      bundle.dataFrames.foreach {
        case (name, df) => {
          df.write.format(format).save(bundleCachePath.toString + '/' + name)
        }
      }
      bundle
    }
  }
}
