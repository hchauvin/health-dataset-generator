package fr.aphp.wind.eds.generator.source.synthea

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.Row

class SyntheaSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  behavior of "SyntheaDataBundle"

  it should "convert to a generic bundle" in {
    val fixme = {
      import org.apache.spark.sql.types._
      StructType(Array(StructField("foo", StringType)))
    }

    val bundle = SyntheaDataBundle(
      patients = spark.createDataFrame(sc.emptyRDD[Row], fixme),
      encounters = spark.createDataFrame(sc.emptyRDD[Row], fixme),
      organizations = spark.createDataFrame(sc.emptyRDD[Row], fixme),
      conditions = spark.createDataFrame(sc.emptyRDD[Row], fixme),
      procedures = spark.createDataFrame(sc.emptyRDD[Row], fixme),
      providers = spark.createDataFrame(sc.emptyRDD[Row], fixme)
    )

    new SyntheaDataBundle(bundle.genericBundle)
  }

  behavior of "generate"

  it should "generate one patient" in {
    val bundle = generate(1)
    bundle.patients.count() should equal(1)

    // We also collect all the other data frames to make sure they can be read
    bundle.genericBundle.dataFrames.foreach {
      case (tableName, df) =>
        try {
          df.collect()
        } catch {
          case e: Exception =>
            throw new RuntimeException(
              s"cannot collect data frame for table ${tableName}",
              e
            )
        }
    }
  }
}
