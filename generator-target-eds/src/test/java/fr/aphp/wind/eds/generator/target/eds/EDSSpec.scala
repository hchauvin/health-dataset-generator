package fr.aphp.wind.eds.generator.target.eds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EDSSpec extends AnyFlatSpec
  with Matchers
  with DataFrameSuiteBase {

  behavior of "EDSDataBundle"

  it should "convert to a generic bundle" in {
    val fixme = {
      import org.apache.spark.sql.types._
      StructType(Array(StructField("foo", StringType)))
    }

    val bundle = EDSDataBundle(
      fhirConcepts = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("fhirConcepts")),
      persons = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("persons")),
      observations = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("observations")),
      visitOccurrences = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("visitOccurrences")),
      notes = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("notes"))
      careSites = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("careSite")),
      conditionOccurrences = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("conditionOccurrences")),
      procedureOccurrences = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("procedureOccurrences")),
      providers = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("providers")),
      costs = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("costs")),
      locations = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("locations")),
      cohortDefinitions = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("cohortDefinitions")),
      cohorts = spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas("cohorts"))
    )

    new EDSDataBundle(bundle.genericBundle)
  }

}
