package fr.aphp.wind.eds.frictionless

import java.nio.file.{Paths}

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import fr.aphp.wind.eds.data.GenericDataBundle
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FrictionlessSpec extends AnyFlatSpec
  with Matchers
  with DataFrameSuiteBase {

  behavior of "fromDataBundle"

  it should "create a data bundle" in {
    import spark.implicits._

    val descriptor = DataPackage.Descriptor(
      resources = Seq(DataResource.Descriptor(
        path = Seq("df1.csv"),
        name = "df1",
        schema = Some("df1.schema.json")),
        DataResource.Descriptor(
          path = Seq("df2.csv"),
          name = "df2",
          schema = Some("df2.schema.json"))))

    val df1 = Seq((1, "foo"), (2, "bar")).toDF("id2", "value1")
    val df2 = Seq((2, "qux"), (3, "wobble")).toDF("id2", "value2")
    val bundle = GenericDataBundle(Map("df1" -> df1, "df2" -> df2))
      .coalesce(1)

    val outputPath = Paths.get("output").toAbsolutePath
    new scala.reflect.io.Directory(outputPath.toFile).deleteRecursively()

    fromDataBundle(descriptor, bundle, outputPath.toFile, moveSinglePartitions=false)
  }
}
