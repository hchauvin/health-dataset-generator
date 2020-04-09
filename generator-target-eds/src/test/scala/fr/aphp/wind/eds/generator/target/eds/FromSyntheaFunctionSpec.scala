package fr.aphp.wind.eds.generator.target.eds

import java.sql.{Date, Timestamp}
import java.time.Month

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FromSyntheaFunctionSpec
    extends AnyFlatSpec
    with Matchers
    with DataFrameSuiteBase {

  behavior of "omopId"

  it should "create a numeric ID" in {
    import sqlContext.implicits._
    val df = Seq(("9cfe98a6-269c-486f-9d1d-4942df1603a4")).toDF("uuid")
    val df2 = df.withColumn("id", FromSynthea.convert.omopId('uuid))
    val collected = df2.collect()
    collected should have length (1)
    collected(0).getAs[Long]("id") should be > 0L
  }

  behavior of "fromSyntheaDatetime"

  it should "convert a datetime" in {
    import sqlContext.implicits._
    val df = Seq(("2010-12-10T22:23Z"), (null)).toDF("datetime_str")
    val df2 = df.withColumn(
      "datetime",
      FromSynthea.convert.fromSyntheaDatetime('datetime_str)
    )
    val collected = df2.collect()
    collected should have length (2)
    val datetime = collected(0).getAs[Timestamp]("datetime").toLocalDateTime
    datetime.getYear should equal(2010)
    datetime.getMonth should equal(Month.DECEMBER)
    datetime.getDayOfMonth should equal(10)
    datetime.getHour should equal(22)
    datetime.getMinute should equal(23)
    datetime.getSecond should equal(0)
    collected(1).getAs[Timestamp]("datetime") should be(null)
  }

  behavior of "fromSyntheaDate"

  it should "convert a date" in {
    import sqlContext.implicits._
    val df = Seq(("2010-12-10"), (null)).toDF("date_str")
    val df2 =
      df.withColumn("date", FromSynthea.convert.fromSyntheaDate('date_str))
    val collected = df2.collect()
    collected should have length (2)
    val date = collected(0).getAs[Date]("date").toLocalDate
    date.getYear should equal(2010)
    date.getMonth should equal(Month.DECEMBER)
    date.getDayOfMonth should equal(10)
    collected(1).getAs[Date]("date") should be(null)
  }

  behavior of "fromSyntheaDateToTimestamp"

  it should "convert a date into a timestamp at midnight" in {
    import sqlContext.implicits._
    val df = Seq(("2010-12-10"), (null)).toDF("date_str")
    val df2 = df.withColumn(
      "datetime",
      FromSynthea.convert.fromSyntheaDateToTimestamp('date_str)
    )
    val collected = df2.collect()
    collected should have length (2)
    val datetime = collected(0).getAs[Timestamp]("datetime").toLocalDateTime
    datetime.getYear should equal(2010)
    datetime.getMonth should equal(Month.DECEMBER)
    datetime.getDayOfMonth should equal(10)
    datetime.getHour should equal(0)
    datetime.getMinute should equal(0)
    datetime.getSecond should equal(0)
    collected(1).getAs[Timestamp]("datetime") should be(null)
  }
}
