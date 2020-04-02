package fr.aphp.wind.eds

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction

package object generator {

  /**
    * A Spark SQL column with random string UUIDs.
    *
    * The random UUIDs are generated with the Spark SQL rand function to
    * give consistent reads.
    */
  lazy val uuidString: Column = {
    val uuidStringUDF: UserDefinedFunction = {
      import org.apache.spark.sql.functions.udf
      udf((x: Double) =>
        (BigDecimal(x) * 1e6).toBigInt.toByteArray.map("%02X" format _).mkString
      )
    }

    import org.apache.spark.sql.functions.rand
    uuidStringUDF(rand)
  }

  /**
    * A spark SQL column with random long UUIDs.
    *
    * The random UUIDs are generated with the Spark SQL rand function to
    * give consistent reads.
    */
  lazy val uuidLong: Column = {
    val uuidLongUDF: UserDefinedFunction = {
      import org.apache.spark.sql.functions.udf
      val scale = math.pow(2, 7)
      udf((x: Double) => (BigDecimal(x) * scale).toLongExact)
    }

    import org.apache.spark.sql.functions.rand
    uuidLongUDF(rand)
  }
}
