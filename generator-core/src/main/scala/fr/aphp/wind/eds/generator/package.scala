package fr.aphp.wind.eds

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction

package object generator {
  private val uuidStringUDF: UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    udf((x: Double) => (BigDecimal(x) * 1e6).toBigInt.toByteArray.map("%02X" format _).mkString)
  }

  lazy val uuidString: Column = {
    import org.apache.spark.sql.functions.rand
    uuidStringUDF(rand)
  }

  private val uuidLongUDF: UserDefinedFunction = {
    import org.apache.spark.sql.functions.udf
    val scale = math.pow(2, 7)
    udf((x: Double) => (BigDecimal(x) * scale).toLongExact)
  }

  lazy val uuidLong: Column = {
    import org.apache.spark.sql.functions.rand
    uuidLongUDF(rand)
  }
}
