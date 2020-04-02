package fr.aphp.wind.eds

import org.apache.spark.sql.DataFrame

package object generator {
  case class GenericDataBundle(dataFrames: Map[String, DataFrame]) {
    def apply(tableName: String): DataFrame = dataFrames(tableName)
  }
}
