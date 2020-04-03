package fr.aphp.wind.eds

import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

package object data {
  case class GenericDataBundle(dataFrames: Map[String, DataFrame]) {
    def apply(tableName: String): DataFrame = dataFrames(tableName)

    def coalesce(partitions: Map[String, Int]): GenericDataBundle = {
      require(partitions.keySet == dataFrames.keySet)

      val dfs = dataFrames.toSeq.sortBy { _._1 }.zip(partitions.toSeq.sortBy { _._1 })
        .map { case ((name, df), (_, parts)) => (name, df.coalesce(parts))}.toMap
      GenericDataBundle(dfs)
    }

    def coalesce(partitions: Int): GenericDataBundle = {
      coalesce(dataFrames.mapValues(_ => partitions))
    }

    def addMissingColumns(schemas: Map[String, StructType]): GenericDataBundle = {
      require(schemas.keySet == dataFrames.keySet)

      val dfs = dataFrames.map { case (tableName, df) => {
        val allFields = schemas(tableName).fields
        val missingColumns = allFields.map { _.name } diff df.columns
        val expandedDF = missingColumns.foldLeft(df)((acc, column) => {
          val field = allFields.find { _.name == column }.get
          require(field.nullable)
          import org.apache.spark.sql.functions.lit
          acc.withColumn(column, lit(null).cast(field.dataType))
        })
        (tableName, expandedDF)
      }}.toMap
      GenericDataBundle(dfs)
    }

    object validate {
      def apply(schemas: Map[String, StructType], allowMissingFields: Boolean = false): Validation = {
        require(schemas.keySet == dataFrames.keySet)

        val errors = dataFrames.toSeq.flatMap(entry => entry match {
          case (tableName, df) => {
            validateDF(df, schemas(tableName),
              allowMissingFields=allowMissingFields).errors.map(e => s"table $tableName: $e")
          }
        })

        if (errors.isEmpty) Valid()
        else Invalid(errors)
      }
    }
  }

  def validateDF(df: DataFrame,
               schema: StructType,
               allowMissingFields: Boolean = false): Validation = {
    def checkMissingFields: Errors = {
      if (allowMissingFields) Seq.empty
      else {
        val missing = schema.fields.map { _.name } diff df.columns
        if (missing.isEmpty) Seq.empty
        else Seq(s"missing fields: ${missing.mkString(", ")}")
      }
    }

    def checkExtraFields: Errors = {
      val extra = df.columns diff schema.fields.map { _.name }
      if (extra.isEmpty) Seq.empty
      else Seq(s"extra fields: ${extra.mkString(", ")} are not in the schema")
    }

    def checkType: Errors = {
      val typeMismatch = df.schema.fields.flatMap(dfField => {
        schema.fields.find { _.name == dfField.name }.map(field =>
          (dfField.name, dfField.dataType.json, field.dataType.json))
      }).filter { case (_, actual, expected) => actual != expected }
      typeMismatch.map { case (name, actual, expected) =>
        s"type mismatch for column ${name}: actual ${actual}, expected ${expected}" }
    }

    val errors = checkMissingFields union checkExtraFields union checkType
    if (errors.isEmpty) Valid()
    else Invalid(errors)
  }

  trait Validation {
    val errors: Seq[String]

    def throwOnErrors(): Unit = {
      if (errors.nonEmpty)
        throw new RuntimeException(s"Validation errors:\n${errors.mkString("\n")}")
    }
  }
  case class Valid() extends Validation {
    val errors: Seq[String] = Seq.empty
  }
  case class Invalid(errors: Seq[String]) extends Validation

  private type Errors = Seq[String]
}
