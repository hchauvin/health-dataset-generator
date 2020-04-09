package fr.aphp.wind.eds

import java.util.Properties

import org.apache.spark.sql.types.{
  BooleanType,
  DataType,
  DateType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
  * Manipulates bundles of dataframes, including schema validation.
  */
package object data {

  /**
    * A data bundle is a collection of named dataframes.
    *
    * @param dataFrames The dataframes in the bundle.  The keys are the dataframe names.
    */
  case class GenericDataBundle(dataFrames: Map[String, DataFrame]) {

    /**
      * Gets a dataframe.
      *
      * @param tableName The name of the dataframe to get.
      * @return The dataframe.  If there is no dataframe for the given name,
      *         an error is thrown.
      */
    def apply(tableName: String): DataFrame = dataFrames(tableName)

    /**
      * Coalesces all the dataframes.
      *
      * @param partitions The number of partitions each dataframe should be coalesced to.
      *                   The dataframe names are given as keys.
      * @return A bundle with the new dataframes.
      */
    def coalesce(partitions: Map[String, Int]): GenericDataBundle = {
      require(partitions.keySet == dataFrames.keySet)

      val dfs = dataFrames.toSeq
        .sortBy { _._1 }
        .zip(partitions.toSeq.sortBy { _._1 })
        .map { case ((name, df), (_, parts)) => (name, df.coalesce(parts)) }
        .toMap
      GenericDataBundle(dfs)
    }

    /**
      * Coalesces all the dataframes.
      *
      * @param partitions The common number of partition each dataframe should be coalescted to.
      * @return A bundle with the new dataframes.
      */
    def coalesce(partitions: Int): GenericDataBundle = {
      coalesce(dataFrames.mapValues(_ => partitions))
    }

    /**
      * Collects all the dataframes.
      */
    def collect(): Map[String, Array[Row]] = {
      dataFrames.map {
        case (name, df) =>
          try {
            (name, df.collect())
          } catch {
            case e: Exception =>
              throw new RuntimeException(s"cannot collect table ${name}")
          }
      }.toMap
    }

    /** Caches all the dataframes. */
    def cache(): GenericDataBundle = {
      GenericDataBundle(
        dataFrames.mapValues { _.cache() }
      )
    }

    /**
      * Adds the columns from given schemas to the dataframes, when they are missing.
      * The missing columns are added with all the values set to "null".
      * The dataframes are not otherwise validated against the schemas.
      *
      * @param schemas The schemas to derive missing columns from.  The dataframe
      *                names are given as keys.
      * @return A bundle with the new dataframes.
      */
    def addMissingColumns(
        schemas: Map[String, StructType]
    ): GenericDataBundle = {
      require(schemas.keySet == dataFrames.keySet)

      val dfs = dataFrames.map {
        case (tableName, df) => {
          val allFields = schemas(tableName).fields
          val missingColumns = allFields.map { _.name } diff df.columns
          val expandedDF = missingColumns.foldLeft(df)((acc, column) => {
            val field = allFields.find { _.name == column }.get
            require(field.nullable)
            import org.apache.spark.sql.functions.lit
            acc.withColumn(column, lit(null).cast(field.dataType))
          })
          (tableName, expandedDF)
        }
      }.toMap
      GenericDataBundle(dfs)
    }

    /**
      * Creates or replace Spark temp views with the dataframes in the bundle.
      *
      * @param renameTable A function to map dataframe names to temp view names.
      */
    def createOrReplaceTempViews(
        renameTable: String => String = identity
    ): Unit = {
      dataFrames.par.foreach {
        case (tableName, df) =>
          df.createOrReplaceTempView(renameTable(tableName))
      }
    }

    /**
      * Saves the dataframes to the Spark warehouse.
      *
      * @param renameTable A function to map dataframe names to table names.
      */
    def saveToWarehouse(renameTable: String => String = identity): Unit = {
      dataFrames.par.foreach {
        case (tableName, df) => df.writeTo(renameTable(tableName))
      }
    }

    /**
      * Saves the dataframes to a DB using a JDBC connection.
      *
      * @param jdbcURL The JDBC URL to the database.
      * @param renameTable A function to map dataframe names to DB table names.
      * @param connectionProperties Connection properties to pass to the JDBC driver.
      */
    def saveToDB(
        jdbcURL: String,
        renameTable: String => String = identity,
        connectionProperties: Properties = new Properties
    ): Unit = {
      dataFrames.par.foreach {
        case (tableName, df) =>
          df.write
            .mode(SaveMode.Append)
            .option("cascadeTruncate", "true")
            .jdbc(jdbcURL, renameTable(tableName), connectionProperties)
      }
    }

    /**
      * Validates the dataframes against schemas.
      *
      * @param schemas The schemas to validate against.  The dataframe names are given as keys.
      * @param allowMissingFields Whether to allow fields declared in a schema to be missing
      *                           in the corresponding dataframe.
      * @return The validation result.
      * @see [[validateDF]], [[Validation.throwOnErrors]]
      */
    def validate(
        schemas: Map[String, StructType],
        allowMissingFields: Boolean = false
    ): Validation = {
      require(
        schemas.keySet == dataFrames.keySet,
        "A schema must be given for each dataframe.  There cannot be schemas " +
          "not associated with a dataframe."
      )

      val errors = dataFrames.toSeq.flatMap(entry =>
        entry match {
          case (tableName, df) => {
            validateDF(
              df,
              schemas(tableName),
              allowMissingFields = allowMissingFields
            ).errors.map(e => s"table $tableName: $e")
          }
        }
      )

      if (errors.isEmpty) Valid()
      else Invalid(errors)
    }
  }

  object GenericDataBundle {

    /**
      * Creates a bundle from Spark temp views.  One dataframe per view is created.
      *
      * @param views The views to use for the bundle.
      * @param renameView A function to map view names to dataframe names.
      * @return A new bundle.
      */
    def fromTempViews(
        views: Seq[String],
        renameView: String => String = identity
    ): GenericDataBundle = {
      val dfs = views.par
        .map { view =>
          (renameView(view), SparkSession.active.sql(s"select * from ${view}"))
        }
        .seq
        .toMap
      GenericDataBundle(dfs)
    }

    /**
      * Creates a bundle from the Spark warehouse.
      *
      * @param tables The tables from the Spark warehouse to use in the bundle.
      * @param renameTable A function to map warehouse table names to dataframe names.
      * @return A new bundle.
      */
    def fromWarehouse(
        tables: Seq[String],
        renameTable: String => String = identity
    ): GenericDataBundle = {
      val dfs = tables.par
        .map { table =>
          (renameTable(table), SparkSession.active.read.table(table))
        }
        .seq
        .toMap
      GenericDataBundle(dfs)
    }

    /**
      * Creates a bundle from DB tables, using JDBC.
      *
      * @param tables The DB tables to use in the bundle.
      * @param jdbcURL The JDBC URL to the DB.
      * @param renameTable A function to map DB table names to dataframe names.
      * @param connectionProperties Connection properties to pass to the JDBC driver.
      * @return A new bundle.
      */
    def fromDB(
        tables: Seq[String],
        jdbcURL: String,
        renameTable: String => String = identity,
        connectionProperties: Properties = new Properties
    ): GenericDataBundle = {
      val dfs = tables.par
        .map { table =>
          (
            renameTable(table),
            SparkSession.active.read
              .jdbc(table, jdbcURL, connectionProperties)
          )
        }
        .seq
        .toMap
      GenericDataBundle(dfs)
    }
  }

  /**
    * Validate a Spark dataframe against a schema.
    *
    * @param df                 The dataframe to validate.
    * @param schema             The schema the dataframe must conform to.
    * @param allowMissingFields Whether to allow fields declared in a schema to be missing
    *                           in the corresponding dataframe.
    * @return The validation result.
    * @see [[Validation.throwOnErrors]]
    */
  def validateDF(
      df: DataFrame,
      schema: StructType,
      allowMissingFields: Boolean = false
  ): Validation = {
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
      val typeMismatch = df.schema.fields
        .flatMap(dfField => {
          schema.fields
            .find { _.name == dfField.name }
            .map(field =>
              (dfField.name, dfField.dataType.json, field.dataType.json)
            )
        })
        .filter { case (_, actual, expected) => actual != expected }
      typeMismatch.map {
        case (name, actual, expected) =>
          s"type mismatch for column ${name}: actual ${actual}, expected ${expected}"
      }
    }

    val errors = checkMissingFields union checkExtraFields union checkType
    if (errors.isEmpty) Valid()
    else Invalid(errors)
  }

  /**
    * The result of a validation.
    *
    * @see [[Valid]], [[Invalid]]
    */
  trait Validation {

    /** Validation errors. This is an empty sequence when the validation is successful. */
    val errors: Seq[String]

    /** Throws the list of validation errors if there are any. */
    def throwOnErrors(): Unit = {
      if (errors.nonEmpty)
        throw new RuntimeException(
          s"Validation errors:\n${errors.mkString("\n")}"
        )
    }
  }

  /** The result of a successful validation. */
  case class Valid() extends Validation {
    val errors: Seq[String] = Seq.empty
  }

  /** The result of a validation failure. */
  case class Invalid(errors: Seq[String]) extends Validation

  /** Validation errors. */
  private type Errors = Seq[String]
}
