// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds.generator.target

import fr.aphp.wind.eds.data.{GenericDataBundle, Validation}
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.io.Source

/**
  * EDS-specific generator.
  *
  * The _Entrepôt des Données de Santé_ is a medical data warehouse
  * developed at the [[https://en.wikipedia.org/wiki/Assistance_Publique_%E2%80%93_H%C3%B4pitaux_de_Paris AP-HP]],
  * the university hospital trust operating in Paris and its surroundings.
  *
  * @see https://eds.aphp.fr
  */
package object eds {

  /**
    * A data bundle containing all the data describing a set of patients and their interaction
    * with the healthcare system.
    */
  case class EDSDataBundle(
      fhirConcepts: DataFrame,
      persons: DataFrame,
      observations: DataFrame,
      visitOccurrences: DataFrame,
      visitDetail: DataFrame,
      notes: DataFrame,
      careSites: DataFrame,
      conditionOccurrences: DataFrame,
      procedureOccurrences: DataFrame,
      providers: DataFrame,
      costs: DataFrame,
      locations: DataFrame,
      cohortDefinitions: DataFrame,
      cohorts: DataFrame,
      role: DataFrame,
      careSiteHistory: DataFrame,
      factRelationship: DataFrame
  ) {
    def this(bundle: GenericDataBundle) {
      this(
        fhirConcepts = bundle("concept_fhir"),
        persons = bundle("person"),
        observations = bundle("observation"),
        visitOccurrences = bundle("visit_occurrence"),
        visitDetail = bundle("visit_detail"),
        notes = bundle("note"),
        careSites = bundle("care_site"),
        conditionOccurrences = bundle("condition_occurrence"),
        procedureOccurrences = bundle("procedure_occurrence"),
        providers = bundle("provider"),
        costs = bundle("cost"),
        locations = bundle("location"),
        cohortDefinitions = bundle("cohort_definition"),
        cohorts = bundle("cohort"),
        role = bundle("role"),
        careSiteHistory = bundle("care_site_history"),
        factRelationship = bundle("fact_relationship")
      )
    }

    /**
      * Converts the bundle with dataframes as fields to a generic bundle with dataframes
      * as map entries.
      */
    def genericBundle: GenericDataBundle = {
      val ans = GenericDataBundle(
        Map(
          "concept_fhir" -> fhirConcepts,
          "person" -> persons,
          "observation" -> observations,
          "visit_occurrence" -> visitOccurrences,
          "visit_detail" -> visitDetail,
          "note" -> notes,
          "care_site" -> careSites,
          "condition_occurrence" -> conditionOccurrences,
          "procedure_occurrence" -> procedureOccurrences,
          "provider" -> providers,
          "cost" -> costs,
          "location" -> locations,
          "cohort_definition" -> cohortDefinitions,
          "cohort" -> cohorts,
          "role" -> role,
          "care_site_history" -> careSiteHistory,
          "fact_relationship" -> factRelationship
        )
      )
      assert(
        ans.dataFrames.keySet == EDSDataBundle.tables,
        s"unexpected or missing keys: " + (
          (ans.dataFrames.keySet diff EDSDataBundle.tables)
            union (EDSDataBundle.tables diff ans.dataFrames.keySet)
        ).reduce { _ + " " + _ }
      )
      ans
    }

    /**
      * Calls [[GenericDataBundle.addMissingColumns]] with the APHP-specific schemas.
      */
    def addMissingColumns(): EDSDataBundle = {
      new EDSDataBundle(genericBundle.addMissingColumns(EDSDataBundle.schemas))
    }

    /**
      * Calls [[GenericDataBundle.validate]] with the APHP-specific schemas.
      */
    def validate(allowMissingFields: Boolean = false): Validation = {
      genericBundle.validate(
        EDSDataBundle.schemas,
        allowMissingFields = allowMissingFields
      )
    }

    /**
      * Calls [[GenericDataBundle.validate]] with the APHP-specific schemas,
      * throws on validation errors, and returns the data bundle.
      *
      * This method is a useful alternative to [[validate]] when chaining
      * operations.
      */
    def checkpoint(allowMissingFields: Boolean = false): EDSDataBundle = {
      validate(allowMissingFields).throwOnErrors()
      this
    }

    /** Caches all the dataframes in the bundle. */
    def cache(): EDSDataBundle = {
      new EDSDataBundle(genericBundle.cache())
    }

    /** Adds mock ETL hashes to the dataframes. */
    def withHash(): EDSDataBundle = {
      val spark = SparkSession.active
      import spark.sqlContext.implicits._
      import org.apache.spark.sql.functions._
      new EDSDataBundle(
        GenericDataBundle(
          genericBundle.dataFrames.mapValues(df => {
            df.withColumn("hash", lit(123))
          })
        )
      )
    }
  }

  object EDSDataBundle {
    import org.apache.spark.sql.avro.SchemaConverters

    /**
      * The tables supported by the AP-HP database.
      */
    val tables: Set[String] = Set(
      "concept_fhir",
      "person",
      "observation",
      "visit_occurrence",
      "visit_detail",
      "note",
      "care_site",
      "condition_occurrence",
      "procedure_occurrence",
      "provider",
      "cost",
      "location",
      "cohort_definition",
      "cohort",
      "role",
      "care_site_history",
      "fact_relationship"
    )

    /**
      * The AP-HP specific schemas.  The keys are the table/dataframe names.
      */
    val schemas: Map[String, StructType] = tables
      .map(table => {
        val schemaJSON = Source
          .fromResource(
            s"fr/aphp/wind/eds/generator/target/eds/avro_schemas/${table}.avro"
          )
          .mkString
        val parser = new Schema.Parser
        val schema = SchemaConverters
          .toSqlType(parser.parse(schemaJSON))
          .dataType
          .asInstanceOf[StructType]
        (table, schema)
      })
      .toMap

    lazy val empty: EDSDataBundle = {
      val spark = SparkSession.active
      new EDSDataBundle(
        GenericDataBundle(
          tables
            .map(table => {
              val emptyDF =
                spark.createDataFrame(
                  spark.sparkContext.emptyRDD[Row],
                  EDSDataBundle.schemas(table)
                )
              (table, emptyDF)
            })
            .toMap
        )
      )
    }
  }

  /** Built-in values and tables that do not change. */
  object builtins {
    private lazy val sqlContext = SparkSession.active.sqlContext

    import sqlContext.implicits._

    /**
      * List of FHIR concepts that are put in generated datasets every time.
      * Additional FHIR concepts are put dependending on what lies in the
      * condition and procedure tables.
      */
    lazy val fhirConcepts: DataFrame = {
      Seq(
        (
          1001L,
          "8532",
          "female",
          "administrative-gender",
          "female",
          "female",
          "http://hl7.org/fhir/administrative-gender"
        ),
        (
          1002L,
          "8507",
          "male",
          "administrative-gender",
          "male",
          "male",
          "http://hl7.org/fhir/administrative-gender"
        ),
        (
          4082735L,
          "182992009",
          "Treatment completed",
          "SNOMED",
          "completed",
          "completed",
          "http://hl7.org/fhir/procedure-status"
        ),
        (
          1003L,
          "XXX",
          "XXX",
          "referred-document-status",
          "final",
          "final",
          "http://hl7.org/fhir/referred-document-status"
        ),
        (
          1004L,
          "XXX",
          "XXX",
          "referred-document-status",
          "active",
          "active",
          "http://hl7.org/fhir/claim-status"
        ),
        (
          // Mock diagnosis for claims/costs
          1005L,
          "XXX",
          "XXX",
          "FIXME",
          "FIXME",
          "FIXME",
          "FIXME"
        ),
        (
          2008111363L,
          "Actif",
          "actif",
          "name",
          "not used",
          "not used",
          "not used"
        ),
        (
          2002861901L,
          "3042942",
          "first",
          "name",
          "not used",
          "not used",
          "not used"
        ),
        (
          2002861897L,
          "3046810",
          "last",
          "name",
          "not used",
          "not used",
          "not used"
        ),
        (
          2002861900L,
          "398093005",
          "last",
          "name",
          "not used",
          "not used",
          "not used"
        )
      ).toDF(
        "concept_id",
        "concept_code",
        "concept_name",
        "vocabulary_reference",
        "fhir_concept_code",
        "fhir_concept_name",
        "fhir_vocabulary_reference"
      )
    }
  }

  /**
    * Creates an empty dataframe that follows the schema of an EDS table.
    *
    * @see [[EDSDataBundle.schemas]]
    */
  def emptyDF(tableName: String): DataFrame = {
    val spark = SparkSession.active
    val sc = spark.sparkContext
    spark.createDataFrame(sc.emptyRDD[Row], EDSDataBundle.schemas(tableName))
  }
}
