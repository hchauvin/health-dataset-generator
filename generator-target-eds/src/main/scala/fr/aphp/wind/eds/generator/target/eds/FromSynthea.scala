// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds.generator.target.eds

import java.sql.{Date, Timestamp}
import java.util.UUID

import fr.aphp.wind.eds.generator.source.synthea.SyntheaDataBundle
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType}
import fr.aphp.wind.eds.generator.uuidLong

/** Converts a data bundle produced by Synthea to the format expected by the EDS.  */
object FromSynthea {

  /**
    * Performs the conversion.
    *
    * @param bundle The Synthea bundle to convert.
    * @return The EDS bundle produced by the conversion.
    */
  def apply(bundle: SyntheaDataBundle): EDSDataBundle = {
    EDSDataBundle(
      fhirConcepts = convert.syntheaConcepts
        .toFhirConcepts(bundle.conditions, bundle.procedures),
      persons = convert.syntheaPatients.toPersons(bundle.patients),
      observations = convert.syntheaPatients.toObservations(bundle.patients),
      visitOccurrences =
        convert.syntheaEncounters.toVisitOccurrences(bundle.encounters),
      visitDetail = emptyDF("visit_detail"),
      notes = convert.syntheaEncounters.toNotes(bundle.encounters),
      careSites =
        convert.syntheaOrganizations.toCareSites(bundle.organizations),
      conditionOccurrences =
        convert.syntheaConditions.toConditionOccurrences(bundle.conditions),
      procedureOccurrences =
        convert.syntheaProcedures.toProcedureOccurrences(bundle.procedures),
      providers = convert.syntheaProviders.toProviders(bundle.providers),
      costs = convert.syntheaProcedures.toCosts(bundle.procedures),
      locations = convert.syntheaPatients.toLocations(bundle.patients),
      cohortDefinitions = emptyDF("cohort_definition"),
      cohorts = emptyDF("cohort"),
      role = emptyDF("role"),
      careSiteHistory = emptyDF("care_site_history"),
      factRelationship = emptyDF("fact_relationship")
    )
  }

  /**
    * All the conversions are done through methods within the "convert" object.
    *
    * The convention is as follows: a Synthea table A is converted to an EDS table B
    * through a method `convert.syntheaA.toB`.
    */
  private[eds] object convert {
    private lazy val sqlContext = SparkSession.active.sqlContext

    // These imports allow to use SQL functions and to refer to dataframe columns
    // without qualifiers.
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    /**
      * User-Defined SQL function that casts a UUID as generated by Synthea into
      * an OMOP-compatible ID.
      */
    private[eds] val omopId: UserDefinedFunction = udf((stringId: String) => {
      // NOTE: UUIDs are 128 bits, but long values are 64.  We only use the
      // least significant bits of the UUIDs, which _might_ give collisions.
      // One way to guard against collisions would be to ensure uniqueness after all the
      // tables are generated.
      math.abs(UUID.fromString(stringId).getLeastSignificantBits)
    })

    /**
      * Converts a column containing datetimes in Synthea format to a column
      * that contains Spark timestamps.
      */
    private[eds] def fromSyntheaDatetime(column: Column): Column = {
      when(column.isNull, typedLit[Timestamp](null))
        .otherwise(to_timestamp(column, "yyyy-MM-dd'T'HH:mm'Z'"))
    }

    /**
      * Converts a column containing dates in Synthea format to a column
      * that contains Spark dates.
      */
    private[eds] def fromSyntheaDate(column: Column): Column = {
      when(column.isNull, typedLit[Date](null))
        .otherwise(to_date(column, "yyyy-MM-dd"))
    }

    /**
      * Converts a column containing dates in Synthea format to a column
      * that contains Spark timestamps.  The timestamps all refer to midnight.
      */
    private[eds] def fromSyntheaDateToTimestamp(column: Column): Column = {
      when(column.isNull, typedLit[Date](null))
        .otherwise(to_timestamp(column, "yyyy-MM-dd"))
    }

    object syntheaConcepts {
      def toFhirConcepts(
          condition_df: DataFrame,
          procedure_df: DataFrame
      ): DataFrame = {

        condition_df
          .select("code", "description")
          .union(procedure_df.select("code", "description"))
          .dropDuplicates("code")
          .withColumn("concept_id", uuidLong)
          .withColumn("vocabulary_reference", lit("SNOMED"))
          .withColumn("fhir_concept_code", 'code)
          .withColumn("fhir_concept_name", 'description)
          .withColumn("fhir_vocabulary_reference", lit("SNOMED"))
          .withColumnRenamed("code", "concept_code")
          .withColumnRenamed("description", "concept_name")
          .select(
            "concept_id",
            "concept_code",
            "concept_name",
            "vocabulary_reference",
            "fhir_concept_code",
            "fhir_concept_name",
            "fhir_vocabulary_reference"
          )
          .union(
            builtins.fhirConcepts
              .select(
                "concept_id",
                "concept_code",
                "concept_name",
                "vocabulary_reference",
                "fhir_concept_code",
                "fhir_concept_name",
                "fhir_vocabulary_reference"
              )
          )
      }
    }

    object syntheaPatients {
      def toPersons(df: DataFrame): DataFrame = {
        df.select("id", "gender", "birthdate", "deathdate")
          .withColumn("person_id", omopId('id))
          .withColumn(
            "gender_concept_id",
            when('gender === "M", 8507L)
              .when('gender === "F", 8532L)
          )
          .withColumn("birth_datetime", fromSyntheaDateToTimestamp('birthdate))
          .withColumn("death_datetime", fromSyntheaDateToTimestamp('deathdate))
          .select(
            "person_id",
            "gender_concept_id",
            "birth_datetime",
            "death_datetime"
          )
          .withColumn(
            "gender_source_concept_id",
            when('gender_concept_id === 8507L, 1001L)
              .when('gender_concept_id === 8532L, 1002L)
          )
          // Status set to "active".
          .withColumn("row_status_source_concept_id", typedLit(2008111363L))
      }

      def toObservations(df: DataFrame): DataFrame = {
        Map(
          "first" -> (3042942L, 2002861901L),
          "last" -> (3046810L, 2002861897L),
          "ssn" -> (398093005L, 2002861900L)
        ).map {
            case (syntheaColumn, (conceptId, sourceConceptId)) =>
              df.select("id", syntheaColumn)
                .where('first.isNotNull)
                .withColumn("observation_concept_id", typedLit(conceptId))
                .withColumn(
                  "observation_source_concept_id",
                  typedLit(sourceConceptId)
                )
                .withColumn("person_id", omopId('id))
                .withColumnRenamed(syntheaColumn, "value_as_string")
                .drop("id")
          }
          .reduce {
            _ union _
          }
      }

      def toLocations(df: DataFrame): DataFrame = {

        df.select("address", "city", "state", "lat", "lon")
          .withColumn("location_id", uuidLong)
          .withColumn("lat", col("lat").cast(DoubleType))
          .withColumn("lon", col("lon").cast(DoubleType))
          .withColumnRenamed("lat", "latitude")
          .withColumnRenamed("lon", "longitude")
      }
    }

    object syntheaEncounters {
      def toVisitOccurrences(df: DataFrame): DataFrame = {
        df.select(
            "id",
            "patient",
            "provider",
            "organization",
            "start",
            "stop",
            "encounterClass"
          )
          .withColumn("visit_occurrence_id", omopId('id))
          .withColumn("person_id", omopId('patient))
          .withColumn("provider_id", omopId('provider))
          .withColumn("care_site_id", omopId('organization))
          .withColumn("visit_start_datetime", fromSyntheaDatetime('start))
          .withColumn("visit_end_datetime", fromSyntheaDatetime('stop))
          .withColumn(
            "visit_concept_id",
            when('encounterClass === "inpatient", 9201L)
              .when(
                'encounterClass === "ambulatory" || 'encounterClass === "outpatient"
                  || 'encounterClass === "wellness",
                9202L
              )
              .when(
                'encounterClass === "emergency" || 'encounterClass === "urgentcare",
                9203
              )
          )
          .withColumn("visit_type_concept_id", lit(44818517L))
          .select(
            "visit_occurrence_id",
            "person_id",
            "provider_id",
            "care_site_id",
            "visit_start_datetime",
            "visit_end_datetime",
            "visit_concept_id",
            "visit_type_concept_id"
          )
      }

      def toNotes(df: DataFrame): DataFrame = {

        df.select("id", "patient", "provider", "organization", "start")
          .withColumn("note_id", uuidLong)
          .withColumn("care_site_id", omopId('organization))
          .withColumn("note_class_concept_id", lit(44814639L))
          .withColumn("note_class_source_value", lit(""))
          .withColumn("note_datetime", fromSyntheaDatetime('start))
          .withColumn("note_event_field_concept_id", lit(1147332L))
          .withColumn("note_title", lit("A simple lorem ipsum"))
          /** http://athena.ohdsi.org/search-terms/terms?conceptClass=Note+Type&domain=Type+Concept */
          .withColumn("note_type_concept_id", lit(44814639L))
          .withColumn("note_type_source_value", lit(""))
          .withColumn("person_id", omopId('patient))
          .withColumn("provider_id", omopId('provider))
          .withColumn("visit_detail_id", omopId('id))
          .withColumn("visit_occurrence_id", omopId('id))
          .withColumn(
            "note_text",
            lit(
              """Lorem ipsum dolor sit amet, consectetur adipiscing elit,
                                        sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."""
            )
          )
          .withColumn("delete_datetime", fromSyntheaDatetime(lit(null)))
          .withColumn("encoding_concept_id", lit(0L))
          .withColumn("insert_datetime", fromSyntheaDatetime('start))
          .withColumn("is_pdf_available", lit(false))
          .withColumn("language_concept_id", lit(4180186L))
          .withColumn("note_event_id", omopId('id))
          .withColumn("update_datetime", fromSyntheaDatetime('start))
          //.withColumn("note_class_source_concept_id", lit(0L))
          .withColumn("row_status_source_concept_id", lit(1003L))
          .drop("id", "patient", "provider", "organization", "start")
      }
    }

    object syntheaOrganizations {
      def toCareSites(df: DataFrame): DataFrame = {
        df.select("id", "name")
          .withColumn("care_site_id", omopId('id))
          .withColumn("care_site_name", 'name)
          .select("care_site_id", "care_site_name")
      }
    }

    object syntheaConditions {
      def toConditionOccurrences(df: DataFrame): DataFrame = {

        df.select("start", "stop", "encounter", "patient", "code")
          .withColumn("condition_occurrence_id", uuidLong)
          .withColumn(
            "condition_start_datetime",
            fromSyntheaDateToTimestamp('start)
          )
          .withColumn(
            "condition_end_datetime",
            fromSyntheaDateToTimestamp('stop)
          )
          .drop("start", "stop")
          .withColumn("visit_occurrence_id", omopId('encounter))
          .withColumn("person_id", omopId('patient))
          .drop("encounter", "patient")
          .withColumnRenamed("code", "condition_source_value")
          .withColumn("condition_type_source_value", lit("SNOMED"))
      }
    }

    object syntheaProcedures {
      def toProcedureOccurrences(df: DataFrame): DataFrame = {

        df.select("code", "date", "patient", "encounter")
          .withColumn("procedure_occurrence_id", uuidLong)
          .withColumnRenamed("code", "procedure_source_value")
          .withColumn("procedure_type_source_value", lit("SNOMED"))
          .withColumn("procedure_datetime", fromSyntheaDatetime('date))
          .drop("date")
          .withColumn("person_id", omopId('patient))
          .withColumn("visit_occurrence_id", omopId('encounter))
          .withColumn("row_status_source_concept_id", lit(4082735L))
          .drop("patient", "encounter")
      }

      def toCosts(df: DataFrame): DataFrame = {

        df.select("patient", "date", "encounter")
          .withColumn("cost_id", uuidLong)
          .withColumn("incurred_datetime", fromSyntheaDate('date))
          .withColumn("person_id", omopId('patient))
          .withColumn("drg_source_concept_id", lit(1005L))
          .withColumn("row_status_source_concept_id", lit(1004L))
          .withColumn(
            "cost_event_field_concept_id",
            lit(1147624L)
          ) // visit_detail
          .withColumn("cost_event_id", omopId('encounter))
          .drop("patient", "date", "encounter")
      }
    }

    object syntheaProviders {
      def toProviders(df: DataFrame): DataFrame = {
        df.select("id", "name", "organization", "gender")
          .withColumn("provider_id", omopId('id))
          .withColumnRenamed("name", "provider_name")
          .withColumn("provider_source_value", 'id)
          .withColumn("npi", 'id)
          .withColumn("dea", 'id)
          .drop("id")
          .withColumn("care_site_id", omopId('organization))
          .drop("organization")
          .withColumn("is_active", lit(true))
          .withColumn(
            "gender_concept_id",
            when('gender === "M", 8507L)
              .when('gender === "F", 8532L)
          )
          .drop("gender")
      }
    }

  }

}
