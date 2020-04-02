package fr.aphp.wind.eds.generator.target.eds

import fr.aphp.wind.eds.generator.source.synthea.SyntheaDataBundle
import org.apache.spark.sql.{DataFrame, SparkSession}

object FromSynthea {
  def apply(bundle: SyntheaDataBundle): EDSBundle = {
    EDSBundle(
      persons = convert.syntheaPatients.toPersons(bundle.patients),
      observations = convert.syntheaPatients.toObservations(bundle.patients),
      visitOccurrences = convert.syntheaEncounters.toVisitOccurrence(bundle.encounters),
      careSite = convert.syntheaOrganizations.toCareSite(bundle.organizations),
      conditionOccurrences = convert.syntheaConditions.toConditionOccurrence(bundle.conditions),
      procedureOccurrences = convert.syntheaProcedures.toProcedureOccurrence(bundle.procedures),
      providers = convert.syntheaProviders.toProvider(bundle.providers)
    )
  }

  private object convert {
    private lazy val sqlContext = SparkSession.active.sqlContext

    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    object syntheaPatients {
      def toPersons(df: DataFrame): DataFrame = {
        df.select("id", "gender", "birthdate", "deathdate")
          .withColumnRenamed("id", "person_id")
          .withColumn("gender_concept_id",
            when('gender === "M", 8507)
              .when('gender === "F", 8532))
          .withColumnRenamed("birthdate", "birth_datetime")
          .withColumnRenamed("deathdate", "death_datetime")
          .select("person_id", "concept_id", "birth_datetime", "death_datetime")
      }

      def toObservations(df: DataFrame): DataFrame = {
        Map("first" -> 3042942, "last" -> 3046810, "ssn" -> 398093005)
          .map { case (syntheaColumn, conceptId) => {
            df.select("id", syntheaColumn)
              .where('first.isNotNull)
              .withColumn("observation_concept_id", typedLit(conceptId))
              .withColumnRenamed("id", "person_id")
              .withColumnRenamed(syntheaColumn, "value_as_string")
          } }
          .reduce { _ union _ }
      }
    }

    object syntheaEncounters {
      def toVisitOccurrence(df: DataFrame): DataFrame = {
        df.select("id", "patient", "provider", "organization", "start", "stop")
          .withColumnRenamed("id", "visit_occurrence_id")
          .withColumnRenamed("patient", "person_id")
          .withColumnRenamed("provider", "provider_id")
          .withColumnRenamed("organization", "care_site_id")
          .withColumnRenamed("start", "visit_start_datetime")
          .withColumnRenamed("stop", "visit_end_datetime")
          .withColumn("visit_concept_id",
            when('encounterClass === "inpatient", 9201)
              .when('encounterClass === "ambulatory" || 'encounterClass === "outpatient"
                || 'encounterClass === "wellness", 9202)
              .when('encounterClass === "emergency" || 'encounterClass === "urgentcare", 9203))
          .withColumn("visit_type_concept_id", lit(44818517))
      }
    }

    object syntheaOrganizations {
      def toCareSite(df: DataFrame): DataFrame = {
        df.select("id", "name")
          .withColumnRenamed("id", "care_site_id")
          .withColumnRenamed("name", "care_site_name")
      }
    }

    object syntheaConditions {
      def toConditionOccurrence(df: DataFrame): DataFrame = {
        df.select("id", "start", "stop", "encounter", "patient", "code")
          .withColumnRenamed("id", "condition_occurrence_id")
          .withColumnRenamed("start", "condition_start_datetime")
          .withColumnRenamed("stop", "condition_end_datetime")
          .withColumnRenamed("encounter", "visit_occurrence_id")
          .withColumnRenamed("patient", "person_id")
          .withColumnRenamed("code", "condition_source_value")
          .withColumn("condition_type_source_value", lit("SNOMED"))
      }
    }

    object syntheaProcedures {
      def toProcedureOccurrence(df: DataFrame): DataFrame = {
        df.select("id", "code", "date", "patient", "encounter")
          .withColumnRenamed("id", "procedure_occurrence_id")
          .withColumnRenamed("code", "procedure_source_value")
          .withColumn("procedure_type_source_value", lit("SNOMED"))
          .withColumnRenamed("date", "procedure_datetime")
          .withColumnRenamed("patient", "person_id")
          .withColumnRenamed("encounter", "visit_occurrence_id")
      }
    }

    object syntheaProviders {
      def toProvider(df: DataFrame): DataFrame = {
        df.select("id", "name", "organization", "gender")
          .withColumnRenamed("id", "provider_id")
          .withColumnRenamed("name", "provider_name")
          .withColumn("provider_source_value", 'id)
          .withColumn("npi", 'id)
          .withColumn("dea", 'id)
          .withColumnRenamed("organization", "care_site_id")
          .withColumn("is_active", lit(true))
          .withColumn("gender_concept_id",
            when('gender === "M", 8507)
              .when('gender === "F", 8532))
      }
    }
  }
}
