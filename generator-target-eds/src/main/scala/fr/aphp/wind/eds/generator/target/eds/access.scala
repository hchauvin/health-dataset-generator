package fr.aphp.wind.eds.generator.target.eds

import fr.aphp.wind.eds.generator._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

/**
  * Adds methods for rights management to [[EDSDataBundle]], using an implicit class.
  *
  * Currently, authorization proceeds from OMOP tables: the principal must have a username
  * set to the "provider_source_value" of a provider.  This provider is assigned specific
  * roles to care sites using "care_site_history", and these roles grant specific
  * access to all the cohorts defined in a care site.
  */
object access {

  implicit class EDSDataBundleWithAccess(bundle: EDSDataBundle) {

    /** Adds cohorts according to a spec. */
    def withCohorts(newCohorts: Seq[CohortSpec]): EDSDataBundle = {
      val sqlContext = SparkSession.active.sqlContext
      import sqlContext.implicits._
      import org.apache.spark.sql.functions._

      bundle.copy(
        cohortDefinitions = bundle.cohortDefinitions
          .select(
            "cohort_definition_id",
            "cohort_definition_name",
            "owner_domain_id",
            "owner_entity_id"
          )
          .union(
            newCohorts
              .map(cd =>
                (cd.cohortDefinitionId, cd.name, "Provider", cd.careSiteId)
              )
              .toDF(
                "cohort_definition_id",
                "cohort_definition_name",
                "owner_domain_id",
                "owner_entity_id"
              )
          ),
        cohorts = bundle.cohorts
          .select("cohort_id", "cohort_definition_id", "subject_id")
          .union(
            newCohorts
              .map(cd =>
                cd.personIds
                  .toDF("subject_id")
                  .withColumn("cohort_id", uuidLong)
                  .withColumn(
                    "cohort_definition_id",
                    lit(cd.cohortDefinitionId)
                  )
              )
              .reduce { _ union _ }
              .select("cohort_id", "cohort_definition_id", "subject_id")
          )
      )
    }

    /** Adds the standard roles in [[standardRoles]]. */
    def withStandardRoles(): EDSDataBundle = {
      val sqlContext = SparkSession.active.sqlContext
      import sqlContext.implicits._

      bundle.copy(
        role = bundle.role
          .select("role_id", "name", "right_read_data_nominative")
          .union(
            Seq(
              (standardRoles.carer, "CARER", true)
            ).toDF("role_id", "name", "right_read_data_nominative")
          )
      )
    }

    /** Adds roles to providers according to a spec. */
    def withProviderRoles(
        providerRoles: Dataset[ProviderRole]
    ): EDSDataBundle = {
      val sqlContext = SparkSession.active.sqlContext
      import sqlContext.implicits._

      val rand = new Random()
      bundle.copy(
        careSiteHistory = bundle.careSiteHistory
          .select(
            "care_site_history_id",
            "care_site_id",
            "entity_id",
            "domain_id",
            "role_id"
          )
          .union(
            providerRoles
              .map(pr =>
                (
                  rand.nextLong,
                  pr.careSiteId,
                  pr.providerId,
                  "Provider",
                  pr.roleId
                )
              )
              .toDF(
                "care_site_history_id",
                "care_site_id",
                "entity_id",
                "domain_id",
                "role_id"
              )
          )
      )
    }

    /**
     * Adds a provider that has access to all the patients ("root access").
     *
     * This is done by having a cohort definition that includes all the patients
     * and to which this "root" provider has access.
     *
     * @param username The username, or "provider_source_value", of the provider.
     *                 This username can be used for authentication by other
     *                 systems.
     * @return A bundle with the new provider and related entries to set up
     *         their access rights.
     */
    def withRootProvider(username: String = "ROOT"): EDSDataBundle = {
      val sqlContext = SparkSession.active.sqlContext
      import sqlContext.implicits._
      import org.apache.spark.sql.functions._

      val providerId = 1L
      val careSiteId = 1L

      val personIds = bundle.persons.select('person_id.as[Long])

      bundle.copy(
        careSites = bundle.careSites
            .select("care_site_id", "care_site_name")
            .union((Seq(
              (careSiteId, "ROOT")
            ).toDF("care_site_id", "care_site_name"))),
        providers = bundle.providers
            .select("provider_id", "provider_source_value", "npi",
              "dea", "care_site_id", "is_active", "gender_concept_id")
            .union(Seq(
              (providerId, username, "ROOT", "ROOT", careSiteId, true, 8532L)
            ).toDF("provider_id", "provider_source_value", "npi",
              "dea", "care_site_id", "is_active", "gender_concept_id"))
        ).withStandardRoles()
          .withProviderRoles(Seq(
            ProviderRole(
              standardRoles.carer,
              providerId,
              careSiteId)
          ).toDS())
        .withCohorts(
          Seq(CohortSpec("ALL", careSiteId, personIds)))
    }
  }

  private val rand = new Random()

  /**
    * Spec for a cohort to add.
    *
    * @param name the name of the cohort (will become `cohort_definition_name`).
    * @param careSiteId The care site that owns the cohort.
    * @param personIds The IDs of the persons to add to the cohort.
    */
  case class CohortSpec(
      name: String,
      careSiteId: Long,
      personIds: Dataset[Long]
  ) {
    val cohortDefinitionId: Long = math.abs(rand.nextLong)
  }

  /**
    * Spec for a provider-role relationship to add.
    *
    * @param roleId The ID of the role (see, e.g., [[standardRoles]]).
    * @param providerId The ID of the provider to grant a role to.
    * @param careSiteId The ID of the care site to give a role for.
    */
  case class ProviderRole(roleId: Long, providerId: Long, careSiteId: Long)

  object standardRoles {
    val carer = 100

    lazy val df: DataFrame = {
      val sqlContext = SparkSession.active.sqlContext
      import sqlContext.implicits._

      Seq(
        (carer, "CARER", true)
      ).toDF("role_id", "name", "right_read_data_nominative")
    }
  }

}
