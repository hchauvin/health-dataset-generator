// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds.generator.target.eds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AccessSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  behavior of "withProviderRoles"

  it should "grant a role to a provider for the given sites" in {
    val providerId = 1L
    val careSiteId1 = 10L
    val careSiteId2 = 20L

    import sqlContext.implicits._
    import access._

    val bundle = EDSDataBundle.empty
      .copy(
        providers = Seq(providerId).toDF("provider_id"),
        careSites = Seq(careSiteId1, careSiteId2).toDF("care_site_id")
      )
      .checkpoint(allowMissingFields = true)
      .withStandardRoles()
      .checkpoint(allowMissingFields = true)
      .checkpoint(allowMissingFields = true)
      .withProviderRoles(
        Seq(
          ProviderRole(standardRoles.carer, providerId, careSiteId1),
          ProviderRole(standardRoles.carer, providerId, careSiteId2)
        ).toDS
      )
      .checkpoint(allowMissingFields = true)
      .cache()

    bundle.genericBundle.collect()

    bundle.careSiteHistory
      .select('care_site_id.as[Long])
      .collect()
      .toSet should equal(Set(careSiteId1, careSiteId2))
  }

  behavior of "withRootProvider"

  it should "grant a newly created 'root' provider access to all the persons" in {
    import sqlContext.implicits._
    import access._

    val bundle = EDSDataBundle.empty
      .copy(persons = Seq(123L, 456L).toDF("person_id"))
      .checkpoint(allowMissingFields=true)
      .withRootProvider("foo")
      .checkpoint(allowMissingFields=true)
      .cache()

    bundle.genericBundle.collect()

    val providers = bundle.providers.collect()
    providers should have length(1)
    providers(0).getAs[String]("provider_source_value") should equal("foo")
    val providerId = providers(0).getAs[Long]("provider_id")

    val careSites = bundle.careSites.collect()
    careSites should have length(1)
    val careSiteId = careSites(0).getAs[Long]("care_site_id")

    val careSiteHistory = bundle.careSiteHistory.collect()
    careSiteHistory should have length(1)
    careSiteHistory(0).getAs[Long]("care_site_id") should equal(careSiteId)
    careSiteHistory(0).getAs[Long]("entity_id") should equal(providerId)

    val cohortDefinitions = bundle.cohortDefinitions.collect()
    cohortDefinitions should have length(1)
    val cohortDefinitionId = cohortDefinitions(0).getAs[Long]("cohort_definition_id")

    val cohorts = bundle.cohorts.collect()
    cohorts should have length(2)
    cohorts.map { _.getAs[Long]("cohort_definition_id") }.toSet should equal(Set(cohortDefinitionId))
    cohorts.map { _.getAs[Long]("subject_id") }.toSet should equal(Set(123L, 456L))
  }
}
