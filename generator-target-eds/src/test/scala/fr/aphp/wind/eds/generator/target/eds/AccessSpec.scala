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
}
