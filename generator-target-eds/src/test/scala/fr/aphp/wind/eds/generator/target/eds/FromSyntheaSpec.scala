package fr.aphp.wind.eds.generator.target.eds

import java.io.File

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import fr.aphp.wind.eds.data.DFSupplyCache
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import fr.aphp.wind.eds.generator.source.synthea
import fr.aphp.wind.eds.generator.source.synthea.SyntheaDataBundle

class FromSyntheaSpec
    extends AnyFlatSpec
    with Matchers
    with DataFrameSuiteBase {

  behavior of "FromSynthea"

  it should "convert a synthea spec" in {
    val cache = new DFSupplyCache()

    val syntheaBundle = new SyntheaDataBundle(
      cache.cache(
        "synthea",
        synthea.tables,
        () => synthea.generate(1).genericBundle
      )
    )
    val edsBundle = FromSynthea(syntheaBundle).cache()
    edsBundle.validate(allowMissingFields = true).throwOnErrors()

    edsBundle.persons.count should equal(syntheaBundle.patients.count)
    edsBundle.visitOccurrences.count should equal(
      syntheaBundle.encounters.count
    )
    edsBundle.careSites.count should equal(syntheaBundle.organizations.count)
    edsBundle.conditionOccurrences.count should equal(
      syntheaBundle.conditions.count
    )
    edsBundle.procedureOccurrences.count should equal(
      syntheaBundle.procedures.count
    )
    edsBundle.providers.count should equal(syntheaBundle.providers.count)

    edsBundle.genericBundle.collect()
  }
}
