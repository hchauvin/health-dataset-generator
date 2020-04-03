package fr.aphp.wind.eds.generator.target.eds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import fr.aphp.wind.eds.generator.source.synthea

class FromSyntheaSpec extends AnyFlatSpec
  with Matchers
  with DataFrameSuiteBase {

  behavior of "FromSynthea"

  it should "convert a synthea spec" in {
    val syntheaBundle = synthea.generate(1)
    val edsBundle = FromSynthea(syntheaBundle)
    edsBundle.validate(allowMissingFields=true).throwOnErrors()

    edsBundle.persons.count should equal (syntheaBundle.patients.count)
    edsBundle.visitOccurrences.count should equal (syntheaBundle.encounters.count)
    edsBundle.careSites.count should equal (syntheaBundle.organizations.count)
    edsBundle.conditionOccurrences.count should equal (syntheaBundle.conditions.count)
    edsBundle.procedureOccurrences.count should equal (syntheaBundle.procedures.count)
    edsBundle.providers.count should equal (syntheaBundle.providers.count)
  }

}
