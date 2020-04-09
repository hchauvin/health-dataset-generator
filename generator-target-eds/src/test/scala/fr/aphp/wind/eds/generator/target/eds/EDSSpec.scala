package fr.aphp.wind.eds.generator.target.eds

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import fr.aphp.wind.eds.data.validateDF

class EDSSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  behavior of "EDSDataBundle"

  it should "convert to a generic bundle" in {
    val bundle = EDSDataBundle.empty
    val nextBundle = new EDSDataBundle(bundle.genericBundle)
    nextBundle.genericBundle.collect()
  }

  behavior of "builtinsFhirConcepts"

  it should "be a valid concept_fhir table" in {
    validateDF(
      builtins.fhirConcepts,
      EDSDataBundle.schemas("concept_fhir"),
      allowMissingFields = true
    ).throwOnErrors()
  }

}
