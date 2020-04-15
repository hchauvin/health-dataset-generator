// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds.generator

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import fr.aphp.wind.eds.generator.Omop.Field
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OmopSpec extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  behavior of "Field.conceptStem"

  it should "extract the concept stem" in {
    Field("foo_concept_id").conceptStem should equal("foo")
  }

}
