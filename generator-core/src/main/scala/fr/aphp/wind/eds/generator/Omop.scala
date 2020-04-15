// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds.generator

import java.util.regex.Pattern

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
  * Helpers to manipulate the OMOP standard.
  *
  * @see https://github.com/OHDSI/CommonDataModel
  */
object Omop {

  /** The field "categories", derived from the field name suffixes. */
  sealed abstract class FieldCategory(
      /** The suffix of the fields that match the category. */
      val fieldSuffix: String,
      /** Whether the field is related to OMOP concepts. */
      val conceptField: Boolean = false,
      /** Whether the field is a "concept source" field. */
      val source: Boolean = false,
      /** Whether the field holds integer values. */
      val int: Boolean = false
  ) extends EnumEntry

  object FieldCategory extends Enum[FieldCategory] {
    val values: immutable.IndexedSeq[FieldCategory] = findValues

    case object TypeSourceValue
        extends FieldCategory(
          "_type_source_value",
          conceptField = true,
          source = true
        )
    case object TypeSourceConceptId
        extends FieldCategory(
          "_type_source_concept_id",
          conceptField = true,
          source = true,
          int = true
        )
    case object TypeConceptId
        extends FieldCategory(
          "_type_concept_id",
          conceptField = true,
          int = true
        )
    case object SourceValue
        extends FieldCategory(
          "_source_value",
          conceptField = true,
          source = true
        )
    case object SourceConceptId
        extends FieldCategory(
          "_source_concept_id",
          conceptField = true,
          source = true,
          int = true
        )
    case object ConceptId
        extends FieldCategory("_concept_id", conceptField = true, int = true)
    case object RecordId extends FieldCategory("_id")
    case object Other extends FieldCategory("")
  }

  /** A DB field.  This class allows accessing metadata for this field. */
  case class Field(name: String) {

    /** The "category" of the field. */
    def category: FieldCategory = {
      if (name.endsWith("_type_source_value")) FieldCategory.TypeSourceValue
      else if (name.endsWith("_type_source_concept_id"))
        FieldCategory.TypeSourceConceptId
      else if (name.endsWith("_type_concept_id")) FieldCategory.TypeConceptId
      if (name.endsWith("_source_value")) FieldCategory.SourceValue
      else if (name.endsWith("_source_concept_id"))
        FieldCategory.SourceConceptId
      else if (name.endsWith("_concept_id")) FieldCategory.ConceptId
      else if (name.endsWith("_id")) FieldCategory.RecordId
      else FieldCategory.Other
    }

    /**
      * If the field is a concept field, the corresponding concept, arrived at
      * after stripping the category suffix (such as "_concept_id").
      */
    def conceptStem: String = {
      Field.conceptStemRe.matcher(name).replaceFirst("")
    }

    /** Gets related concept fields. */
    def concept: FieldConcept = FieldConcept(conceptStem)
  }

  object Field {
    private lazy val conceptStemRe = Pattern.compile(
      "(" + FieldCategory.values
        .filter { _.conceptField }
        .map { _.fieldSuffix }
        .mkString("|") + ")$"
    )
  }

  /** All the related concept fields. */
  case class FieldConcept(stem: String) {
    val typeSourceValue: String =
      stem + FieldCategory.TypeSourceValue.fieldSuffix
    val typeSourceConceptId: String =
      stem + FieldCategory.TypeSourceConceptId.fieldSuffix
    val typeConceptId: String = stem + FieldCategory.TypeConceptId.fieldSuffix
    val sourceValue: String = stem + FieldCategory.SourceValue.fieldSuffix
    val sourceConceptId: String =
      stem + FieldCategory.SourceConceptId.fieldSuffix
    val conceptId: String = stem + FieldCategory.ConceptId.fieldSuffix
  }

  /**
    * Expands all the concept fields that are found in a sequence of fields.
    * For instance, if a "_source_concept_id" field is found, a "_concept_id" field
    * is added.
    */
  def expandConceptFields(names: Seq[String]): Seq[String] = {
    val conceptFields: Seq[Field] = names
      .map { Field(_) }
      .filter(name => name.category != FieldCategory.Other)
      .distinct
    conceptFields.flatMap { field =>
      {
        val stem = field.conceptStem
        FieldCategory.values.filter { _.conceptField }.map {
          stem + _.fieldSuffix
        }
      }
    }
  }
}
