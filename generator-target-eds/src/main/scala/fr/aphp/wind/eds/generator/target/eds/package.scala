package fr.aphp.wind.eds.generator.target

import fr.aphp.wind.eds.data.{GenericDataBundle, Validation}
import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.io.Source

/**
  * EDS-specific generator.
  *
  * The _Entrepôt des Données de Santé_ is a medical data warehouse
  * developed at the [[https://en.wikipedia.org/wiki/Assistance_Publique_%E2%80%93_H%C3%B4pitaux_de_Paris AP-HP]],
  * the university hospital trust operating in Paris and its surroundings.
  *
  * @see https://eds.aphp.fr
  */
package object eds {

  /**
    * A data bundle containing all the data describing a set of patients and their interaction
    * with the healthcare system.
    */
  case class EDSDataBundle(
      persons: DataFrame,
      observations: DataFrame,
      visitOccurrences: DataFrame,
      careSites: DataFrame,
      conditionOccurrences: DataFrame,
      procedureOccurrences: DataFrame,
      providers: DataFrame
  ) {
    def this(bundle: GenericDataBundle) {
      this(
        persons = bundle("persons"),
        observations = bundle("observations"),
        visitOccurrences = bundle("visit_occurrences"),
        careSites = bundle("care_sites"),
        conditionOccurrences = bundle("condition_occurrences"),
        procedureOccurrences = bundle("procedure_occurrences"),
        providers = bundle("providers")
      )
    }

    /**
      * Converts the bundle with dataframes as fields to a generic bundle with dataframes
      * as map entries.
      */
    def genericBundle: GenericDataBundle = {
      GenericDataBundle(
        Map(
          "persons" -> persons,
          "observations" -> observations,
          "visit_occurrences" -> visitOccurrences,
          "care_sites" -> careSites,
          "condition_occurrences" -> conditionOccurrences,
          "procedure_occurrences" -> procedureOccurrences,
          "providers" -> providers
        )
      )
    }

    /**
      * Calls [[GenericDataBundle.addMissingColumns]] with the APHP-specific schemas.
      */
    def addMissingColumns(): EDSDataBundle = {
      new EDSDataBundle(genericBundle.addMissingColumns(EDSDataBundle.schemas))
    }

    /**
      * Calls [[GenericDataBundle.validate]] with the APHP-specific schemas.
      */
    def validate(allowMissingFields: Boolean = false): Validation = {
      genericBundle.validate(
        EDSDataBundle.schemas,
        allowMissingFields = allowMissingFields
      )
    }
  }

  object EDSDataBundle {
    import org.apache.spark.sql.avro.SchemaConverters

    /**
      * The AP-HP specific schemas.  The keys are the table/dataframe names.
      */
    val schemas: Map[String, StructType] = Map(
      "persons" -> "person.avro",
      "observations" -> "observation.avro",
      "visit_occurrences" -> "visit_occurrence.avro",
      "care_sites" -> "care_site.avro",
      "condition_occurrences" -> "condition_occurrence.avro",
      "procedure_occurrences" -> "procedure_occurrence.avro",
      "providers" -> "provider.avro"
    ).mapValues(fileName => {
      val schemaJSON = Source
        .fromResource(
          s"fr/aphp/wind/eds/generator/target/eds/avro_schemas/${fileName}"
        )
        .mkString
      val parser = new Schema.Parser
      SchemaConverters
        .toSqlType(parser.parse(schemaJSON))
        .dataType
        .asInstanceOf[StructType]
    })
  }
}
