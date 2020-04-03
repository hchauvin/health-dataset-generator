package fr.aphp.wind.eds.generator.target

import fr.aphp.wind.eds.data.{GenericDataBundle, Validation}
import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.io.Source

package object eds {
  case class EDSDataBundle(persons: DataFrame, observations: DataFrame, visitOccurrences: DataFrame,
                           careSites: DataFrame, conditionOccurrences: DataFrame, procedureOccurrences: DataFrame,
                           providers: DataFrame) {
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

    def genericBundle: GenericDataBundle = {
      GenericDataBundle(Map(
        "persons" -> persons,
        "observations" -> observations,
        "visit_occurrences" -> visitOccurrences,
        "care_sites" -> careSites,
        "condition_occurrences" -> conditionOccurrences,
        "procedure_occurrences" -> procedureOccurrences,
        "providers" -> providers
      ))
    }

    def addMissingColumns(): EDSDataBundle = {
      new EDSDataBundle(genericBundle.addMissingColumns(EDSDataBundle.schemas))
    }

    def validate(allowMissingFields: Boolean = false): Validation = {
      genericBundle.validate(EDSDataBundle.schemas, allowMissingFields=allowMissingFields)
    }
  }

  object EDSDataBundle {
    import org.apache.spark.sql.avro.SchemaConverters

    val schemas: Map[String, StructType] = Map(
      "persons" -> "person.avro",
      "observations" -> "observation.avro",
      "visit_occurrences" -> "visit_occurrence.avro",
      "care_sites" -> "care_site.avro",
      "condition_occurrences" -> "condition_occurrence.avro",
      "procedure_occurrences" -> "procedure_occurrence.avro",
      "providers" -> "provider.avro"
    ).mapValues(fileName => {
      val schemaJSON = Source.fromResource(s"fr/aphp/wind/eds/generator/target/eds/avro_schemas/${fileName}").mkString
      val parser = new Schema.Parser
      SchemaConverters.toSqlType(parser.parse(schemaJSON)).dataType.asInstanceOf[StructType]
    })
  }
}
