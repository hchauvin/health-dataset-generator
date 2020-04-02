package fr.aphp.wind.eds.generator.target

import fr.aphp.wind.eds.generator.GenericDataBundle
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

package object eds {
  case class EDSBundle(persons: DataFrame, observations: DataFrame, visitOccurrences: DataFrame,
                       careSite: DataFrame, conditionOccurrences: DataFrame, procedureOccurrences: DataFrame,
                       providers: DataFrame) {
    def resolveConcepts: EDSBundle = {
      EDSBundle(persons = genericResolveConcepts(persons),
        observations = genericResolveConcepts(observations),
        visitOccurrences = genericResolveConcepts(visitOccurrences),
        careSite = genericResolveConcepts(careSite),
        conditionOccurrences = genericResolveConcepts(conditionOccurrences),
        procedureOccurrences = genericResolveConcepts(procedureOccurrences),
        providers = genericResolveConcepts(providers)
      )
    }

    def genericBundle: GenericDataBundle = {
      GenericDataBundle(Map(
        "person" -> persons,
        "observation" -> observations,
        "visit_occurrence" -> visitOccurrences,
        "care_site" -> careSite,
        "condition_occurrence" -> conditionOccurrences,
        "procedure_occurrence" -> procedureOccurrences,
        "provider" -> providers
      ))
    }
  }

  private def genericResolveConcepts(df: DataFrame): DataFrame = {
    df
  }
}
