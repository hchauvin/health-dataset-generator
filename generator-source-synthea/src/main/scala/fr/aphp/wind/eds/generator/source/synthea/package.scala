package fr.aphp.wind.eds.generator.source

import java.nio.file.Paths

import fr.aphp.wind.eds.data.GenericDataBundle
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mitre.synthea.engine.Generator
import org.mitre.synthea.engine.Generator.GeneratorOptions
import org.mitre.synthea.export.Exporter.ExporterRuntimeOptions
import org.mitre.synthea.helpers.Config

package object synthea {
  case class SyntheaDataBundle(patients: DataFrame, encounters: DataFrame, organizations: DataFrame,
                               conditions: DataFrame, procedures: DataFrame, providers: DataFrame) {
    def this(bundle: GenericDataBundle) {
      this(
        patients = bundle("patients"),
        encounters = bundle("encounters"),
        organizations = bundle("organizations"),
        conditions = bundle("conditions"),
        procedures = bundle("procedures"),
        providers = bundle("providers"))
    }

    def genericBundle: GenericDataBundle = {
      GenericDataBundle(Map(
        "patients" -> patients,
        "encounters" -> encounters,
        "organizations" -> organizations,
        "conditions" -> conditions,
        "procedures" -> procedures,
        "providers" -> providers
      ))
    }
  }

  object SyntheaDataBundle {
    def fromCsvs(path: String): SyntheaDataBundle = {
      val spark = SparkSession.active
      new SyntheaDataBundle(GenericDataBundle(Seq(
        "patients",
        "encounters",
        "organizations",
        "conditions",
        "procedures",
        "providers"
      ).map(table => (table, spark.read.format("csv").option("header", "true").csv(path + "/csv/" + table + ".csv")))
        .toMap))
    }
  }

  def generate(population: Int): SyntheaDataBundle = {
    val baseDirectory = Paths.get("output").toAbsolutePath
    Config.set("exporter.baseDirectory", baseDirectory.toString) // TODO
    Config.set("exporter.csv.export", "true")
    new Generator(
      new GeneratorOptions {
        population = population
      },
      new ExporterRuntimeOptions {
        yearsOfHistory = 10
      }
    ).run()

    SyntheaDataBundle.fromCsvs("file://" + baseDirectory)
  }
}
