// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package fr.aphp.wind.eds.frictionless

/** A data resource according to the frictionless specification. */
object DataResource {
  case class Descriptor(
      path: Seq[String],
      name: String,
      profile: Option[String] = None,
      title: Option[String] = None,
      description: Option[String] = None,
      format: Option[String] = None,
      mediatype: Option[String] = None,
      encoding: Option[String] = None,
      bytes: Option[Long] = None,
      hash: Option[String] = None,
      sources: Seq[DataPackage.Source] = Seq.empty,
      licenses: Seq[DataPackage.License] = Seq.empty,
      schema: Option[String] = None
  )
}
