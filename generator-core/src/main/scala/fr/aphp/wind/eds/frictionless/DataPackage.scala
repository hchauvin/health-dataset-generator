package fr.aphp.wind.eds.frictionless

/**
 * @see http://specs.frictionlessdata.io/data-package
 */
object DataPackage {
  case class Descriptor(
                         resources: Seq[DataResource.Descriptor],
                                    name: Option[String] = None,
                                    id: Option[String] = None,
                                    licenses: Seq[License] = Seq.empty,
                                    profile: Option[String] = None,
                                    title: Option[String] = None,
                                    description: Option[String] = None,
                                    homepage: Option[String] = None,
                                    version: Option[String] = None,
                                    sources: Seq[Source] = Seq.empty,
                                    contributors: Seq[Source] = Seq.empty,
                                    keywords: Seq[String] = Seq.empty,
                                    image: Option[String] = None,
                                    created: Option[String] = None)

  case class License(name: String, path: String, title: Option[String] = None)

  case class Source(title: String, path: Option[String] = None, email: Option[String] = None)

  case class Contributor(title: String, email: Option[String] = None, path: Option[String] = None,
                         role: Option[String] = None,
                         organization: Option[String] = None)
}
