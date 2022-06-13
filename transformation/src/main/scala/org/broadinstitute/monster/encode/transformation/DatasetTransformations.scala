package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.Dataset
import java.time.OffsetDateTime

/** Transformation logic for ENCODE donor objects. */
object DatasetTransformations {

  /** Transform a raw ENCODE donor into our preferred schema. */
  def transformDataset(): Dataset = {

    Dataset(
      datasetId = "ENCODE",
      label = Some("ENCODE 4 (Pre-release)"),
      xref = "https://www.encodeproject.org/" :: List(),
      title = Some("AnVIL ENCODE Pre-Release Dataset"),
      description = Some(
        "\"The Encyclopedia of DNA Elements (ENCODE) Consortium is an international collaboration of research groups funded by the National Human Genome Research Institute (NHGRI). The goal of ENCODE is to build a comprehensive parts list of functional elements in the human genome, including elements that act at the protein and RNA levels, and regulatory elements that control cells and circumstances in which a gene is active.\n\nNHGRI's AnVIL provides access to a restructured view of \"\"released\"\" ENCODE data modeled to facilitate search and interoperability for researchers.\n\nENCODE integrative analysis (PMID: 22955616; PMCID: PMC3439153)\nENCODE portal (PMID: 31713622 ; PMCID: PMC7061942)\""
      ),
      landingPage = Some(""),
      version = Some("ENCODE 4"),
      seeAlso = "https://www.encodeproject.org/" :: List(),
      conformsTo = Some("Terra Interoperability Model"),
      dateIssued = None,
      lastModifiedDate = Some(OffsetDateTime.now()),
      custodian = Some("anvil-data@broadinstitute.org"),
      contactPoint = "help@lists.anvilproject.org" :: List(),
      owner = Some("https://www.encodeproject.org/help/project-overview/"),
      fundedBy = "http://www.genome.gov/10005107" :: List(),
      generatedBy = "https://www.encodeproject.org/help/project-overview/" :: List(),
      dataUsePermission = Some("http://purl.obolibrary.org/obo/DUO_0000004"),
      license = "https://www.encodeproject.org/help/citing-encode" :: List(),
      dataModality = "Epigenomics" :: List(),
      partOfDatacollectionId = "AnVIL" :: List()
    )
  }
}
