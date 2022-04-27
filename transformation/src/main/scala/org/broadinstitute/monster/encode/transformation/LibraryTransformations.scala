package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.Library
import upack.Msg

/** Transformation logic for ENCODE library objects. */
object LibraryTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Fallback value used by ENCODE for 'strand_specificity' in records that were submitted
    * before they allowed for distinguishing between forward/reverse strands.
    */
  private val specificityPlaceholder = "strand-specific"

  /** Transform a raw ENCODE library into our preferred schema. */
  def transformLibrary(libraryInput: Msg): Library = {
    val specificity = libraryInput.tryRead[String]("strand_specificity")

    Library(
      id = CommonTransformations.readId(libraryInput),
      crossReferences = libraryInput.read[List[String]]("dbxrefs"),
      timeCreated = libraryInput.read[OffsetDateTime]("date_created"),
      award = CommonTransformations.convertToEncodeUrl(libraryInput.read[String]("award")),
      lab = CommonTransformations.convertToEncodeUrl(libraryInput.read[String]("lab")),
      queriedRnpSizeRange = libraryInput.tryRead[String]("queried_RNP_size_range"),
      rnaIntegrityNumber = libraryInput.tryRead[Double]("rna_integrity_number"),
      sizeRange = libraryInput.tryRead[String]("size_range"),
      strandSpecific = specificity.isDefined,
      strandSpecificity = specificity.filterNot(_ == specificityPlaceholder),
      treatments = libraryInput.read[List[String]]("treatments"),
      submittedBy = CommonTransformations.convertToEncodeUrl(libraryInput.read[String]("submitted_by")),
      spikeIns = libraryInput.read[List[String]]("spikeins_used"),
      biosampleId = CommonTransformations.transformId(libraryInput.read[String]("biosample")),
      prepMaterial = libraryInput.tryRead[String]("nucleic_acid_term_id"),
      prepMaterialName = libraryInput.tryRead[String]("nucleic_acid_term_name")
    )
  }

}
