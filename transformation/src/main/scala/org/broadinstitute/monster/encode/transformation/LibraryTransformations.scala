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
    val id = CommonTransformations.readId(libraryInput)
    val pairedEndId = libraryInput.tryRead[String]("strand_specificity")

    Library(
      libraryId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        libraryInput.read[String]("@id")
      ) :: libraryInput.tryRead[List[String]]("dbxrefs").getOrElse(List.empty[String]),
      dateCreated = libraryInput.read[OffsetDateTime]("date_created"),
      award = CommonTransformations.convertToEncodeUrl(libraryInput.read[String]("award")),
      lab = CommonTransformations.convertToEncodeUrl(libraryInput.read[String]("lab")),
      queriedRnpSizeRange = libraryInput.tryRead[String]("queried_RNP_size_range"),
      rnaIntegrityNumber = libraryInput.tryRead[Double]("rna_integrity_number"),
      sizeRange = libraryInput.tryRead[String]("size_range"),
      libraryLayout = pairedEndId.isDefined,
      pairedEndId = pairedEndId.filterNot(_ == specificityPlaceholder),
      sampleTreatment = CommonTransformations.convertToEncodeUrl(
        libraryInput.tryRead[List[String]]("treatments").getOrElse(List.empty[String])
      ),
      submittedBy =
        CommonTransformations.convertToEncodeUrl(libraryInput.read[String]("submitted_by")),
      usedBy = libraryInput.tryRead[List[String]]("spikeins_used").getOrElse(List.empty[String]),
      usesSampleBiosampleId =
        CommonTransformations.transformId(libraryInput.read[String]("biosample")),
      prepMaterial = libraryInput.tryRead[String]("nucleic_acid_term_id"),
      prepMaterialName = libraryInput.tryRead[String]("nucleic_acid_term_name")
    )
  }

}
