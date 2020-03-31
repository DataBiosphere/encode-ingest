package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.Library
import upack.Msg

/** Transformation logic for ENCODE library objects. */
object LibraryTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE library into our preferred schema. */
  def transformLibrary(libraryInput: Msg): Library =
    Library(
      id = CommonTransformations.readId(libraryInput),
      crossReferences = libraryInput.read[Array[String]]("dbxrefs"),
      timeCreated = libraryInput.read[OffsetDateTime]("date_created"),
      award = libraryInput.read[String]("award"),
      lab = libraryInput.read[String]("lab"),
      queriedRnpSizeRange = libraryInput.tryRead[String]("queried_RNP_size_range"),
      rnaIntegrityNumber = libraryInput.tryRead[Double]("rna_integrity_number"),
      sizeRange = libraryInput.tryRead[String]("size_range"),
      strandSpecificity = libraryInput.read[Boolean]("strand_specificity"),
      treatments = libraryInput.read[Array[String]]("treatments"),
      submittedBy = libraryInput.read[String]("submitted_by"),
      spikeIns = libraryInput.read[Array[String]]("spikeins_used"),
      biosampleId = CommonTransformations.transformId(libraryInput.read[String]("biosample"))
    )
}
