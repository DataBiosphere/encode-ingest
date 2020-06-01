package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import upack.Msg
import org.broadinstitute.monster.encode.jadeschema.table.Assay

object AssayTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for assays. */
  def transformExperiment(
    rawExperiment: Msg,
    rawLibraries: Iterable[Msg],
    fileIdToTypeMap: Map[String, FileType]
  ): Assay = {
    val id = CommonTransformations.readId(rawExperiment)

    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawExperiment)
    val usedFileBranches = FileTransformations.splitFileReferences(
      rawExperiment.read[List[String]]("contributing_files"),
      fileIdToTypeMap
    )
    val generatedFileBranches = FileTransformations.splitFileReferences(
      rawExperiment.read[List[String]]("files"),
      fileIdToTypeMap
    )

    val libraryArray = rawLibraries.toList

    Assay(
      id = id,
      crossReferences = rawExperiment.read[List[String]]("dbxrefs"),
      timeCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      description = rawExperiment.tryRead[String]("description"),
      // TODO: This is dangerous, should we make it optional / an array?
      assayCategory = rawExperiment.read[List[String]]("assay_slims").head,
      assayType = rawExperiment.read[String]("assay_term_id"),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawExperiment.read[String]("award"),
      lab = rawExperiment.read[String]("lab"),
      submittedBy = rawExperiment.read[String]("submitted_by"),
      // NOTE: Sorting the arrays below are important for reproducibility.
      biosampleIds = libraryArray.map { lib =>
        CommonTransformations.transformId(lib.read[String]("biosample"))
      }.sorted.distinct,
      usedAlignmentFileIds = usedFileBranches.alignment.sorted,
      usedSequenceFileIds = usedFileBranches.sequence.sorted,
      usedOtherFileIds = usedFileBranches.other.sorted,
      generatedAlignmentFileIds = generatedFileBranches.alignment.sorted,
      generatedSequenceFileIds = generatedFileBranches.sequence.sorted,
      generatedOtherFileIds = generatedFileBranches.other.sorted,
      antibodyIds = libraryArray.flatMap {
        _.tryRead[Array[String]]("antibodies")
          .getOrElse(Array.empty)
          .map(CommonTransformations.transformId)
      }.sorted.distinct,
      libraryIds = libraryArray.map(CommonTransformations.readId).sorted
    )
  }
}
