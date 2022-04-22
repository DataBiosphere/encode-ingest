package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.Experiment
import upack.Msg

object ExperimentTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for experiments. */
  def transformExperiment(
    rawExperiment: Msg,
    rawLibraries: Iterable[Msg],
    fileIdToTypeMap: Map[String, FileType]
  ): Experiment = {

    val libraryArray = rawLibraries.toList

    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawExperiment)

    val usedFileBranches = FileTransformations.splitFileReferences(
      rawExperiment.read[List[String]]("contributing_files"),
      fileIdToTypeMap
    )
    val generatedFileBranches = FileTransformations.splitFileReferences(
      rawExperiment.read[List[String]]("files"),
      fileIdToTypeMap
    )

    Experiment(
      id = CommonTransformations.readId(rawExperiment),
      crossReferences = rawExperiment.read[List[String]]("dbxrefs"),
      timeCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      description = rawExperiment.tryRead[String]("description"),
      dataModality = rawExperiment.tryRead[String]("assay_term_name") match {
        case Some(str) => Some(AssayTransformations.transformAssayTermToDataModality(str))
        case None      => None
      },
      award = rawExperiment.read[String]("award"),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      lab = rawExperiment.read[String]("lab"),
      submittedBy = rawExperiment.read[String]("submitted_by"),
      status = rawExperiment.read[String]("status"),
      usedAlignmentFileIds = usedFileBranches.alignment.sorted,
      usedSequenceFileIds = usedFileBranches.sequence.sorted,
      usedOtherFileIds = usedFileBranches.other.sorted,
      generatedAlignmentFileIds = generatedFileBranches.alignment.sorted,
      generatedSequenceFileIds = generatedFileBranches.sequence.sorted,
      generatedOtherFileIds = generatedFileBranches.other.sorted,
      biosampleIds = libraryArray.map { lib =>
        CommonTransformations.transformId(lib.read[String]("biosample"))
      }.sorted.distinct,
      antibodyIds = libraryArray.flatMap {
        _.tryRead[Array[String]]("antibodies")
          .getOrElse(Array.empty)
          .map(CommonTransformations.transformId)
      }.sorted.distinct,
      libraryIds = libraryArray.map(CommonTransformations.readId).sorted
    )
  }
}
