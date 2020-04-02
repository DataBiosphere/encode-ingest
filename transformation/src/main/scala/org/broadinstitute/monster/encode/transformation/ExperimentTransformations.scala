package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.ExperimentActivity
import upack.Msg

object ExperimentTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema. */
  def transformExperiment(
    rawExperiment: Msg,
    rawLibraries: Iterable[Msg],
    fileIdToTypeMap: Map[String, FileType]
  ): ExperimentActivity = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawExperiment)
    val fileBranches = FileTransformations.splitFileReferences(
      rawExperiment.read[Array[String]]("files"),
      fileIdToTypeMap
    )

    ExperimentActivity(
      id = CommonTransformations.readId(rawExperiment),
      crossReferences = rawExperiment.read[Array[String]]("dbxrefs"),
      timeCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      generatedAlignmentFileIds = fileBranches.alignment,
      generatedSequenceFileIds = fileBranches.sequence,
      generatedOtherFileIds = fileBranches.other,
      description = rawExperiment.tryRead[String]("description"),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawExperiment.read[String]("award"),
      lab = rawExperiment.read[String]("lab"),
      submittedBy = rawExperiment.read[String]("submitted_by"),
      biosampleIds = rawLibraries.map { lib =>
        CommonTransformations.transformId(lib.read[String]("biosample"))
      }.toArray.distinct
    )
  }
}
