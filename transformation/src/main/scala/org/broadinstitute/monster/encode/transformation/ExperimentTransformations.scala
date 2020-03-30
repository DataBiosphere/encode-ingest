package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.ExperimentActivity
import upack.Msg

object ExperimentTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema. */
  def transformExperiment(
    rawExperiment: Msg,
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
      /*
       * FIXME: Filling this in will require:
       *   1. Join raw replicates and libraries on replicate.library = library.@id
       *   2. Group those results by joined.experiment
       *   3. Join the groups with raw experiments on group.experiment = experiment.@id
       *   4. Pass in the (group, experiment) pair to this method, and pull out group.map(_.biosample)
       */
      biosampleIds = Array.empty[String]
    )
  }
}
