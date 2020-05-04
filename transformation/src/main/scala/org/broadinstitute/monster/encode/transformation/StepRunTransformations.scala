package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.StepRun
import upack.Msg

/** Transformation logic for ENCODE analysis step run objects. */
object StepRunTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE analysis step run into our preferred schema. */
  def transformStepRun(
    rawStepRun: Msg,
    rawStepVersion: Msg,
    rawStep: Msg,
    rawGeneratedFiles: Iterable[Msg],
    fileIdToTypeMap: Map[String, FileType]
  ): StepRun = {

    // generate the pipeline run id
    val experimentIds = rawGeneratedFiles.toArray
      .map(_.read[String]("dataset"))
      .distinct
      .map(CommonTransformations.transformId)
    val pipelineIds =
      rawStep.read[Array[String]]("pipelines").map(CommonTransformations.transformId)
    val pipelineRunId: Option[String] =
      if (pipelineIds.length == 1 && experimentIds.length == 1)
        Some(pipelineIds.head + "-" + experimentIds.head)
      else None // TODO log if None

    // branch files
    val generatedFileArray = rawGeneratedFiles.toArray
    val generatedFileBranches = FileTransformations.splitFileReferences(
      generatedFileArray.map(_.read[String]("@id")),
      fileIdToTypeMap
    )
    val usedFileIds = generatedFileArray
      .flatMap(_.read[Array[String]]("derived_from"))
      .distinct // TODO check that this is the correct way to get the used files
    val usedFileBranches = FileTransformations.splitFileReferences(usedFileIds, fileIdToTypeMap)

    StepRun(
      id = CommonTransformations.readId(rawStepRun),
      version = rawStepVersion.read[String]("name"),
      pipelineRunId = pipelineRunId,
      usedAlignmentFileIds = usedFileBranches.alignment.sorted,
      usedSequenceFileIds = usedFileBranches.sequence.sorted,
      usedOtherFileIds = usedFileBranches.other.sorted,
      generatedAlignmentFileIds = generatedFileBranches.alignment.sorted,
      generatedSequenceFileIds = generatedFileBranches.sequence.sorted,
      generatedOtherFileIds = generatedFileBranches.other.sorted
    )
  }
}
