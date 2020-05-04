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
    // TODO determine actual pipeline runs: join experiment or get experiment id from file inputs
    val pipelineIds = rawStep.read[Array[String]]("pipelines")

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
      pipelineRunId = if (pipelineIds.length <= 1) pipelineIds.headOption else None,
      usedAlignmentFileIds = usedFileBranches.alignment.sorted,
      usedOtherFileIds = usedFileBranches.other.sorted,
      usedSequenceFileIds = usedFileBranches.sequence.sorted,
      generatedAlignmentFileIds = generatedFileBranches.alignment.sorted,
      generatedOtherFileIds = generatedFileBranches.other.sorted,
      generatedSequenceFileIds = generatedFileBranches.sequence.sorted
    ) // TODO re-order file fields to match assay order
  }
}
