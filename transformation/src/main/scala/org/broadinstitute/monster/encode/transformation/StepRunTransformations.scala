package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.StepRun
import upack.Msg

/** Transformation logic for ENCODE analysis step run objects. */
object StepRunTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE analysis step run into our preferred schema. */
  def transformStepRun(rawStepRun: Msg, rawStepVersion: Msg, rawStep: Msg): StepRun = {
    val pipelineIds = rawStep.read[Array[String]]("pipelines")

    StepRun(
      id = CommonTransformations.readId(rawStepRun),
      version = rawStepVersion.read[String]("name"),
      pipelineRunId = if (pipelineIds.length <= 1) pipelineIds.headOption else None,
      usedAlignmentFileIds = Array.empty,
      usedOtherFileIds = Array.empty,
      usedSequenceFileIds = Array.empty,
      generatedAlignmentFileIds = Array.empty,
      generatedOtherFileIds = Array.empty,
      generatedSequenceFileIds = Array.empty
    )
  }
}
