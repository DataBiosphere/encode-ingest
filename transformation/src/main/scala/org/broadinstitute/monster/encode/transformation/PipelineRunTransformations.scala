package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.PipelineRun
import org.slf4j.LoggerFactory
import upack.Msg
import org.broadinstitute.monster.common.msg.MsgOps

/** Transformation logic for ENCODE pipeline objects. */
object PipelineRunTransformations {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE pipeline into our preferred schema. */
  def transformPipelineRun(rawPipeline: Msg, experimentId: String): PipelineRun = {
    val pipelineId = rawPipeline.read[String]("@id")
    val pipelineRunId = buildPipelineRunId(pipelineId, experimentId)

    PipelineRun(
      id = pipelineRunId,
      pipelineName = rawPipeline.read[String]("title")
    )
  }

  def getPipelineId(step: Msg, stepRunId: String): Option[String] = {
    val pipelineIds = step.read[Array[String]]("pipelines")
    if (pipelineIds.toSet.size == 1) {
      Some(CommonTransformations.transformId(pipelineIds.head))
    } else {
      logger.warn(
        s"Step run $stepRunId does not have exactly one pipeline: [${pipelineIds.mkString(",")}]"
      )
      None
    }
  }

  def getExperimentId(files: Iterable[Msg], stepRunId: String): Option[String] = {
    val experimentIds = files.toArray.map(_.read[String]("dataset"))
    if (experimentIds.toSet.size == 1) {
      Some(CommonTransformations.transformId(experimentIds.head))
    } else {
      logger.warn(
        s"Step run $stepRunId does not have exactly one experiment: [${experimentIds.mkString(",")}]"
      )
      None
    }
  }

  // TODO add javadoc to all 4 methods
  // TODO see if this method is actually used
  def transformPipelineRunId(
    pipelineId: Option[String],
    experimentId: Option[String]
  ): Option[String] =
    if (pipelineId.isEmpty || experimentId.isEmpty) None
    else Some(buildPipelineRunId(pipelineId.head, experimentId.head))

  def buildPipelineRunId(pipelineId: String, experimentId: String): String =
    s"$pipelineId-$experimentId"
}
