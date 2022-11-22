package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.Analysisactivity
import org.slf4j.LoggerFactory
import upack.Msg
import org.broadinstitute.monster.common.msg.MsgOps

/** Transformation logic for ENCODE pipeline objects. */
object AnalysisActivityTransformations {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE pipeline into our preferred schema. */
  def transformAnalysisActivity(
    rawPipeline: Msg,
    experimentId: String,
    rawGeneratedFiles: Iterable[Msg]
  ): Analysisactivity = {
    val pipelineId = CommonTransformations.readId(rawPipeline)
    val experimentIdAccession = CommonTransformations.transformId(experimentId)
    val pipelineRunId = s"${pipelineId}_${experimentIdAccession}"

    // branch files
    val generatedFileIds = rawGeneratedFiles.map(CommonTransformations.readId).toSet.toList
    val usedFileIds = rawGeneratedFiles
      .flatMap(
        _.tryRead[List[String]]("derived_from")
          .getOrElse(List.empty[String])
          .distinct
          .map(CommonTransformations.transformId)
          .diff(generatedFileIds)
          .sorted
      )
      .toSet
      .toList

    Analysisactivity(
      analysisactivityId = pipelineRunId,
      label = pipelineRunId,
      xref = CommonTransformations.convertToEncodeUrl(rawPipeline.read[String]("@id")) :: List(),
      activityType = Some("Analysis"),
      dataModality = rawPipeline
        .tryRead[List[String]]("assay_term_names")
        .getOrElse(List.empty[String])
        .map(term => AssayActivityTransformations.transformAssayTermToDataModality(term)),
      analysisType = rawPipeline.tryRead[String]("title"),
      assayactivityId = experimentIdAccession,
      usedFileId = usedFileIds,
      generatedFileId = generatedFileIds.sorted
    )
  }

  /**
    * Get the pipeline ID associated with a step.
    * Return None and log a warning if there is an unexpected number of IDs.
    */
  def getPipelineId(step: Msg): Option[String] = {
    val pipelineIds = step.read[Array[String]]("pipelines")
    if (pipelineIds.toSet.size == 1) {
      Some(pipelineIds.head)
    } else {
      val stepId = CommonTransformations.readId(step)
      logger.warn(
        s"Step $stepId does not have exactly one pipeline: [${pipelineIds.mkString(",")}]"
      )
      None
    }
  }

  /**
    * Get the experiment ID associated with a set of files.
    * Return None and log a warning if there is an unexpected number of IDs.
    */
  def getExperimentId(files: Iterable[Msg], stepRunId: String): Option[String] = {
    val experimentIds = files.toArray.map(_.read[String]("dataset"))
    if (experimentIds.toSet.size == 1) {
      Some(experimentIds.head)
    } else {
      logger.warn(
        s"Step run $stepRunId does not have exactly one experiment: [${experimentIds.mkString(",")}]"
      )
      None
    }
  }

  /**
    * Get the pipeline ID and experiment ID associated with a set of files.
    * Return None if there is an unexpected number of IDs for either the experiment or pipeline.
    */
  def getPipelineExperimentIdPair(
    step: Msg,
    files: Iterable[Msg],
    stepRunId: String
  ): Option[(String, String)] =
    for {
      pipelineId <- getPipelineId(step)
      experimentId <- getExperimentId(files, stepRunId)
    } yield {
      (pipelineId, experimentId)
    }

  /** Combine a pipeline ID and experiment ID to generate an ID for a pipeline run. */
  def getPipelineRunId(pipelineId: String, experimentId: String): String =
    s"${CommonTransformations.transformId(pipelineId)}_${CommonTransformations.transformId(experimentId)}"
}
