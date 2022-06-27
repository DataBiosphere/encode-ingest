package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.Stepactivity
import upack.Msg

/** Transformation logic for ENCODE analysis step run objects. */
object StepActivityTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE analysis step run into our preferred schema. */
  def transformStepActivity(
    rawStepRun: Msg,
    rawStepVersion: Msg,
    rawStep: Msg,
    rawExperiment: Option[Msg],
    rawGeneratedFiles: Iterable[Msg]
  ): Stepactivity = {

    // get pipeline run id
    val stepRunId = CommonTransformations.readId(rawStepRun)
    val pipelineRunId = for {
      idPair <- AnalysisActivityTransformations.getPipelineExperimentIdPair(
        rawStep,
        rawGeneratedFiles,
        stepRunId
      )
    } yield {
      AnalysisActivityTransformations.getPipelineRunId(idPair._1, idPair._2)
    }

    // branch files
    val generatedFileArray = rawGeneratedFiles.toSet.toList
    val usedFileIds = generatedFileArray
      .flatMap(_.read[Array[String]]("derived_from").map(CommonTransformations.transformId(_)))
      .distinct

    Stepactivity(
      stepactivityId = stepRunId,
      label = stepRunId,
      version = rawStepVersion.read[String]("name"),
      analysisactivityId = pipelineRunId,
      usedFileId = usedFileIds,
      generatedFileId = generatedFileArray.map(CommonTransformations.readId(_)),
      activityType = Some("step"),
      dataModality = rawExperiment
        .map(AssayActivityTransformations.getDataModalityFromTerm(_, "assay_term_name"))
        .getOrElse(List())
    )
  }
}