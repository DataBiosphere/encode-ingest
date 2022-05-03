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
    rawGeneratedFiles: Iterable[Msg]
  ): StepRun = {

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
    val generatedFileArray = rawGeneratedFiles.toList
    val usedFileIds = generatedFileArray
      .flatMap(_.read[Array[String]]("derived_from"))
      .distinct

    StepRun(
      id = stepRunId,
      label = stepRunId,
      version = rawStepVersion.read[String]("name"),
      pipelineRunId = pipelineRunId,
      used = usedFileIds,
      generated = generatedFileArray.map(CommonTransformations.readId(_))
    )
  }
}
