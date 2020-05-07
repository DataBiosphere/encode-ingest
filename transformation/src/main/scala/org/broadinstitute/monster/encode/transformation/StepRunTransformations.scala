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

    // get pipeline run id
    val stepRunId = CommonTransformations.readId(rawStepRun)
    val pipelineRunId = for {
      idPair <- PipelineRunTransformations.getPipelineExperimentIdPair(
        rawStep,
        rawGeneratedFiles,
        stepRunId
      )
    } yield {
      PipelineRunTransformations.getPipelineRunId(idPair._1, idPair._2)
    }

    // branch files
    val generatedFileArray = rawGeneratedFiles.toArray
    val generatedFileBranches = FileTransformations.splitFileReferences(
      generatedFileArray.map(_.read[String]("@id")),
      fileIdToTypeMap
    )
    val usedFileIds = generatedFileArray
      .flatMap(_.read[Array[String]]("derived_from"))
      .distinct
    val usedFileBranches = FileTransformations.splitFileReferences(usedFileIds, fileIdToTypeMap)

    StepRun(
      id = stepRunId,
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
