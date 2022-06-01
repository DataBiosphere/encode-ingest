package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.Experimentactivity
import upack.Msg

object ExperimentActivityTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for experiments. */
  def transformExperiment(
    experimentId: String,
    rawExperiment: Msg,
    rawLibraries: Iterable[Msg] //,
//    fileIdToTypeMap: Map[String, FileType]
  ): Experimentactivity = {
    val id = CommonTransformations.transformId(experimentId)
    val libraryArray = rawLibraries.toList

    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawExperiment)

    Experimentactivity(
      experimentactivityId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(experimentId) :: rawExperiment
        .tryRead[List[String]]("dbxrefs")
        .getOrElse(List.empty[String]),
      dateCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      description = rawExperiment.tryRead[String]("description"),
      dataModality = rawExperiment
        .tryRead[String]("assay_term_name")
        .map(term => AssayActivityTransformations.transformAssayTermToDataModality(term))
        .toList,
      award = CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("award")),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      lab = CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("lab")),
      submittedBy =
        CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("submitted_by")),
      status = rawExperiment.read[String]("status"),
      usedFileId =
        rawExperiment.tryRead[List[String]]("contributing_files").getOrElse(List.empty[String]),
      generatedFileId = rawExperiment.tryRead[List[String]]("files").getOrElse(List.empty[String]),
      usesSampleBiosampleId = libraryArray.map { lib =>
        CommonTransformations.transformId(lib.read[String]("biosample"))
      }.sorted.distinct,
      antibodyId = libraryArray.flatMap {
        _.tryRead[Array[String]]("antibodies")
          .getOrElse(Array.empty)
          .map(CommonTransformations.transformId)
      }.sorted.distinct,
      libraryId = libraryArray.map(CommonTransformations.readId).sorted
    )
  }
}