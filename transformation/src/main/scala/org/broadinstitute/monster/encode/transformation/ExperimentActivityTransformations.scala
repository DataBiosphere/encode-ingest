package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.Experimentactivity
import upack.Msg

object ExperimentActivityTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for experiments. */
  def transformExperiment(
    rawExperiment: Msg,
    rawLibraries: Iterable[Msg] //,
//    fileIdToTypeMap: Map[String, FileType]
  ): Experimentactivity = {
    val id = CommonTransformations.readId(rawExperiment)
    val libraryArray = rawLibraries.toList

    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawExperiment)

    Experimentactivity(
      id = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        rawExperiment.read[String]("@id")
      ) :: rawExperiment.read[List[String]]("dbxrefs"),
      dateCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      description = rawExperiment.tryRead[String]("description"),
      dataModality = rawExperiment
        .tryRead[String]("assay_term_name")
        .map(term => AssayActivityTransformations.transformAssayTermToDataModality(term)),
      award = rawExperiment.read[String]("award"),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      lab = CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("lab")),
      submittedBy =
        CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("submitted_by")),
      status = rawExperiment.read[String]("status"),
      used = rawExperiment.read[List[String]]("contributing_files"),
      generated = rawExperiment.read[List[String]]("files"),
      usesSample = libraryArray.map { lib =>
        CommonTransformations.transformId(lib.read[String]("biosample"))
      }.sorted.distinct,
      antibody = libraryArray.flatMap {
        _.tryRead[Array[String]]("antibodies")
          .getOrElse(Array.empty)
          .map(CommonTransformations.transformId)
      }.sorted.distinct,
      library = libraryArray.map(CommonTransformations.readId).sorted
    )
  }
}
