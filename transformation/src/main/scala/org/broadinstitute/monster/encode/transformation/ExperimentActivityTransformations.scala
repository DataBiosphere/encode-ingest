package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime, ZoneOffset}
import org.broadinstitute.monster.encode.jadeschema.table.Experimentactivity
import upack.Msg

object ExperimentActivityTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for experiments. */
  def transformExperiment(
    experimentId: String,
    rawExperiment: Msg,
    rawReplicates: Iterable[Msg],
    rawLibraries: Iterable[Msg]
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
      dateSubmitted = rawExperiment
        .tryRead[LocalDate]("date_submitted")
        .map(_.atStartOfDay().atOffset(ZoneOffset.UTC)),
      description = rawExperiment.tryRead[String]("description"),
      activityType = Some("Experiment"),
      dataModality =
        AssayActivityTransformations.getDataModalityFromTerm(rawExperiment, "assay_term_name"),
      award = CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("award")),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      lab = CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("lab")),
      submittedBy =
        CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("submitted_by")),
      status = rawExperiment.read[String]("status"),
      usedFileId = rawExperiment
        .tryRead[List[String]]("contributing_files")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      generatedFileId = rawExperiment
        .tryRead[List[String]]("files")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      usesSampleBiosampleId = libraryArray.map { lib =>
        CommonTransformations.transformId(lib.read[String]("biosample"))
      }.sorted.distinct,
      antibodyId = rawReplicates
        .flatMap(_.tryRead[String]("antibody").map(CommonTransformations.transformId))
        .toList,
      libraryId = libraryArray.map(CommonTransformations.readId).sorted
    )
  }
}
