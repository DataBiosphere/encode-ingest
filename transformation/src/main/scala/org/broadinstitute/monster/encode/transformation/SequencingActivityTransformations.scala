package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.common.msg.MsgOps
import org.broadinstitute.monster.encode.jadeschema.table.Sequencingactivity
import org.slf4j.LoggerFactory
import upack.Msg

import java.time.OffsetDateTime

/** Transformation logic for ENCODE pipeline objects. */
object SequencingActivityTransformations {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE pipeline into our preferred schema. */
  def transformSequencingActivity(
    rawFile: Msg,
    rawLibraries: Seq[Msg]
  ): Sequencingactivity = {
    val fileId = CommonTransformations.readId(rawFile)
    val dataset =
      rawFile.tryRead[String]("dataset").map(CommonTransformations.transformId).getOrElse("")
    val id = s"${fileId}_${dataset}"
    logger.info("starting sequencing activity transform")

    Sequencingactivity(
      sequencingactivityId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: List(),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      activityType = Some("Sequencing"),
      dataModality =
        AssayActivityTransformations.getDataModalityFromListTerm(rawFile, "assay_term_name"),
      generatedFileId = fileId :: List(),
      associatedWith = dataset :: List(),
      usedBiosampleId = rawFile
        .tryRead[List[String]]("origin_batches")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("lab")),
      libraryId = FileTransformations
        .computeLibrariesForFile(rawFile, rawLibraries)
        .getOrElse(List.empty[String]),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform")),
      assayType = List()
    )
  }
}
