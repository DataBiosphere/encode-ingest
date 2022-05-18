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
    rawGeneratedFiles: Iterable[Msg],
    rawLibraries: Seq[Msg]
  ): Sequencingactivity = {
    val fileId = CommonTransformations.readId(rawFile)
    val generatedFileIds = rawGeneratedFiles.map(CommonTransformations.readId(_)).toList
    val dataset = rawFile.tryRead[String]("dataset").map(CommonTransformations.transformId)
    val experimentId = dataset match {
      case None => generatedFileIds match {
        case Nil => "NONE"
        case _ => generatedFileIds.head
      }
      case Some(x) => x
    }
    val id = s"${fileId}_${experimentId}"
    logger.info("starting sequencing activity transform")

    Sequencingactivity(
      sequencingactivityId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: List(),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = rawFile
        .tryRead[List[String]]("assay_term_name")
        .getOrElse(List.empty[String])
        .map(term => AssayActivityTransformations.transformAssayTermToDataModality(term)),
      generatedFileId = fileId :: List(),
      associatedWith = generatedFileIds.sorted,
      usesSampleBiosampleId = rawFile
        .tryRead[List[String]]("origin_batches")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("lab")),
      libraryId = FileTransformations
        .computeLibrariesForFile(rawFile, rawLibraries)
        .getOrElse(List.empty[String]),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform"))
    )
  }
}
