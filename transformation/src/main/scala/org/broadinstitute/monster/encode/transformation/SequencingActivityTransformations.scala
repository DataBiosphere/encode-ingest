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
    val id = CommonTransformations.readId(rawFile)
    val generatedFileIds = rawGeneratedFiles.map(_.read[String]("@id")).toList

    logger.info("starting sequencing activity transform")

    Sequencingactivity(
      id = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: List(),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = rawFile
        .tryRead[String]("assay_term_name")
        .map(term => AssayActivityTransformations.transformAssayTermToDataModality(term)),
      referenceAssembly = rawFile.tryRead[String]("assembly"),
      dataset = rawFile.tryRead[String]("dataset"),
      generated = rawFile.tryRead[List[String]]("derived_from").getOrElse(List.empty[String]),
      tempGenerated = generatedFileIds.sorted,
      usesSample = rawFile.tryRead[List[String]]("origin_batches").getOrElse(List.empty[String]),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("lab")),
      usesLibrary = FileTransformations
        .computeLibrariesForFile(rawFile, rawLibraries)
        .getOrElse(List.empty[String]),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform"))
    )
  }
}
