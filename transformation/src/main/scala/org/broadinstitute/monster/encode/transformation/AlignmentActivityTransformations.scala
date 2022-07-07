package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.common.msg.MsgOps
import org.broadinstitute.monster.encode.jadeschema.table.Alignmentactivity
import upack.Msg

import java.time.OffsetDateTime

/** Transformation logic for ENCODE Alignment files to Alignment activities. */
object AlignmentActivityTransformations {

//  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE pipeline into our preferred schema. */
  def transformAlignmentActivity(
    rawFile: Msg
  ): Alignmentactivity = {
    val fileId = CommonTransformations.readId(rawFile)
    val dataset = CommonTransformations.transformId(rawFile.read[String]("dataset"))
    val id = s"${fileId}_${dataset}"

    Alignmentactivity(
      alignmentactivityId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: List(),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      activityType = Some("alignment"),
      dataModality =
        AssayActivityTransformations.getDataModalityFromListTerm(rawFile, "assay_term_name"),
      generatedFileId = fileId :: List(),
      referenceAssembly = rawFile.tryRead[String]("assembly"),
      usedFileId = rawFile
        .tryRead[List[String]]("derived_from")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("lab"))
    )
  }

}
