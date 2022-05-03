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
    rawFile: Msg,
//                                  fileIdToTypeMap: Map[String, FileType],
    rawGeneratedFiles: Iterable[Msg]
  ): Alignmentactivity = {
    val id = CommonTransformations.readId(rawFile)

    // branch files
    val generatedFileIds = rawGeneratedFiles.map(_.read[String]("@id")).toList
    val usedFileIds = rawGeneratedFiles
      .flatMap(_.read[Array[String]]("derived_from"))
      .toList
      .distinct
      .diff(generatedFileIds)

    Alignmentactivity(
      id = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: List(),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = rawFile
        .tryRead[String]("assay_term_name")
        .map(term => AssayActivityTransformations.transformAssayTermToDataModality(term)),
      referenceAssembly = rawFile.tryRead[String]("assembly"),
      generated = generatedFileIds.sorted,
      uses = rawFile.read[List[String]]("derived_from"),
      tempUses = usedFileIds.sorted,
      lab = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("lab"))
    )
  }

}