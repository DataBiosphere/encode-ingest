package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.common.msg.MsgOps
import org.broadinstitute.monster.encode.jadeschema.table.Sequencingactivity
import upack.Msg

import java.time.OffsetDateTime

/** Transformation logic for ENCODE pipeline objects. */
object SequencingActivityTransformations {

//  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE pipeline into our preferred schema. */
  def transformSequencingActivity(
    rawFile: Msg,
//    fileIdToTypeMap: Map[String, FileType],
    rawGeneratedFiles: Iterable[Msg],
    rawLibraries: Seq[Msg]
  ): Sequencingactivity = {
    val id = CommonTransformations.readId(rawFile)
    // branch files
    val generatedFileIds = rawGeneratedFiles.map(_.read[String]("@id")).toList
//    val usedFileIds = rawGeneratedFiles
//      .flatMap(_.read[Array[String]]("derived_from"))
//      .toList
//      .distinct
//      .diff(generatedFileIds)

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
      generated = rawFile.read[List[String]]("derived_from"),
      tempGenerated = generatedFileIds.sorted,
      usesSample = rawFile.read[List[String]]("origin_batches"),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("lab")),
      usesLibrary =
        FileTransformations.computeLibrariesForFile(rawFile, rawLibraries).getOrElse(List()),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform"))
    )
  }
}
