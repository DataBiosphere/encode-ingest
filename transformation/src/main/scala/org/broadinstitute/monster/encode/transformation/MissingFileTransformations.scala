package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.File
import upack.Msg

import java.time.OffsetDateTime

object MissingFileTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Transform a raw file into our preferred schema.
    */

  def transformMissingFile(
    rawFile: Msg,
    missingType: String
  ): File = {
    val id = CommonTransformations.readId(rawFile)

    File(
      fileId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: rawFile
        .tryRead[List[String]]("dbxrefs")
        .getOrElse(List.empty[String]),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = List(),
      auditLabels = List(),
      maxAuditFlag = None,
      award = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("award")),
      fileFormat = rawFile.tryRead[String]("file_format"),
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      fileType = Some(missingType),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("lab")),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform")),
      qualityMetrics =
        rawFile.tryRead[List[String]]("quality_metrics").getOrElse(List.empty[String]),
      submittedBy = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("submitted_by")),
      readCount = rawFile.tryRead[Long]("read_count"),
      readLength = rawFile.tryRead[Long]("read_length"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      libraryId = List(),
      usesSampleBiosampleId = List(),
      donorId = List(),
      derivedFromFileId = rawFile
        .tryRead[List[String]]("derived_from")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      referenceAssembly = rawFile.tryRead[String]("assembly"),
      cloudPath = None,
      indexCloudPath = None,
      libraryLayout = None,
      pairedEndId = None,
      pairedWithFileId = None
    )
  }
}
