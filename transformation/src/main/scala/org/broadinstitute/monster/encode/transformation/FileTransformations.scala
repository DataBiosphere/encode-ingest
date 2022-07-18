package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.File
import org.slf4j.LoggerFactory
import upack.Msg

object FileTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Output category for sequencing files. */
  private val SequencingCategory = "raw data"

  /** Output category for alignment files. */
  private val AlignmentCategory = "alignment"

  private val logger = LoggerFactory.getLogger(getClass)

  def getFileType(rawFile: Msg): String = {
    val category = rawFile.read[String]("output_category")
    if (category == SequencingCategory) "Sequence"
    else if (category == AlignmentCategory) "Alignment"
    else "Other"
  }

  /** Compute the data modality of a raw file. */
  private def computeDataModality(rawFile: Msg): List[String] = {
    val dataModality: List[String] =
      if (rawFile.tryRead[String]("output_category").contains("reference")) {
        List("Genomic_Assembly")
      } else {
        rawFile
          .tryRead[List[String]]("assay_term_name")
          .getOrElse(List())
          .map(AssayActivityTransformations.transformAssayTermToDataModality(_))
      }
    if (dataModality.isEmpty) {
      logger.warn(
        s"No Data Modality found for assay_term_name in file $rawFile"
      )
    }
    dataModality
  }

  /** 'run_type' value indicating a paired run in ENCODE. */
  private val PairedEndType = "paired-ended"

  // NOTE: This will not work if Library has biosamples in both the biosample field and mixed_biosamples field
  def getBiosamplesFromLibrary(rawLibrary: Msg): List[String] = {
    rawLibrary.tryRead[String]("biosample") match {
      case Some(biosample) => List(biosample)
      case None =>
        rawLibrary.tryRead[List[String]]("mixed_biosamples").getOrElse(List.empty[String])
    }
  }

  def getBiosamples(rawFile: Msg): Option[List[String]] = {
    rawFile.tryRead[List[String]]("origin_batches")
  }

  def getDonorIds(rawFile: Msg): Option[List[String]] = {
    return rawFile.tryRead[List[String]]("donors")
  }

  def computeLibrariesForFile(
    rawFile: Msg,
    rawLibraries: Seq[Msg]
  ): Option[List[String]] = {
    computeLibrariesForBiosamples(getBiosamples(rawFile), rawLibraries)
  }

  def computeLibrariesForBiosamples(
    biosample: Option[List[String]],
    rawLibraries: Seq[Msg]
  ): Option[List[String]] = {
    biosample.map(biosampleList =>
      rawLibraries
        .filterNot(rawLibrary =>
          biosampleList.intersect(getBiosamplesFromLibrary(rawLibrary)).isEmpty
        )
        .map(filteredLibrary => CommonTransformations.readId(filteredLibrary))
        .toList
    )
  }

  /**
    * Transform a raw file into our preferred schema.
    */

  def transformFile(
    rawFile: Msg,
    rawLibraries: Seq[Msg]
  ): File = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val modality = computeDataModality(rawFile)
    val id = CommonTransformations.readId(rawFile)

    val biosample = getBiosamples(rawFile)

    val pairedEndId = rawFile.tryRead[String]("paired_end") match {
      case None        => None
      case Some("1")   => Some(1L)
      case Some("2")   => Some(2L)
      case Some("1,2") => None
      case Some(other) =>
        logger.warn(s"Encountered unknown run_type in file $id: '$other''")
        None
    }

    File(
      fileId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("@id")) :: rawFile
        .tryRead[List[String]]("dbxrefs")
        .getOrElse(List.empty[String]),
      dateCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("award")),
      fileFormat = rawFile.tryRead[String]("file_format"),
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      fileType = Some(getFileType(rawFile)),
      lab = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("lab")),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform")),
      qualityMetrics =
        rawFile.tryRead[List[String]]("quality_metrics").getOrElse(List.empty[String]),
      submittedBy = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("submitted_by")),
      readCount = rawFile.tryRead[Long]("read_count"),
      readLength = rawFile.tryRead[Long]("read_length"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      libraryId = computeLibrariesForBiosamples(biosample, rawLibraries)
        .getOrElse(List.empty[String]),
      usesSampleBiosampleId =
        biosample.getOrElse(List[String]()).map(CommonTransformations.transformId(_)),
      donorId =
        getDonorIds(rawFile).getOrElse(List.empty[String]).map(CommonTransformations.transformId(_)),
      derivedFromFileId = rawFile
        .tryRead[List[String]]("derived_from")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId(_)),
      referenceAssembly = rawFile.tryRead[String]("assembly"),
      cloudPath = None,
      indexCloudPath = None,
      libraryLayout = rawFile.tryRead[String]("run_type").map(_ == PairedEndType),
      pairedEndId = pairedEndId,
      pairedWithFileId =
        rawFile.tryRead[String]("paired_with").map(CommonTransformations.transformId)
    )
  }
}
