package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import com.spotify.scio.values.{SCollection, SideInput}
import org.broadinstitute.monster.encode.jadeschema.table.File
import org.slf4j.LoggerFactory
import upack.Msg

object FileTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /**
    * Generic container for containers-of-files, divided according to
    * our ENCODE file categories.
    *
    * @tparam F the type of the inner container
    * @tparam V the type representing files, stored within the inner containers
    * @param sequence container of files with category = sequence
    * @param alignment container of files with category = alignment
    * @param other container of files that don't fall into other categories
    */
  case class FileBranches[F[_], V](sequence: F[V], alignment: F[V], other: F[V])

  /** Output category for sequencing files. */
  private val SequencingCategory = "raw data"

  /** Output category for alignment files. */
  private val AlignmentCategory = "alignment"

  private val logger = LoggerFactory.getLogger(getClass)

  /** Divide a stream of raw files into branches, with one branch per file category. */
  def partitionRawFiles(
    rawStream: SCollection[(Msg, Option[Msg])]
  ): FileBranches[SCollection, (Msg, Option[Msg])] = {
    val Seq(sequence, alignment, other) = rawStream
      .withName("Split raw files by category")
      .partition(
        3,
        {
          case (rawFile, _) =>
            val category = rawFile.read[String]("output_category")
            if (category == SequencingCategory) 0
            else if (category == AlignmentCategory) 1
            else 2
        }
      )

    FileBranches(sequence, alignment, other)
  }

  /**
    * Construct a map from file ID -> category, for use as a side
    * input in downstream processing.
    */
  def buildIdTypeMap(
    branches: FileBranches[SCollection, (Msg, Option[Msg])]
  ): SideInput[Map[String, FileType]] = {
    def extractFileIdAndTagType(typ: FileType)(rawFile: (Msg, Option[Msg])): (String, FileType) =
      CommonTransformations.readId(rawFile._1) -> typ

    val taggedIds = branches.sequence.transform("Tag file IDs with category") { sequenceBranch =>
      val sequenceIds = sequenceBranch.map(extractFileIdAndTagType(FileType.Sequence))
      val alignmentIds =
        branches.alignment.map(extractFileIdAndTagType(FileType.Alignment))
      val otherIds = branches.other.map(extractFileIdAndTagType(FileType.Other))
      SCollection.unionAll(List(sequenceIds, alignmentIds, otherIds))
    }

    taggedIds.withName("Build ID->category map").asMapSideInput
  }

  /**
    * Split a set of raw links to ENCODE files, with one branch per file category.
    *
    * NOTE: Any  IDs not found within the input id -> category map will be discarded.
    * This will happen if a file derives from a restricted / archived file. We've been
    * told that it's better to remove the references than it is to have dangling
    * pointers to nonexistent rows.
    */
  def splitFileReferences(
    references: List[String],
    idsToType: Map[String, FileType]
  ): FileBranches[List, String] = {
    val (seq, align, other) = references.map { rawId =>
      val id = CommonTransformations.transformId(rawId)

      idsToType.get(id) match {
        case Some(FileType.Sequence)  => (Some(id), None, None)
        case Some(FileType.Alignment) => (None, Some(id), None)
        case Some(FileType.Other)     => (None, None, Some(id))
        case None =>
          logger.warn(s"No type found for file '$rawId'")
          (None, None, None)
      }
    }.unzip3

    FileBranches(seq.flatten, align.flatten, other.flatten)
  }

  /** Compute the data modality of a raw file. */
  private def computeDataModality(rawFile: Msg, rawExperiment: Option[Msg]): List[String] = {
    val dataModality =
      if (rawFile.tryRead[String]("output_category").contains("reference")) {
        Some("Genomic_Assembly")
      } else {
        rawExperiment.flatMap(e =>
          e.tryRead[String]("assay_title")
            .map(AssayActivityTransformations.transformAssayTermToDataModality(_))
        )
      }
    if (dataModality.isEmpty) {
      logger.warn(
        s"No Data Modality found for assay_title in file $rawFile for experiment $rawExperiment"
      )
    }
    dataModality.toList
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
    rawExperiment: Option[Msg],
    rawLibraries: Seq[Msg]
  ): File = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val modality = computeDataModality(rawFile, rawExperiment)
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
      id = id,
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
      lab = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("lab")),
      platform = CommonTransformations.convertToEncodeUrl(rawFile.tryRead[String]("platform")),
      qualityMetrics =
        rawFile.tryRead[List[String]]("quality_metrics").getOrElse(List.empty[String]),
      submittedBy = CommonTransformations.convertToEncodeUrl(rawFile.read[String]("submitted_by")),
      readCount = rawFile.tryRead[Long]("read_count"),
      readLength = rawFile.tryRead[Long]("read_length"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      library =
        computeLibrariesForBiosamples(biosample, rawLibraries).getOrElse(List.empty[String]),
      usesSample = biosample.getOrElse(List[String]()),
      donor = getDonorIds(rawFile).getOrElse(List.empty[String]),
      derivedFrom = rawFile.tryRead[List[String]]("derived_from").getOrElse(List.empty[String]),
      referenceAssembly = rawFile.tryRead[String]("assembly"),
      cloudPath = None,
      indexCloudPath = None,
      libraryLayout = rawFile.tryRead[String]("run_type").map(_ == PairedEndType),
      pairedEndId = pairedEndId,
      pairedFile = rawFile.tryRead[String]("paired_with").map(CommonTransformations.transformId)
    )
  }
}
