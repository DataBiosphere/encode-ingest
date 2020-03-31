package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import com.spotify.scio.values.{SCollection, SideInput}
import org.broadinstitute.monster.encode.jadeschema.table.{AlignmentFile, OtherFile, SequenceFile}
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
  def partitionRawFiles(rawStream: SCollection[Msg]): FileBranches[SCollection, Msg] = {
    val Seq(sequence, alignment, other) = rawStream
      .withName("Split raw files by category")
      .partition(
        3,
        rawFile => {
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
    branches: FileBranches[SCollection, Msg]
  ): SideInput[Map[String, FileType]] = {
    def extractFileIdAndTagType(typ: FileType)(rawFile: Msg): (String, FileType) =
      CommonTransformations.readId(rawFile) -> typ

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
    references: Array[String],
    idsToType: Map[String, FileType]
  ): FileBranches[Array, String] = {
    val (seq, align, other) = references.map { rawId =>
      val id = CommonTransformations.transformId(rawId)

      idsToType.get(id) match {
        case Some(FileType.Alignment) => (Some(id), None, None)
        case Some(FileType.Sequence)  => (None, Some(id), None)
        case Some(FileType.Other)     => (None, None, Some(id))
        case None =>
          logger.warn(s"No type found for file '$rawId'")
          (None, None, None)
      }
    }.unzip3

    FileBranches(seq.flatten, align.flatten, other.flatten)
  }

  /** Compute the data modality of a raw file. */
  private def computeDataModality(rawFile: Msg): String =
    rawFile match {
      // FIXME: Actually compute something here once we know the formula.
      case _ => "I'm a modality!"
    }

  /** 'run_type' value indicating a paired run in ENCODE. */
  private val PairedEndType = "paired-ended"

  /**
    * Transform a raw sequence file into our preferred schema.
    *
    * NOTE: This assumes that the input file has already been verified
    * to be a sequence file.
    */
  def transformSequenceFile(rawFile: Msg): SequenceFile = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val modality = computeDataModality(rawFile)
    val id = CommonTransformations.readId(rawFile)

    val pairedEndId = rawFile.tryRead[String]("paired_end") match {
      case None        => None
      case Some("1")   => Some(1L)
      case Some("2")   => Some(2L)
      case Some("1,2") => None
      case Some(other) =>
        logger.warn(s"Encountered unknown run_type in file $id: '$other''")
        None
    }

    SequenceFile(
      id = id,
      crossReferences = rawFile.read[Array[String]]("dbxrefs"),
      timeCreated = rawFile.read[OffsetDateTime]("date_created"),
      lab = rawFile.read[String]("lab"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawFile.read[String]("award"),
      fileFormat = rawFile.read[String]("file_format"),
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[Array[String]]("quality_metrics"),
      submittedBy = rawFile.read[String]("submitted_by"),
      libraryId = rawFile.tryRead[String]("library").map(CommonTransformations.transformId),
      readCount = rawFile.tryRead[Long]("read_count"),
      readLength = rawFile.tryRead[Long]("read_length"),
      pairedLibraryLayout = rawFile.tryRead[String]("run_type").map(_ == PairedEndType),
      pairedEndIdentifier = pairedEndId,
      pairedWithSequenceFileId =
        rawFile.tryRead[String]("paired_with").map(CommonTransformations.transformId),
      cloudPath = None
    )
  }

  /**
    * Transform a raw alignment file into our preferred schema.
    *
    * NOTE: This assumes that the input file has already been verified
    * to be an alignment file.
    */
  def transformAlignmentFile(
    rawFile: Msg,
    idsToType: Map[String, FileType]
  ): AlignmentFile = {
    val id = CommonTransformations.readId(rawFile)
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val parentBranches =
      splitFileReferences(rawFile.read[Array[String]]("derived_from"), idsToType)
    val modality = computeDataModality(rawFile)

    AlignmentFile(
      id = id,
      crossReferences = rawFile.read[Array[String]]("dbxrefs"),
      timeCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawFile.read[String]("award"),
      fileFormat = rawFile.read[String]("file_format"),
      lab = rawFile.read[String]("lab"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[Array[String]]("quality_metrics"),
      submittedBy = rawFile.read[String]("submitted_by"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      derivedFromAlignmentFileIds = parentBranches.alignment,
      derivedFromSequenceFileIds = parentBranches.sequence,
      derivedFromOtherFileIds = parentBranches.other,
      referenceAssembly = rawFile.read[String]("assembly"),
      cloudPath = None,
      indexCloudPath = None
    )
  }

  /** Transform a raw file into our preferred schema for generic files. */
  def transformOtherFile(
    rawFile: Msg,
    idsToType: Map[String, FileType]
  ): OtherFile = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val parentBranches =
      splitFileReferences(rawFile.read[Array[String]]("derived_from"), idsToType)
    val modality = computeDataModality(rawFile)

    OtherFile(
      id = CommonTransformations.readId(rawFile),
      crossReferences = rawFile.read[Array[String]]("dbxrefs"),
      timeCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawFile.read[String]("award"),
      fileFormat = rawFile.read[String]("file_format"),
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      lab = rawFile.read[String]("lab"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[Array[String]]("quality_metrics"),
      submittedBy = rawFile.read[String]("submitted_by"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      derivedFromAlignmentFileIds = parentBranches.alignment,
      derivedFromSequenceFileIds = parentBranches.sequence,
      derivedFromOtherFileIds = parentBranches.other,
      cloudPath = None
    )
  }
}
