package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import com.spotify.scio.values.{SCollection, SideInput}
import enumeratum.{Enum, EnumEntry}
import org.broadinstitute.monster.encode.jadeschema.table.{
  AlignmentFile,
  OtherFile,
  SequenceFile
}
import org.slf4j.LoggerFactory
import upack.Msg

object FileTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Category of file stored by ENCODE. */
  sealed trait FileType extends EnumEntry with Product with Serializable

  private object FileType extends Enum[FileType] {
    override val values = findValues

    case object Sequence extends FileType
    case object Alignment extends FileType
    case object Other extends FileType
  }

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
    val Seq(sequence, alignment, other) =
      rawStream.partition(
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

    val sequenceIds = branches.sequence.map(extractFileIdAndTagType(FileType.Sequence))
    val alignmentIds = branches.alignment.map(extractFileIdAndTagType(FileType.Alignment))
    val otherIds = branches.other.map(extractFileIdAndTagType(FileType.Other))

    SCollection.unionAll(List(sequenceIds, alignmentIds, otherIds)).asMapSideInput
  }

  /**
    * Split the derives_from links in a raw file into branches, with one branch
    * per file category.
    *
    * NOTE: Any parent IDs not found within the input id -> category map will be
    * discarded. We don't expect this to ever happen outside of testing, but if
    * it does it feels safer to omit info than it does to expose incorrect info.
    */
  private def splitParentReferences(
    rawFile: Msg,
    idsToType: Map[String, FileType]
  ): FileBranches[Array, String] = {
    val baseId = CommonTransformations.readId(rawFile)

    val (seq, align, other) = rawFile
      .tryRead[Array[String]]("derived_from")
      .getOrElse(Array.empty)
      .map(CommonTransformations.transformId)
      .map { id =>
        idsToType.get(id) match {
          case Some(FileType.Alignment) => (Some(id), None, None)
          case Some(FileType.Sequence)  => (None, Some(id), None)
          case Some(FileType.Other)     => (None, None, Some(id))
          case None =>
            logger.warn(
              s"File '$id' referenced in derived_from of file '$baseId' not found in type map"
            )
            (None, None, None)
        }
      }
      .unzip3

    FileBranches(seq.flatten, align.flatten, other.flatten)
  }

  /** Compute the data modality of a raw file. */
  private def computeDataModality(rawFile: Msg): String = {
    rawFile match {
      // FIXME: Actually compute something here once we know the formula.
      case _ => "I'm a modality!"
    }
  }

  /**
    * Transform a raw sequence file into our preferred schema.
    *
    * NOTE: This assumes that the input file has already been verified
    * to be a sequence file.
    */
  def transformSequenceFile(rawFile: Msg): SequenceFile = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val modality = computeDataModality(rawFile)

    SequenceFile(
      id = CommonTransformations.readId(rawFile),
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
      restrictionEnzymes = rawFile
        .tryRead[Array[String]]("restriction_enzymes")
        .getOrElse(Array.empty),
      submittedBy = rawFile.read[String]("submitted_by"),
      libraryId =
        rawFile.tryRead[String]("library").map(CommonTransformations.transformId),
      libraryLayout = rawFile.tryRead[String]("run_type"),
      readCount = rawFile.tryRead[Long]("read_count"),
      readLength = rawFile.tryRead[Long]("read_length"),
      pairedEndIdentifier = rawFile.tryRead[Long]("paired_end"),
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
    val parentBranches = splitParentReferences(rawFile, idsToType)
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
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      lab = rawFile.read[String]("lab"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[Array[String]]("quality_metrics"),
      restrictionEnzymes = rawFile
        .tryRead[Array[String]]("restriction_enzymes")
        .getOrElse(Array.empty),
      submittedBy = rawFile.read[String]("submitted_by"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      derivedFromAlignmentFileIds = parentBranches.alignment,
      derivedFromSequenceFileIds = parentBranches.sequence,
      derivedFromOtherFileIds = parentBranches.other,
      mappedReadLength = rawFile.tryRead[Long]("mapped_read_length"),
      runType = rawFile.tryRead[String]("mapped_run_type"),
      readStructure = rawFile
        .tryRead[Array[String]]("read_structure")
        .getOrElse(Array.empty),
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
    val parentBranches = splitParentReferences(rawFile, idsToType)
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
      restrictionEnzymes = rawFile
        .tryRead[Array[String]]("restriction_enzymes")
        .getOrElse(Array.empty),
      submittedBy = rawFile.read[String]("submitted_by"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      derivedFromAlignmentFileIds = parentBranches.alignment,
      derivedFromSequenceFileIds = parentBranches.sequence,
      derivedFromOtherFileIds = parentBranches.other,
      cloudPath = None
    )
  }
}
