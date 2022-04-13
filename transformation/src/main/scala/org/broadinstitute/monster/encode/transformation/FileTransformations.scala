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
  private def computeDataModality(rawFile: Msg, rawExperiment: Option[Msg]): Option[String] = {
    val dataModality =
      if (rawFile.tryRead[String]("output_category").contains("reference")) {
        Some("Genomic_Assembly")
      } else {
        rawExperiment.flatMap(e => e.tryRead[String]("assay_title")).collect {
          case "3' RACE"                 => "Transcriptomic"
          case "4C"                      => "Epigenomic_3D Contact Maps"
          case "5' RACE"                 => "Transcriptomic"
          case "5' RLM RACE"             => "Transcriptomic"
          case "5C"                      => "Epigenomic_3D Contact Maps"
          case "ATAC-seq"                => "Epigenomic_DNAChromatinAccessibility"
          case "Bru-seq"                 => "Transcriptomic_Unbiased"
          case "BruChase-seq"            => "Transcriptomic_Unbiased"
          case "BruUV-seq"               => "Transcriptomic_Unbiased"
          case "CAGE"                    => "Transcriptomic_Unbiased"
          case "ChIA-PET"                => "Epigenomic_3D Contact Maps"
          case "Circulome-seq"           => "Genomic"
          case "Clone-seq"               => "Proteomic"
          case "genotyping array"        => "Genomic_Genotyping_Targeted"
          case "Control ChIP-seq"        => "Epigenomic_DNABinding"
          case "Control eCLIP"           => "Epigenomic_RNABinding"
          case "CRISPR RNA-seq"          => "Transcriptomic_Unbiased"
          case "CRISPRi RNA-seq"         => "Transcriptomic_Unbiased"
          case "direct RNA-seq"          => "Transcriptomic_Unbiased"
          case "DNAme array"             => "Epigenomic_DNAMethylation"
          case "DNA-PET"                 => "Epigenomic_3D Contact Maps"
          case "DNase-seq"               => "Epigenomic_DNAChromatinAccessibility"
          case "FAIRE-seq"               => "Epigenomic_DNAChromatinAccessibility"
          case "GM DNase-seq"            => "Epigenomic_DNAChromatinAccessibility"
          case "genotype phasing by HiC" => "Genomic_Assembly"
          case "genotyping HTS"          => "Genomic_Genotyping_Whole Genomic"
          case "Hi-C"                    => "Epigenomic_3D Contact Maps"
          case "Histone ChIP-seq"        => "Epigenomic_DNABinding_HistoneModificationLocation"
          case "iCLIP"                   => "Epigenomic_RNABinding"
          case "icSHAPE"                 => "Epigenomic_RNABinding"
          case "long read RNA-seq"       => "Transcriptomic_Unbiased"
          case "MeDIP-seq"               => "Epigenomic_DNAMethylation"
          case "microRNA counts"         => "Transcriptomic_Unbiased"
          case "microRNA-seq"            => "Transcriptomic_Unbiased"
          case "MNase-seq"               => "Epigenomic_DNAChromatinAccessibility"
          case "MRE-seq"                 => "Epigenomic_DNAMethylation"
          case "PAS-seq"                 => "Transcriptomic_Unbiased"
          case "PLAC-seq"                => "Epigenomic_DNAChromatinAccessibility"
          case "polyA minus RNA-seq"     => "Transcriptomic_Unbiased"
          case "polyA plus RNA-seq"      => "Transcriptomic_Unbiased"
          case "PRO-cap"                 => "Transcriptomic"
          case "PRO-seq"                 => "Transcriptomic"
          case "MS-MS"                   => "Proteomic"
          case "RAMPAGE"                 => "Transcriptomic_Unbiased"
          case "Repli-chip"              => "Genomic"
          case "Repli-seq"               => "Genomic"
          case "RIP-chip"                => "Epigenomic_RNABinding"
          case "RIP-seq"                 => "Epigenomic_RNABinding"
          case "RNA Bind-n-Seq"          => "Epigenomic_RNABinding"
          case "RNA microarray"          => "Transcriptomic_Targeted"
          case "RNA-PET"                 => "Transcriptomic_Unbiased"
          case "RRBS"                    => "Epigenomic_DNAMethylation"
          case "shRNA RNA-seq"           => "Transcriptomic_Unbiased"
          case "scRNA-seq"               => "Transcriptomic_Unbiased"
          case "single-cell ATAC-seq"    => "Epigenomic_DNAChromatinAccessibility"
          case "snATAC-seq"              => "Epigenomic_DNAChromatinAccessibility"
          case "siRNA RNA-seq"           => "Transcriptomic_Unbiased"
          case "small RNA-seq"           => "Transcriptomic_Unbiased"
          case "Switchgear"              => "Epigenomic_RNABinding"
          case "TAB-seq"                 => "Epigenomic_DNAMethylation"
          case "TF ChIP-seq"             => "Epigenomic_DNABinding_TranscriptomeFactorLocation"
          case "total RNA-seq"           => "Transcriptomic_Unbiased"
          case "WGS"                     => "Genomic_Genotyping_Whole Genomic"
          case "WGBS"                    => "Epigenomic_DNAMethylation"
        }
      }
    if (dataModality.isEmpty) {
      logger.warn(
        s"No Data Modality found for assay_title in file $rawFile for experiment $rawExperiment"
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
      case None            => rawLibrary.tryRead[List[String]]("mixed_biosamples").getOrElse(List())
    }
  }

  def getBiosamples(rawFile: Msg): Option[List[String]] = {
    return rawFile.tryRead[List[String]]("origin_batches")
  }

  def getDonorIds(rawFile: Msg): Option[List[String]] = {
    return rawFile.tryRead[List[String]]("donors")
  }

  def computeLibrariesForFile(
    biosampleids: Option[List[String]],
    rawLibraries: Seq[Msg]
  ): Option[List[String]] = {
    return biosampleids match {
      case Some(biosampleList) => {
        Some(
          rawLibraries
            .filterNot(rawLibrary =>
              biosampleList.intersect(getBiosamplesFromLibrary(rawLibrary)).isEmpty
            )
            .map(filteredLibrary => CommonTransformations.readId(filteredLibrary))
            .toList
        )
      }
      case None => None
    }
  }

  /**
    * Transform a raw sequence file into our preferred schema.
    *
    * NOTE: This assumes that the input file has already been verified
    * to be a sequence file.
    */
  def transformSequenceFile(
    rawFile: Msg,
    rawExperiment: Option[Msg],
    rawLibraries: Seq[Msg]
  ): SequenceFile = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val modality = computeDataModality(rawFile, rawExperiment)
    val id = CommonTransformations.readId(rawFile)

    val biosampleids = getBiosamples(rawFile)

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
      crossReferences = rawFile.read[List[String]]("dbxrefs"),
      timeCreated = rawFile.read[OffsetDateTime]("date_created"),
      lab = rawFile.read[String]("lab"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawFile.read[String]("award"),
      fileFormat = rawFile.read[String]("file_format"),
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[List[String]]("quality_metrics"),
      submittedBy = rawFile.read[String]("submitted_by"),
//      libraryId = rawFile.tryRead[String]("library").map(CommonTransformations.transformId),
      biosampleIds = biosampleids.getOrElse(List[String]()),
      libraryIds = computeLibrariesForFile(biosampleids, rawLibraries).getOrElse(List()),
      donorIds = getDonorIds(rawFile).getOrElse(List()),
      readCount = rawFile.tryRead[Long]("read_count"),
      readLength = rawFile.tryRead[Long]("read_length"),
      pairedLibraryLayout = rawFile.tryRead[String]("run_type").map(_ == PairedEndType),
      pairedEndIdentifier = pairedEndId,
      pairedWithSequenceFileId =
        rawFile.tryRead[String]("paired_with").map(CommonTransformations.transformId)
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
    idsToType: Map[String, FileType],
    rawExperiment: Option[Msg],
    rawLibraries: Seq[Msg]
  ): AlignmentFile = {
    val id = CommonTransformations.readId(rawFile)
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val parentBranches = splitFileReferences(
      rawFile.tryRead[List[String]]("derived_from").getOrElse(Nil),
      idsToType
    )
    val modality = computeDataModality(rawFile, rawExperiment)

    val biosampleids = getBiosamples(rawFile)

    AlignmentFile(
      id = id,
      crossReferences = rawFile.read[List[String]]("dbxrefs"),
      timeCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawFile.read[String]("award"),
      fileFormat = rawFile.read[String]("file_format"),
      lab = rawFile.read[String]("lab"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[List[String]]("quality_metrics"),
      submittedBy = rawFile.read[String]("submitted_by"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      biosampleIds = biosampleids.getOrElse(List[String]()),
      libraryIds = computeLibrariesForFile(biosampleids, rawLibraries).getOrElse(List()),
      donorIds = getDonorIds(rawFile).getOrElse(List()),
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
    idsToType: Map[String, FileType],
    rawExperiment: Option[Msg],
    rawLibraries: Seq[Msg]
  ): OtherFile = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(rawFile)
    val parentBranches = splitFileReferences(
      rawFile.tryRead[List[String]]("derived_from").getOrElse(Nil),
      idsToType
    )
    val modality = computeDataModality(rawFile, rawExperiment)

    val biosampleids = getBiosamples(rawFile)

    OtherFile(
      id = CommonTransformations.readId(rawFile),
      crossReferences = rawFile.read[List[String]]("dbxrefs"),
      timeCreated = rawFile.read[OffsetDateTime]("date_created"),
      dataModality = modality,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = rawFile.read[String]("award"),
      fileFormat = rawFile.read[String]("file_format"),
      fileFormatType = rawFile.tryRead[String]("file_format_type"),
      lab = rawFile.read[String]("lab"),
      platform = rawFile.tryRead[String]("platform"),
      qualityMetrics = rawFile.read[List[String]]("quality_metrics"),
      submittedBy = rawFile.read[String]("submitted_by"),
      genomeAnnotation = rawFile.tryRead[String]("genome_annotation"),
      biosampleIds = biosampleids.getOrElse(List[String]()),
      libraryIds = computeLibrariesForFile(biosampleids, rawLibraries).getOrElse(List()),
      donorIds = getDonorIds(rawFile).getOrElse(List()),
      derivedFromAlignmentFileIds = parentBranches.alignment,
      derivedFromSequenceFileIds = parentBranches.sequence,
      derivedFromOtherFileIds = parentBranches.other,
      cloudPath = None
    )
  }
}
