package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime
import upack.Msg
import org.broadinstitute.monster.encode.jadeschema.table.Assayactivity

object AssayActivityTransformations {

  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for assays. */
  def transformAssayActivity(
    rawExperiment: Msg
  ): Assayactivity = {
    val id = CommonTransformations.readId(rawExperiment)

    Assayactivity(
      id = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(rawExperiment.read[String]("@id")) ::
        rawExperiment.read[List[String]]("dbxrefs"),
      dateCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      assayCategory = rawExperiment.read[List[String]]("assay_slims").headOption,
      assayType = rawExperiment.read[String]("assay_term_id"),
      dataModality = transformAssayTermToDataModality(rawExperiment.read[String]("assay_term_name"))
    )
  }

  /**
    * This code will work for assay_term_name and assay_term_title
    */
  def transformAssayTermToDataModality(assayTerm: String): String = {
    assayTerm match {
      case "3' RACE"                                              => "Transcriptomic"
      case "4C"                                                   => "Epigenomic_3D Contact Maps"
      case "5' RACE"                                              => "Transcriptomic"
      case "5' RLM RACE"                                          => "Transcriptomic"
      case "5C"                                                   => "Epigenomic_3D Contact Maps"
      case "ATAC-seq"                                             => "Epigenomic_DNAChromatinAccessibility"
      case "Bru-seq"                                              => "Transcriptomic_NonTargeted"
      case "BruChase-seq"                                         => "Transcriptomic_NonTargeted"
      case "BruUV-seq"                                            => "Transcriptomic_NonTargeted"
      case "CAGE"                                                 => "Transcriptomic_NonTargeted"
      case "capture Hi-C"                                         => "Epigenomic_3D Contact Maps"
      case "ChIA-PET"                                             => "Epigenomic_3D Contact Maps"
      case "ChIP-seq"                                             => "Epigenomic_DNABinding"
      case "Circulome-seq"                                        => "Genomic"
      case "Clone-seq"                                            => "Proteomic"
      case "comparative genomic hybridization by array"           => "Genomic_Genotyping"
      case "Control ChIP-seq"                                     => "Epigenomic_DNABinding"
      case "Control eCLIP"                                        => "Epigenomic_RNABinding"
      case "CRISPR RNA-seq"                                       => "Transcriptomic_NonTargeted"
      case "CRISPR genome editing followed by RNA-seq"            => "Transcriptomic_NonTargeted"
      case "CRISPRi RNA-seq"                                      => "Transcriptomic_NonTargeted"
      case "CRISPRi followed by RNA-seq"                          => "Transcriptomic_NonTargeted"
      case "CUT&RUN"                                              => "Epigenomic_DNABinding"
      case "CUT&Tag"                                              => "Epigenomic_DNABinding"
      case "direct RNA-seq"                                       => "Transcriptomic_NonTargeted"
      case "DNAme array"                                          => "Epigenomic_DNAMethylation"
      case "DNA methylation profiling by array assay"             => "Epigenomic_DNAMethylation"
      case "DNA-PET"                                              => "Genomic_Genotyping"
      case "DNase-seq"                                            => "Epigenomic_DNAChromatinAccessibility"
      case "eCLIP"                                                => "Epigenomic_RNABinding"
      case "FACS CRISPR screen"                                   => "!FACS CRISPR screen"
      case "FAIRE-seq"                                            => "Epigenomic_DNAChromatinAccessibility"
      case "Flow-FISH CRISPR screen"                              => "!Flow-FISH CRISPR screen"
      case "GM DNase-seq"                                         => "Epigenomic_DNAChromatinAccessibility"
      case "genetic modification followed by DNase-seq"           => "Epigenomic_DNAChromatinAccessibility"
      case "genotype phasing by HiC"                              => "Genomic_Assembly"
      case "GRO-cap"                                              => "Transcriptomic_NonTargeted"
      case "GRO-seq"                                              => "Transcriptomic_NonTargeted"
      case "genotyping array"                                     => "Genomic_Genotyping"
      case "genotyping HTS"                                       => "Genomic_Genotyping_Whole Genomic"
      case "Hi-C"                                                 => "Epigenomic_3D Contact Maps"
      case "HiC"                                                  => "Epigenomic_3D Contact Maps"
      case "Histone ChIP-seq"                                     => "Epigenomic_DNABinding"
      case "iCLIP"                                                => "Epigenomic_RNABinding"
      case "icLASER"                                              => "Epigenomic_RNAStructure"
      case "icSHAPE"                                              => "Epigenomic_RNAStructure"
      case "LC/MS label-free quantitative proteomics"             => "Proteomic"
      case "LC-MS/MS isobaric label quantitative proteomics"      => "Proteomic"
      case "long read RNA-seq"                                    => "Transcriptomic_NonTargeted"
      case "long read single-cell RNA-seq"                        => "Transcriptomic_NonTargeted"
      case "MeDIP-seq"                                            => "Epigenomic_DNAMethylation"
      case "microRNA counts"                                      => "Transcriptomic_NonTargeted"
      case "microRNA-seq"                                         => "Transcriptomic_NonTargeted"
      case "Mint-ChIP-seq"                                        => "Epigenomic_DNABinding"
      case "MNase-seq"                                            => "Epigenomic_DNAChromatinAccessibility"
      case "MPRA"                                                 => "Massively parallel reporter assay"
      case "MRE-seq"                                              => "Epigenomic_DNAMethylation"
      case "PAS-seq"                                              => "Transcriptomic_NonTargeted"
      case "perturbation followed by scRNA-seq"                   => "!perturbation followed by scRNA-seq"
      case "perturbation followed by snATAC-seq"                  => "!perturbation followed by snATAC-seq"
      case "PLAC-seq"                                             => "Epigenomic_DNAChromatinAccessibility"
      case "pooled clone sequencing"                              => "Library Preparation"
      case "polyA minus RNA-seq"                                  => "Transcriptomic_NonTargeted"
      case "polyA plus RNA-seq"                                   => "Transcriptomic_NonTargeted"
      case "PRO-cap"                                              => "Transcriptomic"
      case "PRO-seq"                                              => "Transcriptomic"
      case "proliferation CRISPR screen"                          => "!proliferation CRISPR screen"
      case "MS-MS"                                                => "Proteomic"
      case "protein sequencing by tandem mass spectrometry assay" => "Proteomic"
      case "RAMPAGE"                                              => "Transcriptomic_NonTargeted"
      case "Repli-chip"                                           => "Genomic"
      case "Repli-seq"                                            => "Genomic"
      case "Ribo-seq"                                             => "Proteomic"
      case "RIP-chip"                                             => "Epigenomic_RNABinding"
      case "RIP-seq"                                              => "Epigenomic_RNABinding"
      case "RNA Bind-n-Seq"                                       => "Epigenomic_RNABinding"
      case "RNA microarray"                                       => "Transcriptomic_Targeted"
      case "RNA-PET"                                              => "Transcriptomic_NonTargeted"
      case "RNA-seq"                                              => "Transcriptomic_NonTargeted"
      case "RRBS"                                                 => "Epigenomic_DNAMethylation"
      case "shRNA RNA-seq"                                        => "Transcriptomic_NonTargeted"
      case "shRNA knockdown followed by RNA-seq"                  => "Transcriptomic_NonTargeted"
      case "scRNA-seq"                                            => "Transcriptomic_NonTargeted"
      case "single-cell RNA sequencing assay"                     => "Transcriptomic_NonTargeted"
      case "single-cell ATAC-seq"                                 => "Epigenomic_DNAChromatinAccessibility"
      case "snATAC-seq"                                           => "Epigenomic_DNAChromatinAccessibility"
      case "single-nucleus ATAC-seq"                              => "Epigenomic_DNAChromatinAccessibility"
      case "siRNA RNA-seq"                                        => "Transcriptomic_NonTargeted"
      case "siRNA knockdown followed by RNA-seq"                  => "Transcriptomic_NonTargeted"
      case "small RNA-seq"                                        => "Transcriptomic_NonTargeted"
      case "SPRITE"                                               => "Epigenomic_3D Contact Maps"
      case "SPRITE-IP"                                            => "Epigenomic_3D Contact Maps"
      case "STARR-seq"                                            => "Massively parallel reporter assay"
      case "Switchgear"                                           => "Epigenomic_RNABinding"
      case "TAB-seq"                                              => "Epigenomic_DNAMethylation"
      case "TF ChIP-seq"                                          => "Epigenomic_DNABinding_TranscriptomeFactorLocation"
      case "total RNA-seq"                                        => "Transcriptomic_NonTargeted"
      case "transcription profiling by array assay"               => "Transcriptomic_NonTargeted"
      case "WGS"                                                  => "Genomic_Genotyping_Whole Genomic"
      case "whole genome sequencing assay"                        => "Genomic_Genotyping_Whole Genomic"
      case "WGBS"                                                 => "Epigenomic_DNAMethylation"
      case "whole-genome shotgun bisulfite sequencing"            => "Epigenomic_DNAMethylation"
      // this will match any string and we can prepend it with !!! so it is easy to search in the DB
      case x: String => "!" + x
    }

  }
}
