package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import upack.Msg
import org.broadinstitute.monster.encode.jadeschema.table.Assay

object AssayTransformations {

  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for assays. */
  def transformAssay(
    rawExperiment: Msg
  ): Assay = {
    val id = CommonTransformations.readId(rawExperiment)

    Assay(
      id = id,
      timeCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      description = rawExperiment.tryRead[String]("description"),
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
      case "CAGE"                                                 => "Transcriptomic_Unbiased"
      case "capture Hi-C"                                         => "???"
      case "ChIA-PET"                                             => "Epigenomic_3D Contact Maps"
      case "ChIP-seq"                                             => "Epigenomic_DNABinding"
      case "Circulome-seq"                                        => "Genomic"
      case "Clone-seq"                                            => "Proteomic"
      case "comparative genomic hybridization by array"           => "???"
      case "Control ChIP-seq"                                     => "Epigenomic_DNABinding"
      case "Control eCLIP"                                        => "Epigenomic_RNABinding"
      case "CRISPR RNA-seq"                                       => "Transcriptomic_NonTargeted"
      case "CRISPR genome editing followed by RNA-seq"            => "Transcriptomic_NonTargeted"
      case "CRISPRi RNA-seq"                                      => "Transcriptomic_NonTargeted"
      case "CRISPRi followed by RNA-seq"                          => "Transcriptomic_NonTargeted"
      case "CUT&RUN"                                              => "???"
      case "CUT&Tag"                                              => "???"
      case "direct RNA-seq"                                       => "Transcriptomic_NonTargeted"
      case "DNAme array"                                          => "Epigenomic_DNAMethylation"
      case "DNA methylation profiling by array assay"             => "Epigenomic_DNAMethylation"
      case "DNA-PET"                                              => "Epigenomic_3D Contact Maps"
      case "DNase-seq"                                            => "Epigenomic_DNAChromatinAccessibility"
      case "eCLIP"                                                => "???"
      case "FAIRE-seq"                                            => "Epigenomic_DNAChromatinAccessibility"
      case "GM DNase-seq"                                         => "Epigenomic_DNAChromatinAccessibility"
      case "genetic modification followed by DNase-seq"           => "Epigenomic_DNAChromatinAccessibility"
      case "genotype phasing by HiC"                              => "Genomic_Assembly"
      case "GRO-cap"                                              => "???"
      case "GRO-seq"                                              => "???"
      case "genotyping array"                                     => "Genomic_Genotyping"
      case "genotyping HTS"                                       => "Genomic_Genotyping_Whole Genomic"
      case "Hi-C"                                                 => "Epigenomic_3D Contact Maps"
      case "HiC"                                                  => "Epigenomic_3D Contact Maps"
      case "Histone ChIP-seq"                                     => "Epigenomic_DNABinding_HistoneModificationLocation"
      case "iCLIP"                                                => "Epigenomic_RNABinding"
      case "icLASER"                                              => "???"
      case "icSHAPE"                                              => "Epigenomic_RNABinding"
      case "LC/MS label-free quantitative proteomics"             => "Proteomic"
      case "LC-MS/MS isobaric label quantitative proteomics"      => "Proteomic"
      case "long read RNA-seq"                                    => "Transcriptomic_NonTargeted"
      case "long read single-cell RNA-seq"                        => "Transcriptomic_NonTargeted"
      case "MeDIP-seq"                                            => "Epigenomic_DNAMethylation"
      case "microRNA counts"                                      => "Transcriptomic_NonTargeted"
      case "microRNA-seq"                                         => "Transcriptomic_NonTargeted"
      case "Mint-ChIP-seq"                                        => "???"
      case "MNase-seq"                                            => "Epigenomic_DNAChromatinAccessibility"
      case "MRE-seq"                                              => "Epigenomic_DNAMethylation"
      case "PAS-seq"                                              => "Transcriptomic_NonTargeted"
      case "PLAC-seq"                                             => "Epigenomic_DNAChromatinAccessibility"
      case "polyA minus RNA-seq"                                  => "Transcriptomic_NonTargeted"
      case "polyA plus RNA-seq"                                   => "Transcriptomic_NonTargeted"
      case "PRO-cap"                                              => "Transcriptomic"
      case "PRO-seq"                                              => "Transcriptomic"
      case "MS-MS"                                                => "Proteomic"
      case "protein sequencing by tandem mass spectrometry assay" => "Proteomic"
      case "RAMPAGE"                                              => "Transcriptomic_NonTargeted"
      case "Repli-chip"                                           => "Genomic"
      case "Repli-seq"                                            => "Genomic"
      case "Ribo-seq"                                             => "???"
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
      case "SPRITE"                                               => "???"
      case "SPRITE-IP"                                            => "???"
      case "Switchgear"                                           => "Epigenomic_RNABinding"
      case "TAB-seq"                                              => "Epigenomic_DNAMethylation"
      case "TF ChIP-seq"                                          => "Epigenomic_DNABinding_TranscriptomeFactorLocation"
      case "total RNA-seq"                                        => "Transcriptomic_NonTargeted"
      case "WGS"                                                  => "Genomic_Genotyping_Whole Genomic"
      case "whole genome sequencing assay"                        => "Genomic_Genotyping_Whole Genomic"
      case "WGBS"                                                 => "Epigenomic_DNAMethylation"
      case "whole-genome shotgun bisulfite sequencing"            => "Epigenomic_DNAMethylation"
    }

  }
}
