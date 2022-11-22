package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime, ZoneOffset}
import org.broadinstitute.monster.encode.jadeschema.table.Biosample
import org.slf4j.LoggerFactory
import upack.Msg

/** Transformation logic for ENCODE biosample objects. */
object BiosampleTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE biosample into our preferred schema. */
  def transformBiosample(
    biosampleInput: Msg,
    joinedType: Option[Msg],
    joinedLibraries: Iterable[Msg],
    geneticMods: Iterable[Msg]
  ): Biosample = {

    def getMergedGeneticModStringAttribute(attribute: String) = {
      geneticMods.map(_.tryRead[String](attribute)).flatten.toSet.toList
    }

    def getMergedGeneticModStringListAttribute(attribute: String) = {
      geneticMods.flatMap(_.tryRead[List[String]](attribute)).flatten.toSet.toList
    }

    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(biosampleInput)
    val id = CommonTransformations.readId(biosampleInput)
    val partNumbers = joinedLibraries
      .flatMap(library => library.tryRead[String]("product_id"))
      .toSet[String]
    val lotIds = joinedLibraries
      .flatMap(library => library.tryRead[String]("lot_id"))
      .toSet[String]
    val libraryPrepIds = joinedLibraries
      .map(library => CommonTransformations.readId(library))
      .toList

    if (joinedType.isEmpty) {
      logger.warn(s"Biosample '$id' has no associated type!")
    }

    val organism_type =
      biosampleInput.tryRead[String]("organism").map(CommonTransformations.transformId).getOrElse("")
    val life_stage_attribute = organism_type + "_life_stage"

    val rawAge = biosampleInput.tryRead[String]("age")
    val (ageLowerBound, ageUpperBound) = CommonTransformations.computeAgeLowerAndUpperbounds(rawAge)

    val classification = joinedType.map(_.read[String]("classification"))

    val anatomicalSiteList: List[String] = classification match {
      case Some("tissue") | Some("organoid") => joinedType.map(_.read[String]("term_id")).toList
      case Some("cell line") | Some("primary cell") | Some("in vitro differentiated cells") =>
        joinedType.map(_.read[List[String]]("organ_slims")).getOrElse(List())
      case _ => List()
    }

    Biosample(
      biosampleId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        biosampleInput.read[String]("@id")
      ) :: biosampleInput.tryRead[List[String]]("dbxrefs").getOrElse(List.empty[String]),
      dateCreated = biosampleInput.read[OffsetDateTime]("date_created"),
      donorAgeAtCollectionLowerBound = ageLowerBound,
      donorAgeAtCollectionUpperBound = ageUpperBound,
      donorAgeAtCollectionUnit = biosampleInput.tryRead[String]("age_units"),
      donorAgeAtCollectionLifeStage = biosampleInput.tryRead[String](life_stage_attribute),
      donorAgeAtCollectionAgeCategory = None,
      source = CommonTransformations.convertToEncodeUrl(biosampleInput.tryRead[String]("source")),
      dateCollected = biosampleInput
        .tryRead[LocalDate]("date_obtained")
        .map(_.atStartOfDay().atOffset(ZoneOffset.UTC)),
      partOfDatasetId = List("ENCODE"),
      derivedFromBiosampleId =
        biosampleInput.tryRead[String]("part_of").map(CommonTransformations.transformId),
      anatomicalSite = anatomicalSiteList.headOption,
      biosampleType = classification,
      aprioriCellType = classification match {
        case Some("tissue") | Some("organoid") | Some("cell line") =>
          joinedType.map(_.read[List[String]]("cell_slims")).getOrElse(List())
        case Some("primary cell") | Some("in vitro differentiated cells") =>
          joinedType.map(_.read[String]("term_id")).toList
        case _ => List()
      },
      cellLine = classification match {
        case Some("cell line") => joinedType.map(_.read[String]("term_id"))
        case _                 => None
      },
      preservationState = biosampleInput.tryRead[String]("preservation_method"),
      seeAlso = biosampleInput.tryRead[String]("url"),
      donorId =
        biosampleInput.tryRead[String]("donor").map(CommonTransformations.transformId).toList,
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      diagnosisId = List[String](),
      disease = biosampleInput.tryRead[List[String]]("disease_term_id").map(_.head),
      award = CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("award")),
      cellIsolationMethod = biosampleInput.tryRead[String]("cell_isolation_method"),
      geneticMod = CommonTransformations.convertToEncodeUrl(
        biosampleInput.tryRead[List[String]]("applied_modifications").getOrElse(List.empty[String])
      ),
      healthStatus = biosampleInput.tryRead[String]("health_status"),
      lab = CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("lab")),
      sampleTreatment = CommonTransformations.convertToEncodeUrl(
        biosampleInput.tryRead[List[String]]("treatments").getOrElse(List.empty[String])
      ),
      wasPerturbed = biosampleInput.read[Boolean]("perturbed"),
      submittedBy =
        CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("submitted_by")),
      partNumber = if (partNumbers.size > 1) {
        logger.warn(
          s"Biosample '$id' has multiple product ids: [${partNumbers.mkString(",")}]."
        )
        None
      } else {
        partNumbers.headOption
      },
      lot = if (lotIds.size > 1) {
        logger.warn(s"Biosample '$id' has multiple lot ids: [${lotIds.mkString(",")}].")
        None
      } else {
        lotIds.headOption
      },
      libraryPrepId = libraryPrepIds,
      geneticModMerged = getMergedGeneticModStringAttribute("accession"),
      perturbation = getMergedGeneticModStringAttribute("pertubation"),
      geneticModType = getMergedGeneticModStringAttribute("purpose ")
        ::: getMergedGeneticModStringAttribute("category"),
      geneticModMethod = getMergedGeneticModStringAttribute("method"),
      nucleicAcidDeliveryMethod =
        getMergedGeneticModStringListAttribute("nucleic_acid_delivery_method"),
      modifiedSiteByTarget = CommonTransformations.convertToEncodeUrl(
        getMergedGeneticModStringAttribute("modified_site_by_target_id")
      ),
      modifiedSiteByGene = CommonTransformations.convertToEncodeUrl(
        getMergedGeneticModStringAttribute("modified_site_by_gene_id")
      ),
      modifiedSiteNonspecific = getMergedGeneticModStringAttribute("modified_site_nonspecific"),
      modifiedSiteByCoordinatesAssembly = geneticMods
        .map(_.tryRead[String]("modified_site_by_coordinates", "assembly"))
        .flatten
        .toSet
        .toList,
      modifiedSiteByCoordinatesChromosome = geneticMods
        .map(_.tryRead[String]("modified_site_by_coordinates", "chromosome"))
        .flatten
        .toSet
        .toList,
      modifiedSiteByCoordinatesStart = geneticMods
        .map(_.tryRead[Long]("modified_site_by_coordinates", "start"))
        .flatten
        .toSet
        .toList,
      modifiedSiteByCoordinatesEnd =
        geneticMods.map(_.tryRead[Long]("modified_site_by_coordinates", "end")).flatten.toSet.toList,
      introducedElements = getMergedGeneticModStringAttribute("introduced_elements"),
      guideType = getMergedGeneticModStringAttribute("guide_type"),
      introducedSequence = getMergedGeneticModStringAttribute("introduced_sequence"),
      introducedGene = getMergedGeneticModStringAttribute("introduced_gene"),
      introducedTagsName = List(),
//        geneticMods.flatMap(_.tryRead[List[String]]("introduced_tags", "name")).flatten.toList,
      introducedTagsLocation = List(),
//        geneticMods.flatMap(_.tryRead[List[String]]("introduced_tags", "location")).flatten.toList,
      introducedTagsPromoterUsed = List(),
//        geneticMods.flatMap(_.tryRead[List[String]]("introduced_tags", "promoter")).flatten.toList,
      introducedElementsDonor = getMergedGeneticModStringAttribute("introduced_elements_donor"),
      introducedElementsOrganism =
        getMergedGeneticModStringAttribute("introduced_elements_organism"),
      guideRnaSequence = getMergedGeneticModStringListAttribute("guide_rna_sequences"),
      rnaiSequence = getMergedGeneticModStringListAttribute("rnai_seqeunces"),
      leftRvdSequence = List(),
//      geneticMods
//        .flatMap(_.tryRead[List[String]]("RVD_sequence_pairs", "left_RVD_sequence"))
//        .flatten
//        .toList,
      rightRvdSequence = List(),
//      geneticMods
//        .flatMap(_.tryRead[List[String]]("RVD_sequence_pairs", "right_RVD_sequence"))
//        .flatten
//        .toList,
      document = getMergedGeneticModStringListAttribute("documents")
        .map(CommonTransformations.convertToEncodeUrl),
      treatment = getMergedGeneticModStringListAttribute("treatments")
        .map(CommonTransformations.transformId),
      zygosity = getMergedGeneticModStringAttribute("zygosity"),
      moi = getMergedGeneticModStringAttribute("MOI"),
      crisprSystem = getMergedGeneticModStringListAttribute("CRISPR_system"),
      casSpecies = getMergedGeneticModStringAttribute("cas_species"),
      description = getMergedGeneticModStringAttribute("description")
    )

  }

}
