package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}
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
    val (ageLowerbound, ageUpperbound) = CommonTransformations.computeAgeLowerAndUpperbounds(rawAge)

    Biosample(
      biosampleId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        biosampleInput.read[String]("@id")
      ) :: biosampleInput.tryRead[List[String]]("dbxrefs").getOrElse(List.empty[String]),
      dateCreated = biosampleInput.read[OffsetDateTime]("date_created"),
      donorAgeAtCollectionAgeLowerbound = ageLowerbound,
      donorAgeAtCollectionAgeUpperbound = ageUpperbound,
      donorAgeAtCollectionAgeUnit = biosampleInput.tryRead[String]("age_units"),
      donorAgeAtCollectionAgeStage = biosampleInput.tryRead[String](life_stage_attribute),
      donorAgeAtCollectionAgeCategory = None,
      source = CommonTransformations.convertToEncodeUrl(biosampleInput.tryRead[String]("source")),
      dateObtained = biosampleInput.tryRead[LocalDate]("date_obtained"),
      partOfDatasetId = Some("ENCODE"),
      derivedFromBiosampleId =
        biosampleInput.tryRead[String]("part_of").map(CommonTransformations.transformId),
      anatomicalSite = joinedType.map(_.read[String]("term_id")),
      biosampleType = joinedType.map(_.read[String]("classification")),
      preservationState = biosampleInput.tryRead[String]("preservation_method"),
      seeAlso = biosampleInput.tryRead[String]("url"),
      donorId = biosampleInput.tryRead[String]("donor").map(CommonTransformations.transformId),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
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
      libraryPrep = libraryPrepIds,
      geneticModMerged = getMergedGeneticModStringAttribute("accession"),
      perturbation = getMergedGeneticModStringAttribute("pertubation"),
      geneticModType = getMergedGeneticModStringAttribute("purpose ")
        ::: getMergedGeneticModStringAttribute("category"),
      geneticModMethod = getMergedGeneticModStringAttribute("method"),
      nucleicAcidDeliveryMethod =
        getMergedGeneticModStringAttribute("nucleic_acid_delivery_method"),
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
      guideType = getMergedGeneticModStringAttribute("guild_type"),
      introducedSequence = getMergedGeneticModStringAttribute("introduced_sequence"),
      introducedGene = getMergedGeneticModStringAttribute("introduced_gene"),
      introducedTagsName =
        geneticMods.map(_.tryRead[String]("introduced_tags", "name")).flatten.toSet.toList,
      introducedTagsLocation =
        geneticMods.map(_.tryRead[String]("introduced_tags", "location")).flatten.toSet.toList,
      introducedTagsPromoterUsed =
        geneticMods.map(_.tryRead[String]("introduced_tags", "promoter_used")).flatten.toSet.toList,
      introducedElementsDonor = getMergedGeneticModStringAttribute("introduced_elements_donor"),
      introducedElementsOrganism =
        getMergedGeneticModStringAttribute("introduced_elements_organism"),
      guideRnaSequence = getMergedGeneticModStringAttribute("guide_rna_sequence"),
      rnaiSequence = getMergedGeneticModStringAttribute("rnai_seqeunce"),
      leftRvdSequence = geneticMods
        .map(_.tryRead[String]("RVD_sequence_pairs", "left_RVD_sequence"))
        .flatten
        .toSet
        .toList,
      rightRvdSequence = geneticMods
        .map(_.tryRead[String]("RVD_sequence_pairs", "right_RVD_sequence"))
        .flatten
        .toSet
        .toList,
      document = getMergedGeneticModStringAttribute("documents"),
      treatment = getMergedGeneticModStringAttribute("treatments"),
      zygosity = getMergedGeneticModStringAttribute("zygosity"),
      moi = getMergedGeneticModStringAttribute("MOI"),
      crisprSystem = getMergedGeneticModStringAttribute("CRISPR_system"),
      casSpecies = getMergedGeneticModStringAttribute("cas_species"),
      description = getMergedGeneticModStringAttribute("description")
    )

  }

}
