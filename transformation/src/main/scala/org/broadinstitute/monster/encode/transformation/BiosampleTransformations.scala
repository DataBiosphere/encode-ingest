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
    joinedLibraries: Iterable[Msg]
  ): Biosample = {
    val biosampleId = CommonTransformations.readId(biosampleInput)
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(biosampleInput)

    val productIds = joinedLibraries
      .flatMap(library => library.tryRead[String]("product_id"))
      .toSet[String]
    val lotIds = joinedLibraries
      .flatMap(library => library.tryRead[String]("lot_id"))
      .toSet[String]

    if (joinedType.isEmpty) {
      logger.warn(s"Biosample '$biosampleId' has no associated type!")
    }

    Biosample(
      id = CommonTransformations.readId(biosampleInput),
      crossReferences = CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("@id")) :: biosampleInput.read[List[String]]("dbxrefs"),
      timeCreated = biosampleInput.read[OffsetDateTime]("date_created"),
      source = CommonTransformations.convertToEncodeUrl(biosampleInput.tryRead[String]("source")),
      dateObtained = biosampleInput.tryRead[LocalDate]("date_obtained"),
      derivedFromBiosampleId =
        biosampleInput.tryRead[String]("part_of").map(CommonTransformations.transformId),
      anatomicalSite = joinedType.map(_.read[String]("term_id")),
      biosampleType = joinedType.map(_.read[String]("classification")),
      samplePreservationState = biosampleInput.tryRead[String]("preservation_method"),
      seeAlso = biosampleInput.tryRead[String]("url"),
      donorId = biosampleInput.tryRead[String]("donor").map(CommonTransformations.transformId),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("award")),
      cellIsolationMethod = biosampleInput.tryRead[String]("cell_isolation_method"),
      geneticModifications = biosampleInput.read[List[String]]("applied_modifications"),
      healthStatus = biosampleInput.tryRead[String]("health_status"),
      lab = CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("lab")),
      treatments = CommonTransformations.convertToEncodeUrl(biosampleInput.read[List[String]]("treatments")),
      wasPerturbed = biosampleInput.read[Boolean]("perturbed"),
      submittedBy = CommonTransformations.convertToEncodeUrl(biosampleInput.read[String]("submitted_by")),
      productId = if (productIds.size > 1) {
        logger.warn(
          s"Biosample '$biosampleId' has multiple product ids: [${productIds.mkString(",")}]."
        )
        None
      } else {
        productIds.headOption
      },
      lotId = if (lotIds.size > 1) {
        logger.warn(s"Biosample '$biosampleId' has multiple lot ids: [${lotIds.mkString(",")}].")
        None
      } else {
        lotIds.headOption
      }
    )
  }
}
