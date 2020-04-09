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
  def transformBiosample(biosampleInput: Msg, joinedLibraries: Iterable[Msg]): Biosample = {
    val biosampleId = CommonTransformations.readId(biosampleInput)
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(biosampleInput)

    val productIds = joinedLibraries
      .flatMap(library => library.tryRead[String]("product_id"))
      .toSet[String]
    val lotIds = joinedLibraries
      .flatMap(library => library.tryRead[String]("lot_id"))
      .toSet[String]

    Biosample(
      id = CommonTransformations.readId(biosampleInput),
      crossReferences = biosampleInput.read[Array[String]]("dbxrefs"),
      timeCreated = biosampleInput.read[OffsetDateTime]("date_created"),
      source = biosampleInput.tryRead[String]("source"),
      dateObtained = biosampleInput.tryRead[LocalDate]("date_obtained"),
      derivedFromBiosampleId =
        biosampleInput.tryRead[String]("part_of").map(CommonTransformations.transformId),
      // TODO needs biosampleType join
      anatomicalSite = "site",
      // TODO needs biosampleType join
      biosampleType = "type",
      samplePreservationState = biosampleInput.tryRead[String]("preservation_method"),
      seeAlso = biosampleInput.tryRead[String]("url"),
      donorId = CommonTransformations.transformId(biosampleInput.read[String]("donor")),
      auditLabels = auditLabels,
      maxAuditFlag = auditLevel,
      award = biosampleInput.read[String]("award"),
      cellIsolationMethod = biosampleInput.tryRead[String]("cell_isolation_method"),
      geneticModifications = biosampleInput.read[Array[String]]("applied_modifications"),
      healthStatus = biosampleInput.tryRead[String]("health_status"),
      lab = biosampleInput.read[String]("lab"),
      // TODO needs to join with experiments
      replicationType = "repType",
      treatments = biosampleInput.read[Array[String]]("treatments"),
      wasPerturbed = biosampleInput.read[Boolean]("perturbed"),
      submittedBy = biosampleInput.read[String]("submitted_by"),
      productId = if (productIds.size > 1) {
        logger.warn(
          s"Biosample '$biosampleId' has multiple product ids: [${productIds.mkString(",")}]."
        )
        None
      } else {
        productIds.headOption
      },
      lotId = if (productIds.size > 1) {
        logger.warn(s"Biosample '$biosampleId' has multiple lot ids: [${lotIds.mkString(",")}].")
        None
      } else {
        lotIds.headOption
      }
    )
  }
}
