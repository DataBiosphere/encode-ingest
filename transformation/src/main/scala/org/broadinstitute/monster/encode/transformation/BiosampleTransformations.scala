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
    val biosample_id = CommonTransformations.readId(biosampleInput)
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(biosampleInput)

    var product_ids = joinedLibraries
      .flatMap(library => library.tryRead[String]("product_id"))
      .toSet[String]
    var lot_ids = joinedLibraries
      .flatMap(library => library.tryRead[String]("lot_id"))
      .toSet[String]

    // check that there are not more than one unique product_id or lot_id.
    // If there are multiple, default to None.
    if (product_ids.size > 1) {
      product_ids = Set()
      logger.warn(
        s"Biosample '$biosample_id' has more than one product id in: [${product_ids.mkString(",")}]."
      )
    }
    if (product_ids.size > 1) {
      lot_ids = Set()
      logger.warn(
        s"Biosample '$biosample_id' has more than one lot id in: [${lot_ids.mkString(",")}]."
      )
    }

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
      productId = product_ids.headOption,
      lotId = lot_ids.headOption
    )
  }
}
