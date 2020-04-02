package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.Biosample
import upack.Msg

/** Transformation logic for ENCODE biosample objects. */
object BiosampleTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE biosample into our preferred schema. */
  def transformBiosample(biosampleInput: Msg): Biosample = {
    val (auditLevel, auditLabels) = CommonTransformations.summarizeAudits(biosampleInput)
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
      // TODO needs replicates
      biologicalReplicateId = 42,
      cellIsolationMethod = biosampleInput.tryRead[String]("cell_isolation_method"),
      geneticModifications = biosampleInput.read[Array[String]]("applied_modifications"),
      healthStatus = biosampleInput.tryRead[String]("health_status"),
      lab = biosampleInput.read[String]("lab"),
      // TODO needs to join with experiments
      replicationType = "repType",
      // TODO needs replicates
      technicalReplicateId = 43,
      treatments = biosampleInput.read[Array[String]]("treatments"),
      wasPerturbed = biosampleInput.read[Boolean]("perturbed"),
      submittedBy = biosampleInput.read[String]("submitted_by")
    )
  }
}
