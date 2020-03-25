package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.Biosample
import upack.Msg

/** Transformation logic for ENCODE biosample objects. */
object BiosampleTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE biosample into our preferred schema. */
  def transformBiosample(biosampleInput: Msg): Biosample = {

    Biosample(
      id = CommonTransformations.readId(biosampleInput),
      crossReferences = biosampleInput.read[Array[String]]("dbxrefs"),
      timeCreated = biosampleInput.read[OffsetDateTime]("date_created"),
      source = biosampleInput.tryRead[String]("source"),
      // DONE ABOVE HERE
      // FIXME don't know what timeObtained maps to
      timeObtained = "timestamp",
      derivedFromBiosampleId = biosampleInput.tryRead[String]("age_units"),
      anatomicalSite = biosampleInput.tryRead[String]("biosample_ontology").flatMap(site =>
      site.),
      biosampleType = biosampleInput.tryRead[String]("organism"),
      samplePreservationState = biosampleInput.tryRead[String](
        "preservation_method").fold("not reported")(identity), // DONE
      seeAlso = biosampleInput.tryRead[Array[String]]("url"), // DONE
      donorIds = "",
      auditLabels = biosampleInput.tryRead[Array[String]]("audit"), // DONE
      maxAuditFlag = biosampleInput
        .read[Array[String]]("parents")
        .map(CommonTransformations.transformId),
      award = biosampleInput.tryRead[String]("award"), // DONE
      biologicalReplicateId = biosampleInput.tryRead[String]("submitted_by"),
      cellIsolationMethods = biosampleInput.tryRead[Array[String]]("cell_isolation_method"), // DONE
      geneticModifications = biosampleInput.tryRead[Array[String]]("applied_modifications"), // DONE
      healthStatus = biosampleInput.tryRead[String]("health_status"), // DONE
      lab = biosampleInput.tryRead[String]("lab"), // DONE
      replicationType = "",
      technicalReplicateId = "",
      // DONE BELOW HERE
      treatments = biosampleInput.read[Array[String]]("treatments"),
      wasPerturbed = biosampleInput.read[Boolean]("perturbed"),
      submittedBy = biosampleInput.read[String]("submitted_by")
    )
  }
}
