package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.Sampletreatmentactivity
import upack.Msg

import java.time.OffsetDateTime

/** Transformation logic for ENCODE donor objects. */
object SampleTreatmentActivityTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE donor into our preferred schema. */
  def transformSampleTreatment(treatmentInput: Msg): Sampletreatmentactivity = {
    val id = CommonTransformations.readId(treatmentInput)

    Sampletreatmentactivity(
      sampletreatmentactivityId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        treatmentInput.read[String]("@id")
      ) :: treatmentInput
        .tryRead[List[String]]("dbxrefs")
        .getOrElse(List.empty[String]),
      dateCreated = Some(treatmentInput.read[OffsetDateTime]("date_created")),
      activityType = Some("sampletreatment"),
      dataModality = List(),
      sampleTreatmentMethod = treatmentInput.tryRead[String]("treatment_type"),
      treatmentTermId = treatmentInput.tryRead[String]("treatment_term_id"),
      treatmentTermName = treatmentInput.tryRead[String]("treatment_term_name"),
      amount = treatmentInput.tryRead[Double]("amount"),
      amountUnits = treatmentInput.tryRead[String]("amount_units"),
      duration = treatmentInput.tryRead[Double]("duration"),
      durationUnits = treatmentInput.tryRead[String]("duration_units"),
      sampleTreatmentType = treatmentInput.tryRead[String]("purpose")
    )
  }
}
