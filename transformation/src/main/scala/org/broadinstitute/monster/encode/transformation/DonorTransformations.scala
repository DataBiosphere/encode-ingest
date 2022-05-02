package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.Donor
import upack.Msg

/** Transformation logic for ENCODE donor objects. */
object DonorTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE donor into our preferred schema. */
  def transformDonor(donorInput: Msg): Donor = {
    val id = CommonTransformations.readId(donorInput)
    val rawAge = donorInput.tryRead[String]("age")
    val (ageLowerbound, ageUpperbound) = rawAge.fold((Option.empty[Long], Option.empty[Long])) {
      raw =>
        if (raw.equals("90 or above")) {
          (Some(90), None)
        } else {
          val splitIdx = raw.indexOf('-')
          if (splitIdx == -1) {
            val parsed = raw.toLong
            (Some(parsed), Some(parsed))
          } else {
            val (min, max) = (raw.take(splitIdx), raw.drop(splitIdx + 1))
            (Some(min.toLong), Some(max.toLong))
          }
        }
    }

    Donor(
      id = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("@id")) :: donorInput
        .read[List[String]]("dbxrefs"),
      dateCreated = donorInput.read[OffsetDateTime]("time_created"),
      ageLowerbound = ageLowerbound,
      ageUpperbound = ageUpperbound,
      ageUnit = donorInput.tryRead[String]("age_units"),
      reportedEthnicity =
        donorInput.tryRead[List[String]]("ethnicity").getOrElse(List.empty[String]),
      organismType = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("organism")),
      phenotypicSex = donorInput.tryRead[String]("sex"),
      award = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("award")),
      lab = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("lab")),
      lifeStage = donorInput.tryRead[String]("life_stage"),
      parent = donorInput
        .read[List[String]]("parents")
        .map(CommonTransformations.transformId),
      sibling = donorInput
        .tryRead[String]("twin")
        .map(CommonTransformations.transformId),
      submittedBy = donorInput.read[String]("submitted_by")
    )
  }
}
