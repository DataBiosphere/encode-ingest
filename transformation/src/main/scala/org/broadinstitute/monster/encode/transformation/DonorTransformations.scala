package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.Donor
import upack.Msg

/** Transformation logic for ENCODE donor objects. */
object DonorTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE donor into our preferred schema. */
  def transformDonor(donorInput: Msg): Donor = {
    val rawAge = donorInput.tryRead[String]("age")
    val (ageMin, ageMax) = rawAge.fold((Option.empty[Long], Option.empty[Long])) { raw =>
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
      id = CommonTransformations.readId(donorInput),
      crossReferences = donorInput.read[List[String]]("dbxrefs"),
      timeCreated = donorInput.read[OffsetDateTime]("date_created"),
      ageMin = ageMin,
      ageMax = ageMax,
      ageUnit = donorInput.tryRead[String]("age_units"),
      ethnicity = donorInput.tryRead[List[String]]("ethnicity").getOrElse(List.empty[String]),
      organism = donorInput.read[String]("organism"),
      sex = donorInput.tryRead[String]("sex"),
      award = donorInput.read[String]("award"),
      lab = donorInput.read[String]("lab"),
      lifeStage = donorInput.tryRead[String]("life_stage"),
      parentDonorIds = donorInput
        .read[List[String]]("parents")
        .map(CommonTransformations.transformId),
      twinDonorId = donorInput
        .tryRead[String]("twin")
        .map(CommonTransformations.transformId),
      submittedBy = donorInput.read[String]("submitted_by")
    )
  }
}
