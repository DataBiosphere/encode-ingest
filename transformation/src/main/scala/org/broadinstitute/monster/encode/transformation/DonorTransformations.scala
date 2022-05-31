package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime

import org.broadinstitute.monster.encode.jadeschema.table.Donor
import upack.Msg

/** Transformation logic for ENCODE donor objects. */
object DonorTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE donor into our preferred schema. */
  def transformDonor(donorInput: Msg, organism: Option[Msg]): Donor = {
    val id = CommonTransformations.readId(donorInput)
    val rawAge = donorInput.tryRead[String]("age")
    val (ageLowerbound, ageUpperbound) = CommonTransformations.computeAgeLowerAndUpperbounds(rawAge)

    Donor(
      donorId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("@id")) :: donorInput
        .tryRead[List[String]]("dbxrefs")
        .getOrElse(List.empty[String]),
      dateCreated = donorInput.read[OffsetDateTime]("date_created"),
      ageAgeLowerbound = ageLowerbound,
      ageAgeUpperbound = ageUpperbound,
      ageAgeUnit = donorInput.tryRead[String]("age_units"),
      ageAgeStage = donorInput.tryRead[String]("life_stage"),
      ageAgeCategory = None,
      reportedEthnicity =
        donorInput.tryRead[List[String]]("ethnicity").getOrElse(List.empty[String]),
      organismType = organism
        .map(msg => msg.read[String]("scientific_name"))
        .getOrElse(donorInput.read[String]("organism")),
      phenotypicSex = donorInput.tryRead[String]("sex"),
      partOfDatasetId = Some("ENCODE"),
      award = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("award")),
      lab = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("lab")),
      parentDonorId = donorInput
        .tryRead[List[String]]("parents")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId),
      siblingDonorId = donorInput
        .tryRead[String]("twin")
        .map(CommonTransformations.transformId),
      submittedBy = CommonTransformations.convertToEncodeUrl(donorInput.read[String]("submitted_by"))
    )
  }
}
