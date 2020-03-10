package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.common.StorageIO
import org.broadinstitute.monster.encode.jadeschema.table.HumanDonor
import org.threeten.bp.OffsetDateTime
import upack.Msg

object TransformationPipeline {

  def transformDonor(donorInput: Msg): HumanDonor = {
    HumanDonor(
      id = donorInput.read[String]("accession"),
      crossReferences = Array[String](), // dbxrefs
      timeCreated = donorInput.read[OffsetDateTime]("date_created"),
      age = donorInput.read[String]("age"),
      ethnicities = Array[String](), // ethinicity
      organism = donorInput.read[String]("organism"),
      sex = donorInput.read[String]("sex"),
      award = donorInput.read[String]("award"),
      lab = donorInput.read[String]("lab"),
      lifeStage = donorInput.read[String]("life_stage"),
      parentIds = Array[String](), // parents (correct id?)
      status = donorInput.read[String]("status"),
      twinIds = Array[String](), // twin (correct id?)
      submittedBy = donorInput.read[String]("submitted_by")
    )
  }

  def transformDonors(
    pipelineContext: ScioContext,
    inputPrefix: String,
    outputPrefix: String
  ): Unit = {
    // read in extracted info
    val donorInputs = StorageIO
      .readJsonLists(
        pipelineContext,
        "Donors",
        s"${inputPrefix}/Donors/*.json"
      )

    // transform
    //val humanDonorInputs = donorInputs.filter(donor => donor.extract[String]("type") == "HumanDonor")
    val humanDonorOutput = donorInputs.map(transformDonor)

    // write back to storage
    StorageIO.writeJsonLists(
      humanDonorOutput,
      "HumanDonors",
      s"${outputPrefix}/human_donor"
    )
    ()
  }
}
