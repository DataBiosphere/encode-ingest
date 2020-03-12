package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import java.time.OffsetDateTime
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.jadeschema.table.HumanDonor
import upack.Msg

object EncodeTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /**
    * Schedule all the steps for the Encode transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // read in extracted info
    val donorInputs = StorageIO
      .readJsonLists(
        ctx,
        "Donors",
        s"${args.inputPrefix}/Donor/*.json"
      )

    val humanDonorOutput = donorInputs.map(transformDonor)

    // write back to storage
    StorageIO.writeJsonLists(
      humanDonorOutput,
      "HumanDonors",
      s"${args.outputPrefix}/human_donor"
    )
    ()
  }

  def transformDonor(donorInput: Msg): HumanDonor = {
    HumanDonor(
      id = donorInput.read[String]("accession"),
      crossReferences = donorInput.read[Array[String]]("dbxrefs"),
      timeCreated = donorInput.read[OffsetDateTime]("date_created"),
      age = donorInput.tryRead[String]("age"),
      ethnicities = Array() ++ donorInput.tryRead[String]("ethnicity"),
      organism = donorInput.read[String]("organism"),
      sex = donorInput.read[String]("sex"),
      award = donorInput.read[String]("award"),
      lab = donorInput.read[String]("lab"),
      lifeStage = donorInput.tryRead[String]("life_stage"),
      parentIds = donorInput.read[Array[String]]("parents"),
      status = donorInput.read[String]("status"),
      twinIds = Array() ++ donorInput.tryRead[String]("twin"),
      submittedBy = donorInput.read[String]("submitted_by")
    )
  }
}
