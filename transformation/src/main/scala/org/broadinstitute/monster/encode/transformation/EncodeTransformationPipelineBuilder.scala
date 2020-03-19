package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import java.time.OffsetDateTime
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.jadeschema.table._
//import scala.util.matching.Regex
import upack.Msg

object EncodeTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /** (De)serializer for the ODTs we extract from raw data. */
  implicit val odtCoder: Coder[OffsetDateTime] = Coder.xmap(Coder.stringCoder)(
    OffsetDateTime.parse(_),
    _.toString
  )

  /**
    * Schedule all the steps for the Encode transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    // read in extracted info
    val donorInputs = StorageIO
      .readJsonLists(ctx, "Donors", s"${args.inputPrefix}/Donor/*.json")
    val antibodyInputs = StorageIO
      .readJsonLists(ctx, "AntibodyLot", s"${args.inputPrefix}/AntibodyLot/*.json")

    val donorOutput = donorInputs.map(transformDonor)
    val antibodyOutput = antibodyInputs.map(transformAntibody)

    // write back to storage
    StorageIO.writeJsonLists(donorOutput, "Donors", s"${args.outputPrefix}/donor")
    StorageIO.writeJsonLists(
      antibodyOutput,
      "Antibodies",
      s"${args.outputPrefix}/antibody"
    )
    ()
  }

  def transformDonor(donorInput: Msg): Donor = {
    Donor(
      id = donorInput.read[String]("accession"),
      crossReferences = donorInput.read[Array[String]]("dbxrefs"),
      timeCreated = donorInput.read[OffsetDateTime]("date_created"),
      age = donorInput.tryRead[Long]("age"),
      ageUnit = donorInput.tryRead[String]("age_units"),
      ethnicity = donorInput.tryRead[String]("ethnicity"),
      organism = donorInput.read[String]("organism"),
      sex = donorInput.read[String]("sex"),
      award = donorInput.read[String]("award"),
      lab = donorInput.read[String]("lab"),
      lifeStage = donorInput.tryRead[String]("life_stage"),
      parentIds = donorInput.read[Array[String]]("parents"),
      twinId = donorInput.tryRead[String]("twin"),
      submittedBy = donorInput.read[String]("submitted_by")
    )
  }

  def transformAntibody(antibodyInput: Msg): Antibody = {
    // Regex matching human species flag in encode targets, used to filter out non-human targets.
    // We expect that targets will have the same name after species info is removed, so we only need the first.
    val someTarget = antibodyInput
      .read[Array[String]]("targets")
      .head

    // extract code from target string (remove "/target/" and species info), get rid of synthetic targets
    var mappedTarget = Some(someTarget.replaceAll(raw"\/targets\/|-.*\/", "")): Option[
      String
    ]
    if (someTarget.matches(".*synthetic_tag/")) {
      mappedTarget = None
    }

    Antibody(
      id = antibodyInput.read[String]("accession"),
      crossReferences = antibodyInput.read[Array[String]]("dbxrefs"),
      timeCreated = antibodyInput.read[OffsetDateTime]("date_created"),
      source = antibodyInput.read[String]("source"),
      clonality = antibodyInput.read[String]("clonality"),
      hostOrganism = antibodyInput.read[String]("host_organism"),
      target = mappedTarget,
      award = antibodyInput.read[String]("award"),
      isotype = antibodyInput.tryRead[String]("isotype"),
      lab = antibodyInput.read[String]("lab"),
      lotId = antibodyInput.read[String]("lot_id"),
      productId = antibodyInput.read[String]("product_id"),
      purificationMethods = antibodyInput.read[Array[String]]("purifications")
    )
  }
}
