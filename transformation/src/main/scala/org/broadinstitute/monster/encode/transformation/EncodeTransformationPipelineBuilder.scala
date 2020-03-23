package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import java.time.OffsetDateTime

import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.EncodeEntity
import org.broadinstitute.monster.encode.jadeschema.table._
import upack.{Msg, Obj, Str}

import scala.collection.mutable

object EncodeTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /** (De)serializer for the ODTs we extract from raw data. */
  implicit val odtCoder: Coder[OffsetDateTime] = Coder.xmap(Coder.stringCoder)(
    OffsetDateTime.parse(_),
    _.toString
  )

  /** Output category for sequencing files. */
  val SequencingCategory = "raw data"

  /** Output category for alignment files. */
  val AlignmentCategory = "alignment"

  /**
    * Schedule all the steps for the Encode transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    def readRawEntities(entityType: EncodeEntity): SCollection[Msg] =
      StorageIO
        .readJsonLists(
          ctx,
          entityType.entryName,
          s"${args.inputPrefix}/${entityType.entryName}/*.json"
        )
        .map(removeUnknowns)

    // read in extracted info
    val donorInputs = readRawEntities(EncodeEntity.Donor)
    val fileInputs = readRawEntities(EncodeEntity.File)

    val donorOutput = donorInputs.map(transformDonor)

    // write back to storage
    StorageIO.writeJsonLists(
      donorOutput,
      "Donors",
      s"${args.outputPrefix}/donor"
    )

    // Split the file stream based on category.
    val Seq(sequencingFiles, alignmentFiles, otherFiles) =
      fileInputs.partition(
        3,
        rawFile => {
          val category = rawFile.read[String]("output_category")
          if (category == SequencingCategory) 0
          else if (category == AlignmentCategory) 1
          else 2
        }
      )
    // FIXME: We should ultimately write out mapped files.
    StorageIO.writeJsonLists(
      sequencingFiles,
      "Sequencing Files",
      s"${args.outputPrefix}/sequencing_file"
    )
    StorageIO.writeJsonLists(
      alignmentFiles,
      "Alignment Files",
      s"${args.outputPrefix}/alignment_file"
    )
    StorageIO.writeJsonLists(
      otherFiles,
      "Other Files",
      s"${args.outputPrefix}/other_file"
    )
    ()
  }

  /** Message value used by ENCODE in place of null. */
  val UnknownValue: Msg = Str("unknown")

  /**
    * Process an arbitrary ENCODE object to strip out all
    * values representing null / absence of data.
    */
  def removeUnknowns(rawObject: Msg): Msg = {
    val cleaned = new mutable.LinkedHashMap[Msg, Msg]()
    rawObject.obj.foreach {
      case (_, UnknownValue) => ()
      case (k, v)            => cleaned += k -> v
    }
    new Obj(cleaned)
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
}
