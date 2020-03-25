package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder

import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.EncodeEntity
import upack.Msg

object EncodeTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

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
    def readRawEntities(entityType: EncodeEntity): SCollection[Msg] = {
      val name = entityType.entryName

      StorageIO
        .readJsonLists(ctx, name, s"${args.inputPrefix}/$name/*.json")
        .withName(s"Strip unknown values from '$name' objects")
        .map(CommonTransformations.removeUnknowns)
    }

    // read in extracted info
    val donorInputs = readRawEntities(EncodeEntity.Donor)
    val biosampleInputs = readRawEntities(EncodeEntity.Biosample)
    val fileInputs = readRawEntities(EncodeEntity.File)

    val donorOutput = donorInputs
      .withName("Transform Donor objects")
      .map(DonorTransformations.transformDonor)

    val biosampleOutput = biosampleInputs
        .withName("Transform Biosample objects")
        .map(BiosampleTransformations.transformBiosample)

    // write back to storage
    StorageIO.writeJsonLists(
      donorOutput,
      "Donors",
      s"${args.outputPrefix}/donor"
    )

    StorageIO.writeJsonLists(
      biosampleOutput,
      "Biosamples",
      s"${args.outputPrefix}/biosample"
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
}
