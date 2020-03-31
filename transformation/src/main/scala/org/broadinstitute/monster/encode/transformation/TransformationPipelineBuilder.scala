package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder

import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.EncodeEntity
import upack.Msg

object TransformationPipelineBuilder extends PipelineBuilder[Args] {
  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

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

    // Donors can be processed in isolation.
    val donorInputs = readRawEntities(EncodeEntity.Donor)
    val donorOutput = donorInputs
      .withName("Transform donors")
      .map(DonorTransformations.transformDonor)
    StorageIO.writeJsonLists(donorOutput, "Donors", s"${args.outputPrefix}/donor")

    // So can antibodies.
    val antibodyInputs = readRawEntities(EncodeEntity.AntibodyLot)
    val antibodyOutput = antibodyInputs
      .withName("Transform antibodies")
      .map(AntibodyTransformations.transformAntibody)
    StorageIO.writeJsonLists(
      antibodyOutput,
      "Antibodies",
      s"${args.outputPrefix}/antibody"
    )

    // Files are more complicated.
    val fileInputs = readRawEntities(EncodeEntity.File)
    // Split the file stream by output category.
    val fileBranches = FileTransformations.partitionRawFiles(fileInputs)
    val fileIdToType = FileTransformations.buildIdTypeMap(fileBranches)

    val sequenceFileOutput = fileBranches.sequence
      .withName("Transform sequence files")
      .map(FileTransformations.transformSequenceFile)
    val alignmentFileOutput = fileBranches.alignment
      .withSideInputs(fileIdToType)
      .withName("Transform alignment files")
      .map { (rawFile, sideCtx) =>
        FileTransformations.transformAlignmentFile(rawFile, sideCtx(fileIdToType))
      }
      .toSCollection
    val otherFileOutput = fileBranches.other
      .withSideInputs(fileIdToType)
      .withName("Transform other files")
      .map { (rawFile, sideCtx) =>
        FileTransformations.transformOtherFile(rawFile, sideCtx(fileIdToType))
      }
      .toSCollection

    StorageIO.writeJsonLists(
      sequenceFileOutput,
      "Sequence Files",
      s"${args.outputPrefix}/sequence_file"
    )
    StorageIO.writeJsonLists(
      alignmentFileOutput,
      "Alignment Files",
      s"${args.outputPrefix}/alignment_file"
    )
    StorageIO.writeJsonLists(
      otherFileOutput,
      "Other Files",
      s"${args.outputPrefix}/other_file"
    )

    // Experiments merge two different raw streams.
    val experimentInputs = readRawEntities(EncodeEntity.Experiment)
    val fcExperimentInputs =
      readRawEntities(EncodeEntity.FunctionalCharacterizationExperiment)
    val experimentOutput = experimentInputs
      .withName("Merge experiments")
      .union(fcExperimentInputs)
      .withSideInputs(fileIdToType)
      .withName("Transform experiments")
      .map { (rawExperiment, sideCtx) =>
        ExperimentTransformations.transformExperiment(
          rawExperiment,
          sideCtx(fileIdToType)
        )
      }
      .toSCollection
    StorageIO.writeJsonLists(
      experimentOutput,
      "Experiments",
      s"${args.outputPrefix}/experiment_activity"
    )
    ()
  }
}