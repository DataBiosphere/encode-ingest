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

    // The Antibody transformation needs information from the Target objects
    val antibodyInputs = readRawEntities(EncodeEntity.AntibodyLot)
    val targetInputs = readRawEntities(EncodeEntity.Target)
    val targetsById = targetInputs
      .withName("Key targets by id")
      .keyBy(_.read[String]("@id"))

    // join antibodies (currently a messy process) TODO: clean up
    val antibodyOutput = antibodyInputs.map { rawAntibody =>
      rawAntibody
        .tryRead[Array[String]]("targets")
        .getOrElse(Array.empty)
        .map((_, rawAntibody))
    }.flatten
      .keyBy(_._1) // key by target id
      .leftOuterJoin(targetsById)
      .values
      .groupBy(_._1._2.read[String]("@id")) // group by antibody id
      .values
      .map { expandedValues =>
        val antibody = expandedValues.head._1._2
        val joinedTargets = expandedValues.flatMap(_._2)
        AntibodyTransformations.transformAntibody(antibody, joinedTargets)
      }

    StorageIO.writeJsonLists(
      antibodyOutput,
      "Antibodies",
      s"${args.outputPrefix}/antibody"
    )

    // Libraries can also be processed in isolation
    val libraryInputs = readRawEntities(EncodeEntity.Library)
    val libraryOutput = libraryInputs
      .withName("Transform libraries")
      .map(LibraryTransformations.transformLibrary)
    StorageIO.writeJsonLists(
      libraryOutput,
      "Libraries",
      s"${args.outputPrefix}/library"
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

    // Experiments merge two different raw streams,
    // and join against both replicates and libraries.
    val experimentInputs = readRawEntities(EncodeEntity.Experiment)
    val fcExperimentInputs = readRawEntities(EncodeEntity.FunctionalCharacterizationExperiment)
    val replicateInputs = readRawEntities(EncodeEntity.Replicate)

    val librariesByExperiment = {
      val keyedReplicates = replicateInputs
        .withName("Key replicates by library")
        .keyBy(_.read[String]("library"))
      val keyedLibraries = libraryInputs
        .withName("Key libraries by ID")
        .keyBy(_.read[String]("@id"))

      keyedReplicates
        .withName("Join replicates and libraries")
        .leftOuterJoin(keyedLibraries)
        .values
        .flatMap {
          case (replicate, maybeLibrary) =>
            maybeLibrary.map(lib => replicate.read[String]("experiment") -> lib)
        }
        .groupByKey
    }

    val experimentOutput = experimentInputs
      .withName("Merge experiments")
      .union(fcExperimentInputs)
      .withName("Key experiments by ID")
      .keyBy(_.read[String]("@id"))
      .withName("Join experiments and libraries")
      .leftOuterJoin(librariesByExperiment)
      .values
      .withSideInputs(fileIdToType)
      .withName("Transform experiments")
      .map {
        case ((rawExperiment, rawLibraries), sideCtx) =>
          ExperimentTransformations.transformExperiment(
            rawExperiment,
            rawLibraries.toIterable.flatten,
            sideCtx(fileIdToType)
          )
      }
      .toSCollection
    StorageIO.writeJsonLists(
      experimentOutput,
      "Experiments",
      s"${args.outputPrefix}/experiment_activity"
    )

    // Biosample transformation needs Libraries, Experiments, and BiosampleTypes
    val biosampleInputs = readRawEntities(EncodeEntity.Biosample)
    val biosampleTypeInputs = readRawEntities(EncodeEntity.BiosampleType)

    val typesById = biosampleTypeInputs
      .withName("Key biosample types by ID")
      .keyBy(_.read[String]("@id"))

    val biosamplesWithTypes = biosampleInputs
      .withName("Key biosamples by type")
      .keyBy(_.read[String]("biosample_ontology"))
      .leftOuterJoin(typesById)
      .values

    val librariesByBiosample = libraryInputs
      .withName("Key libraries by biosample")
      .keyBy(_.read[String]("biosample"))
      .groupByKey

    val biosampleOutput = biosamplesWithTypes
      .withName("Key biosamples by ID")
      .keyBy {
        case (rawSample, _) =>
          rawSample.read[String]("@id")
      }
      .leftOuterJoin(librariesByBiosample)
      .values
      .withName("Transform biosamples")
      .map {
        case ((biosample, joinedType), joinedLibraries) =>
          BiosampleTransformations.transformBiosample(
            biosample,
            joinedType,
            joinedLibraries.toIterable.flatten
          )
      }
    StorageIO.writeJsonLists(
      biosampleOutput,
      "Biosamples",
      s"${args.outputPrefix}/biosample"
    )
    ()
  }
}
