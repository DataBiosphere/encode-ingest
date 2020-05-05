package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder

import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.EncodeEntity
import org.broadinstitute.monster.encode.transformation.PipelineRunTransformations.{
  getExperimentId,
  getPipelineId
}
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
    val keyedTargets = targetInputs
      .withName("Key targets by id")
      .keyBy(_.read[String]("@id"))

    val antibodyTargetPairs = antibodyInputs
      .withName("Create an id pair for each antibody-target relationship")
      .flatMap { rawAntibody =>
        rawAntibody.tryRead[Array[String]]("targets").getOrElse(Array.empty).map { targetId =>
          rawAntibody.read[String]("@id") -> targetId
        }
      }

    // Join target objects to the Antibody-Target pairs
    val targetsByAntibodyId = antibodyTargetPairs
      .withName("Key antibody-target pairs by target id")
      .keyBy(_._2)
      .join(keyedTargets)
      .values
      .map {
        case (antibodyTargetPair, target) =>
          antibodyTargetPair._1 -> target
      }
      .groupByKey

    // join Targets to Antibodies
    val antibodyOutput = antibodyInputs
      .withName("Key antibodies by ID")
      .keyBy(_.read[String]("@id"))
      .leftOuterJoin(targetsByAntibodyId)
      .values
      .withName("Transform Antibodies")
      .map {
        case (antibody, joinedTargets) =>
          AntibodyTransformations.transformAntibody(
            antibody,
            joinedTargets.toIterable.flatten
          )
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

    // Experiments merge two different raw streams
    val experimentInputs = readRawEntities(EncodeEntity.Experiment)
    val fcExperimentInputs = readRawEntities(EncodeEntity.FunctionalCharacterizationExperiment)

    val experimentsById = experimentInputs
      .withName("Merge experiments")
      .union(fcExperimentInputs)
      .withName("Key experiments by ID")
      .keyBy(_.read[String]("@id"))

    val fileWithExperiments = fileInputs
      .withName("Key files by experiments")
      .keyBy(_.read[String]("dataset"))
      .leftOuterJoin(experimentsById)
      .values

    // Split the file stream by output category.
    val fileBranches = FileTransformations.partitionRawFiles(fileWithExperiments)
    val fileIdToType = FileTransformations.buildIdTypeMap(fileBranches)

    val sequenceFileOutput = fileBranches.sequence
      .withName("Transform sequence files")
      .map {
        case (rawFile, rawExperiment) =>
          FileTransformations.transformSequenceFile(rawFile, rawExperiment)
      }
    val alignmentFileOutput = fileBranches.alignment
      .withSideInputs(fileIdToType)
      .withName("Transform alignment files")
      .map {
        case ((rawFile, rawExperiment), sideCtx) =>
          FileTransformations.transformAlignmentFile(rawFile, sideCtx(fileIdToType), rawExperiment)
      }
      .toSCollection
    val otherFileOutput = fileBranches.other
      .withSideInputs(fileIdToType)
      .withName("Transform other files")
      .map {
        case ((rawFile, rawExperiment), sideCtx) =>
          FileTransformations.transformOtherFile(rawFile, sideCtx(fileIdToType), rawExperiment)
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

    // Experiments join against both replicates and libraries
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

    // Get analysis step objects
    val analysisStepRuns = readRawEntities(EncodeEntity.AnalysisStepRun)
    val analysisStepVersionsById = readRawEntities(EncodeEntity.AnalysisStepVersion)
      .withName("Key analysis step versions by ID")
      .keyBy(_.read[String]("@id"))
    val analysisStepsById = readRawEntities(EncodeEntity.AnalysisStep)
      .withName("Key analysis steps by ID")
      .keyBy(_.read[String]("@id"))
    val filesByStepRun = fileInputs
      .withName("Key files by step run ID")
      .keyBy(_.tryRead[String]("step_run").getOrElse("")) // TODO make this less hacky
      .groupByKey

    // Join AnalysisStepRuns, AnalysisStepVersions, AnalysisSteps, Files and Experiments.
    // They will be used in both the StepRun and PipelineRun transformations.
    val joinedStepRuns = analysisStepRuns
      .withName("Key step runs by analysis step version")
      .keyBy(_.read[String]("analysis_step_version"))
      .join(analysisStepVersionsById)
      .values
      .withName("Key step runs by analysis step")
      .keyBy(_._2.read[String]("analysis_step"))
      .join(analysisStepsById)
      .values // outputs ((stepRun, stepVersion), step)
      .withName("Key step runs by step run ID")
      .keyBy(_._1._1.read[String]("@id"))
      .leftOuterJoin(filesByStepRun)
      .values
      .withSideInputs(fileIdToType)
      .withName("Transform step runs")

//    val processedJoinedStepRuns = joinedStepRuns.map {
//      case ((((stepRun, stepVersion), step), generatedFiles), sideCtx) =>
//        val flattenedFiles = generatedFiles.toIterable.flatten
//        val pipelineRunId = PipelineRunTransformations.transformPipelineRunId(stepRun, step, flattenedFiles)
//        (stepRun, stepVersion, pipelineRunId, generatedFiles, sideCtx)
//    }

    val stepRunOutput = joinedStepRuns.map {
      case ((((stepRun, stepVersion), step), generatedFiles), sideCtx) =>
        StepRunTransformations.transformStepRun(
          stepRun,
          stepVersion,
          step,
          generatedFiles.toIterable.flatten,
          sideCtx(fileIdToType)
        )
    }.toSCollection
    StorageIO.writeJsonLists(
      stepRunOutput,
      "Step Runs",
      s"${args.outputPrefix}/step_run"
    )

    // Transform PipelineRuns
    val pipelinesById = readRawEntities(EncodeEntity.Pipeline)
      .withName("Key pipelines by ID")
      .keyBy(_.read[String]("@id"))

    val pipelineRunOut = joinedStepRuns.flatMap {
      case ((((stepRun, _), step), generatedFiles), _) =>
        val stepRunId = CommonTransformations.readId(stepRun)
        val pipelineId = getPipelineId(step, stepRunId)
        val experimentId = getExperimentId(generatedFiles.toIterable.flatten, stepRunId)
        if (pipelineId.isEmpty || experimentId.isEmpty) None
        else Some((pipelineId.head, experimentId.head))
    }.toSCollection.distinct
      .withName("Key pipeline-experiment ID tuples by pipeline ID")
      .keyBy(_._1)
      .join(pipelinesById)
      .values
      .map {
        case ((_, experimentID), pipeline) =>
          PipelineRunTransformations.transformPipelineRun(pipeline, experimentID)
      }
    StorageIO.writeJsonLists(
      pipelineRunOut,
      "Pipeline Runs",
      s"${args.outputPrefix}/pipeline_run"
    )

    val assayOutput = experimentsById
      .withName("Join experiments and libraries")
      .leftOuterJoin(librariesByExperiment)
      .values
      .withSideInputs(fileIdToType)
      .withName("Transform experiments")
      .map {
        case ((rawExperiment, rawLibraries), sideCtx) =>
          AssayTransformations.transformExperiment(
            rawExperiment,
            rawLibraries.toIterable.flatten,
            sideCtx(fileIdToType)
          )
      }
      .toSCollection
    StorageIO.writeJsonLists(
      assayOutput,
      "Assays",
      s"${args.outputPrefix}/assay"
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
