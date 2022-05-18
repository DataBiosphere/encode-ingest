package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.EncodeEntity
import upack.Msg

object TransformationPipelineBuilder extends PipelineBuilder[Args] {

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
    // 03-2022 donor Id is needed by file transfor
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
    // 03-2022 Library Id needed by File transform
    val libraryInputs = readRawEntities(EncodeEntity.Library)
    val libraryOutput = libraryInputs
      .withName("Transform libraries")
      .map(LibraryTransformations.transformLibrary)

    StorageIO.writeJsonLists(
      libraryOutput,
      "Libraries",
      s"${args.outputPrefix}/library"
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

    // TODO? update for mixed_biosamples field?
    val librariesByBiosample = libraryInputs
      .withName("Key libraries by biosample")
      .keyBy(_.read[String]("biosample"))
      .groupByKey

    val libraryPrepOutput = libraryInputs
      .withName("Transform library preparation activity")
      .map(LibraryPreparationActivityTransformations.transformLibraryPreparationActivity)
    StorageIO.writeJsonLists(
      libraryPrepOutput,
      "LibraryPreparationActivity",
      s"${args.outputPrefix}/librarypreparationactivity"
    )

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

    // Experiments merge two different raw streams
    // Experiments contribute to Assay Activities and Experiment Activities
    val experimentInputs = readRawEntities(EncodeEntity.Experiment)
    val fcExperimentInputs = readRawEntities(EncodeEntity.FunctionalCharacterizationExperiment)

    // Files are more complicated.
    val fileInputs = readRawEntities(EncodeEntity.File)

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

    val alignmentActivityOutput = fileBranches.alignment
      .withName("Transform alignment files")
      .map {
        case (rawFile, rawExperiment) =>
          AlignmentActivityTransformations.transformAlignmentActivity(
            rawFile,
            rawExperiment
          )
      }
    StorageIO.writeJsonLists(
      alignmentActivityOutput,
      "Alignment Activity",
      s"${args.outputPrefix}/alignmentactivity"
    )

    val libraryData: SideInput[Seq[Msg]] = libraryInputs.asListSideInput

    val fileOutput = fileWithExperiments
      .withSideInputs(libraryData)
      .withName("Transform all files")
      .map {
        case ((rawFile, rawExperiment), sideCtx) =>
          FileTransformations.transformFile(rawFile, rawExperiment, sideCtx(libraryData))
      }
      .toSCollection
    StorageIO.writeJsonLists(
      fileOutput,
      "Files",
      s"${args.outputPrefix}/file"
    )

    val sequenceActivityOutput = fileBranches.sequence
      .withSideInputs(libraryData)
      .withName("Transform sequence files")
      .map {
        case ((rawFile, rawExperiment), sideCtx) =>
          SequencingActivityTransformations.transformSequencingActivity(
            rawFile,
            rawExperiment,
            sideCtx(libraryData)
          )
      }
      .toSCollection
    StorageIO.writeJsonLists(
      sequenceActivityOutput,
      "Sequence Activity",
      s"${args.outputPrefix}/sequencingactivity"
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
      .flatMap { file =>
        for (stepRunId <- file.tryRead[String]("step_run"))
          yield stepRunId -> file
      }
      .groupByKey

    // Join AnalysisStepRuns, AnalysisStepVersions, AnalysisSteps, and Files.
    // They will be used in both the StepRun and PipelineRun transformations.
    val stepRunInfo = analysisStepRuns
      .withName("Key step runs by analysis step version")
      .keyBy(_.read[String]("analysis_step_version"))
      .join(analysisStepVersionsById)
      .values // tuple of the format (stepRun, stepVersion)
      .withName("Key step runs by analysis step")
      .keyBy(_._2.read[String]("analysis_step"))
      .join(analysisStepsById)
      .values // tuple of the format ((stepRun, stepVersion), step)
      .withName("Key step runs by step run ID")
      .keyBy(_._1._1.read[String]("@id"))
      .leftOuterJoin(filesByStepRun)
      .values // tuple of the format ((stepRun, stepVersion), step), generatedFiles)

    // Transform step runs
    val stepRunOutput = stepRunInfo
      .withName("Transform step runs")
      .map {
        case (((stepRun, stepVersion), step), generatedFiles) =>
          StepActivityTransformations.transformStepActivity(
            stepRun,
            stepVersion,
            step,
            generatedFiles.toIterable.flatten
          )
      }
    StorageIO.writeJsonLists(
      stepRunOutput,
      "Step Runs",
      s"${args.outputPrefix}/step_run"
    )

    // Transform pipeline runs
    val pipelinesById = readRawEntities(EncodeEntity.Pipeline)
      .withName("Key pipelines by ID")
      .keyBy(_.read[String]("@id"))

    val pipelineRunOut = stepRunInfo.flatMap {
      case (((stepRun, _), step), generatedFiles) =>
        val filesIterable = generatedFiles.toIterable.flatten
        for {
          idPair <- AnalysisActivityTransformations.getPipelineExperimentIdPair(
            step,
            filesIterable,
            CommonTransformations.readId(stepRun)
          )
        } yield (idPair, filesIterable)
    }.groupBy(_._1)
      .withName("Key pipeline run tuples by pipeline ID")
      .map {
        case ((pipelineId, experimentId), stepRunGroup) =>
          pipelineId -> ((experimentId, stepRunGroup.flatMap(_._2)))
      }
      .join(pipelinesById)
      .values // tuple of the format ((experimentId, generatedFiles), pipeline)
      .withName("Transform pipeline runs")
      .map {
        case ((experimentId, generatedFiles), pipeline) =>
          AnalysisActivityTransformations.transformAnalysisActivity(
            pipeline,
            experimentId,
            generatedFiles
          )
      }
    StorageIO.writeJsonLists(
      pipelineRunOut,
      "Pipeline Runs",
      s"${args.outputPrefix}/analysisactivity"
    )

    val filesByExperiment = fileInputs
      .withName("Key files by experiments")
      .keyBy(_.read[String]("dataset"))
      .groupByKey

    val assayActivityOutput = experimentsById
      .withName("Transform Assay Activities")
      .leftOuterJoin(
        filesByExperiment
      ) // tuple of format (expId, (experiment, Option[Iterable[file]]))
      .withName("With Libraries")
      .leftOuterJoin(
        (librariesByExperiment)
      ) // (expId, ((experiment, Option[I[file]]), Option[I[Library]]))
      .values
      .withName("With Libraries")
      .map {
        case ((rawExperiment, rawFiles), rawLibraries) =>
          AssayActivityTransformations.transformAssayActivity(
            rawExperiment,
            rawFiles.toIterable.flatten,
            rawLibraries.toIterable.flatten
          )
      }

    StorageIO.writeJsonLists(
      assayActivityOutput,
      "Assay Activities",
      s"${args.outputPrefix}/assayactivity"
    )

    val experimentOutput = experimentsById
      .withName("Join experiments and libraries")
      .leftOuterJoin(librariesByExperiment)
      //      .values
      .withName("Transform experiments")
      .map {
        case (experimentId, (rawExperiment, rawLibraries)) =>
          ExperimentActivityTransformations.transformExperiment(
            experimentId,
            rawExperiment,
            rawLibraries.toIterable.flatten
          )
      }
    StorageIO.writeJsonLists(
      experimentOutput,
      "Experiment",
      s"${args.outputPrefix}/experimentactivity"
    )
    ()
  }
}
