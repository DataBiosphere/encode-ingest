package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.EncodeEntity
import org.broadinstitute.monster.encode.jadeschema.table.Dataset
import upack.Msg

object TransformationPipelineBuilder extends PipelineBuilder[Args] {

  def readRawEntities(
    entityType: EncodeEntity,
    ctx: ScioContext,
    inputPrefix: String
  ): SCollection[Msg] = {
    readRawEntities(entityType.entryName, ctx, inputPrefix)
  }

  def readRawEntities(
    name: String,
    ctx: ScioContext,
    inputPrefix: String
  ): SCollection[Msg] = {
    StorageIO
      .readJsonLists(ctx, name, s"${inputPrefix}/$name/*.json")
      .withName(s"Strip unknown values from '$name' objects")
      .map(CommonTransformations.removeUnknowns)
  }

  /**
    * Schedule all the steps for the Encode transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val keyedOrganisms = getKeyedOrganisms(ctx, args.inputPrefix)

    val datasetOutput = DatasetTransformations.transformDataset()
    val datasetPColl: PCollection[Dataset] = ctx.pipeline.apply(Create.of[Dataset](datasetOutput))
    val datasetSColl: SCollection[Dataset] = ctx.wrap(datasetPColl)
    StorageIO.writeJsonLists(
      datasetSColl,
      "EncodeDataset",
      s"${args.outputPrefix}/dataset"
    )

    val referenceFileInputs = readRawEntities(EncodeEntity.Reference, ctx, args.inputPrefix)
    transformReferenceFileSet(args.outputPrefix, keyedOrganisms, referenceFileInputs)

    val donorInputs = readRawEntities(EncodeEntity.Donor, ctx, args.inputPrefix)
    transformDonor(args.outputPrefix, keyedOrganisms, donorInputs)

    // The Antibody transformation needs information from the Target objects
    transformAntibody(args.outputPrefix, ctx, args.inputPrefix)

    // Libraries can also be processed in isolation
    // 03-2022 Library Id needed by File transform
    val libraryInputs = readRawEntities(EncodeEntity.Library, ctx, args.inputPrefix)
    transformLibrary(args.outputPrefix, libraryInputs)

    // Biosample transformation needs Libraries, Experiments, and BiosampleTypes
    val biosampleInputs = readRawEntities(EncodeEntity.Biosample, ctx, args.inputPrefix)
    val biosampleTypeInputs = readRawEntities(EncodeEntity.BiosampleType, ctx, args.inputPrefix)

    val typesById = biosampleTypeInputs
      .withName("Key biosample types by ID")
      .keyBy(_.read[String]("@id"))

    val biosamplesWithTypes = biosampleInputs
      .withName("Key biosamples by type")
      .keyBy(_.read[String]("biosample_ontology"))
      .leftOuterJoin(typesById)
      .values

    // TODO? update for mixed_biosamples field?
    val librariesByBiosample = getLibrariesByBiosample(libraryInputs)

    val geneticModsInputs = readRawEntities(EncodeEntity.GeneticModification, ctx, args.inputPrefix)
    val geneticModsByBiosample = getGeneticModsByBiosample(geneticModsInputs)

    transformBiosample(
      args.outputPrefix,
      biosamplesWithTypes,
      librariesByBiosample,
      geneticModsByBiosample
    )

    val treatmentInputs = readRawEntities(EncodeEntity.Treatment, ctx, args.inputPrefix)
    transformSampleTreatmentActivity(args.outputPrefix, treatmentInputs);

    // Experiments merge two different raw streams
    // Experiments contribute to Assay Activities and Experiment Activities
    val experimentInputs = readRawEntities(EncodeEntity.Experiment, ctx, args.inputPrefix)
    val fcExperimentInputs =
      readRawEntities(EncodeEntity.FunctionalCharacterizationExperiment, ctx, args.inputPrefix)

    // Files are more complicated.
//    val fileInputs = readRawEntities(EncodeEntity.File, ctx, args.inputPrefix)

    val experimentsById = experimentInputs
      .withName("Merge experiments")
      .union(fcExperimentInputs)
      .withName("Key experiments by ID")
      .keyBy(_.read[String]("@id"))

    // Split the file stream by output category.
    val sequenceFiles = readRawEntities("SequenceFiles", ctx, args.inputPrefix)
    val alignmentFiles = readRawEntities("AlignmentFiles", ctx, args.inputPrefix)
    val signalFiles = readRawEntities("SignalFiles", ctx, args.inputPrefix)
    val annotationFiles = readRawEntities("AnnotationFiles", ctx, args.inputPrefix)
    val otherFiles = readRawEntities("OtherFiles", ctx, args.inputPrefix)

    val allFiles = alignmentFiles
      .union(sequenceFiles)
      .union(signalFiles)
      .union(annotationFiles)
      .union(otherFiles)

    val filesForStepRun = allFiles
      .filterNot(_.tryRead[String]("step_run").isEmpty)
    transformAlignmentActivity(args.outputPrefix, alignmentFiles)

    val libraryData: SideInput[Seq[Msg]] = libraryInputs.asListSideInput

    transformFiles(args.outputPrefix, allFiles, libraryData)
    transformSequenceActivity(args.outputPrefix, sequenceFiles, libraryData)

    // Experiments join against both replicates and libraries
    val replicateInputs = readRawEntities(EncodeEntity.Replicate, ctx, args.inputPrefix)
    val replicatesByExperiment = replicateInputs
      .withName("Key replicates by experiment")
      .keyBy(_.read[String]("experiment"))
      .groupByKey

    val librariesByExperiment = getLibrariesByExperiment(libraryInputs, replicateInputs)
    val experimentByLibrary =
      getExperimentByLibrary(libraryInputs, replicateInputs, experimentInputs)

    transformLibraryPreparationActivity(args.outputPrefix, libraryInputs, experimentByLibrary)

    // Get analysis step objects
    val stepRunInfo: SCollection[(((Msg, Msg), Msg), Option[Iterable[Msg]])] =
      getStepRunInfo(filesForStepRun, ctx, args.inputPrefix)
    transformStepActivity(args.outputPrefix, stepRunInfo)

    // Transform pipeline runs
    val pipelinesById = readRawEntities(EncodeEntity.Pipeline, ctx, args.inputPrefix)
      .withName("Key pipelines by ID")
      .keyBy(_.read[String]("@id"))

    transformAnalysisActivity(args.outputPrefix, stepRunInfo, pipelinesById)
    transformAssayActivity(
      args.outputPrefix,
      allFiles,
      experimentsById,
      replicatesByExperiment,
      librariesByExperiment
    )
    transformExperiment(
      args.outputPrefix,
      experimentsById,
      replicatesByExperiment,
      librariesByExperiment
    )
    ()
  }

  // get set of genetic mod objects for each biosample

  private def transformBiosample(
    outputPrefix: String,
    biosamplesWithTypes: SCollection[(Msg, Option[Msg])],
    librariesByBiosample: SCollection[(String, Iterable[Msg])],
    geneticModsByBiosample: SCollection[(String, Iterable[Msg])]
  ) = {
    val biosampleOutput = biosamplesWithTypes
      .withName("Key biosamples by ID")
      .keyBy {
        case (rawSample, _) =>
          rawSample.read[String]("@id")
      }
      .leftOuterJoin(librariesByBiosample)
      .withName("With libraries")
      .leftOuterJoin(geneticModsByBiosample)
      .values
      .withName("and genetic mods")
      .map {
        case (((biosample, joinedType), joinedLibraries), joinedGeneticMods) =>
          BiosampleTransformations.transformBiosample(
            biosample,
            joinedType,
            joinedLibraries.toIterable.flatten,
            joinedGeneticMods.toIterable.flatten
          )
      }
    StorageIO.writeJsonLists(
      biosampleOutput,
      "Biosamples",
      s"${outputPrefix}/biosample"
    )
    ()
  }

  private def transformSampleTreatmentActivity(
    outputPrefix: String,
    treatments: SCollection[Msg]
  ) = {
    val treatmentOutputs =
      treatments.map(SampleTreatmentActivityTransformations.transformSampleTreatment)
    StorageIO.writeJsonLists(
      treatmentOutputs,
      "SampleTreatmentActivities",
      s"${outputPrefix}/sampletreatmentactivity"
    )
    ()
  }

  private def getLibrariesByBiosample(libraryInputs: SCollection[Msg]) = {
    libraryInputs
      .withName("Key libraries by biosample")
      .keyBy(_.read[String]("biosample"))
      .groupByKey
  }

  private def getGeneticModsByBiosample(geneticMods: SCollection[Msg]) = {
    geneticMods
      .withName("Key genetic mods by biosample")
      .flatMap { rawMod =>
        rawMod.tryRead[Array[String]]("biosamples_modified").getOrElse(Array.empty).map { bioId =>
          bioId -> rawMod
        }
      }
      .groupByKey
  }

  private def transformLibraryPreparationActivity(
    outputPrefix: String,
    libraryInputs: SCollection[Msg],
    experimentByLibrary: SCollection[(String, Msg)]
  ) = {
    val keyedLibrary = libraryInputs
      .withName("Keyed libraries")
      .keyBy(_.read[String]("@id"))
    val libraryPrepOutput = keyedLibrary
      .withName("Transform library preparation activity")
      .leftOuterJoin(experimentByLibrary)
      .values
      .map {
        case (library, maybeExperiment) =>
          LibraryPreparationActivityTransformations.transformLibraryPreparationActivity(
            library,
            maybeExperiment
          )
      }
    StorageIO.writeJsonLists(
      libraryPrepOutput,
      "LibraryPreparationActivity",
      s"${outputPrefix}/librarypreparationactivity"
    )
    ()
  }

  private def transformAntibody(outputPrefix: String, ctx: ScioContext, inputPrefix: String) = {
    val antibodyInputs = readRawEntities(EncodeEntity.AntibodyLot, ctx, inputPrefix)
    val targetInputs = readRawEntities(EncodeEntity.Target, ctx, inputPrefix)
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
      s"${outputPrefix}/antibody"
    )
    ()
  }

  private def transformReferenceFileSet(
    outputPrefix: String,
    keyedOrganisms: SCollection[(String, Msg)],
    referenceInputs: SCollection[Msg]
  ) = {
    val referenceFileOutputs = referenceInputs
      .withName("Key by organism")
      .keyBy(_.tryRead[String]("organism").getOrElse(""))
      .leftOuterJoin(keyedOrganisms)
      .values
      .withName("Transform reference file set")
      .map {
        case (referenceFileSet, organism) =>
          ReferenceFileSetTransformations.transformReferenceFileSet(referenceFileSet, organism)
      }
    StorageIO.writeJsonLists(
      referenceFileOutputs,
      "ReferenceFileSet",
      s"${outputPrefix}/referencefileset"
    )
    ()
  }

  private def getKeyedOrganisms(ctx: ScioContext, inputPrefix: String) = {
    val organismInputs = readRawEntities(EncodeEntity.Organism, ctx, inputPrefix)
    organismInputs
      .withName("Key by name")
      .keyBy(_.read[String]("@id"))
  }

  private def transformDonor(
    outputPrefix: String,
    keyedOrganisms: SCollection[(String, Msg)],
    donorInputs: SCollection[Msg]
  ) = {
    val donorOutput = donorInputs
      .withName("Key by organism")
      .keyBy(_.read[String]("organism"))
      .leftOuterJoin(keyedOrganisms)
      .values
      .withName("Transform donors")
      .map {
        case (donor, organism) => DonorTransformations.transformDonor(donor, organism)
      }
    StorageIO.writeJsonLists(donorOutput, "Donors", s"${outputPrefix}/donor")
    ()
  }

  private def transformLibrary(outputPrefix: String, libraryInputs: SCollection[Msg]) = {
    val libraryOutput = libraryInputs
      .withName("Transform libraries")
      .map(LibraryTransformations.transformLibrary)

    StorageIO.writeJsonLists(
      libraryOutput,
      "Libraries",
      s"${outputPrefix}/library"
    )
    ()
  }

  private def transformFiles(
    outputPrefix: String,
    files: SCollection[Msg],
    libraryData: SideInput[Seq[Msg]]
  ) = {
    val fileOutput = files
      .withSideInputs(libraryData)
      .withName("Transform all files")
      .map {
        case (rawFile, sideCtx) =>
          FileTransformations.transformFile(rawFile, sideCtx(libraryData))
      }
      .toSCollection
    StorageIO.writeJsonLists(
      fileOutput,
      "Files",
      s"${outputPrefix}/file"
    )
    ()
  }

  private def transformSequenceActivity(
    outputPrefix: String,
    seqFiles: SCollection[Msg],
    libraryData: SideInput[Seq[Msg]]
  ) = {
    val sequenceActivityOutput = seqFiles
      .withSideInputs(libraryData)
      .withName("Transform sequence files")
      .map {
        case (rawFile, sideCtx) =>
          SequencingActivityTransformations.transformSequencingActivity(
            rawFile,
            sideCtx(libraryData)
          )
      }
      .toSCollection
    StorageIO.writeJsonLists(
      sequenceActivityOutput,
      "Sequence Activity",
      s"${outputPrefix}/sequencingactivity"
    )
    ()
  }

  private def getExperimentByLibrary(
    libraryInputs: SCollection[Msg],
    replicateInputs: SCollection[Msg],
    experimentInputs: SCollection[Msg]
  ): SCollection[(String, Msg)] = {
    val replicatedByLibraryId = replicateInputs
      .withName("Key replicates by library")
      .keyBy(_.read[String]("library"))
    val keyedLibraries = libraryInputs
      .withName("Key libraries by ID")
      .keyBy(_.read[String]("@id"))

    val libraryByReplicatedId = replicatedByLibraryId
      .withName("Join replicates and libraries")
      .leftOuterJoin(keyedLibraries)
      .values
      .flatMap {
        case (replicate, maybeLibrary) =>
          maybeLibrary.map(library => (replicate.read[String]("@id") -> library))
      }

    val experimentByReplicateId = experimentInputs
      .withName("key experiments by replicate")
      .keyBy(_.read[List[String]]("replicates"))
      .flatMap(entry => entry._1.map(item => item -> entry._2))

    experimentByReplicateId
      .withName("Joind experiments and libraries")
      .leftOuterJoin(libraryByReplicatedId)
      .values
      .flatMap {
        case (experiment, maybeLibrary) =>
          maybeLibrary.map(library => (library.read[String]("@id") -> experiment))
      }
      .distinctBy[String](_._1)
  }

  private def getLibrariesByExperiment(
    libraryInputs: SCollection[Msg],
    replicateInputs: SCollection[Msg]
  ): SCollection[(String, Iterable[Msg])] = {

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

  private def getStepRunInfo(
    fileInputs: SCollection[Msg],
    ctx: ScioContext,
    inputPrefix: String
  ): SCollection[(((Msg, Msg), Msg), Option[Iterable[Msg]])] = {
    val analysisStepRuns = readRawEntities(EncodeEntity.AnalysisStepRun, ctx, inputPrefix)
    val analysisStepVersionsById =
      readRawEntities(EncodeEntity.AnalysisStepVersion, ctx, inputPrefix)
        .withName("Key analysis step versions by ID")
        .keyBy(_.read[String]("@id"))
    val analysisStepsById = readRawEntities(EncodeEntity.AnalysisStep, ctx, inputPrefix)
      .withName("Key analysis steps by ID")
      .keyBy(_.read[String]("@id"))
    val filesByStepRun = fileInputs
      .withName("Key files by step run ID")
      .keyBy(_.read[String]("step_run"))
      .groupByKey

    // Join AnalysisStepRuns, AnalysisStepVersions, AnalysisSteps, and Files.
    // They will be used in both the StepRun and PipelineRun transformations.
    analysisStepRuns
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

  }

  private def transformStepActivity(
    outputPrefix: String,
    stepRunInfo: SCollection[(((Msg, Msg), Msg), Option[Iterable[Msg]])]
  ) = {

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
      "Step Activity",
      s"${outputPrefix}/stepactivity"
    )
    ()
  }

  private def transformAnalysisActivity(
    outputPrefix: String,
    stepRunInfo: SCollection[(((Msg, Msg), Msg), Option[Iterable[Msg]])],
    pipelinesById: SCollection[(String, Msg)]
  ) = {
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
      s"${outputPrefix}/analysisactivity"
    )
    ()
  }

  private def transformAssayActivity(
    outputPrefix: String,
    fileInputs: SCollection[Msg],
    experimentsById: SCollection[(String, Msg)],
    replicatesByExperiment: SCollection[(String, Iterable[Msg])],
    librariesByExperiment: SCollection[(String, Iterable[Msg])]
  ) = {
    val filesByExperiment = fileInputs
      .withName("Key files by experiments")
      .keyBy(_.read[String]("dataset"))
      .groupByKey

    val assayActivityOutput = experimentsById
      .withName("Transform Assay Activities")
      .leftOuterJoin(
        filesByExperiment
      ) // tuple of format (expId, (experiment, Option[Iterable[file]]))
      .withName("With Replicates")
      .leftOuterJoin(
        replicatesByExperiment
      ) // (expId, ((experiment, Option[I[file]]), Option[I[Replicate]]))
      .withName("With Libraries")
      .leftOuterJoin(
        librariesByExperiment
      )
      .values //((experiment, Option[I[file]]), Option[I[Replicate]]), Option[I[Library]])
      .map {
        case (((rawExperiment, rawFiles), rawReplicates), rawLibraries) =>
          AssayActivityTransformations.transformAssayActivity(
            rawExperiment,
            rawFiles.toIterable.flatten,
            rawReplicates.toIterable.flatten,
            rawLibraries.toIterable.flatten
          )
      }

    StorageIO.writeJsonLists(
      assayActivityOutput,
      "Assay Activities",
      s"${outputPrefix}/assayactivity"
    )
    ()
  }

  private def transformAlignmentActivity(
    outputPrefix: String,
    alignmentFiles: SCollection[Msg]
  ) = {
    val alignmentActivityOutput = alignmentFiles
      .withName("Transform alignment files")
      .map(AlignmentActivityTransformations.transformAlignmentActivity)

    StorageIO.writeJsonLists(
      alignmentActivityOutput,
      "Alignment Activity",
      s"${outputPrefix}/alignmentactivity"
    )
    ()
  }

  private def transformExperiment(
    outputPrefix: String,
    experimentsById: SCollection[(String, Msg)],
    replicatesByExperiment: SCollection[(String, Iterable[Msg])],
    librariesByExperiment: SCollection[(String, Iterable[Msg])]
  ) = {
    val experimentOutput = experimentsById
      .withName("Join experiments and replicates")
      .leftOuterJoin(replicatesByExperiment)
      .withName("Join libraries")
      .leftOuterJoin(librariesByExperiment)
      .withName("Transform experiments")
      .map {
        case (experimentId, ((rawExperiment, rawReplicates), rawLibraries)) =>
          ExperimentActivityTransformations.transformExperiment(
            experimentId,
            rawExperiment,
            rawReplicates.toIterable.flatten,
            rawLibraries.toIterable.flatten
          )
      }
    StorageIO.writeJsonLists(
      experimentOutput,
      "Experiment",
      s"${outputPrefix}/experimentactivity"
    )
    ()
  }

}
