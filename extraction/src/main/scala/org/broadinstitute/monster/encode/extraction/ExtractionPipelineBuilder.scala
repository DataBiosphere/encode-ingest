package org.broadinstitute.monster.encode.extraction

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.ParDo
import org.broadinstitute.monster.common.PipelineBuilder
import org.broadinstitute.monster.common.StorageIO.writeJsonListsGeneric
import org.broadinstitute.monster.common.msg.{MsgOps, UpackMsgCoder}
import org.broadinstitute.monster.encode.EncodeEntity
import upack._

import scala.concurrent.Future

/**
  * Builder for the ENCODE metadata extraction pipeline.
  *
  * @param getClient function that will produce a client which can interact with the ENCODE API
  */
class ExtractionPipelineBuilder(getClient: () => EncodeClient)
    extends PipelineBuilder[Args]
    with Serializable {
  import ExtractionPipelineBuilder._

  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /** Convenience alias to make signatures a little less nasty. */
  private type LookupParams = (EncodeEntity, List[(String, String)], List[(String, String)])

  private val lookupFn =
    new ScalaAsyncLookupDoFn[LookupParams, Msg, EncodeClient](MaxConcurrentRequests) {

      override def asyncLookup(
        client: EncodeClient,
        params: LookupParams
      ): Future[Msg] = {
        val (encodeEntity, posFilters, negFilters) = params
        client.get(encodeEntity, posFilters, negFilters)
      }
      override protected def newClient(): EncodeClient = getClient()
    }

  // Batch size of 64 seems to work well in practice.
  private val batchSize = 64L

  /**
    * Map a  batch of search parameters into MessagePack entities from
    * ENCODE matching those parameters.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    * @param filterBatches batches of key=value and key!=value filters to include in API queries
    */
  def getEntities(
    encodeEntity: EncodeEntity,
    filterBatches: SCollection[(List[(String, String)], List[(String, String)])]
  ): SCollection[Msg] =
    filterBatches.transform(s"Query ${encodeEntity.entryName} data") {
      _.withName("Construct full query parameters").map {
        case (pos, neg) => (encodeEntity, pos, neg)
      }.withName("Query ENCODE API")
        .applyKvTransform(ParDo.of(lookupFn))
        .withName("Extract result graph")
        .flatMap(_.getValue.fold(throw _, _.read[Array[Msg]]("@graph")))
    }

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    /*
     * Generic helper method for extracting entities and saving them.
     *
     * @param encodeEntity type of objects to extract from the API
     * @param queryBatches batches of query parameters to use when querying the API
     * @param negativeFilters batch of query parameters which should be used to
     *                        restrict the results returned by matching on queryBatches
     */
    def extractEntities(
      encodeEntity: EncodeEntity,
      queryBatches: SCollection[List[(String, String)]],
      negativeFilters: List[(String, String)]
    ): SCollection[Msg] = {
      val out = getEntities(encodeEntity, queryBatches.map(_ -> negativeFilters))
        .distinctBy(_.read[String]("@id"))
      writeJsonListsGeneric(
        out,
        encodeEntity.entryName,
        s"${args.outputDir}/${encodeEntity.entryName}"
      )
      out
    }

    /*
     * Generic helper method for extracting linked entities and saving them.
     *
     * @param sourceEntityType type of objects contained in `sourceEntities`
     * @param sourceField key within `sourceEntities` to extract for use as a query filter
     * @param sourceEntities raw objects of type `sourceEntityType` containing data
     *                       which can be used to pull `targetEntityType` objects
     * @param targetEntityType type of objects to extract from the API
     * @param targetField field in `targetEntityType` objects with values that match the
     *                    `sourceField` values from `sourceEntityType` objects
     */
    def extractLinkedEntities(
      sourceEntityType: EncodeEntity,
      sourceField: String,
      sourceEntities: SCollection[Msg],
      targetEntityType: EncodeEntity,
      targetField: String
    ): SCollection[Msg] = {
      val description =
        s"Build queries: ${targetEntityType.entryName}.$targetField=${sourceEntityType.entryName}.$sourceField"

      val queries = sourceEntities.transform(description) { entities =>
        val joinValues =
          entities.flatMap(_.tryRead[Array[String]](sourceField).getOrElse(Array.empty))
        groupValues(batchSize, joinValues.map(targetField -> _))
      }
      extractEntities(targetEntityType, queries, Nil)
    }

    // biosamples are the first one and follow a different pattern, so we don't use the generic method
    val biosamples = extractEntities(
      EncodeEntity.Biosample,
      ctx.withName("Inject initial query").parallelize(List(args.initialQuery)),
      Nil
    )

    val refrenceQuery: List[(String, String)] = List("status" -> "released")
    extractEntities(
      EncodeEntity.Reference,
      ctx
        .withName("Initial reference query")
        .parallelize(List(refrenceQuery)),
      Nil
    )

    // Don't need to use donors or biosample-types apart from storing them, so we don't assign them outputs here.
    extractLinkedEntities(
      sourceEntityType = EncodeEntity.Biosample,
      sourceField = "donor",
      sourceEntities = biosamples,
      targetEntityType = EncodeEntity.Donor,
      targetField = "@id"
    )
    extractLinkedEntities(
      sourceEntityType = EncodeEntity.Biosample,
      sourceField = "biosample_ontology",
      sourceEntities = biosamples,
      targetEntityType = EncodeEntity.BiosampleType,
      targetField = "@id"
    )
    extractLinkedEntities(
      sourceEntityType = EncodeEntity.Biosample,
      sourceField = "organism",
      sourceEntities = biosamples,
      targetEntityType = EncodeEntity.Organism,
      targetField = "@id"
    )

    extractLinkedEntities(
      sourceEntityType = EncodeEntity.Biosample,
      sourceField = "@id",
      sourceEntities = biosamples,
      targetEntityType = EncodeEntity.GeneticModification,
      targetField = "biosamples_modified"
    )

    val libraries = extractLinkedEntities(
      sourceEntityType = EncodeEntity.Biosample,
      sourceField = "accession",
      sourceEntities = biosamples,
      targetEntityType = EncodeEntity.Library,
      targetField = "biosample.accession"
    )

    val replicates = extractLinkedEntities(
      sourceEntityType = EncodeEntity.Library,
      sourceField = "accession",
      sourceEntities = libraries,
      targetEntityType = EncodeEntity.Replicate,
      targetField = "library.accession"
    )

    val antibodies = extractLinkedEntities(
      sourceEntityType = EncodeEntity.Replicate,
      sourceField = "antibody",
      sourceEntities = replicates,
      targetEntityType = EncodeEntity.AntibodyLot,
      targetField = "@id"
    )

    extractLinkedEntities(
      sourceEntityType = EncodeEntity.AntibodyLot,
      sourceField = "targets",
      sourceEntities = antibodies,
      targetEntityType = EncodeEntity.Target,
      targetField = "@id"
    )

    // partition the replicates stream into two separate SCollection[Msg]
    // passing in a new function to check to see if the experiment type
    val (fcReplicate, expReplicate) = replicates
      .withName("Split by experiment type")
      .partition(isFunctionalCharacterizationReplicate)

    val experiments = extractLinkedEntities(
      sourceEntityType = EncodeEntity.Replicate,
      sourceField = "experiment",
      sourceEntities = expReplicate,
      targetEntityType = EncodeEntity.Experiment,
      targetField = "@id"
    )
    val fcExperiments = extractLinkedEntities(
      sourceEntityType = EncodeEntity.Replicate,
      sourceField = "experiment",
      sourceEntities = fcReplicate,
      targetEntityType = EncodeEntity.FunctionalCharacterizationExperiment,
      targetField = "@id"
    )

    // Extracting files is too complicated to fit into the usual pattern.
    val files = {
      val allExperiments = experiments.withName("Merge experiments").union(fcExperiments)
      val allFileIds = allExperiments
        .withName("Extract file IDs")
        .flatMap { msg =>
          val inputFiles = msg.read[Array[String]]("contributing_files")
          val outputFiles = msg.read[Array[String]]("files")

          List.concat(inputFiles, outputFiles)
        }
        .distinct

      val queryBatches = allFileIds.transform(s"Build queries in File.@id") { ids =>
        groupValues(batchSize, ids.map("@id" -> _)).map(_ -> NegativeFileFilters)
      }

      getEntities(EncodeEntity.File, queryBatches)
    }

    writeJsonListsGeneric(
      files,
      EncodeEntity.File.entryName,
      s"${args.outputDir}/${EncodeEntity.File.entryName}"
    )

    val analysisStepRuns = extractLinkedEntities(
      sourceEntityType = EncodeEntity.File,
      sourceField = "step_run",
      sourceEntities = files,
      targetEntityType = EncodeEntity.AnalysisStepRun,
      targetField = "@id"
    )

    val analysisStepVersions = extractLinkedEntities(
      sourceEntityType = EncodeEntity.AnalysisStepRun,
      sourceField = "analysis_step_version",
      sourceEntities = analysisStepRuns,
      targetEntityType = EncodeEntity.AnalysisStepVersion,
      targetField = "@id"
    )

    val analysisSteps = extractLinkedEntities(
      sourceEntityType = EncodeEntity.AnalysisStepVersion,
      sourceField = "analysis_step",
      sourceEntities = analysisStepVersions,
      targetEntityType = EncodeEntity.AnalysisStep,
      targetField = "@id"
    )

    extractLinkedEntities(
      sourceEntityType = EncodeEntity.AnalysisStep,
      sourceField = "pipelines",
      sourceEntities = analysisSteps,
      targetEntityType = EncodeEntity.Pipeline,
      targetField = "@id"
    )

    ()
  }
}

object ExtractionPipelineBuilder {

  /**
    * Max number of HTTP requests to have in-flight at any time.
    *
    * Picked arbitrarily; the default is 1000 if not set, which
    * definitely makes the ENCODE server unhappy.
    */
  val MaxConcurrentRequests = 8

  /**
    * "Negative" filters to include in all file searches, to filter
    * out records we don't want to bother extracting.
    */
  val NegativeFileFilters: List[(String, String)] = List(
    "restricted" -> "true"
  )

  /**
    * Determines whether a replicate is linked to a FunctionalCharacterizationExperiment
    * (vs. a "normal" Experiment).
    */
  def isFunctionalCharacterizationReplicate(replicate: Msg): Boolean =
    replicate
      .read[String]("experiment")
      .startsWith("/functional-characterization-experiments/")

  /**
    * Group the values in a stream into fixed-size batches.
    *
    * NOTE: AFAIK, the ordering of items in the batches will depend on how
    * elements are spread across all the pipeline workers, so it shouldn't
    * be relied upon.
    *
    * FIXME: Move this to monster-scio-utils so it can be reused.
    *
    * @param batchSize max number of elements to include per output batch
    * @param vals stream of values to group
    */
  def groupValues[V: Coder](batchSize: Long, vals: SCollection[V]): SCollection[List[V]] =
    vals
      .withName("Apply fake keys")
      .map("" -> _)
      .batchByKey(batchSize)
      .withName("Strip away fake keys")
      .map(_._2.toList)
}
