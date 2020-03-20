package org.broadinstitute.monster.encode.extraction

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.ParDo
import org.broadinstitute.monster.common.PipelineBuilder
import org.broadinstitute.monster.common.StorageIO.writeJsonLists
import org.broadinstitute.monster.common.msg.{MsgOps, UpackMsgCoder}
import org.broadinstitute.monster.encode.EncodeEntity
import upack._

import scala.concurrent.Future

/**
  *
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
  private type LookupParams = (EncodeEntity, List[(String, String)])

  private val lookupFn =
    new ScalaAsyncLookupDoFn[LookupParams, Msg, EncodeClient](MaxConcurrentRequests) {

      override def asyncLookup(
        client: EncodeClient,
        params: LookupParams
      ): Future[Msg] = {
        val (encodeEntity, filters) = params
        client.get(encodeEntity, filters)
      }
      override protected def newClient(): EncodeClient = getClient()
    }

  /**
    * Map a  batch of search parameters into MessagePack entities from
    * ENCODE matching those parameters.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    * @param filterBatches batches of key=value filters to include in API queries
    */
  def getEntities(
    encodeEntity: EncodeEntity,
    filterBatches: SCollection[List[(String, String)]]
  ): SCollection[Msg] =
    filterBatches
      .withName("Construct full query parameters")
      .map(encodeEntity -> _)
      .withName("Query ENCODE API")
      .applyKvTransform(ParDo.of(lookupFn))
      .withName("Extract result graph")
      .flatMap(_.getValue.fold(throw _, _.read[Array[Msg]]("@graph")))

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    /**
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
        s"Query ${targetEntityType.entryName} data using ${sourceEntityType.entryName} objects"

      val out = sourceEntities.transform(description) { entities =>
        val joinValues = getDistinctValues(sourceField, entities)
        val queryBatches = groupValues(args.batchSize, joinValues.map(targetField -> _))
        getEntities(targetEntityType, queryBatches)
      }
      writeJsonLists(
        out,
        s"${targetEntityType.entryName}",
        s"${args.outputDir}/${targetEntityType.entryName}"
      )
      out
    }

    // biosamples are the first one and follow a different pattern, so we don't use the generic method
    val biosamples = ctx
      .withName("Inject initial query")
      .parallelize(List(args.initialQuery))
      .transform(s"Extract ${EncodeEntity.Biosample} data") { initialQuery =>
        val biosamples = getEntities(EncodeEntity.Biosample, initialQuery)
        writeJsonLists(
          biosamples,
          s"${EncodeEntity.Biosample}",
          s"${args.outputDir}/${EncodeEntity.Biosample.entryName}"
        )
        biosamples
      }

    // don't need to use donors apart from storing them, so we don't assign an output here
    extractLinkedEntities(
      sourceEntityType = EncodeEntity.Biosample,
      sourceField = "donor",
      sourceEntities = biosamples,
      targetEntityType = EncodeEntity.Donor,
      targetField = "@id"
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
    //passing in a new function to check to see if the experiment type
    val (fcReplicate, expReplicate) =
      replicates.partition(isFunctionalCharacterizationReplicate)

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

    val files = extractLinkedEntities(
      // Fudging a little, but it's only used for logging so it's ok.
      sourceEntityType = EncodeEntity.Experiment,
      sourceField = "files",
      sourceEntities = SCollection.unionAll(List(experiments, fcExperiments)),
      targetEntityType = EncodeEntity.File,
      targetField = "@id"
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

    extractLinkedEntities(
      sourceEntityType = EncodeEntity.AnalysisStepVersion,
      sourceField = "analysis_step",
      sourceEntities = analysisStepVersions,
      targetEntityType = EncodeEntity.AnalysisStep,
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
    * Determines whether a replicate is linked to a FunctionalCharacterizationExperiment
    * (vs. a "normal" Experiment).
    */
  def isFunctionalCharacterizationReplicate(replicate: Msg): Boolean = {
    replicate
      .read[String]("experiment")
      .startsWith("/functional-characterization-experiments/")
  }

  /**
    * Extract the value(s) of a key from a stream of raw objects, filtering
    * out duplicates.
    *
    * If `key` is an array in any object, the items of the array will be
    * flattened into the stream and de-duped.
    *
    * @param key field in the input objects containing the values to extract
    * @param objects stream of raw objects to extract `key` from
    */
  def getDistinctValues(key: String, objects: SCollection[Msg]): SCollection[String] =
    objects.flatMap(_.tryRead[Array[String]](key).getOrElse(Array.empty)).distinct

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
