package org.broadinstitute.monster.encode.extraction

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import org.broadinstitute.monster.common.PipelineBuilder
import org.broadinstitute.monster.common.StorageIO.writeJsonLists
import org.broadinstitute.monster.common.msg.{MsgOps, UpackMsgCoder}
import upack._

import scala.collection.JavaConverters._
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
  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    /**
      * Generic helper method for extracting linked entities and saving them.
      *
      * @param entityToExtract the entity which should be extracted
      * @param matchingField name of the field to use as an identifier for the entityToExtract
      *                      (should match the id that's used in the linkedField)
      * @param linkingEntities the parent entity (which references a set of encodeEntities)
      *                        from which ID extraction will be based
      * @param linkedField name of the entityToExtract field which references the linkingEntities (default is @id)
      * @return the extracted linked entities
      */
    def extractLinkedEntities(
      entityToExtract: EncodeEntity,
      matchingField: String,
      linkingEntities: SCollection[Msg],
      linkedField: String = "@id"
    ): SCollection[Msg] = {
      linkingEntities.transform(
        s"Extract ${matchingField} from ${entityToExtract.entryName} data"
      ) { linkingEntities =>
        val out = getEntitiesByField(
          entityToExtract,
          args.batchSize,
          linkedField
        ) {
          getIds(
            matchingField
          )(linkingEntities)
        }
        writeJsonLists(
          out,
          s"${entityToExtract.entryName}",
          s"${args.outputDir}/${entityToExtract.entryName}"
        )
        out
      }
    }

    // biosamples are the first one and follow a different pattern, so we don't use the generic method
    val biosamples = ctx
      .parallelize(List(List("organism.name" -> "human")))
      .transform(s"Extract ${EncodeEntity.Biosample} data") { rawData =>
        val biosamples = getEntities(EncodeEntity.Biosample)(rawData)
        writeJsonLists(
          biosamples,
          s"${EncodeEntity.Biosample}",
          s"${args.outputDir}/${EncodeEntity.Biosample.entryName}"
        )
        biosamples
      }

    // don't need to use donors apart from storing them, so we don't assign an output here
    extractLinkedEntities(EncodeEntity.Donor, "donor", biosamples)

    val libraries = extractLinkedEntities(
      entityToExtract = EncodeEntity.Library,
      matchingField = "accession",
      linkingEntities = biosamples,
      linkedField = "biosample.accession"
    )

    val replicates = extractLinkedEntities(
      entityToExtract = EncodeEntity.Replicate,
      matchingField = "accession",
      linkingEntities = libraries,
      linkedField = "library.accession"
    )

    val antibodies = extractLinkedEntities(
      entityToExtract = EncodeEntity.AntibodyLot,
      matchingField = "antibody",
      linkingEntities = replicates
    )

    extractLinkedEntities(
      entityToExtract = EncodeEntity.Target,
      matchingField = "targets",
      linkingEntities = antibodies
    )

    // partition the replicates stream into two separate SCollection[Msg]
    //passing in a new function to check to see if the experiment type
    val (fcReplicate, expReplicate) = replicates.partition { replicate =>
      ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(replicate)
    }

    val experiments = extractLinkedEntities(
      entityToExtract = EncodeEntity.Experiment,
      matchingField = "experiment",
      linkingEntities = expReplicate
    )

    val fcExperiments = extractLinkedEntities(
      entityToExtract = EncodeEntity.FunctionalCharacterizationExperiment,
      matchingField = "experiment",
      linkingEntities = fcReplicate
    )

    val files = extractLinkedEntities(
      entityToExtract = EncodeEntity.File,
      matchingField = "@id",
      linkingEntities = SCollection.unionAll(List(experiments, fcExperiments)),
      linkedField = "dataset"
    )

    val analysisStepRuns = extractLinkedEntities(
      entityToExtract = EncodeEntity.AnalysisStepRun,
      matchingField = "step_run",
      linkingEntities = files
    )

    val analysisStepVersions = extractLinkedEntities(
      entityToExtract = EncodeEntity.AnalysisStepVersion,
      matchingField = "analysis_step_version",
      linkingEntities = analysisStepRuns
    )

    extractLinkedEntities(
      entityToExtract = EncodeEntity.AnalysisStep,
      matchingField = "analysis_step",
      linkingEntities = analysisStepVersions
    )
    ()
  }

  /**
    * Pipeline stage which maps batches of query params to output payloads
    * by sending the query params to the ENCODE search API.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  private def encodeLookup(encodeEntity: EncodeEntity) =
    new ScalaAsyncLookupDoFn[List[(String, String)], Msg, EncodeClient] {

      override def asyncLookup(
        client: EncodeClient,
        params: List[(String, String)]
      ): Future[Msg] = {
        client.get(encodeEntity, params)
      }

      override protected def newClient(): EncodeClient = getClient()
    }

  /**
    * Pipeline stage which maps batches of search parameters into MessagePack entities
    * from ENCODE matching those parameters.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  def getEntities(
    encodeEntity: EncodeEntity
  ): SCollection[List[(String, String)]] => SCollection[Msg] =
    _.applyKvTransform(ParDo.of(encodeLookup(encodeEntity))).flatMap { kv =>
      kv.getValue
        .fold(
          throw _,
          _.read[Array[Msg]]("@graph")
        )
    }

  /**
    * Pipeline stage which extracts IDs from downloaded MessagePack entities for
    * use in subsequent queries.
    *
    * @param referenceField field in the input MessagePacks containing the IDs
    *                       to extract
    */
  def getIds(
    referenceField: String
  ): SCollection[Msg] => SCollection[String] =
    collection =>
      collection.flatMap { msg =>
        msg.tryRead[Array[String]](referenceField).getOrElse(Array.empty)
      }.distinct

  /**
    * Pipeline stage which maps entity IDs into corresponding MessagePack entities
    * downloaded from ENCODE.
    *
    * @param encodeEntity the type of ENCODE entity the input IDs correspond to
    * @param batchSize the number of elements in a batch stream
    * @param fieldName the field name to get the entity by, is "@id" by default
    */
  def getEntitiesByField(
    encodeEntity: EncodeEntity,
    batchSize: Long,
    fieldName: String = "@id"
  ): SCollection[String] => SCollection[Msg] = { idStream =>
    val paramsBatchStream =
      idStream
        .map(KV.of("key", _))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .applyKvTransform(GroupIntoBatches.ofSize(batchSize))
        .map(_.getValue)
        .map { ids =>
          ids.asScala.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
            (fieldName -> ref) :: acc
          }
        }
    getEntities(encodeEntity)(paramsBatchStream)
  }
}

object ExtractionPipelineBuilder {

  // determines whether a replicate is linked to "type=functional-characterization-experiment"
  def isFunctionalCharacterizationReplicate(replicate: Msg): Boolean = {
    replicate
      .read[String]("experiment")
      .startsWith("/functional-characterization-experiments/")
  }
}