package org.broadinstitute.monster.encode.extraction

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.PipelineBuilder
import org.broadinstitute.monster.common.StorageIO.writeJsonLists
import org.broadinstitute.monster.common.msg.MsgOps
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import upack._

object ExtractionPipelineBuilder extends PipelineBuilder[Args] {

  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  // determines whether a replicate is linked to "type=functional-characterization-experiment"
  def isFunctionalCharacterizationReplicate(replicate: Msg): Boolean = {
    replicate
      .read[String]("experiment")
      .startsWith("/functional-characterization-experiments/")
  }

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
        val out = EncodeExtractions.getEntitiesByField(
          entityToExtract,
          args.batchSize,
          linkedField
        ) {
          EncodeExtractions.getIds(
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
        val biosamples = EncodeExtractions.getEntities(EncodeEntity.Biosample)(rawData)
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
      isFunctionalCharacterizationReplicate(replicate)
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

    extractLinkedEntities(
      entityToExtract = EncodeEntity.File,
      matchingField = "@id",
      linkingEntities = SCollection.unionAll(List(experiments, fcExperiments)),
      linkedField = "dataset"
    )
    ()
  }
}
