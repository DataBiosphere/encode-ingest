package org.broadinstitute.monster.encode.extraction

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.EncodeIngestBuildInfo
import org.broadinstitute.monster.common.msg.MsgOps
import org.broadinstitute.monster.common.StorageIO._
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import upack._

/**
  * ETL workflow for scraping the latest entity metadata from ENCODE.
  */
object ExtractionPipeline {

  @AppName("ENCODE Extraction Pipeline")
  @AppVersion(EncodeIngestBuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.encode.ExtractionPipeline")
  /**
    * Command-line arguments for the ETL workflow.
    *
    * scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
    * parsers + help text for these args (as well as Beams' underlying options)
    */
  case class Args(
    @HelpMessage(
      "Path to directory where the extracted raw ENCODE metadata should be written"
    )
    outputDir: String,
    @HelpMessage(
      "Batch size that defines the number of elements in a batch when making certain API calls"
    )
    batchSize: Long
  )

  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  // determines whether a replicate is linked to "type=functional-characterization-experiment"
  def isFunctionalCharacterizationReplicate(replicate: Msg): Boolean = {
    replicate
      .read[String]("experiment")
      .startsWith("/functional-characterization-experiments/")
  }

  /**
    * pulling raw metadata using the ENCODE search client API for the following specific entity types...
    * Biosamples, donors, and libraries
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

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
          parsedArgs.batchSize,
          linkedField
        ) {
          EncodeExtractions.getIds(
            matchingField
          )(linkingEntities)
        }
        writeJsonLists(
          out,
          s"${entityToExtract.entryName}",
          s"${parsedArgs.outputDir}/${entityToExtract.entryName}"
        )
        out
      }
    }

    // biosamples are the first one and follow a different pattern, so we don't use the generic method
    val biosamples = pipelineContext
      .parallelize(List(List("organism.name" -> "human")))
      .transform(s"Extract ${EncodeEntity.Biosample} data") { rawData =>
        val biosamples = EncodeExtractions.getEntities(EncodeEntity.Biosample)(rawData)
        writeJsonLists(
          biosamples,
          s"${EncodeEntity.Biosample}",
          s"${parsedArgs.outputDir}/${EncodeEntity.Biosample.entryName}"
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

    // partition the replicates stream into two separate SCollection[Msg]
    //passing in a new function to check to see if the experiment type
    val (fcReplicate, expReplicate) = replicates.partition { replicate =>
      isFunctionalCharacterizationReplicate(replicate)
    }

    extractLinkedEntities(
      entityToExtract = EncodeEntity.AntibodyLot,
      matchingField = "antibody",
      linkingEntities = replicates
    )

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

    pipelineContext.run()
    ()
  }
}
