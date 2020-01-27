package org.broadinstitute.monster.encode.extraction

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
import io.circe.JsonObject
import org.broadinstitute.monster.EncodeIngestBuildInfo

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

  /**
    * pulling raw metadata using the ENCODE search client API for the following specific entity types...
    * Biosamples, donors, and libraries
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    /**
      * Generic helper method for extracting linked entities and saving them.
      *
      * @param encodeEntity the entity from which to extract a field
      * @param referenceField the field name to use as an identifier in getIds
      * @param prevData the data to base the ID extraction off of
      * @param entityFieldName the field name to extract if it isn't @id
      * @return the extracted linked entities
      */
    def extractLinkedEntities(
      encodeEntity: EncodeEntity,
      referenceField: String,
      prevData: SCollection[JsonObject],
      entityFieldName: String = "@id",
      manyReferences: Boolean = false
    ): SCollection[JsonObject] = {
      prevData.transform(s"Extract ${encodeEntity.entryName} data") { prevData =>
        val out = EncodeExtractions.getEntitiesByField(
          encodeEntity,
          parsedArgs.batchSize,
          entityFieldName
        ) {
          EncodeExtractions.getIds(
            referenceField,
            manyReferences
          )(prevData)
        }
        out.saveAsJsonFile(s"${parsedArgs.outputDir}/${encodeEntity.entryName}")
        out
      }
    }

    // biosamples are the first one and follow a different pattern, so we don't use the generic method
    val biosamples = pipelineContext
      .parallelize(List(List("organism.name" -> "human")))
      .transform(s"Extract ${EncodeEntity.Biosample} data") { rawData =>
        val biosamples = EncodeExtractions.getEntities(EncodeEntity.Biosample)(rawData)
        biosamples.saveAsJsonFile(
          s"${parsedArgs.outputDir}/${EncodeEntity.Biosample.entryName}"
        )
        biosamples
      }

    // don't need to use donors apart from storing them, so we don't assign an output here
    extractLinkedEntities(EncodeEntity.Donor, "donor", biosamples)

    val libraries = extractLinkedEntities(
      EncodeEntity.Library,
      "accession",
      biosamples,
      "biosample.accession"
    )

    val replicates = extractLinkedEntities(
      EncodeEntity.Replicate,
      "accession",
      libraries,
      "library.accession"
    )

    // don't need to use experiments RIGHT NOW apart from storing them, so we don't assign an output here
    // TODO output experiments for future use of experiments to get files
    extractLinkedEntities(
      EncodeEntity.Experiment,
      "experiment",
      replicates
    )

    pipelineContext.run()
    ()
  }
}
