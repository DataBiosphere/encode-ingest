package org.broadinstitute.monster.encode.extraction

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.extra.json._
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
    outputDir: String
  )

  /**
    * pulling raw metadata using the ENCODE search client API for the following specific entity types...
    * Biosamples, donors, and libraries
    */
  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    val biosamples = {
      val additionalParams =
        pipelineContext.parallelize(List(List("organism.name" -> "human")))

      EncodeExtractions.getEntities(EncodeEntity.Biosample)(additionalParams)
    }

    val donors = {
      val idParams = EncodeExtractions.getIds(
        EncodeEntity.Donor.entryName,
        referenceField = "donor",
        manyReferences = false
      )(biosamples)

      EncodeExtractions.getEntitiesByField(EncodeEntity.Donor)(idParams)
    }

    val libraries = {
      val biosampleIdParams = EncodeExtractions.getIds(
        EncodeEntity.Biosample.entryName,
        referenceField = "accession",
        manyReferences = false
      )(biosamples)

      EncodeExtractions.getEntitiesByField(EncodeEntity.Library, "biosample.accession")(
        biosampleIdParams
      )
    }

    biosamples.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Biosample.entryName}")
    donors.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Donor.entryName}")
    libraries.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Library.entryName}")

    pipelineContext.run()
    ()
  }
}
