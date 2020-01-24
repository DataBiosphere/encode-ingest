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
    outputDir: String,
    @HelpMessage(
      "Batch size that defines the number of elements in a batch when making certain API calls"
    )
    batchSize: Int
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

      EncodeExtractions.getEntitiesByField(EncodeEntity.Donor, parsedArgs.batchSize)(idParams)
    }

    val libraries = {
      val biosampleAccessionParams = EncodeExtractions.getIds(
        EncodeEntity.Biosample.entryName,
        referenceField = "accession",
        manyReferences = false
      )(biosamples)

      EncodeExtractions.getEntitiesByField(EncodeEntity.Library, parsedArgs.batchSize,"biosample.accession")(
        biosampleAccessionParams
      )
    }

    val replicates = {
      val libraryAccessionParams = EncodeExtractions.getIds(
        EncodeEntity.Library.entryName,
        referenceField = "accession",
        manyReferences = false
      )(libraries)

      EncodeExtractions.getEntitiesByField(EncodeEntity.Replicate, parsedArgs.batchSize,"library.accession")(
        libraryAccessionParams
      )
    }

    val experiments = {
      val experimentIdParams = EncodeExtractions.getIds(
        EncodeEntity.Experiment.entryName,
        referenceField = "@id",
        manyReferences = false
      )(replicates)

      EncodeExtractions.getEntitiesByField(EncodeEntity.Experiment, parsedArgs.batchSize)(experimentIdParams)
    }

    biosamples.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Biosample.entryName}")
    donors.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Donor.entryName}")
    libraries.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Library.entryName}")
    replicates.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Replicate.entryName}")
    experiments.saveAsJsonFile(s"${parsedArgs.outputDir}/${EncodeEntity.Experiment.entryName}")

    pipelineContext.run()
    ()
  }
}
