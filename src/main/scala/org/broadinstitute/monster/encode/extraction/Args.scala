package org.broadinstitute.monster.encode.extraction

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.EncodeIngestBuildInfo

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
