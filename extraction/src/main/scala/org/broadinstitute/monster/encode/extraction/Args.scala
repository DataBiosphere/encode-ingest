package org.broadinstitute.monster.encode.extraction

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import caseapp.core.Error.MalformedValue
import caseapp.core.argparser.{ArgParser, SimpleArgParser}
import org.broadinstitute.monster.buildinfo.EncodeExtractionBuildInfo

/**
  * Command-line arguments for the ETL workflow.
  *
  * scio's `ContextAndArgs.typed` delegates down to `caseapp`, which will generate
  * parsers + help text for these args (as well as Beams' underlying options)
  */
@AppName("ENCODE Extraction Pipeline")
@AppVersion(EncodeExtractionBuildInfo.version)
@ProgName("org.broadinstitute.monster.etl.encode.ExtractionPipeline")
case class Args(
  @HelpMessage(
    "Path to directory where the extracted raw ENCODE metadata should be written"
  )
  outputDir: String,
  @HelpMessage(
    "Initial query to target a specific entry-point to the pipeline."
  )
//  initialQuery: List[(String, String)] = List("organism.name" -> "mouse")
  initialQuery: List[(String, String)] = List()
)


object Args {

  implicit val tupleParser: ArgParser[(String, String)] =
    SimpleArgParser.from("key=value") { s =>
      val i = s.indexOf("=")
      if (i == -1) {
        Left(MalformedValue("key=value", "Missing '=' delimiter"))
      } else {
        Right(s.take(i) -> s.drop(i + 1))
      }
    }
}
