package org.broadinstitute.monster.encode.transformation

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.EncodeTransformationPipelineBuildInfo

@AppName("Encode transformation pipeline")
@AppVersion(EncodeTransformationPipelineBuildInfo.version)
@ProgName("org.broadinstitute.monster.etl.encode.EncodeTransformationPipeline")
case class Args(
  @HelpMessage("Path to the top-level directory where JSON was extracted")
  inputPrefix: String,
  @HelpMessage("Path where transformed JSON should be written")
  outputPrefix: String
)
