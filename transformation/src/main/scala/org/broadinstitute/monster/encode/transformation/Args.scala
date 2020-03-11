package org.broadinstitute.monster.encode.transformation

import caseapp.HelpMessage

// TODO: Add AppVersion/AppName/ProgName back in?
case class Args(
  @HelpMessage("Path to the top-level directory where JSON was extracted")
  inputPrefix: String,
  @HelpMessage("Path where transformed JSON should be written")
  outputPrefix: String
)
