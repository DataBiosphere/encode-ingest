package org.broadinstitute.monster.encode.backfill

import caseapp.AppName
import caseapp.AppVersion
import org.broadinstitute.monster.buildinfo.EncodeExplorerFileBackfillBuildInfo
import caseapp.ProgName
import caseapp.HelpMessage

@AppName("ENCODE bulk-file backfill generator")
@AppVersion(EncodeExplorerFileBackfillBuildInfo.version)
@ProgName("org.broadinstitute.monster.encode.backfill.FileBackfillGenerator")
case class Args(
  @HelpMessage("Connection name for the Cloud SQL instance to read from")
  cloudSqlInstanceName: String,
  @HelpMessage("Database within the target Cloud SQL instance to read from")
  cloudSqlDb: String,
  @HelpMessage("Identity to use when connecting to Cloud SQL")
  cloudSqlUsername: String,
  @HelpMessage("Password to use for the target Cloud SQL identity")
  cloudSqlPassword: String,
  @HelpMessage("Path where bulk file requests should be written")
  outputPrefix: String
)
