val enumeratumVersion = "1.5.13"

val scalatestVersion = "3.1.0"

lazy val `encode-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
    ),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalatestVersion,
    ).map(_ % Test),
    scioReleaseBucketName := "TODO",
    scioSnapshotBucketName := "TODO"
  )
