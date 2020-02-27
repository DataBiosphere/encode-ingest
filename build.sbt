val enumeratumVersion = "1.5.13"

lazy val `encode-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
    )
  )
