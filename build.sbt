val enumeratumVersion = "1.5.13"

val okhttpVersion = "4.4.0"

lazy val `encode-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.squareup.okhttp3" % "okhttp" % okhttpVersion
    )
  )
