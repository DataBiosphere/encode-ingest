import org.broadinstitute.monster.sbt.model.JadeIdentifier

val enumeratumVersion = "1.5.13"
val logbackVersion = "1.2.3"
val scioVersion = "0.8.0"

lazy val `encode-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_encode")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of the ENCODE project, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.encode.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.encode.jadeschema.struct",
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.spotify" %% "scio-extra" % scioVersion,
    )
  )
