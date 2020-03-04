import org.broadinstitute.monster.sbt.model.JadeIdentifier

val enumeratumVersion = "1.5.13"
val okhttpVersion = "4.4.0"

lazy val `encode-ingest` = project
  .in(file("."))
  .aggregate(`encode-extraction`, `encode-transformation`)
  .settings(publish / skip := true)

lazy val `encode-extraction` = project
  .in(file("extraction"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.squareup.okhttp3" % "okhttp" % okhttpVersion
    )
  )

lazy val `encode-transformation` = project
  .in(file("transformation"))
  .enablePlugins(MonsterJadeDatasetPlugin, MonsterScioPipelinePlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
        .fromString("broad_dsp_encode")
        .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of ENCODE, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.encode.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.encode.jadeschema.struct"
  )
