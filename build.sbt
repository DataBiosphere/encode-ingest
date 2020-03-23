import org.broadinstitute.monster.sbt.model.JadeIdentifier

val enumeratumVersion = "1.5.13"
val okhttpVersion = "4.4.0"

lazy val `encode-ingest` = project
  .in(file("."))
  .aggregate(
    `encode-common`,
    `encode-extraction`,
    `encode-schema`,
    `encode-transformation-pipeline`
  )
  .settings(publish / skip := true)

lazy val `encode-common` = project
  .in(file("common"))
  .enablePlugins(MonsterBasePlugin)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion
    )
  )

lazy val `encode-extraction` = project
  .in(file("extraction"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`encode-common`)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.squareup.okhttp3" % "okhttp" % okhttpVersion
    )
  )

lazy val `encode-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_encode")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of ENCODE, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.encode.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.encode.jadeschema.struct"
  )

lazy val `encode-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`encode-schema`, `encode-common`)
