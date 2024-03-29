import _root_.io.circe.Json

val enumeratumVersion = "1.5.15"
val okhttpVersion = "4.4.1"
val postgresDriverVersion = "42.2.12"
val postgresSocketFactoryVersion = "1.0.15"
val scioJdbcVersion = "0.8.4"
val snappyVersion = "1.1.8.2"

lazy val `encode-ingest` = project
  .in(file("."))
  .aggregate(
    `encode-common`,
    `encode-extraction`,
    `encode-schema`,
    `encode-transformation-pipeline`,
    `encode-explorer-file-backfill`,
    `encode-orchestration-workflow`
  )
  .settings(publish / skip := true)

lazy val `encode-common` = project
  .in(file("common"))
  .enablePlugins(MonsterBasePlugin)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "org.xerial.snappy" % "snappy-java" % snappyVersion
    )
  )

lazy val `encode-extraction` = project
  .in(file("extraction"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`encode-common`)
  .settings(
    libraryDependencies ++= Seq(
      "com.squareup.okhttp3" % "okhttp" % okhttpVersion
    )
  )

lazy val `encode-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeTablePackage := "org.broadinstitute.monster.encode.jadeschema.table",
    jadeTableFragmentPackage := "org.broadinstitute.monster.encode.jadeschema.fragment",
    jadeStructPackage := "org.broadinstitute.monster.encode.jadeschema.struct"
  )

lazy val `encode-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`encode-schema`, `encode-common`)

lazy val `encode-explorer-file-backfill` = project
  .in(file("explorer/file-backfill"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.google.cloud.sql" % "postgres-socket-factory" % postgresSocketFactoryVersion,
      "com.spotify" %% "scio-jdbc" % scioJdbcVersion,
      "org.postgresql" % "postgresql" % postgresDriverVersion
    )
  )

lazy val `encode-orchestration-workflow` = project
  .in(file("orchestration"))
  .enablePlugins(MonsterHelmPlugin)
  .settings(
    helmChartOrganization := "DataBiosphere",
    helmChartRepository := "encode-ingest",
    helmInjectVersionValues := { (baseValues, version) =>
      val schemaVersionValues = Json.obj(
        "argoTemplates" -> Json.obj(
          "diffBQTable" -> Json.obj(
            "schemaImageVersion" -> Json.fromString(version)
          )
        )
      )
      baseValues.deepMerge(schemaVersionValues)
    }
  )
