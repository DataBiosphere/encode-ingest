import org.broadinstitute.monster.sbt.model.JadeIdentifier

lazy val `encode-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_encode")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of the ENCODE project, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.encode.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.encode.jadeschema.struct"
  )
