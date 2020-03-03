val patternBase =
  "org/broadinstitute/monster/[module](_[scalaVersion])(_[sbtVersion])/[revision]"

val publishPatterns = Patterns()
  .withIsMavenCompatible(false)
  .withIvyPatterns(Vector(s"$patternBase/ivy-[revision].xml"))
  .withArtifactPatterns(Vector(s"$patternBase/[module]-[revision](-[classifier]).[ext]"))

resolvers += Resolver.url(
  "Broad Artifactory",
  new URL("https://broadinstitute.jfrog.io/broadinstitute/libs-release/")
)(publishPatterns)

val sbtPluginsVersion = "0.9.0"

addSbtPlugin("org.broadinstitute.monster" % "sbt-plugins-jade" % sbtPluginsVersion)

addSbtPlugin("org.broadinstitute.monster" % "sbt-plugins-scio" % sbtPluginsVersion)