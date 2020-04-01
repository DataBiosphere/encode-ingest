package org.broadinstitute.monster.encode.extraction

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec

class ExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  val allParams: List[(String, String)] = {
    List(
      // Default Human Params
      "organism.name" -> "human",
      // Human Test Sample
      "accession" -> "ENCBS488IUR"
    )
  }

  /// KICK OFF EXTRACTION
  override val testArgs =
    Args(outputDir.pathAsString, allParams)
  override val builder = ExtractionPipeline.pipelineBuilder

  // Paths for entity files downloaded
  val analysisStepPath = (outputDir.pathAsString + "/AnalysisStep")
  val analysisStepRunPath = (outputDir.pathAsString + "/AnalysisStepRun")
  val analysisStepVersionPath = (outputDir.pathAsString + "/AnalysisStepVersion")
  val antibodyLotPath = (outputDir.pathAsString + "/AntibodyLot")
  val biosamplePath = (outputDir.pathAsString + "/Biosample")
  val donorPath = (outputDir.pathAsString + "/Donor")
  val experimentPath = (outputDir.pathAsString + "/Experiment")
  val filePath = (outputDir.pathAsString + "/File")

  val functionalCharacterizationExperimentPath =
    (outputDir.pathAsString + "/FunctionalCharacterizationExperiment")
  val libraryPath = (outputDir.pathAsString + "/Library")
  val replicatePath = (outputDir.pathAsString + "/Replicate")
  val targetPath = (outputDir.pathAsString + "/Target")

  /// VERIFY DOWNLOADS
  behavior of "ExtractionPipelineBuilder"

  it should "successfully download AnalysisStep files" in {
    analysisStepPath.nonEmpty should be(true)
  }
  it should "successfully download AnalysisStepRun files" in {
    analysisStepRunPath.nonEmpty should be(true)
  }
  it should "successfully download AnalysisStepVersion files" in {
    analysisStepVersionPath.nonEmpty should be(true)
  }
  it should "successfully download AntibodyLot files" in {
    antibodyLotPath.nonEmpty should be(true)
  }
  it should "successfully download Biosample files" in {
    biosamplePath.nonEmpty should be(true)
  }
  it should "successfully download Donor files" in {
    donorPath.nonEmpty should be(true)
  }
  it should "successfully download Experiment files" in {
    experimentPath.nonEmpty should be(true)
  }
  it should "successfully download File files" in {
    filePath.nonEmpty should be(true)
  }
  it should "successfully download FunctionalCharacterizationExperiment files" in {
    functionalCharacterizationExperimentPath.nonEmpty should be(true)
  }
  it should "successfully download Library files" in {
    libraryPath.nonEmpty should be(true)
  }
  it should "successfully download Replicate files" in {
    replicatePath.nonEmpty should be(true)
  }
  it should "successfully download Target files" in {
    targetPath.nonEmpty should be(true)
  }
}
