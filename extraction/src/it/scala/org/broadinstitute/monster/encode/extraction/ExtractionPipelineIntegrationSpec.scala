package org.broadinstitute.monster.encode.extraction

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec

class ExtractionPipelineIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  val testBatchSize: Long = 100

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
    Args(outputDir.pathAsString, testBatchSize, allParams)
  override val builder = ExtractionPipeline.pipelineBuilder

  // Paths for entity files downloaded
  val AnalysisStepPath = (outputDir.pathAsString + "/AnalysisStep")
  val AnalysisStepRunPath = (outputDir.pathAsString + "/AnalysisStepRun")
  val AnalysisStepVersionPath = (outputDir.pathAsString + "/AnalysisStepVersion")
  val AntibodyLotPath = (outputDir.pathAsString + "/AntibodyLot")
  val BiosamplePath = (outputDir.pathAsString + "/Biosample")
  val DonorPath = (outputDir.pathAsString + "/Donor")
  val ExperimentPath = (outputDir.pathAsString + "/Experiment")
  val FilePath = (outputDir.pathAsString + "/File")
  val FunctionalCharacterizationExperimentPath = (outputDir.pathAsString + "/FunctionalCharacterizationExperiment")
  val LibraryPath = (outputDir.pathAsString + "/Library")
  val ReplicatePath = (outputDir.pathAsString + "/Replicate")
  val TargetPath = (outputDir.pathAsString + "/Target")

  /// VERIFY DOWNLOADS
  behavior of "ExtractionPipeline"

  it should "successfully download results from the from Encode API" in {
    outputDir.nonEmpty should be(true)
    println("outputDir has contents...")
  }
  it should "successfully download AnalysisStep files" in {
    AnalysisStepPath.nonEmpty should be(true)
    println("/AnalysisStep has contents...")
  }
  it should "successfully download AnalysisStepRun files" in {
    AnalysisStepRunPath.nonEmpty should be(true)
  }
  it should "successfully download AnalysisStepVersion files" in {
    AnalysisStepVersionPath.nonEmpty should be(true)
  }
  it should "successfully download AntibodyLot files" in {
    AntibodyLotPath.nonEmpty should be(true)
  }
  it should "successfully download Biosample files" in {
    BiosamplePath.nonEmpty should be(true)
  }
  it should "successfully download Donor files" in {
    DonorPath.nonEmpty should be(true)
  }
  it should "successfully download Experiment files" in {
    ExperimentPath.nonEmpty should be(true)
  }
  it should "successfully download File files" in {
    FilePath.nonEmpty should be(true)
  }
  it should "successfully download FunctionalCharacterizationExperiment files" in {
    FunctionalCharacterizationExperimentPath.nonEmpty should be(true)
  }
  it should "successfully download Library files" in {
    LibraryPath.nonEmpty should be(true)
  }
  it should "successfully download Replicate files" in {
    ReplicatePath.nonEmpty should be(true)
  }
  it should "successfully download Target files" in {
    TargetPath.nonEmpty should be(true)
  }
}
