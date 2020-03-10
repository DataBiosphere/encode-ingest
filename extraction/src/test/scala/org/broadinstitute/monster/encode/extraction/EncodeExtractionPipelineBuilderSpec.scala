package org.broadinstitute.monster.encode.extraction

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.common.msg.MsgOps
import upack._

import scala.collection.mutable

object EncodeExtractionPipelineBuilderSpec {
  val fakeIds = 1 to 1

  // biosamples
  val biosampleParams = ("organism.name", "human")

  val biosampleOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("donor") -> Str(i.toString),
          Str("accession") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // donors
  val donorParams = ("@id", "1")

  val donorOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // libraries
  val libraryParams = ("biosample.accession", "1")

  val libraryOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("accession") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // replicates
  val replicateParams = ("library.accession", "1")

  val replicateIds = 1 to 2

  val replicateOut = Obj(
    Str("@graph") -> new Arr(
      replicateIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("antibody") -> Str("1"),
          Str("experiment") -> Str(
            if (i == 1) {
              i.toString
            } else {
              "/functional-characterization-experiments/" + i.toString
            }
          )
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // antibodies
  val antibodyParams = ("@id", "1")

  val antibodyOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("targets") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // targets
  val targetParams = ("@id", "1")

  val targetOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // experiments
  val experimentParams = ("@id", "1")

  val experimentOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // fcexperiments
  val fcExperimentParams = ("@id", "/functional-characterization-experiments/2")

  val fcExperimentOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // files
  val fileParams = ("dataset", "1")

  val fileOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("step_run") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // analysisStepRuns
  val analysisStepRunParams = ("@id", "1")

  val analysisStepRunOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("analysis_step_version") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // analysisStepVersions
  val analysisStepVersionParams = ("@id", "1")

  val analysisStepVersionOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString),
          Str("analysis_step") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  // analysisStep
  val analysisStepParams = ("@id", "1")

  val analysisStepOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

  val responseMap = Map[(EncodeEntity, (String, String)), Msg](
    (EncodeEntity.Biosample, biosampleParams) -> biosampleOut,
    (EncodeEntity.Donor, donorParams) -> donorOut,
    (EncodeEntity.Library, libraryParams) -> libraryOut,
    (EncodeEntity.Replicate, replicateParams) -> replicateOut,
    (EncodeEntity.AntibodyLot, antibodyParams) -> antibodyOut,
    (EncodeEntity.Target, targetParams) -> targetOut,
    (EncodeEntity.Experiment, experimentParams) -> experimentOut,
    (EncodeEntity.FunctionalCharacterizationExperiment, fcExperimentParams) -> fcExperimentOut,
    (EncodeEntity.File, fileParams) -> fileOut,
    (EncodeEntity.AnalysisStepRun, analysisStepRunParams) -> analysisStepRunOut,
    (EncodeEntity.AnalysisStepVersion, analysisStepVersionParams) -> analysisStepVersionOut,
    (EncodeEntity.AnalysisStep, analysisStepParams) -> analysisStepOut
  )

  val mockClient = new MockEncodeClient(responseMap)
}

class EncodeExtractionPipelineBuilderSpec extends PipelineBuilderSpec[Args] {
  import EncodeExtractionPipelineBuilderSpec._

  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  override val testArgs = Args(
    outputDir = outputDir.pathAsString,
    // batchSize is 2 so that the fcexperiment piece of the pipeline actually has something to interact with
    batchSize = 2L
  )

  override val builder =
    new ExtractionPipelineBuilder(getClient = () => mockClient)

  behavior of "EncodeExtractionPipelineBuilder"

  it should "query ENCODE in the expected way" in {
    mockClient.recordedRequests.toSet shouldBe Set(
      (EncodeEntity.Biosample, biosampleParams),
      (EncodeEntity.Donor, donorParams),
      (EncodeEntity.Library, libraryParams),
      (EncodeEntity.Replicate, replicateParams),
      (EncodeEntity.AntibodyLot, antibodyParams),
      (EncodeEntity.Target, targetParams),
      (EncodeEntity.Experiment, experimentParams),
      (EncodeEntity.FunctionalCharacterizationExperiment, fcExperimentParams),
      (EncodeEntity.File, fileParams),
      (EncodeEntity.AnalysisStepRun, analysisStepRunParams),
      (EncodeEntity.AnalysisStepVersion, analysisStepVersionParams),
      (EncodeEntity.AnalysisStep, analysisStepParams)
    )
  }

  it should "write downloaded outputs to disk" in {
    readMsgs(outputDir, s"${EncodeEntity.Biosample}/*.json") shouldBe
      biosampleOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.Donor}/*.json") shouldBe
      donorOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.Library}/*.json") shouldBe
      libraryOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.Replicate}/*.json") shouldBe
      replicateOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.AntibodyLot}/*.json") shouldBe
      antibodyOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.Target}/*.json") shouldBe
      targetOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.Experiment}/*.json") shouldBe
      experimentOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.FunctionalCharacterizationExperiment}/*.json") shouldBe
      fcExperimentOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.File}/*.json") shouldBe
      fileOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStepRun}/*.json") shouldBe
      analysisStepRunOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStepVersion}/*.json") shouldBe
      analysisStepVersionOut.read[Array[Msg]]("@graph").toSet
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStep}/*.json") shouldBe
      analysisStepOut.read[Array[Msg]]("@graph").toSet
  }
}
