package org.broadinstitute.monster.encode.extraction

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.encode.EncodeEntity
import org.broadinstitute.monster.common.msg.JsonParser
import upack._

object ExtractionPipelineBuilderSpec {
  // biosamples
  val biosampleParams = ("organism.name", "human")

  val biosampleOut = Obj(
    Str("@id") -> Str("1"),
    Str("donor") -> Str("1"),
    Str("accession") -> Str("1")
  )

  // donors
  val donorParams = ("@id", "1")
  val donorOut = Obj(Str("@id") -> Str("1"))

  // libraries
  val libraryParams = ("biosample.accession", "1")

  val libraryOut = Obj(
    Str("@id") -> Str("1"),
    Str("accession") -> Str("1")
  )

  // replicates
  val replicateParams = ("library.accession", "1")
  val replicateIds = 1 to 2

  val replicateOut = replicateIds.map { i =>
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
    )
  }.toList

  // antibodies
  val antibodyParams = ("@id", "1")

  val antibodyOut = Obj(
    Str("@id") -> Str("1"),
    Str("targets") -> Str("1")
  )

  // targets
  val targetParams = ("@id", "1")
  val targetOut = Obj(Str("@id") -> Str("1"))

  // experiments
  val experimentParams = ("@id", "1")
  val experimentOut = Obj(Str("files") -> Arr(Str("1"), Str("3")))

  // fcexperiments
  val fcExperimentParams = ("@id", "/functional-characterization-experiments/2")
  val fcExperimentOut = Obj(Str("files") -> Arr(Str("2")))

  // files
  val fileIds = 1 to 3

  val fileRequestResponse = fileIds.map { i =>
    (EncodeEntity.File, ("@id", i.toString)) -> Obj(
      Str("@id") -> Str(i.toString),
      Str("step_run") -> Str("1")
    )
  }.toMap[(EncodeEntity, (String, String)), Msg]

  // analysisStepRuns
  val analysisStepRunParams = ("@id", "1")

  val analysisStepRunOut = Obj(
    Str("@id") -> Str("1"),
    Str("analysis_step_version") -> Str("1")
  )

  // analysisStepVersions
  val analysisStepVersionParams = ("@id", "1")

  val analysisStepVersionOut = Obj(
    Str("@id") -> Str("1"),
    Str("analysis_step") -> Str("1")
  )

  // analysisStep
  val analysisStepParams = ("@id", "1")
  val analysisStepOut = Obj(Str("@id") -> Str("1"))

  val responseMap = fileRequestResponse.mapValues(List(_)) ++ Map(
    (EncodeEntity.Biosample, biosampleParams) -> List(biosampleOut),
    (EncodeEntity.Donor, donorParams) -> List(donorOut),
    (EncodeEntity.Library, libraryParams) -> List(libraryOut),
    (EncodeEntity.Replicate, replicateParams) -> replicateOut,
    (EncodeEntity.AntibodyLot, antibodyParams) -> List(antibodyOut),
    (EncodeEntity.Target, targetParams) -> List(targetOut),
    (EncodeEntity.Experiment, experimentParams) -> List(experimentOut),
    (EncodeEntity.FunctionalCharacterizationExperiment, fcExperimentParams) -> List(
      fcExperimentOut
    ),
    (EncodeEntity.AnalysisStepRun, analysisStepRunParams) -> List(analysisStepRunOut),
    (EncodeEntity.AnalysisStepVersion, analysisStepVersionParams) -> List(
      analysisStepVersionOut
    ),
    (EncodeEntity.AnalysisStep, analysisStepParams) -> List(analysisStepOut)
  )

  val mockClient = new MockEncodeClient(responseMap)

  // examples to test isFunctionalCharacterizationReplicate
  val fcExample =
    JsonParser.parseEncodedJson(
      """{
        "@id": "/replicates/d80982c0-ae21-4e83-b3c9-fc7ec356c435/",
        "@type": [
          "Replicate",
          "Item"
        ],
        "aliases": [
          "tim-reddy:ggr-starrseq-input-rep11-rep"
        ],
        "biological_replicate_number": 11,
        "date_created": "2019-11-07T21:22:51.002163+00:00",
        "experiment": "/functional-characterization-experiments/ENCSR652MCH/",
        "libraries": [
           "ed595bdb-bab5-4b3b-acbf-221a131f4b4c"
        ],
        "library": "/libraries/ENCLB079TYN/",
        "schema_version": "9",
        "status": "released",
        "submitted_by": "/users/a37a6376-c5a2-4fe0-b1af-7f078eb997c9/",
        "technical_replicate_number": 1,
        "uuid": "d80982c0-ae21-4e83-b3c9-fc7ec356c435"
        }"""
    )

  val expExample =
    JsonParser.parseEncodedJson(
      """{
      "@id": "/replicates/3c996486-0a1b-466c-be00-6d7cd7a89581/",
      "@type": [
        "Replicate",
        "Item"
      ],
      "aliases": [],
      "biological_replicate_number": 1,
      "date_created": "2019-11-21T23:02:31.637903+00:00",
      "experiment": "/experiments/ENCSR426KOP/",
      "libraries": [
        "f88d7865-b60b-418f-a0b1-0bf3fabc3c95"
      ],
      "library": "/libraries/ENCLB355PRN/",
      "schema_version": "9",
      "status": "released",
      "submitted_by": "/users/bc5b62f7-ce28-4a1e-b6b3-81c9c5a86d7a/",
      "technical_replicate_number": 1,
      "uuid": "3c996486-0a1b-466c-be00-6d7cd7a89581"
      }"""
    )
}

class ExtractionPipelineBuilderSpec extends PipelineBuilderSpec[Args] {
  import ExtractionPipelineBuilderSpec._

  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  override val testArgs = Args(
    outputDir = outputDir.pathAsString,
    batchSize = 2L
  )

  override val builder =
    new ExtractionPipelineBuilder(getClient = () => mockClient)

  behavior of "ExtractionPipelineBuilder"

  it should "query and write Biosample data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Biosample, biosampleParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Biosample}/*.json") shouldBe Set(biosampleOut)
  }

  it should "query and write Donor data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Donor, donorParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Donor}/*.json") shouldBe Set(donorOut)
  }

  it should "query and write Library data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Library, libraryParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Library}/*.json") shouldBe Set(libraryOut)
  }

  it should "query and write Replicate data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Replicate, replicateParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Replicate}/*.json") shouldBe replicateOut.toSet
  }

  it should "query and write Antibody data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AntibodyLot, antibodyParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AntibodyLot}/*.json") shouldBe Set(antibodyOut)
  }

  it should "query and write Target data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Target, targetParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Target}/*.json") shouldBe Set(targetOut)
  }

  it should "query and write Experiment data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Experiment, experimentParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Experiment}/*.json") shouldBe Set(experimentOut)
  }

  it should "query and write FunctionalCharacterizationExperiment data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.FunctionalCharacterizationExperiment, fcExperimentParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.FunctionalCharacterizationExperiment}/*.json") shouldBe
      Set(fcExperimentOut)
  }

  it should "query and write File data as expected" in {
    mockClient.recordedRequests.toSet should contain allElementsOf fileRequestResponse.keys
    readMsgs(outputDir, s"${EncodeEntity.File}/*.json") shouldBe fileRequestResponse.values.toSet
  }

  it should "query and write AnalysisStepRun data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AnalysisStepRun, analysisStepRunParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStepRun}/*.json") shouldBe Set(
      analysisStepRunOut
    )
  }

  it should "query and write AnalysisStepVersion data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AnalysisStepVersion, analysisStepVersionParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStepVersion}/*.json") shouldBe
      Set(analysisStepVersionOut)
  }

  it should "query and write AnalysisStep data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AnalysisStep, analysisStepParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStep}/*.json") shouldBe
      Set(analysisStepOut)
  }

  it should "Distinguish between normal experiments and functional-characterization-experiments" in {
    //functional-characterization-experiments case
    ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(fcExample) shouldBe true
    //normal experiments case
    ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(expExample) shouldBe false
  }
}
