package org.broadinstitute.monster.encode.extraction

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.common.msg.{JsonParser, MsgOps}
import org.broadinstitute.monster.encode.EncodeEntity
import upack._

import scala.collection.mutable

object EncodeExtractionPipelineBuilderSpec {
  // biosamples
  val biosampleParams = ("organism.name", "human")

  val biosampleOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1"),
          Str("donor") -> Str("1"),
          Str("accession") -> Str("1")
        ): Msg
      )
    )
  )

  // donors
  val donorParams = ("@id", "1")

  val donorOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1")
        ): Msg
      )
    )
  )

  // libraries
  val libraryParams = ("biosample.accession", "1")

  val libraryOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1"),
          Str("accession") -> Str("1")
        ): Msg
      )
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
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1"),
          Str("targets") -> Str("1")
        ): Msg
      )
    )
  )

  // targets
  val targetParams = ("@id", "1")

  val targetOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1")
        ): Msg
      )
    )
  )

  // experiments
  val experimentParams = ("@id", "1")

  val experimentOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1")
        ): Msg
      )
    )
  )

  // fcexperiments
  val fcExperimentParams = ("@id", "/functional-characterization-experiments/2")

  val fcExperimentOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1")
        ): Msg
      )
    )
  )

  // files
  val fileParams = ("dataset", "1")

  val fileOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1"),
          Str("step_run") -> Str("1")
        ): Msg
      )
    )
  )

  // analysisStepRuns
  val analysisStepRunParams = ("@id", "1")

  val analysisStepRunOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1"),
          Str("analysis_step_version") -> Str("1")
        ): Msg
      )
    )
  )

  // analysisStepVersions
  val analysisStepVersionParams = ("@id", "1")

  val analysisStepVersionOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1"),
          Str("analysis_step") -> Str("1")
        ): Msg
      )
    )
  )

  // analysisStep
  val analysisStepParams = ("@id", "1")

  val analysisStepOut = Obj(
    Str("@graph") -> new Arr(
      mutable.ArrayBuffer(
        Obj(
          Str("@id") -> Str("1")
        ): Msg
      )
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

  it should "query and write Biosample data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Biosample, biosampleParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Biosample}/*.json") shouldBe
      biosampleOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write Donor data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Donor, donorParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Donor}/*.json") shouldBe
      donorOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write Library data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Library, libraryParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Library}/*.json") shouldBe
      libraryOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write Replicate data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Replicate, replicateParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Replicate}/*.json") shouldBe
      replicateOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write Antibody data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AntibodyLot, antibodyParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AntibodyLot}/*.json") shouldBe
      antibodyOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write Target data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Target, targetParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Target}/*.json") shouldBe
      targetOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write Experiment data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.Experiment, experimentParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.Experiment}/*.json") shouldBe
      experimentOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write FunctionalCharacterizationExperiment data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.FunctionalCharacterizationExperiment, fcExperimentParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.FunctionalCharacterizationExperiment}/*.json") shouldBe
      fcExperimentOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write File data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.File, fileParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.File}/*.json") shouldBe
      fileOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write AnalysisStepRun data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AnalysisStepRun, analysisStepRunParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStepRun}/*.json") shouldBe
      analysisStepRunOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write AnalysisStepVersion data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AnalysisStepVersion, analysisStepVersionParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStepVersion}/*.json") shouldBe
      analysisStepVersionOut.read[Array[Msg]]("@graph").toSet
  }

  it should "query and write AnalysisStep data as expected" in {
    mockClient.recordedRequests.toSet should contain(
      (EncodeEntity.AnalysisStep, analysisStepParams)
    )
    readMsgs(outputDir, s"${EncodeEntity.AnalysisStep}/*.json") shouldBe
      analysisStepOut.read[Array[Msg]]("@graph").toSet
  }

  it should "Distinguish between normal experiments and functional-characterization-experiments" in {
    //functional-characterization-experiments case
    ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(fcExample) shouldBe true
    //normal experiments case
    ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(expExample) shouldBe false
  }
}
