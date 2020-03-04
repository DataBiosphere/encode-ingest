package org.broadinstitute.monster.encode.extraction

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.monster.common.msg.JsonParser

class ExtractionPipelineSpec extends AnyFlatSpec with Matchers {
  behavior of "ExtractionPipeline"

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

  it should "Distinguish between normal experiments and functional-characterization-experiments" in {
    //functional-characterization-experiments case
    ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(fcExample) shouldBe true
    //normal experiments case
    ExtractionPipelineBuilder.isFunctionalCharacterizationReplicate(expExample) shouldBe false
  }
}
