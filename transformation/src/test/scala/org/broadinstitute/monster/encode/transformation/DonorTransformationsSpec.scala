package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.common.msg.JsonParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upack.Msg

class DonorTransformationsSpec extends AnyFlatSpec with Matchers {
  it should "parse a well formed donor record" in {
    val inputJson =
      """{
        |"@id":"/human-donors/ABC123/",
        |"accession": "ABC123",
        |"dbxrefs": [],
        |"date_created": "2020-12-24T18:00:00.111222+00:00",
        |"organism": "/organisms/human",
        |"award": "/awards/xyz_award",
        |"lab": "/labs/example-lab",
        |"parents": [],
        |"submitted_by": "/users/123-abc",
        |"age": "30-33"
        |}
        |""".stripMargin
    val inputMsg: Msg = JsonParser.parseEncodedJson(inputJson)

    val output = DonorTransformations.transformDonor(inputMsg)

    output.id shouldBe "ABC123"
    output.award shouldBe "/awards/xyz_award"
    output.submittedBy shouldBe "/users/123-abc"
    output.lab shouldBe "/labs/example-lab"
    output.ageMax shouldBe Some(33)
    output.ageMin shouldBe Some(30)
    output.ethnicity shouldBe List.empty[String]
  }

  it should "parse ages 90 or above" in {
    val inputJson =
      """{
        |"@id":"/human-donors/ABC123/",
        |"accession": "ABC123",
        |"dbxrefs": [],
        |"date_created": "2020-12-24T18:00:00.111222+00:00",
        |"organism": "/organisms/human",
        |"award": "/awards/xyz_award",
        |"ethnicity": ["ethn1", "ethn2"],
        |"lab": "/labs/example-lab",
        |"parents": [],
        |"submitted_by": "/users/123-abc",
        |"age": "90 or above"
        |}
        |""".stripMargin
    val inputMsg: Msg = JsonParser.parseEncodedJson(inputJson)

    val output = DonorTransformations.transformDonor(inputMsg)

    output.ageMin shouldBe Some(90)
    output.ageMax shouldBe None
    output.ethnicity shouldBe List("ethn1", "ethn2")
  }
}
