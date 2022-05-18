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

    output.donorId shouldBe "ABC123"
    output.award shouldBe "https://www.encodeproject.org/awards/xyz_award"
    output.submittedBy shouldBe "https://www.encodeproject.org/users/123-abc"
    output.lab shouldBe "https://www.encodeproject.org/labs/example-lab"
    output.ageAgeUpperbound shouldBe Some(33)
    output.ageAgeLowerbound shouldBe Some(30)
    output.reportedEthnicity shouldBe List.empty[String]
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

    output.ageAgeLowerbound shouldBe Some(90)
    output.ageAgeUpperbound shouldBe None
    output.reportedEthnicity shouldBe List("ethn1", "ethn2")
  }
}
