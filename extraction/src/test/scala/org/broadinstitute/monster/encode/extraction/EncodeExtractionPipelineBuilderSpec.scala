package org.broadinstitute.monster.encode.extraction

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import upack._

import scala.collection.mutable

object EncodeExtractionPipelineBuilderSpec {
  val fakeIds = 1 to 10

  val initParams = List(
    ("frame", "object"),
    ("status", "released"),
    ("limit", "all"),
    ("format", "json")
  )

  val biosampleParams = initParams ++ List(("type", EncodeEntity.Biosample.entryName))

  val biosampleOut = Obj(
    Str("@graph") -> new Arr(
      fakeIds.map { i =>
        Obj(
          Str("@id") -> Str(i.toString)
        ): Msg
      }.to[mutable.ArrayBuffer]
    )
  )

//  val responseMap = Map(biosampleParams -> biosampleOut.map(Arr(_) : Msg).to[mutable.ArrayBuffer])
  val responseMap = Map(
    biosampleParams.toSet -> biosampleOut
  )

  val mockClient = new MockEncodeClient(responseMap)
}

class EncodeExtractionPipelineBuilderSpec extends PipelineBuilderSpec[Args] {
  import EncodeExtractionPipelineBuilderSpec._

  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  override val testArgs = Args(
    outputDir = outputDir.pathAsString,
    batchSize = 1L
  )

  override val builder =
    new ExtractionPipelineBuilder(getClient = () => mockClient)

  behavior of "EncodeExtractionPipelineBuilder"

  it should "query ENCODE in the expected way" in {
    ???
  }

//  it should "write downloaded outputs to disk" in {
//    readMsgs(outputDir) shouldBe expectedOut.toSet
//  }
}
