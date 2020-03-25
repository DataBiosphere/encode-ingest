package org.broadinstitute.monster.encode.transformation

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec

class TransformationPipelineBuilderSpec extends PipelineBuilderSpec[Args] {
  behavior of "TransformationPipelineBuilder"

  private val testFileLocation = s"${File.currentWorkingDirectory}/src/test/test-files"
  private val truthDir = File.currentWorkingDirectory / "src" / "test" / "test-files" / "outputs"
  private val compareDir = File.currentWorkingDirectory / "src" / "test" / "test-files" / "outputs-to-compare"
  private val compareDirString = compareDir.pathAsString
  private val inputDirString = s"$testFileLocation/inputs"

  override val testArgs =
    Args(inputPrefix = inputDirString, outputPrefix = compareDirString)

  override def afterAll(): Unit = {
    compareDir.delete()
    ()
  }

  override val builder = TransformationPipelineBuilder

  /**
    *
    * Helper method to call the parsing method on the truth-files and the files-to-test.
    *
    * @param subDir The sub-directory of the outputs dir containing the files to read
    * @return A tuple of Set of Json, where the first one is the Set-to-test and the second one is the truth-Set
    */
  private def compareTruthAndCompSets(subDir: String): Unit = {
    it should s"have written the correct $subDir data" in {
      val expected = readMsgs(truthDir / subDir)
      val actual = readMsgs(compareDir / subDir)
      actual should contain theSameElementsAs expected
    }
  }

  private val outputDirs = Set(
    "donor",
    "alignment_file",
    "other_file",
    "sequence_file"
  )

  outputDirs.foreach {
    it should behave like compareTruthAndCompSets(_)
  }
}
