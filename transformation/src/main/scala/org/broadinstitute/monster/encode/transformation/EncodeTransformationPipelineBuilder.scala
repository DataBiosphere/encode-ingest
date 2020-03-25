package org.broadinstitute.monster.encode.transformation

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder

import com.spotify.scio.values.SCollection
import java.time.OffsetDateTime
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.encode.jadeschema.table._
import org.slf4j.LoggerFactory
import org.broadinstitute.monster.encode.EncodeEntity
import upack.Msg

object EncodeTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /** (De)serializer for the upack messages we read from storage. */
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /** Output category for sequencing files. */
  val SequencingCategory = "raw data"

  /** Output category for alignment files. */
  val AlignmentCategory = "alignment"

  val EncodeIdPattern = "/[^/]+/([^/]+)/".r

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * Schedule all the steps for the Encode transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    def readRawEntities(entityType: EncodeEntity): SCollection[Msg] = {
      val name = entityType.entryName

      StorageIO
        .readJsonLists(ctx, name, s"${args.inputPrefix}/$name/*.json")
        .withName(s"Strip unknown values from '$name' objects")
        .map(CommonTransformations.removeUnknowns)
    }

    // read in extracted info
    val donorInputs = readRawEntities(EncodeEntity.Donor)
    val antibodyInputs = readRawEntities(EncodeEntity.Antibody)
    val fileInputs = readRawEntities(EncodeEntity.File)

    val donorOutput = donorInputs
      .withName("Transform Donor objects")
      .map(DonorTransformations.transformDonor)
    val antibodyOutput = antibodyInputs.map(transformAntibody)

    // write back to storage
    StorageIO.writeJsonLists(donorOutput, "Donors", s"${args.outputPrefix}/donor")
    StorageIO.writeJsonLists(
      antibodyOutput,
      "Antibodies",
      s"${args.outputPrefix}/antibody"
    )

    // Split the file stream based on category.
    val Seq(sequencingFiles, alignmentFiles, otherFiles) =
      fileInputs.partition(
        3,
        rawFile => {
          val category = rawFile.read[String]("output_category")
          if (category == SequencingCategory) 0
          else if (category == AlignmentCategory) 1
          else 2
        }
      )
    // FIXME: We should ultimately write out mapped files.
    StorageIO.writeJsonLists(
      sequencingFiles,
      "Sequencing Files",
      s"${args.outputPrefix}/sequencing_file"
    )
    StorageIO.writeJsonLists(
      alignmentFiles,
      "Alignment Files",
      s"${args.outputPrefix}/alignment_file"
    )
    StorageIO.writeJsonLists(
      otherFiles,
      "Other Files",
      s"${args.outputPrefix}/other_file"
    )
    ()
  }

  def transformAntibody(antibodyInput: Msg): Antibody = {
    val id = antibodyInput.read[String]("accession")
    val targets = antibodyInput
      .read[Array[String]]("targets")

    // Use regular expressions to remove everything but the actual target name, which should be the same across
    // all targets in the list. Remove "synthetic_tag" type targets.
    val mappedTargets = targets.collect {
      case EncodeIdPattern(target) => target
    } // filter out non-matches
    .flatMap { target =>
      val (front, back) = target.splitAt(target.lastIndexOf('-'))
      if (back == "-synthetic_tag") None
      else Some(front)
    }
    val firstMappedTarget = mappedTargets.headOption

    // check that all other target values match the first target value
    if (!mappedTargets.forall(target => target == firstMappedTarget)) {
      logger.warn(
        s"Antibody '$id' contains multiple target types in [${mappedTargets.mkString(",")}]."
      )
    }

    Antibody(
      id = id,
      crossReferences = antibodyInput.read[Array[String]]("dbxrefs"),
      timeCreated = antibodyInput.read[OffsetDateTime]("date_created"),
      source = antibodyInput.read[String]("source"),
      clonality = antibodyInput.read[String]("clonality"),
      hostOrganism = antibodyInput.read[String]("host_organism"),
      target = firstMappedTarget,
      award = antibodyInput.read[String]("award"),
      isotype = antibodyInput.tryRead[String]("isotype"),
      lab = antibodyInput.read[String]("lab"),
      lotId = antibodyInput.read[String]("lot_id"),
      productId = antibodyInput.read[String]("product_id"),
      purificationMethods = antibodyInput.read[Array[String]]("purifications")
    )
  }
}
