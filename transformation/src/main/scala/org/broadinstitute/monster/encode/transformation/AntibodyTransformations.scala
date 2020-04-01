package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime
import org.broadinstitute.monster.encode.jadeschema.table.Antibody
import org.slf4j.LoggerFactory
import upack.Msg

/** Transformation logic for ENCODE antibody objects. */
object AntibodyTransformations {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Transform a raw ENCODE antibody into our preferred schema. */
  def transformAntibody(antibodyInput: Msg): Antibody = {
    import org.broadinstitute.monster.common.msg.MsgOps

    // Use regular expressions to remove everything but the actual target name, which should be the same across
    // all targets in the list. Remove "synthetic_tag" type targets.
    val mappedTargets = antibodyInput
      .read[Array[String]]("targets")
      .map(CommonTransformations.transformId)
      .flatMap { target =>
        val (front, back) = target.splitAt(target.lastIndexOf('-'))
        if (back == "-synthetic_tag") None
        else Some(front)
      }
      .toSet[String]

    // check that all other target values match the first target value
    val id = CommonTransformations.readId(antibodyInput)
    if (mappedTargets.size > 1) {
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
      target = mappedTargets.headOption,
      award = antibodyInput.read[String]("award"),
      isotype = antibodyInput.tryRead[String]("isotype"),
      lab = antibodyInput.read[String]("lab"),
      lotId = antibodyInput.read[String]("lot_id"),
      productId = antibodyInput.read[String]("product_id"),
      purificationMethods = antibodyInput.read[Array[String]]("purifications")
    )
  }
}
