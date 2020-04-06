package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime
import org.broadinstitute.monster.encode.jadeschema.table.Antibody
import upack.Msg

/** Transformation logic for ENCODE antibody objects. */
object AntibodyTransformations {

  /** Transform a raw ENCODE antibody into our preferred schema. */
  def transformAntibody(antibodyInput: Msg): Antibody = {
    import org.broadinstitute.monster.common.msg.MsgOps

    // Use regular expressions to remove everything but the actual target name. Remove any non-human targets.
    val mappedTargets = antibodyInput
      .tryRead[Array[String]]("targets")
      .getOrElse(Array.empty)
      .map(CommonTransformations.transformId)
      .flatMap { target =>
        val (front, back) = target.splitAt(target.lastIndexOf('-'))
        if (back == "-human") Some(front)
        else None
      }

    Antibody(
      id = CommonTransformations.readId(antibodyInput),
      crossReferences = antibodyInput.read[Array[String]]("dbxrefs"),
      timeCreated = antibodyInput.read[OffsetDateTime]("date_created"),
      source = antibodyInput.read[String]("source"),
      clonality = antibodyInput.tryRead[String]("clonality"),
      hostOrganism = antibodyInput.read[String]("host_organism"),
      targets = mappedTargets,
      award = antibodyInput.read[String]("award"),
      isotype = antibodyInput.tryRead[String]("isotype"),
      lab = antibodyInput.read[String]("lab"),
      lotId = antibodyInput.tryRead[String]("lot_id"),
      productId = antibodyInput.read[String]("product_id"),
      purificationMethods = antibodyInput.read[Array[String]]("purifications")
    )
  }
}
