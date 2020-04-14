package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime
import org.broadinstitute.monster.encode.jadeschema.table.Antibody
import upack.Msg

/** Transformation logic for ENCODE antibody objects. */
object AntibodyTransformations {

  /** Transform a raw ENCODE antibody into our preferred schema. */
  def transformAntibody(antibodyInput: Msg, joinedTargets: Iterable[Msg]): Antibody = {
    import org.broadinstitute.monster.common.msg.MsgOps

    val targetNames = joinedTargets
      .filter(_.read[String]("organism") == "/organisms/human/")
      .map(_.read[String]("label"))
      .toArray
      .sorted
      .distinct

    Antibody(
      id = CommonTransformations.readId(antibodyInput),
      crossReferences = antibodyInput.read[Array[String]]("dbxrefs"),
      timeCreated = antibodyInput.read[OffsetDateTime]("date_created"),
      source = antibodyInput.read[String]("source"),
      clonality = antibodyInput.tryRead[String]("clonality"),
      hostOrganism = antibodyInput.read[String]("host_organism"),
      targets = targetNames,
      award = antibodyInput.read[String]("award"),
      isotype = antibodyInput.tryRead[String]("isotype"),
      lab = antibodyInput.read[String]("lab"),
      lotId = antibodyInput.tryRead[String]("lot_id"),
      productId = antibodyInput.read[String]("product_id"),
      purificationMethods = antibodyInput.read[Array[String]]("purifications")
    )
  }
}
