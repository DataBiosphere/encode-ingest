package org.broadinstitute.monster.encode.transformation

import java.time.OffsetDateTime
import org.broadinstitute.monster.encode.jadeschema.table.Antibody
import upack.Msg

/** Transformation logic for ENCODE antibody objects. */
object AntibodyTransformations {

  /** Transform a raw ENCODE antibody into our preferred schema. */
  def transformAntibody(antibodyInput: Msg, joinedTargets: Iterable[Msg]): Antibody = {
    import org.broadinstitute.monster.common.msg.MsgOps

    val id = CommonTransformations.readId(antibodyInput)
    val targetName = joinedTargets
      .filter(_.tryRead[String]("organism").contains("/organisms/human/"))
      .map(_.read[String]("label"))
      .headOption

    Antibody(
      antibodyId = CommonTransformations.readId(antibodyInput),
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        antibodyInput.read[String]("@id")
      ) :: antibodyInput.tryRead[List[String]]("dbxrefs").getOrElse(List.empty[String]),
      dateCreated = antibodyInput.read[OffsetDateTime]("date_created"),
      source = CommonTransformations.convertToEncodeUrl(antibodyInput.read[String]("source")),
      clonality = antibodyInput.tryRead[String]("clonality"),
      hostOrganism =
        CommonTransformations.convertToEncodeUrl(antibodyInput.read[String]("host_organism")),
      target = targetName,
      award = CommonTransformations.convertToEncodeUrl(antibodyInput.read[String]("award")),
      isotype = antibodyInput.tryRead[String]("isotype"),
      lab = CommonTransformations.convertToEncodeUrl(antibodyInput.read[String]("lab")),
      lot = antibodyInput.tryRead[String]("lot_id"),
      partNumber = antibodyInput.read[String]("product_id"),
      purificationMethods =
        antibodyInput.tryRead[List[String]]("purifications").getOrElse(List.empty[String])
    )
  }
}
