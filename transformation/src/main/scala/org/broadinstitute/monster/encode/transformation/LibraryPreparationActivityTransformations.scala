package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.Librarypreparationactivity
import upack.Msg

import java.time.OffsetDateTime

/** Transformation logic for ENCODE library objects. */
object LibraryPreparationActivityTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE library preparation activity into our preferred schema. */
  def transformLibraryPreparationActivity(libraryInput: Msg): Librarypreparationactivity = {
    val id = CommonTransformations.readId(libraryInput)

    Librarypreparationactivity(
      id = id,
      label = id,
      dateCreated = libraryInput.read[OffsetDateTime]("date_created"),
      lab = CommonTransformations.convertToEncodeUrl(libraryInput.tryRead[String]("lab")),
      generated = Some(id),
      usesSample =
        CommonTransformations.transformId(libraryInput.read[String]("biosample")) :: List()
    )
  }

}
