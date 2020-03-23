package org.broadinstitute.monster.encode

import java.time.OffsetDateTime

import com.spotify.scio.coders.Coder
import enumeratum.{Enum, EnumEntry}

package object transformation {

  /** (De)serializer for enumeratum values. */
  implicit def enumCoder[E <: EnumEntry](implicit E: Enum[E]): Coder[E] =
    Coder.xmap(Coder.stringCoder)(
      E.namesToValuesMap(_),
      _.entryName
    )

  /** (De)serializer for the ODTs we extract from raw data. */
  implicit val odtCoder: Coder[OffsetDateTime] = Coder.xmap(Coder.stringCoder)(
    OffsetDateTime.parse(_),
    _.toString
  )
}
