package org.broadinstitute.monster.encode

import com.spotify.scio.coders.Coder
import enumeratum.{Enum, EnumEntry}

package object transformation {

  /** (De)serializer for enumeratum values. */
  implicit def enumCoder[E <: EnumEntry](implicit E: Enum[E]): Coder[E] =
    Coder.xmap(Coder.stringCoder)(
      E.namesToValuesMap(_),
      _.entryName
    )
}
