package org.broadinstitute.monster.encode.transformation

import enumeratum.{Enum, EnumEntry}

/** Category of file stored by ENCODE. */
sealed trait FileType extends EnumEntry with Product with Serializable

private object FileType extends Enum[FileType] {
  override val values = findValues

  case object Sequence extends FileType
  case object Alignment extends FileType
  case object Other extends FileType
}
