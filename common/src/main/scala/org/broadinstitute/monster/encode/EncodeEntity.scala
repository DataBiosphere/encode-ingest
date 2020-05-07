package org.broadinstitute.monster.encode

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
  * An entity type found in ENCODE that we also want
  * (with modifications) in our data model.
  */
sealed trait EncodeEntity extends EnumEntry

object EncodeEntity extends Enum[EncodeEntity] {
  case object Biosample extends EncodeEntity

  case object BiosampleType extends EncodeEntity

  case object Donor extends EncodeEntity

  case object Experiment extends EncodeEntity

  case object FunctionalCharacterizationExperiment extends EncodeEntity

  case object File extends EncodeEntity

  case object Library extends EncodeEntity

  case object Replicate extends EncodeEntity

  case object AntibodyLot extends EncodeEntity

  case object Target extends EncodeEntity

  case object Audit extends EncodeEntity

  case object AnalysisStep extends EncodeEntity

  case object AnalysisStepVersion extends EncodeEntity

  case object AnalysisStepRun extends EncodeEntity

  case object Pipeline extends EncodeEntity

  override def values: immutable.IndexedSeq[EncodeEntity] = findValues
}
