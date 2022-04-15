package org.broadinstitute.monster.encode.transformation

import java.time.{LocalDate, OffsetDateTime}

import org.broadinstitute.monster.encode.jadeschema.table.Experiment
import upack.Msg

object ExperimentTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw experiment into our preferred schema for experiments. */
  def transformExperiment(
    rawExperiment: Msg
  ): Experiment = {

    Experiment(
      id = CommonTransformations.readId(rawExperiment),
      crossReferences = rawExperiment.read[List[String]]("dbxrefs"),
      timeCreated = rawExperiment.read[OffsetDateTime]("date_created"),
      dateSubmitted = rawExperiment.tryRead[LocalDate]("date_submitted"),
      description = rawExperiment.tryRead[String]("description"),
      dataModality = rawExperiment.tryRead[String]("assay_term_name") match {
        case Some(str) => Some(AssayTransformations.transformAssayTermToDataModality(str))
        case None      => None
      },
      award = rawExperiment.read[String]("award"),
      lab = rawExperiment.read[String]("lab"),
      submittedBy = rawExperiment.read[String]("submitted_by"),
      status = rawExperiment.read[String]("status")
    )
  }
}
