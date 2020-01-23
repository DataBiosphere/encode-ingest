package org.broadinstitute.monster.etl.encode

import enumeratum.{Enum, EnumEntry}

/**
  * An entity type found in ENCODE that we also want
  * (with modifications) in our data model.
  */
sealed trait EncodeEntity extends EnumEntry {

  /**
    * Fields from the original ENCODE JSON objects of the
    * entity type which should be retained.
    *
    * Assumption: trimming happens before renaming in the ETL workflow,
    * so these strings should match the ENCODE names.
    */
  def fieldsToKeep: Set[String] = Set(
    "@id",
    "accession",
    "aliases",
    "award",
    "date_created",
    "dbxrefs",
    "lab",
    "status",
    "submitted_by"
  )

  /**
    * `old` -> `new` mappings for fields from ENCODE which
    * should be renamed.
    */
  def fieldsToRename: List[(String, String)] = List(
    "@id" -> "label",
    "accession" -> "close_match",
    "award" -> "sponsor",
    "date_created" -> "created_at"
  )

  /**
    * ENCODE fields which should be turned into links back to the ENCODE site.
    *
    * Assumption: linking happens after renaming, so these strings should match our
    * data model's names.
    */
  def linkFields: Set[String] = Set("close_match", "sponsor", "submitted_by")

  /**
    * ENCODE fields of the form '/<entity-type>/<entity-label>/' which should be
    * transformed into just '<entity-label>'.
    *
    * Assumption: labeling happens after renaming, so these strings should match our
    * data model's names.
    */
  def labelFields: Set[String] = Set("label", "lab")

  /**
    * Fields which should be collapsed into a single "aliases" array.
    *
    * Assumption: collapsing happens after renaming, so these strings should match
    * our data model's names.
    */
  def aliasFields: Set[String] = Set("aliases", "dbxrefs")

  /**
    * String ID for the entity type that will be queried
    */
  def encodeApiName: String
}

object EncodeEntity extends Enum[EncodeEntity] {
  override val values = findValues

  case object Biosample extends EncodeEntity {

    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "biosample_ontology",
        "cell_isolation_method",
        "date_obtained",
        "source"
      )
    )

    override def fieldsToRename: List[(String, String)] =
      ("biosample_ontology" -> "biosample_type_id") :: super.fieldsToRename

    override def labelFields: Set[String] = super.labelFields.union(
      Set(
        "biosample_type_id",
        "source"
      )
    )

    override def encodeApiName: String = "Biosample"
  }

  case object Donor extends EncodeEntity {

    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "age_units",
        "age",
        "ethnicity",
        "external_ids",
        "health_status",
        "life_stage",
        "organism",
        "sex"
      )
    )

    override def fieldsToRename: List[(String, String)] = super.fieldsToRename ::: List(
      "health_status" -> "phenotype",
      "organism" -> "organism_id"
    )

    override def aliasFields: Set[String] = super.aliasFields + "external_ids"

    override def labelFields: Set[String] = super.labelFields + "organism_id"

    override def encodeApiName: String = "HumanDonor"
  }

  case object Experiment extends EncodeEntity {

    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "date_released",
        "date_submitted",
        "description",
        "target"
      )
    )

    override def linkFields: Set[String] = super.linkFields + "target"

    override def encodeApiName = "Experiment"
  }

  case object File extends EncodeEntity {

    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "file_format",
        "file_format_type",
        "file_size",
        "md5sum",
        "notes",
        "output_type",
        "paired_end",
        "platform",
        "read_count",
        "read_length",
        "run_type",
        "s3_uri",
        "status"
      )
    )

    override def fieldsToRename: List[(String, String)] = super.fieldsToRename ::: List(
      "file_format" -> "format",
      "file_format_type" -> "file_sub_type",
      "paired_end" -> "paired_end_identifier",
      "platform" -> "platform_id",
      "run_type" -> "paired_end",
      "s3_uri" -> "file_path"
    )

    override def aliasFields: Set[String] = super.aliasFields + "external_ids"

    override def labelFields: Set[String] = super.labelFields + "platform_id"

    override def encodeApiName = "File"
  }

  case object Library extends EncodeEntity {
    override def fieldsToKeep: Set[String] = super.fieldsToKeep + "strand_specificity"

    override def fieldsToRename: List[(String, String)] =
      ("strand_specificity" -> "strand_specific") :: super.fieldsToRename

    override def encodeApiName = "Library"
  }

  case object Replicate extends EncodeEntity {
    override def fieldsToKeep: Set[String] = ???

    override def fieldsToRename: List[(String, String)] = ???

    override def linkFields: Set[String] = ???

    override def labelFields: Set[String] = ???

    override def aliasFields: Set[String] = ???

    override def encodeApiName = "Replicate"
  }

  case object Audit extends EncodeEntity {
    override def fieldsToKeep: Set[String] = ???

    override def fieldsToRename: List[(String, String)] = ???

    override def linkFields: Set[String] = ???

    override def labelFields: Set[String] = ???

    override def aliasFields: Set[String] = ???

    override def encodeApiName = "File"
  }
}
