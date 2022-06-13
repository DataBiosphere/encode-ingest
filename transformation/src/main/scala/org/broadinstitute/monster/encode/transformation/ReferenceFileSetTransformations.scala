package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.encode.jadeschema.table.Referencefileset
import upack.Msg

/** Transformation logic for ENCODE donor objects. */
object ReferenceFileSetTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Transform a raw ENCODE donor into our preferred schema. */
  def transformReferenceFileSet(referencesInput: Msg, organism: Option[Msg]): Referencefileset = {
    val id = CommonTransformations.readId(referencesInput)

    Referencefileset(
      referencefilesetId = id,
      label = id,
      xref = CommonTransformations.convertToEncodeUrl(
        referencesInput.read[String]("@id")
      ) :: referencesInput
        .tryRead[List[String]]("dbxrefs")
        .getOrElse(List.empty[String]),
      referenceAssembly =
        referencesInput.tryRead[List[String]]("reference_assembly").getOrElse(List()),
      description = referencesInput.tryRead[String]("description"),
      document = referencesInput.tryRead[List[String]]("document").getOrElse(List()),
      doid = referencesInput.tryRead[String]("diod"),
      organism = organism
        .map(msg => msg.read[String]("scientific_name")),
      elementsSelectionMethod =
        referencesInput.tryRead[List[String]]("elements_selection_method").getOrElse(List()),
      examinedLoci = referencesInput.read[List[String]]("examined_loci"),
      examinedRegion = referencesInput.read[List[String]]("examined_region"),
      lab = CommonTransformations.convertToEncodeUrl(referencesInput.tryRead[String]("lab")),
      referenceType = referencesInput.tryRead[String]("reference_type"),
      references = referencesInput.read[List[String]]("references"),
      softwareUsed = referencesInput.read[List[String]]("software_used"),
      donorId = referencesInput
        .tryRead[List[String]]("donor")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId),
      generatedFileId = referencesInput
        .tryRead[List[String]]("files")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId),
      derivedFromFileId = referencesInput
        .tryRead[List[String]]("derived_from_")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId),
      originalFileId = referencesInput
        .tryRead[List[String]]("original_files")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId),
      relatedFileId = referencesInput
        .tryRead[List[String]]("related_files")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId),
      relatedPipelineId = referencesInput
        .tryRead[List[String]]("related_pipelines")
        .getOrElse(List.empty[String])
        .map(CommonTransformations.transformId)
    )
  }
}
