package org.broadinstitute.monster.encode.transformation

import upack.{Msg, Obj, Str}

import scala.collection.mutable

/** Transformation logic that applies across multiple ENCODE objects. */
object CommonTransformations {
  import org.broadinstitute.monster.common.msg.MsgOps

  /** Message value used by ENCODE in place of null. */
  private val UnknownValue: Msg = Str("unknown")

  /**
    * Process an arbitrary ENCODE object to strip out all
    * values representing null / absence of data.
    */
  def removeUnknowns(rawObject: Msg): Msg = {
    val cleaned = new mutable.LinkedHashMap[Msg, Msg]()
    rawObject.obj.foreach {
      case (_, UnknownValue) => ()
      case (k, v)            => cleaned += k -> v
    }
    new Obj(cleaned)
  }

  /**
    * Regex matching raw ENCODE @id values, capturing the accession /
    * other ID value and dropping the entity-type prefix.
    */
  private val IdPattern = "/[^/]+/([^/]+)/".r("accession")

  /**
    * Transform a raw ENCODE id into our preferred format by stripping
    * its prefix and suffix.
    */
  def transformId(rawId: String): String =
    IdPattern.findAllIn(rawId).group("accession")

  /**
    * Read the @id out of a raw ENCODE object, transforming it into our
    * preferred format along the way.
    */
  def readId(rawObj: Msg): String = transformId(rawObj.read[String]("@id"))

  /**
    * Prepend the encode base url to any string that represent an encode url path
    */
  val urlPrefix = "https://www.encodeproject.org"
  def convertToEncodeUrl(value: String): String = urlPrefix + value

  def convertToEncodeUrl(optValue: Option[String]): Option[String] =
    optValue.map(value => urlPrefix + value)

  def convertToEncodeUrl(values: List[String]): List[String] =
    values.map(value => convertToEncodeUrl(value))

  /**
    * Summarize any audit records found in a raw ENCODE object, returning:
    *   1. A color label for the max audit level present
    *   2. The set of unique audit categories present
    */
  def summarizeAudits(rawObject: Msg): (Option[String], List[String]) =
    rawObject.tryRead[Msg]("audit").fold((Option.empty[String], List.empty[String])) { audits =>
      // ENCODE's audits are grouped by severity, but they also include
      // that label within each record, so it's fine to simplify and strip
      // away the grouping.
      val allAudits = audits.obj.valuesIterator.flatMap(_.arr)

      val (maxLevel, labels) = allAudits.foldLeft((0L, Set.empty[String])) {
        case ((max, acc), audit) =>
          val label = audit.read[String]("category")
          val level = audit.read[Long]("level")

          (math.max(max, level), acc + label)
      }

      val levelColor = maxLevel match {
        case 40 => "yellow"
        case 50 => "orange"
        case 60 => "red"
        case _  => "white"
      }

      (Some(levelColor), labels.toList.sorted)
    }
}
