package org.broadinstitute.monster.encode.backfill

import io.circe.Encoder
import io.circe.Json

case class FileIngestRequest(sourcePath: String, targetPath: String)

object FileIngestRequest {

  implicit val encoder: Encoder[FileIngestRequest] = request =>
    Json.obj(
      "sourcePath" -> Json.fromString(request.sourcePath),
      "targetPath" -> Json.fromString(request.targetPath)
    )
}
