package org.broadinstitute.monster.encode.extraction

import org.broadinstitute.monster.encode.EncodeEntity
import upack._

import scala.concurrent.Future
import scala.collection.mutable

/** Mock interface for ENCODE API. */
class MockEncodeClient(
  responseMap: Map[(EncodeEntity, (String, String)), List[Msg]]
) extends EncodeClient {
  val recordedRequests = mutable.Set[(EncodeEntity, (String, String))]()

  override def get(entity: EncodeEntity, params: List[(String, String)]): Future[Msg] = {
    val withEntity = params.map(entity -> _)
    recordedRequests ++= withEntity
    val responses = withEntity.flatMap(responseMap.get).flatten
    Future.successful(Obj(Str("@graph") -> Arr(responses: _*)))
  }
}
