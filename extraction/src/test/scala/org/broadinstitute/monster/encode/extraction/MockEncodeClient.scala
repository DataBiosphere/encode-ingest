package org.broadinstitute.monster.encode.extraction

import org.broadinstitute.monster.encode.EncodeEntity
import upack._

import scala.concurrent.Future
import scala.collection.mutable

/** Mock interface for ENCODE API. */
class MockEncodeClient(
  responseMap: Map[(EncodeEntity, (String, String), List[(String, String)]), List[Msg]]
) extends EncodeClient {
  val recordedRequests = mutable.Set[(EncodeEntity, (String, String), List[(String, String)])]()

  override def get(
    entity: EncodeEntity,
    params: List[(String, String)],
    negParams: List[(String, String)]
  ): Future[Msg] = {
    val withEntity = params.map(p => (entity, p, negParams))
    recordedRequests ++= withEntity
    val responses = withEntity.flatMap(responseMap.get).flatten
    Future.successful(Obj(Str("@graph") -> Arr(responses: _*)))
  }
}
