package org.broadinstitute.monster.encode.extraction

import org.broadinstitute.monster.encode.EncodeEntity
import upack.Msg

import scala.concurrent.Future
import scala.collection.mutable

/** Mock interface for ENCODE API. */
class MockEncodeClient(
  responseMap: Map[(EncodeEntity, (String, String)), Msg]
) extends EncodeClient {
  val recordedRequests = mutable.Set[(EncodeEntity, (String, String))]()

  override def get(entity: EncodeEntity, params: List[(String, String)]): Future[Msg] = {
    val paramsToUse = params.head
    recordedRequests.add((entity, paramsToUse))
    responseMap
      .get((entity, paramsToUse))
      .fold(
        Future.failed[Msg](
          new RuntimeException(s"Entity: ${entity} and params: ${paramsToUse.toString}")
        )
      )(Future.successful)
  }
}
