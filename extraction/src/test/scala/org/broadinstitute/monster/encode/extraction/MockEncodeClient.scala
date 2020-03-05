package org.broadinstitute.monster.encode.extraction

import upack.Msg

import scala.concurrent.Future
import scala.collection.mutable

class MockEncodeClient(
  responseMap: Map[Set[(String, String)], Msg]
) extends EncodeClient {
  val recordedRequests = mutable.Set[Set[(String, String)]]()

  override def get(entity: EncodeEntity, params: List[(String, String)]): Future[Msg] = {
    val paramsToUse = params.toSet
    recordedRequests.add(paramsToUse)
    responseMap
      .get(paramsToUse)
      .fold(
        Future.failed[Msg](
          new RuntimeException(s"Entity: ${entity} and params: ${params.toString}")
        )
      )(Future.successful)
  }
}
