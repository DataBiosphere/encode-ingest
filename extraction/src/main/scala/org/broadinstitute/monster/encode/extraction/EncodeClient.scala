package org.broadinstitute.monster.encode.extraction

import java.io.IOException
import java.time.Duration

import okhttp3.{Call, Callback, OkHttpClient, Request, Response}
import org.broadinstitute.monster.common.msg.JsonParser
import org.broadinstitute.monster.encode.EncodeEntity
import org.slf4j.LoggerFactory
import upack.{Arr, Msg, Obj, Str}

import scala.concurrent.{Future, Promise}

/** Interface for clients that hits ENCODE API. */
trait EncodeClient extends Serializable {

  /**
    * TODO
    *
    * @param entity
    * @param params
    * @param negativeParams
    * @return
    */
  def get(
    entity: EncodeEntity,
    params: List[(String, String)],
    negativeParams: List[(String, String)]
  ): Future[Msg]
}

object EncodeClient {
  private val timeout = Duration.ofSeconds(60)

  private val baseParams =
    List("frame=object", "status=released", "limit=all", "format=json")

  def apply(): EncodeClient = {
    val logger = LoggerFactory.getLogger(getClass)

    val client = new OkHttpClient.Builder()
      .connectTimeout(timeout)
      .readTimeout(timeout)
      .build()

    (entity, params, negParams) => {
      val paramStrings = params.map {
        case (key, value) =>
          s"$key=$value"
      }
      val negParamStrings = negParams.map {
        case (key, value) =>
          s"$key!=$value"
      }
      val allParams = s"type=${entity.entryName}" :: baseParams ::: paramStrings ::: negParamStrings
      val url = s"https://www.encodeproject.org/search/?${allParams.mkString(sep = "&")}"
      val p = Promise[Msg]()

      val request = new Request.Builder()
        .url(url)
        .get
        .build

      logger.debug(s"New API Query: [$request]")
      client
        .newCall(request)
        .enqueue(new Callback {
          override def onFailure(call: Call, e: IOException): Unit = {
            p.failure(e)
            ()
          }

          override def onResponse(call: Call, response: Response): Unit = {
            if (response.isSuccessful) {
              p.success(JsonParser.parseEncodedJson(response.body.string))
            } else if (response.code() == 404) {
              p.success(Obj(Str("@graph") -> Arr()))
            } else {
              p.failure(
                new RuntimeException(s"ENCODE lookup failed: $response")
              )
            }
            ()
          }
        })

      p.future
    }
  }
}
