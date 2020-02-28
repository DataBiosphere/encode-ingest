package org.broadinstitute.monster.encode.extraction

import java.io.IOException
import java.time.Duration

import okhttp3.{Call, Callback, OkHttpClient, Request, Response}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}

trait EncodeClient extends Serializable {
  def get(entity: EncodeEntity, params: List[(String, String)]): Future[String]
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

    (entity, params) => {
      val paramStrings = params.map {
        case (key, value) =>
          s"$key=$value"
      }
      val allParams = s"type=${entity.entryName}" :: baseParams ::: paramStrings
      val url = s"https://www.encodeproject.org/search/?${allParams.mkString(sep = "&")}"
      val p = Promise[String]()

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
              p.success(response.body.string)
            } else if (response.code() == 404) {
              p.success("""{ "@graph": [] }""")
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
