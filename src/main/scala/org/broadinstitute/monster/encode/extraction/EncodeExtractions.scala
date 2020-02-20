package org.broadinstitute.monster.encode.extraction

import java.io.{IOException}

import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms._
import com.spotify.scio.values.SCollection
import com.squareup.okhttp.{Callback, OkHttpClient, Request, Response}
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import org.broadinstitute.monster.common.msg.JsonParser
import org.broadinstitute.monster.common.msg.MsgOps
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import upack.Msg

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/** Ingest step responsible for pulling raw metadata for a specific entity type from the ENCODE API. */
object EncodeExtractions {
  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  /** HTTP client to use for querying ENCODE APIs. */
  val client = new OkHttpClient()

  /**
    * Pipeline stage which maps batches of query params to output payloads
    * by sending the query params to the ENCODE search API.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  class EncodeLookup(encodeEntity: EncodeEntity)
      extends ScalaAsyncLookupDoFn[List[(String, String)], String, OkHttpClient] {

    private val baseParams =
      List("frame=object", "status=released", "limit=all", "format=json")

    override def asyncLookup(
      client: OkHttpClient,
      params: List[(String, String)]
    ): Future[String] = {
      val paramStrings = params.map {
        case (key, value) =>
          s"$key=$value"
      }
      val allParams = s"type=${encodeEntity.entryName}" :: baseParams ::: paramStrings

      get(client, allParams)
    }

    /**
      * Construct a future which will either complete with the stringified
      * payload resulting from querying a url, or fail.
      *
      * @param client the HTTP client to use in the query
      * @param params ???
      */
    private def get(client: OkHttpClient, params: List[String]): Future[String] = {
      val url = s"https://www.encodeproject.org/search/?${params.mkString(sep = "&")}"
      val promise = Promise[String]()

      val request = new Request.Builder()
        .url(url)
        .get
        .build

      client
        .newCall(request)
        .enqueue(new Callback {
          override def onFailure(request: Request, e: IOException): Unit = {
            promise.failure(e)
            ()
          }

          override def onResponse(response: Response): Unit = {
            if (response.isSuccessful) {
              promise.success(response.body.string)
            } else if (response.code() == 404) {
              promise.success("""{ "@graph": [] }""")
            } else {
              promise.failure(
                new RuntimeException(s"ENCODE lookup failed: $response")
              )
            }
            ()
          }
        })

      promise.future
    }

    override protected def newClient(): OkHttpClient = client
  }

  /**
    * Pipeline stage which maps batches of search parameters into MessagePack entities
    * from ENCODE matching those parameters.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  def getEntities(
    encodeEntity: EncodeEntity
  ): SCollection[List[(String, String)]] => SCollection[Msg] =
    _.applyKvTransform(ParDo.of(new EncodeLookup(encodeEntity))).flatMap { kv =>
      kv.getValue
        .fold(
          throw _,
          JsonParser.parseEncodedJson
        )
        .read[Array[Msg]]("@graph")
    }

  /**
    * Pipeline stage which extracts IDs from downloaded MessagePack entities for
    * use in subsequent queries.
    *
    * @param referenceField field in the input MessagePacks containing the IDs
    *                       to extract
    */
  def getIds(
    referenceField: String
  ): SCollection[Msg] => SCollection[String] =
    collection =>
      collection.flatMap { msg =>
        msg.tryRead[Array[String]](referenceField).getOrElse(Array.empty)
      }.distinct

  /**
    * Pipeline stage which maps entity IDs into corresponding MessagePack entities
    * downloaded from ENCODE.
    *
    * @param encodeEntity the type of ENCODE entity the input IDs correspond to
    * @param batchSize the number of elements in a batch stream
    * @param fieldName the field name to get the entity by, is "@id" by default
    */
  def getEntitiesByField(
    encodeEntity: EncodeEntity,
    batchSize: Long,
    fieldName: String = "@id"
  ): SCollection[String] => SCollection[Msg] = { idStream =>
    val paramsBatchStream =
      idStream
        .map(KV.of("key", _))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .applyKvTransform(GroupIntoBatches.ofSize(batchSize))
        .map(_.getValue)
        .map { ids =>
          ids.asScala.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
            (fieldName -> ref) :: acc
          }
        }
    getEntities(encodeEntity)(paramsBatchStream)
  }
}
