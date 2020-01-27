package org.broadinstitute.monster.encode.extraction

import java.io.{IOException, InputStream, OutputStream}
import java.nio.ByteBuffer

import com.google.common.io.ByteStreams
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms._
import com.spotify.scio.values.SCollection
import com.squareup.okhttp.{Callback, OkHttpClient, Request, Response}
import io.circe.JsonObject
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.beam.sdk.coders.{Coder => BeamCoder}
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.sdk.values.KV

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

/** Ingest step responsible for pulling raw metadata for a specific entity type from the ENCODE API. */
object EncodeExtractions {

  // TODO when updating to upack and msg, will need to change this coder as well
  implicit val jsonCoder: Coder[JsonObject] = Coder.beam {
    new BeamCoder[JsonObject] {
      val parser = new JawnParser()

      override def encode(value: JsonObject, outStream: OutputStream): Unit =
        Option(value).foreach { jsonObj =>
          val bytes = jsonObj.asJson.noSpaces.getBytes
          VarInt.encode(bytes.length, outStream)
          outStream.write(bytes)
        }
      override def decode(inStream: InputStream): JsonObject = {
        val numBytes = VarInt.decodeInt(inStream)
        val bytes = new Array[Byte](numBytes)
        ByteStreams.readFully(inStream, bytes)
        parser
          .decodeByteBuffer[JsonObject](ByteBuffer.wrap(bytes))
          .fold(throw _, identity)
      }
      override def getCoderArguments: java.util.List[_ <: BeamCoder[_]] =
        java.util.Collections.emptyList()
      override def verifyDeterministic(): Unit =
        throw new BeamCoder.NonDeterministicException(
          this,
          "Equal Msgs don't necessarily encode to the same bytes"
        )
    }
  }

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
      List("frame=object", "status=released", "limit=100", "format=json")

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
    * Pipeline stage which maps batches of search parameters into JSON entities
    * from ENCODE matching those parameters.
    *
    * @param encodeEntity the type of ENCODE entity the stage should query
    */
  def getEntities(
    encodeEntity: EncodeEntity
  ): SCollection[List[(String, String)]] => SCollection[JsonObject] =
    _.transform(s"Download ${encodeEntity.entryName} Entities") {
      _.applyKvTransform(ParDo.of(new EncodeLookup(encodeEntity))).flatMap { kv =>
        kv.getValue.fold(
          throw _,
          value => {
            val decoded = for {
              // TODO change to upack and msg instead of circe and json, dear future monster
              json <- io.circe.parser.parse(value)
              cursor = json.hcursor
              objects <- cursor.downField("@graph").as[Vector[JsonObject]]
            } yield {
              objects
            }
            decoded.fold(throw _, identity)
          }
        )
      }
    }

  /**
    * Pipeline stage which extracts IDs from downloaded JSON entities for
    * use in subsequent queries.
    *
    * @param entryName display name for the type of entity whose IDs will
    *                  be extracted in this stage
    * @param referenceField field in the input JSONs containing the IDs
    *                       to extract
    * @param manyReferences whether or not `referenceField` is an array
    */
  def getIds(
    entryName: String,
    referenceField: String,
    manyReferences: Boolean
  ): SCollection[JsonObject] => SCollection[String] =
    _.transform(s"Get $entryName IDs") { collection =>
      collection.flatMap { jsonObj =>
        jsonObj(referenceField).toIterable.flatMap { referenceJson =>
          val references = for {
            refValues <- if (manyReferences) {
              referenceJson.as[List[String]]
            } else {
              referenceJson.as[String].map { reference =>
                List(reference)
              }
            }
          } yield {
            refValues
          }
          references.toOption
        }.flatten
      }.distinct
    }

  /**
    * Pipeline stage which maps entity IDs into corresponding JSON entities
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
  ): SCollection[String] => SCollection[JsonObject] = { idStream =>
    val paramsBatchStream =
      idStream.transform(s"Build ${encodeEntity.entryName} ID Queries") {
        _.map(KV.of("key", _))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .applyKvTransform(GroupIntoBatches.ofSize(batchSize))
          .map(_.getValue)
          .map { ids =>
            ids.asScala.foldLeft(List.empty[(String, String)]) { (acc, ref) =>
              (fieldName -> ref) :: acc
            }
          }
      }

    getEntities(encodeEntity)(paramsBatchStream)
  }
}
