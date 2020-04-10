package org.broadinstitute.monster.encode.backfill

import com.spotify.scio.jdbc._
import org.broadinstitute.monster.common.ScioApp
import org.broadinstitute.monster.common.PipelineBuilder
import upack._
import org.broadinstitute.monster.common.StorageIO
import com.spotify.scio.coders.Coder
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import java.sql.ResultSet

object FileBackfillGenerator extends ScioApp[Args] {

  implicit val coder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  override def pipelineBuilder: PipelineBuilder[Args] = { (context, args) =>
    val connectionOpts = connectionOptions(args)
    val readOpts = readOptions(connectionOpts)
    StorageIO.writeJsonLists(
      context.withName("Read ingest requests from DB").jdbcSelect(readOpts),
      "File ingest requests",
      args.outputPrefix
    )
    ()
  }

  private def connectionOptions(args: Args): JdbcConnectionOptions = {
    val urlParams = Map(
      "cloudSqlInstance" -> args.cloudSqlInstanceName,
      "socketFactory" -> "com.google.cloud.sql.postgres.SocketFactory",
      "user" -> args.cloudSqlUsername,
      "password" -> args.cloudSqlPassword
    ).map {
      case (k, v) => s"$k=$v"
    }

    JdbcConnectionOptions(
      username = args.cloudSqlUsername,
      password = Some(args.cloudSqlPassword),
      driverClass = classOf[org.postgresql.Driver],
      connectionUrl = s"jdbc:postgresql:///${args.cloudSqlDb}?${urlParams.mkString("&")}"
    )
  }

  private val query =
    s"""SELECT file_format, data_type, md5sum, file_gs_path
       |FROM files
       |WHERE file_available_in_gcs
       |""".stripMargin

  private def readOptions(
    connectionOpts: JdbcConnectionOptions
  ): JdbcReadOptions[FileIngestRequest] =
    JdbcReadOptions(
      connectionOptions = connectionOpts,
      query = query,
      rowMapper = buildRequest
    )

  private def buildRequest(sqlRow: ResultSet): FileIngestRequest = {
    val format = sqlRow.getString("file_format")
    val outputType = sqlRow.getString("data_type")
    val md5 = sqlRow.getString("md5sum")
    val cloudPath = sqlRow.getString("file_gs_path")

    val cleanedType = outputType.replaceAll("\\s+", "-").toLowerCase()
    val basename = {
      val splitPoint = cloudPath.lastIndexOf('/')
      cloudPath.drop(splitPoint + 1)
    }
    val targetPath = s"/$cleanedType/$format/$md5/$basename"

    FileIngestRequest(sourcePath = cloudPath, targetPath = targetPath)
  }
}
