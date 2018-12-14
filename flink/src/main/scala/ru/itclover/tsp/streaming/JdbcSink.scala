package ru.itclover.tsp.streaming
import java.sql.Timestamp
import java.time.{LocalDateTime, ZonedDateTime, ZoneId}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.types.Row
import ru.itclover.tsp.core.Incident
import ru.itclover.tsp.io.output.{JDBCOutputConf, RowSchema}
import ru.itclover.tsp.utils.ErrorsADT.ConfigErr


object JdbcSink {
  def create(conf: JDBCOutputConf): Either[ConfigErr, JdbcSink] = {
    // .. TODO Check sink availability and type correctness
    Right(JdbcSink(conf))
  }
}

case class JdbcSink(conf: JDBCOutputConf) extends FlinkSink[Row] {
  import conf._

  val log = Logger[JdbcSink]
  val DEFAULT_BATCH_INTERVAL = 1000000

  override def format = {
    val insertQuery = getInsertQuery(tableName, rowSchema)
    log.info(s"Configure JDBCOutput with insertQuery = `$insertQuery`")
    JDBCOutputFormat.buildJDBCOutputFormat()
        .setDrivername(driverName)
        .setDBUrl(jdbcUrl)
        .setUsername(userName.getOrElse(""))
        .setPassword(password.getOrElse(""))
        .setQuery(insertQuery)
        .setSqlTypes(rowSchema.fieldTypes.toArray)
        .setBatchInterval(batchInterval.getOrElse(DEFAULT_BATCH_INTERVAL))
        .finish()
  }

  def getInsertQuery(tableName: String, rowSchema: RowSchema) = {
    val columns = rowSchema.fieldsNames.map(_.toString().tail)
    val statements = columns.map(_ => "?").mkString(", ")
    s"INSERT INTO ${tableName} (${columns.mkString(", ")}) VALUES (${statements})"
  }

  def nowInUtcMillis: Double = {
    val zonedDt = ZonedDateTime.of(LocalDateTime.now, ZoneId.systemDefault)
    val utc = zonedDt.withZoneSameInstant(ZoneId.of("UTC"))
    Timestamp.valueOf(utc.toLocalDateTime).getTime / 1000.0
  }

  def payloadToJson(payload: Seq[(String, Any)]): String = {
    payload.map {
      case (fld, value) if value.isInstanceOf[String] => s""""${fld}":"${value}""""
      case (fld, value)                               => s""""${fld}":$value"""
    } mkString ("{", ",", "}")
  }
}
