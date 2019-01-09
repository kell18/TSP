package ru.itclover.tsp.streaming

import cats.syntax.either._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.io.RichInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import ru.itclover.tsp.io.input._
import ru.itclover.tsp.services.JdbcService
import ru.itclover.tsp.utils.ErrorsADT._
import ru.itclover.tsp.utils.RowOps.{RowIdxExtractor, RowTsTimeExtractor}
import ru.itclover.tsp.JDBCInputFormatProps
import scala.language.higherKinds


object JdbcSourceConf {
  def create(conf: JDBCInputConf)(implicit strEnv: StreamExecutionEnvironment): Either[ConfigErr, JdbcSourceConf] =
    JdbcService.fetchFieldsTypesInfo(conf.driverName, conf.jdbcUrl, conf.query)
      .toEither
      .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      .map(JdbcSourceConf(conf, _))
}


case class JdbcSourceConf(inpConf: JDBCInputConf, fieldsClasses: Seq[(Symbol, Class[_])])(
  implicit streamEnv: StreamExecutionEnvironment
) extends SourceConf[Row, Int, Any] {

  import inpConf._

  val stageName = "JDBC input processing stage"
  val log = Logger[JdbcSourceConf]
  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  val partitionsIdx = partitionFields.map(fieldsIdxMap)

  require(fieldsIdxMap.get(datetimeField).isDefined, "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields.map(fieldsIdxMap.get)
    .find(idx => idx.isEmpty || idx.get >= fieldsIdxMap.size).flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.get}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val fieldsTypesInfo: Array[TypeInformation[_]] = fieldsClasses.map(c => TypeInformation.of(c._2)).toArray
  val rowTypesInfo = new RowTypeInfo(fieldsTypesInfo, fieldsClasses.map(_._1.toString.tail).toArray)

  val emptyEvent = {
    val r = new Row(fieldsIdx.length)
    fieldsIdx.foreach { case (_, ind) => r.setField(ind, 0) }
    r
  }

  override def fieldToEKey = {
    fieldId: Symbol => fieldsIdxMap(fieldId)
  }

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  val tsMultiplier = timestampMultiplier.getOrElse {
    log.info("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }
  override def timeExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, datetimeField)
  override def extractor = RowIdxExtractor()

  val inputFormat: RichInputFormat[Row, InputSplit] =
    JDBCInputFormatProps
      .buildJDBCInputFormat()
      .setDrivername(driverName)
      .setDBUrl(jdbcUrl)
      .setUsername(userName.getOrElse(""))
      .setPassword(password.getOrElse(""))
      .setQuery(query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()
}

