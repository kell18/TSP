package ru.itclover.tsp.streaming

import cats.syntax.either._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.types.Row
import org.influxdb.dto.QueryResult
import ru.itclover.tsp.io.input._
import ru.itclover.tsp.services.InfluxDBService
import ru.itclover.tsp.utils.ErrorsADT._
import ru.itclover.tsp.utils.RowOps.{RowIdxExtractor, RowIsoTimeExtractor}


object InfluxDBSourceConf {
  def create(conf: InfluxDBInputConf)(implicit strEnv: StreamExecutionEnvironment): Either[ConfigErr, InfluxDBSourceConf] =
    InfluxDBService.fetchFieldsTypesInfo(conf.query, conf.influxConf)
      .toEither
      .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      .map(InfluxDBSourceConf(conf, _))
}

case class InfluxDBSourceConf(inpConf: InfluxDBInputConf, fieldsClasses: Seq[(Symbol, Class[_])])(
  implicit streamEnv: StreamExecutionEnvironment
) extends SourceConf[Row, Int, Any] {

  import inpConf._

  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
  val stageName = "InfluxDB input processing stage"
  val defaultTimeoutSec = 200L

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

  override def fieldToEKey = (fieldId: Symbol) => fieldsIdxMap(fieldId)

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: Row => serializablePI.map(event.getField).mkString
  }

  override def timeExtractor = RowIsoTimeExtractor(timeIndex, datetimeField)
  override def extractor = RowIdxExtractor()

  val inputFormat =
    InfluxDBInputFormat
      .create()
      .url(url)
      .timeoutSec(timeoutSec.getOrElse(defaultTimeoutSec))
      .username(userName.getOrElse(""))
      .password(password.getOrElse(""))
      .database(dbName)
      .query(query)
      .and()
      .buildIt()
}