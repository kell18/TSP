package ru.itclover.tsp.streaming

import java.util
import scala.collection.mutable
import scala.language.higherKinds
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.types.Row
import scala.collection.JavaConverters._

object FlinkSources {

  def jdbc(env: StreamExecutionEnvironment) = (src: JdbcSourceConf) => {
    import src._
    val extractor = src.timeExtractor
    val stream = env
      .createInput(inputFormat)(rowTypesInfo)
      .assignAscendingTimestamps(x => extractor.apply(x).toMillis)
      .name(stageName)
    inpConf.parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None    => stream
    }
  }


  def influx(env: StreamExecutionEnvironment): Source[DataStream, Row, InfluxDBSourceConf] =
    (src: InfluxDBSourceConf) => {
      import src._
      val serFieldsIdxMap = fieldsIdxMap // for task serialization
      val extractor = src.timeExtractor
      val stream = env
        .createInput(inputFormat)(queryResultTypeInfo)
        .flatMap(queryResult => {
          // extract Flink.rows form series of points
          if (queryResult == null || queryResult.getSeries == null) {
            mutable.Buffer[Row]()
          } else
            for {
              series   <- queryResult.getSeries.asScala
              valueSet <- series.getValues.asScala
              if valueSet != null
            } yield {
              val tags =
                if (series.getTags != null) series.getTags.asScala
                else new util.HashMap[String, String]().asScala
              val row = new Row(tags.size + valueSet.size())
              val fieldsAndValues = tags ++ series.getColumns.asScala.zip(valueSet.asScala)
              fieldsAndValues.foreach {
                case (field, value) => row.setField(serFieldsIdxMap(Symbol(field)), value)
              }
              row
            }
        })(rowTypesInfo)
        .assignAscendingTimestamps(x => extractor.apply(x).toMillis)
        .name(stageName)

      inpConf.parallelism match {
        case Some(p) => stream.setParallelism(p)
        case None    => stream
      }
    }
}
