package ru.itclover.tsp.streaming

import cats.Semigroup
import cats.syntax.all._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.util.Collector
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.mappers.FlatMappersCombinator
import ru.itclover.tsp.core.IncidentInstances.semigroup
import scala.reflect.ClassTag

case class FlinkStreamAlg() extends StreamAlg[DataStream, KeyedStream, TypeInformation] {

  override def keyBy[In, K: TypeInformation](stream: DataStream[In])(partitioner: In => K, maxPartitions: Int): KeyedStream[In, K] =
    stream
      .setMaxParallelism(maxPartitions) // .. todo(1) check is correct (ConcModEx)
      .keyBy(partitioner)

  override def flatMapWithState[In, State: ClassTag, Out: TypeInformation, K](stream: KeyedStream[In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]]
  ) =
    stream.flatMap(new FlatMappersCombinator(mappers))

  override def map[In, Out: TypeInformation, State](stream: DataStream[In])(f: In => Out)  =
    stream.map(f)

  override def reduceNearby[In: Semigroup: TimeExtractor, K](stream: KeyedStream[In, K])(
    getSessionSizeMs: K => Long
  ): DataStream[In] = {
    val selector = stream.javaStream
      .asInstanceOf[org.apache.flink.streaming.api.datastream.KeyedStream[In, K]]
      .getKeySelector
    stream
      .window(EventTimeSessionWindows.withDynamicGap { el: In =>
         val key = selector.getKey(el)
         getSessionSizeMs(key)
      })
      .reduce { (a: In, b: In) => a |+| b }
      .name("Uniting adjacent items")
  }

  //noinspection ConvertibleToMethodValue
  override def addSink[T](stream: DataStream[T])(sink: Sink[T]): DataStream[T] = {
    sink match {
      case s: FlinkSink[T] => stream.writeUsingOutputFormat(s.format)
      case _ => stream.addSink(sink.writeRecord(_))
    }
    stream
  }
}
