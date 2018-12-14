package ru.itclover.tsp.streaming

import cats.Semigroup
import cats.syntax.semigroup._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SessionWindowTimeGapExtractor}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.mappers.FlatMappersCombinator
import ru.itclover.tsp.core.IncidentInstances.semigroup
import scala.reflect.ClassTag

case class FlinkStreamAlg() extends StreamAlg[DataStream, KeyedStream, TypeInformation] {

  override def createStream[In, Key, Item](source: Source[In, Key, Item, DataStream]) = {
    val extractor = source.timeExtractor
    source
      .createStream
      .assignAscendingTimestamps(x => extractor.apply(x).toMillis)
  }

  override def keyBy[In, K: TypeInformation](stream: DataStream[In])(partitioner: In => K, maxPartitions: Int): KeyedStream[In, K] =
    stream
      .setMaxParallelism(maxPartitions) // .. check is correct
      .keyBy(partitioner)

  override def flatMapWithState[In, State: ClassTag, Out: TypeInformation, K](stream: KeyedStream[In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]]
  ) =
    stream.flatMap(new FlatMappersCombinator(mappers))

  override def map[In, Out: TypeInformation, State](stream: DataStream[In])(f: In => Out)  =
    stream.map(f)

  override def reduceNearby[In: Semigroup: TimeExtractor, K](stream: KeyedStream[In, K])(
    getSessionSize: In => Long
  ): DataStream[In] =
    stream
      .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[In] {
        override def extract(element: In): Long = getSessionSize(element)
      }))
      .reduce { _ |+| _ }
      .name("Uniting adjacent items")

  //noinspection ConvertibleToMethodValue
  override def addSink[T](stream: DataStream[T])(sink: Sink[T]): DataStream[T] = {
    sink match {
      case s: FlinkSink[T] => stream.writeUsingOutputFormat(s.format)
      case _ => stream.addSink(sink.writeRecord(_))
    }
    stream
  }
}
