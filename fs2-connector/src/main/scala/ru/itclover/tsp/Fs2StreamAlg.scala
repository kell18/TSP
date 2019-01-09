package ru.itclover.tsp

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import cats.Semigroup
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import com.typesafe.scalalogging.Logger
import fs2.{Chunk, Stream}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.streaming.{StatefulFlatMapper, StreamAlg}
import ru.itclover.tsp.utils.Fs2Ops.{groupBy, reduceLeft}
import ru.itclover.tsp.Fs2StreamAlg.{KeyedStream, NoTypeInfo}
import scala.language.higherKinds

object Fs2StreamAlg {
  type NoTypeInfo[T] = Nothing

  type KeyedStream[+F[_], +O, K] = Stream[F, (K, Stream[F, O])]
}

case class Fs2StreamAlg[F[_]](maxChunkSize: Int = 1024, maxSessionSize: Int = 1000000)(
  implicit F: Concurrent[F],
  Timer: Timer[F]
) extends StreamAlg[Stream[F, ?], KeyedStream[F, +?, ?], NoTypeInfo] {

  import Fs2StreamAlg._

  val log = Logger[Fs2StreamAlg[F]]

  def keyBy[In, K: NoTypeInfo](stream: Stream[F, In])(partitioner: In => K, maxPartitions: Int) = {
    log.info("Ignoring maxPartitions arg, current Fs2.keyBy algorithm doesn't support on-disk keys spilling.")
    // `parJoin(max)` (bounded) - not working due to some complexities in groupBy algorithm
    stream.through(groupBy[F, K, In](partitioner, maxChunkSize))
  }

  def map[In, Out: NoTypeInfo, State](stream: Stream[F, In])(f: In => Out) = stream.map(f)

  def flatMapWithState[In, State, Out, K](stream: KeyedStream[F, In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]]
  )(implicit State: ClassTag[State], noTypeInfo: NoTypeInfo[Out]) = {
    val initStates: Seq[State] = mappers.map(_.initialState)

    stream.map {
      case (_, innerS) =>
        innerS.chunks
          .map {
            _.mapAccumulate(initStates) {
              case (states, el) =>
                val mappersStates: Seq[(StatefulFlatMapper[In, State, Out], State)] = mappers.zip(states)
                val resultsAndStates: Seq[(Seq[Out], State)] = mappersStates.map { case (m, s) => m.apply(el, s) }
                (resultsAndStates.map(_._2), resultsAndStates.flatMap(_._1))
            }
          }
          .flatMap { case (_, chunk) => Stream.chunk(chunk.flatMap(s => Chunk.seq(s))) }
    }.parJoinUnbounded
  }

  def reduceNearby[In: Semigroup: TimeExtractor, K](stream: KeyedStream[F, In, K])(
    getSessionSizeMs: K => Long
  ) =
    stream.map {
      case (key, inner) =>
        val sessionSize = FiniteDuration.apply(getSessionSizeMs(key), "millisecond")
        inner
        // todo lazy groupWithin to prevent OOMs in general (may be make new `window` method-space in Fs2 lib)
          .groupWithin(maxSessionSize, sessionSize) // maxSessionSize here to prevent OOM exceptions
          .map(chunk => reduceLeft(chunk))
          .unNone
    }.parJoinUnbounded

  def addSink[T](stream: Stream[F, T])(sink: streaming.Sink[T]) = stream.map { el =>
    sink.writeRecord(el)
    el
  }
}
