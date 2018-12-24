package ru.itclover.tsp

import cats.{Eq, Id, Semigroup}
import cats.arrow.FunctionK
import cats.effect.{Concurrent, ContextShift, IO, IOApp}
import cats.implicits._
import fs2.{Stream, _}
import ru.itclover.tsp.io.TimeExtractor
import ru.itclover.tsp.streaming.{Source, StatefulFlatMapper}
import ru.itclover.tsp.utils.Fs2Ops.groupBy
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.reflect.ClassTag

object Fs2TestApp extends App {
  import java.util.concurrent.Executors

  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(50)))

  val toIO: FunctionK[Pure, IO] = new FunctionK[Pure, IO] {
    def apply[A](l: Pure[A]): IO[A] = IO(l)
  }
  val toId: FunctionK[Pure, Id] = new FunctionK[Pure, Id] {
    def apply[A](l: Pure[A]): Id[A] = l
  }

  val str = Stream(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11).covary[IO]

  println(str.compile.toList.unsafeRunSync())

  val r: Stream[Id, (Int, Stream[Id, Int])] = groupBy[Id, Int, Int] { x =>
    println("Key by " + x)
    x % 3
  }.apply(str)


  val parts = r.compile.toList
  parts.foreach { case (k, p) =>
    println("Key: " + k + " values: " + p.compile.toList)
  }
}

object Fs2StreamAlg {
  type TypeInfo[T] = Nothing

  type PureStream[+F[_], +O] = Stream[F, O]

  type KeyedPureS[+F[_], +O, K] = Stream[F, (K, Stream[F, O])]
}

case class Fs2StreamAlg[F[_]](maxChunkSize: Int = 1024)(implicit F: Concurrent[F]) {
  import Fs2StreamAlg._

  def createStream[In, Key, Item](
    source: Source[In, Key, Item, PureStream]
  ) = source.createStream

  def keyBy[In, K: Eq](stream: PureStream[F, In])(partitioner: In => K, maxPartitions: Int): KeyedPureS[F, In, K] = {
    log.info("Ignoring maxPartitions arg, current Fs2.keyBy algorithm doesn't support on-disk keys spilling.")
    groupBy(partitioner, maxChunkSize)
  }

  def map[In, Out: TypeInfo, State](stream: PureStream[F, In])(f: In => Out) = stream.map(f)

  def flatMapWithState[In, State: ClassTag, Out, K](stream: KeyedPureS[F, In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]]
  ) = {
    val initStates = mappers.map(_.initialState)
    stream
      .mapAccumulate(mutable.Map.empty[K, Seq[State]]) {
        case (statesMap, (key, chunk)) =>
          val states = statesMap.getOrElse(key, initStates)
          val (newStates, newChunk) = chunk.mapAccumulate[Seq[State], Seq[Out]](states) {
            case (st, in) =>
              val mAndS: Seq[(StatefulFlatMapper[In, State, Out], State)] = mappers.zip(st)
              val newResAndStates: Seq[(Seq[Out], State)] = mAndS.map { case (m, s) => m.apply(in, s) }
              (newResAndStates.map(_._2), newResAndStates.flatMap(_._1))
          }
          statesMap(key) = newStates
          (statesMap, newChunk.flatMap(seq => Chunk.seq(seq)))
      }
      .flatMap { case (_, chunk) => Stream.chunk(chunk) }
  }

  def reduceNearby[In: Semigroup: TimeExtractor, K](stream: KeyedPureS[In, K])(getSessionSize: In => Long) =
    stream

  def addSink[T](stream: PureStream[T])(sink: streaming.Sink[T]) = ???
}

