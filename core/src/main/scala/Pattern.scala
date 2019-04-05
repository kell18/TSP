package ru.itclover.tsp.v2

import cats.{Foldable, Functor, Monad, Order}
import ru.itclover.tsp.v2.Pattern.{Idx, _}

import scala.collection.{mutable => m}
import scala.language.higherKinds

import org.openjdk.jmh.annotations.{Scope, State}

/**
  * Main trait for all patterns, basically just a function from state and events chunk to new a state.
  *
  * @tparam Event underlying data
  * @tparam T Type of the results in the S
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  */

@State (Scope.Benchmark)
trait Pattern[Event, S <: PState[T, S], T] extends Serializable {

  /**
    * Creates initial state. Has to be called only once
    *
    * @return initial state
    */
  def initialState(): S

  /**
    * @param oldState - previous state
    * @param events - new events to be processed
    * @tparam F Container for state (some simple monad mostly)
    * @tparam Cont Container for yet another chunk of Events
    * @return
    */
  def apply[F[_]: Monad, Cont[_]: Foldable: Functor](oldState: S, events: Cont[Event]): F[S]
}

trait IdxValue[+T] {
  def index: Idx // For internal use in patterns
  def value: Result[T] // actual result
  def start: Idx
  def end: Idx
}

object IdxValue {

  def apply[T](index: Idx, value: Result[T]): IdxValue[T] = new IdxValueSimple[T](index, value)
  def unapply[T](arg: IdxValue[T]): Option[(Idx, Result[T])] = Some(arg.index -> arg.value)

  /// Union the segments with a custom result
  def union[T1, T2, T3](iv1: IdxValue[T1], iv2: IdxValue[T2], func: (Result[T1], Result[T2]) => Result[T3]): IdxValue[T3] = IdxValueSegment(
    index = Math.min(iv1.start, iv2.start),
    start = Math.min(iv1.start, iv2.start),
    end = Math.max(iv1.end, iv2.end),
    value = func(iv1.value, iv2.value)
  )

  case class IdxValueSimple[T](index: Idx, value: Result[T]) extends IdxValue[T] {
    override def start: Idx = index
    override def end: Idx = index
  }

  case class IdxValueSegment[T](index: Idx, start: Idx, end: Idx, value: Result[T]) extends IdxValue[T]
}

object Pattern {

  type Idx = Long

  type QI[T] = m.Queue[IdxValue[T]]

  trait IdxExtractor[Event] extends Serializable with Order[Idx] {
    def apply(e: Event): Idx
  }

  class TsIdxExtractor[Event](eventToTs: Event => Long) extends IdxExtractor[Event] {
    val maxCounter: Int = 10e5.toInt // should be power of 10
    var counter: Int = 0

    override def apply(e: Event): Idx = {
      counter = (counter + 1) % maxCounter
      tsToIdx(eventToTs(e))
    }

    override def compare(x: Idx, y: Idx) = idxToTs(x) compare idxToTs(y)

    def idxToTs(idx: Idx): Long = idx / maxCounter

    def tsToIdx(ts: Long): Idx = ts * maxCounter + counter //todo ts << 5 & counter ?
  }

  object IdxExtractor {
    implicit class GetIdx[T](val event: T) extends AnyVal {
      def index(implicit te: IdxExtractor[T]): Idx = te.apply(event)
    }

    def of[E](f: E => Idx): IdxExtractor[E] = new IdxExtractor[E] {
      override def apply(e: E): Idx = f(e)

      override def compare(x: Idx, y: Idx) = x compare y
    }
  }
}
