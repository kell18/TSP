package ru.itclover.tsp.v2
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.v2.Pattern.{IdxExtractor, QI, TsIdxExtractor}
import ru.itclover.tsp.v2.Pattern.IdxExtractor._
import cats.syntax.functor._
import cats.syntax.foldable._
import ru.itclover.tsp.core.Time.{MaxWindow, MinWindow}
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.v2.IdxValue.IdxValueSimple

import scala.collection.{mutable => m}
import scala.language.higherKinds

// TODO Rename to FunctionP(attern)?
/** Simple Pattern */
class SimplePattern[Event: TsIdxExtractor, T](f: Event => Result[T]) extends Pattern[Event, SimplePState[T], T] {
  private var maxWindow: Window = MaxWindow

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SimplePState[T],
    events: Cont[Event]
  ): F[SimplePState[T]] = {
    val cleanedState = purgeQueue[T, SimplePState[T]](oldState, maxWindow)
    Monad[F].pure(SimplePState(events.map(e => IdxValueSimple(e.index, f(e))).foldLeft(cleanedState.queue) {
      case (oldStateQ, b) => { oldStateQ.enqueue(b); oldStateQ } // .. style?
    }))
  }
  override def initialState(): SimplePState[T] = SimplePState(m.Queue.empty)

  override def setMaxWindow(window: Window) = {
    this.maxWindow = window
  }

  override def maxWindowWhichWasSet: Window = this.maxWindow
}

case class SimplePState[T](override val queue: QI[T]) extends PState[T, SimplePState[T]] {
  override def copyWithQueue(queue: QI[T]): SimplePState[T] = this.copy(queue = queue)
}

case class ConstPattern[Event: TsIdxExtractor, T](value: T) extends SimplePattern[Event, T](_ => Result.succ(value))
