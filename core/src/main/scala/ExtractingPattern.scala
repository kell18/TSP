package ru.itclover.tsp.v2
import ru.itclover.tsp.core.Time.MinWindow
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.io.{Decoder, Extractor}
import ru.itclover.tsp.v2.Pattern.{IdxExtractor, TsIdxExtractor}

import scala.language.higherKinds

class ExtractingPattern[Event: TsIdxExtractor, EKey, EItem, T, S <: PState[T, S]]
(key: EKey, keyName: Symbol)
(
  implicit extract: Extractor[Event, EKey, EItem],
  decoder: Decoder[EItem, T]
) extends SimplePattern[Event, T]({ e =>
      val r = extract(e, key)
      Result.succ(r)
    }) {
  override def setMaxWindow(window: Window) = {}

  override def maxWindowWhichWasSet: Window = MinWindow
}
