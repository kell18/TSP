package ru.itclover.tsp.streaming

import ru.itclover.tsp.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.io.input.InputConf
import scala.language.higherKinds

trait SourceConf[Event, EKey, EItem] extends Product with Serializable {
  def inpConf: InputConf[Event, EKey, EItem]

  def emptyEvent: Event

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def fieldToEKey: Symbol => EKey

  def partitioner: Event => String

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]
}
