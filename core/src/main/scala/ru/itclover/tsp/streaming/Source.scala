package ru.itclover.tsp.streaming

import ru.itclover.tsp.io.{Extractor, TimeExtractor}
import ru.itclover.tsp.io.input.InputConf
import scala.language.higherKinds

trait Source[Event, EKey, EItem, Stream[_]] extends Product with Serializable {
  def createStream: Stream[Event]

  def conf: InputConf[Event, EKey, EItem]

  def emptyEvent: Event

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def isEventTerminal: Event => Boolean

  def fieldToEKey: Symbol => EKey

  def partitioner: Event => String

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]
}

object StreamSource {
  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) = {
    allFields.find { field => !excludedFields.contains(field) }
  }
}
