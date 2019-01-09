package ru.itclover.tsp.streaming

import scala.language.higherKinds

/**
  * Factory for a streams
  * @tparam Stream Producing stream
  * @tparam Event
  * @tparam Conf Any SourceConf with [[Event]] as param
  */
trait Source[Stream[_], Event, Conf <: SourceConf[Event, _, _]] {
  def create(conf: Conf): Stream[Event]
}
