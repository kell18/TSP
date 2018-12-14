package ru.itclover.tsp.streaming

import ru.itclover.tsp.io.output.OutputConf
import scala.language.higherKinds

trait Sink[T] {
  def conf: OutputConf[T]

  def writeRecord(t: T): Unit
}
