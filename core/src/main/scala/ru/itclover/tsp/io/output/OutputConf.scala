package ru.itclover.tsp.io.output

import scala.language.higherKinds

trait OutputConf[Event] {
  def forwardedFieldsIds: Seq[Symbol]

  def parallelism: Option[Int]
}
