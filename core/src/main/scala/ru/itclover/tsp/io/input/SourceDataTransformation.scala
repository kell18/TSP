package ru.itclover.tsp.io.input

trait SourceDataTransformationConf

abstract class SourceDataTransformation[Event, EKey, EValue](val `type`: String) extends Serializable {
  val config: SourceDataTransformationConf
}