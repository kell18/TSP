package ru.itclover.tsp.io.input


case class NarrowDataUnfolding[Event, EKey, EValue](key: EKey, value: EValue, fieldsTimeoutsMs: Map[EKey, Long], defaultTimeout: Option[Long] = None)
   extends SourceDataTransformation[Event, EKey, EValue]("NarrowDataUnfolding") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
}

case class WideDataFilling[Event, EKey, EValue](fieldsTimeoutsMs: Map[EKey, Long], defaultTimeout: Option[Long] = None)
  extends SourceDataTransformation[Event, EKey, EValue]("WideDataFilling") with SourceDataTransformationConf {
  override val config: SourceDataTransformationConf = this
}