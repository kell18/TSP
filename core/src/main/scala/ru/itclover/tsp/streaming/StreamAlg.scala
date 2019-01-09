package ru.itclover.tsp.streaming

import scala.reflect.ClassTag
import cats.Semigroup
import ru.itclover.tsp.core.{Incident, IncidentId}
import ru.itclover.tsp.io.TimeExtractor
import scala.language.higherKinds


trait StreamAlg[S[_], KeyedS[_, _] <: S[_], TypeInfo[_]] {
  def keyBy[In, K: TypeInfo](stream: S[In])(partitioner: In => K, maxPartitions: Int): KeyedS[In, K]

  def map[In, Out: TypeInfo, State](stream: S[In])(f: In => Out): S[Out]

  def flatMapWithState[In, State: ClassTag, Out: TypeInfo, K](stream: KeyedS[In, K])(
    mappers: Seq[StatefulFlatMapper[In, State, Out]]
  ): S[Out]

  def reduceNearby[In: Semigroup: TimeExtractor, K](stream: KeyedS[In, K])(getSessionSizeMs: K => Long): S[In]

  def addSink[T](stream: S[T])(sink: Sink[T]): S[T]
}

/**
  *
  * @tparam TypeInfo
  */
trait TypeInfoSet[TypeInfo[_]] {
  implicit def incident: TypeInfo[Incident]
  implicit def incidentId: TypeInfo[IncidentId]
  implicit def string: TypeInfo[String]
}
