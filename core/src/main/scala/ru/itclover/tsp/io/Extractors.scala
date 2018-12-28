package ru.itclover.tsp.io

import ru.itclover.tsp.core.{Incident, Time}

trait Extractor[Event, EKey, EItem] extends Serializable {
  def apply[T](e: Event, k: EKey)(implicit T: Decoder[EItem, T]): T
}


trait KVExtractor[Event, EKey, EItem] extends Serializable {
  def apply[T](e: Event, k: EKey): (EKey, EItem)
}


trait TimeExtractor[Event] extends Serializable {
  def apply(e: Event): Time
}

object TimeExtractorInstances {
  implicit val incidentTI_from = new TimeExtractor[Incident] {
    override def apply(e: Incident) = e.segment.from
  }

  implicit val incidentTI_to = new TimeExtractor[Incident] {
    override def apply(e: Incident) = e.segment.to
  }
}