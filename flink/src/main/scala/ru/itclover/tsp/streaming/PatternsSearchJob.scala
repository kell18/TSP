package ru.itclover.tsp.streaming

import cats.data.Validated
import cats.Traverse
import cats.implicits._
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.{Incident, IncidentId, Pattern, RawPattern}
import ru.itclover.tsp.core.IncidentInstances.semigroup
import ru.itclover.tsp.dsl.{PatternBuilder, PatternMetadata}
import ru.itclover.tsp.io._
import ru.itclover.tsp.io.TimeExtractorInstances.incidentTI_from
import ru.itclover.tsp.mappers._
import ru.itclover.tsp.phases.TimeMeasurementPhases.TimeMeasurementPattern
import ru.itclover.tsp.utils.Bucketizer
import ru.itclover.tsp.utils.Bucketizer.Bucket
import ru.itclover.tsp.utils.Bucketizer.WeightExtractorInstances.phasesWeightExtractor
import ru.itclover.tsp.utils.ErrorsADT.{ConfigErr, InvalidPatternsCode}
import ru.itclover.tsp.Segment
import scala.language.higherKinds

// .. type factory
case class PatternsSearchJob[In, InKey, InItem, S[_], KeyedS[_, _] <: S[_], TypeInfo[_]](
  streamAlg: StreamAlg[S, KeyedS, TypeInfo],
  srcConf: SourceConf[In, InKey, InItem],
  srcFactory: Source[S, In, SourceConf[In, InKey, InItem]],
  decoders: BasicDecoders[InItem],
  typeInfoSet: TypeInfoSet[TypeInfo]
) {

  import srcConf.{extractor, timeExtractor}
  import decoders._
  import typeInfoSet._
  import PatternsSearchJob._

  val maxPartitions = srcConf.inpConf.maxPartitions

  def patternsSearchStream[OutE: TypeInfo, OutKey](
    rawPatterns: Seq[RawPattern],
    sink: Sink[OutE],
    resultMapper: Incident => OutE
  ): Either[ConfigErr, (Seq[RichPattern[In]], Vector[S[OutE]])] = {
    preparePatterns[In, InKey, InItem](rawPatterns, srcConf.fieldToEKey, srcConf.inpConf.defaultToleranceFraction.getOrElse(0)) map { patterns =>
      val forwardFields = sink.conf.forwardedFieldsIds.map(id => (id, srcConf.fieldToEKey(id)))
      val incidents = cleanIncidentsFromPatterns(patterns, forwardFields)
      val mapped = incidents.map(x => streamAlg.map(x)(resultMapper))
      (patterns, mapped.map(m => streamAlg.addSink(m)(sink)))
    }
  }

  def cleanIncidentsFromPatterns(
    richPatterns: Seq[RichPattern[In]],
    forwardedFields: Seq[(Symbol, InKey)]
  ): Vector[S[Incident]] =
    for {
      sourceBucket   <- bucketizePatterns(richPatterns, srcConf.inpConf.numParallelSources.getOrElse(1))
      stream         =  srcFactory.create(srcConf)
      patternsBucket <- bucketizePatterns(sourceBucket.items, srcConf.inpConf.patternsParallelism.getOrElse(1))
    } yield {
      val singleIncidents = incidentsFromPatterns(stream, patternsBucket.items, forwardedFields)

      if (srcConf.inpConf.defaultEventsGapMs > 0L) {
        val keyedIncidents = streamAlg.keyBy(singleIncidents)(_.id, maxPartitions)
        val gap = srcConf.inpConf.defaultEventsGapMs
        val patternsToWindows = richPatterns.map { case ((_, meta), raw) => raw.id -> meta.maxWindowMs }.toMap
        streamAlg.reduceNearby(keyedIncidents)(inc =>
          // Get max window of pattern, or, if it is = 0 - defaultEventsGapMs from conf
          patternsToWindows.get(inc.patternId).filter(_ > 0L).getOrElse(gap)
        )
      } else
        singleIncidents
    }

  def incidentsFromPatterns(
    stream: S[In],
    patterns: Seq[RichPattern[In]],
    forwardedFields: Seq[(Symbol, InKey)]
  ): S[Incident] = {
    val mappers: Seq[StatefulFlatMapper[In, Any, Incident]] = patterns.map {
      case ((pattern, meta), rawP) =>
        val allForwardFields = forwardedFields ++ rawP.forwardedFields.map(id => (id, srcConf.fieldToEKey(id)))
        val toIncidents = ToIncidentsMapper(
          rawP.id,
          allForwardFields.map { case (id, k) => id.toString.tail -> k },
          rawP.payload.toSeq,
          srcConf.inpConf.partitionFields.map(srcConf.fieldToEKey)
        )
        PatternFlatMapper(
          pattern,
          toIncidents.apply,
          srcConf.inpConf.eventsMaxGapMs,
          srcConf.emptyEvent
        )(timeExtractor).asInstanceOf[StatefulFlatMapper[In, Any, Incident]]
    }
    val keyed = streamAlg.keyBy(stream)(srcConf.partitioner, maxPartitions)
    streamAlg.flatMapWithState(keyed)(mappers)
  }
}

object PatternsSearchJob {
  type RichPattern[E] = ((Pattern[E, _, Segment], PatternMetadata), RawPattern)

  val log = Logger("PatternsSearchJob")
  def maxPartitionsParallelism = 8192

  def preparePatterns[E: TimeExtractor, EKey, EItem](
    rawPatterns: Seq[RawPattern],
    fieldsIdxMap: Symbol => EKey,
    toleranceFraction: Double
  )(
    implicit extractor: Extractor[E, EKey, EItem],
    dDecoder: Decoder[EItem, Double]
  ): Either[ConfigErr, List[RichPattern[E]]] = {
    Traverse[List]
      .traverse(rawPatterns.toList)(
        p =>
          Validated
            .fromEither(PatternBuilder.build[E, EKey, EItem](p.sourceCode, fieldsIdxMap.apply, toleranceFraction))
            .leftMap(err => List(s"PatternID#${p.id}, error: " + err))
            .map(pat => (TimeMeasurementPattern(pat._1, p.id, p.sourceCode), pat._2))
      )
      .leftMap[ConfigErr](InvalidPatternsCode(_))
      .map(_.zip(rawPatterns))
      .toEither
  }

  def bucketizePatterns[E](patterns: Seq[RichPattern[E]], parallelism: Int): Vector[Bucket[RichPattern[E]]] = {
    val patternsBuckets = if (parallelism > patterns.length) {
      log.warn(
        s"Patterns parallelism conf ($parallelism) is higher than amount of " +
        s"phases - ${patterns.length}, setting patternsParallelism to amount of phases."
      )
      Bucketizer.bucketizeByWeight(patterns, patterns.length)
    } else {
      Bucketizer.bucketizeByWeight(patterns, parallelism)
    }
    log.info("Patterns Buckets:\n" + Bucketizer.bucketsToString(patternsBuckets))
    patternsBuckets
  }
}
