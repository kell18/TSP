package ru.itclover.tsp.utils


import scala.collection.{mutable => m}
import scala.language.higherKinds
import fs2.concurrent.Queue
import cats.implicits._
import cats.effect.Concurrent
import cats.{Applicative, CommutativeApplicative, Traverse}
import cats.effect.concurrent.Ref
import com.typesafe.scalalogging.Logger
import fs2.{Pipe, Stream}

object Fs2Ops {

  val log = Logger("Fs2Ops")

  /**
    * Split input Stream to Stream of (Key, PartitionStream), where each PartitionStream contains elements only
    * for givent Key.
    * @param selector Key extractor
    * @param maxChunkSize max chunk size for each PartitionStream
    */
  def groupBy[F[_], K, O](selector: O => K, maxChunkSize: Int = 1024)(
    implicit F: Concurrent[F]
  ): Pipe[F, O, (K, Stream[F, O])] = { in =>
    Stream.eval(Ref.of[F, Map[K, Queue[F, Option[O]]]](Map.empty)).flatMap { refToQueues =>
      val cleanup = {
        refToQueues.get.flatMap(_.toList.traverse(_._2.enqueue1(None))).map(_ => ())   // Probably to terminate queues
      }
      (in ++ Stream.eval(cleanup).drain)
        .evalMap { el =>
          refToQueues.get.map { queues =>
            val key = selector(el)
            if (queues.size > tooMuchPartitions)
              log.warn(s"Number of partitions for groupBy becomes too large (${queues.size}, possible OutOfMemory.")
            queues
              .get(key)
              .fold {
                for {
                  newQ <- Queue.unbounded[F, Option[O]]                               // Create a new queue
                  _    <- refToQueues.modify(m => (m + (key -> newQ), m))             // Update the ref of queues
                  _    <- newQ.enqueue1(el.some)                                      // Add new element (new queue)
                } yield (key -> newQ.dequeueChunk(maxChunkSize).unNoneTerminate).some // Make stream from queue
              } {
                _.enqueue1(el.some) as None                                           // Add new element (old queue)
              }
          }.flatten
        }
        .unNone
        .onFinalize(cleanup)
    }
  }

  def tooMuchPartitions = 2e16 // arbitrary large number to notice

}
