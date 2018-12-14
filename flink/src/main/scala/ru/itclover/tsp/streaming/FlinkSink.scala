package ru.itclover.tsp.streaming

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

/**
  * Flink specific sink. Implements OutputFormat and contains it as a
  * member to be properly initialized by Flink
  * @tparam T type of items to write
  */
trait FlinkSink[T] extends OutputFormat[T] with Sink[T] {
  def format: OutputFormat[T]

  override def configure(parameters: Configuration): Unit = format.configure(parameters)

  override def open(taskNumber: Int, numTasks: Int): Unit = format.open(taskNumber, numTasks)

  override def writeRecord(record: T): Unit = format.writeRecord(record)

  override def close(): Unit = format.close()
}
