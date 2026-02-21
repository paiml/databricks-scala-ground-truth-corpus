package com.paiml.databricks.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Structured Streaming processor patterns for Databricks.
  *
  * Implements readStream/writeStream patterns, trigger configurations,
  * and common stream processing transformations.
  */
object StreamProcessor {

  /** Creates a streaming DataFrame from a rate source (for testing). */
  def rateStream(spark: SparkSession, rowsPerSecond: Int = 10): DataFrame =
    spark.readStream
      .format("rate")
      .option("rowsPerSecond", rowsPerSecond.toLong)
      .load()

  /** Creates a streaming DataFrame from JSON files in a directory. */
  def jsonFileStream(spark: SparkSession, path: String, schema: StructType): DataFrame =
    spark.readStream
      .format("json")
      .schema(schema)
      .load(path)

  /** Creates a streaming DataFrame from CSV files in a directory. */
  def csvFileStream(
      spark: SparkSession,
      path: String,
      schema: StructType,
      hasHeader: Boolean = true
  ): DataFrame =
    spark.readStream
      .format("csv")
      .schema(schema)
      .option("header", hasHeader)
      .load(path)

  /** Creates a streaming DataFrame from a Delta table. */
  def deltaStream(spark: SparkSession, path: String): DataFrame =
    spark.readStream
      .format("delta")
      .load(path)

  /** Writes a streaming DataFrame to a Delta table. */
  def writeToDelta(
      stream: DataFrame,
      path: String,
      checkpointPath: String,
      outputMode: OutputMode = OutputMode.Append(),
      trigger: Trigger = Trigger.ProcessingTime("10 seconds")
  ): StreamingQuery =
    stream.writeStream
      .format("delta")
      .outputMode(outputMode)
      .option("checkpointLocation", checkpointPath)
      .trigger(trigger)
      .start(path)

  /** Writes a streaming DataFrame to the console (for debugging). */
  def writeToConsole(
      stream: DataFrame,
      outputMode: OutputMode = OutputMode.Append(),
      trigger: Trigger = Trigger.ProcessingTime("5 seconds")
  ): StreamingQuery =
    stream.writeStream
      .format("console")
      .outputMode(outputMode)
      .trigger(trigger)
      .start()

  /** Writes a streaming DataFrame to memory (for testing). */
  def writeToMemory(
      stream: DataFrame,
      queryName: String,
      outputMode: OutputMode = OutputMode.Append()
  ): StreamingQuery =
    stream.writeStream
      .format("memory")
      .queryName(queryName)
      .outputMode(outputMode)
      .start()

  /** Adds a processing timestamp and watermark to a streaming DataFrame. */
  def withWatermark(
      stream: DataFrame,
      eventTimeCol: String,
      delayThreshold: String
  ): DataFrame =
    stream.withWatermark(eventTimeCol, delayThreshold)

  /** Deduplicates a stream based on a unique ID within a watermark window. */
  def deduplicateStream(
      stream: DataFrame,
      idCol: String,
      eventTimeCol: String,
      watermarkDelay: String
  ): DataFrame =
    stream
      .withWatermark(eventTimeCol, watermarkDelay)
      .dropDuplicates(idCol, eventTimeCol)

  /** Filters a stream and adds a processing timestamp column. */
  def filterAndTimestamp(
      stream: DataFrame,
      filterCondition: String,
      timestampCol: String = "processing_time"
  ): DataFrame =
    stream
      .filter(expr(filterCondition))
      .withColumn(timestampCol, current_timestamp())
}
