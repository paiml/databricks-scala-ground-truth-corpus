package com.paiml.databricks.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Stream join patterns for Structured Streaming.
  *
  * Implements stream-static and stream-stream join patterns
  * with watermark-based state management.
  */
object StreamingJoin {

  /** Joins a streaming DataFrame with a static (batch) DataFrame. */
  def streamStaticJoin(
      stream: DataFrame,
      staticDf: DataFrame,
      joinCol: String,
      joinType: String = "inner"
  ): DataFrame =
    stream.join(staticDf, Seq(joinCol), joinType)

  /** Joins two streaming DataFrames with watermarks on both sides.
    *
    * Both streams must have watermarks defined. The join condition
    * includes a time range constraint to bound state.
    */
  def streamStreamJoin(
      leftStream: DataFrame,
      rightStream: DataFrame,
      joinCol: String,
      leftTimeCol: String,
      rightTimeCol: String,
      timeRangeSeconds: Long
  ): DataFrame =
    leftStream.join(
      rightStream,
      leftStream(joinCol) === rightStream(joinCol) &&
        leftStream(leftTimeCol) >= rightStream(rightTimeCol) - expr(s"INTERVAL $timeRangeSeconds SECONDS") &&
        leftStream(leftTimeCol) <= rightStream(rightTimeCol) + expr(s"INTERVAL $timeRangeSeconds SECONDS"),
      "inner"
    )

  /** Left outer stream-stream join with time range constraint. */
  def streamStreamLeftJoin(
      leftStream: DataFrame,
      rightStream: DataFrame,
      joinCol: String,
      leftTimeCol: String,
      rightTimeCol: String,
      timeRangeSeconds: Long
  ): DataFrame =
    leftStream.join(
      rightStream,
      leftStream(joinCol) === rightStream(joinCol) &&
        leftStream(leftTimeCol) >= rightStream(rightTimeCol) - expr(s"INTERVAL $timeRangeSeconds SECONDS") &&
        leftStream(leftTimeCol) <= rightStream(rightTimeCol) + expr(s"INTERVAL $timeRangeSeconds SECONDS"),
      "left_outer"
    )

  /** Enriches a stream with lookup data from a static table.
    *
    * Performs a broadcast join for small lookup tables.
    */
  def enrichWithLookup(
      stream: DataFrame,
      lookupDf: DataFrame,
      joinCol: String
  ): DataFrame =
    stream.join(broadcast(lookupDf), Seq(joinCol), "left")

  /** Joins a stream with a slowly-changing static DataFrame.
    *
    * The static DataFrame is re-read periodically by using foreachBatch.
    * This method returns the join logic as a function suitable for foreachBatch.
    */
  def dynamicLookupJoinFn(
      lookupPath: String,
      joinCol: String
  ): (DataFrame, Long) => Unit = { (batchDf: DataFrame, _: Long) =>
    val spark = batchDf.sparkSession
    val lookup = spark.read.format("delta").load(lookupPath)
    val enriched = batchDf.join(broadcast(lookup), Seq(joinCol), "left")
    enriched.write.format("noop").mode("overwrite").save()
  }
}
