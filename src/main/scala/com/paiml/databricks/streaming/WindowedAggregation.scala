package com.paiml.databricks.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Windowed aggregation patterns for Structured Streaming.
  *
  * Implements tumbling, sliding, and session window aggregations
  * with watermarking for late data handling.
  */
object WindowedAggregation {

  /** Tumbling window aggregation: non-overlapping, fixed-size windows.
    *
    * @param stream      Streaming DataFrame with an event time column
    * @param eventTimeCol Column name containing event timestamps
    * @param windowDuration Window size (e.g., "5 minutes", "1 hour")
    * @param aggCol       Column to aggregate
    * @param watermarkDelay Watermark delay for late data (e.g., "10 minutes")
    */
  def tumblingWindowCount(
      stream: DataFrame,
      eventTimeCol: String,
      windowDuration: String,
      aggCol: String,
      watermarkDelay: String
  ): DataFrame =
    stream
      .withWatermark(eventTimeCol, watermarkDelay)
      .groupBy(window(col(eventTimeCol), windowDuration))
      .agg(
        count(aggCol).as("count"),
        sum(col(aggCol)).as("total"),
        avg(col(aggCol)).as("average")
      )

  /** Sliding window aggregation: overlapping windows with a slide interval. */
  def slidingWindowStats(
      stream: DataFrame,
      eventTimeCol: String,
      windowDuration: String,
      slideDuration: String,
      aggCol: String,
      watermarkDelay: String
  ): DataFrame =
    stream
      .withWatermark(eventTimeCol, watermarkDelay)
      .groupBy(window(col(eventTimeCol), windowDuration, slideDuration))
      .agg(
        count("*").as("count"),
        avg(col(aggCol)).as("avg_value"),
        min(col(aggCol)).as("min_value"),
        max(col(aggCol)).as("max_value"),
        stddev(col(aggCol)).as("stddev_value")
      )

  /** Tumbling window with grouping by an additional key column. */
  def windowedGroupBy(
      stream: DataFrame,
      eventTimeCol: String,
      windowDuration: String,
      groupCol: String,
      aggCol: String,
      watermarkDelay: String
  ): DataFrame =
    stream
      .withWatermark(eventTimeCol, watermarkDelay)
      .groupBy(
        window(col(eventTimeCol), windowDuration),
        col(groupCol)
      )
      .agg(
        count("*").as("event_count"),
        sum(col(aggCol)).as("total_value")
      )

  /** Multi-metric aggregation within a tumbling window. */
  def windowedMultiMetric(
      stream: DataFrame,
      eventTimeCol: String,
      windowDuration: String,
      metrics: Map[String, String],
      watermarkDelay: String
  ): DataFrame = {
    val aggExprs = metrics.map { case (colName, aggFunc) =>
      aggFunc match {
        case "count" => count(colName).as(s"${colName}_count")
        case "sum"   => sum(col(colName)).as(s"${colName}_sum")
        case "avg"   => avg(col(colName)).as(s"${colName}_avg")
        case "min"   => min(col(colName)).as(s"${colName}_min")
        case "max"   => max(col(colName)).as(s"${colName}_max")
        case _       => count(colName).as(s"${colName}_count")
      }
    }.toSeq

    stream
      .withWatermark(eventTimeCol, watermarkDelay)
      .groupBy(window(col(eventTimeCol), windowDuration))
      .agg(aggExprs.head, aggExprs.tail: _*)
  }
}
