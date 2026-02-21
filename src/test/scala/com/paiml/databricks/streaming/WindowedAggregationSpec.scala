package com.paiml.databricks.streaming

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WindowedAggregationSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  behavior of "WindowedAggregation"

  it should "create tumbling window aggregation on a stream" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val windowed = WindowedAggregation.tumblingWindowCount(
      stream, "timestamp", "10 seconds", "value", "30 seconds"
    )
    windowed.isStreaming shouldBe true
    windowed.schema.fieldNames should contain allOf ("count", "total", "average")
  }

  it should "create sliding window aggregation on a stream" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val windowed = WindowedAggregation.slidingWindowStats(
      stream, "timestamp", "30 seconds", "10 seconds", "value", "1 minute"
    )
    windowed.isStreaming shouldBe true
    windowed.schema.fieldNames should contain allOf (
      "count", "avg_value", "min_value", "max_value", "stddev_value"
    )
  }

  it should "create windowed group-by aggregation" in {
    val stream = StreamProcessor.rateStream(spark, 100)
      .withColumn("group", org.apache.spark.sql.functions.lit("A"))
    val windowed = WindowedAggregation.windowedGroupBy(
      stream, "timestamp", "10 seconds", "group", "value", "30 seconds"
    )
    windowed.isStreaming shouldBe true
    windowed.schema.fieldNames should contain allOf ("event_count", "total_value")
  }

  it should "create windowed multi-metric aggregation with avg" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val metrics = Map("value" -> "avg")
    val windowed = WindowedAggregation.windowedMultiMetric(
      stream, "timestamp", "10 seconds", metrics, "30 seconds"
    )
    windowed.isStreaming shouldBe true
    windowed.schema.fieldNames should contain("value_avg")
  }

  it should "exercise all aggregation branches in windowedMultiMetric" in {
    val stream = StreamProcessor.rateStream(spark, 100)
      .withColumn("v1", org.apache.spark.sql.functions.col("value"))
      .withColumn("v2", org.apache.spark.sql.functions.col("value"))
      .withColumn("v3", org.apache.spark.sql.functions.col("value"))
      .withColumn("v4", org.apache.spark.sql.functions.col("value"))
      .withColumn("v5", org.apache.spark.sql.functions.col("value"))

    val metrics = Map(
      "v1" -> "count",
      "v2" -> "sum",
      "v3" -> "min",
      "v4" -> "max",
      "v5" -> "unknown"
    )
    val windowed = WindowedAggregation.windowedMultiMetric(
      stream, "timestamp", "10 seconds", metrics, "30 seconds"
    )
    windowed.isStreaming shouldBe true
    windowed.schema.fieldNames should contain("v1_count")
    windowed.schema.fieldNames should contain("v2_sum")
    windowed.schema.fieldNames should contain("v3_min")
    windowed.schema.fieldNames should contain("v4_max")
    windowed.schema.fieldNames should contain("v5_count") // unknown defaults to count
  }

  it should "process tumbling window with memory sink" in {
    val stream = StreamProcessor.rateStream(spark, 100)
    val windowed = WindowedAggregation.tumblingWindowCount(
      stream, "timestamp", "5 seconds", "value", "10 seconds"
    )
    val query = StreamProcessor.writeToMemory(
      windowed, "tumbling_test",
      org.apache.spark.sql.streaming.OutputMode.Update()
    )
    try {
      query.processAllAvailable()
      val result = spark.sql("SELECT * FROM tumbling_test")
      result should not be null
    } finally {
      query.stop()
    }
  }
}
