package com.paiml.databricks.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Window function patterns for analytical queries in Databricks.
  *
  * Implements ranking, running aggregates, lead/lag, and cumulative distributions
  * using Spark's Window specification API.
  */
object WindowFunctions {

  /** Adds a row_number column partitioned by partitionCol, ordered by orderCol. */
  def withRowNumber(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      outputCol: String = "row_num"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
    df.withColumn(outputCol, row_number().over(w))
  }

  /** Adds a rank column (ties get the same rank, gaps after ties). */
  def withRank(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      outputCol: String = "rank"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
    df.withColumn(outputCol, rank().over(w))
  }

  /** Adds a dense_rank column (no gaps in ranking). */
  def withDenseRank(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      outputCol: String = "dense_rank"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
    df.withColumn(outputCol, dense_rank().over(w))
  }

  /** Adds a running sum column within each partition. */
  def withRunningSum(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      sumCol: String,
      outputCol: String = "running_sum"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df.withColumn(outputCol, sum(col(sumCol)).over(w))
  }

  /** Adds a moving average column over the last N rows within each partition. */
  def withMovingAverage(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      avgCol: String,
      windowSize: Int,
      outputCol: String = "moving_avg"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
      .rowsBetween(-(windowSize - 1).toLong, Window.currentRow)
    df.withColumn(outputCol, avg(col(avgCol)).over(w))
  }

  /** Adds lead and lag columns for a given offset. */
  def withLeadLag(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      valueCol: String,
      offset: Int = 1
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
    df.withColumn(s"${valueCol}_lead", lead(col(valueCol), offset).over(w))
      .withColumn(s"${valueCol}_lag", lag(col(valueCol), offset).over(w))
  }

  /** Adds a percent_rank column (value between 0 and 1). */
  def withPercentRank(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      outputCol: String = "pct_rank"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
    df.withColumn(outputCol, percent_rank().over(w))
  }

  /** Assigns rows to N approximately equal-sized buckets within each partition. */
  def withNtile(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      n: Int,
      outputCol: String = "bucket"
  ): DataFrame = {
    val w = Window.partitionBy(partitionCol).orderBy(col(orderCol))
    df.withColumn(outputCol, ntile(n).over(w))
  }

  /** Picks the top-K rows per partition by a given ordering column. */
  def topKPerPartition(
      df: DataFrame,
      partitionCol: String,
      orderCol: String,
      k: Int,
      ascending: Boolean = true
  ): DataFrame = {
    val orderExpr = if (ascending) col(orderCol).asc else col(orderCol).desc
    val w = Window.partitionBy(partitionCol).orderBy(orderExpr)
    df.withColumn("_rn", row_number().over(w))
      .filter(col("_rn") <= k)
      .drop("_rn")
  }
}
