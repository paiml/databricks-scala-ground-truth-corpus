package com.paiml.databricks.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Join strategy patterns for Databricks Spark workloads.
  *
  * Demonstrates broadcast joins, skew handling, SCD Type 2 merges,
  * and anti/semi join patterns commonly used in data engineering.
  */
object JoinPatterns {

  /** Broadcast join: forces the smaller DataFrame to be broadcast to all executors. */
  def broadcastJoin(
      large: DataFrame,
      small: DataFrame,
      joinCol: String
  ): DataFrame =
    large.join(broadcast(small), Seq(joinCol), "inner")

  /** Left anti join: rows in left that have no match in right. */
  def antiJoin(left: DataFrame, right: DataFrame, joinCol: String): DataFrame =
    left.join(right, Seq(joinCol), "left_anti")

  /** Left semi join: rows in left that have a match in right (no right columns). */
  def semiJoin(left: DataFrame, right: DataFrame, joinCol: String): DataFrame =
    left.join(right, Seq(joinCol), "left_semi")

  /** Multi-column join on a sequence of column names. */
  def multiColumnJoin(
      left: DataFrame,
      right: DataFrame,
      joinCols: Seq[String],
      joinType: String = "inner"
  ): DataFrame =
    left.join(right, joinCols, joinType)

  /** Salted join to handle data skew on a join key.
    *
    * Adds a random salt [0, numBuckets) to distribute skewed keys across partitions.
    */
  def saltedJoin(
      skewed: DataFrame,
      other: DataFrame,
      joinCol: String,
      numBuckets: Int
  ): DataFrame = {
    val saltedSkewed = skewed.withColumn("_salt", (rand() * numBuckets).cast("int"))
    val explodedOther = other.withColumn("_salt", explode(array(
      (0 until numBuckets).map(lit(_)): _*
    )))
    saltedSkewed.join(explodedOther, Seq(joinCol, "_salt"), "inner").drop("_salt")
  }

  /** Slowly Changing Dimension Type 2 merge pattern.
    *
    * Identifies new, changed, and unchanged records between current and incoming data.
    * Returns a DataFrame with an `_scd_action` column: "INSERT", "UPDATE", or "NO_CHANGE".
    */
  def scdType2Detect(
      current: DataFrame,
      incoming: DataFrame,
      keyCol: String,
      compareCols: Seq[String]
  ): DataFrame = {
    val joined = incoming.as("inc")
      .join(current.as("cur"), col(s"inc.$keyCol") === col(s"cur.$keyCol"), "left")

    val changeCondition = compareCols.map { c =>
      col(s"inc.$c") =!= col(s"cur.$c") || col(s"cur.$c").isNull
    }.reduce(_ || _)

    val isNew = col(s"cur.$keyCol").isNull

    joined.select(
      col(s"inc.*"),
      when(isNew, lit("INSERT"))
        .when(changeCondition, lit("UPDATE"))
        .otherwise(lit("NO_CHANGE"))
        .as("_scd_action")
    )
  }

  /** Cross join (cartesian product) with an optional filter condition. */
  def crossJoinFiltered(
      left: DataFrame,
      right: DataFrame,
      filterExpr: Option[String] = None
  ): DataFrame = {
    val crossed = left.crossJoin(right)
    filterExpr.map(crossed.filter).getOrElse(crossed)
  }
}
