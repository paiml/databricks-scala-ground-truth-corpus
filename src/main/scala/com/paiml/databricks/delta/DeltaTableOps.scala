package com.paiml.databricks.delta

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

/** Core Delta Lake table operations.
  *
  * Implements CRUD, MERGE (upsert), time travel, and table maintenance
  * patterns for Delta Lake on Databricks.
  */
object DeltaTableOps {

  /** Writes a DataFrame as a new Delta table (overwrite mode). */
  def createDeltaTable(df: DataFrame, path: String): Unit =
    df.write.format("delta").mode("overwrite").save(path)

  /** Appends data to an existing Delta table. */
  def appendToDelta(df: DataFrame, path: String): Unit =
    df.write.format("delta").mode("append").save(path)

  /** Reads a Delta table from a path. */
  def readDelta(spark: SparkSession, path: String): DataFrame =
    spark.read.format("delta").load(path)

  /** Reads a Delta table at a specific version (time travel). */
  def readDeltaVersion(spark: SparkSession, path: String, version: Long): DataFrame =
    spark.read.format("delta").option("versionAsOf", version).load(path)

  /** Reads a Delta table at a specific timestamp (time travel). */
  def readDeltaTimestamp(spark: SparkSession, path: String, timestamp: String): DataFrame =
    spark.read.format("delta").option("timestampAsOf", timestamp).load(path)

  /** Upserts (MERGE) incoming data into a Delta table.
    *
    * Matches on keyCol: updates existing rows, inserts new rows.
    */
  def upsert(
      deltaTable: DeltaTable,
      updates: DataFrame,
      keyCol: String
  ): Unit =
    deltaTable.as("target")
      .merge(updates.as("source"), s"target.$keyCol = source.$keyCol")
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

  /** Deletes rows from a Delta table matching a condition. */
  def deleteWhere(deltaTable: DeltaTable, condition: String): Unit =
    deltaTable.delete(condition)

  /** Updates rows in a Delta table matching a condition. */
  def updateWhere(
      deltaTable: DeltaTable,
      condition: String,
      updates: Map[String, String]
  ): Unit = {
    val updateExprs: Map[String, Column] = updates.map { case (k, v) => k -> expr(v) }
    deltaTable.update(expr(condition), updateExprs)
  }

  /** Returns the transaction log history of a Delta table. */
  def history(deltaTable: DeltaTable, limit: Int = 10): DataFrame =
    deltaTable.history(limit)

  /** Compacts small files in a Delta table. */
  def compact(spark: SparkSession, path: String, numFiles: Int = 1): Unit =
    spark.read.format("delta").load(path)
      .repartition(numFiles)
      .write.format("delta").mode("overwrite")
      .option("dataChange", "false")
      .save(path)

  /** Vacuums old versions from a Delta table (removes files older than retentionHours). */
  def vacuum(deltaTable: DeltaTable, retentionHours: Double = 168.0): Unit = {
    val _ = deltaTable.vacuum(retentionHours)
  }

  /** Returns the current version number of a Delta table. */
  def currentVersion(deltaTable: DeltaTable): Long = {
    val hist = deltaTable.history(1)
    hist.select("version").first().getLong(0)
  }
}
