package com.paiml.databricks.delta

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

/** Change Data Capture (CDC) patterns for Delta Lake.
  *
  * Implements CDC detection, SCD Type 1/2 merge operations,
  * and change feed processing for data warehouse patterns.
  */
object ChangeDataCapture {

  /** Detects changes between source and target DataFrames.
    *
    * Returns rows with a `_change_type` column: "insert", "update", or "delete".
    */
  def detectChanges(
      source: DataFrame,
      target: DataFrame,
      keyCol: String,
      compareCols: Seq[String]
  ): DataFrame = {
    val joined = source.as("src")
      .join(target.as("tgt"), col(s"src.$keyCol") === col(s"tgt.$keyCol"), "full_outer")

    val isInsert = col(s"tgt.$keyCol").isNull
    val isDelete = col(s"src.$keyCol").isNull
    val isUpdate = compareCols.map { c =>
      col(s"src.$c") =!= col(s"tgt.$c")
    }.reduce(_ || _)

    joined.select(
      coalesce(col(s"src.$keyCol"), col(s"tgt.$keyCol")).as(keyCol),
      when(isInsert, lit("insert"))
        .when(isDelete, lit("delete"))
        .when(isUpdate, lit("update"))
        .otherwise(lit("no_change"))
        .as("_change_type")
    ).filter(col("_change_type") =!= "no_change")
  }

  /** SCD Type 1 merge: overwrites existing records, inserts new ones. */
  def scdType1Merge(
      deltaTable: DeltaTable,
      updates: DataFrame,
      keyCol: String
  ): Unit =
    deltaTable.as("target")
      .merge(updates.as("source"), s"target.$keyCol = source.$keyCol")
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

  /** SCD Type 2 merge: closes existing records and inserts new versions.
    *
    * Expects the target table to have `effective_date`, `end_date`, and `is_current` columns.
    */
  def scdType2Merge(
      deltaTable: DeltaTable,
      updates: DataFrame,
      keyCol: String,
      compareCols: Seq[String],
      effectiveDate: String
  ): Unit = {
    val changeCondition = compareCols.map { c =>
      s"target.$c <> source.$c"
    }.mkString(" OR ")

    // Close existing records that have changed
    deltaTable.as("target")
      .merge(
        updates.as("source"),
        s"target.$keyCol = source.$keyCol AND target.is_current = true"
      )
      .whenMatched(changeCondition)
      .update(Map(
        "end_date" -> lit(effectiveDate).cast("date"),
        "is_current" -> lit(false)
      ))
      .execute()

    // Insert new versions
    val newRecords = updates.withColumn("effective_date", lit(effectiveDate).cast("date"))
      .withColumn("end_date", lit("9999-12-31").cast("date"))
      .withColumn("is_current", lit(true))

    deltaTable.as("target")
      .merge(
        newRecords.as("source"),
        s"target.$keyCol = source.$keyCol AND target.is_current = true AND ($changeCondition)"
      )
      .whenNotMatched.insertAll()
      .execute()
  }

  /** Processes a batch of CDC events (insert/update/delete) into a Delta table. */
  def applyCdcBatch(
      deltaTable: DeltaTable,
      cdcEvents: DataFrame,
      keyCol: String
  ): Unit = {
    val inserts = cdcEvents.filter(col("_change_type") === "insert").drop("_change_type")
    val updates = cdcEvents.filter(col("_change_type") === "update").drop("_change_type")
    val deletes = cdcEvents.filter(col("_change_type") === "delete")

    // Apply deletes
    if (!deletes.isEmpty) {
      val deleteKeys = deletes.select(keyCol).collect().map(_.get(0).toString)
      deltaTable.delete(col(keyCol).isin(deleteKeys: _*))
    }

    // Apply upserts (inserts + updates)
    val upserts = inserts.union(updates)
    if (!upserts.isEmpty) {
      DeltaTableOps.upsert(deltaTable, upserts, keyCol)
    }
  }

  /** Counts changes by type from a CDC events DataFrame. */
  def changeSummary(cdcEvents: DataFrame): DataFrame =
    cdcEvents.groupBy("_change_type").count()
}
