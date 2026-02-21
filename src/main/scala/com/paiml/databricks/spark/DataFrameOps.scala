package com.paiml.databricks.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Core DataFrame transformation patterns for Databricks workloads.
  *
  * Implements idiomatic Spark DataFrame operations including filtering, projection, aggregation,
  * and schema manipulation. All methods are pure transformations on DataFrames.
  */
object DataFrameOps {

  /** Adds a column with the length of a string column. */
  def withStringLength(df: DataFrame, inputCol: String, outputCol: String): DataFrame =
    df.withColumn(outputCol, length(col(inputCol)))

  /** Filters rows where a numeric column falls within [low, high] inclusive. */
  def filterRange(df: DataFrame, colName: String, low: Double, high: Double): DataFrame =
    df.filter(col(colName) >= low && col(colName) <= high)

  /** Renames multiple columns using a mapping. */
  def renameColumns(df: DataFrame, mapping: Map[String, String]): DataFrame =
    mapping.foldLeft(df) { case (accDf, (oldName, newName)) =>
      accDf.withColumnRenamed(oldName, newName)
    }

  /** Casts a column to a target data type. */
  def castColumn(df: DataFrame, colName: String, targetType: DataType): DataFrame =
    df.withColumn(colName, col(colName).cast(targetType))

  /** Drops rows where any of the specified columns contain null. */
  def dropNullsIn(df: DataFrame, columns: Seq[String]): DataFrame =
    df.na.drop(columns)

  /** Fills null values in specified columns with a default. */
  def fillNulls(df: DataFrame, defaults: Map[String, Any]): DataFrame =
    df.na.fill(defaults)

  /** Adds a monotonically increasing ID column. */
  def withRowId(df: DataFrame, idCol: String = "row_id"): DataFrame =
    df.withColumn(idCol, monotonically_increasing_id())

  /** Flattens a struct column into top-level columns. */
  def flattenStruct(df: DataFrame, structCol: String): DataFrame = {
    val fields = df.schema(structCol).dataType.asInstanceOf[StructType].fields
    fields.foldLeft(df) { (accDf, field) =>
      accDf.withColumn(s"${structCol}_${field.name}", col(s"$structCol.${field.name}"))
    }.drop(structCol)
  }

  /** Deduplicates rows by a subset of columns, keeping the first occurrence. */
  def deduplicateBy(df: DataFrame, columns: Seq[String]): DataFrame =
    df.dropDuplicates(columns)

  /** Pivots a DataFrame: rows become columns based on pivotCol values. */
  def pivotAggregation(
      df: DataFrame,
      groupCol: String,
      pivotCol: String,
      aggCol: String
  ): DataFrame =
    df.groupBy(groupCol).pivot(pivotCol).agg(sum(col(aggCol)))

  /** Unpivots (melts) selected value columns into key-value rows. */
  def unpivot(
      df: DataFrame,
      idCols: Seq[String],
      valueCols: Seq[String],
      keyColName: String,
      valueColName: String
  ): DataFrame = {
    val idColumns = idCols.map(col)
    val exprs = valueCols.map { c =>
      struct(lit(c).as(keyColName), col(c).cast(StringType).as(valueColName))
    }
    df.select(idColumns :+ explode(array(exprs: _*)).as("_tmp"): _*)
      .select(idColumns :+ col(s"_tmp.$keyColName").as(keyColName) :+
        col(s"_tmp.$valueColName").as(valueColName): _*)
  }

  /** Computes basic statistics (count, mean, stddev, min, max) for numeric columns. */
  def summaryStats(df: DataFrame, columns: Seq[String]): DataFrame =
    df.select(columns.map(col): _*).describe()
}
