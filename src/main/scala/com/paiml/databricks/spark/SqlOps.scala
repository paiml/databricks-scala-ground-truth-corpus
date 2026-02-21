package com.paiml.databricks.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Spark SQL operations for Databricks workloads.
  *
  * Wraps common SQL patterns including temp view management, parameterized queries, and CTAS.
  */
object SqlOps {

  /** Registers a DataFrame as a temporary SQL view and returns the view name. */
  def registerTempView(df: DataFrame, viewName: String): String = {
    df.createOrReplaceTempView(viewName)
    viewName
  }

  /** Executes a SQL query against the SparkSession. */
  def query(spark: SparkSession, sql: String): DataFrame =
    spark.sql(sql)

  /** Executes a SQL query with simple string parameter substitution.
    *
    * Parameters are referenced as `{key}` in the SQL string.
    */
  def parameterizedQuery(
      spark: SparkSession,
      sqlTemplate: String,
      params: Map[String, String]
  ): DataFrame = {
    val resolvedSql = params.foldLeft(sqlTemplate) { case (sql, (key, value)) =>
      sql.replace(s"{$key}", value)
    }
    spark.sql(resolvedSql)
  }

  /** Creates a database if it does not exist. */
  def createDatabase(spark: SparkSession, dbName: String): Unit = {
    val _ = spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
  }

  /** Lists all tables in a database. */
  def listTables(spark: SparkSession, dbName: String): DataFrame =
    spark.sql(s"SHOW TABLES IN $dbName")

  /** Describes the schema of a table. */
  def describeTable(spark: SparkSession, tableName: String): DataFrame =
    spark.sql(s"DESCRIBE TABLE $tableName")

  /** Runs EXPLAIN on a query to get the execution plan. */
  def explainQuery(spark: SparkSession, sql: String): String = {
    val df = spark.sql(sql)
    val plan = df.queryExecution.simpleString
    plan
  }

  /** Creates a table from a SQL SELECT (CTAS pattern). */
  def createTableAs(
      spark: SparkSession,
      tableName: String,
      selectSql: String
  ): Unit = {
    val _ = spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName AS $selectSql")
  }

  /** Drops a table if it exists. */
  def dropTable(spark: SparkSession, tableName: String): Unit = {
    val _ = spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
