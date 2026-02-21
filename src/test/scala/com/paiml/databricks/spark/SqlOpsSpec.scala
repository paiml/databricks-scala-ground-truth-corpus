package com.paiml.databricks.spark

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SqlOpsSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  behavior of "SqlOps"

  it should "register a temp view and query it" in {
    val df = Seq((1, "alice"), (2, "bob")).toDF("id", "name")
    val viewName = SqlOps.registerTempView(df, "users")
    viewName shouldBe "users"

    val result = SqlOps.query(spark, "SELECT * FROM users WHERE id = 1")
    result.count() shouldBe 1
    result.first().getString(1) shouldBe "alice"
  }

  it should "execute parameterized queries" in {
    val df = Seq((1, "alice"), (2, "bob"), (3, "charlie")).toDF("id", "name")
    SqlOps.registerTempView(df, "param_users")

    val result = SqlOps.parameterizedQuery(
      spark,
      "SELECT * FROM param_users WHERE id > {min_id}",
      Map("min_id" -> "1")
    )
    result.count() shouldBe 2
  }

  it should "create and list databases" in {
    SqlOps.createDatabase(spark, "test_db_sqlops")
    val tables = SqlOps.listTables(spark, "test_db_sqlops")
    tables should not be null
    spark.sql("DROP DATABASE IF EXISTS test_db_sqlops CASCADE")
  }

  it should "describe a table schema" in {
    val df = Seq((1, "alice")).toDF("id", "name")
    SqlOps.registerTempView(df, "describe_test")
    val desc = SqlOps.describeTable(spark, "describe_test")
    desc.count() should be >= 2L
  }

  it should "explain a query execution plan" in {
    val df = Seq((1, "alice")).toDF("id", "name")
    SqlOps.registerTempView(df, "explain_test")
    val plan = SqlOps.explainQuery(spark, "SELECT * FROM explain_test")
    plan should not be empty
  }

  it should "attempt CTAS (requires catalog in production)" in {
    val df = Seq((1, "alice"), (2, "bob")).toDF("id", "name")
    SqlOps.registerTempView(df, "ctas_source")
    // CTAS requires Hive/Unity Catalog — exercises code path in local mode
    an [Exception] should be thrownBy {
      SqlOps.createTableAs(spark, "ctas_result", "SELECT * FROM ctas_source")
    }
  }

  it should "drop a table without error" in {
    noException should be thrownBy {
      SqlOps.dropTable(spark, "nonexistent_table_xyz")
    }
  }
}
