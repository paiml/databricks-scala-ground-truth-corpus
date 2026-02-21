package com.paiml.databricks.spark

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.apache.spark.sql.types._

class DataFrameOpsSpec
    extends AnyFlatSpec
    with Matchers
    with SharedSparkSession
    with ScalaCheckPropertyChecks {

  import spark.implicits._

  behavior of "DataFrameOps"

  it should "add string length column" in {
    val df = Seq("hello", "world!", "").toDF("text")
    val result = DataFrameOps.withStringLength(df, "text", "text_len")
    val lengths = result.select("text_len").as[Int].collect()
    lengths should contain theSameElementsAs Seq(5, 6, 0)
  }

  it should "filter rows within a numeric range" in {
    val df = Seq(1.0, 2.5, 3.0, 4.5, 5.0).toDF("value")
    val result = DataFrameOps.filterRange(df, "value", 2.0, 4.0)
    result.count() shouldBe 2
  }

  it should "rename columns according to mapping" in {
    val df = Seq((1, "a")).toDF("id", "name")
    val result = DataFrameOps.renameColumns(df, Map("id" -> "user_id", "name" -> "user_name"))
    result.columns should contain theSameElementsAs Seq("user_id", "user_name")
  }

  it should "cast a column to a target type" in {
    val df = Seq("1", "2", "3").toDF("num_str")
    val result = DataFrameOps.castColumn(df, "num_str", IntegerType)
    result.schema("num_str").dataType shouldBe IntegerType
  }

  it should "drop rows with nulls in specified columns" in {
    val df = Seq(
      (Some(1), Some("a")),
      (None, Some("b")),
      (Some(3), None)
    ).toDF("id", "name")
    val result = DataFrameOps.dropNullsIn(df, Seq("id"))
    result.count() shouldBe 2
  }

  it should "fill nulls with defaults" in {
    val df = Seq(
      (Some(1), Some("a")),
      (None, None)
    ).toDF("id", "name")
    val result = DataFrameOps.fillNulls(df, Map("id" -> 0, "name" -> "unknown"))
    val row = result.filter("id = 0").first()
    row.getString(1) shouldBe "unknown"
  }

  it should "add monotonically increasing row IDs" in {
    val df = Seq("a", "b", "c").toDF("letter")
    val result = DataFrameOps.withRowId(df, "rid")
    result.columns should contain("rid")
    result.select("rid").distinct().count() shouldBe 3
  }

  it should "flatten a struct column" in {
    val df = Seq((1, (10, "inner"))).toDF("id", "details")
    val result = DataFrameOps.flattenStruct(df, "details")
    result.columns should contain allOf ("details__1", "details__2")
    result.columns should not contain "details"
  }

  it should "deduplicate by subset of columns" in {
    val df = Seq((1, "a"), (1, "b"), (2, "c")).toDF("id", "val")
    val result = DataFrameOps.deduplicateBy(df, Seq("id"))
    result.count() shouldBe 2
  }

  it should "pivot aggregation" in {
    val df = Seq(
      ("A", "X", 10), ("A", "Y", 20),
      ("B", "X", 30), ("B", "Y", 40)
    ).toDF("group", "category", "amount")
    val result = DataFrameOps.pivotAggregation(df, "group", "category", "amount")
    result.columns should contain allOf ("group", "X", "Y")
    result.count() shouldBe 2
  }

  it should "unpivot value columns into key-value rows" in {
    val df = Seq(("A", 10, 20), ("B", 30, 40)).toDF("id", "metric1", "metric2")
    val result = DataFrameOps.unpivot(df, Seq("id"), Seq("metric1", "metric2"), "metric", "value")
    result.count() shouldBe 4
    result.columns should contain allOf ("id", "metric", "value")
  }

  it should "compute summary statistics" in {
    val df = Seq(1.0, 2.0, 3.0, 4.0, 5.0).toDF("val")
    val result = DataFrameOps.summaryStats(df, Seq("val"))
    result.count() shouldBe 5 // count, mean, stddev, min, max
  }
}
