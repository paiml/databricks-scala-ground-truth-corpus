package com.paiml.databricks.spark

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WindowFunctionsSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  private lazy val salesDf = Seq(
    ("electronics", "laptop", 1000.0),
    ("electronics", "phone", 800.0),
    ("electronics", "tablet", 600.0),
    ("clothing", "shirt", 50.0),
    ("clothing", "pants", 80.0),
    ("clothing", "jacket", 120.0)
  ).toDF("category", "product", "price")

  behavior of "WindowFunctions"

  it should "assign row numbers within partitions" in {
    val result = WindowFunctions.withRowNumber(salesDf, "category", "price")
    val electronics = result.filter("category = 'electronics'")
      .orderBy("row_num").select("row_num").as[Int].collect()
    electronics shouldBe Array(1, 2, 3)
  }

  it should "assign rank with ties" in {
    val df = Seq(
      ("A", 10), ("A", 10), ("A", 20)
    ).toDF("group", "score")
    val result = WindowFunctions.withRank(df, "group", "score")
    val ranks = result.orderBy("score").select("rank").as[Int].collect()
    ranks shouldBe Array(1, 1, 3)
  }

  it should "assign dense rank without gaps" in {
    val df = Seq(
      ("A", 10), ("A", 10), ("A", 20)
    ).toDF("group", "score")
    val result = WindowFunctions.withDenseRank(df, "group", "score")
    val ranks = result.orderBy("score").select("dense_rank").as[Int].collect()
    ranks shouldBe Array(1, 1, 2)
  }

  it should "compute running sum within partitions" in {
    val df = Seq(
      ("A", 1, 10.0), ("A", 2, 20.0), ("A", 3, 30.0)
    ).toDF("group", "order", "amount")
    val result = WindowFunctions.withRunningSum(df, "group", "order", "amount")
    val sums = result.orderBy("order").select("running_sum").as[Double].collect()
    sums shouldBe Array(10.0, 30.0, 60.0)
  }

  it should "compute moving average" in {
    val df = Seq(
      ("A", 1, 10.0), ("A", 2, 20.0), ("A", 3, 30.0), ("A", 4, 40.0)
    ).toDF("group", "order", "value")
    val result = WindowFunctions.withMovingAverage(df, "group", "order", "value", 2)
    val avgs = result.orderBy("order").select("moving_avg").as[Double].collect()
    avgs(0) shouldBe 10.0 // only 1 row in window
    avgs(1) shouldBe 15.0 // avg(10, 20)
    avgs(2) shouldBe 25.0 // avg(20, 30)
    avgs(3) shouldBe 35.0 // avg(30, 40)
  }

  it should "add lead and lag columns" in {
    val df = Seq(
      ("A", 1, 100), ("A", 2, 200), ("A", 3, 300)
    ).toDF("group", "order", "value")
    val result = WindowFunctions.withLeadLag(df, "group", "order", "value")
    val row = result.filter("order = 2").first()
    row.getAs[Int]("value_lead") shouldBe 300
    row.getAs[Int]("value_lag") shouldBe 100
  }

  it should "compute percent rank" in {
    val result = WindowFunctions.withPercentRank(salesDf, "category", "price")
    result.columns should contain("pct_rank")
    result.count() shouldBe 6
  }

  it should "assign ntile buckets" in {
    val result = WindowFunctions.withNtile(salesDf, "category", "price", 2)
    val buckets = result.select("bucket").as[Int].collect().distinct
    buckets should contain allOf (1, 2)
  }

  it should "select top-K per partition ascending" in {
    val result = WindowFunctions.topKPerPartition(salesDf, "category", "price", 2, ascending = true)
    result.count() shouldBe 4 // 2 per category
    val cheapestElectronics = result.filter("category = 'electronics'")
      .orderBy("price").select("price").as[Double].collect()
    cheapestElectronics should have length 2
    cheapestElectronics(0) shouldBe 600.0
  }

  it should "select top-K per partition descending" in {
    val result = WindowFunctions.topKPerPartition(salesDf, "category", "price", 1, ascending = false)
    result.count() shouldBe 2
    val mostExpensive = result.filter("category = 'electronics'")
      .select("price").as[Double].collect()
    mostExpensive.head shouldBe 1000.0
  }
}
