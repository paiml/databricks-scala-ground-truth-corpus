package com.paiml.databricks.spark

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinPatternsSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  private lazy val orders = Seq(
    (1, "alice", 100.0), (2, "bob", 200.0), (3, "charlie", 300.0)
  ).toDF("order_id", "customer", "amount")

  private lazy val customers = Seq(
    ("alice", "US"), ("bob", "UK"), ("dave", "FR")
  ).toDF("customer", "country")

  behavior of "JoinPatterns"

  it should "perform broadcast join" in {
    val result = JoinPatterns.broadcastJoin(orders, customers, "customer")
    result.count() shouldBe 2
    result.columns should contain("country")
  }

  it should "perform anti join" in {
    val result = JoinPatterns.antiJoin(orders, customers, "customer")
    result.count() shouldBe 1
    result.first().getAs[String]("customer") shouldBe "charlie"
  }

  it should "perform semi join" in {
    val result = JoinPatterns.semiJoin(orders, customers, "customer")
    result.count() shouldBe 2
    result.columns should not contain "country"
  }

  it should "perform multi-column join" in {
    val left = Seq((1, "A", 10), (2, "B", 20)).toDF("id", "cat", "val")
    val right = Seq((1, "A", "x"), (2, "C", "y")).toDF("id", "cat", "label")
    val result = JoinPatterns.multiColumnJoin(left, right, Seq("id", "cat"))
    result.count() shouldBe 1
  }

  it should "perform salted join for skew handling" in {
    val skewed = Seq(
      (1, "a"), (1, "b"), (1, "c"), (2, "d")
    ).toDF("key", "val1")
    val other = Seq((1, "x"), (2, "y")).toDF("key", "val2")
    val result = JoinPatterns.saltedJoin(skewed, other, "key", 4)
    result.count() shouldBe 4
    result.columns should not contain "_salt"
  }

  it should "detect SCD Type 2 actions" in {
    val current = Seq(
      (1, "alice", "US"), (2, "bob", "UK")
    ).toDF("id", "name", "country")
    val incoming = Seq(
      (1, "alice", "US"),   // no change
      (2, "bob", "DE"),     // update
      (3, "charlie", "FR")  // insert
    ).toDF("id", "name", "country")

    val result = JoinPatterns.scdType2Detect(current, incoming, "id", Seq("name", "country"))
    val actions = result.select("_scd_action").as[String].collect()
    actions should contain("NO_CHANGE")
    actions should contain("UPDATE")
    actions should contain("INSERT")
  }

  it should "perform cross join with filter" in {
    val left = Seq(1, 2, 3).toDF("a")
    val right = Seq(10, 20).toDF("b")
    val result = JoinPatterns.crossJoinFiltered(left, right, Some("a + b > 12"))
    result.count() shouldBe 4 // (2,20), (3,10), (3,20), (2,20)... let me check
    // 1+10=11 no, 1+20=21 yes, 2+10=12 no, 2+20=22 yes, 3+10=13 yes, 3+20=23 yes
    result.count() shouldBe 4
  }

  it should "perform cross join without filter" in {
    val left = Seq(1, 2).toDF("a")
    val right = Seq(10, 20).toDF("b")
    val result = JoinPatterns.crossJoinFiltered(left, right)
    result.count() shouldBe 4
  }
}
