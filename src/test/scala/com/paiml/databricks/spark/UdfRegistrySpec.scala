package com.paiml.databricks.spark

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen

class UdfRegistrySpec
    extends AnyFlatSpec
    with Matchers
    with SharedSparkSession
    with ScalaCheckPropertyChecks {

  import spark.implicits._

  behavior of "UdfRegistry"

  it should "normalize whitespace" in {
    val df = Seq("  hello   world  ", "no  extra", null).toDF("text")
    val result = df.select(UdfRegistry.normalizeWhitespace($"text").as("clean"))
      .as[String].collect()
    result(0) shouldBe "hello world"
    result(1) shouldBe "no extra"
    result(2) shouldBe null
  }

  it should "mask PII correctly" in {
    val df = Seq("123456789", "1234", "ab", null).toDF("ssn")
    val result = df.select(UdfRegistry.maskPii($"ssn").as("masked"))
      .as[String].collect()
    result(0) shouldBe "*****6789"
    result(1) shouldBe "1234"
    result(2) shouldBe "ab"
    result(3) shouldBe null
  }

  it should "extract email domain" in {
    val df = Seq("user@Example.COM", "bad-email", null).toDF("email")
    val result = df.select(UdfRegistry.emailDomain($"email").as("domain"))
      .as[String].collect()
    result(0) shouldBe "example.com"
    result(1) shouldBe null
    result(2) shouldBe null
  }

  it should "convert to title case" in {
    val df = Seq("hello world", "UPPER CASE", null).toDF("text")
    val result = df.select(UdfRegistry.titleCase($"text").as("titled"))
      .as[String].collect()
    result(0) shouldBe "Hello World"
    result(1) shouldBe "Upper Case"
    result(2) shouldBe null
  }

  it should "validate phone numbers" in {
    val df = Seq(
      "555-123-4567", "(555) 123 4567", "+1 555 1234567",
      "123", "not-a-phone", null
    ).toDF("phone")
    val result = df.select(UdfRegistry.isValidPhone($"phone").as("valid"))
      .as[Boolean].collect()
    result(0) shouldBe true
    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe false
    result(4) shouldBe false
    result(5) shouldBe false
  }

  it should "clamp values to range" in {
    val df = Seq(-5.0, 0.0, 50.0, 100.0, 150.0).toDF("value")
    val clamp = UdfRegistry.clampUdf(0.0, 100.0)
    val result = df.select(clamp($"value").as("clamped")).as[Double].collect()
    result should contain theSameElementsInOrderAs Seq(0.0, 0.0, 50.0, 100.0, 100.0)
  }

  it should "register all UDFs without error" in {
    noException should be thrownBy UdfRegistry.registerAll(spark)
    // Verify one is callable via SQL
    val df = Seq("  test  ").toDF("text")
    SqlOps.registerTempView(df, "udf_test")
    UdfRegistry.registerAll(spark)
    val result = SqlOps.query(spark, "SELECT normalize_whitespace(text) as clean FROM udf_test")
    result.first().getString(0) shouldBe "test"
  }

  it should "handle normalizeWhitespace property: trimmed output has no leading/trailing spaces" in {
    forAll(Gen.alphaNumStr) { s: String =>
      whenever(s.nonEmpty) {
        val df = Seq(s"  $s  ").toDF("text")
        val result = df.select(UdfRegistry.normalizeWhitespace($"text").as("clean"))
          .as[String].collect().head
        result shouldBe result.trim
      }
    }
  }
}
