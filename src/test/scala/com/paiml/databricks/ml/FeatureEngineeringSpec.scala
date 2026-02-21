package com.paiml.databricks.ml

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FeatureEngineeringSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  behavior of "FeatureEngineering"

  it should "one-hot encode a categorical column" in {
    val df = Seq("red", "blue", "green", "red").toDF("color")
    val (result, indexer, encoder) = FeatureEngineering.oneHotEncode(df, "color", "color_ohe")
    result.columns should contain("color_ohe")
    result.count() shouldBe 4
    indexer should not be null
    encoder should not be null
  }

  it should "min-max scale a numeric column" in {
    val df = Seq(0.0, 50.0, 100.0).toDF("value")
    val (result, scaler) = FeatureEngineering.minMaxScale(df, "value", "value_scaled")
    result.columns should contain("value_scaled")
    scaler should not be null
  }

  it should "standard scale a numeric column" in {
    val df = Seq(10.0, 20.0, 30.0).toDF("value")
    val (result, scaler) = FeatureEngineering.standardScale(df, "value", "value_std")
    result.columns should contain("value_std")
    scaler should not be null
  }

  it should "bucketize a continuous column" in {
    val df = Seq(5.0, 15.0, 25.0, 35.0).toDF("age")
    val splits = Array(Double.NegativeInfinity, 10.0, 20.0, 30.0, Double.PositiveInfinity)
    val result = FeatureEngineering.bucketize(df, "age", "age_bucket", splits)
    val buckets = result.select("age_bucket").as[Double].collect()
    buckets shouldBe Array(0.0, 1.0, 2.0, 3.0)
  }

  it should "assemble features into a vector" in {
    val df = Seq((1.0, 2.0, 3.0)).toDF("f1", "f2", "f3")
    val result = FeatureEngineering.assembleFeatures(df, Array("f1", "f2", "f3"))
    result.columns should contain("features")
    result.count() shouldBe 1
  }

  it should "extract TF-IDF features from text" in {
    val df = Seq(
      "hello world foo",
      "world bar baz",
      "hello foo bar"
    ).toDF("text")
    val result = FeatureEngineering.tfidfFeatures(df, "text", "tfidf", numFeatures = 100)
    result.columns should contain("tfidf")
    result.count() shouldBe 3
  }

  it should "impute missing values with mean" in {
    val df = Seq(
      (Some(10.0), Some(1.0)),
      (Some(20.0), None),
      (None, Some(3.0))
    ).toDF("a", "b")
    val result = FeatureEngineering.imputeMean(df, Seq("a", "b"))
    result.columns should contain allOf ("a_imputed", "b_imputed")
    result.count() shouldBe 3
  }
}
