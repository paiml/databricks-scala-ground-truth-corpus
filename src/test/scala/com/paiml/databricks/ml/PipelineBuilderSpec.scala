package com.paiml.databricks.ml

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PipelineBuilderSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  behavior of "PipelineBuilder"

  private lazy val classificationData = {
    Seq(
      (1.0, 2.0, 3.0, "A"), (4.0, 5.0, 6.0, "B"),
      (7.0, 8.0, 9.0, "A"), (2.0, 3.0, 4.0, "B"),
      (5.0, 6.0, 7.0, "A"), (8.0, 9.0, 10.0, "B"),
      (1.5, 2.5, 3.5, "A"), (4.5, 5.5, 6.5, "B"),
      (3.0, 4.0, 5.0, "A"), (6.0, 7.0, 8.0, "B")
    ).toDF("f1", "f2", "f3", "label")
  }

  private lazy val regressionData = {
    Seq(
      (1.0, 2.0, 10.0), (2.0, 3.0, 20.0),
      (3.0, 4.0, 30.0), (4.0, 5.0, 40.0),
      (5.0, 6.0, 50.0), (6.0, 7.0, 60.0),
      (7.0, 8.0, 70.0), (8.0, 9.0, 80.0)
    ).toDF("f1", "f2", "target")
  }

  it should "build and fit a logistic regression pipeline" in {
    val pipeline = PipelineBuilder.classificationPipeline(
      Array("f1", "f2", "f3"), "label", "logistic_regression"
    )
    pipeline.getStages should have length 3
    val model = PipelineBuilder.fitPipeline(pipeline, classificationData)
    model should not be null
  }

  it should "build and fit a random forest classifier pipeline" in {
    val pipeline = PipelineBuilder.classificationPipeline(
      Array("f1", "f2", "f3"), "label", "random_forest"
    )
    val model = PipelineBuilder.fitPipeline(pipeline, classificationData)
    model should not be null
  }

  it should "build and fit a GBT classifier pipeline" in {
    val pipeline = PipelineBuilder.classificationPipeline(
      Array("f1", "f2", "f3"), "label", "gbt"
    )
    val model = PipelineBuilder.fitPipeline(pipeline, classificationData)
    model should not be null
  }

  it should "build and fit a linear regression pipeline" in {
    val pipeline = PipelineBuilder.regressionPipeline(
      Array("f1", "f2"), "target", "linear_regression"
    )
    pipeline.getStages should have length 2
    val model = PipelineBuilder.fitPipeline(pipeline, regressionData)
    model should not be null
  }

  it should "build and fit a random forest regressor pipeline" in {
    val pipeline = PipelineBuilder.regressionPipeline(
      Array("f1", "f2"), "target", "random_forest"
    )
    val model = PipelineBuilder.fitPipeline(pipeline, regressionData)
    model should not be null
  }

  it should "build and fit a GBT regressor pipeline" in {
    val pipeline = PipelineBuilder.regressionPipeline(
      Array("f1", "f2"), "target", "gbt"
    )
    val model = PipelineBuilder.fitPipeline(pipeline, regressionData)
    model should not be null
  }

  it should "build a preprocessing pipeline with categorical and numeric columns" in {
    val pipeline = PipelineBuilder.preprocessingPipeline(
      Array("cat1"), Array("num1", "num2")
    )
    pipeline.getStages should have length 3 // indexer, encoder, assembler
  }
}
