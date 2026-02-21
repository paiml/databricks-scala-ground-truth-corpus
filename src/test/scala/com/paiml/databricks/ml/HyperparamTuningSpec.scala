package com.paiml.databricks.ml

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.tuning.ParamGridBuilder

class HyperparamTuningSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  private lazy val trainingData = {
    val data = Seq(
      (0.0, 1.0, 2.0), (0.0, 2.0, 3.0), (0.0, 3.0, 4.0),
      (1.0, 10.0, 11.0), (1.0, 11.0, 12.0), (1.0, 12.0, 13.0),
      (0.0, 1.5, 2.5), (1.0, 10.5, 11.5),
      (0.0, 2.5, 3.5), (1.0, 11.5, 12.5)
    ).toDF("label", "f1", "f2")
    val assembler = new VectorAssembler()
      .setInputCols(Array("f1", "f2")).setOutputCol("features")
    assembler.transform(data)
  }

  behavior of "HyperparamTuning"

  it should "create binary classification evaluator" in {
    val evaluator = HyperparamTuning.binaryClassificationEvaluator()
    evaluator.getMetricName shouldBe "areaUnderROC"
  }

  it should "create multiclass evaluator" in {
    val evaluator = HyperparamTuning.multiclassEvaluator(metric = "f1")
    evaluator.getMetricName shouldBe "f1"
  }

  it should "create regression evaluator" in {
    val evaluator = HyperparamTuning.regressionEvaluator(metric = "mae")
    evaluator.getMetricName shouldBe "mae"
  }

  it should "run cross-validation and return best model" in {
    val lr = new LogisticRegression()
    val pipeline = new Pipeline().setStages(Array[PipelineStage](lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01, 0.1))
      .build()
    val evaluator = HyperparamTuning.binaryClassificationEvaluator()

    val cvModel = HyperparamTuning.crossValidate(
      pipeline, paramGrid, trainingData, evaluator, numFolds = 2, parallelism = 1
    )
    cvModel should not be null
    val metrics = HyperparamTuning.crossValidationMetrics(cvModel)
    metrics should have length 2
    metrics.foreach(_ should be >= 0.0)
  }

  it should "extract best params from cross-validation" in {
    val lr = new LogisticRegression()
    val pipeline = new Pipeline().setStages(Array[PipelineStage](lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01, 0.1))
      .build()
    val evaluator = HyperparamTuning.binaryClassificationEvaluator()

    val cvModel = HyperparamTuning.crossValidate(
      pipeline, paramGrid, trainingData, evaluator, numFolds = 2, parallelism = 1
    )
    val best = HyperparamTuning.bestParams(cvModel)
    best should not be null
  }

  it should "run train-validation split" in {
    val lr = new LogisticRegression()
    val pipeline = new Pipeline().setStages(Array[PipelineStage](lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.01, 0.1))
      .build()
    val evaluator = HyperparamTuning.binaryClassificationEvaluator()

    val tvsModel = HyperparamTuning.trainValidationSplit(
      pipeline, paramGrid, trainingData, evaluator, trainRatio = 0.7
    )
    tvsModel should not be null
    val metrics = HyperparamTuning.trainValMetrics(tvsModel)
    metrics should have length 2
  }
}
