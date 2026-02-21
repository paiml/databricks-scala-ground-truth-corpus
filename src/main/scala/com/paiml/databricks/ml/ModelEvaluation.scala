package com.paiml.databricks.ml

import org.apache.spark.ml.evaluation._
import org.apache.spark.sql.DataFrame

/** Model evaluation utilities for Spark MLlib.
  *
  * Provides evaluation metrics for classification, regression, and ranking models
  * using Spark's built-in evaluators.
  */
object ModelEvaluation {

  /** Evaluates binary classification using area under ROC. */
  def binaryAUROC(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelCol)
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
    evaluator.evaluate(predictions)
  }

  /** Evaluates binary classification using area under PR curve. */
  def binaryAUPR(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelCol)
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderPR")
    evaluator.evaluate(predictions)
  }

  /** Evaluates multiclass classification accuracy. */
  def multiclassAccuracy(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    evaluator.evaluate(predictions)
  }

  /** Evaluates multiclass F1 score. */
  def multiclassF1(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("f1")
    evaluator.evaluate(predictions)
  }

  /** Evaluates regression using RMSE. */
  def regressionRMSE(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    evaluator.evaluate(predictions)
  }

  /** Evaluates regression using R-squared. */
  def regressionR2(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("r2")
    evaluator.evaluate(predictions)
  }

  /** Evaluates regression using MAE. */
  def regressionMAE(predictions: DataFrame, labelCol: String = "label"): Double = {
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("mae")
    evaluator.evaluate(predictions)
  }

  /** Returns a map of all classification metrics for a predictions DataFrame. */
  def classificationReport(
      predictions: DataFrame,
      labelCol: String = "label"
  ): Map[String, Double] = {
    val metrics = Seq("accuracy", "weightedPrecision", "weightedRecall", "f1")
    metrics.map { metric =>
      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol(labelCol)
        .setPredictionCol("prediction")
        .setMetricName(metric)
      metric -> evaluator.evaluate(predictions)
    }.toMap
  }

  /** Returns a map of all regression metrics for a predictions DataFrame. */
  def regressionReport(
      predictions: DataFrame,
      labelCol: String = "label"
  ): Map[String, Double] = {
    val metrics = Seq("rmse", "mse", "mae", "r2")
    metrics.map { metric =>
      val evaluator = new RegressionEvaluator()
        .setLabelCol(labelCol)
        .setPredictionCol("prediction")
        .setMetricName(metric)
      metric -> evaluator.evaluate(predictions)
    }.toMap
  }
}
