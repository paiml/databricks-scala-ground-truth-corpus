package com.paiml.databricks.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

/** Hyperparameter tuning patterns for Spark MLlib.
  *
  * Provides CrossValidator and TrainValidationSplit wrappers with
  * configurable param grids and evaluation metrics.
  */
object HyperparamTuning {

  /** Runs k-fold cross-validation and returns the best model. */
  def crossValidate(
      pipeline: Pipeline,
      paramGrid: Array[ParamMap],
      trainingData: DataFrame,
      evaluator: Evaluator,
      numFolds: Int = 5,
      parallelism: Int = 4
  ): CrossValidatorModel = {
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setNumFolds(numFolds)
      .setParallelism(parallelism)
    cv.fit(trainingData)
  }

  /** Runs train-validation split (faster than cross-validation). */
  def trainValidationSplit(
      pipeline: Pipeline,
      paramGrid: Array[ParamMap],
      trainingData: DataFrame,
      evaluator: Evaluator,
      trainRatio: Double = 0.8
  ): TrainValidationSplitModel = {
    val tvs = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setTrainRatio(trainRatio)
    tvs.fit(trainingData)
  }

  /** Creates a BinaryClassificationEvaluator for AUROC. */
  def binaryClassificationEvaluator(
      labelCol: String = "label",
      metric: String = "areaUnderROC"
  ): BinaryClassificationEvaluator =
    new BinaryClassificationEvaluator()
      .setLabelCol(labelCol)
      .setRawPredictionCol("rawPrediction")
      .setMetricName(metric)

  /** Creates a MulticlassClassificationEvaluator. */
  def multiclassEvaluator(
      labelCol: String = "label",
      metric: String = "accuracy"
  ): MulticlassClassificationEvaluator =
    new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName(metric)

  /** Creates a RegressionEvaluator. */
  def regressionEvaluator(
      labelCol: String = "label",
      metric: String = "rmse"
  ): RegressionEvaluator =
    new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName(metric)

  /** Extracts the average metrics from a CrossValidatorModel. */
  def crossValidationMetrics(model: CrossValidatorModel): Array[Double] =
    model.avgMetrics

  /** Extracts the validation metrics from a TrainValidationSplitModel. */
  def trainValMetrics(model: TrainValidationSplitModel): Array[Double] =
    model.validationMetrics

  /** Returns the best param map from a CrossValidatorModel. */
  def bestParams(model: CrossValidatorModel): ParamMap = {
    val bestIdx = model.avgMetrics.zipWithIndex.maxBy(_._1)._2
    model.getEstimatorParamMaps(bestIdx)
  }
}
