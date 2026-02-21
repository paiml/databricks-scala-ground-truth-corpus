package com.paiml.databricks.ml

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier, GBTClassifier}
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressor, GBTRegressor}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame

/** MLlib Pipeline construction patterns.
  *
  * Provides factory methods for building end-to-end ML pipelines with
  * preprocessing stages, feature assembly, and model training.
  */
object PipelineBuilder {

  /** Builds a classification pipeline with string indexing, feature assembly, and a classifier. */
  def classificationPipeline(
      featureCols: Array[String],
      labelCol: String,
      classifier: String = "logistic_regression"
  ): Pipeline = {
    val labelIndexer = new StringIndexer()
      .setInputCol(labelCol)
      .setOutputCol("label_idx")
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val model: PipelineStage = classifier match {
      case "random_forest" =>
        new RandomForestClassifier()
          .setLabelCol("label_idx")
          .setFeaturesCol("features")
          .setNumTrees(100)
          .setMaxDepth(5)
      case "gbt" =>
        new GBTClassifier()
          .setLabelCol("label_idx")
          .setFeaturesCol("features")
          .setMaxIter(20)
          .setMaxDepth(5)
      case _ =>
        new LogisticRegression()
          .setLabelCol("label_idx")
          .setFeaturesCol("features")
          .setMaxIter(100)
          .setRegParam(0.01)
    }

    new Pipeline().setStages(Array(labelIndexer, assembler, model))
  }

  /** Builds a regression pipeline with feature assembly and regressor. */
  def regressionPipeline(
      featureCols: Array[String],
      labelCol: String,
      regressor: String = "linear_regression"
  ): Pipeline = {
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val model: PipelineStage = regressor match {
      case "random_forest" =>
        new RandomForestRegressor()
          .setLabelCol(labelCol)
          .setFeaturesCol("features")
          .setNumTrees(100)
          .setMaxDepth(5)
      case "gbt" =>
        new GBTRegressor()
          .setLabelCol(labelCol)
          .setFeaturesCol("features")
          .setMaxIter(20)
          .setMaxDepth(5)
      case _ =>
        new LinearRegression()
          .setLabelCol(labelCol)
          .setFeaturesCol("features")
          .setMaxIter(100)
          .setRegParam(0.01)
          .setElasticNetParam(0.5)
    }

    new Pipeline().setStages(Array(assembler, model))
  }

  /** Fits a pipeline and returns the trained model. */
  def fitPipeline(pipeline: Pipeline, trainingData: DataFrame): PipelineModel =
    pipeline.fit(trainingData)

  /** Creates a preprocessing-only pipeline (no model) for feature transformation. */
  def preprocessingPipeline(
      categoricalCols: Array[String],
      numericCols: Array[String]
  ): Pipeline = {
    val indexers = categoricalCols.map { c =>
      new StringIndexer()
        .setInputCol(c)
        .setOutputCol(s"${c}_idx")
        .setHandleInvalid("keep")
    }

    val encoders = categoricalCols.map { c =>
      new OneHotEncoder()
        .setInputCol(s"${c}_idx")
        .setOutputCol(s"${c}_ohe")
        .setDropLast(true)
    }

    val allFeatureCols = categoricalCols.map(c => s"${c}_ohe") ++ numericCols
    val assembler = new VectorAssembler()
      .setInputCols(allFeatureCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    new Pipeline().setStages(indexers ++ encoders :+ assembler)
  }
}
