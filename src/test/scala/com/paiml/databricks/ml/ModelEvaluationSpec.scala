package com.paiml.databricks.ml

import com.paiml.databricks.SharedSparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

class ModelEvaluationSpec extends AnyFlatSpec with Matchers with SharedSparkSession {

  import spark.implicits._

  private lazy val classificationPredictions = {
    val data = Seq(
      (0.0, 1.0, 2.0), (0.0, 2.0, 3.0), (0.0, 3.0, 4.0),
      (1.0, 10.0, 11.0), (1.0, 11.0, 12.0), (1.0, 12.0, 13.0),
      (0.0, 1.5, 2.5), (1.0, 10.5, 11.5)
    ).toDF("label", "f1", "f2")
    val assembler = new VectorAssembler()
      .setInputCols(Array("f1", "f2")).setOutputCol("features")
    val assembled = assembler.transform(data)
    val lr = new LogisticRegression().setMaxIter(10)
    lr.fit(assembled).transform(assembled)
  }

  private lazy val regressionPredictions = {
    val data = Seq(
      (10.0, 1.0, 2.0), (20.0, 2.0, 3.0), (30.0, 3.0, 4.0),
      (40.0, 4.0, 5.0), (50.0, 5.0, 6.0), (60.0, 6.0, 7.0)
    ).toDF("label", "f1", "f2")
    val assembler = new VectorAssembler()
      .setInputCols(Array("f1", "f2")).setOutputCol("features")
    val assembled = assembler.transform(data)
    val lr = new LinearRegression().setMaxIter(10)
    lr.fit(assembled).transform(assembled)
  }

  behavior of "ModelEvaluation"

  it should "compute binary AUROC" in {
    val auroc = ModelEvaluation.binaryAUROC(classificationPredictions)
    auroc should be >= 0.0
    auroc should be <= 1.0
  }

  it should "compute binary AUPR" in {
    val aupr = ModelEvaluation.binaryAUPR(classificationPredictions)
    aupr should be >= 0.0
    aupr should be <= 1.0
  }

  it should "compute multiclass accuracy" in {
    val accuracy = ModelEvaluation.multiclassAccuracy(classificationPredictions)
    accuracy should be >= 0.0
    accuracy should be <= 1.0
  }

  it should "compute multiclass F1" in {
    val f1 = ModelEvaluation.multiclassF1(classificationPredictions)
    f1 should be >= 0.0
    f1 should be <= 1.0
  }

  it should "compute regression RMSE" in {
    val rmse = ModelEvaluation.regressionRMSE(regressionPredictions)
    rmse should be >= 0.0
  }

  it should "compute regression R-squared" in {
    val r2 = ModelEvaluation.regressionR2(regressionPredictions)
    r2 should be <= 1.0
  }

  it should "compute regression MAE" in {
    val mae = ModelEvaluation.regressionMAE(regressionPredictions)
    mae should be >= 0.0
  }

  it should "generate classification report with all metrics" in {
    val report = ModelEvaluation.classificationReport(classificationPredictions)
    report should contain key "accuracy"
    report should contain key "f1"
    report should contain key "weightedPrecision"
    report should contain key "weightedRecall"
  }

  it should "generate regression report with all metrics" in {
    val report = ModelEvaluation.regressionReport(regressionPredictions)
    report should contain key "rmse"
    report should contain key "mse"
    report should contain key "mae"
    report should contain key "r2"
  }
}
