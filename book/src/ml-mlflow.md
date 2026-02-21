# ML and MLflow

## FeatureEngineering

Feature transforms: one-hot encoding, scaling, bucketization, TF-IDF, imputation.

```scala
import com.paiml.databricks.ml.FeatureEngineering

val (encoded, indexer, encoder) = FeatureEngineering.oneHotEncode(df, "color", "color_ohe")
val features = FeatureEngineering.assembleFeatures(df, Array("f1", "f2", "f3"))
```

## PipelineBuilder

MLlib pipeline construction for classification and regression.

```scala
import com.paiml.databricks.ml.PipelineBuilder

val pipeline = PipelineBuilder.classificationPipeline(
  Array("f1", "f2", "f3"), "label", "random_forest")
val model = PipelineBuilder.fitPipeline(pipeline, trainingData)
```

## ModelEvaluation

Evaluation metrics for classification (AUROC, F1, accuracy) and regression (RMSE, R2, MAE).

```scala
import com.paiml.databricks.ml.ModelEvaluation

val report = ModelEvaluation.classificationReport(predictions)
// Map(accuracy -> 0.95, f1 -> 0.94, weightedPrecision -> 0.95, weightedRecall -> 0.95)
```

## HyperparamTuning

Cross-validation and train-validation split with configurable param grids.

```scala
import com.paiml.databricks.ml.HyperparamTuning

val cvModel = HyperparamTuning.crossValidate(
  pipeline, paramGrid, trainingData, evaluator, numFolds = 5)
val bestConfig = HyperparamTuning.bestParams(cvModel)
```
