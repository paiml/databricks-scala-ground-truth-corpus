# ML/MLflow Specification

## Scope
Feature engineering, MLlib pipeline construction, model evaluation metrics, and hyperparameter tuning.

## Modules
- **FeatureEngineering**: 7 feature transforms (OHE, MinMax, Standard, Bucketize, Assemble, TF-IDF, Impute)
- **PipelineBuilder**: 4 pipeline factories (classification, regression, preprocessing, fit)
- **ModelEvaluation**: 9 evaluation metrics (AUROC, AUPR, accuracy, F1, RMSE, R2, MAE, reports)
- **HyperparamTuning**: 8 tuning utilities (CV, TVS, evaluator factories, metric extraction)

## Falsification Targets
- Classification metrics must be in [0, 1]
- Regression RMSE and MAE must be non-negative
- R2 must be <= 1.0
- Pipeline stages count must match configured components
- Cross-validation metrics array length must equal param grid size
