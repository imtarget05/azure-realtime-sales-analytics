# Metrics Report -- XGBoost

| Metric | XGBoost | Naive (lag_1) | Improvement |
|--------|-------------|---------------|-------------|
| MAE    | 794.45 | 1359.55 | 41.6% |
| RMSE   | 1141.21 | 2047.50 | 44.3% |
| MAPE   | 12.27% | 20.68% | 40.7% |
| R2     | 0.8585 | 0.5446 | -- |

**Training data**: 100,000 samples (< 2015)
**Validation data**: 196,029 samples (>= 2015)
**Stores**: 1,115
**Date range**: 2013-01-01 -> 2015-07-31
