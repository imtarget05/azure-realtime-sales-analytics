"""
Tests for ML training pipeline — train_model, retrain_and_compare, scoring.
"""

import os
import json
import pytest
import pandas as pd
import numpy as np

from ml.train_model import generate_training_data, prepare_features, train_one_model


# ── Data Generation ─────────────────────────────────────────────────

class TestDataGeneration:
    def test_generate_correct_row_count(self):
        df = generate_training_data(500)
        assert len(df) == 500

    def test_generate_has_required_columns(self):
        df = generate_training_data(100)
        required = ["hour", "month", "is_weekend", "temperature",
                     "is_rainy", "holiday", "revenue", "quantity"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_generate_no_nulls(self):
        df = generate_training_data(200)
        assert df.isnull().sum().sum() == 0

    def test_generate_revenue_non_negative(self):
        df = generate_training_data(300)
        assert (df["revenue"] >= 0).all()

    def test_generate_quantity_positive(self):
        df = generate_training_data(300)
        assert (df["quantity"] > 0).all()

    def test_generate_hour_range(self):
        df = generate_training_data(500)
        assert df["hour"].min() >= 0
        assert df["hour"].max() <= 23

    def test_generate_month_range(self):
        df = generate_training_data(500)
        assert df["month"].min() >= 1
        assert df["month"].max() <= 12


# ── Feature Preparation ─────────────────────────────────────────────

class TestFeaturePreparation:
    def test_prepare_returns_correct_shapes(self):
        df = generate_training_data(500)
        X, y_qty, y_rev, label_encoders, cat_features = prepare_features(df)
        assert X.shape[0] == 500
        assert len(y_qty) == 500
        assert len(y_rev) == 500
        assert X.shape[1] > 0

    def test_prepare_label_encoders_returned(self):
        df = generate_training_data(100)
        _, _, _, label_encoders, _ = prepare_features(df)
        assert isinstance(label_encoders, dict)
        assert len(label_encoders) > 0

    def test_prepare_missing_column_raises(self):
        df = generate_training_data(100)
        df = df.drop(columns=["temperature"])
        with pytest.raises(KeyError):
            prepare_features(df)


# ── Model Training ──────────────────────────────────────────────────

class TestModelTraining:
    @pytest.fixture(scope="class")
    def trained_model(self):
        df = generate_training_data(1000)
        X, y_qty, y_rev, _, _ = prepare_features(df)
        model, metrics, importances, X_test, y_test_arr, y_test, y_pred = \
            train_one_model(X, y_qty, name="test_qty")
        return model, metrics, y_test, y_pred

    def test_r2_within_bounds(self, trained_model):
        _, metrics, _, _ = trained_model
        assert -1.0 <= metrics["r2_score"] <= 1.0

    def test_mae_non_negative(self, trained_model):
        _, metrics, _, _ = trained_model
        assert metrics["mae"] >= 0

    def test_rmse_non_negative(self, trained_model):
        _, metrics, _, _ = trained_model
        assert metrics["rmse"] >= 0

    def test_predictions_same_length(self, trained_model):
        _, _, y_test, y_pred = trained_model
        assert len(y_pred) == len(y_test)

    def test_model_not_none(self, trained_model):
        model, _, _, _ = trained_model
        assert model is not None

    def test_model_can_predict(self, trained_model):
        model, _, _, _ = trained_model
        df = generate_training_data(10)
        X, _, _, _, _ = prepare_features(df)
        preds = model.predict(X)
        assert len(preds) == 10


# ── Drift Monitor ───────────────────────────────────────────────────

class TestDriftMonitor:
    def test_compute_metrics_perfect(self):
        from ml.drift_monitor import compute_metrics
        df = pd.DataFrame({
            "predicted_revenue": [100.0, 200.0, 300.0],
            "actual_revenue": [100.0, 200.0, 300.0],
        })
        metrics = compute_metrics(df)
        assert metrics["mae"] == 0.0
        assert metrics["n_samples"] == 3

    def test_compute_metrics_with_drift(self):
        from ml.drift_monitor import compute_metrics
        df = pd.DataFrame({
            "predicted_revenue": [100.0, 200.0, 300.0],
            "actual_revenue": [150.0, 250.0, 350.0],
        })
        metrics = compute_metrics(df)
        assert metrics["mae"] == 50.0
        assert metrics["mape"] > 0

    def test_compute_metrics_empty(self):
        from ml.drift_monitor import compute_metrics
        df = pd.DataFrame(columns=["predicted_revenue", "actual_revenue"])
        metrics = compute_metrics(df)
        assert metrics["n_samples"] == 0
        assert metrics["mae"] == 0.0

    def test_compute_metrics_single_sample(self):
        from ml.drift_monitor import compute_metrics
        df = pd.DataFrame({
            "predicted_revenue": [100.0],
            "actual_revenue": [120.0],
        })
        metrics = compute_metrics(df)
        assert metrics["n_samples"] == 1
        assert metrics["mae"] == 20.0
