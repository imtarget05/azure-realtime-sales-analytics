import json
import pytest
from datetime import datetime, timezone
from ml.drift_monitor import compute_metrics
import pandas as pd
import numpy as np

# ─────────────────────────────────────────────────────────────
# 1. Test Drift Monitor Logic
# ─────────────────────────────────────────────────────────────

def test_compute_metrics_with_drift():
    """Test if MAE is calculated correctly when there is drift."""
    data = {
        "predicted_revenue": [100.0, 200.0, 300.0],
        "actual_revenue": [150.0, 250.0, 350.0]  # Drift of 50 in each
    }
    df = pd.DataFrame(data)
    metrics = compute_metrics(df)
    
    assert metrics["n_samples"] == 3
    assert metrics["mae"] == 50.0
    assert metrics["mean_actual"] == 250.0

def test_compute_metrics_empty():
    """Test metrics calculation with empty data."""
    df = pd.DataFrame(columns=["predicted_revenue", "actual_revenue"])
    metrics = compute_metrics(df)
    
    assert metrics["n_samples"] == 0
    assert metrics["mae"] == 0.0

# ─────────────────────────────────────────────────────────────
# 2. Test Data Validation Logic (Mocking Azure Function logic)
# ─────────────────────────────────────────────────────────────

def validate_event_mock(event):
    """Mirror of logic in azure_functions/ValidateSalesEvent/__init__.py"""
    required = {"timestamp", "store_id", "product_id", "quantity", "price"}
    if not all(k in event for k in required):
        return False
    if not isinstance(event["quantity"], int) or event["quantity"] <= 0:
        return False
    return True

def test_event_validation_success():
    valid_event = {
        "timestamp": "2026-04-03T12:00:00Z",
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": 5,
        "price": 1.5
    }
    assert validate_event_mock(valid_event) is True

def test_event_validation_failure_missing_field():
    invalid_event = {
        "store_id": "S01",
        "quantity": 5
    }
    assert validate_event_mock(invalid_event) is False

def test_event_validation_failure_invalid_type():
    invalid_event = {
        "timestamp": "2026-04-03T12:00:00Z",
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": "five", # Should be int
        "price": 1.5
    }
    assert validate_event_mock(invalid_event) is False
