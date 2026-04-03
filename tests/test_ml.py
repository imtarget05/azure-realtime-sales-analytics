import pandas as pd
import pytest

from ml.train_model import generate_training_data, prepare_features, train_one_model
from ml.drift_monitor import compute_metrics


def test_input_validation_missing_required_feature_column():
    """Input validation: missing required feature column should fail fast."""
    df = generate_training_data(100)
    df = df.drop(columns=["temperature"])

    with pytest.raises(KeyError):
        prepare_features(df)


def test_model_output_shape_matches_test_size():
    """Model output shape: y_pred length must match y_test length."""
    df = generate_training_data(800)
    X, y_qty, _, _, _ = prepare_features(df)

    _, metrics, _, _, _, y_test, y_pred = train_one_model(X, y_qty, name="quantity_test")

    assert len(y_pred) == len(y_test)
    assert metrics["r2_score"] <= 1.0
    assert metrics["r2_score"] >= -1.0


def test_drift_logic_mae_exceeds_threshold():
    """Drift logic: MAE should exceed threshold on strongly shifted predictions."""
    df = pd.DataFrame(
        {
            "predicted_revenue": [100.0, 110.0, 95.0, 105.0],
            "actual_revenue": [180.0, 175.0, 170.0, 190.0],
        }
    )

    metrics = compute_metrics(df)

    assert metrics["n_samples"] == 4
    assert metrics["mae"] > 25.0
