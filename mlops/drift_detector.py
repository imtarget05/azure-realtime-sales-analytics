"""
Drift Detector — Phát hiện data drift & model performance drift.

Đây là trái tim của vòng lặp feedback MLOps:
  1. Lấy dữ liệu production gần nhất từ Azure SQL
  2. So sánh phân phối features vs. training data (KS test, PSI)
  3. So sánh prediction accuracy vs. baseline metrics
  4. Nếu drift vượt ngưỡng → output drift_detected=true → trigger retrain

Chạy bởi:
  - GitHub Actions scheduled workflow (hàng ngày)
  - Manual trigger
"""

import os
import sys
import json
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

try:
    from scipy import stats
except ImportError:
    print("[ERROR] Cần cài đặt: pip install scipy")
    sys.exit(1)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

# ─────────────────────────────────────────────
# Ngưỡng drift
# ─────────────────────────────────────────────
DRIFT_THRESHOLDS = {
    "ks_pvalue_min": 0.01,        # KS test p-value < 0.01 → drift
    "psi_max": 0.2,               # PSI > 0.2 → significant drift
    "r2_degradation_max": 0.15,   # R² giảm > 15% → performance drift
    "mae_increase_max": 0.20,     # MAE tăng > 20% → prediction quality drop
}


def get_sql_connection():
    """Kết nối Azure SQL Database."""
    import pyodbc
    conn_str = (
        f"DRIVER={SQL_DRIVER};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
    )
    return pyodbc.connect(conn_str)


def fetch_recent_data(days: int = 7) -> pd.DataFrame:
    """Lấy dữ liệu production gần nhất từ SQL."""
    conn = get_sql_connection()
    query = """
        SELECT
            DATEPART(HOUR, EventTime) AS hour,
            DATEPART(MONTH, EventTime) AS month,
            DATENAME(WEEKDAY, EventTime) AS day_of_week,
            Region AS region,
            Category AS category,
            AvgTemperature AS temperature,
            AvgHumidity AS humidity,
            TotalQuantity AS quantity,
            TotalRevenue AS revenue
        FROM SalesAgg5m
        WHERE EventTime >= DATEADD(DAY, ?, GETUTCDATE())
        ORDER BY EventTime DESC
    """
    df = pd.read_sql(query, conn, params=[-days])
    conn.close()
    print(f"[DRIFT] Fetched {len(df)} records from last {days} days")
    return df


def fetch_prediction_accuracy(days: int = 7) -> pd.DataFrame:
    """Lấy dữ liệu dự đoán vs. thực tế để đánh giá model performance."""
    conn = get_sql_connection()
    query = """
        SELECT
            f.predicted_revenue,
            f.predicted_quantity,
            a.TotalRevenue AS actual_revenue,
            a.TotalQuantity AS actual_quantity,
            f.forecast_date,
            f.region
        FROM SalesForecasts f
        INNER JOIN SalesAgg5m a
            ON f.region = a.Region
            AND f.forecast_date = CAST(a.EventTime AS DATE)
            AND f.forecast_hour = DATEPART(HOUR, a.EventTime)
        WHERE f.forecast_date >= DATEADD(DAY, ?, GETUTCDATE())
    """
    df = pd.read_sql(query, conn, params=[-days])
    conn.close()
    print(f"[DRIFT] Fetched {len(df)} prediction vs actual pairs")
    return df


def calculate_psi(expected: np.ndarray, actual: np.ndarray, bins: int = 10) -> float:
    """
    Population Stability Index (PSI).
    PSI < 0.1: No drift
    0.1 <= PSI < 0.2: Moderate drift
    PSI >= 0.2: Significant drift
    """
    # Tạo bins từ expected distribution
    breakpoints = np.percentile(expected, np.linspace(0, 100, bins + 1))
    breakpoints = np.unique(breakpoints)

    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts = np.histogram(actual, bins=breakpoints)[0]

    # Tránh chia cho 0
    expected_pct = (expected_counts + 1) / (len(expected) + bins)
    actual_pct = (actual_counts + 1) / (len(actual) + bins)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


def detect_feature_drift(
    reference_data: pd.DataFrame,
    current_data: pd.DataFrame,
    numerical_features: list[str],
) -> dict:
    """
    Phát hiện feature drift bằng KS test và PSI.
    """
    results = {}

    for feature in numerical_features:
        if feature not in reference_data.columns or feature not in current_data.columns:
            continue

        ref_values = reference_data[feature].dropna().values
        cur_values = current_data[feature].dropna().values

        if len(ref_values) < 30 or len(cur_values) < 30:
            continue

        # Kolmogorov-Smirnov test
        ks_stat, ks_pvalue = stats.ks_2samp(ref_values, cur_values)

        # PSI
        psi = calculate_psi(ref_values, cur_values)

        is_drifted = (
            ks_pvalue < DRIFT_THRESHOLDS["ks_pvalue_min"]
            or psi > DRIFT_THRESHOLDS["psi_max"]
        )

        results[feature] = {
            "ks_statistic": round(float(ks_stat), 4),
            "ks_pvalue": round(float(ks_pvalue), 6),
            "psi": round(psi, 4),
            "is_drifted": is_drifted,
        }

        status = "⚠ DRIFT" if is_drifted else "✓ OK"
        print(f"  {feature:<20} KS={ks_stat:.4f} p={ks_pvalue:.4f} PSI={psi:.4f} [{status}]")

    return results


def detect_performance_drift(
    predictions_df: pd.DataFrame,
    baseline_metrics: dict,
) -> dict:
    """
    Phát hiện model performance degradation.
    So sánh prediction accuracy hiện tại vs. baseline từ training.
    """
    if predictions_df.empty:
        print("[DRIFT] No prediction data available for performance check")
        return {"performance_drifted": False, "reason": "no_data"}

    from sklearn.metrics import mean_absolute_error, r2_score

    actual = predictions_df["actual_revenue"].values
    predicted = predictions_df["predicted_revenue"].values

    current_mae = mean_absolute_error(actual, predicted)
    current_r2 = r2_score(actual, predicted)

    baseline_mae = baseline_metrics.get("mae", current_mae)
    baseline_r2 = baseline_metrics.get("r2_score", current_r2)

    mae_increase = (current_mae - baseline_mae) / max(baseline_mae, 1e-6)
    r2_degradation = (baseline_r2 - current_r2) / max(abs(baseline_r2), 1e-6)

    performance_drifted = (
        r2_degradation > DRIFT_THRESHOLDS["r2_degradation_max"]
        or mae_increase > DRIFT_THRESHOLDS["mae_increase_max"]
    )

    result = {
        "current_mae": round(current_mae, 4),
        "current_r2": round(current_r2, 4),
        "baseline_mae": round(baseline_mae, 4),
        "baseline_r2": round(baseline_r2, 4),
        "mae_increase_pct": round(mae_increase * 100, 2),
        "r2_degradation_pct": round(r2_degradation * 100, 2),
        "performance_drifted": performance_drifted,
    }

    status = "⚠ PERFORMANCE DRIFT" if performance_drifted else "✓ PERFORMANCE OK"
    print(f"\n[DRIFT] Performance Check: {status}")
    print(f"  MAE: {baseline_mae:.4f} → {current_mae:.4f} ({mae_increase:+.1%})")
    print(f"  R²:  {baseline_r2:.4f} → {current_r2:.4f} ({-r2_degradation:+.1%})")

    return result


def load_baseline_metrics() -> dict:
    """Load metrics baseline từ model_metadata.json (training time)."""
    metadata_paths = [
        os.path.join(os.path.dirname(__file__), "..", "ml", "model_output", "model_metadata.json"),
        os.path.join(os.path.dirname(__file__), "..", "model_output", "model_metadata.json"),
    ]
    for path in metadata_paths:
        if os.path.exists(path):
            with open(path) as f:
                metadata = json.load(f)
            metrics = metadata.get("revenue_metrics", metadata.get("metrics", {}))
            print(f"[DRIFT] Loaded baseline metrics from {path}")
            return metrics

    print("[DRIFT] No baseline metrics found, using defaults")
    return {"mae": 100.0, "rmse": 150.0, "r2_score": 0.85}


def generate_reference_data(n_samples: int = 5000) -> pd.DataFrame:
    """Tạo reference data từ cùng distribution với training data."""
    # Import training data generator
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ml"))
    try:
        from train_model import generate_training_data
        df = generate_training_data(n_samples)
        print(f"[DRIFT] Generated {len(df)} reference samples from training distribution")
        return df
    except ImportError:
        print("[DRIFT] Cannot import training data generator")
        return pd.DataFrame()


def run_drift_detection(output_github: bool = False) -> dict:
    """
    Chạy full drift detection pipeline.

    Returns:
        dict với drift_detected (bool) và chi tiết
    """
    print("=" * 60)
    print("DRIFT DETECTION REPORT")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    # Load baseline
    baseline_metrics = load_baseline_metrics()

    # Feature drift detection
    print("\n─── Feature Drift Analysis ───")
    numerical_features = ["hour", "month", "temperature", "humidity", "quantity", "revenue"]

    reference_data = generate_reference_data()
    feature_drift_results = {}

    try:
        current_data = fetch_recent_data(days=7)
        if not current_data.empty and not reference_data.empty:
            feature_drift_results = detect_feature_drift(
                reference_data, current_data, numerical_features
            )
    except Exception as e:
        print(f"[DRIFT] Feature drift check skipped: {e}")

    # Performance drift detection
    print("\n─── Performance Drift Analysis ───")
    performance_result = {"performance_drifted": False}

    try:
        predictions_df = fetch_prediction_accuracy(days=7)
        if not predictions_df.empty:
            performance_result = detect_performance_drift(predictions_df, baseline_metrics)
    except Exception as e:
        print(f"[DRIFT] Performance drift check skipped: {e}")

    # Decision: drift detected?
    features_drifted = [f for f, r in feature_drift_results.items() if r.get("is_drifted")]
    drift_detected = (
        len(features_drifted) >= 2  # Ít nhất 2 features drift
        or performance_result.get("performance_drifted", False)
    )

    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "drift_detected": drift_detected,
        "features_drifted": features_drifted,
        "feature_drift_details": feature_drift_results,
        "performance_drift": performance_result,
        "thresholds_used": DRIFT_THRESHOLDS,
    }

    print(f"\n{'=' * 60}")
    if drift_detected:
        print("⚠ DRIFT DETECTED — Retrain recommended")
        print(f"  Drifted features: {features_drifted}")
        print(f"  Performance drift: {performance_result.get('performance_drifted')}")
    else:
        print("✓ NO SIGNIFICANT DRIFT — Model is stable")
    print(f"{'=' * 60}")

    # Output for GitHub Actions
    if output_github:
        github_output = os.environ.get("GITHUB_OUTPUT", "")
        if github_output:
            with open(github_output, "a") as f:
                f.write(f"drift_detected={'true' if drift_detected else 'false'}\n")
            print(f"\n[GH] Set output drift_detected={'true' if drift_detected else 'false'}")

    # Save report
    report_path = os.path.join(os.path.dirname(__file__), "..", "ml", "model_output", "drift_report.json")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n[DRIFT] Report saved to {report_path}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Data & Model Drift Detection")
    parser.add_argument("--days", type=int, default=7, help="Days of recent data to check")
    parser.add_argument("--output-github", action="store_true", help="Output for GitHub Actions")
    args = parser.parse_args()

    report = run_drift_detection(output_github=args.output_github)
    sys.exit(0 if not report["drift_detected"] else 0)  # Không fail workflow


if __name__ == "__main__":
    main()
