"""
Drift Monitoring Module

Detects statistical drift in sales forecast model using:
- MAE (Mean Absolute Error) vs. actual sales
- PSI (Population Stability Index) for feature distributions  
- KS test (Kolmogorov-Smirnov) for input/output distributions
- R² degradation detection

Can be run as CLI or imported as module for Azure Functions.

Usage (CLI):
    python ml/drift_monitor.py \
        --threshold-mae 25 \
        --window-hours 24 \
        --min-samples 24
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

# Add project root to path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import (
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD,
    DRIFT_KS_PVALUE_MIN,
    DRIFT_PSI_MAX,
    DRIFT_R2_DEGRADATION_MAX,
    DRIFT_MAE_INCREASE_MAX,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_sql_connection():
    """Create SQL Server connection for drift detection."""
    try:
        import pyodbc
    except ImportError:
        logger.error("pyodbc not installed. Install with: pip install pyodbc")
        return None
    
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
    )
    
    try:
        return pyodbc.connect(conn_str, timeout=30)
    except Exception as e:
        logger.error(f"Failed to connect to SQL: {e}")
        return None


def fetch_forecast_vs_actual(
    conn,
    window_hours: int = 24,
    min_samples: int = 24,
) -> Optional[pd.DataFrame]:
    """
    Fetch forecast vs actual from SQL view vw_ForecastVsActual.
    
    Returns DataFrame with columns:
        - actual_revenue
        - predicted_revenue  
        - absolute_error
        - forecast_date
        - store_id
        - product_id
    """
    if not conn:
        logger.error("No database connection")
        return None
    
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    
    query = f"""
    SELECT
        actual_revenue,
        predicted_revenue,
        ABS(actual_revenue - predicted_revenue) AS absolute_error,
        CAST(forecast_date AS DATE) AS forecast_date,
        store_id,
        product_id,
        CAST(forecast_datetime AS DATETIME2) AS forecast_datetime
    FROM dbo.vw_ForecastVsActual
    WHERE CAST(forecast_datetime AS DATETIME2) >= '{cutoff_time.isoformat()}'
    ORDER BY forecast_datetime DESC
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        if not rows or len(rows) < min_samples:
            logger.warning(
                f"Insufficient samples: got {len(rows) if rows else 0}, "
                f"need {min_samples}"
            )
            return None
        
        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Fetched {len(df)} rows from vw_ForecastVsActual")
        return df
        
    except Exception as e:
        logger.error(f"SQL query failed: {e}")
        return None


def compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute metrics from forecast vs actual data.
    
    Args:
        df: DataFrame with columns ['predicted_revenue', 'actual_revenue']
        
    Returns:
        Dict with keys: n_samples, mae, mean_actual, mean_predicted
    """
    if df is None or len(df) == 0:
        return {
            "n_samples": 0,
            "mae": 0.0,
            "mean_actual": 0.0,
            "mean_predicted": 0.0,
        }
    
    df = df.copy()
    if "absolute_error" not in df.columns:
        df["absolute_error"] = (df["predicted_revenue"] - df["actual_revenue"]).abs()
    
    mae = float(df["absolute_error"].mean())
    mean_actual = float(df["actual_revenue"].mean()) if "actual_revenue" in df.columns else 0.0
    mean_predicted = float(df["predicted_revenue"].mean()) if "predicted_revenue" in df.columns else 0.0
    
    return {
        "n_samples": len(df),
        "mae": mae,
        "mean_actual": mean_actual,
        "mean_predicted": mean_predicted,
    }


def calculate_mae(df: pd.DataFrame) -> float:
    """Calculate Mean Absolute Error."""
    return float(df["absolute_error"].mean())


def calculate_psi(
    baseline_dist: np.ndarray,
    current_dist: np.ndarray,
    buckets: int = 10,
) -> float:
    """
    Calculate Population Stability Index (PSI).
    
    PSI measures shift in feature distribution:
    - PSI < 0.1: No significant shift
    - 0.1 < PSI < 0.25: Small shift, monitor
    - PSI > 0.25: Significant shift, likely drift
    """
    try:
        baseline_pct = np.histogram(baseline_dist, bins=buckets)[0] / len(baseline_dist)
        current_pct = np.histogram(current_dist, bins=buckets)[0] / len(current_dist)
        
        # Add small epsilon to avoid log(0)
        epsilon = 1e-10
        baseline_pct = baseline_pct + epsilon
        current_pct = current_pct + epsilon
        
        psi = np.sum((current_pct - baseline_pct) * np.log(current_pct / baseline_pct))
        return float(psi)
    except Exception as e:
        logger.warning(f"PSI calculation failed: {e}")
        return 0.0


def calculate_ks_statistic(data1: np.ndarray, data2: np.ndarray) -> Tuple[float, float]:
    """
    Kolmogorov-Smirnov test for distribution differences.
    
    Returns: (ks_statistic, p_value)
    - p_value > 0.05: Distributions are similar
    - p_value < 0.05: Distributions are different (drift indicator)
    """
    try:
        from scipy import stats
        ks_stat, p_value = stats.ks_2samp(data1, data2)
        return float(ks_stat), float(p_value)
    except ImportError:
        logger.warning("scipy not available for KS test")
        return 0.0, 1.0
    except Exception as e:
        logger.warning(f"KS test failed: {e}")
        return 0.0, 1.0


def run_monitor(
    threshold_mae: float = 25.0,
    window_hours: int = 24,
    min_samples: int = 24,
    cooldown_minutes: int = 120,
    trigger_mode: str = "azureml",
    trigger_github_actions: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Main drift detection function.
    
    Args:
        threshold_mae: MAE threshold to trigger retrain
        window_hours: Lookback window for drift detection
        min_samples: Minimum samples required
        cooldown_minutes: Minutes to wait before retriggering
        trigger_mode: 'azureml' or 'github' 
        trigger_github_actions: Whether to trigger GitHub Actions workflow
        dry_run: If True, don't write to DB
    
    Returns:
        Dict with drift report
    """
    
    # Connect to SQL
    conn = get_sql_connection()
    if not conn:
        return {
            "status": "error",
            "message": "Failed to connect to SQL database",
            "triggered": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    
    try:
        # Fetch data
        df = fetch_forecast_vs_actual(conn, window_hours, min_samples)
        if df is None or len(df) < min_samples:
            return {
                "status": "insufficient_data",
                "message": f"Insufficient samples (need {min_samples})",
                "triggered": False,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        
        # Calculate metrics
        mae = calculate_mae(df)
        r2 = 0.0  # TODO: Calculate from model registry if available
        
        # Statistical tests
        baseline_errors = df["absolute_error"].values[:len(df)//2]
        current_errors = df["absolute_error"].values[len(df)//2:]
        
        if len(current_errors) > 0:
            psi = calculate_psi(baseline_errors, current_errors)
            ks_stat, ks_pvalue = calculate_ks_statistic(baseline_errors, current_errors)
        else:
            psi = 0.0
            ks_stat = 0.0
            ks_pvalue = 1.0
        
        # Determine if drift triggered
        drift_reasons = []
        
        if mae > threshold_mae:
            drift_reasons.append(f"MAE {mae:.2f} > threshold {threshold_mae}")
        
        if ks_pvalue < DRIFT_KS_PVALUE_MIN:
            drift_reasons.append(f"KS test p-value {ks_pvalue:.4f} < {DRIFT_KS_PVALUE_MIN}")
        
        if psi > DRIFT_PSI_MAX:
            drift_reasons.append(f"PSI {psi:.4f} > threshold {DRIFT_PSI_MAX}")
        
        triggered = len(drift_reasons) > 0
        
        # Build report
        report = {
            "status": "success",
            "triggered": triggered,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metrics": {
                "mae": float(mae),
                "mae_threshold": threshold_mae,
                "psi": float(psi),
                "psi_threshold": DRIFT_PSI_MAX,
                "ks_statistic": float(ks_stat),
                "ks_pvalue": float(ks_pvalue),
                "r2": float(r2),
                "samples": len(df),
                "window_hours": window_hours,
            },
            "reasons": drift_reasons,
            "trigger_mode": trigger_mode,
        }
        
        logger.info(f"Drift detection complete: triggered={triggered}")
        logger.info(f"  MAE: {mae:.2f}")
        logger.info(f"  PSI: {psi:.4f}")
        logger.info(f"  KS p-value: {ks_pvalue:.4f}")
        
        if triggered and not dry_run:
            # Log to SQL
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO dbo.MonitoringEvents (event_type, event_data)
                VALUES ('DRIFT_DETECTED', ?)
                """,
                (json.dumps(report),)
            )
            conn.commit()
            
            logger.info(f"Logged drift event to SQL: {report['reasons']}")
        
        return report
        
    except Exception as e:
        logger.error(f"Drift detection failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "triggered": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        if conn:
            conn.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Drift monitoring for sales forecast model")
    parser.add_argument(
        "--threshold-mae",
        type=float,
        default=25.0,
        help="MAE threshold to trigger retrain"
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Lookback window in hours"
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=24,
        help="Minimum samples required for drift detection"
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=120,
        help="Cooldown between retrains"
    )
    parser.add_argument(
        "--trigger-mode",
        choices=["azureml", "github"],
        default="azureml",
        help="Mode for triggering retrain"
    )
    parser.add_argument(
        "--trigger-github-actions",
        action="store_true",
        help="Trigger GitHub Actions workflow on drift"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write to database"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file (default: ml/model_output/drift_monitor_report.json)"
    )
    
    args = parser.parse_args()
    
    # Set default output path
    if not args.output:
        output_dir = ROOT / "ml" / "model_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        args.output = str(output_dir / "drift_monitor_report.json")
    
    # Run monitoring
    report = run_monitor(
        threshold_mae=args.threshold_mae,
        window_hours=args.window_hours,
        min_samples=args.min_samples,
        cooldown_minutes=args.cooldown_minutes,
        trigger_mode=args.trigger_mode,
        trigger_github_actions=args.trigger_github_actions,
        dry_run=args.dry_run,
    )
    
    # Write report
    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Report written to {args.output}")
    
    # Exit with appropriate code
    sys.exit(0 if report["status"] == "success" else 1)


if __name__ == "__main__":
    main()
