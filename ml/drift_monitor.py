"""
MAE drift monitor for continuous training.

Flow:
1) Read latest forecast-vs-actual rows from SQL view dbo.vw_ForecastVsActual
2) Compute MAE on a recent time window
3) If MAE exceeds threshold, auto-trigger retrain_and_compare.py --promote
4) Persist a JSON report for audit/demo and web/reporting traceability
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import SQL_DATABASE, SQL_DRIVER, SQL_PASSWORD, SQL_SERVER, SQL_USERNAME


BASE_DIR = Path(__file__).resolve().parent
MODEL_OUTPUT_DIR = BASE_DIR / "model_output"
REPORT_PATH = MODEL_OUTPUT_DIR / "drift_monitor_report.json"
STATE_PATH = MODEL_OUTPUT_DIR / "drift_monitor_state.json"


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_sql_connection():
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


def fetch_recent_forecast_vs_actual(window_hours: int = 24) -> pd.DataFrame:
    conn = get_sql_connection()
    query = """
        SELECT
            forecast_date,
            ISNULL(forecast_hour, 0) AS forecast_hour,
            store_id,
            category,
            predicted_revenue,
            actual_revenue,
            model_version,
            DATEADD(HOUR, ISNULL(forecast_hour, 0), CAST(forecast_date AS datetime2)) AS forecast_dt
        FROM dbo.vw_ForecastVsActual
        WHERE DATEADD(HOUR, ISNULL(forecast_hour, 0), CAST(forecast_date AS datetime2))
              >= DATEADD(HOUR, ?, SYSUTCDATETIME())
          AND predicted_revenue IS NOT NULL
          AND actual_revenue IS NOT NULL
        ORDER BY forecast_dt DESC
    """
    df = pd.read_sql(query, conn, params=[-window_hours])
    conn.close()
    return df


def compute_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "n_samples": 0,
            "mae": 0.0,
            "mape": 0.0,
            "mean_actual": 0.0,
        }

    y_pred = df["predicted_revenue"].astype(float).to_numpy()
    y_true = df["actual_revenue"].astype(float).to_numpy()

    abs_err = np.abs(y_true - y_pred)
    mae = float(np.mean(abs_err))

    non_zero = np.abs(y_true) > 1e-9
    mape = float(np.mean(abs_err[non_zero] / np.abs(y_true[non_zero])) * 100.0) if np.any(non_zero) else 0.0

    return {
        "n_samples": int(len(df)),
        "mae": round(mae, 4),
        "mape": round(mape, 4),
        "mean_actual": round(float(np.mean(y_true)), 4),
    }


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    MODEL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _save_report(report: dict) -> None:
    MODEL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def _in_cooldown(last_trigger_at: str, cooldown_minutes: int) -> bool:
    if not last_trigger_at:
        return False
    try:
        last_dt = datetime.fromisoformat(last_trigger_at.replace("Z", ""))
    except ValueError:
        return False
    return datetime.utcnow() - last_dt < timedelta(minutes=cooldown_minutes)


def trigger_retrain(promote: bool = True) -> dict:
    script_path = BASE_DIR / "retrain_and_compare.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--new-samples",
        os.getenv("DRIFT_RETRAIN_SAMPLES", "80000"),
        "--new-estimators",
        os.getenv("DRIFT_RETRAIN_ESTIMATORS", "300"),
        "--new-depth",
        os.getenv("DRIFT_RETRAIN_MAX_DEPTH", "6"),
        "--new-lr",
        os.getenv("DRIFT_RETRAIN_LEARNING_RATE", "0.1"),
    ]
    if promote:
        cmd.append("--promote")

    result = subprocess.run(
        cmd,
        cwd=str(BASE_DIR.parent),
        capture_output=True,
        text=True,
        timeout=1200,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    return {
        "command": " ".join(cmd),
        "returncode": result.returncode,
        "success": result.returncode == 0,
        "stdout_tail": (result.stdout or "")[-3000:],
        "stderr_tail": (result.stderr or "")[-1500:],
    }


def trigger_azureml_pipeline() -> dict:
    script_path = BASE_DIR.parent / "mlops" / "trigger_training_pipeline.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--n-samples",
        os.getenv("DRIFT_RETRAIN_SAMPLES", "80000"),
        "--timeout",
        os.getenv("DRIFT_AZURE_PIPELINE_TIMEOUT_MIN", "90"),
    ]

    result = subprocess.run(
        cmd,
        cwd=str(BASE_DIR.parent),
        capture_output=True,
        text=True,
        timeout=1800,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )

    return {
        "command": " ".join(cmd),
        "returncode": result.returncode,
        "success": result.returncode == 0,
        "stdout_tail": (result.stdout or "")[-3000:],
        "stderr_tail": (result.stderr or "")[-1500:],
    }


def trigger_github_actions_workflow() -> dict:
    """
    Dispatch GitHub Actions workflow via REST API.
    Required env vars:
      - GITHUB_TOKEN (repo scope)
      - GITHUB_REPO  (owner/repo)
    Optional env vars:
      - GITHUB_WORKFLOW_FILE (default: ci-cd-mlops.yml)
      - GITHUB_REF (default: main)
    """
    token = os.getenv("GITHUB_TOKEN", "").strip()
    repo = os.getenv("GITHUB_REPO", "").strip()
    workflow_file = os.getenv("GITHUB_WORKFLOW_FILE", "ci-cd-mlops.yml").strip()
    ref = os.getenv("GITHUB_REF", "main").strip()

    if not token or not repo:
        return {
            "success": False,
            "error": "Missing GITHUB_TOKEN or GITHUB_REPO",
        }

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    payload = {
        "ref": ref,
        "inputs": {
            "force_retrain": "true",
            "skip_terraform": "true",
        },
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        ok = response.status_code in (201, 204)
        return {
            "success": ok,
            "status_code": response.status_code,
            "workflow_file": workflow_file,
            "repo": repo,
            "ref": ref,
            "response_tail": (response.text or "")[-800:],
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "workflow_file": workflow_file,
            "repo": repo,
        }


def run_monitor(
    threshold_mae: float,
    window_hours: int,
    min_samples: int,
    cooldown_minutes: int,
    trigger_mode: str,
    trigger_github_actions: bool,
    dry_run: bool,
) -> dict:
    started_at = _utcnow_iso()
    state = _load_state()

    try:
        df = fetch_recent_forecast_vs_actual(window_hours=window_hours)
        metrics = compute_metrics(df)

        should_trigger = (
            metrics["n_samples"] >= min_samples
            and metrics["mae"] > threshold_mae
            and not _in_cooldown(state.get("last_trigger_at", ""), cooldown_minutes)
        )

        trigger_result = None
        if should_trigger:
            if dry_run:
                trigger_result = {"success": True, "dry_run": True, "message": "Would trigger retrain"}
            else:
                trigger_result = {}
                if trigger_mode in ("local", "both"):
                    trigger_result["local"] = trigger_retrain(promote=True)
                if trigger_mode in ("azureml", "both"):
                    trigger_result["azureml"] = trigger_azureml_pipeline()
                if trigger_github_actions:
                    trigger_result["github_actions"] = trigger_github_actions_workflow()

                success_flags = [
                    payload.get("success", False)
                    for payload in trigger_result.values()
                    if isinstance(payload, dict)
                ]
                if success_flags and all(success_flags):
                    state["last_trigger_at"] = _utcnow_iso()
                    _save_state(state)

        report = {
            "timestamp": started_at,
            "window_hours": window_hours,
            "threshold_mae": threshold_mae,
            "min_samples": min_samples,
            "cooldown_minutes": cooldown_minutes,
            "trigger_mode": trigger_mode,
            "trigger_github_actions": trigger_github_actions,
            "metrics": metrics,
            "triggered": bool(should_trigger),
            "trigger_result": trigger_result,
            "status": "ok",
        }
    except Exception as exc:
        report = {
            "timestamp": started_at,
            "window_hours": window_hours,
            "threshold_mae": threshold_mae,
            "min_samples": min_samples,
            "cooldown_minutes": cooldown_minutes,
            "trigger_mode": trigger_mode,
            "trigger_github_actions": trigger_github_actions,
            "triggered": False,
            "status": "error",
            "error": str(exc),
        }

    _save_report(report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor MAE drift and auto-trigger retraining")
    parser.add_argument("--threshold-mae", type=float, default=float(os.getenv("DRIFT_MAE_ABS_THRESHOLD", "25")))
    parser.add_argument("--window-hours", type=int, default=int(os.getenv("DRIFT_MONITOR_WINDOW_HOURS", "24")))
    parser.add_argument("--min-samples", type=int, default=int(os.getenv("DRIFT_MONITOR_MIN_SAMPLES", "24")))
    parser.add_argument("--cooldown-minutes", type=int, default=int(os.getenv("DRIFT_MONITOR_COOLDOWN_MINUTES", "120")))
    parser.add_argument(
        "--trigger-mode",
        choices=["local", "azureml", "both"],
        default=os.getenv("DRIFT_TRIGGER_MODE", "local"),
        help="local: retrain_and_compare --promote, azureml: mlops pipeline, both: run both",
    )
    parser.add_argument(
        "--trigger-github-actions",
        action="store_true",
        default=os.getenv("DRIFT_TRIGGER_GITHUB_ACTIONS", "false").lower() in ("1", "true", "yes"),
        help="Also dispatch GitHub Actions workflow ci-cd-mlops.yml when drift is detected",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute and log only, no retrain trigger")
    args = parser.parse_args()

    report = run_monitor(
        threshold_mae=args.threshold_mae,
        window_hours=args.window_hours,
        min_samples=args.min_samples,
        cooldown_minutes=args.cooldown_minutes,
        trigger_mode=args.trigger_mode,
        trigger_github_actions=args.trigger_github_actions,
        dry_run=args.dry_run,
    )

    mae = report.get("metrics", {}).get("mae")
    if mae is not None:
        print(f"Current MAE: {mae} (Threshold: {args.threshold_mae})")
    if report.get("triggered"):
        print("ALERT: Model drift detected! Triggering retrain pipeline...")
    else:
        print("No drift alert. Monitor cycle completed.")

    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
