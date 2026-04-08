"""
Model health check & automatic rollback.

After a new model is promoted, this module monitors its live performance.
If the error rate exceeds a threshold within a grace period, it automatically
rolls back to the previous model version and alerts the team.

Usage:
    python monitoring/model_health_check.py              # check & rollback if needed
    python monitoring/model_health_check.py --dry-run    # check only, don't rollback
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_OUTPUT = BASE_DIR / "ml" / "model_output"
BACKUP_DIR = MODEL_OUTPUT / "rollback_backup"
HEALTH_REPORT_PATH = MODEL_OUTPUT / "model_health_report.json"

# Files that constitute a complete model snapshot
MODEL_FILES = [
    "revenue_model.pkl",
    "quantity_model.pkl",
    "label_encoders.pkl",
    "model_metadata.json",
]


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def backup_current_model() -> bool:
    """Save current model files as rollback snapshot."""
    if not all((MODEL_OUTPUT / f).exists() for f in MODEL_FILES):
        return False

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    for f in MODEL_FILES:
        src = MODEL_OUTPUT / f
        dst = BACKUP_DIR / f
        shutil.copy2(str(src), str(dst))

    # Save backup timestamp
    meta = {"backed_up_at": _utcnow_iso()}
    meta_src = MODEL_OUTPUT / "model_metadata.json"
    if meta_src.exists():
        with open(meta_src, "r", encoding="utf-8") as fh:
            orig = json.load(fh)
        meta["model_version"] = orig.get("model_version", "unknown")
        meta["trained_at"] = orig.get("trained_at", "")

    (BACKUP_DIR / "backup_info.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return True


def rollback_model() -> dict:
    """Restore model from rollback backup."""
    if not BACKUP_DIR.exists():
        return {"success": False, "error": "No backup found"}

    if not all((BACKUP_DIR / f).exists() for f in MODEL_FILES):
        return {"success": False, "error": "Incomplete backup"}

    for f in MODEL_FILES:
        src = BACKUP_DIR / f
        dst = MODEL_OUTPUT / f
        shutil.copy2(str(src), str(dst))

    # Load backup info
    info_path = BACKUP_DIR / "backup_info.json"
    backup_info = {}
    if info_path.exists():
        with open(info_path, "r", encoding="utf-8") as fh:
            backup_info = json.load(fh)

    return {
        "success": True,
        "rolled_back_to": backup_info.get("model_version", "unknown"),
        "original_trained_at": backup_info.get("trained_at", ""),
        "rollback_time": _utcnow_iso(),
    }


def check_model_health(
    error_rate_threshold: float = 50.0,
    grace_period_hours: int = 2,
    min_predictions: int = 10,
) -> dict:
    """
    Check if the current model is performing well enough.

    Reads the drift monitor report and retrain history to determine if
    the latest model version has degraded beyond acceptable bounds.
    """
    report = {
        "timestamp": _utcnow_iso(),
        "healthy": True,
        "reason": "",
        "current_model": None,
        "metrics": {},
    }

    # Load current model metadata
    meta_path = MODEL_OUTPUT / "model_metadata.json"
    if not meta_path.exists():
        report["healthy"] = True
        report["reason"] = "No model deployed"
        return report

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    report["current_model"] = meta.get("model_version", "unknown")

    # Check if model was recently deployed (within grace period)
    trained_at = meta.get("trained_at", "")
    if trained_at:
        try:
            trained_dt = datetime.fromisoformat(trained_at.replace("Z", ""))
            if datetime.utcnow() - trained_dt < timedelta(hours=grace_period_hours):
                report["reason"] = f"Model within grace period ({grace_period_hours}h)"
                return report
        except (ValueError, TypeError):
            pass

    # Load drift monitor report for current MAE
    drift_path = MODEL_OUTPUT / "drift_monitor_report.json"
    if not drift_path.exists():
        report["reason"] = "No drift report available"
        return report

    with open(drift_path, "r", encoding="utf-8") as f:
        drift = json.load(f)

    metrics = drift.get("metrics", {})
    mae = metrics.get("mae", 0)
    mape = metrics.get("mape", 0)
    n_samples = metrics.get("n_samples", 0)
    threshold = drift.get("threshold_mae", 25.0)

    report["metrics"] = {
        "mae": mae,
        "mape": mape,
        "n_samples": n_samples,
        "threshold": threshold,
    }

    if n_samples < min_predictions:
        report["reason"] = f"Insufficient predictions ({n_samples} < {min_predictions})"
        return report

    # Check degradation: MAPE > threshold or MAE significantly above threshold
    if mape > error_rate_threshold:
        report["healthy"] = False
        report["reason"] = f"MAPE {mape}% exceeds {error_rate_threshold}% threshold"
    elif mae > threshold * 2:
        report["healthy"] = False
        report["reason"] = f"MAE {mae} is 2x above threshold {threshold}"

    # Check retrain history — if last 2 retrains were REJECT, model is unstable
    history_path = MODEL_OUTPUT / "retrain_history" / "history_index.json"
    if history_path.exists():
        with open(history_path, "r", encoding="utf-8") as f:
            history = json.load(f)
        if isinstance(history, list) and len(history) >= 2:
            last_two = history[-2:]
            if all(r.get("decision") == "REJECT" for r in last_two):
                report["healthy"] = False
                report["reason"] = "Last 2 retrains rejected — model instability"

    return report


def run_health_check(dry_run: bool = False) -> dict:
    """Run health check and rollback if needed."""
    health = check_model_health()

    result = {
        "health_check": health,
        "action_taken": "none",
        "rollback_result": None,
    }

    if not health["healthy"]:
        print(f"[ALERT] Model unhealthy: {health['reason']}")

        if dry_run:
            result["action_taken"] = "dry_run_rollback"
            print("[DRY-RUN] Would rollback model")
        else:
            # Attempt rollback
            rollback = rollback_model()
            result["rollback_result"] = rollback
            if rollback["success"]:
                result["action_taken"] = "rolled_back"
                print(f"[ROLLBACK] Restored model: {rollback['rolled_back_to']}")
            else:
                result["action_taken"] = "rollback_failed"
                print(f"[ERROR] Rollback failed: {rollback['error']}")

        # Notify via Slack/Teams
        try:
            from monitoring.notifications import send_slack_alert
            webhook = os.getenv("SLACK_WEBHOOK_URL", "")
            if webhook:
                send_slack_alert(
                    webhook,
                    f"🚨 Model Rollback: {health['reason']}\n"
                    f"Action: {result['action_taken']}",
                    level="error",
                )
        except Exception as e:
            print(f"[WARN] Notification failed: {e}")

        # Log to SQL
        try:
            from monitoring.notifications import log_to_sql
            log_to_sql(
                event_type="model_rollback",
                mae_value=health["metrics"].get("mae", 0),
                threshold=health["metrics"].get("threshold", 0),
                retrain_triggered=False,
                details=json.dumps(result, default=str),
            )
        except Exception as e:
            print(f"[WARN] SQL logging failed: {e}")
    else:
        print(f"[OK] Model healthy: {health['reason'] or 'All checks passed'}")

    # Persist report
    MODEL_OUTPUT.mkdir(parents=True, exist_ok=True)
    HEALTH_REPORT_PATH.write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model health check & rollback")
    parser.add_argument("--dry-run", action="store_true", help="Check only, don't rollback")
    args = parser.parse_args()
    run_health_check(dry_run=args.dry_run)
