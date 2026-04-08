"""
Azure Function: Auto Drift Monitor
Trigger: Timer (every 1 hour)

Flow:
  1. Check MAE from SQL vw_ForecastVsActual
  2. If MAE > threshold -> trigger retrain
  3. Log to SQL MonitoringEvents table
  4. Send Slack/Teams notification
"""

import json
import logging
import os
import sys
from datetime import datetime

import azure.functions as func

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


def main(timer: func.TimerRequest) -> None:
    logging.info("DriftMonitor triggered at %s", datetime.utcnow().isoformat())

    if timer.past_due:
        logging.warning("Timer is past due, running anyway")

    try:
        from ml.drift_monitor import run_monitor

        threshold_mae = float(os.getenv("DRIFT_MAE_ABS_THRESHOLD", "25"))
        window_hours = int(os.getenv("DRIFT_MONITOR_WINDOW_HOURS", "24"))
        min_samples = int(os.getenv("DRIFT_MONITOR_MIN_SAMPLES", "24"))
        cooldown_minutes = int(os.getenv("DRIFT_MONITOR_COOLDOWN_MINUTES", "120"))
        trigger_mode = os.getenv("DRIFT_TRIGGER_MODE", "local")

        report = run_monitor(
            threshold_mae=threshold_mae,
            window_hours=window_hours,
            min_samples=min_samples,
            cooldown_minutes=cooldown_minutes,
            trigger_mode=trigger_mode,
            trigger_github_actions=os.getenv(
                "DRIFT_TRIGGER_GITHUB_ACTIONS", "false"
            ).lower() in ("1", "true"),
            dry_run=False,
        )

        mae = report.get("metrics", {}).get("mae", 0)
        triggered = report.get("triggered", False)

        logging.info(
            "Drift check: MAE=%.4f threshold=%.1f triggered=%s",
            mae, threshold_mae, triggered,
        )

        # Log to SQL MonitoringEvents
        _log_to_sql(report)

        # Send notifications if drift detected
        if triggered:
            _send_notifications(report)

    except Exception as e:
        logging.error("DriftMonitor failed: %s", e, exc_info=True)
        _log_to_sql({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metrics": {"mae": 0},
            "threshold_mae": float(os.getenv("DRIFT_MAE_ABS_THRESHOLD", "25")),
            "triggered": False,
            "status": "error",
            "error": str(e),
        })


def _log_to_sql(report: dict) -> None:
    """Log monitoring event to SQL MonitoringEvents table."""
    try:
        from monitoring.notifications import log_to_sql
        metrics = report.get("metrics", {})
        event_type = "drift_detected" if report.get("triggered") else "drift_check_ok"
        if report.get("status") == "error":
            event_type = "monitor_error"

        log_to_sql(
            event_type=event_type,
            mae_value=metrics.get("mae", 0),
            threshold=report.get("threshold_mae", 0),
            model_version=None,
            retrain_triggered=report.get("triggered", False),
            details=json.dumps(report, default=str),
        )
    except Exception as e:
        logging.warning("SQL logging failed: %s", e)


def _send_notifications(report: dict) -> None:
    """Send drift alerts via Slack/Teams."""
    try:
        from monitoring.notifications import send_slack_alert, send_teams_alert

        slack_url = os.getenv("SLACK_WEBHOOK_URL", "").strip()
        if slack_url:
            metrics = report.get("metrics", {})
            msg = (
                f"*Model Drift Detected*\n"
                f"MAE: {metrics.get('mae', 0):.4f} "
                f"(threshold: {report.get('threshold_mae', 0)})\n"
                f"Samples: {metrics.get('n_samples', 0)}\n"
                f"Retrain triggered: {report.get('triggered', False)}"
            )
            send_slack_alert(slack_url, msg, level="warning")

        teams_url = os.getenv("TEAMS_WEBHOOK_URL", "").strip()
        if teams_url:
            send_teams_alert(teams_url, report)

    except Exception as e:
        logging.warning("Notification failed: %s", e)
