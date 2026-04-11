"""
Notification & logging module for MLOps monitoring events.

Supports:
  - Slack Incoming Webhook
  - Microsoft Teams Adaptive Card webhook
  - SQL MonitoringEvents table logging
"""

import json
import logging
import os

import requests

_logger = logging.getLogger(__name__)


def send_slack_alert(webhook_url: str, message: str, level: str = "info") -> dict:
    """Send a message to Slack via Incoming Webhook."""
    color_map = {"info": "#4f8cff", "warning": "#fdcb6e", "error": "#ff6b6b", "success": "#00b894"}
    payload = {
        "attachments": [{
            "color": color_map.get(level, "#4f8cff"),
            "text": message,
            "footer": "Sales Analytics MLOps",
        }]
    }
    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
        ok = resp.status_code == 200
        if ok:
            _logger.info("Slack alert sent")
        else:
            _logger.warning("Slack alert failed: %s %s", resp.status_code, resp.text[:200])
        return {"success": ok, "status_code": resp.status_code}
    except Exception as e:
        _logger.error("Slack alert error: %s", e)
        return {"success": False, "error": str(e)}


def send_teams_alert(webhook_url: str, report: dict) -> dict:
    """Send a Microsoft Teams Adaptive Card notification."""
    metrics = report.get("metrics", {})
    triggered = report.get("triggered", False)

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "size": "Large",
                        "weight": "Bolder",
                        "text": "🚨 Model Drift Detected" if triggered else "✅ Drift Check OK",
                        "color": "Attention" if triggered else "Good",
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "MAE", "value": f"{metrics.get('mae', 0):.4f}"},
                            {"title": "Threshold", "value": str(report.get("threshold_mae", 0))},
                            {"title": "MAPE", "value": f"{metrics.get('mape', 0):.2f}%"},
                            {"title": "Samples", "value": str(metrics.get("n_samples", 0))},
                            {"title": "Retrain", "value": "Triggered" if triggered else "Not needed"},
                        ],
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Timestamp: {report.get('timestamp', '')}",
                        "size": "Small",
                        "isSubtle": True,
                    },
                ],
            },
        }],
    }

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        ok = resp.status_code in (200, 202)
        if ok:
            _logger.info("Teams alert sent")
        else:
            _logger.warning("Teams alert failed: %s %s", resp.status_code, resp.text[:200])
        return {"success": ok, "status_code": resp.status_code}
    except Exception as e:
        _logger.error("Teams alert error: %s", e)
        return {"success": False, "error": str(e)}


def log_to_sql(
    event_type: str,
    mae_value: float = 0.0,
    threshold: float = 0.0,
    model_version: str = None,
    retrain_triggered: bool = False,
    details: str = None,
) -> bool:
    """Log a monitoring event to the SQL MonitoringEvents table."""
    try:
        import pyodbc
        from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

        if not SQL_SERVER or not SQL_USERNAME or not SQL_PASSWORD:
            _logger.info("SQL not configured, skipping event log")
            return False

        conn_str = (
            f"DRIVER={SQL_DRIVER};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO dbo.MonitoringEvents
                (event_type, mae_value, threshold, model_version, retrain_triggered, details)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            event_type,
            mae_value,
            threshold,
            model_version,
            1 if retrain_triggered else 0,
            details,
        )
        conn.commit()
        conn.close()
        _logger.info("Logged event '%s' to SQL", event_type)
        return True
    except Exception as e:
        _logger.warning("SQL log failed: %s", e)
        return False
