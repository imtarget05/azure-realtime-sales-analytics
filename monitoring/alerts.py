"""
Alert notifications for drift detection and MLOps events.

Supports:
  - Slack webhook
  - Email (SMTP)

Configure via environment variables:
  ALERT_SLACK_WEBHOOK_URL  — Slack Incoming Webhook URL
  ALERT_EMAIL_ENABLED      — "true" to enable email
  ALERT_SMTP_SERVER        — SMTP host (e.g. smtp.gmail.com)
  ALERT_SMTP_PORT          — SMTP port (default 587)
  ALERT_SMTP_USERNAME      — SMTP login
  ALERT_SMTP_PASSWORD      — SMTP password
  ALERT_EMAIL_FROM         — Sender email
  ALERT_EMAIL_TO           — Recipient(s), comma-separated
"""

import json
import logging
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

import requests

_logger = logging.getLogger(__name__)


def send_drift_alert(report: dict) -> dict:
    """Send drift alert through all configured channels. Returns summary."""
    results = {}

    # Slack
    slack_url = os.getenv("ALERT_SLACK_WEBHOOK_URL", "").strip() or os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if slack_url:
        results["slack"] = _send_slack(slack_url, report)

    # Email
    if os.getenv("ALERT_EMAIL_ENABLED", "").lower() in ("1", "true"):
        results["email"] = _send_email(report)

    if not results:
        _logger.info("No alert channels configured (set ALERT_SLACK_WEBHOOK_URL or ALERT_EMAIL_ENABLED)")
        results["status"] = "no_channels_configured"

    return results


def _build_alert_text(report: dict) -> str:
    """Build human-readable alert message from drift report."""
    metrics = report.get("metrics", {})
    mae = metrics.get("mae", 0)
    mape = metrics.get("mape", 0)
    n_samples = metrics.get("n_samples", 0)
    threshold = report.get("threshold_mae", 0)
    timestamp = report.get("timestamp", datetime.utcnow().isoformat())

    return (
        f"🚨 *Model Drift Detected*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"*MAE:* {mae:.4f} (threshold: {threshold})\n"
        f"*MAPE:* {mape:.2f}%\n"
        f"*Samples:* {n_samples}\n"
        f"*Mean Actual Revenue:* ${metrics.get('mean_actual', 0):.2f}\n"
        f"*Window:* {report.get('window_hours', 24)}h\n"
        f"*Trigger Mode:* {report.get('trigger_mode', 'N/A')}\n"
        f"*Timestamp:* {timestamp}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"Retrain has been {'triggered' if report.get('triggered') else 'NOT triggered'}."
    )


def _send_slack(webhook_url: str, report: dict) -> dict:
    """Send alert to Slack webhook."""
    try:
        text = _build_alert_text(report)
        payload = {
            "text": text,
            "username": "MLOps Drift Monitor",
            "icon_emoji": ":warning:",
        }
        resp = requests.post(webhook_url, json=payload, timeout=15)
        ok = resp.status_code == 200
        if ok:
            _logger.info("Slack alert sent successfully")
        else:
            _logger.warning(f"Slack alert failed: {resp.status_code} {resp.text}")
        return {"success": ok, "status_code": resp.status_code}
    except Exception as e:
        _logger.error(f"Slack alert error: {e}")
        return {"success": False, "error": str(e)}


def _send_email(report: dict) -> dict:
    """Send alert via SMTP."""
    smtp_server = os.getenv("ALERT_SMTP_SERVER", "").strip()
    smtp_port = int(os.getenv("ALERT_SMTP_PORT", "587"))
    smtp_user = os.getenv("ALERT_SMTP_USERNAME", "").strip()
    smtp_pass = os.getenv("ALERT_SMTP_PASSWORD", "").strip()
    email_from = os.getenv("ALERT_EMAIL_FROM", smtp_user).strip()
    email_to = os.getenv("ALERT_EMAIL_TO", "").strip()

    if not all([smtp_server, smtp_user, smtp_pass, email_to]):
        _logger.warning("Email alert: missing SMTP config (ALERT_SMTP_SERVER/USERNAME/PASSWORD/EMAIL_TO)")
        return {"success": False, "error": "incomplete SMTP configuration"}

    try:
        metrics = report.get("metrics", {})
        subject = f"[MLOps Alert] Model Drift Detected — MAE={metrics.get('mae', 0):.4f}"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = email_from
        msg["To"] = email_to

        text = _build_alert_text(report).replace("*", "")
        html = f"""
        <html><body style="font-family:Arial,sans-serif;background:#0f1117;color:#e1e4ea;padding:24px;">
        <div style="max-width:600px;margin:0 auto;background:#1a1d27;border-radius:12px;padding:24px;border:1px solid #2d3140;">
            <h2 style="color:#ff6b6b;">🚨 Model Drift Detected</h2>
            <table style="width:100%;border-collapse:collapse;margin-top:16px;">
                <tr><td style="padding:8px;color:#8b90a0;">MAE</td><td style="padding:8px;font-weight:bold;color:#ff6b6b;">{metrics.get('mae', 0):.4f}</td></tr>
                <tr><td style="padding:8px;color:#8b90a0;">Threshold</td><td style="padding:8px;">{report.get('threshold_mae', 0)}</td></tr>
                <tr><td style="padding:8px;color:#8b90a0;">MAPE</td><td style="padding:8px;">{metrics.get('mape', 0):.2f}%</td></tr>
                <tr><td style="padding:8px;color:#8b90a0;">Samples</td><td style="padding:8px;">{metrics.get('n_samples', 0)}</td></tr>
                <tr><td style="padding:8px;color:#8b90a0;">Window</td><td style="padding:8px;">{report.get('window_hours', 24)}h</td></tr>
                <tr><td style="padding:8px;color:#8b90a0;">Timestamp</td><td style="padding:8px;">{report.get('timestamp', '')}</td></tr>
            </table>
            <p style="margin-top:16px;color:{'#00b894' if report.get('triggered') else '#fdcb6e'};">
                Retrain: <strong>{'TRIGGERED' if report.get('triggered') else 'NOT triggered'}</strong>
            </p>
        </div>
        </body></html>
        """

        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))

        recipients = [r.strip() for r in email_to.split(",") if r.strip()]
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(email_from, recipients, msg.as_string())

        _logger.info(f"Email alert sent to {email_to}")
        return {"success": True, "recipients": recipients}
    except Exception as e:
        _logger.error(f"Email alert error: {e}")
        return {"success": False, "error": str(e)}
