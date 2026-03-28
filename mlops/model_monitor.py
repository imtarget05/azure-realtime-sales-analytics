"""
Model Monitor — Giám sát liên tục model performance trong production.

Tích hợp với:
  - Azure Application Insights (log metrics)
  - Azure SQL (đọc predictions vs. actuals)
  - Drift Detector (trigger khi phát hiện vấn đề)

Chạy liên tục (scheduled) hoặc được gọi sau mỗi batch prediction.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER,
    DRIFT_R2_DEGRADATION_MAX, DRIFT_MAE_INCREASE_MAX, DRIFT_CHECK_DAYS,
    AUTO_RETRAIN_ENABLED,
)

try:
    from monitoring.telemetry import track_metric, track_event, logger
except ImportError:
    import logging
    logger = logging.getLogger("model_monitor")

    def track_metric(name, value, properties=None):
        logger.info(f"[METRIC] {name}={value}")

    def track_event(name, properties=None):
        logger.info(f"[EVENT] {name} {properties}")


class ModelMonitor:
    """
    Giám sát model health trong production.

    Metrics tracked:
      - prediction_latency: Thời gian dự đoán
      - prediction_accuracy: MAE, RMSE, R² (so vs. actual)
      - prediction_volume: Số lượng predictions/phút
      - feature_distribution: Mean, std của input features
      - drift_score: PSI score tổng hợp
    """

    def __init__(self):
        self.metrics_buffer = []
        self.last_health_check = None
        self.alert_cooldown = {}  # Tránh spam alerts

    def log_prediction(
        self,
        input_features: dict,
        predictions: dict,
        latency_ms: float,
        model_version: str,
    ):
        """Log mỗi prediction để theo dõi."""
        track_metric("prediction_latency_ms", latency_ms, {
            "model_version": model_version,
        })
        track_metric("predicted_revenue", predictions.get("predicted_revenue", 0), {
            "region": input_features.get("region", "unknown"),
            "category": input_features.get("category", "unknown"),
        })

        self.metrics_buffer.append({
            "timestamp": datetime.utcnow().isoformat(),
            "latency_ms": latency_ms,
            "model_version": model_version,
            "predicted_revenue": predictions.get("predicted_revenue", 0),
            "predicted_quantity": predictions.get("predicted_quantity", 0),
        })

        # Flush buffer mỗi 100 predictions
        if len(self.metrics_buffer) >= 100:
            self._flush_metrics()

    def _flush_metrics(self):
        """Aggregate và log batch metrics."""
        if not self.metrics_buffer:
            return

        latencies = [m["latency_ms"] for m in self.metrics_buffer]
        revenues = [m["predicted_revenue"] for m in self.metrics_buffer]

        track_metric("prediction_latency_p50", float(np.percentile(latencies, 50)))
        track_metric("prediction_latency_p95", float(np.percentile(latencies, 95)))
        track_metric("prediction_latency_p99", float(np.percentile(latencies, 99)))
        track_metric("prediction_volume", len(self.metrics_buffer))
        track_metric("prediction_revenue_mean", float(np.mean(revenues)))

        # Alert nếu latency quá cao
        p95 = np.percentile(latencies, 95)
        if p95 > 2000:  # > 2 seconds
            self._send_alert("high_latency", f"P95 latency = {p95:.0f}ms")

        self.metrics_buffer = []

    def check_health(self) -> dict:
        """
        Kiểm tra sức khoẻ model.
        Gọi từ scheduled job hoặc API endpoint.
        """
        health = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "checks": {},
        }

        # Check 1: Prediction volume
        try:
            volume = self._get_recent_prediction_count()
            health["checks"]["prediction_volume"] = {
                "value": volume,
                "status": "ok" if volume > 0 else "warning",
            }
            if volume == 0:
                health["status"] = "degraded"
        except Exception as e:
            health["checks"]["prediction_volume"] = {"status": "error", "error": str(e)}

        # Check 2: Prediction accuracy (nếu có actuals)
        try:
            accuracy = self._get_recent_accuracy()
            if accuracy:
                health["checks"]["accuracy"] = accuracy
                if accuracy.get("r2_score", 1) < 0.5:
                    health["status"] = "degraded"
                    self._send_alert("low_accuracy", f"R²={accuracy['r2_score']:.4f}")
        except Exception as e:
            health["checks"]["accuracy"] = {"status": "error", "error": str(e)}

        # Check 3: Endpoint responsiveness
        try:
            from config.settings import AML_ENDPOINT_URL, AML_API_KEY
            if AML_ENDPOINT_URL and not AML_ENDPOINT_URL.startswith("<"):
                import requests
                start = time.time()
                resp = requests.post(
                    AML_ENDPOINT_URL,
                    json={"data": [self._sample_input()]},
                    headers={"Authorization": f"Bearer {AML_API_KEY}"},
                    timeout=10,
                )
                latency = (time.time() - start) * 1000
                health["checks"]["endpoint"] = {
                    "status_code": resp.status_code,
                    "latency_ms": round(latency, 1),
                    "status": "ok" if resp.status_code == 200 else "error",
                }
        except Exception as e:
            health["checks"]["endpoint"] = {"status": "error", "error": str(e)}

        track_event("model_health_check", health)
        self.last_health_check = health
        return health

    def _get_recent_prediction_count(self, hours: int = 1) -> int:
        """Đếm predictions trong N giờ gần nhất."""
        import pyodbc
        conn_str = (
            f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM SalesForecasts WHERE created_at >= DATEADD(HOUR, ?, GETUTCDATE())",
            -hours,
        )
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def _get_recent_accuracy(self, days: int = None) -> dict | None:
        """Tính accuracy metrics từ predictions vs. actuals."""
        if days is None:
            days = DRIFT_CHECK_DAYS
        try:
            from mlops.drift_detector import fetch_prediction_accuracy
            df = fetch_prediction_accuracy(days=days)
            if df.empty:
                return None

            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
            actual = df["actual_revenue"].values
            predicted = df["predicted_revenue"].values

            return {
                "mae": round(float(mean_absolute_error(actual, predicted)), 4),
                "rmse": round(float(np.sqrt(mean_squared_error(actual, predicted))), 4),
                "r2_score": round(float(r2_score(actual, predicted)), 4),
                "n_samples": len(df),
                "status": "ok",
            }
        except Exception:
            return None

    def _sample_input(self) -> dict:
        """Tạo sample input để test endpoint."""
        return {
            "hour": 14, "day_of_month": 15, "month": 6,
            "is_weekend": 0, "hour_sin": 0.0, "hour_cos": -1.0,
            "month_sin": 0.0, "month_cos": -1.0,
            "day_of_week_encoded": 2, "region_encoded": 1,
            "category_encoded": 0, "temperature": 25.0,
            "humidity": 60.0, "is_rainy": 0,
        }

    def _send_alert(self, alert_type: str, message: str):
        """Gửi alert (với cooldown để tránh spam)."""
        now = datetime.utcnow()
        last_alert = self.alert_cooldown.get(alert_type)

        if last_alert and (now - last_alert) < timedelta(hours=1):
            return  # Still in cooldown

        self.alert_cooldown[alert_type] = now
        track_event("model_alert", {
            "alert_type": alert_type,
            "message": message,
            "severity": "warning",
        })
        logger.warning(f"[ALERT] {alert_type}: {message}")


# Singleton instance
monitor = ModelMonitor()
