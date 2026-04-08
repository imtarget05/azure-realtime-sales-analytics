"""
Tests for monitoring modules: model_health_check, ab_shadow_test, notifications.
These cover the new MLOps automation features added for full pipeline coverage.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pytest


# ── Model Health Check Tests ─────────────────────────────────────


class TestModelHealthCheck:
    """Tests for monitoring/model_health_check.py"""

    def setup_method(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.model_output = self.tmpdir / "ml" / "model_output"
        self.model_output.mkdir(parents=True)
        self.backup_dir = self.model_output / "rollback_backup"

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_model_files(self, target_dir: Path, version: str = "v1.0"):
        target_dir.mkdir(parents=True, exist_ok=True)
        meta = {
            "model_version": version,
            "trained_at": "2026-01-01T00:00:00Z",
            "training_samples": 1000,
            "revenue_metrics": {"r2_score": 0.95, "mae": 10.0},
            "quantity_metrics": {"r2_score": 0.90, "mae": 5.0},
        }
        (target_dir / "model_metadata.json").write_text(json.dumps(meta), encoding="utf-8")
        for f in ["revenue_model.pkl", "quantity_model.pkl", "label_encoders.pkl"]:
            (target_dir / f).write_bytes(b"fake_model_data")

    def test_backup_current_model(self):
        from monitoring.model_health_check import backup_current_model, MODEL_FILES

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.model_health_check.BACKUP_DIR", self.backup_dir):
            self._write_model_files(self.model_output, "v2.0")
            result = backup_current_model()
            assert result is True
            assert self.backup_dir.exists()
            for f in MODEL_FILES:
                assert (self.backup_dir / f).exists()

    def test_backup_fails_with_missing_files(self):
        from monitoring.model_health_check import backup_current_model

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.model_health_check.BACKUP_DIR", self.backup_dir):
            result = backup_current_model()
            assert result is False

    def test_rollback_model(self):
        from monitoring.model_health_check import rollback_model

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.model_health_check.BACKUP_DIR", self.backup_dir):
            # Create backup with backup_info.json
            self._write_model_files(self.backup_dir, "v1.0")
            (self.backup_dir / "backup_info.json").write_text(
                json.dumps({"model_version": "v1.0", "backed_up_at": "2026-01-01T00:00:00Z"}),
                encoding="utf-8",
            )
            # Override current model
            self._write_model_files(self.model_output, "v2.0")

            result = rollback_model()
            assert result["success"] is True
            assert result["rolled_back_to"] == "v1.0"

            # Verify current model is now v1.0
            with open(self.model_output / "model_metadata.json", "r") as f:
                meta = json.load(f)
            assert meta["model_version"] == "v1.0"

    def test_rollback_fails_no_backup(self):
        from monitoring.model_health_check import rollback_model

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.model_health_check.BACKUP_DIR", self.backup_dir):
            result = rollback_model()
            assert result["success"] is False

    def test_check_model_health_no_model(self):
        from monitoring.model_health_check import check_model_health

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output):
            result = check_model_health()
            assert result["healthy"] is True
            assert result["reason"] == "No model deployed"

    def test_check_model_health_no_drift_report(self):
        from monitoring.model_health_check import check_model_health

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output):
            self._write_model_files(self.model_output, "v1.0")
            # Override trained_at to be older than grace period
            meta_path = self.model_output / "model_metadata.json"
            meta = json.loads(meta_path.read_text())
            meta["trained_at"] = "2025-01-01T00:00:00Z"
            meta_path.write_text(json.dumps(meta))

            result = check_model_health()
            assert result["healthy"] is True
            assert "No drift report" in result["reason"]

    def test_check_model_health_high_mape(self):
        from monitoring.model_health_check import check_model_health

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output):
            self._write_model_files(self.model_output, "v1.0")
            # Old model (outside grace period)
            meta_path = self.model_output / "model_metadata.json"
            meta = json.loads(meta_path.read_text())
            meta["trained_at"] = "2025-01-01T00:00:00Z"
            meta_path.write_text(json.dumps(meta))

            # High MAPE drift report
            drift = {
                "metrics": {"mae": 30.0, "mape": 80.0, "n_samples": 100},
                "threshold_mae": 25.0,
                "triggered": True,
            }
            (self.model_output / "drift_monitor_report.json").write_text(json.dumps(drift))

            result = check_model_health()
            assert result["healthy"] is False
            assert "MAPE" in result["reason"]

    def test_run_health_check_healthy(self):
        from monitoring.model_health_check import run_health_check

        with patch("monitoring.model_health_check.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.model_health_check.HEALTH_REPORT_PATH",
                   self.model_output / "model_health_report.json"):
            result = run_health_check(dry_run=True)
            assert result["action_taken"] == "none"


# ── A/B Shadow Testing Tests ─────────────────────────────────────


class TestABShadowTest:
    """Tests for monitoring/ab_shadow_test.py"""

    def setup_method(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.model_output = self.tmpdir / "ml" / "model_output"
        self.model_output.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_enable_shadow(self):
        from monitoring.ab_shadow_test import enable_shadow

        with patch("monitoring.ab_shadow_test.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.ab_shadow_test.SHADOW_DIR", self.model_output / "shadow"), \
             patch("monitoring.ab_shadow_test.SHADOW_CONFIG_PATH",
                   self.model_output / "shadow_config.json"), \
             patch("monitoring.ab_shadow_test.SHADOW_LOG_PATH",
                   self.model_output / "shadow_predictions.jsonl"):
            config = enable_shadow(shadow_traffic_pct=15.0, duration_hours=12)
            assert config["enabled"] is True
            assert config["shadow_traffic_pct"] == 15.0
            assert config["duration_hours"] == 12

    def test_disable_shadow(self):
        from monitoring.ab_shadow_test import enable_shadow, disable_shadow

        config_path = self.model_output / "shadow_config.json"
        with patch("monitoring.ab_shadow_test.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.ab_shadow_test.SHADOW_DIR", self.model_output / "shadow"), \
             patch("monitoring.ab_shadow_test.SHADOW_CONFIG_PATH", config_path), \
             patch("monitoring.ab_shadow_test.SHADOW_LOG_PATH",
                   self.model_output / "shadow_predictions.jsonl"):
            enable_shadow()
            disable_shadow()
            with open(config_path, "r") as f:
                cfg = json.load(f)
            assert cfg["enabled"] is False

    def test_log_shadow_prediction(self):
        from monitoring.ab_shadow_test import log_shadow_prediction

        log_path = self.model_output / "shadow_predictions.jsonl"
        with patch("monitoring.ab_shadow_test.SHADOW_LOG_PATH", log_path):
            log_shadow_prediction(
                input_data={"hour": 12, "store_id": "S01", "product_id": "COKE"},
                primary_result={"predicted_revenue": 100.0, "predicted_quantity": 50},
                shadow_result={"predicted_revenue": 105.0, "predicted_quantity": 52},
            )
            assert log_path.exists()
            line = log_path.read_text().strip()
            entry = json.loads(line)
            assert entry["primary"]["revenue"] == 100.0
            assert entry["shadow"]["revenue"] == 105.0

    def test_evaluate_shadow_insufficient_data(self):
        from monitoring.ab_shadow_test import evaluate_shadow

        result_path = self.model_output / "shadow_result.json"
        log_path = self.model_output / "shadow_predictions.jsonl"
        with patch("monitoring.ab_shadow_test.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.ab_shadow_test.SHADOW_LOG_PATH", log_path), \
             patch("monitoring.ab_shadow_test.SHADOW_RESULT_PATH", result_path), \
             patch("monitoring.ab_shadow_test.SHADOW_CONFIG_PATH",
                   self.model_output / "shadow_config.json"):
            result = evaluate_shadow()
            assert result["decision"] == "INSUFFICIENT_DATA"

    def test_evaluate_shadow_with_correlated_data(self):
        from monitoring.ab_shadow_test import evaluate_shadow, log_shadow_prediction

        log_path = self.model_output / "shadow_predictions.jsonl"
        result_path = self.model_output / "shadow_result.json"
        config_path = self.model_output / "shadow_config.json"

        # Write config with low min_predictions
        config_path.write_text(json.dumps({
            "enabled": True, "min_predictions": 10
        }))

        with patch("monitoring.ab_shadow_test.MODEL_OUTPUT", self.model_output), \
             patch("monitoring.ab_shadow_test.SHADOW_LOG_PATH", log_path), \
             patch("monitoring.ab_shadow_test.SHADOW_RESULT_PATH", result_path), \
             patch("monitoring.ab_shadow_test.SHADOW_CONFIG_PATH", config_path):
            # Log highly correlated predictions
            for i in range(20):
                base = 100 + i * 5
                log_shadow_prediction(
                    input_data={"hour": i % 24},
                    primary_result={"predicted_revenue": base, "predicted_quantity": 10},
                    shadow_result={"predicted_revenue": base + 1, "predicted_quantity": 10},
                )

            result = evaluate_shadow()
            assert result["decision"] == "PROMOTE_SHADOW"
            assert result["metrics"]["correlation"] > 0.95

    def test_should_use_shadow_disabled(self):
        from monitoring.ab_shadow_test import should_use_shadow

        with patch("monitoring.ab_shadow_test.SHADOW_CONFIG_PATH",
                   self.model_output / "nonexistent.json"):
            assert should_use_shadow() is False


# ── Notifications Tests ──────────────────────────────────────────


class TestNotifications:
    """Tests for monitoring/notifications.py"""

    @patch("monitoring.notifications.requests.post")
    def test_send_slack_alert_success(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)

        from monitoring.notifications import send_slack_alert
        result = send_slack_alert(
            webhook_url="https://hooks.slack.com/test",
            message="Test drift alert",
            level="warning",
        )
        assert result["success"] is True
        mock_post.assert_called_once()

    @patch("monitoring.notifications.requests.post")
    def test_send_slack_alert_failure(self, mock_post):
        mock_post.side_effect = Exception("Connection refused")

        from monitoring.notifications import send_slack_alert
        result = send_slack_alert(
            webhook_url="https://hooks.slack.com/test",
            message="Test alert",
        )
        assert result["success"] is False

    @patch("monitoring.notifications.requests.post")
    def test_send_teams_alert(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)

        from monitoring.notifications import send_teams_alert
        report = {
            "metrics": {"mae": 30.0, "mape": 15.0, "n_samples": 50},
            "triggered": True,
            "threshold_mae": 25.0,
            "timestamp": "2026-01-01T00:00:00Z",
        }
        result = send_teams_alert("https://webhook.office.com/test", report)
        assert result["success"] is True

    def test_send_slack_empty_webhook(self):
        from monitoring.notifications import send_slack_alert
        result = send_slack_alert("", "test")
        assert result["success"] is False

    def test_send_teams_empty_webhook(self):
        from monitoring.notifications import send_teams_alert
        result = send_teams_alert("", {})
        assert result["success"] is False


# ── Alerts Module Tests ──────────────────────────────────────────


class TestAlerts:
    """Tests for monitoring/alerts.py"""

    @patch.dict(os.environ, {"ALERT_SLACK_WEBHOOK_URL": ""})
    def test_send_drift_alert_no_channels(self):
        from monitoring.alerts import send_drift_alert
        report = {
            "timestamp": "2026-01-01T00:00:00Z",
            "metrics": {"mae": 30.0, "mape": 15.0, "n_samples": 50},
            "triggered": True,
            "threshold_mae": 25.0,
            "window_hours": 24,
            "trigger_mode": "local",
        }
        result = send_drift_alert(report)
        assert isinstance(result, dict)

    @patch("monitoring.alerts.requests.post")
    @patch.dict(os.environ, {"ALERT_SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"})
    def test_send_drift_alert_slack(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)

        from monitoring.alerts import send_drift_alert
        report = {
            "timestamp": "2026-01-01T00:00:00Z",
            "metrics": {"mae": 30.0, "mape": 15.0, "n_samples": 50},
            "triggered": True,
            "threshold_mae": 25.0,
            "window_hours": 24,
            "trigger_mode": "local",
        }
        result = send_drift_alert(report)
        assert "slack" in result


# ── Drift Monitor Compute Metrics Tests ──────────────────────────


class TestDriftMonitorMetrics:
    """Additional tests for ml/drift_monitor.py compute_metrics function."""

    def test_compute_metrics_empty(self):
        import pandas as pd
        from ml.drift_monitor import compute_metrics
        result = compute_metrics(pd.DataFrame())
        assert result["n_samples"] == 0
        assert result["mae"] == 0.0

    def test_compute_metrics_perfect(self):
        import pandas as pd
        from ml.drift_monitor import compute_metrics
        df = pd.DataFrame({
            "predicted_revenue": [100, 200, 300],
            "actual_revenue": [100, 200, 300],
        })
        result = compute_metrics(df)
        assert result["mae"] == 0.0
        assert result["mape"] == 0.0
        assert result["n_samples"] == 3

    def test_compute_metrics_drift(self):
        import pandas as pd
        from ml.drift_monitor import compute_metrics
        df = pd.DataFrame({
            "predicted_revenue": [100, 200, 300],
            "actual_revenue": [130, 230, 330],
        })
        result = compute_metrics(df)
        assert result["mae"] == 30.0
        assert result["n_samples"] == 3


# ── Health Endpoint Enhanced Tests ───────────────────────────────


class TestHealthEndpoint:
    """Tests for enhanced /api/health endpoint."""

    @pytest.fixture
    def client(self):
        from webapp.app import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c

    def test_health_returns_json(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "healthy"

    def test_health_contains_model_version(self, client):
        resp = client.get("/api/health")
        data = resp.get_json()
        assert "model_version" in data

    def test_health_contains_drift_monitor(self, client):
        resp = client.get("/api/health")
        data = resp.get_json()
        assert "drift_monitor" in data
        assert "status" in data["drift_monitor"]

    def test_health_contains_rollback_info(self, client):
        resp = client.get("/api/health")
        data = resp.get_json()
        assert "rollback_available" in data


# ── Dashboard Rendering Tests ────────────────────────────────────


class TestDashboardRendering:
    """Tests for /dashboard with various data states."""

    @pytest.fixture
    def client(self):
        from webapp.app import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c

    def test_dashboard_empty_state(self, client):
        resp = client.get("/dashboard")
        assert resp.status_code == 200
        html = resp.data.decode()
        assert "Monitoring Dashboard" in html

    def test_dashboard_has_sections(self, client):
        resp = client.get("/dashboard")
        html = resp.data.decode()
        assert "Model Version" in html
        assert "Current MAE" in html
        assert "System Status" in html
        assert "Monitoring Events" in html

    def test_dashboard_auto_refresh_meta(self, client):
        resp = client.get("/dashboard")
        html = resp.data.decode()
        assert 'http-equiv="refresh"' in html

    def test_dashboard_has_sse_connection(self, client):
        resp = client.get("/dashboard")
        html = resp.data.decode()
        assert "EventSource" in html

    def test_dashboard_has_navigation(self, client):
        resp = client.get("/dashboard")
        html = resp.data.decode()
        assert "/model-report" in html
        assert "/api/health" in html


# ── SSE Endpoint Tests ──────────────────────────────────────────


class TestSSEEndpoint:
    """Tests for /api/sse/dashboard SSE endpoint."""

    @pytest.fixture
    def client(self):
        from webapp.app import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c

    def test_sse_returns_event_stream(self, client):
        resp = client.get("/api/sse/dashboard")
        assert resp.content_type.startswith("text/event-stream")

    def test_sse_no_cache(self, client):
        resp = client.get("/api/sse/dashboard")
        assert resp.headers.get("Cache-Control") == "no-cache"


# ── Model Report Navigation Tests ───────────────────────────────


class TestModelReportNavigation:
    """Tests that model_report has correct navigation (no retrain links)."""

    @pytest.fixture
    def client(self):
        from webapp.app import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c

    def test_model_report_no_retrain_link(self, client):
        resp = client.get("/model-report")
        html = resp.data.decode()
        assert 'href="/retrain"' not in html

    def test_model_report_has_dashboard_link(self, client):
        resp = client.get("/model-report")
        html = resp.data.decode()
        assert "/dashboard" in html

    def test_model_report_has_health_link(self, client):
        resp = client.get("/model-report")
        html = resp.data.decode()
        assert "/api/health" in html


# ── Cooldown Logic Tests ─────────────────────────────────────────


class TestCooldownLogic:
    """Tests for drift monitor cooldown mechanism."""

    def test_not_in_cooldown_never_triggered(self):
        from ml.drift_monitor import _in_cooldown
        assert _in_cooldown("", 120) is False

    def test_in_cooldown_recent(self):
        from ml.drift_monitor import _in_cooldown
        from datetime import datetime
        recent = datetime.utcnow().isoformat() + "Z"
        assert _in_cooldown(recent, 120) is True

    def test_not_in_cooldown_old(self):
        from ml.drift_monitor import _in_cooldown
        old = "2020-01-01T00:00:00Z"
        assert _in_cooldown(old, 120) is False

    def test_in_cooldown_invalid_timestamp(self):
        from ml.drift_monitor import _in_cooldown
        assert _in_cooldown("invalid-date", 120) is False


# ── Distributed Lock Tests ───────────────────────────────────────


class TestDistributedLock:
    """Tests for drift monitor file-based distributed lock."""

    def setup_method(self):
        self.tmpdir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_acquire_and_release(self):
        from ml.drift_monitor import _acquire_lock, _release_lock
        lock_path = self.tmpdir / "test.lock"
        with patch("ml.drift_monitor.LOCK_PATH", lock_path), \
             patch("ml.drift_monitor.MODEL_OUTPUT_DIR", self.tmpdir):
            assert _acquire_lock() is True
            assert lock_path.exists()
            _release_lock()
            assert not lock_path.exists()

    def test_lock_prevents_double_acquire(self):
        from ml.drift_monitor import _acquire_lock, _release_lock
        lock_path = self.tmpdir / "test.lock"
        with patch("ml.drift_monitor.LOCK_PATH", lock_path), \
             patch("ml.drift_monitor.MODEL_OUTPUT_DIR", self.tmpdir):
            assert _acquire_lock() is True
            # Same PID — second acquire should fail (lock held by us, process alive)
            assert _acquire_lock() is False
            _release_lock()

    def test_stale_lock_is_removed(self):
        from ml.drift_monitor import _acquire_lock, _release_lock, _utcnow_iso
        lock_path = self.tmpdir / "test.lock"
        # Write a lock with a non-existent PID
        lock_data = {"pid": 99999999, "created_at": _utcnow_iso()}
        lock_path.write_text(json.dumps(lock_data), encoding="utf-8")
        with patch("ml.drift_monitor.LOCK_PATH", lock_path), \
             patch("ml.drift_monitor.MODEL_OUTPUT_DIR", self.tmpdir):
            assert _acquire_lock() is True
            _release_lock()


# ── Run Monitor Tests ────────────────────────────────────────────


class TestRunMonitor:
    """Tests for the run_monitor orchestration function."""

    def setup_method(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.model_output = self.tmpdir / "model_output"
        self.model_output.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_run_monitor_no_drift(self):
        """When MAE is below threshold, should not trigger retrain."""
        import pandas as pd
        from ml.drift_monitor import run_monitor

        fake_df = pd.DataFrame({
            "predicted_revenue": [100.0, 200.0, 300.0],
            "actual_revenue": [102.0, 198.0, 301.0],
        })

        with patch("ml.drift_monitor.fetch_recent_forecast_vs_actual", return_value=fake_df), \
             patch("ml.drift_monitor.LOCK_PATH", self.model_output / "test.lock"), \
             patch("ml.drift_monitor.MODEL_OUTPUT_DIR", self.model_output), \
             patch("ml.drift_monitor.REPORT_PATH", self.model_output / "report.json"), \
             patch("ml.drift_monitor.STATE_PATH", self.model_output / "state.json"), \
             patch("monitoring.notifications.log_to_sql"):
            report = run_monitor(
                threshold_mae=50.0,
                window_hours=24,
                min_samples=1,
                cooldown_minutes=120,
                trigger_mode="local",
                trigger_github_actions=False,
                dry_run=False,
            )
        assert report["status"] == "ok"
        assert report["triggered"] is False
        assert report["metrics"]["mae"] < 50.0

    def test_run_monitor_drift_detected_dry_run(self):
        """When MAE exceeds threshold in dry_run mode, should report trigger without retraining."""
        import pandas as pd
        from ml.drift_monitor import run_monitor

        fake_df = pd.DataFrame({
            "predicted_revenue": [100.0, 200.0, 300.0],
            "actual_revenue": [200.0, 400.0, 500.0],
        })

        with patch("ml.drift_monitor.fetch_recent_forecast_vs_actual", return_value=fake_df), \
             patch("ml.drift_monitor.LOCK_PATH", self.model_output / "test.lock"), \
             patch("ml.drift_monitor.MODEL_OUTPUT_DIR", self.model_output), \
             patch("ml.drift_monitor.REPORT_PATH", self.model_output / "report.json"), \
             patch("ml.drift_monitor.STATE_PATH", self.model_output / "state.json"), \
             patch("monitoring.notifications.log_to_sql"), \
             patch("monitoring.alerts.send_drift_alert", return_value={"slack": True}):
            report = run_monitor(
                threshold_mae=10.0,
                window_hours=24,
                min_samples=1,
                cooldown_minutes=120,
                trigger_mode="local",
                trigger_github_actions=False,
                dry_run=True,
            )
        assert report["status"] == "ok"
        assert report["triggered"] is True
        assert report["trigger_result"]["dry_run"] is True

    def test_run_monitor_skipped_when_locked(self):
        """When lock is held, run_monitor should return skipped status."""
        from ml.drift_monitor import run_monitor

        lock_path = self.model_output / "test.lock"
        # Pre-create a lock held by current PID (simulates already running)
        lock_data = {"pid": os.getpid(), "created_at": "2099-01-01T00:00:00Z"}
        lock_path.write_text(json.dumps(lock_data), encoding="utf-8")

        with patch("ml.drift_monitor.LOCK_PATH", lock_path), \
             patch("ml.drift_monitor.MODEL_OUTPUT_DIR", self.model_output), \
             patch("ml.drift_monitor.REPORT_PATH", self.model_output / "report.json"):
            report = run_monitor(
                threshold_mae=25.0,
                window_hours=24,
                min_samples=24,
                cooldown_minutes=120,
                trigger_mode="local",
                trigger_github_actions=False,
                dry_run=False,
            )
        assert report["status"] == "skipped"
        assert report["triggered"] is False
