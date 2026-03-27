"""
Azure Monitor + Application Insights integration.

Provides centralized logging, metrics, and alerting for all pipeline components:
  - Azure Functions → Logs & invocation metrics
  - Stream Analytics → Job metrics & errors
  - Azure SQL → Query performance & DTU usage
  - ML Endpoint → Inference latency & error rate

Usage:
    from monitoring.telemetry import get_logger, track_event, track_metric

    logger = get_logger("data_generator")
    logger.info("Generator started")

    track_event("SalesEventGenerated", {"store_id": "S01", "product_id": "COKE"})
    track_metric("EventLatencyMs", 142.5)
"""

import logging
import os
import time
from functools import wraps
from typing import Any

# Application Insights SDK
try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
    from opencensus.ext.azure import metrics_exporter
    from opencensus.stats import aggregation, measure, stats, view
    _APPINSIGHTS_AVAILABLE = True
except ImportError:
    _APPINSIGHTS_AVAILABLE = False

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
APPINSIGHTS_CONNECTION_STRING = os.getenv(
    "APPINSIGHTS_CONNECTION_STRING",
    os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
)

_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get a logger that sends to both console and Application Insights."""
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Console handler
    if not logger.handlers:
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(console)

    # Application Insights handler
    if _APPINSIGHTS_AVAILABLE and APPINSIGHTS_CONNECTION_STRING:
        try:
            azure_handler = AzureLogHandler(
                connection_string=APPINSIGHTS_CONNECTION_STRING
            )
            azure_handler.setLevel(level)
            logger.addHandler(azure_handler)
        except Exception as exc:
            logger.warning("Could not attach App Insights handler: %s", exc)

    _loggers[name] = logger
    return logger


def track_event(name: str, properties: dict[str, Any] | None = None) -> None:
    """Send a custom event to Application Insights."""
    logger = get_logger("telemetry")
    props = properties or {}
    logger.info("CustomEvent: %s | %s", name, props)


def track_metric(name: str, value: float, properties: dict[str, Any] | None = None) -> None:
    """Send a custom metric to Application Insights."""
    logger = get_logger("telemetry")
    logger.info("Metric: %s = %.4f | %s", name, value, properties or {})


def track_dependency(name: str, target: str, duration_ms: float, success: bool) -> None:
    """Track an external dependency call (SQL, API, etc.)."""
    logger = get_logger("telemetry")
    status = "success" if success else "failed"
    logger.info("Dependency: %s → %s [%s] %.1fms", name, target, status, duration_ms)


def track_exception(exc: Exception, properties: dict[str, Any] | None = None) -> None:
    """Track an exception in Application Insights."""
    logger = get_logger("telemetry")
    logger.error("Exception: %s | %s", exc, properties or {}, exc_info=True)


# ──────────────────────────────────────────────
# Decorator for automatic performance tracking
# ──────────────────────────────────────────────
def monitor_performance(component: str):
    """Decorator to track function execution time and errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            success = True
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as exc:
                success = False
                track_exception(exc, {"component": component, "function": func.__name__})
                raise
            finally:
                duration_ms = (time.time() - start) * 1000
                track_metric(f"{component}.duration_ms", duration_ms)
                track_dependency(
                    name=f"{component}.{func.__name__}",
                    target=component,
                    duration_ms=duration_ms,
                    success=success,
                )
        return wrapper
    return decorator


# ──────────────────────────────────────────────
# Pipeline health dashboard data
# ──────────────────────────────────────────────
class PipelineHealthMonitor:
    """Collects and reports health status for all pipeline components."""

    COMPONENTS = [
        "data_generator",
        "event_hubs",
        "azure_functions",
        "stream_analytics",
        "azure_sql",
        "ml_endpoint",
        "power_bi",
    ]

    def __init__(self):
        self.logger = get_logger("pipeline_health")
        self._status: dict[str, dict] = {}

    def update_status(self, component: str, healthy: bool, details: str = "") -> None:
        """Update health status for a component."""
        self._status[component] = {
            "healthy": healthy,
            "details": details,
            "last_check": time.time(),
        }
        level = logging.INFO if healthy else logging.WARNING
        self.logger.log(level, "Health [%s]: %s — %s", component,
                        "HEALTHY" if healthy else "UNHEALTHY", details)

    def get_report(self) -> dict[str, Any]:
        """Get full pipeline health report."""
        all_healthy = all(s.get("healthy", False) for s in self._status.values())
        return {
            "overall": "HEALTHY" if all_healthy else "DEGRADED",
            "components": self._status,
            "checked_at": time.time(),
        }
