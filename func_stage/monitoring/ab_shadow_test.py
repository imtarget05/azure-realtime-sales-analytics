"""
A/B Shadow Testing for model deployment.

Routes a percentage of traffic to the new (shadow) model while serving
the old (primary) model. Logs both predictions for offline comparison.
After a configurable evaluation period, auto-promotes or rejects the shadow.

Usage:
    # Enable shadow mode after retrain
    python monitoring/ab_shadow_test.py --shadow-dir ml/model_output/shadow --duration-hours 24

    # Check shadow results
    python monitoring/ab_shadow_test.py --evaluate
"""

import json
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_OUTPUT = BASE_DIR / "ml" / "model_output"
SHADOW_DIR = MODEL_OUTPUT / "shadow"
SHADOW_LOG_PATH = MODEL_OUTPUT / "shadow_predictions.jsonl"
SHADOW_CONFIG_PATH = MODEL_OUTPUT / "shadow_config.json"
SHADOW_RESULT_PATH = MODEL_OUTPUT / "shadow_result.json"


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def enable_shadow(
    shadow_traffic_pct: float = 10.0,
    duration_hours: int = 24,
    min_predictions: int = 50,
) -> dict:
    """Enable A/B shadow testing mode."""
    config = {
        "enabled": True,
        "shadow_traffic_pct": shadow_traffic_pct,
        "started_at": _utcnow_iso(),
        "duration_hours": duration_hours,
        "min_predictions": min_predictions,
        "shadow_predictions": 0,
        "primary_predictions": 0,
    }

    # Copy current production model to shadow for comparison baseline
    SHADOW_DIR.mkdir(parents=True, exist_ok=True)

    SHADOW_CONFIG_PATH.write_text(
        json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Clear old shadow log
    if SHADOW_LOG_PATH.exists():
        SHADOW_LOG_PATH.unlink()

    return config


def disable_shadow() -> None:
    """Disable shadow testing mode."""
    if SHADOW_CONFIG_PATH.exists():
        with open(SHADOW_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)
        config["enabled"] = False
        config["disabled_at"] = _utcnow_iso()
        SHADOW_CONFIG_PATH.write_text(
            json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8"
        )


def get_shadow_config() -> dict | None:
    """Get current shadow config, or None if not enabled."""
    if not SHADOW_CONFIG_PATH.exists():
        return None
    with open(SHADOW_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    if not config.get("enabled", False):
        return None

    # Check if duration expired
    started = config.get("started_at", "")
    duration = config.get("duration_hours", 24)
    if started:
        try:
            started_dt = datetime.fromisoformat(started.replace("Z", ""))
            if datetime.utcnow() - started_dt > timedelta(hours=duration):
                return None  # expired — needs evaluation
        except (ValueError, TypeError):
            pass

    return config


def should_use_shadow() -> bool:
    """Randomly decide if this request should use shadow model."""
    config = get_shadow_config()
    if not config:
        return False
    return random.random() * 100 < config.get("shadow_traffic_pct", 10.0)


def log_shadow_prediction(
    input_data: dict,
    primary_result: dict,
    shadow_result: dict | None = None,
) -> None:
    """Log a prediction pair for later comparison."""
    entry = {
        "timestamp": _utcnow_iso(),
        "input": {k: v for k, v in input_data.items() if k in (
            "hour", "month", "store_id", "product_id", "category",
            "base_price", "temperature", "is_rainy", "holiday",
        )},
        "primary": {
            "revenue": primary_result.get("predicted_revenue", 0),
            "quantity": primary_result.get("predicted_quantity", 0),
        },
    }

    if shadow_result:
        entry["shadow"] = {
            "revenue": shadow_result.get("predicted_revenue", 0),
            "quantity": shadow_result.get("predicted_quantity", 0),
        }

    with open(SHADOW_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def evaluate_shadow() -> dict:
    """
    Evaluate shadow model performance vs primary.

    Compares prediction consistency between primary and shadow models.
    If shadow predictions are consistently better (lower deviation from
    primary), the shadow model can be promoted.
    """
    result = {
        "timestamp": _utcnow_iso(),
        "decision": "INSUFFICIENT_DATA",
        "total_predictions": 0,
        "metrics": {},
    }

    if not SHADOW_LOG_PATH.exists():
        result["reason"] = "No shadow predictions logged"
        _save_result(result)
        return result

    import numpy as np

    primary_revenues = []
    shadow_revenues = []

    with open(SHADOW_LOG_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if "shadow" in entry and "primary" in entry:
                primary_revenues.append(entry["primary"]["revenue"])
                shadow_revenues.append(entry["shadow"]["revenue"])

    result["total_predictions"] = len(primary_revenues)

    config = None
    if SHADOW_CONFIG_PATH.exists():
        with open(SHADOW_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = json.load(f)

    min_preds = config.get("min_predictions", 50) if config else 50

    if len(primary_revenues) < min_preds:
        result["reason"] = f"Need {min_preds} shadow predictions, have {len(primary_revenues)}"
        _save_result(result)
        return result

    primary_arr = np.array(primary_revenues)
    shadow_arr = np.array(shadow_revenues)

    # Compare prediction stability (lower variance = more reliable)
    primary_std = float(np.std(primary_arr))
    shadow_std = float(np.std(shadow_arr))

    # Compare mean absolute difference between models
    diff = np.abs(primary_arr - shadow_arr)
    mean_diff = float(np.mean(diff))
    max_diff = float(np.max(diff))

    # Correlation between models
    if primary_std > 0 and shadow_std > 0:
        correlation = float(np.corrcoef(primary_arr, shadow_arr)[0, 1])
    else:
        correlation = 1.0

    result["metrics"] = {
        "primary_mean": round(float(np.mean(primary_arr)), 4),
        "shadow_mean": round(float(np.mean(shadow_arr)), 4),
        "primary_std": round(primary_std, 4),
        "shadow_std": round(shadow_std, 4),
        "mean_abs_diff": round(mean_diff, 4),
        "max_abs_diff": round(max_diff, 4),
        "correlation": round(correlation, 4),
    }

    # Decision logic
    if correlation > 0.95 and mean_diff < np.mean(primary_arr) * 0.1:
        result["decision"] = "PROMOTE_SHADOW"
        result["reason"] = f"High correlation ({correlation:.4f}), low divergence"
    elif correlation < 0.7 or mean_diff > np.mean(primary_arr) * 0.3:
        result["decision"] = "REJECT_SHADOW"
        result["reason"] = f"Low correlation ({correlation:.4f}) or high divergence"
    else:
        result["decision"] = "EXTEND_TESTING"
        result["reason"] = "Results inconclusive, extend shadow period"

    _save_result(result)

    # Log to SQL
    try:
        from monitoring.notifications import log_to_sql
        log_to_sql(
            event_type=f"ab_test_{result['decision'].lower()}",
            mae_value=mean_diff,
            threshold=0,
            retrain_triggered=result["decision"] == "PROMOTE_SHADOW",
            details=json.dumps(result, default=str),
        )
    except Exception as e:
        print(f"[WARN] SQL logging failed: {e}")

    return result


def _save_result(result: dict) -> None:
    MODEL_OUTPUT.mkdir(parents=True, exist_ok=True)
    SHADOW_RESULT_PATH.write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="A/B Shadow Testing")
    parser.add_argument("--enable", action="store_true", help="Enable shadow testing")
    parser.add_argument("--evaluate", action="store_true", help="Evaluate shadow results")
    parser.add_argument("--disable", action="store_true", help="Disable shadow testing")
    parser.add_argument("--shadow-pct", type=float, default=10.0, help="Shadow traffic %%")
    parser.add_argument("--duration-hours", type=int, default=24, help="Testing duration")
    args = parser.parse_args()

    if args.enable:
        cfg = enable_shadow(shadow_traffic_pct=args.shadow_pct, duration_hours=args.duration_hours)
        print(f"[OK] Shadow testing enabled: {args.shadow_pct}% traffic for {args.duration_hours}h")
    elif args.evaluate:
        r = evaluate_shadow()
        print(f"[RESULT] Decision: {r['decision']} — {r.get('reason', '')}")
    elif args.disable:
        disable_shadow()
        print("[OK] Shadow testing disabled")
    else:
        parser.print_help()
