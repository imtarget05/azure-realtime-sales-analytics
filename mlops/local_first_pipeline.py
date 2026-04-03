"""
Local-first MLOps pipeline for demo and stabilization before Azure deployment.

Flow:
  1) Bootstrap local model artifacts if missing
  2) Retrain + gate check + optional promote
  3) Smoke test local prediction path
  4) Save pipeline report to ml/model_output/local_pipeline_report.json

Usage examples:
  python mlops/local_first_pipeline.py
  python mlops/local_first_pipeline.py --bootstrap-samples 20000 --retrain-samples 30000
  python mlops/local_first_pipeline.py --no-promote
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODEL_DIR = os.path.join(ROOT_DIR, "ml", "model_output")
REPORT_PATH = os.path.join(MODEL_DIR, "local_pipeline_report.json")


def _now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _run_cmd(cmd: list[str], timeout: int = 1200, env: dict | None = None) -> dict:
    """Run subprocess and return structured result with logs."""
    started_at = _now()
    proc = subprocess.run(
        cmd,
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    return {
        "command": " ".join(cmd),
        "returncode": proc.returncode,
        "started_at": started_at,
        "finished_at": _now(),
        "stdout_tail": (proc.stdout or "")[-6000:],
        "stderr_tail": (proc.stderr or "")[-4000:],
    }


def _model_artifacts_exist() -> bool:
    required = [
        os.path.join(MODEL_DIR, "revenue_model.pkl"),
        os.path.join(MODEL_DIR, "quantity_model.pkl"),
        os.path.join(MODEL_DIR, "label_encoders.pkl"),
        os.path.join(MODEL_DIR, "model_metadata.json"),
    ]
    return all(os.path.exists(p) for p in required)


def _ensure_bootstrap_model(bootstrap_samples: int) -> dict:
    if _model_artifacts_exist():
        return {
            "step": "bootstrap_train",
            "skipped": True,
            "reason": "model artifacts already exist",
            "finished_at": _now(),
        }

    os.makedirs(MODEL_DIR, exist_ok=True)
    cmd = [
        sys.executable,
        os.path.join("ml", "train_model.py"),
        "--output-dir",
        os.path.join("ml", "model_output"),
        "--n-samples",
        str(bootstrap_samples),
    ]
    result = _run_cmd(cmd, timeout=1800, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    result["step"] = "bootstrap_train"
    result["ok"] = result["returncode"] == 0
    return result


def _run_retrain(retrain_samples: int, n_estimators: int, max_depth: int, lr: float, promote: bool) -> dict:
    cmd = [
        sys.executable,
        os.path.join("ml", "retrain_and_compare.py"),
        "--new-samples",
        str(retrain_samples),
        "--new-estimators",
        str(n_estimators),
        "--new-depth",
        str(max_depth),
        "--new-lr",
        str(lr),
    ]
    if promote:
        cmd.append("--promote")

    result = _run_cmd(cmd, timeout=2400, env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    result["step"] = "retrain_compare"
    result["ok"] = result["returncode"] == 0
    return result


def _smoke_test_local_prediction() -> dict:
    """Run one local prediction with cloud endpoint disabled to ensure fallback works."""
    code = (
        "import json;"
        "from webapp.app import call_ml_endpoint;"
        "sample={'hour':14,'day_of_month':15,'month':6,'is_weekend':0,'store_id':'S01',"
        "'product_id':'COKE','category':'Beverage','temperature':28,'is_rainy':0,'holiday':0};"
        "res=call_ml_endpoint(sample);"
        "print(json.dumps({'status':res.get('status'),'source':res.get('source')}, ensure_ascii=False))"
    )
    env = {
        **os.environ,
        "PYTHONIOENCODING": "utf-8",
        "AML_ENDPOINT_URL": "",
        "ML_ENDPOINT_URL": "",
        "AML_API_KEY": "",
        "ML_API_KEY": "",
    }
    cmd = [sys.executable, "-c", code]
    result = _run_cmd(cmd, timeout=120, env=env)
    result["step"] = "smoke_predict"
    result["ok"] = result["returncode"] == 0

    source = None
    if result["ok"]:
        lines = [ln.strip() for ln in result["stdout_tail"].splitlines() if ln.strip()]
        if lines:
            try:
                payload = json.loads(lines[-1])
                source = payload.get("source")
            except Exception:
                source = None
    result["prediction_source"] = source
    return result


def run_local_pipeline(
    bootstrap_samples: int,
    retrain_samples: int,
    n_estimators: int,
    max_depth: int,
    learning_rate: float,
    promote: bool,
) -> dict:
    report = {
        "pipeline": "local-first-mlops",
        "started_at": _now(),
        "params": {
            "bootstrap_samples": bootstrap_samples,
            "retrain_samples": retrain_samples,
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "promote": promote,
        },
        "steps": [],
    }

    bootstrap = _ensure_bootstrap_model(bootstrap_samples)
    report["steps"].append(bootstrap)
    if bootstrap.get("ok") is False:
        report["status"] = "failed"
        report["failed_step"] = "bootstrap_train"
        report["finished_at"] = _now()
        return report

    retrain = _run_retrain(retrain_samples, n_estimators, max_depth, learning_rate, promote)
    report["steps"].append(retrain)
    if not retrain.get("ok"):
        report["status"] = "failed"
        report["failed_step"] = "retrain_compare"
        report["finished_at"] = _now()
        return report

    smoke = _smoke_test_local_prediction()
    report["steps"].append(smoke)
    if not smoke.get("ok"):
        report["status"] = "failed"
        report["failed_step"] = "smoke_predict"
    else:
        report["status"] = "success"

    report["finished_at"] = _now()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local-first MLOps pipeline")
    parser.add_argument("--bootstrap-samples", type=int, default=30000)
    parser.add_argument("--retrain-samples", type=int, default=50000)
    parser.add_argument("--n-estimators", type=int, default=250)
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--learning-rate", type=float, default=0.1)
    parser.add_argument("--no-promote", action="store_true", help="Do not promote retrained model")
    args = parser.parse_args()

    report = run_local_pipeline(
        bootstrap_samples=args.bootstrap_samples,
        retrain_samples=args.retrain_samples,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        promote=not args.no_promote,
    )

    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("=" * 60)
    print("LOCAL-FIRST MLOPS PIPELINE")
    print("Status:", report.get("status"))
    print("Report:", REPORT_PATH)
    print("=" * 60)

    return 0 if report.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
