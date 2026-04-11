"""
AML Job Entry Point: Train model AND register to AML model registry.

Runs INSIDE Azure ML compute cluster (CommandJob).
Triggered by ADF → WebActivity → AML Jobs REST API.

Flow:
  1. Load data (Azure SQL → synthetic fallback)
  2. Train revenue + quantity GradientBoosting models
  3. Save artifacts to ./outputs/model_output/
  4. Register model in AML workspace registry (via azureml.core)
  5. Log metrics (AML + MLflow)
"""

import os
import sys
import json
import argparse
from datetime import datetime

# Allow imports from the same directory
_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _DIR)
sys.path.insert(0, os.path.join(_DIR, ".."))

# ── Training helpers from train_model.py ─────────────────────────
from train_model import (
    load_sql_training_data,
    generate_training_data,
    prepare_features,
    train_one_model,
    generate_charts,
    _generate_summary_chart,
)

import joblib
import numpy as np
import sklearn

# ── AML v1 SDK (available via azureml-defaults in conda_env.yml) ─
AZUREML_AVAILABLE = False
try:
    from azureml.core import Run, Model
    AZUREML_AVAILABLE = True
except ImportError:
    print("[WARN] azureml.core not available — model will not be auto-registered")

# ── MLflow (backup logging) ────────────────────────────────────────
MLFLOW_AVAILABLE = False
try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    pass


def main():
    parser = argparse.ArgumentParser(description="Train + Register Sales Forecast Model")
    parser.add_argument("--n-samples", type=int, default=30000,
                        help="Number of synthetic samples if SQL unavailable.")
    parser.add_argument("--output-dir", type=str, default="outputs/model_output",
                        help="Directory to save model artifacts.")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print("=" * 60)
    print("  SALES FORECAST TRAINING JOB")
    print(f"  Start: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    # ── Get AML run context ───────────────────────────────────────
    run = None
    workspace = None
    if AZUREML_AVAILABLE:
        try:
            run = Run.get_context()
            # If offline run (not inside AML), workspace will be None
            if hasattr(run, "experiment") and hasattr(run.experiment, "workspace"):
                workspace = run.experiment.workspace
                print(f"[AML] Connected to workspace: {workspace.name}")
            else:
                run = None
        except Exception as e:
            print(f"[WARN] Could not get AML run context: {e}")
            run = None

    # ── Load data ─────────────────────────────────────────────────
    sql_df = load_sql_training_data(min_samples=1000)
    if sql_df is not None:
        df = sql_df
        data_source = "sql"
        print(f"[DATA] Using Azure SQL: {len(df)} samples")
    else:
        print(f"[DATA] SQL unavailable — generating {args.n_samples} synthetic samples")
        df = generate_training_data(args.n_samples)
        data_source = "synthetic"

    # ── Feature engineering ───────────────────────────────────────
    X, y_qty, y_rev, label_encoders, feature_cols = prepare_features(df)

    # ── Train models ──────────────────────────────────────────────
    rev_model, rev_metrics, X_tr_r, X_te_r, y_tr_r, y_te_r, y_pred_r = train_one_model(X, y_rev, "revenue")
    qty_model, qty_metrics, X_tr_q, X_te_q, y_tr_q, y_te_q, y_pred_q = train_one_model(X, y_qty, "quantity")

    rev_r2 = rev_metrics.get("r2_score", 0)
    print(f"\n[RESULT] Revenue R²={rev_r2:.4f}  |  Quantity R²={qty_metrics.get('r2_score', 0):.4f}")

    # ── Save artifacts ────────────────────────────────────────────
    joblib.dump(rev_model,      os.path.join(args.output_dir, "revenue_model.pkl"))
    joblib.dump(qty_model,      os.path.join(args.output_dir, "quantity_model.pkl"))
    joblib.dump(label_encoders, os.path.join(args.output_dir, "label_encoders.pkl"))

    metadata = {
        "feature_columns": feature_cols,
        "categorical_columns": ["store_id", "product_id", "category"],
        "revenue_metrics": rev_metrics,
        "quantity_metrics": qty_metrics,
        "training_samples": len(df),
        "data_source": data_source,
        "trained_at": datetime.utcnow().isoformat(),
        "model_version": "v2.0",
        "sklearn_version": sklearn.__version__,
        "revenue_r2": rev_r2,
        "revenue_rmse": rev_metrics.get("rmse"),
        "algorithm": "GradientBoostingRegressor",
        "n_features": len(feature_cols),
    }
    meta_path = os.path.join(args.output_dir, "model_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"[SAVE] Artifacts → {args.output_dir}")

    # ── Log metrics to AML ────────────────────────────────────────
    if run:
        try:
            for k, v in rev_metrics.items():
                if isinstance(v, (int, float)):
                    run.log(f"revenue_{k}", v)
            for k, v in qty_metrics.items():
                if isinstance(v, (int, float)):
                    run.log(f"quantity_{k}", v)
            run.log("training_samples", len(df))
            run.log("data_source_sql", 1 if data_source == "sql" else 0)
            print("[AML] Metrics logged to run")
        except Exception as e:
            print(f"[WARN] Metric logging failed: {e}")

    if MLFLOW_AVAILABLE:
        try:
            mlflow.log_metric("revenue_r2", rev_r2)
            mlflow.log_metric("revenue_mae", rev_metrics.get("mae", 0))
            mlflow.log_metric("quantity_r2", qty_metrics.get("r2_score", 0))
        except Exception:
            pass

    # ── Register model in AML registry ───────────────────────────
    if workspace is not None:
        try:
            print(f"\n[REGISTER] Registering 'sales-forecast-model' in workspace {workspace.name}...")
            model = Model.register(
                workspace=workspace,
                model_path=args.output_dir,
                model_name="sales-forecast-model",
                description=(
                    f"ADF MLOps pipeline — {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} | "
                    f"R²={rev_r2:.4f} | source={data_source}"
                ),
                tags={
                    "r2_score":         str(rev_r2),
                    "mae":              str(rev_metrics.get("mae", "")),
                    "rmse":             str(rev_metrics.get("rmse", "")),
                    "source":           "adf_mlops_pipeline",
                    "data_source":      data_source,
                    "training_samples": str(len(df)),
                    "trained_at":       datetime.utcnow().isoformat(),
                    "metric_r2_score":  str(rev_r2),
                    "algorithm":        "GradientBoostingRegressor",
                    "sklearn_version":  sklearn.__version__,
                },
            )
            print(f"[REGISTER] SUCCESS: {model.name} v{model.version} (R²={rev_r2:.4f})")
        except Exception as e:
            print(f"[WARN] Auto-registration failed: {e}")
            print("[WARN] Artifacts saved — manual registration possible from outputs/")
    else:
        print("[INFO] Running outside AML context — skipping auto-registration")
        print(f"[INFO] To register manually: mlops/model_registry.py register --path {args.output_dir}")

    print("\n" + "=" * 60)
    print(f"  TRAINING COMPLETE: Revenue R²={rev_r2:.4f}")
    print(f"  Artifacts: {args.output_dir}")
    print(f"  End: {datetime.utcnow().isoformat()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
