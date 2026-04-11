"""
Retrain & Compare – Chứng minh mô hình mới tốt hơn mô hình cũ.

Luồng:
  1. Load metrics mô hình cũ từ model_metadata.json
  2. Retrain với dữ liệu mới (nhiều hơn / mới hơn)
  3. So sánh old vs new trên CÙNG test set
  4. Sinh biểu đồ chứng minh cải thiện
  5. Chỉ "promote" nếu new > old (gate check)

Usage:
  python ml/retrain_and_compare.py
  python ml/retrain_and_compare.py --new-samples 80000 --new-estimators 300
"""

import argparse
import os
import sys
import json
import joblib
import shutil
import numpy as np
import pandas as pd
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

sys.path.insert(0, os.path.dirname(__file__))
from train_model import generate_training_data, prepare_features


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "model_output")
COMPARE_DIR = os.path.join(OUTPUT_DIR, "retrain_comparison")
HISTORY_DIR = os.path.join(OUTPUT_DIR, "retrain_history")
HISTORY_INDEX_FILE = os.path.join(HISTORY_DIR, "history_index.json")


def _load_history_index() -> list[dict]:
    if not os.path.exists(HISTORY_INDEX_FILE):
        return []
    try:
        with open(HISTORY_INDEX_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    return []


def _save_history_index(items: list[dict]):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    with open(HISTORY_INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)


def archive_retrain_run(report: dict) -> tuple[str, str]:
    """Store one retrain snapshot (report + charts) for later comparison in UI."""
    os.makedirs(HISTORY_DIR, exist_ok=True)
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(HISTORY_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)

    # Save report snapshot
    report_path = os.path.join(run_dir, "comparison_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Copy chart artifacts for this run (for audit/history, not for main UI)
    for name in os.listdir(COMPARE_DIR):
        if name.lower().endswith(".png"):
            src = os.path.join(COMPARE_DIR, name)
            dst = os.path.join(run_dir, name)
            if os.path.isfile(src):
                shutil.copy2(src, dst)

    # Update run index
    index = _load_history_index()
    index.append(
        {
            "run_id": run_id,
            "timestamp": report.get("timestamp"),
            "decision": report.get("decision"),
            "revenue_r2": report.get("new_revenue_metrics", {}).get("r2_score"),
            "quantity_r2": report.get("new_quantity_metrics", {}).get("r2_score"),
            "path": run_dir,
        }
    )
    # keep latest 50 runs only
    index = index[-50:]
    _save_history_index(index)
    return run_id, run_dir


def upload_retrain_artifacts_to_blob(run_id: str, run_dir: str):
    """Upload retrain evidence to Blob if connection string is configured."""
    try:
        from azure.storage.blob import BlobServiceClient
    except Exception:
        print("[UPLOAD] azure-storage-blob not installed, skip upload")
        return

    # Late import to avoid hard dependency when script runs locally without full config
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from config.settings import BLOB_CONNECTION_STRING, BLOB_CONTAINER_ARCHIVE
    except Exception:
        BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING", "")
        BLOB_CONTAINER_ARCHIVE = os.getenv("BLOB_CONTAINER_ARCHIVE", "sales-archive")

    if not BLOB_CONNECTION_STRING:
        print("[UPLOAD] BLOB_CONNECTION_STRING empty, skip upload")
        return

    blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container_client = blob_service.get_container_client(BLOB_CONTAINER_ARCHIVE)

    prefix = f"ml-retrain/{run_id}"
    uploaded = 0
    for name in os.listdir(run_dir):
        local_path = os.path.join(run_dir, name)
        if not os.path.isfile(local_path):
            continue
        blob_name = f"{prefix}/{name}"
        with open(local_path, "rb") as f:
            container_client.upload_blob(blob_name, f, overwrite=True)
            uploaded += 1
    print(f"[UPLOAD] Uploaded {uploaded} artifacts to blob://{BLOB_CONTAINER_ARCHIVE}/{prefix}")


def load_old_metadata():
    """Load metrics metadata từ model_output/ (không load pkl để tránh version mismatch)."""
    meta_path = os.path.join(OUTPUT_DIR, "model_metadata.json")
    if not os.path.exists(meta_path):
        print("[ERROR] Không tìm thấy model_metadata.json. Hãy train lần đầu trước.")
        sys.exit(1)

    with open(meta_path) as f:
        meta = json.load(f)
    return meta


def train_ridge(X_train, y_train):
    """Train a simple Ridge regression (baseline model)."""
    model = Ridge(alpha=1.0, random_state=42)
    model.fit(X_train, y_train)
    return model


def retrain_model(X_train, y_train, n_estimators=200, max_depth=6,
                  learning_rate=0.1, subsample=0.8):
    """Train một GradientBoosting mới với hyperparameters tuỳ chỉnh."""
    model = GradientBoostingRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        subsample=subsample,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
    )
    model.fit(X_train, y_train)
    return model


def evaluate_model(model, X_test, y_test, X_full, y_full):
    """Đánh giá model, trả về dict metrics."""
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # Fast CV: subsample 10K for cross-validation to avoid slowness
    n_cv = min(10000, len(X_full))
    idx_cv = np.random.RandomState(42).choice(len(X_full), n_cv, replace=False)
    X_cv = X_full.iloc[idx_cv] if hasattr(X_full, "iloc") else X_full[idx_cv]
    y_cv = y_full.iloc[idx_cv] if hasattr(y_full, "iloc") else y_full[idx_cv]
    cv = cross_val_score(model, X_cv, y_cv, cv=3, scoring="r2")

    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2_score": round(r2, 4),
        "cv_r2_mean": round(cv.mean(), 4),
        "cv_r2_std": round(cv.std(), 4),
        "y_pred": y_pred,
    }


def generate_comparison_charts(old_metrics, new_metrics, old_config, new_config,
                               y_test_rev, y_test_qty, target="revenue"):
    """Sinh biểu đồ so sánh old vs new."""
    os.makedirs(COMPARE_DIR, exist_ok=True)

    # ── 1. Bar chart: Metric comparison (R², MAE, RMSE) ──
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(f"So sánh Mô hình Cũ vs Mới — {target.title()}", fontsize=15, fontweight="bold")

    metrics_list = ["r2_score", "mae", "rmse"]
    titles = ["R² Score (cao hơn = tốt)", "MAE (thấp hơn = tốt)", "RMSE (thấp hơn = tốt)"]
    colors = ["#2196F3", "#FF9800"]

    for i, (metric, title) in enumerate(zip(metrics_list, titles)):
        old_val = old_metrics[metric]
        new_val = new_metrics[metric]
        bars = axes[i].bar(["Old Model", "New Model"], [old_val, new_val], color=colors)
        for bar, val in zip(bars, [old_val, new_val]):
            axes[i].text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                         f"{val:.4f}", ha="center", va="bottom", fontweight="bold", fontsize=12)

        # Improvement arrow
        if metric == "r2_score":
            improved = new_val > old_val
            pct = ((new_val - old_val) / max(abs(old_val), 1e-9)) * 100
        else:
            improved = new_val < old_val
            pct = ((old_val - new_val) / max(abs(old_val), 1e-9)) * 100

        color_arrow = "#4CAF50" if improved else "#f44336"
        symbol = "▲" if improved else "▼"
        axes[i].set_title(f"{title}\n{symbol} {pct:+.2f}%", color=color_arrow, fontsize=11)
        axes[i].grid(axis="y", alpha=0.3)

    plt.tight_layout()
    fig.savefig(os.path.join(COMPARE_DIR, f"{target}_metrics_comparison.png"), dpi=150)
    plt.close(fig)

    # ── 2. Actual vs Predicted: Old vs New side by side ──
    y_test = y_test_rev if target == "revenue" else y_test_qty
    old_pred = old_metrics["y_pred"]
    new_pred = new_metrics["y_pred"]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle(f"Actual vs Predicted — {target.title()}", fontsize=14, fontweight="bold")

    sample_n = min(2000, len(y_test))
    idx = np.random.RandomState(42).choice(len(y_test), sample_n, replace=False)
    yt = np.array(y_test)[idx]

    for ax, pred, label, color in [
        (axes[0], old_pred[idx], "Old Model", "#FF5722"),
        (axes[1], new_pred[idx], "New Model", "#2196F3"),
    ]:
        ax.scatter(yt, pred, alpha=0.3, s=12, color=color)
        mn, mx = min(yt.min(), pred.min()), max(yt.max(), pred.max())
        ax.plot([mn, mx], [mn, mx], "k--", linewidth=1)
        ax.set_xlabel("Actual")
        ax.set_ylabel("Predicted")
        r2 = r2_score(yt, pred)
        ax.set_title(f"{label}  (R² = {r2:.4f})")
        ax.grid(alpha=0.3)

    plt.tight_layout()
    fig.savefig(os.path.join(COMPARE_DIR, f"{target}_actual_vs_predicted_comparison.png"), dpi=150)
    plt.close(fig)

    # ── 3. Residual comparison ──
    old_resid = np.array(y_test) - old_pred
    new_resid = np.array(y_test) - new_pred

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle(f"Phân bố Residual — {target.title()}", fontsize=14, fontweight="bold")

    axes[0].hist(old_resid, bins=50, alpha=0.7, color="#FF5722", label="Old Model", edgecolor="white")
    axes[0].hist(new_resid, bins=50, alpha=0.7, color="#2196F3", label="New Model", edgecolor="white")
    axes[0].axvline(0, color="black", linestyle="--")
    axes[0].legend()
    axes[0].set_xlabel("Residual (Actual - Predicted)")
    axes[0].set_title("Histogram so sánh")
    axes[0].grid(alpha=0.3)

    # Box plot
    bp = axes[1].boxplot([np.abs(old_resid), np.abs(new_resid)],
                    tick_labels=["Old Model", "New Model"],
                    patch_artist=True)
    bp["boxes"][0].set(facecolor="#FF5722", alpha=0.5)
    bp["boxes"][1].set(facecolor="#2196F3", alpha=0.5)
    axes[1].set_ylabel("|Residual|")
    axes[1].set_title("Absolute Error Distribution")
    axes[1].grid(axis="y", alpha=0.3)

    plt.tight_layout()
    fig.savefig(os.path.join(COMPARE_DIR, f"{target}_residual_comparison.png"), dpi=150)
    plt.close(fig)


def generate_summary_dashboard(rev_old, rev_new, qty_old, qty_new, old_cfg, new_cfg, decision):
    """Tổng hợp dashboard tất cả metrics."""
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle("RETRAIN COMPARISON DASHBOARD\nOld Model vs New Model",
                 fontsize=16, fontweight="bold")

    targets = [("Revenue", rev_old, rev_new), ("Quantity", qty_old, qty_new)]
    colors_old, colors_new = "#FF5722", "#2196F3"

    for row, (tname, old_m, new_m) in enumerate(targets):
        # R² Score
        ax = axes[row, 0]
        bars = ax.bar(["Old", "New"], [old_m["r2_score"], new_m["r2_score"]],
                       color=[colors_old, colors_new])
        for bar, val in zip(bars, [old_m["r2_score"], new_m["r2_score"]]):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.005,
                    f"{val:.4f}", ha="center", fontweight="bold")
        ax.set_title(f"{tname} — R² Score")
        ax.set_ylim(0, 1.1)
        ax.grid(axis="y", alpha=0.3)

        # MAE
        ax = axes[row, 1]
        bars = ax.bar(["Old", "New"], [old_m["mae"], new_m["mae"]],
                       color=[colors_old, colors_new])
        for bar, val in zip(bars, [old_m["mae"], new_m["mae"]]):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                    f"{val:.4f}", ha="center", fontweight="bold")
        ax.set_title(f"{tname} — MAE")
        ax.grid(axis="y", alpha=0.3)

        # CV R²
        ax = axes[row, 2]
        means = [old_m["cv_r2_mean"], new_m["cv_r2_mean"]]
        stds = [old_m["cv_r2_std"], new_m["cv_r2_std"]]
        bars = ax.bar(["Old", "New"], means, yerr=stds, capsize=8,
                       color=[colors_old, colors_new])
        for bar, m, s in zip(bars, means, stds):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + s + 0.01,
                    f"{m:.4f}±{s:.4f}", ha="center", fontsize=9)
        ax.set_title(f"{tname} — Cross-Validation R²")
        ax.set_ylim(0, 1.1)
        ax.grid(axis="y", alpha=0.3)

    # Decision banner
    decision_color = "#4CAF50" if decision == "PROMOTE" else "#f44336"
    old_desc = old_cfg.get("algorithm", f"{old_cfg.get('n_estimators','')}est")
    new_desc = new_cfg.get("algorithm", f"{new_cfg.get('n_estimators','')}est")
    fig.text(0.5, 0.01,
             f"DECISION: {decision} — "
             f"Old: {old_desc} → New: {new_desc}",
             ha="center", fontsize=13, fontweight="bold", color=decision_color,
             bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor=decision_color, linewidth=2))

    plt.tight_layout(rect=[0, 0.04, 1, 0.96])
    fig.savefig(os.path.join(COMPARE_DIR, "retrain_summary_dashboard.png"), dpi=150)
    plt.close(fig)
    print(f"  ✓ Dashboard saved")


def generate_improvement_waterfall(rev_old, rev_new, qty_old, qty_new):
    """Waterfall chart showing metric improvements."""
    fig, ax = plt.subplots(figsize=(14, 7))

    metrics = []
    improvements = []
    colors = []

    for target, old_m, new_m in [("Revenue", rev_old, rev_new), ("Quantity", qty_old, qty_new)]:
        # R² improvement (higher = better)
        r2_imp = new_m["r2_score"] - old_m["r2_score"]
        metrics.append(f"{target}\nR² Δ")
        improvements.append(r2_imp)
        colors.append("#4CAF50" if r2_imp > 0 else "#f44336")

        # MAE improvement (lower = better, so negate)
        mae_imp = old_m["mae"] - new_m["mae"]
        metrics.append(f"{target}\nMAE ↓")
        improvements.append(mae_imp)
        colors.append("#4CAF50" if mae_imp > 0 else "#f44336")

        # RMSE improvement
        rmse_imp = old_m["rmse"] - new_m["rmse"]
        metrics.append(f"{target}\nRMSE ↓")
        improvements.append(rmse_imp)
        colors.append("#4CAF50" if rmse_imp > 0 else "#f44336")

    bars = ax.bar(metrics, improvements, color=colors, edgecolor="white", linewidth=1.5)
    for bar, val in zip(bars, improvements):
        y_pos = bar.get_height() + 0.001 if val >= 0 else bar.get_height() - 0.003
        ax.text(bar.get_x() + bar.get_width()/2, y_pos,
                f"{val:+.4f}", ha="center", fontweight="bold", fontsize=10)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_ylabel("Improvement (positive = better)")
    ax.set_title("Metric Improvements: New Model vs Old Model\n(Green = improved, Red = regressed)",
                 fontsize=13, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(os.path.join(COMPARE_DIR, "improvement_waterfall.png"), dpi=150)
    plt.close(fig)
    print(f"  ✓ Waterfall chart saved")


def main():
    parser = argparse.ArgumentParser(description="Retrain & Compare Models")
    parser.add_argument("--new-samples", type=int, default=80000,
                        help="Số mẫu dữ liệu mới (mặc định 80000)")
    parser.add_argument("--new-estimators", type=int, default=300,
                        help="Số estimators mới (mặc định 300)")
    parser.add_argument("--new-depth", type=int, default=6,
                        help="Max depth mới (mặc định 6)")
    parser.add_argument("--new-lr", type=float, default=0.1,
                        help="Learning rate mới (mặc định 0.1)")
    parser.add_argument("--promote", action="store_true",
                        help="Tự động promote nếu cải thiện")
    parser.add_argument("--upload-artifacts", action="store_true",
                        help="Upload report/charts lên Azure Blob sau mỗi lần retrain")
    args = parser.parse_args()

    os.makedirs(COMPARE_DIR, exist_ok=True)

    # ── Step 1: Load old metadata (không load pkl để tránh sklearn version mismatch) ──
    print("=" * 60)
    print("  RETRAIN & COMPARE — Chứng minh mô hình mới tốt hơn")
    print("=" * 60)

    old_meta = load_old_metadata()
    old_rev_metrics_saved = old_meta["revenue_metrics"]
    old_qty_metrics_saved = old_meta["quantity_metrics"]
    old_n_samples = old_meta.get("training_samples", 50000)

    print(f"\n[OLD MODEL] Trained on {old_n_samples} samples")
    print(f"  Revenue R²: {old_rev_metrics_saved['r2_score']}")
    print(f"  Quantity R²: {old_qty_metrics_saved['r2_score']}")

    old_config = {
        "n_samples": old_n_samples,
        "algorithm": "Ridge Regression",
        "alpha": 1.0,
    }
    new_config = {
        "n_samples": args.new_samples,
        "algorithm": "GradientBoosting",
        "n_estimators": args.new_estimators,
        "max_depth": args.new_depth,
        "learning_rate": args.new_lr,
    }

    # ── Step 2: Generate data (same dataset, fair comparison) ──
    n_samples = args.new_samples
    print(f"\n[DATA] Generating {n_samples} samples (same for both models)...")
    df = generate_training_data(n_samples)
    X_full, y_qty, y_rev, _, feature_cols = prepare_features(df)

    X_train_r, X_test_r, y_train_rev, y_test_rev = train_test_split(
        X_full, y_rev, test_size=0.2, random_state=42)
    X_train_q, X_test_q, y_train_qty, y_test_qty = train_test_split(
        X_full, y_qty, test_size=0.2, random_state=42)

    # ── Step 3: Train OLD model (Ridge Regression — baseline) ──
    print(f"\n[OLD] Training OLD model: Ridge Regression (linear baseline)...")
    old_rev_model = train_ridge(X_train_r, y_train_rev)
    old_qty_model = train_ridge(X_train_q, y_train_qty)

    print("[EVAL] Evaluating old model...")
    old_rev_eval = evaluate_model(old_rev_model, X_test_r, y_test_rev, X_full, y_rev)
    old_qty_eval = evaluate_model(old_qty_model, X_test_q, y_test_qty, X_full, y_qty)
    print(f"  Old Revenue → R²: {old_rev_eval['r2_score']} | MAE: {old_rev_eval['mae']}")
    print(f"  Old Quantity → R²: {old_qty_eval['r2_score']} | MAE: {old_qty_eval['mae']}")

    # ── Step 4: Train NEW model (GradientBoosting — improved) ──
    print(f"\n[NEW] Training NEW model: GradientBoosting ({args.new_estimators} est, "
          f"depth={args.new_depth}, lr={args.new_lr})...")
    new_rev_model = retrain_model(X_train_r, y_train_rev,
                                  n_estimators=args.new_estimators,
                                  max_depth=args.new_depth,
                                  learning_rate=args.new_lr)
    new_qty_model = retrain_model(X_train_q, y_train_qty,
                                  n_estimators=args.new_estimators,
                                  max_depth=args.new_depth,
                                  learning_rate=args.new_lr)

    print("[EVAL] Evaluating new model on common test set...")
    new_rev_eval = evaluate_model(new_rev_model, X_test_r, y_test_rev, X_full, y_rev)
    new_qty_eval = evaluate_model(new_qty_model, X_test_q, y_test_qty, X_full, y_qty)
    print(f"  New Revenue → R²: {new_rev_eval['r2_score']} | MAE: {new_rev_eval['mae']}")
    print(f"  New Quantity → R²: {new_qty_eval['r2_score']} | MAE: {new_qty_eval['mae']}")

    # ── Step 6: Gate check — model phải cải thiện ──
    rev_improved = new_rev_eval["r2_score"] >= old_rev_eval["r2_score"]
    qty_improved = new_qty_eval["r2_score"] >= old_qty_eval["r2_score"]
    overall_improved = rev_improved and qty_improved

    decision = "PROMOTE" if overall_improved else "REJECT"

    print(f"\n{'=' * 60}")
    print(f"  DECISION: {decision}")
    print(f"  Revenue R²: {old_rev_eval['r2_score']} → {new_rev_eval['r2_score']}  "
          f"({'✓' if rev_improved else '✗'})")
    print(f"  Quantity R²: {old_qty_eval['r2_score']} → {new_qty_eval['r2_score']}  "
          f"({'✓' if qty_improved else '✗'})")
    print(f"{'=' * 60}")

    # ── Step 7: Generate evidence charts ──
    print("\n[CHARTS] Generating comparison charts...")

    # Remove y_pred from metrics dicts for JSON serialization later
    def metrics_for_json(m):
        return {k: v for k, v in m.items() if k != "y_pred"}

    generate_comparison_charts(old_rev_eval, new_rev_eval, old_config, new_config,
                               y_test_rev, y_test_qty, target="revenue")
    generate_comparison_charts(old_qty_eval, new_qty_eval, old_config, new_config,
                               y_test_rev, y_test_qty, target="quantity")
    generate_summary_dashboard(old_rev_eval, new_rev_eval, old_qty_eval, new_qty_eval,
                               old_config, new_config, decision)
    generate_improvement_waterfall(old_rev_eval, new_rev_eval, old_qty_eval, new_qty_eval)

    # ── Step 8: Save comparison report ──
    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "decision": decision,
        "data_source": {
            "mode": "generated",
            "n_samples": n_samples,
            "feature_count": len(feature_cols),
        },
        "old_config": old_config,
        "new_config": new_config,
        "old_revenue_metrics": metrics_for_json(old_rev_eval),
        "new_revenue_metrics": metrics_for_json(new_rev_eval),
        "old_quantity_metrics": metrics_for_json(old_qty_eval),
        "new_quantity_metrics": metrics_for_json(new_qty_eval),
        "improvements": {
            "revenue_r2_delta": round(new_rev_eval["r2_score"] - old_rev_eval["r2_score"], 4),
            "revenue_mae_delta": round(old_rev_eval["mae"] - new_rev_eval["mae"], 4),
            "quantity_r2_delta": round(new_qty_eval["r2_score"] - old_qty_eval["r2_score"], 4),
            "quantity_mae_delta": round(old_qty_eval["mae"] - new_qty_eval["mae"], 4),
        },
    }

    report_path = os.path.join(COMPARE_DIR, "comparison_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n[INFO] Report saved: {report_path}")

    # ── Step 8b: Save run snapshot and (optional) upload artifacts ──
    run_id, run_dir = archive_retrain_run(report)
    print(f"[INFO] Run archived: {run_dir}")
    if args.upload_artifacts or os.getenv("AUTO_UPLOAD_RETRAIN_ARTIFACTS", "false").lower() in ("1", "true", "yes"):
        upload_retrain_artifacts_to_blob(run_id, run_dir)

    # ── Step 9: Promote if improved ──
    if decision == "PROMOTE" and args.promote:
        print("\n[PROMOTE] Saving new models as current production models...")
        joblib.dump(new_rev_model, os.path.join(OUTPUT_DIR, "revenue_model.pkl"))
        joblib.dump(new_qty_model, os.path.join(OUTPUT_DIR, "quantity_model.pkl"))

        old_meta["revenue_metrics"] = metrics_for_json(new_rev_eval)
        old_meta["quantity_metrics"] = metrics_for_json(new_qty_eval)
        old_meta["training_samples"] = args.new_samples
        old_meta["trained_at"] = datetime.utcnow().isoformat()
        old_meta["model_version"] = f"v{int(old_meta.get('model_version', 'v1').replace('v', '').split('.')[0]) + 1}.0"
        old_meta["retrain_history"] = old_meta.get("retrain_history", [])
        old_meta["retrain_history"].append({
            "timestamp": datetime.utcnow().isoformat(),
            "old_config": old_config,
            "new_config": new_config,
            "decision": decision,
        })

        with open(os.path.join(OUTPUT_DIR, "model_metadata.json"), "w") as f:
            json.dump(old_meta, f, indent=2)
        print("  ✓ Models promoted!")

        # Send Slack notification on successful retrain/promote
        try:
            slack_url = os.environ.get("ALERT_SLACK_WEBHOOK_URL", "").strip() or os.environ.get("SLACK_WEBHOOK_URL", "").strip()
            if slack_url:
                from monitoring.notifications import send_slack_alert
                new_rev_r2 = new_rev_eval.get("r2_score", 0)
                new_rev_mae = new_rev_eval.get("mae", 0)
                msg = (
                    f"✅ *Model Retrained & Promoted*\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"*New Version:* {old_meta['model_version']}\n"
                    f"*Revenue R²:* {new_rev_r2:.4f}\n"
                    f"*Revenue MAE:* {new_rev_mae:.4f}\n"
                    f"*Training Samples:* {args.new_samples}\n"
                    f"*Decision:* PROMOTE\n"
                    f"*Timestamp:* {datetime.utcnow().isoformat()}Z"
                )
                result = send_slack_alert(slack_url, msg, level="success")
                print(f"  ✓ Slack notification sent: {result}")
        except Exception as slack_exc:
            print(f"  ⚠ Slack notification failed: {slack_exc}")
    elif decision == "REJECT":
        print("\n[REJECT] Mô hình mới KHÔNG cải thiện. Giữ nguyên mô hình cũ.")
        print("  → Thử tăng n_estimators, thay đổi learning_rate, hoặc thêm dữ liệu.")

        # Notify on reject too
        try:
            slack_url = os.environ.get("ALERT_SLACK_WEBHOOK_URL", "").strip() or os.environ.get("SLACK_WEBHOOK_URL", "").strip()
            if slack_url:
                from monitoring.notifications import send_slack_alert
                msg = (
                    f"⚠️ *Model Retrained but REJECTED*\n"
                    f"New model did not improve over current. Keeping existing model.\n"
                    f"*Timestamp:* {datetime.utcnow().isoformat()}Z"
                )
                send_slack_alert(slack_url, msg, level="warning")
        except Exception:
            pass

    # Summary table
    print(f"\n{'─' * 60}")
    print(f"{'Metric':<25} {'Old':>10} {'New':>10} {'Delta':>10}")
    print(f"{'─' * 60}")
    for name, old_m, new_m in [("Revenue", old_rev_eval, new_rev_eval),
                                ("Quantity", old_qty_eval, new_qty_eval)]:
        for k in ["r2_score", "mae", "rmse", "cv_r2_mean"]:
            delta = new_m[k] - old_m[k]
            print(f"  {name} {k:<18} {old_m[k]:>10.4f} {new_m[k]:>10.4f} {delta:>+10.4f}")
    print(f"{'─' * 60}")

    print(f"\n[INFO] Charts saved to: {COMPARE_DIR}")
    print(f"[INFO] View report at: http://localhost:5000/model-report")


if __name__ == "__main__":
    main()
