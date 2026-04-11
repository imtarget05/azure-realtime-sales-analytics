"""
Train demand-forecasting model (Gradient Boosting).
Features are aligned with the data generators' schema:
  store_id, product_id, hour, day_of_month, month, is_weekend,
  temperature, is_rainy, holiday
"""

import argparse
import os
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score, learning_curve
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import sklearn

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    from azureml.core import Run
    AZUREML_AVAILABLE = True
except ImportError:
    AZUREML_AVAILABLE = False
    print("[WARN] Azure ML SDK not available. Running locally.")

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Load training data from Azure SQL ─────────────────────────────
def load_sql_training_data(min_samples: int = 1000) -> pd.DataFrame | None:
    """
    Load real sales transactions from Azure SQL and transform to training format.
    Returns None if SQL is unavailable or insufficient data.
    """
    try:
        from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
    except ImportError:
        # Running inside AML compute — read from environment variables injected by ADF / job spec
        SQL_SERVER   = os.environ.get("SQL_SERVER", "")
        SQL_DATABASE = os.environ.get("SQL_DATABASE", "SalesAnalyticsDB")
        SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
        SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")
        SQL_DRIVER   = os.environ.get("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")

    if not SQL_SERVER or not SQL_USERNAME or not SQL_PASSWORD:
        print("[INFO] SQL credentials not configured, skipping SQL data load")
        return None

    try:
        import pyodbc
    except ImportError:
        print("[WARN] pyodbc not installed, skipping SQL data load")
        return None

    try:
        conn_str = (
            f"DRIVER={SQL_DRIVER};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};"
            f"PWD={SQL_PASSWORD};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
        )
        conn = pyodbc.connect(conn_str)
        query = """
            SELECT
                DATEPART(HOUR, event_time) AS hour,
                DATEPART(DAY, event_time)  AS day_of_month,
                DATEPART(MONTH, event_time) AS month,
                CASE WHEN DATEPART(WEEKDAY, event_time) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
                store_id,
                product_id,
                category,
                ISNULL(temperature, 25.0) AS temperature,
                CASE WHEN ISNULL(weather, 'Clear') IN ('Rain', 'Rainy', 'Storm') THEN 1 ELSE 0 END AS is_rainy,
                ISNULL(holiday, 0) AS holiday,
                units_sold AS quantity,
                revenue
            FROM dbo.SalesTransactions
            WHERE revenue IS NOT NULL AND units_sold IS NOT NULL
            ORDER BY event_time DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()

        if len(df) < min_samples:
            print(f"[INFO] SQL has only {len(df)} rows (need {min_samples}), skipping")
            return None

        print(f"[INFO] Loaded {len(df)} training samples from Azure SQL")
        return df

    except Exception as e:
        print(f"[WARN] Failed to load SQL training data: {e}")
        return None


# ── Synthetic data that mirrors the generator schema ──────────────
def generate_training_data(n_samples: int = 50000) -> pd.DataFrame:
    np.random.seed(42)

    stores = ["S01", "S02", "S03"]
    products = [
        {"id": "COKE",  "category": "Beverage", "min_p": 1.2, "max_p": 1.8},
        {"id": "PEPSI", "category": "Beverage", "min_p": 1.1, "max_p": 1.7},
        {"id": "BREAD", "category": "Bakery",   "min_p": 0.8, "max_p": 1.5},
        {"id": "MILK",  "category": "Dairy",    "min_p": 1.0, "max_p": 2.2},
    ]

    start_date = datetime(2025, 1, 1)
    dates = [start_date + timedelta(hours=i) for i in range(n_samples)]

    rows = []
    for dt in dates:
        store = np.random.choice(stores)
        prod = products[np.random.randint(len(products))]
        hour = dt.hour
        month = dt.month
        day_of_month = dt.day
        is_weekend = 1 if dt.weekday() >= 5 else 0

        temperature = 25 + 10 * np.sin(2 * np.pi * (month - 1) / 12) + np.random.normal(0, 3)
        is_rainy = int(np.random.random() < 0.3)
        holiday = int(np.random.random() < 0.05)

        # Revenue drivers
        weekend_f = 1.3 if is_weekend else 1.0
        hour_f = 1.5 if (10 <= hour <= 14 or 18 <= hour <= 21) else (1.0 if 6 <= hour <= 17 else 0.3)
        rain_f = 0.8 if is_rainy else 1.0
        holiday_f = 1.4 if holiday else 1.0
        cat_f = {"Beverage": 1.0, "Bakery": 0.9, "Dairy": 0.85}.get(prod["category"], 1.0)

        base_revenue = 50
        revenue = max(0, base_revenue * weekend_f * hour_f * rain_f * holiday_f * cat_f
                       + np.random.normal(0, 10))
        quantity = max(1, int(revenue / np.random.uniform(prod["min_p"], prod["max_p"])))

        rows.append({
            "hour": hour,
            "day_of_month": day_of_month,
            "month": month,
            "is_weekend": is_weekend,
            "store_id": store,
            "product_id": prod["id"],
            "category": prod["category"],
            "temperature": round(temperature, 1),
            "is_rainy": is_rainy,
            "holiday": holiday,
            "quantity": quantity,
            "revenue": round(revenue, 2),
        })

    return pd.DataFrame(rows)


# ── Feature preparation ──────────────────────────────────────────
def prepare_features(df: pd.DataFrame) -> tuple:
    label_encoders = {}
    df_enc = df.copy()

    for col in ["store_id", "product_id", "category"]:
        le = LabelEncoder()
        df_enc[col + "_enc"] = le.fit_transform(df_enc[col])
        label_encoders[col] = le

    df_enc["hour_sin"] = np.sin(2 * np.pi * df_enc["hour"] / 24)
    df_enc["hour_cos"] = np.cos(2 * np.pi * df_enc["hour"] / 24)
    df_enc["month_sin"] = np.sin(2 * np.pi * df_enc["month"] / 12)
    df_enc["month_cos"] = np.cos(2 * np.pi * df_enc["month"] / 12)

    feature_cols = [
        "hour", "day_of_month", "month", "is_weekend",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
        "store_id_enc", "product_id_enc", "category_enc",
        "temperature", "is_rainy", "holiday",
    ]

    X = df_enc[feature_cols]
    return X, df_enc["quantity"], df_enc["revenue"], label_encoders, feature_cols


# ── Training ─────────────────────────────────────────────────────
def train_one_model(X, y, name: str = "revenue"):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = GradientBoostingRegressor(
        n_estimators=200, max_depth=6, learning_rate=0.1,
        subsample=0.8, min_samples_split=10, min_samples_leaf=5,
        random_state=42,
    )

    print(f"\n[INFO] Training model: {name}")
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    cv = cross_val_score(model, X, y, cv=5, scoring="r2")

    metrics = {
        "model_name": name,
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2_score": round(r2, 4),
        "cv_r2_mean": round(cv.mean(), 4),
        "cv_r2_std": round(cv.std(), 4),
    }

    print(f"  MAE:  {mae:.4f}  |  RMSE: {rmse:.4f}  |  R2: {r2:.4f}  |  CV R2: {cv.mean():.4f}")

    top5 = sorted(zip(X.columns, model.feature_importances_), key=lambda x: x[1], reverse=True)[:5]
    for feat, imp in top5:
        print(f"    {feat}: {imp:.4f}")

    return model, metrics, X_train, X_test, y_train, y_test, y_pred


# ── Chart generation ─────────────────────────────────────────────
def generate_charts(model, X_train, X_test, y_train, y_test, y_pred, name, feature_cols, chart_dir):
    """Generate evaluation charts for a trained model."""
    os.makedirs(chart_dir, exist_ok=True)
    print(f"[INFO] Generating charts for {name} -> {chart_dir}")

    # 1. Feature importance
    fig, ax = plt.subplots(figsize=(10, 6))
    importances = model.feature_importances_
    idx_sorted = np.argsort(importances)
    ax.barh(range(len(idx_sorted)), importances[idx_sorted], color="#2196F3")
    ax.set_yticks(range(len(idx_sorted)))
    ax.set_yticklabels([feature_cols[i] for i in idx_sorted])
    ax.set_xlabel("Feature Importance")
    ax.set_title(f"Feature Importance — {name.title()} Model")
    plt.tight_layout()
    fig.savefig(os.path.join(chart_dir, f"{name}_feature_importance.png"), dpi=150)
    plt.close(fig)

    # 2. Actual vs Predicted scatter
    fig, ax = plt.subplots(figsize=(8, 8))
    sample_idx = np.random.RandomState(42).choice(len(y_test), min(2000, len(y_test)), replace=False)
    y_t = np.array(y_test)[sample_idx]
    y_p = y_pred[sample_idx]
    ax.scatter(y_t, y_p, alpha=0.3, s=10, color="#FF5722")
    mn, mx = min(y_t.min(), y_p.min()), max(y_t.max(), y_p.max())
    ax.plot([mn, mx], [mn, mx], "k--", linewidth=1, label="Perfect prediction")
    ax.set_xlabel("Actual")
    ax.set_ylabel("Predicted")
    ax.set_title(f"Actual vs Predicted — {name.title()} Model")
    ax.legend()
    plt.tight_layout()
    fig.savefig(os.path.join(chart_dir, f"{name}_actual_vs_predicted.png"), dpi=150)
    plt.close(fig)

    # 3. Residual distribution
    residuals = np.array(y_test) - y_pred
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].hist(residuals, bins=50, color="#4CAF50", edgecolor="white", alpha=0.8)
    axes[0].axvline(0, color="red", linestyle="--")
    axes[0].set_xlabel("Residual (Actual - Predicted)")
    axes[0].set_ylabel("Frequency")
    axes[0].set_title(f"Residual Distribution — {name.title()}")

    axes[1].scatter(y_pred, residuals, alpha=0.2, s=8, color="#9C27B0")
    axes[1].axhline(0, color="red", linestyle="--")
    axes[1].set_xlabel("Predicted")
    axes[1].set_ylabel("Residual")
    axes[1].set_title(f"Residuals vs Predicted — {name.title()}")
    plt.tight_layout()
    fig.savefig(os.path.join(chart_dir, f"{name}_residuals.png"), dpi=150)
    plt.close(fig)

    # 4. Learning curve
    fig, ax = plt.subplots(figsize=(10, 6))
    train_sizes, train_scores, val_scores = learning_curve(
        GradientBoostingRegressor(
            n_estimators=100, max_depth=6, learning_rate=0.1,
            subsample=0.8, random_state=42,
        ),
        X_train, y_train,
        cv=3, scoring="r2",
        train_sizes=np.linspace(0.1, 1.0, 8),
        n_jobs=-1, random_state=42,
    )
    ax.plot(train_sizes, train_scores.mean(axis=1), "o-", color="#2196F3", label="Train R²")
    ax.plot(train_sizes, val_scores.mean(axis=1), "o-", color="#FF9800", label="Validation R²")
    ax.fill_between(train_sizes,
                    train_scores.mean(axis=1) - train_scores.std(axis=1),
                    train_scores.mean(axis=1) + train_scores.std(axis=1),
                    alpha=0.1, color="#2196F3")
    ax.fill_between(train_sizes,
                    val_scores.mean(axis=1) - val_scores.std(axis=1),
                    val_scores.mean(axis=1) + val_scores.std(axis=1),
                    alpha=0.1, color="#FF9800")
    ax.set_xlabel("Training Samples")
    ax.set_ylabel("R² Score")
    ax.set_title(f"Learning Curve — {name.title()} Model")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    fig.savefig(os.path.join(chart_dir, f"{name}_learning_curve.png"), dpi=150)
    plt.close(fig)

    # 5. Prediction error by hour (if hour feature exists)
    if "hour" in feature_cols:
        fig, ax = plt.subplots(figsize=(10, 5))
        hour_idx = feature_cols.index("hour")
        hours = X_test.iloc[:, hour_idx] if hasattr(X_test, "iloc") else X_test[:, hour_idx]
        err_df = pd.DataFrame({"hour": hours, "abs_error": np.abs(residuals)})
        hourly = err_df.groupby("hour")["abs_error"].mean()
        ax.bar(hourly.index, hourly.values, color="#00BCD4", edgecolor="white")
        ax.set_xlabel("Hour of Day")
        ax.set_ylabel("Mean Absolute Error")
        ax.set_title(f"MAE by Hour — {name.title()} Model")
        ax.grid(axis="y", alpha=0.3)
        plt.tight_layout()
        fig.savefig(os.path.join(chart_dir, f"{name}_error_by_hour.png"), dpi=150)
        plt.close(fig)

    print(f"  ✓ Charts saved to {chart_dir}")


def _generate_summary_chart(rev_metrics, qty_metrics, chart_dir):
    """Side-by-side comparison of revenue & quantity models."""
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    fig.suptitle("Model Evaluation Summary", fontsize=14, fontweight="bold")

    labels = ["Revenue", "Quantity"]
    colors = ["#2196F3", "#FF9800"]

    # R² score
    r2_vals = [rev_metrics["r2_score"], qty_metrics["r2_score"]]
    axes[0].bar(labels, r2_vals, color=colors)
    for i, v in enumerate(r2_vals):
        axes[0].text(i, v + 0.01, f"{v:.4f}", ha="center", fontweight="bold")
    axes[0].set_ylabel("R² Score")
    axes[0].set_title("R² Score")
    axes[0].set_ylim(0, 1.1)
    axes[0].grid(axis="y", alpha=0.3)

    # MAE
    mae_vals = [rev_metrics["mae"], qty_metrics["mae"]]
    axes[1].bar(labels, mae_vals, color=colors)
    for i, v in enumerate(mae_vals):
        axes[1].text(i, v + 0.1, f"{v:.2f}", ha="center", fontweight="bold")
    axes[1].set_ylabel("MAE")
    axes[1].set_title("Mean Absolute Error")
    axes[1].grid(axis="y", alpha=0.3)

    # CV R² (with error bars)
    cv_means = [rev_metrics["cv_r2_mean"], qty_metrics["cv_r2_mean"]]
    cv_stds = [rev_metrics["cv_r2_std"], qty_metrics["cv_r2_std"]]
    axes[2].bar(labels, cv_means, color=colors, yerr=cv_stds, capsize=8)
    for i, v in enumerate(cv_means):
        axes[2].text(i, v + cv_stds[i] + 0.02, f"{v:.4f}±{cv_stds[i]:.4f}", ha="center", fontsize=9)
    axes[2].set_ylabel("CV R² Score")
    axes[2].set_title("Cross-Validation R²")
    axes[2].set_ylim(0, 1.1)
    axes[2].grid(axis="y", alpha=0.3)

    plt.tight_layout()
    fig.savefig(os.path.join(chart_dir, "model_summary_comparison.png"), dpi=150)
    plt.close(fig)
    print(f"  ✓ Summary chart saved")


# ── main ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Train Sales Forecasting Model")
    parser.add_argument("--data-path", type=str, default=None)
    parser.add_argument("--output-dir", type=str, default="ml/model_output")
    parser.add_argument("--n-samples", type=int, default=50000)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    run = None
    if AZUREML_AVAILABLE:
        try:
            run = Run.get_context()
            if hasattr(run, "experiment"):
                print(f"[INFO] Azure ML Experiment: {run.experiment.name}")
        except Exception:
            run = None

    if args.data_path and os.path.exists(args.data_path):
        print(f"[INFO] Loading data from {args.data_path}")
        df = pd.read_csv(args.data_path)
        data_source = "csv"
    else:
        # Try SQL first, fall back to synthetic
        sql_df = load_sql_training_data(min_samples=1000)
        if sql_df is not None:
            df = sql_df
            data_source = "sql"
            print(f"[INFO] Using real SQL data ({len(df)} samples)")
        else:
            print(f"[INFO] Generating synthetic data ({args.n_samples} samples)...")
            df = generate_training_data(args.n_samples)
            data_source = "synthetic"
        df.to_csv(os.path.join(args.output_dir, "training_data.csv"), index=False)

    print(f"[INFO] Shape: {df.shape}")

    X, y_qty, y_rev, label_encoders, feature_cols = prepare_features(df)

    revenue_model, rev_metrics, X_tr_r, X_te_r, y_tr_r, y_te_r, y_pred_r = train_one_model(X, y_rev, "revenue")
    quantity_model, qty_metrics, X_tr_q, X_te_q, y_tr_q, y_te_q, y_pred_q = train_one_model(X, y_qty, "quantity")

    # Generate evaluation charts
    chart_dir = os.path.join(args.output_dir, "charts")
    generate_charts(revenue_model, X_tr_r, X_te_r, y_tr_r, y_te_r, y_pred_r, "revenue", feature_cols, chart_dir)
    generate_charts(quantity_model, X_tr_q, X_te_q, y_tr_q, y_te_q, y_pred_q, "quantity", feature_cols, chart_dir)

    # Summary comparison chart
    _generate_summary_chart(rev_metrics, qty_metrics, chart_dir)

    joblib.dump(revenue_model,  os.path.join(args.output_dir, "revenue_model.pkl"))
    joblib.dump(quantity_model, os.path.join(args.output_dir, "quantity_model.pkl"))
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
        "revenue_r2": rev_metrics.get("r2_score"),
        "revenue_rmse": rev_metrics.get("rmse"),
        "algorithm": "GradientBoostingRegressor",
        "n_features": len(feature_cols),
    }
    with open(os.path.join(args.output_dir, "model_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\n[INFO] Models saved to: {args.output_dir}")

    if run and hasattr(run, "log"):
        for k, v in rev_metrics.items():
            if isinstance(v, (int, float)):
                run.log(f"revenue_{k}", v)
        for k, v in qty_metrics.items():
            if isinstance(v, (int, float)):
                run.log(f"quantity_{k}", v)
    # ── Register model in AML Model Registry ─────────────────────
    if run and hasattr(run, 'experiment') and AZUREML_AVAILABLE:
        try:
            print("\n[INFO] Registering model in AML Model Registry...")
            registered = run.register_model(
                model_name="sales-forecast-model",
                model_path="outputs/model_output",
                description=(
                    f"GradientBoostingRegressor — {data_source} data, "
                    f"{len(df):,} samples — R²={rev_metrics['r2_score']:.4f}"
                ),
                tags={
                    "framework": "scikit-learn",
                    "algorithm": "GradientBoostingRegressor",
                    "data_source": data_source,
                    "training_samples": str(len(df)),
                    "trained_at": metadata["trained_at"],
                    "stage": "production",
                },
                properties={
                    "r2_score":          str(rev_metrics["r2_score"]),
                    "mae":               str(rev_metrics["mae"]),
                    "rmse":              str(rev_metrics["rmse"]),
                    "quantity_r2":       str(qty_metrics["r2_score"]),
                    "training_samples":  str(len(df)),
                    "sklearn_version":   metadata["sklearn_version"],
                },
            )
            print(f"[INFO] Model registered: {registered.name} v{registered.version}")
            print(f"[INFO] Model ID: {registered.id}")
        except Exception as _reg_err:
            print(f"[WARN] AML model registration failed (non-fatal): {_reg_err}")

    if run and hasattr(run, 'complete'):        run.complete()


if __name__ == "__main__":
    main()
