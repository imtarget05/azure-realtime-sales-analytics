"""
So sánh & Đánh giá tổng hợp 2 pipeline ML:
  - Rossmann real data (ml/data/, 4 models)
  - Simulated data (50K, 9+ models)

Sinh biểu đồ đánh giá output cho cả 2 hệ thống.
"""

import os
import sys
import time
import json
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
import matplotlib.patches as mpatches

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import (
    RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor,
)
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

try:
    from lightgbm import LGBMRegressor
    HAS_LGB = True
except ImportError:
    HAS_LGB = False

try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except ImportError:
    HAS_XGB = False


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "model_output", "evaluation_charts")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# PIPELINE A: Rossmann Real Data (ml/data/)
# ============================================================

def load_rossmann_data():
    """Load dữ liệu Rossmann thật."""
    data_dir = os.path.join(BASE_DIR, "data")
    train = pd.read_csv(
        os.path.join(data_dir, "train.csv"),
        parse_dates=["Date"],
        dtype={"StateHoliday": str, "SchoolHoliday": str},
        low_memory=False,
    )
    store = pd.read_csv(os.path.join(data_dir, "store.csv"))

    # Merge
    train = train.merge(store, how="left", on="Store")

    # Fill missing
    train["CompetitionDistance"] = train["CompetitionDistance"].fillna(1e5)
    train["CompetitionOpenSinceMonth"] = train["CompetitionOpenSinceMonth"].fillna(1)
    train["CompetitionOpenSinceYear"] = train["CompetitionOpenSinceYear"].fillna(
        train["Date"].dt.year
    )
    train["Promo2SinceWeek"] = train["Promo2SinceWeek"].fillna(0)
    train["Promo2SinceYear"] = train["Promo2SinceYear"].fillna(0)
    train["PromoInterval"] = train["PromoInterval"].fillna("")

    # IsPromoMonth
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    promo_months = train["PromoInterval"].str.split(",").apply(
        lambda x: [month_map[m] for m in x if m in month_map]
        if isinstance(x, list) else []
    )
    train["IsPromoMonth"] = [
        d.month in p for d, p in zip(train["Date"], promo_months)
    ]
    train["IsPromoMonth"] = train["IsPromoMonth"].astype(int)

    # Time features
    train["Year"] = train["Date"].dt.year
    train["Month"] = train["Date"].dt.month
    train["Day"] = train["Date"].dt.day
    train["DayOfWeek"] = train["Date"].dt.dayofweek
    train["WeekOfYear"] = train["Date"].dt.isocalendar().week.astype(int)
    train["IsWeekend"] = (train["DayOfWeek"] >= 5).astype(int)
    train["CompetitionOpenMonths"] = (
        (train["Year"] - train["CompetitionOpenSinceYear"]) * 12
        + (train["Month"] - train["CompetitionOpenSinceMonth"])
    ).clip(lower=0)

    # Filter
    train = train[(train["Open"] == 1) & (train["Sales"] > 0)].copy()

    # Lag features
    train = train.sort_values(["Store", "Date"])
    for lag in [1, 3, 7]:
        train[f"Sales_lag_{lag}"] = train.groupby("Store")["Sales"].shift(lag)
    train.fillna(0, inplace=True)

    # Encode
    for col in ["StateHoliday", "StoreType", "Assortment", "SchoolHoliday"]:
        if col in train.columns:
            train[col] = train[col].astype("category").cat.codes

    features = [
        "Store", "DayOfWeek", "Promo", "SchoolHoliday", "IsWeekend",
        "StoreType", "Assortment", "CompetitionDistance", "CompetitionOpenMonths",
        "Promo2", "IsPromoMonth", "Year", "Month", "Day", "WeekOfYear",
        "Sales_lag_1", "Sales_lag_3", "Sales_lag_7",
    ]

    X = train[features].copy()
    y = train["Sales"].copy()

    # Time-based split
    train_mask = train["Date"] < "2015-01-01"
    X_train, X_val = X[train_mask], X[~train_mask]
    y_train, y_val = y[train_mask], y[~train_mask]

    # Subsample training data for speed (keep all val data for fair evaluation)
    MAX_TRAIN = 100000
    if len(X_train) > MAX_TRAIN:
        idx = np.random.RandomState(42).choice(len(X_train), MAX_TRAIN, replace=False)
        X_train = X_train.iloc[idx].reset_index(drop=True)
        y_train = y_train.iloc[idx].reset_index(drop=True)
        print(f"  [INFO] Rossmann training subsampled: {MAX_TRAIN} samples (from {sum(train_mask)})")

    return X_train, X_val, y_train, y_val, features, train


# ============================================================
# PIPELINE B: Simulated Data (ml/)
# ============================================================

def generate_sim_data(n_samples=50000):
    """Sinh dữ liệu giả lập (giống ml/train_model.py + compare_models.py)."""
    np.random.seed(42)
    hours = np.random.randint(0, 24, n_samples)
    days_of_week = np.random.randint(0, 7, n_samples)
    months = np.random.randint(1, 13, n_samples)
    is_weekend = (days_of_week >= 5).astype(int)
    is_online = np.random.randint(0, 2, n_samples)
    category_ids = np.random.randint(0, 5, n_samples)
    region_ids = np.random.randint(0, 4, n_samples)
    product_ids = np.random.randint(0, 20, n_samples)
    base_prices = np.array([10, 25, 50, 100, 200])[category_ids]
    discount_pct = np.random.choice([0, 5, 10, 15, 20, 25], n_samples)
    quantity = np.random.randint(1, 11, n_samples)
    temperature = 15 + 15 * np.sin(2 * np.pi * (months - 1) / 12) + np.random.normal(0, 3, n_samples)
    humidity = 50 + np.random.normal(0, 15, n_samples)
    is_rainy = (np.random.random(n_samples) < 0.3).astype(int)

    hour_effect = np.sin(hours * np.pi / 12) * 20 + 10
    weekend_effect = is_weekend * 15
    month_effect = np.sin((months - 1) * np.pi / 6) * 10
    noise = np.random.normal(0, 5, n_samples)
    revenue = (
        base_prices * quantity * (1 - discount_pct / 100)
        + hour_effect + weekend_effect + month_effect + noise
    )
    revenue = np.maximum(revenue, 1)

    df = pd.DataFrame({
        "hour": hours, "day_of_week": days_of_week, "month": months,
        "is_weekend": is_weekend, "is_online": is_online,
        "category_id": category_ids, "region_id": region_ids,
        "product_id": product_ids, "base_price": base_prices,
        "discount_percent": discount_pct, "quantity": quantity,
        "hour_sin": np.sin(2 * np.pi * hours / 24),
        "hour_cos": np.cos(2 * np.pi * hours / 24),
        "month_sin": np.sin(2 * np.pi * months / 12),
        "month_cos": np.cos(2 * np.pi * months / 12),
        "temperature": temperature, "humidity": humidity,
        "is_rainy": is_rainy,
        "revenue": revenue,
    })
    feature_cols = [c for c in df.columns if c != "revenue"]
    return df[feature_cols], df["revenue"], feature_cols


# ============================================================
# UNIFIED MODEL ZOO
# ============================================================

def get_all_models():
    """Gộp toàn bộ mô hình từ cả 2 pipeline."""
    models = {
        # --- từ ml/compare_models.py ---
        "Linear Regression": LinearRegression(),
        "Ridge": Ridge(alpha=1.0),
        "Lasso": Lasso(alpha=0.1),
        "Decision Tree": DecisionTreeRegressor(max_depth=10, random_state=42),
        "KNN (k=5)": KNeighborsRegressor(n_neighbors=5, n_jobs=-1),
        "AdaBoost": AdaBoostRegressor(n_estimators=100, random_state=42),
        # --- từ cả 2 ---
        "Random Forest": RandomForestRegressor(
            n_estimators=100, max_depth=10, min_samples_leaf=10,
            random_state=42, n_jobs=-1,
        ),
        "Gradient Boosting": GradientBoostingRegressor(
            n_estimators=100, max_depth=5, learning_rate=0.1,
            subsample=0.8, random_state=42,
        ),
    }
    if HAS_LGB:
        models["LightGBM"] = LGBMRegressor(
            n_estimators=200, learning_rate=0.05, num_leaves=31,
            max_depth=8, subsample=0.8, colsample_bytree=0.8,
            random_state=42, verbose=-1, n_jobs=-1,
        )
    if HAS_XGB:
        models["XGBoost"] = XGBRegressor(
            n_estimators=200, learning_rate=0.05, max_depth=6,
            subsample=0.8, colsample_bytree=0.8,
            random_state=42, verbosity=0, n_jobs=-1,
        )
    return models


def evaluate_models(X_train, X_val, y_train, y_val, models, pipeline_name,
                    scale_for=None):
    """Huấn luyện & đánh giá tất cả models trên 1 dataset."""
    if scale_for is None:
        scale_for = set()

    scaler = StandardScaler()
    X_train_arr = X_train.values if hasattr(X_train, "values") else X_train
    X_val_arr = X_val.values if hasattr(X_val, "values") else X_val
    X_train_scaled = scaler.fit_transform(X_train_arr)
    X_val_scaled = scaler.transform(X_val_arr)

    results = []
    trained = {}

    for name, model in models.items():
        print(f"  [{pipeline_name}] Training {name}...")

        use_scaled = name in scale_for
        Xtr = X_train_scaled if use_scaled else X_train_arr
        Xte = X_val_scaled if use_scaled else X_val_arr

        # KNN/SVR/AdaBoost/GBR: subsample on large datasets (too slow otherwise)
        slow_models = {"SVR (RBF)", "KNN (k=5)", "AdaBoost", "Gradient Boosting"}
        if name in slow_models and len(Xtr) > 20000:
            subsample_n = 20000
            idx = np.random.choice(len(Xtr), subsample_n, replace=False)
            if hasattr(Xtr, "iloc"):
                Xtr_fit = Xtr.iloc[idx].values
            else:
                Xtr_fit = Xtr[idx]
            if hasattr(y_train, "iloc"):
                ytr_fit = y_train.iloc[idx].values
            elif hasattr(y_train, "values"):
                ytr_fit = y_train.values[idx]
            else:
                ytr_fit = y_train[idx]
            print(f"    [SUBSAMPLE] {name} dùng {subsample_n}/{len(Xtr)} mẫu (tối ưu tốc độ)")
        else:
            Xtr_fit = Xtr.values if hasattr(Xtr, "values") else Xtr
            ytr_fit = y_train.values if hasattr(y_train, "values") else y_train

        t0 = time.time()
        model.fit(Xtr_fit, ytr_fit)
        train_time = time.time() - t0

        y_pred = model.predict(Xte)
        y_val_arr = y_val.values if hasattr(y_val, "values") else y_val

        mae = mean_absolute_error(y_val_arr, y_pred)
        rmse = np.sqrt(mean_squared_error(y_val_arr, y_pred))
        r2 = r2_score(y_val_arr, y_pred)
        mask = y_val_arr > 0
        mape = np.mean(np.abs((y_val_arr[mask] - y_pred[mask]) / y_val_arr[mask])) * 100

        # Train metrics (overfit check)
        y_train_pred = model.predict(Xtr_fit)
        ytr_arr = ytr_fit if isinstance(ytr_fit, np.ndarray) else np.array(ytr_fit)
        train_r2 = r2_score(ytr_arr, y_train_pred)

        results.append({
            "Model": name, "Pipeline": pipeline_name,
            "MAE": round(mae, 2), "RMSE": round(rmse, 2),
            "R2": round(r2, 4), "MAPE": round(mape, 2),
            "Train_R2": round(train_r2, 4),
            "Train_Time": round(train_time, 2),
        })
        trained[name] = {"model": model, "y_pred": y_pred}

        print(f"    MAE={mae:.2f} | R2={r2:.4f} | MAPE={mape:.1f}% | {train_time:.1f}s")

    return pd.DataFrame(results), trained


# ============================================================
# BIỂU ĐỒ 1: So sánh tổng quan 2 Pipeline
# ============================================================

def chart_pipeline_overview(rossmann_df, sim_df):
    """Biểu đồ tổng quan so sánh 2 pipeline."""
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle(
        "ĐÁNH GIÁ TỔNG HỢP 2 PIPELINE ML\n"
        "Rossmann Real Data (844K) vs Simulated Data (50K)",
        fontsize=16, fontweight="bold", y=0.98,
    )

    # ── Chart 1: R² Score comparison ──
    ax = axes[0, 0]
    # Chọn models chung
    common_models = set(rossmann_df["Model"]) & set(sim_df["Model"])
    ross_common = rossmann_df[rossmann_df["Model"].isin(common_models)].sort_values("Model")
    sim_common = sim_df[sim_df["Model"].isin(common_models)].sort_values("Model")
    x = np.arange(len(common_models))
    w = 0.35
    models_sorted = sorted(common_models)
    ross_r2 = [ross_common[ross_common["Model"] == m]["R2"].values[0] for m in models_sorted]
    sim_r2 = [sim_common[sim_common["Model"] == m]["R2"].values[0] for m in models_sorted]
    ax.bar(x - w/2, ross_r2, w, label="Rossmann (Real)", color="#2196F3")
    ax.bar(x + w/2, sim_r2, w, label="Simulated", color="#FF9800")
    ax.set_xticks(x)
    ax.set_xticklabels(models_sorted, rotation=25, ha="right", fontsize=8)
    ax.set_ylabel("R² Score")
    ax.set_title("R² Score — Models chung")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)

    # ── Chart 2: MAE comparison ──
    ax = axes[0, 1]
    ross_mae = [ross_common[ross_common["Model"] == m]["MAE"].values[0] for m in models_sorted]
    sim_mae = [sim_common[sim_common["Model"] == m]["MAE"].values[0] for m in models_sorted]
    ax.bar(x - w/2, ross_mae, w, label="Rossmann", color="#2196F3")
    ax.bar(x + w/2, sim_mae, w, label="Simulated", color="#FF9800")
    ax.set_xticks(x)
    ax.set_xticklabels(models_sorted, rotation=25, ha="right", fontsize=8)
    ax.set_ylabel("MAE")
    ax.set_title("MAE — Models chung (thấp hơn = tốt)")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)

    # ── Chart 3: MAPE comparison ──
    ax = axes[0, 2]
    ross_mape = [ross_common[ross_common["Model"] == m]["MAPE"].values[0] for m in models_sorted]
    sim_mape = [sim_common[sim_common["Model"] == m]["MAPE"].values[0] for m in models_sorted]
    ax.bar(x - w/2, ross_mape, w, label="Rossmann", color="#2196F3")
    ax.bar(x + w/2, sim_mape, w, label="Simulated", color="#FF9800")
    ax.set_xticks(x)
    ax.set_xticklabels(models_sorted, rotation=25, ha="right", fontsize=8)
    ax.set_ylabel("MAPE (%)")
    ax.set_title("MAPE (%) — Models chung")
    ax.legend(fontsize=9)
    ax.grid(axis="y", alpha=0.3)

    # ── Chart 4: Overfitting check (Train R² vs Val R²) ──
    ax = axes[1, 0]
    all_df = pd.concat([rossmann_df, sim_df], ignore_index=True)
    for pipe, color, marker in [("Rossmann", "#2196F3", "o"), ("Simulated", "#FF9800", "s")]:
        subset = all_df[all_df["Pipeline"] == pipe]
        ax.scatter(subset["Train_R2"], subset["R2"], c=color, marker=marker,
                   s=80, alpha=0.8, label=pipe, edgecolors="black", linewidth=0.5)
        for _, row in subset.iterrows():
            ax.annotate(row["Model"], (row["Train_R2"], row["R2"]),
                       fontsize=6, alpha=0.7, ha="left", va="bottom")
    ax.plot([0, 1], [0, 1], "k--", alpha=0.4, label="Perfect (no overfit)")
    ax.set_xlabel("Train R²")
    ax.set_ylabel("Validation R²")
    ax.set_title("Overfitting Check (gần đường = ít overfit)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)

    # ── Chart 5: Training Time ──
    ax = axes[1, 1]
    all_sorted = all_df.sort_values("Train_Time", ascending=True)
    colors = ["#2196F3" if p == "Rossmann" else "#FF9800" for p in all_sorted["Pipeline"]]
    labels = [f"{r['Model']} ({r['Pipeline'][:4]})" for _, r in all_sorted.iterrows()]
    ax.barh(labels, all_sorted["Train_Time"], color=colors)
    ax.set_xlabel("Seconds")
    ax.set_title("Training Time")
    ax.grid(axis="x", alpha=0.3)

    # ── Chart 6: Best Model Summary Table ──
    ax = axes[1, 2]
    ax.axis("off")
    ross_best = rossmann_df.sort_values("MAE").iloc[0]
    sim_best = sim_df.sort_values("MAE").iloc[0]

    table_data = [
        ["", "Rossmann (Real)", "Simulated"],
        ["Best Model", ross_best["Model"], sim_best["Model"]],
        ["MAE", f'{ross_best["MAE"]:.2f}', f'{sim_best["MAE"]:.2f}'],
        ["RMSE", f'{ross_best["RMSE"]:.2f}', f'{sim_best["RMSE"]:.2f}'],
        ["R²", f'{ross_best["R2"]:.4f}', f'{sim_best["R2"]:.4f}'],
        ["MAPE", f'{ross_best["MAPE"]:.1f}%', f'{sim_best["MAPE"]:.1f}%'],
        ["Train Time", f'{ross_best["Train_Time"]:.1f}s', f'{sim_best["Train_Time"]:.1f}s'],
        ["Overfit Gap",
         f'{(ross_best["Train_R2"] - ross_best["R2"]):.4f}',
         f'{(sim_best["Train_R2"] - sim_best["R2"]):.4f}'],
    ]
    table = ax.table(cellText=table_data, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.8)
    # Header row
    for j in range(3):
        table[0, j].set_facecolor("#E3F2FD")
        table[0, j].set_text_props(fontweight="bold")
    for i in range(1, len(table_data)):
        table[i, 0].set_facecolor("#F5F5F5")
        table[i, 0].set_text_props(fontweight="bold")
    ax.set_title("Best Model (mỗi pipeline)", fontweight="bold", pad=20)

    plt.tight_layout(rect=[0, 0, 1, 0.94])
    path = os.path.join(OUTPUT_DIR, "01_pipeline_comparison.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ============================================================
# BIỂU ĐỒ 2: Actual vs Predicted — cả 2 pipeline
# ============================================================

def chart_actual_vs_predicted(y_val_ross, trained_ross, y_val_sim, trained_sim,
                              rossmann_df, sim_df):
    """Actual vs Predicted cho best model mỗi pipeline."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("Actual vs Predicted — Best Model mỗi Pipeline",
                 fontsize=14, fontweight="bold")

    for i, (name, y_val, trained, res_df, color, pipe_label) in enumerate([
        ("best_ross", y_val_ross, trained_ross, rossmann_df, "#E91E63", "Rossmann"),
        ("best_sim", y_val_sim, trained_sim, sim_df, "#2196F3", "Simulated"),
    ]):
        best_name = res_df.sort_values("MAE").iloc[0]["Model"]
        y_pred = trained[best_name]["y_pred"]
        y_actual = y_val.values if hasattr(y_val, "values") else y_val

        ax = axes[i]
        rng = np.random.RandomState(42)
        n = min(5000, len(y_actual))
        idx = rng.choice(len(y_actual), n, replace=False)
        ax.scatter(y_actual[idx], y_pred[idx], alpha=0.2, s=5, color=color)
        lims = [0, max(y_actual[idx].max(), y_pred[idx].max())]
        ax.plot(lims, lims, "k--", alpha=0.6, linewidth=1.5, label="Perfect")
        r2 = r2_score(y_actual, y_pred)
        mae = mean_absolute_error(y_actual, y_pred)
        ax.set_xlabel("Actual")
        ax.set_ylabel("Predicted")
        ax.set_title(f"{pipe_label}: {best_name}\nR²={r2:.4f} | MAE={mae:.2f}")
        ax.legend()
        ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "02_actual_vs_predicted.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ============================================================
# BIỂU ĐỒ 3: Residual Analysis cả 2
# ============================================================

def chart_residuals(y_val_ross, trained_ross, y_val_sim, trained_sim,
                    rossmann_df, sim_df):
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Residual Analysis — Best Model mỗi Pipeline",
                 fontsize=14, fontweight="bold")

    for col, (y_val, trained, res_df, pipe, color) in enumerate([
        (y_val_ross, trained_ross, rossmann_df, "Rossmann", "#2196F3"),
        (y_val_sim, trained_sim, sim_df, "Simulated", "#FF9800"),
    ]):
        best_name = res_df.sort_values("MAE").iloc[0]["Model"]
        y_pred = trained[best_name]["y_pred"]
        y_actual = y_val.values if hasattr(y_val, "values") else y_val
        residuals = y_actual - y_pred

        # Histogram
        ax = axes[0, col]
        ax.hist(residuals, bins=100, color=color, alpha=0.7, edgecolor="white")
        ax.axvline(0, color="red", linestyle="--", linewidth=1.5)
        ax.axvline(np.mean(residuals), color="orange", linewidth=1,
                   label=f"Mean={np.mean(residuals):.1f}")
        ax.set_title(f"{pipe}: {best_name} — Residual Distribution")
        ax.legend()
        ax.grid(alpha=0.3)

        # Residual vs Predicted
        ax = axes[1, col]
        rng = np.random.RandomState(42)
        n = min(5000, len(residuals))
        idx = rng.choice(len(residuals), n, replace=False)
        ax.scatter(y_pred[idx], residuals[idx], alpha=0.15, s=5, color=color)
        ax.axhline(0, color="red", linestyle="--", linewidth=1)
        ax.set_xlabel("Predicted Value")
        ax.set_ylabel("Residual")
        ax.set_title(f"{pipe}: Residual vs Predicted")
        ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "03_residual_analysis.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ============================================================
# BIỂU ĐỒ 4: Feature Importance so sánh
# ============================================================

def chart_feature_importance(trained_ross, trained_sim, rossmann_df, sim_df,
                             features_ross, features_sim):
    fig, axes = plt.subplots(1, 2, figsize=(18, 8))
    fig.suptitle("Feature Importance — Best Model mỗi Pipeline",
                 fontsize=14, fontweight="bold")

    for i, (trained, res_df, feats, pipe, cmap) in enumerate([
        (trained_ross, rossmann_df, features_ross, "Rossmann", "RdYlGn"),
        (trained_sim, sim_df, features_sim, "Simulated", "YlOrRd"),
    ]):
        best_name = res_df.sort_values("MAE").iloc[0]["Model"]
        model = trained[best_name]["model"]
        ax = axes[i]

        if hasattr(model, "feature_importances_"):
            importances = model.feature_importances_
            indices = np.argsort(importances)
            colors = plt.cm.get_cmap(cmap)(np.linspace(0.2, 0.9, len(feats)))
            ax.barh(range(len(feats)), importances[indices], color=colors)
            ax.set_yticks(range(len(feats)))
            ax.set_yticklabels([feats[j] for j in indices], fontsize=9)
            ax.set_title(f"{pipe}: {best_name}")
            ax.set_xlabel("Importance")
            ax.grid(axis="x", alpha=0.3)
        else:
            ax.text(0.5, 0.5, f"{best_name}\nNo feature_importances_",
                    ha="center", va="center", transform=ax.transAxes)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "04_feature_importance.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ============================================================
# BIỂU ĐỒ 5: Architecture — Tái sử dụng & Gộp
# ============================================================

def chart_merge_architecture():
    """Biểu đồ kiến trúc gộp 2 pipeline."""
    fig, ax = plt.subplots(figsize=(20, 14))
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 14)
    ax.axis("off")
    ax.set_title(
        "KIẾN TRÚC GỘP — TÁI SỬ DỤNG COMPONENTS TỪ CẢ 2 PIPELINE",
        fontsize=16, fontweight="bold", pad=20,
    )

    def draw_box(x, y, w, h, text, color, alpha=0.3, fontsize=9):
        rect = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.15",
                              facecolor=color, alpha=alpha, edgecolor="black", linewidth=1.5)
        ax.add_patch(rect)
        ax.text(x + w/2, y + h/2, text, ha="center", va="center",
                fontsize=fontsize, fontweight="bold", wrap=True)

    def draw_arrow(x1, y1, x2, y2, color="black"):
        ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                    arrowprops=dict(arrowstyle="->", color=color, lw=1.5))

    # === Row 1: Data Sources ===
    draw_box(0.5, 12, 4, 1.2, "ml/data/\n(Rossmann 844K real)", "#BBDEFB")
    draw_box(5.5, 12, 4, 1.2, "ml/train_model.py\n(Sinh 50K simulated)", "#FFE0B2")
    draw_box(10.5, 12, 4, 1.2, "Azure SQL Database\n(Streaming data thực)", "#C8E6C9")

    # === Row 2: Training ===
    draw_box(0.5, 9.5, 5.5, 2, "TRAINING PIPELINE (GỘP)\n────────────────\n"
             "✓ 10 models (LGB, XGB, RF, GBR, DT,\n"
             "   Ridge, Lasso, KNN, Ada, Linear)\n"
             "✓ Time-based split (từ ML/)\n"
             "✓ Cross-validation (từ ml/)\n"
             "✓ Overfit detection (từ ML/)", "#E3F2FD", fontsize=8)
    draw_box(7, 9.5, 5.5, 2, "EVALUATION (GỘP)\n────────────────\n"
             "✓ 6+ biểu đồ (matplotlib + plotly)\n"
             "✓ Actual vs Predicted (từ ML/)\n"
             "✓ Residual + Feature Import (ML/)\n"
             "✓ Radar chart (từ ml/)\n"
             "✓ Pipeline comparison (MỚI)", "#FFF3E0", fontsize=8)
    draw_box(13.5, 9.5, 5.5, 2, "SCORING (GỘP)\n────────────────\n"
             "✓ Azure ML Endpoint (từ ml/score.py)\n"
             "✓ Batch scoring (từ ML/score.py)\n"
             "✓ Auto-detect model type\n"
             "✓ Confidence interval (từ ml/)\n"
             "✓ Unified metadata format", "#E8F5E9", fontsize=8)

    # Arrows from data -> training
    draw_arrow(2.5, 12, 3.25, 11.5, "#2196F3")
    draw_arrow(7.5, 12, 3.25, 11.5, "#FF9800")
    draw_arrow(12.5, 12, 9.75, 11.5, "#4CAF50")

    # === Row 3: Deployment ===
    draw_box(0.5, 6.5, 4, 2.2, "DEPLOY (từ ml/)\n────────────────\n"
             "deploy_model.py\nconda_env.yml\n→ Azure ML Endpoint", "#F3E5F5")
    draw_box(5.5, 6.5, 4, 2.2, "FORECAST (từ ml/)\n────────────────\n"
             "realtime_forecast.py\n24h × 5 regions × 4 cats\n→ Azure SQL", "#E0F7FA")
    draw_box(10.5, 6.5, 4, 2.2, "MONITORING\n────────────────\n"
             "monitoring/telemetry.py\nApp Insights + Alerts\n→ Azure Monitor", "#FFF9C4")
    draw_box(15.5, 6.5, 4, 2.2, "CI/CD\n────────────────\n"
             ".github/workflows/\n3 pipelines: Sim,\nFunctions, ML", "#FFCCBC")

    # Arrows
    draw_arrow(3.25, 9.5, 2.5, 8.7, "#9C27B0")
    draw_arrow(9.75, 9.5, 7.5, 8.7, "#00BCD4")
    draw_arrow(9.75, 9.5, 12.5, 8.7, "#FFC107")
    draw_arrow(15.75, 9.5, 17.5, 8.7, "#FF5722")

    # === Row 4: Legend ===
    legend_items = [
        mpatches.Patch(facecolor="#BBDEFB", label="Rossmann Real Data (ml/data/)"),
        mpatches.Patch(facecolor="#FFE0B2", label="Từ ml/ (Simulated)"),
        mpatches.Patch(facecolor="#C8E6C9", label="Azure Streaming (mới)"),
        mpatches.Patch(facecolor="#E3F2FD", label="GỘP (cả 2 pipeline)"),
    ]
    ax.legend(handles=legend_items, loc="lower center", ncol=4, fontsize=10,
              frameon=True, fancybox=True, shadow=True)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "05_merge_architecture.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ============================================================
# BIỂU ĐỒ 6: Radar Chart — Top models cả 2 pipeline
# ============================================================

def chart_radar(rossmann_df, sim_df):
    """Radar chart so sánh top models."""
    all_df = pd.concat([rossmann_df, sim_df], ignore_index=True)

    # Top 3 mỗi pipeline
    ross_top = rossmann_df.nsmallest(3, "MAE")
    sim_top = sim_df.nsmallest(3, "MAE")
    selected = pd.concat([ross_top, sim_top])

    metrics = ["R2", "MAE", "RMSE", "MAPE", "Train_Time"]
    labels = ["R²↑", "MAE↓", "RMSE↓", "MAPE↓", "Speed↑"]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection="polar"))
    angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
    angles += angles[:1]

    colors = plt.cm.tab10(np.linspace(0, 1, len(selected)))

    for idx, (_, row) in enumerate(selected.iterrows()):
        vals = []
        for m in metrics:
            col = all_df[m]
            if m == "R2":
                vals.append(row[m])
            else:
                rng = col.max() - col.min()
                vals.append(1 - (row[m] - col.min()) / (rng + 1e-9))
        vals += vals[:1]
        label = f"{row['Model']} ({row['Pipeline'][:4]})"
        ax.plot(angles, vals, "o-", linewidth=2, label=label, color=colors[idx])
        ax.fill(angles, vals, alpha=0.05, color=colors[idx])

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_ylim(0, 1.05)
    ax.set_title("Radar Chart — Top 3 Models mỗi Pipeline\n(gần rìa = tốt hơn)",
                 fontsize=13, fontweight="bold", pad=30)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.1), fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "06_radar_chart.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 70)
    print("  ĐÁNH GIÁ TỔNG HỢP 2 PIPELINE ML")
    print("  Rossmann Real Data (ml/data/) vs Simulated Data")
    print("=" * 70)

    # ── Load datasets ──
    print("\n[1/6] Load Rossmann real data...")
    X_train_r, X_val_r, y_train_r, y_val_r, feats_r, _ = load_rossmann_data()
    print(f"  Train: {X_train_r.shape}, Val: {X_val_r.shape}")

    print("\n[2/6] Generate simulated data...")
    X_sim, y_sim, feats_s = generate_sim_data(50000)
    X_train_s, X_val_s, y_train_s, y_val_s = train_test_split(
        X_sim, y_sim, test_size=0.2, random_state=42
    )
    print(f"  Train: {X_train_s.shape}, Val: {X_val_s.shape}")

    # ── Train & evaluate ──
    models = get_all_models()
    scale_for = {"Linear Regression", "Ridge", "Lasso", "KNN (k=5)"}

    print(f"\n[3/6] Evaluate on Rossmann ({len(models)} models)...")
    ross_df, trained_r = evaluate_models(
        X_train_r, X_val_r, y_train_r, y_val_r, models,
        "Rossmann", scale_for,
    )

    # Reset models for second run
    models2 = get_all_models()
    print(f"\n[4/6] Evaluate on Simulated ({len(models2)} models)...")
    sim_df, trained_s = evaluate_models(
        X_train_s.values, X_val_s.values, y_train_s, y_val_s, models2,
        "Simulated", scale_for,
    )

    # ── Print results ──
    print("\n" + "=" * 80)
    print("ROSSMANN PIPELINE:")
    print(ross_df.sort_values("MAE")[
        ["Model", "MAE", "RMSE", "R2", "MAPE", "Train_R2", "Train_Time"]
    ].to_string(index=False))

    print("\nSIMULATED PIPELINE:")
    print(sim_df.sort_values("MAE")[
        ["Model", "MAE", "RMSE", "R2", "MAPE", "Train_R2", "Train_Time"]
    ].to_string(index=False))

    # ── Save JSON report ──
    report = {
        "rossmann_results": ross_df.sort_values("MAE").to_dict(orient="records"),
        "simulated_results": sim_df.sort_values("MAE").to_dict(orient="records"),
        "rossmann_best": ross_df.sort_values("MAE").iloc[0].to_dict(),
        "simulated_best": sim_df.sort_values("MAE").iloc[0].to_dict(),
        "common_models": sorted(set(ross_df["Model"]) & set(sim_df["Model"])),
        "reuse_analysis": {
            "from_rossmann_pipeline": [
                "Time-based split strategy",
                "Lag features (Sales_lag_1/3/7)",
                "Overfit detection (train vs val)",
                "5 chart types (comparison, scatter, residual, importance, trend)",
                "Feature engineering (competition, promo, time)",
            ],
            "from_ml": [
                "Azure ML Endpoint deployment (deploy_model.py)",
                "Realtime forecast cycle (realtime_forecast.py)",
                "init/run scoring pattern (score.py)",
                "Confidence intervals",
                "Conda environment (conda_env.yml)",
                "9-model comparison framework (compare_models.py)",
                "Radar chart + Plotly interactive charts",
            ],
        },
    }
    report_path = os.path.join(OUTPUT_DIR, "evaluation_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nReport: {report_path}")

    # ── Generate charts ──
    print("\n[5/6] Generating charts...")
    chart_pipeline_overview(ross_df, sim_df)
    chart_actual_vs_predicted(y_val_r, trained_r, y_val_s, trained_s,
                              ross_df, sim_df)
    chart_residuals(y_val_r, trained_r, y_val_s, trained_s, ross_df, sim_df)
    chart_feature_importance(trained_r, trained_s, ross_df, sim_df,
                             feats_r, feats_s)
    chart_merge_architecture()
    chart_radar(ross_df, sim_df)

    print(f"\n[6/6] Hoàn tất!")
    print(f"  Output dir: {OUTPUT_DIR}")
    print(f"  6 biểu đồ + 1 report JSON")

    # Summary
    rb = ross_df.sort_values("MAE").iloc[0]
    sb = sim_df.sort_values("MAE").iloc[0]
    print(f"\n{'='*70}")
    print(f"  KẾT LUẬN:")
    print(f"  Rossmann best: {rb['Model']} (R²={rb['R2']:.4f}, MAE={rb['MAE']:.2f})")
    print(f"  Simulated best: {sb['Model']} (R²={sb['R2']:.4f}, MAE={sb['MAE']:.2f})")
    print(f"  Models chung (tái sử dụng): {len(set(ross_df['Model']) & set(sim_df['Model']))}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
