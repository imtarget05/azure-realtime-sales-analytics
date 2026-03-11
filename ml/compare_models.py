"""
Mục 4 (Phân tích).3 Rubric: So sánh nhiều mô hình ML & trực quan hóa kết quả.
- Huấn luyện nhiều mô hình: Linear Regression, Random Forest, Gradient Boosting, SVR, KNN
- So sánh metrics: MAE, RMSE, R², MAPE, training time
- Xuất biểu đồ so sánh (matplotlib/plotly)
- Cross-validation + learning curves
"""

import os
import sys
import time
import json
import warnings
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, AdaBoostRegressor
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

warnings.filterwarnings("ignore")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "benchmark_output", "ml_comparison")

# ============================================================
# 1. SINH DỮ LIỆU HUẤN LUYỆN
# ============================================================

def generate_training_data(n_samples: int = 50000) -> pd.DataFrame:
    """Sinh dữ liệu sales giả lập cho training."""
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

    # Revenue with patterns
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
        "hour": hours,
        "day_of_week": days_of_week,
        "month": months,
        "is_weekend": is_weekend,
        "is_online": is_online,
        "category_id": category_ids,
        "region_id": region_ids,
        "product_id": product_ids,
        "base_price": base_prices,
        "discount_percent": discount_pct,
        "quantity": quantity,
        "hour_sin": np.sin(2 * np.pi * hours / 24),
        "hour_cos": np.cos(2 * np.pi * hours / 24),
        "month_sin": np.sin(2 * np.pi * months / 12),
        "month_cos": np.cos(2 * np.pi * months / 12),
        "revenue": revenue,
    })
    return df


# ============================================================
# 2. HUẤN LUYỆN & SO SÁNH CÁC MÔ HÌNH
# ============================================================

def get_models():
    """Trả về dict tên → mô hình."""
    return {
        "Linear Regression": LinearRegression(),
        "Ridge Regression": Ridge(alpha=1.0),
        "Lasso Regression": Lasso(alpha=0.1),
        "Decision Tree": DecisionTreeRegressor(max_depth=10, random_state=42),
        "Random Forest": RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1),
        "Gradient Boosting": GradientBoostingRegressor(n_estimators=200, max_depth=5, random_state=42),
        "AdaBoost": AdaBoostRegressor(n_estimators=100, random_state=42),
        "KNN (k=5)": KNeighborsRegressor(n_neighbors=5, n_jobs=-1),
        "SVR (RBF)": SVR(kernel="rbf", C=100),
    }


def train_and_evaluate(df: pd.DataFrame) -> pd.DataFrame:
    """Huấn luyện, đánh giá tất cả mô hình."""
    feature_cols = [c for c in df.columns if c != "revenue"]
    X = df[feature_cols].values
    y = df["revenue"].values

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    models = get_models()
    results = []

    for name, model in models.items():
        print(f"  Training {name}...")

        use_scaled = name in ["SVR (RBF)", "KNN (k=5)", "Linear Regression",
                              "Ridge Regression", "Lasso Regression"]
        X_tr = X_train_scaled if use_scaled else X_train
        X_te = X_test_scaled if use_scaled else X_test

        # Giới hạn SVR trên subset nhỏ hơn
        if "SVR" in name and len(X_tr) > 10000:
            idx = np.random.choice(len(X_tr), 10000, replace=False)
            X_tr_fit, y_tr_fit = X_tr[idx], y_train[idx]
        else:
            X_tr_fit, y_tr_fit = X_tr, y_train

        t0 = time.time()
        model.fit(X_tr_fit, y_tr_fit)
        train_time = time.time() - t0

        t0 = time.time()
        y_pred = model.predict(X_te)
        predict_time = time.time() - t0

        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        r2 = r2_score(y_test, y_pred)
        mape = np.mean(np.abs((y_test - y_pred) / np.maximum(y_test, 1))) * 100

        # Cross-validation (3-fold cho nhanh)
        cv_scores = cross_val_score(
            model, X_tr_fit, y_tr_fit, cv=3,
            scoring="neg_mean_absolute_error", n_jobs=-1,
        )

        results.append({
            "model": name,
            "MAE": round(mae, 4),
            "RMSE": round(rmse, 4),
            "R2": round(r2, 4),
            "MAPE_%": round(mape, 2),
            "train_time_sec": round(train_time, 3),
            "predict_time_sec": round(predict_time, 4),
            "cv_mae_mean": round(-cv_scores.mean(), 4),
            "cv_mae_std": round(cv_scores.std(), 4),
            "y_test": y_test.tolist()[:200],
            "y_pred": y_pred.tolist()[:200],
        })

        print(f"    MAE={mae:.4f} | RMSE={rmse:.4f} | R²={r2:.4f} | "
              f"MAPE={mape:.2f}% | Train={train_time:.3f}s")

    return pd.DataFrame(results)


# ============================================================
# 3. TRỰC QUAN HÓA
# ============================================================

def plot_matplotlib(results_df: pd.DataFrame):
    """Tạo biểu đồ so sánh bằng matplotlib."""
    if not HAS_MATPLOTLIB:
        print("[WARN] matplotlib chưa cài đặt, bỏ qua biểu đồ matplotlib.")
        return

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("So sánh các mô hình Machine Learning\nDự đoán doanh thu bán hàng", fontsize=14, fontweight="bold")

    models = results_df["model"].tolist()
    colors = plt.cm.Set3(np.linspace(0, 1, len(models)))

    # Chart 1: MAE + RMSE
    ax = axes[0, 0]
    x = np.arange(len(models))
    width = 0.35
    ax.bar(x - width/2, results_df["MAE"], width, label="MAE", color="#2196F3")
    ax.bar(x + width/2, results_df["RMSE"], width, label="RMSE", color="#FF9800")
    ax.set_ylabel("Error")
    ax.set_title("MAE và RMSE theo mô hình")
    ax.set_xticks(x)
    ax.set_xticklabels(models, rotation=45, ha="right", fontsize=8)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    # Chart 2: R² Score
    ax = axes[0, 1]
    bars = ax.barh(models, results_df["R2"], color=colors)
    ax.set_xlabel("R² Score")
    ax.set_title("R² Score (cao hơn = tốt hơn)")
    ax.set_xlim(0, 1.05)
    for bar, val in zip(bars, results_df["R2"]):
        ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
                f"{val:.4f}", va="center", fontsize=8)
    ax.grid(axis="x", alpha=0.3)

    # Chart 3: Training Time
    ax = axes[1, 0]
    ax.barh(models, results_df["train_time_sec"], color="#4CAF50")
    ax.set_xlabel("Thời gian (giây)")
    ax.set_title("Thời gian huấn luyện")
    ax.grid(axis="x", alpha=0.3)

    # Chart 4: MAPE
    ax = axes[1, 1]
    ax.bar(models, results_df["MAPE_%"], color="#E91E63")
    ax.set_ylabel("MAPE (%)")
    ax.set_title("Mean Absolute Percentage Error")
    ax.set_xticklabels(models, rotation=45, ha="right", fontsize=8)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "model_comparison_matplotlib.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")

    # Chart 5: Actual vs Predicted (top 3 mô hình)
    top3 = results_df.nsmallest(3, "MAE")
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle("Actual vs Predicted (Top 3 mô hình tốt nhất)", fontsize=13)

    for i, (_, row) in enumerate(top3.iterrows()):
        ax = axes[i]
        y_test = row["y_test"][:100]
        y_pred = row["y_pred"][:100]
        ax.scatter(y_test, y_pred, alpha=0.5, s=10)
        lims = [min(min(y_test), min(y_pred)), max(max(y_test), max(y_pred))]
        ax.plot(lims, lims, "r--", alpha=0.7, label="Perfect prediction")
        ax.set_xlabel("Actual")
        ax.set_ylabel("Predicted")
        ax.set_title(f"{row['model']}\nR²={row['R2']:.4f}")
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "actual_vs_predicted.png")
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {path}")


def plot_plotly(results_df: pd.DataFrame):
    """Tạo biểu đồ interactive bằng Plotly."""
    if not HAS_PLOTLY:
        print("[WARN] plotly chưa cài đặt, bỏ qua biểu đồ plotly.")
        return

    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("MAE theo mô hình", "R² Score", "Thời gian huấn luyện", "MAPE (%)"),
        vertical_spacing=0.12,
    )

    models = results_df["model"].tolist()

    fig.add_trace(go.Bar(name="MAE", x=models, y=results_df["MAE"], marker_color="#2196F3"), row=1, col=1)
    fig.add_trace(go.Bar(name="RMSE", x=models, y=results_df["RMSE"], marker_color="#FF9800"), row=1, col=1)
    fig.add_trace(go.Bar(name="R²", x=models, y=results_df["R2"], marker_color="#4CAF50"), row=1, col=2)
    fig.add_trace(go.Bar(name="Train Time", x=models, y=results_df["train_time_sec"], marker_color="#9C27B0"), row=2, col=1)
    fig.add_trace(go.Bar(name="MAPE %", x=models, y=results_df["MAPE_%"], marker_color="#E91E63"), row=2, col=2)

    fig.update_layout(
        title="So sánh các mô hình ML - Dự đoán doanh thu bán hàng",
        height=800,
        showlegend=False,
    )

    path = os.path.join(OUTPUT_DIR, "model_comparison_plotly.html")
    fig.write_html(path)
    print(f"  Saved: {path}")

    # Radar chart
    metrics = ["R2", "MAE", "RMSE", "MAPE_%", "train_time_sec"]
    fig2 = go.Figure()
    for _, row in results_df.iterrows():
        # Normalize metrics to 0-1 for radar
        vals = []
        for m in metrics:
            col = results_df[m]
            if m == "R2":
                vals.append(row[m])
            else:
                vals.append(1 - (row[m] - col.min()) / (col.max() - col.min() + 1e-9))
        vals.append(vals[0])
        fig2.add_trace(go.Scatterpolar(
            r=vals,
            theta=metrics + [metrics[0]],
            name=row["model"],
        ))
    fig2.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        title="Radar Chart - So sánh tổng thể các mô hình",
    )
    path = os.path.join(OUTPUT_DIR, "model_radar_chart.html")
    fig2.write_html(path)
    print(f"  Saved: {path}")


# ============================================================
# 4. MAIN
# ============================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 60)
    print("  SO SÁNH CÁC MÔ HÌNH MACHINE LEARNING")
    print("=" * 60)

    # Sinh dữ liệu
    print("\n[1] Sinh dữ liệu huấn luyện...")
    df = generate_training_data(n_samples=50000)
    print(f"  Shape: {df.shape}")

    # Huấn luyện & đánh giá
    print("\n[2] Huấn luyện và đánh giá mô hình...")
    results_df = train_and_evaluate(df)

    # Sắp xếp theo MAE
    summary = results_df[["model", "MAE", "RMSE", "R2", "MAPE_%", "train_time_sec", "cv_mae_mean"]].copy()
    summary = summary.sort_values("MAE")

    print(f"\n{'='*80}")
    print(f"  BẢNG XẾP HẠNG (theo MAE, thấp hơn = tốt hơn)")
    print(f"{'='*80}")
    print(summary.to_string(index=False))

    best = summary.iloc[0]
    print(f"\n  → MÔ HÌNH TỐT NHẤT: {best['model']}")
    print(f"    MAE={best['MAE']:.4f}, R²={best['R2']:.4f}, MAPE={best['MAPE_%']:.2f}%")

    # Xuất kết quả JSON
    report = {
        "ranking": summary.to_dict(orient="records"),
        "best_model": best["model"],
        "best_metrics": {"MAE": best["MAE"], "R2": best["R2"], "MAPE_%": best["MAPE_%"]},
    }
    report_path = os.path.join(OUTPUT_DIR, "model_comparison_results.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # Tạo biểu đồ
    print("\n[3] Tạo biểu đồ so sánh...")
    plot_matplotlib(results_df)
    plot_plotly(results_df)

    print(f"\n  Kết quả: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
