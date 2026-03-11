"""
Huấn luyện mô hình dự đoán nhu cầu bán hàng (Demand Forecasting)
sử dụng Azure Machine Learning.

Mô hình: Gradient Boosting Regressor dự đoán doanh thu theo giờ.
"""

import argparse
import os
import json
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder

# Thử import Azure ML SDK (không bắt buộc cho local training)
try:
    from azureml.core import Run, Model, Workspace
    AZUREML_AVAILABLE = True
except ImportError:
    AZUREML_AVAILABLE = False
    print("[WARN] Azure ML SDK không có sẵn. Chạy ở chế độ local.")


def generate_training_data(n_samples: int = 50000) -> pd.DataFrame:
    """
    Sinh dữ liệu huấn luyện giả lập nếu chưa có dữ liệu thực.
    Trong thực tế, dữ liệu sẽ được lấy từ Azure SQL Database.
    """
    np.random.seed(42)

    regions = ["North", "South", "East", "West", "Central"]
    categories = ["Electronics", "Clothing", "Home", "Accessories"]
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # Tạo dữ liệu theo thời gian
    start_date = datetime(2025, 1, 1)
    dates = [start_date + timedelta(hours=i) for i in range(n_samples)]

    data = []
    for dt in dates:
        region = np.random.choice(regions)
        category = np.random.choice(categories)
        day_name = dt.strftime("%A")
        hour = dt.hour
        month = dt.month
        day_of_month = dt.day

        # Yếu tố ảnh hưởng doanh thu
        # - Cuối tuần bán nhiều hơn
        weekend_factor = 1.3 if day_name in ["Saturday", "Sunday"] else 1.0
        # - Giờ cao điểm (10-14h, 18-21h)
        if 10 <= hour <= 14 or 18 <= hour <= 21:
            hour_factor = 1.5
        elif 6 <= hour <= 9 or 15 <= hour <= 17:
            hour_factor = 1.0
        else:
            hour_factor = 0.3
        # - Mùa mua sắm (tháng 11-12)
        season_factor = 1.5 if month in [11, 12] else (1.2 if month in [6, 7] else 1.0)
        # - Vùng
        region_factors = {"North": 0.8, "South": 1.2, "East": 1.0, "West": 1.1, "Central": 0.9}
        region_factor = region_factors[region]
        # - Danh mục
        category_factors = {"Electronics": 1.5, "Clothing": 1.0, "Home": 0.8, "Accessories": 0.6}
        category_factor = category_factors[category]

        # Thời tiết (giả lập)
        temperature = 15 + 15 * np.sin(2 * np.pi * (month - 1) / 12) + np.random.normal(0, 3)
        humidity = 50 + np.random.normal(0, 15)
        is_rainy = np.random.random() < 0.3
        weather_factor = 0.8 if is_rainy else 1.0

        base_revenue = 500
        revenue = (base_revenue * weekend_factor * hour_factor * season_factor
                   * region_factor * category_factor * weather_factor
                   + np.random.normal(0, 100))
        revenue = max(0, revenue)

        quantity = max(1, int(revenue / (50 + np.random.normal(0, 10))))

        data.append({
            "date": dt.strftime("%Y-%m-%d"),
            "hour": hour,
            "day_of_week": day_name,
            "day_of_month": day_of_month,
            "month": month,
            "is_weekend": 1 if day_name in ["Saturday", "Sunday"] else 0,
            "region": region,
            "category": category,
            "temperature": round(temperature, 1),
            "humidity": round(humidity, 1),
            "is_rainy": int(is_rainy),
            "quantity": quantity,
            "revenue": round(revenue, 2),
        })

    return pd.DataFrame(data)


def prepare_features(df: pd.DataFrame) -> tuple:
    """Chuẩn bị features cho mô hình."""
    # Encode categorical features
    label_encoders = {}
    categorical_cols = ["day_of_week", "region", "category"]

    df_encoded = df.copy()
    for col in categorical_cols:
        le = LabelEncoder()
        df_encoded[col + "_encoded"] = le.fit_transform(df_encoded[col])
        label_encoders[col] = le

    # Tạo cyclic features cho giờ và tháng
    df_encoded["hour_sin"] = np.sin(2 * np.pi * df_encoded["hour"] / 24)
    df_encoded["hour_cos"] = np.cos(2 * np.pi * df_encoded["hour"] / 24)
    df_encoded["month_sin"] = np.sin(2 * np.pi * df_encoded["month"] / 12)
    df_encoded["month_cos"] = np.cos(2 * np.pi * df_encoded["month"] / 12)

    feature_cols = [
        "hour", "day_of_month", "month", "is_weekend",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
        "day_of_week_encoded", "region_encoded", "category_encoded",
        "temperature", "humidity", "is_rainy",
    ]

    X = df_encoded[feature_cols]
    y_quantity = df_encoded["quantity"]
    y_revenue = df_encoded["revenue"]

    return X, y_quantity, y_revenue, label_encoders, feature_cols


def train_model(X, y, model_name: str = "revenue"):
    """Huấn luyện mô hình Gradient Boosting."""
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
    )

    print(f"\n[INFO] Huấn luyện mô hình {model_name}...")
    model.fit(X_train, y_train)

    # Đánh giá
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    # Cross-validation
    cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')

    metrics = {
        "model_name": model_name,
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2_score": round(r2, 4),
        "cv_r2_mean": round(cv_scores.mean(), 4),
        "cv_r2_std": round(cv_scores.std(), 4),
    }

    print(f"  MAE:      {mae:.4f}")
    print(f"  RMSE:     {rmse:.4f}")
    print(f"  R² Score: {r2:.4f}")
    print(f"  CV R² :   {cv_scores.mean():.4f} (+/- {cv_scores.std():.4f})")

    # Feature importance
    feature_importance = dict(zip(X.columns, model.feature_importances_))
    sorted_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
    print(f"\n  Top 5 Features quan trọng nhất ({model_name}):")
    for feat, imp in sorted_features[:5]:
        print(f"    {feat}: {imp:.4f}")

    return model, metrics


def main():
    parser = argparse.ArgumentParser(description="Train Sales Forecasting Model")
    parser.add_argument("--data-path", type=str, default=None, help="Path to training data CSV")
    parser.add_argument("--output-dir", type=str, default="./model_output", help="Output directory")
    parser.add_argument("--n-samples", type=int, default=50000, help="Number of samples to generate")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Azure ML Run context
    run = None
    if AZUREML_AVAILABLE:
        try:
            run = Run.get_context()
            if hasattr(run, 'experiment'):
                print(f"[INFO] Azure ML Experiment: {run.experiment.name}")
        except Exception:
            run = None

    # Load hoặc sinh dữ liệu
    if args.data_path and os.path.exists(args.data_path):
        print(f"[INFO] Đọc dữ liệu từ {args.data_path}")
        df = pd.read_csv(args.data_path)
    else:
        print(f"[INFO] Sinh dữ liệu huấn luyện giả lập ({args.n_samples} mẫu)...")
        df = generate_training_data(args.n_samples)
        # Lưu dữ liệu
        data_path = os.path.join(args.output_dir, "training_data.csv")
        df.to_csv(data_path, index=False)
        print(f"[INFO] Đã lưu dữ liệu tại {data_path}")

    print(f"[INFO] Shape: {df.shape}")
    print(f"[INFO] Columns: {list(df.columns)}")

    # Chuẩn bị features
    X, y_quantity, y_revenue, label_encoders, feature_cols = prepare_features(df)

    # Huấn luyện mô hình dự đoán doanh thu
    revenue_model, revenue_metrics = train_model(X, y_revenue, "revenue")

    # Huấn luyện mô hình dự đoán số lượng
    quantity_model, quantity_metrics = train_model(X, y_quantity, "quantity")

    # Lưu mô hình
    revenue_model_path = os.path.join(args.output_dir, "revenue_model.pkl")
    quantity_model_path = os.path.join(args.output_dir, "quantity_model.pkl")
    encoders_path = os.path.join(args.output_dir, "label_encoders.pkl")
    metadata_path = os.path.join(args.output_dir, "model_metadata.json")

    joblib.dump(revenue_model, revenue_model_path)
    joblib.dump(quantity_model, quantity_model_path)
    joblib.dump(label_encoders, encoders_path)

    metadata = {
        "feature_columns": feature_cols,
        "revenue_metrics": revenue_metrics,
        "quantity_metrics": quantity_metrics,
        "training_samples": len(df),
        "trained_at": datetime.utcnow().isoformat(),
        "model_version": "v1.0",
    }
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\n[INFO] Mô hình đã lưu tại: {args.output_dir}")
    print(f"  - {revenue_model_path}")
    print(f"  - {quantity_model_path}")
    print(f"  - {encoders_path}")
    print(f"  - {metadata_path}")

    # Log metrics lên Azure ML (nếu có)
    if run and hasattr(run, 'log'):
        for key, value in revenue_metrics.items():
            if isinstance(value, (int, float)):
                run.log(f"revenue_{key}", value)
        for key, value in quantity_metrics.items():
            if isinstance(value, (int, float)):
                run.log(f"quantity_{key}", value)
        run.complete()
        print("[INFO] Đã log metrics lên Azure ML.")


if __name__ == "__main__":
    main()
