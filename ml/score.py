"""
Scoring script cho Azure ML Online Endpoint.
Nhận request dự đoán doanh thu/số lượng bán hàng và trả kết quả.
"""

import os
import json
import joblib
import numpy as np
import pandas as pd


def init():
    """Khởi tạo mô hình khi endpoint được deploy."""
    global revenue_model, quantity_model, label_encoders, metadata

    model_dir = os.getenv("AZUREML_MODEL_DIR", "./model_output")

    revenue_model = joblib.load(os.path.join(model_dir, "revenue_model.pkl"))
    quantity_model = joblib.load(os.path.join(model_dir, "quantity_model.pkl"))
    label_encoders = joblib.load(os.path.join(model_dir, "label_encoders.pkl"))

    with open(os.path.join(model_dir, "model_metadata.json"), "r") as f:
        metadata = json.load(f)

    print(f"[INFO] Models loaded. Version: {metadata.get('model_version', 'unknown')}")


def run(raw_data: str) -> str:
    """
    Xử lý request dự đoán.
    
    Input JSON format:
    {
        "data": [
            {
                "hour": 14,
                "day_of_month": 15,
                "month": 3,
                "day_of_week": "Monday",
                "is_weekend": 0,
                "region": "North",
                "category": "Electronics",
                "temperature": 22.5,
                "humidity": 65.0,
                "is_rainy": 0
            }
        ]
    }
    """
    try:
        input_data = json.loads(raw_data)
        records = input_data.get("data", [])

        if not records:
            return json.dumps({"error": "No data provided"})

        df = pd.DataFrame(records)

        # Encode categorical features
        for col in ["day_of_week", "region", "category"]:
            if col in df.columns and col in label_encoders:
                le = label_encoders[col]
                df[col + "_encoded"] = df[col].apply(
                    lambda x: le.transform([x])[0] if x in le.classes_ else -1
                )

        # Cyclic features
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

        feature_cols = metadata.get("feature_columns", [])
        X = df[feature_cols]

        # Dự đoán
        predicted_revenue = revenue_model.predict(X)
        predicted_quantity = quantity_model.predict(X)

        # Tính khoảng tin cậy (dựa trên RMSE từ training)
        revenue_rmse = metadata.get("revenue_metrics", {}).get("rmse", 50)
        quantity_rmse = metadata.get("quantity_metrics", {}).get("rmse", 2)

        results = []
        for i in range(len(records)):
            result = {
                "input": records[i],
                "predicted_revenue": round(float(predicted_revenue[i]), 2),
                "predicted_quantity": max(1, int(round(predicted_quantity[i]))),
                "confidence_interval": {
                    "revenue_lower": round(float(predicted_revenue[i] - 1.96 * revenue_rmse), 2),
                    "revenue_upper": round(float(predicted_revenue[i] + 1.96 * revenue_rmse), 2),
                    "quantity_lower": max(0, int(round(predicted_quantity[i] - 1.96 * quantity_rmse))),
                    "quantity_upper": int(round(predicted_quantity[i] + 1.96 * quantity_rmse)),
                },
                "model_version": metadata.get("model_version", "unknown"),
            }
            results.append(result)

        return json.dumps({"predictions": results})

    except Exception as e:
        return json.dumps({"error": str(e)})


# Cho phép test local
if __name__ == "__main__":
    init()

    test_input = json.dumps({
        "data": [
            {
                "hour": 14,
                "day_of_month": 15,
                "month": 3,
                "day_of_week": "Monday",
                "is_weekend": 0,
                "region": "North",
                "category": "Electronics",
                "temperature": 22.5,
                "humidity": 65.0,
                "is_rainy": 0
            },
            {
                "hour": 20,
                "day_of_month": 15,
                "month": 12,
                "day_of_week": "Saturday",
                "is_weekend": 1,
                "region": "South",
                "category": "Clothing",
                "temperature": 30.0,
                "humidity": 70.0,
                "is_rainy": 1
            }
        ]
    })

    result = run(test_input)
    print(json.dumps(json.loads(result), indent=2))
