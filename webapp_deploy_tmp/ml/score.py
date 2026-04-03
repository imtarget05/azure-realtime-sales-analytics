"""
Scoring script for Azure ML Online Endpoint.
Features aligned with train_model.py:
  hour, day_of_month, month, is_weekend, store_id, product_id, category,
  temperature, is_rainy, holiday
"""

import os
import json
import joblib
import numpy as np
import pandas as pd


def init():
    """Load models on endpoint startup."""
    global revenue_model, quantity_model, label_encoders, metadata

    model_root = os.getenv("AZUREML_MODEL_DIR", "./model_output")
    model_dir = model_root

    # Registry model artifacts may be packed in a nested `model_output/` folder.
    if not os.path.exists(os.path.join(model_dir, "revenue_model.pkl")):
        nested_dir = os.path.join(model_root, "model_output")
        if os.path.exists(os.path.join(nested_dir, "revenue_model.pkl")):
            model_dir = nested_dir

    revenue_model = joblib.load(os.path.join(model_dir, "revenue_model.pkl"))
    quantity_model = joblib.load(os.path.join(model_dir, "quantity_model.pkl"))
    label_encoders = joblib.load(os.path.join(model_dir, "label_encoders.pkl"))

    with open(os.path.join(model_dir, "model_metadata.json"), "r") as f:
        metadata = json.load(f)

    print(f"[INFO] Models loaded. Version: {metadata.get('model_version', 'unknown')}")


def run(raw_data: str) -> str:
    """
    Input JSON:
    {
      "data": [
        {
          "hour": 14,
          "day_of_month": 15,
          "month": 6,
          "is_weekend": 0,
          "store_id": "S01",
          "product_id": "COKE",
          "category": "Beverage",
          "temperature": 28.0,
          "is_rainy": 0,
          "holiday": 0
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

        # Encode categoricals
        cat_cols = metadata.get("categorical_columns", ["store_id", "product_id", "category"])
        for col in cat_cols:
            if col in df.columns and col in label_encoders:
                le = label_encoders[col]
                df[col + "_enc"] = df[col].apply(
                    lambda x: le.transform([x])[0] if x in le.classes_ else -1
                )

        # Cyclic features
        df["hour_sin"]  = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"]  = np.cos(2 * np.pi * df["hour"] / 24)
        df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
        df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

        feature_cols = metadata.get("feature_columns", [])
        X = df[feature_cols]

        pred_rev = revenue_model.predict(X)
        pred_qty = quantity_model.predict(X)

        rev_rmse = metadata.get("revenue_metrics", {}).get("rmse", 10)
        qty_rmse = metadata.get("quantity_metrics", {}).get("rmse", 2)

        results = []
        for i in range(len(records)):
            results.append({
                "input": records[i],
                "predicted_revenue":  round(float(pred_rev[i]), 2),
                "predicted_quantity": max(1, int(round(pred_qty[i]))),
                "confidence_interval": {
                    "revenue_lower":  round(float(pred_rev[i] - 1.96 * rev_rmse), 2),
                    "revenue_upper":  round(float(pred_rev[i] + 1.96 * rev_rmse), 2),
                    "quantity_lower": max(0, int(round(pred_qty[i] - 1.96 * qty_rmse))),
                    "quantity_upper": int(round(pred_qty[i] + 1.96 * qty_rmse)),
                },
                "model_version": metadata.get("model_version", "unknown"),
            })

        return json.dumps({"predictions": results})

    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    init()

    test_input = json.dumps({
        "data": [
            {
                "hour": 14, "day_of_month": 15, "month": 3,
                "is_weekend": 0, "store_id": "S01", "product_id": "COKE",
                "category": "Beverage", "temperature": 28.0,
                "is_rainy": 0, "holiday": 0
            },
            {
                "hour": 20, "day_of_month": 15, "month": 12,
                "is_weekend": 1, "store_id": "S02", "product_id": "MILK",
                "category": "Dairy", "temperature": 30.0,
                "is_rainy": 1, "holiday": 1
            }
        ]
    })

    result = run(test_input)
    print(json.dumps(json.loads(result), indent=2))
