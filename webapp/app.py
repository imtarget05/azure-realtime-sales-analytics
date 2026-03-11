"""
Mục 4 (Phân tích).4 Rubric: Web App gọi ML API.
- Flask web app cho phép nhập tham số
- Gọi Azure ML Online Endpoint
- Hiển thị kết quả dự đoán
- Trực quan hóa kết quả
"""

import os
import sys
import json
import requests
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import Flask, render_template, request, jsonify

from config.settings import (
    ML_ENDPOINT_URL, ML_API_KEY,
    PRODUCTS, REGIONS, CUSTOMER_SEGMENTS, PAYMENT_METHODS,
)

app = Flask(__name__)


def call_ml_endpoint(input_data: dict) -> dict:
    """
    Gọi Azure ML Online Endpoint để dự đoán doanh thu.
    Trả về kết quả dự đoán hoặc mock data nếu endpoint chưa sẵn sàng.
    """
    if ML_ENDPOINT_URL and ML_API_KEY:
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {ML_API_KEY}",
            }
            payload = {"input_data": {"data": [input_data]}}
            response = requests.post(ML_ENDPOINT_URL, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return {"status": "success", "prediction": response.json(), "source": "Azure ML Endpoint"}
        except Exception as e:
            return {"status": "error", "message": str(e), "source": "Azure ML Endpoint"}

    # Mock prediction khi chưa có endpoint
    import random
    base = input_data.get("base_price", 50) * input_data.get("quantity", 1)
    discount = base * input_data.get("discount_percent", 0) / 100
    predicted = base - discount + random.uniform(-10, 20)
    return {
        "status": "success",
        "prediction": {
            "predicted_revenue": round(predicted, 2),
            "confidence_interval": {
                "lower": round(predicted * 0.85, 2),
                "upper": round(predicted * 1.15, 2),
            },
            "predicted_quantity": input_data.get("quantity", 1),
        },
        "source": "Mock (endpoint chưa được cấu hình)",
    }


@app.route("/")
def index():
    """Trang chủ - form nhập tham số."""
    return render_template("index.html",
                           products=PRODUCTS,
                           regions=REGIONS,
                           segments=CUSTOMER_SEGMENTS,
                           payment_methods=PAYMENT_METHODS)


@app.route("/predict", methods=["POST"])
def predict():
    """API endpoint nhận form data, gọi ML API, trả kết quả."""
    try:
        data = {
            "hour": int(request.form.get("hour", 12)),
            "day_of_week": int(request.form.get("day_of_week", 1)),
            "month": int(request.form.get("month", 6)),
            "is_weekend": 1 if int(request.form.get("day_of_week", 1)) >= 5 else 0,
            "is_online": int(request.form.get("is_online", 0)),
            "category_id": int(request.form.get("category_id", 0)),
            "region_id": int(request.form.get("region_id", 0)),
            "product_id": int(request.form.get("product_id", 0)),
            "base_price": float(request.form.get("base_price", 50)),
            "discount_percent": int(request.form.get("discount_percent", 0)),
            "quantity": int(request.form.get("quantity", 1)),
        }
        import math
        data["hour_sin"] = round(math.sin(2 * math.pi * data["hour"] / 24), 6)
        data["hour_cos"] = round(math.cos(2 * math.pi * data["hour"] / 24), 6)
        data["month_sin"] = round(math.sin(2 * math.pi * data["month"] / 12), 6)
        data["month_cos"] = round(math.cos(2 * math.pi * data["month"] / 12), 6)

        result = call_ml_endpoint(data)

        return render_template("result.html", input_data=data, result=result)
    except Exception as e:
        return render_template("result.html", input_data={}, result={"status": "error", "message": str(e)})


@app.route("/api/predict", methods=["POST"])
def api_predict():
    """REST API endpoint (JSON in → JSON out)."""
    try:
        data = request.get_json()
        result = call_ml_endpoint(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/api/health")
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "ml_endpoint_configured": bool(ML_ENDPOINT_URL),
    })


if __name__ == "__main__":
    print("=" * 50)
    print("  Sales Prediction Web App")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(debug=True, port=5000)
