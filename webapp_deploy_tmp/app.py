"""
Flask web app – Sales prediction via Azure ML endpoint.
Routes: /, /predict (form), /api/predict (JSON), /api/health
"""

import os
import sys
import json
import math
import requests
from datetime import datetime

from flask import Flask, render_template, request, jsonify

# Read ML config directly from env vars (works both locally and on Azure App Service)
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from config.settings import ML_ENDPOINT_URL, ML_API_KEY
except ImportError:
    from dotenv import load_dotenv
    load_dotenv()
    ML_ENDPOINT_URL = os.getenv("AML_ENDPOINT_URL", "")
    ML_API_KEY = os.getenv("AML_API_KEY", "")

_base_dir = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__,
            template_folder=os.path.join(_base_dir, "templates"),
            static_folder=os.path.join(_base_dir, "static"))

# ── Product / store catalog (mirrors config.settings) ──
STORES = [
    {"id": "S01", "name": "Ho Chi Minh City"},
    {"id": "S02", "name": "Ha Noi"},
    {"id": "S03", "name": "Da Nang"},
]
PRODUCTS = [
    {"id": "COKE",  "name": "Coca-Cola", "category": "Beverage"},
    {"id": "PEPSI", "name": "Pepsi",     "category": "Beverage"},
    {"id": "BREAD", "name": "Bread",     "category": "Bakery"},
    {"id": "MILK",  "name": "Milk",      "category": "Dairy"},
]
PRODUCT_MAP = {p["id"]: p for p in PRODUCTS}


def call_ml_endpoint(input_data: dict) -> dict:
    """Call Azure ML Online Endpoint. Falls back to mock if unconfigured."""
    if ML_ENDPOINT_URL and ML_API_KEY and not ML_ENDPOINT_URL.startswith("<"):
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {ML_API_KEY}",
            }
            payload = {"data": [input_data]}
            resp = requests.post(ML_ENDPOINT_URL, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            # AML endpoint double-serializes: run() returns json.dumps()
            # so resp.json() may return a string instead of dict
            if isinstance(body, str):
                body = json.loads(body)
            pred = body.get("predictions", [{}])[0]
            return {
                "status": "success",
                "prediction": {
                    "predicted_revenue": pred.get("predicted_revenue", 0),
                    "predicted_quantity": pred.get("predicted_quantity", 0),
                    "confidence_interval": pred.get("confidence_interval", {}),
                },
                "source": "Azure ML Endpoint",
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "source": "Azure ML Endpoint"}

    # Mock prediction
    import random
    rev = 40 + random.uniform(-10, 30)
    return {
        "status": "success",
        "prediction": {
            "predicted_revenue": round(rev, 2),
            "predicted_quantity": random.randint(5, 50),
            "confidence_interval": {
                "lower": round(rev * 0.8, 2),
                "upper": round(rev * 1.2, 2),
            },
        },
        "source": "Mock (endpoint not configured)",
    }


@app.route("/")
def index():
    return render_template("index.html", stores=STORES, products=PRODUCTS)


@app.route("/predict", methods=["POST"])
def predict():
    """Form POST → ML → result page."""
    try:
        hour = int(request.form.get("hour", 12))
        day_of_week = int(request.form.get("day_of_week", 2))
        month = int(request.form.get("month", 6))
        store_id = request.form.get("store_id", "S01")
        product_id = request.form.get("product_id", "COKE")
        temperature = float(request.form.get("temperature", 28))
        is_rainy = int(request.form.get("is_rainy", 0))
        holiday = int(request.form.get("holiday", 0))

        product = PRODUCT_MAP.get(product_id, PRODUCTS[0])

        data = {
            "hour": hour,
            "day_of_month": 15,
            "month": month,
            "is_weekend": 1 if day_of_week >= 5 else 0,
            "store_id": store_id,
            "product_id": product_id,
            "category": product["category"],
            "temperature": temperature,
            "is_rainy": is_rainy,
            "holiday": holiday,
        }

        result = call_ml_endpoint(data)
        return render_template("result.html", input_data=data, result=result)
    except Exception as e:
        return render_template("result.html", input_data={},
                               result={"status": "error", "message": str(e)})


@app.route("/api/predict", methods=["GET", "POST"])
def api_predict():
    """REST API (JSON in/out)."""
    if request.method == "GET":
        return jsonify({
            "usage": "POST JSON to this endpoint",
            "example": {
                "hour": 14, "day_of_month": 15, "month": 6,
                "is_weekend": 0, "store_id": "S01", "product_id": "COKE",
                "category": "Beverage", "temperature": 28,
                "is_rainy": 0, "holiday": 0,
            },
            "docs": "Send POST with Content-Type: application/json",
        })
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
        "ml_endpoint_configured": bool(ML_ENDPOINT_URL and not ML_ENDPOINT_URL.startswith("<")),
    })


@app.route("/model-report")
def model_report():
    """Show retrain comparison report with charts and metrics."""
    # Try multiple possible locations for the comparison report
    candidates = [
        os.path.join(_base_dir, "..", "ml", "model_output", "retrain_comparison"),
        os.path.join(_base_dir, "ml", "model_output", "retrain_comparison"),
    ]
    report_dir = None
    for c in candidates:
        if os.path.isdir(c):
            report_dir = c
            break

    report = None
    if report_dir:
        report_file = os.path.join(report_dir, "comparison_report.json")
        if os.path.exists(report_file):
            with open(report_file) as f:
                report = json.load(f)

    # Collect chart images (serve as base64 inline)
    import base64
    charts = {}
    if os.path.isdir(report_dir):
        for fname in sorted(os.listdir(report_dir)):
            if fname.endswith(".png"):
                with open(os.path.join(report_dir, fname), "rb") as img:
                    charts[fname] = base64.b64encode(img.read()).decode()

    return render_template("model_report.html", report=report, charts=charts)


if __name__ == "__main__":
    print("=" * 50)
    print("  Sales Prediction Web App")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(
        debug=os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true"),
        port=int(os.getenv("FLASK_PORT", "5000")),
    )
