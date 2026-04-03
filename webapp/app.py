"""
Flask web app – Sales prediction via Azure ML endpoint.
Routes: /, /predict (form), /api/predict (JSON), /api/health
"""

import os
import sys
import json
import math
import subprocess
import threading
import requests
import numpy as np
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

# ── Product / store catalog – lấy từ config.settings (single source of truth) ──
try:
    from config.settings import PRODUCTS as _CFG_PRODUCTS, STORE_IDS, STORE_LOCATIONS
    STORES = [{"id": sid, "name": STORE_LOCATIONS[sid]["name"]} for sid in STORE_IDS]
    PRODUCTS = [{"id": p["id"], "name": p["name"], "category": p["category"],
                 "base_price": p.get("base_price", 0)} for p in _CFG_PRODUCTS]
except ImportError:
    STORES = [
        {"id": "S01", "name": "Hồ Chí Minh"},
        {"id": "S02", "name": "Hà Nội"},
        {"id": "S03", "name": "Đà Nẵng"},
    ]
    PRODUCTS = [
        {"id": "COKE",  "name": "Coca-Cola",      "category": "Beverage",         "base_price": 1.50},
        {"id": "PEPSI", "name": "Pepsi",           "category": "Beverage",         "base_price": 1.40},
        {"id": "BREAD", "name": "Bread",           "category": "Bakery",           "base_price": 1.15},
        {"id": "MILK",  "name": "Milk",            "category": "Dairy",            "base_price": 1.60},
        {"id": "P001",  "name": "Laptop",          "category": "Electronics",      "base_price": 999.99},
        {"id": "P002",  "name": "Smartphone",      "category": "Electronics",      "base_price": 699.99},
        {"id": "P003",  "name": "Headphones",      "category": "Electronics",      "base_price": 149.99},
        {"id": "P004",  "name": "Tablet",          "category": "Electronics",      "base_price": 499.99},
        {"id": "P005",  "name": "Smart Watch",     "category": "Electronics",      "base_price": 299.99},
        {"id": "P006",  "name": "T-Shirt",         "category": "Clothing",         "base_price": 29.99},
        {"id": "P007",  "name": "Jeans",           "category": "Clothing",         "base_price": 59.99},
        {"id": "P008",  "name": "Sneakers",        "category": "Clothing",         "base_price": 89.99},
        {"id": "P009",  "name": "Coffee Maker",    "category": "Home",             "base_price": 79.99},
        {"id": "P010",  "name": "Blender",         "category": "Home",             "base_price": 49.99},
        {"id": "P011",  "name": "Desk Lamp",       "category": "Home",             "base_price": 34.99},
        {"id": "P012",  "name": "Backpack",        "category": "Accessories",      "base_price": 45.99},
        {"id": "P013",  "name": "Sunglasses",      "category": "Accessories",      "base_price": 129.99},
        {"id": "P014",  "name": "Wireless Mouse",  "category": "Electronics",      "base_price": 39.99},
        {"id": "P015",  "name": "Keyboard",        "category": "Electronics",      "base_price": 69.99},
        {"id": "P016",  "name": "Nước ép cam",     "category": "Beverage",         "base_price": 2.50},
        {"id": "P017",  "name": "Trà xanh",        "category": "Beverage",         "base_price": 1.80},
        {"id": "P018",  "name": "Bánh mì sandwich","category": "Bakery",           "base_price": 3.50},
        {"id": "P019",  "name": "Sữa chua",        "category": "Dairy",            "base_price": 1.20},
        {"id": "P020",  "name": "Phô mai",         "category": "Dairy",            "base_price": 4.99},
        {"id": "P021",  "name": "Khoai tây chiên", "category": "Snacks",           "base_price": 1.99},
        {"id": "P022",  "name": "Socola",          "category": "Snacks",           "base_price": 3.49},
        {"id": "P023",  "name": "Bánh quy",        "category": "Snacks",           "base_price": 2.29},
        {"id": "P024",  "name": "Kem chống nắng",  "category": "Health & Beauty",  "base_price": 12.99},
        {"id": "P025",  "name": "Dầu gội",         "category": "Health & Beauty",  "base_price": 7.99},
        {"id": "P026",  "name": "Kem đánh răng",   "category": "Health & Beauty",  "base_price": 3.49},
        {"id": "P027",  "name": "Bóng đá",         "category": "Sports",           "base_price": 24.99},
        {"id": "P028",  "name": "Bình nước thể thao","category": "Sports",         "base_price": 14.99},
        {"id": "P029",  "name": "Sổ tay",          "category": "Stationery",       "base_price": 5.99},
        {"id": "P030",  "name": "Bút bi",          "category": "Stationery",       "base_price": 1.49},
        {"id": "P031",  "name": "Rubik",           "category": "Toys",             "base_price": 8.99},
    ]
PRODUCT_MAP = {p["id"]: p for p in PRODUCTS}

# ── Local model loading (score locally using pkl files) ──────────
_MODEL_DIR = os.path.join(_base_dir, "..", "ml", "model_output")
_local_models = {"revenue": None, "quantity": None, "encoders": None, "metadata": None}


def _load_local_models():
    """Load pkl models from ml/model_output/ for local scoring."""
    import joblib
    meta_path = os.path.join(_MODEL_DIR, "model_metadata.json")
    rev_path = os.path.join(_MODEL_DIR, "revenue_model.pkl")
    qty_path = os.path.join(_MODEL_DIR, "quantity_model.pkl")
    enc_path = os.path.join(_MODEL_DIR, "label_encoders.pkl")

    if not all(os.path.exists(p) for p in [meta_path, rev_path, qty_path, enc_path]):
        return False

    _local_models["revenue"] = joblib.load(rev_path)
    _local_models["quantity"] = joblib.load(qty_path)
    _local_models["encoders"] = joblib.load(enc_path)
    with open(meta_path, "r", encoding="utf-8") as f:
        _local_models["metadata"] = json.load(f)
    print(f"[INFO] Local models loaded: {_local_models['metadata'].get('model_version', '?')}")
    return True


def _score_local(input_data: dict) -> dict | None:
    """Score using local pkl models (same logic as ml/score.py)."""
    if not _local_models["metadata"]:
        if not _load_local_models():
            return None

    import pandas as pd
    meta = _local_models["metadata"]
    le_map = _local_models["encoders"]

    df = pd.DataFrame([input_data])

    cat_cols = meta.get("categorical_columns", ["store_id", "product_id", "category"])
    for col in cat_cols:
        if col in df.columns and col in le_map:
            le = le_map[col]
            df[col + "_enc"] = df[col].apply(
                lambda x, _le=le: _le.transform([x])[0] if x in _le.classes_ else -1
            )

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    feature_cols = meta.get("feature_columns", [])
    X = df[feature_cols]

    pred_rev = float(_local_models["revenue"].predict(X)[0])
    pred_qty = int(round(float(_local_models["quantity"].predict(X)[0])))

    rev_rmse = meta.get("revenue_metrics", {}).get("rmse", 10)
    qty_rmse = meta.get("quantity_metrics", {}).get("rmse", 2)

    return {
        "status": "success",
        "prediction": {
            "predicted_revenue": round(pred_rev, 2),
            "predicted_quantity": max(1, pred_qty),
            "confidence_interval": {
                "lower": round(pred_rev - 1.96 * rev_rmse, 2),
                "upper": round(pred_rev + 1.96 * rev_rmse, 2),
                "quantity_lower": max(0, int(round(pred_qty - 1.96 * qty_rmse))),
                "quantity_upper": int(round(pred_qty + 1.96 * qty_rmse)),
            },
        },
        "source": "Local Model (" + meta.get("model_version", "?") + ")",
        "model_version": meta.get("model_version", "unknown"),
    }


def call_ml_endpoint(input_data: dict) -> dict:
    """Call Azure ML Online Endpoint → fallback local models → fallback mock."""
    # 1) Try Azure ML endpoint
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
            if isinstance(body, str):
                body = json.loads(body)
            pred = body.get("predictions", [{}])[0]
            ci_raw = pred.get("confidence_interval", {}) or {}
            ci = {
                "lower": ci_raw.get("lower", ci_raw.get("revenue_lower", 0)),
                "upper": ci_raw.get("upper", ci_raw.get("revenue_upper", 0)),
                "quantity_lower": ci_raw.get("quantity_lower"),
                "quantity_upper": ci_raw.get("quantity_upper"),
            }
            return {
                "status": "success",
                "prediction": {
                    "predicted_revenue": pred.get("predicted_revenue", 0),
                    "predicted_quantity": pred.get("predicted_quantity", 0),
                    "confidence_interval": ci,
                },
                "source": "Azure ML Endpoint",
            }
        except Exception:
            pass  # Fall through to local model

    # 2) Try local pkl models
    try:
        local_result = _score_local(input_data)
        if local_result:
            return local_result
    except Exception as e:
        print(f"[WARN] Local scoring failed: {e}")

    # 3) Fallback mock
    import random
    base = PRODUCT_MAP.get(input_data.get("product_id"), {}).get("base_price", 50)
    rev = base * random.uniform(0.8, 1.5)
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
        "source": "Mock (no model available)",
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
            "product_name": product.get("name", product_id),
            "category": product["category"],
            "base_price": product.get("base_price", 0),
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


def _get_report_dir():
    """Return the first existing retrain_comparison directory."""
    candidates = [
        os.path.join(_base_dir, "..", "ml", "model_output", "retrain_comparison"),
        os.path.join(_base_dir, "ml", "model_output", "retrain_comparison"),
    ]
    for c in candidates:
        if os.path.isdir(c):
            return os.path.abspath(c)
    return None


@app.route("/model-report-image/<path:filename>")
def model_report_image(filename):
    """Serve retrain chart images dynamically from the comparison directory."""
    from flask import send_from_directory, abort
    import re
    if not re.match(r'^[\w\-]+\.png$', filename):
        abort(404)
    report_dir = _get_report_dir()
    if not report_dir:
        abort(404)
    return send_from_directory(report_dir, filename)


@app.route("/model-report")
def model_report():
    """Show retrain report from latest run plus previous-run comparison."""
    report_dir = _get_report_dir()

    report = None
    chart_files = []
    history = []
    latest_run = None
    previous_run = None

    if report_dir:
        report_file = os.path.join(report_dir, "comparison_report.json")
        if os.path.exists(report_file):
            with open(report_file, encoding="utf-8") as f:
                report = json.load(f)

        # Collect chart PNGs ordered by a preferred display sequence
        preferred = [
            "retrain_summary_dashboard.png",
            "improvement_waterfall.png",
            "revenue_metrics_comparison.png",
            "quantity_metrics_comparison.png",
            "revenue_actual_vs_predicted_comparison.png",
            "quantity_actual_vs_predicted_comparison.png",
            "revenue_residual_comparison.png",
            "quantity_residual_comparison.png",
        ]
        existing = {f for f in os.listdir(report_dir) if f.lower().endswith(".png")}
        chart_files = [f for f in preferred if f in existing]
        # Append any remaining PNGs not in preferred list
        chart_files += sorted(existing - set(chart_files))

        history_index_file = os.path.join(_base_dir, "..", "ml", "model_output", "retrain_history", "history_index.json")
        if os.path.exists(history_index_file):
            with open(history_index_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, list):
                    history = raw
                    if len(history) >= 1:
                        latest_run = history[-1]
                    if len(history) >= 2:
                        previous_run = history[-2]

    return render_template(
        "model_report.html",
        report=report,
        chart_files=chart_files,
        latest_run=latest_run,
        previous_run=previous_run,
        history_count=len(history),
    )


# ── Retrain routes ────────────────────────────────────────────────
_retrain_lock = threading.Lock()
_retrain_status = {"running": False, "last_result": None}


def _run_retrain(n_samples: int, n_estimators: int, max_depth: int, lr: float, promote: bool):
    """Run retrain_and_compare.py in a subprocess."""
    try:
        script = os.path.join(_base_dir, "..", "ml", "retrain_and_compare.py")
        cmd = [
            sys.executable, script,
            "--new-samples", str(n_samples),
            "--new-estimators", str(n_estimators),
            "--new-depth", str(max_depth),
            "--new-lr", str(lr),
        ]
        if promote:
            cmd.append("--promote")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600,
                                cwd=os.path.join(_base_dir, ".."),
                                env={**os.environ, "PYTHONIOENCODING": "utf-8"})
        _retrain_status["last_result"] = {
            "success": result.returncode == 0,
            "stdout": result.stdout[-5000:] if result.stdout else "",
            "stderr": result.stderr[-2000:] if result.stderr else "",
            "timestamp": datetime.now().isoformat(),
        }
        # Reload local models after retrain
        if result.returncode == 0:
            _local_models["metadata"] = None  # force reload on next predict
    except Exception as e:
        _retrain_status["last_result"] = {
            "success": False,
            "stdout": "",
            "stderr": str(e),
            "timestamp": datetime.now().isoformat(),
        }
    finally:
        _retrain_status["running"] = False


@app.route("/retrain", methods=["GET", "POST"])
def retrain():
    """UI page to trigger retrain locally."""
    if request.method == "GET":
        return render_template("retrain.html",
                               status=_retrain_status,
                               running=_retrain_status["running"])

    # POST – kick off retrain
    if _retrain_status["running"]:
        return render_template("retrain.html",
                               status=_retrain_status,
                               running=True,
                               message="Retrain đang chạy, vui lòng chờ...")

    n_samples = int(request.form.get("n_samples", 80000))
    n_estimators = int(request.form.get("n_estimators", 300))
    max_depth = int(request.form.get("max_depth", 6))
    lr = float(request.form.get("learning_rate", 0.1))
    promote = request.form.get("promote") == "1"

    _retrain_status["running"] = True
    _retrain_status["last_result"] = None

    t = threading.Thread(target=_run_retrain,
                         args=(n_samples, n_estimators, max_depth, lr, promote),
                         daemon=True)
    t.start()

    return render_template("retrain.html",
                           status=_retrain_status,
                           running=True,
                           message="Retrain đã bắt đầu! Trang sẽ tự cập nhật...")


@app.route("/api/retrain", methods=["POST"])
def api_retrain():
    """JSON API to trigger retrain."""
    if _retrain_status["running"]:
        return jsonify({"status": "busy", "message": "Retrain is already running"}), 409

    data = request.get_json() or {}
    n_samples = int(data.get("n_samples", 80000))
    n_estimators = int(data.get("n_estimators", 300))
    max_depth = int(data.get("max_depth", 6))
    lr = float(data.get("learning_rate", 0.1))
    promote = bool(data.get("promote", True))

    _retrain_status["running"] = True
    _retrain_status["last_result"] = None

    t = threading.Thread(target=_run_retrain,
                         args=(n_samples, n_estimators, max_depth, lr, promote),
                         daemon=True)
    t.start()

    return jsonify({"status": "started", "message": "Retrain triggered"})


@app.route("/api/retrain/status")
def api_retrain_status():
    """Poll retrain progress."""
    return jsonify({
        "running": _retrain_status["running"],
        "last_result": _retrain_status["last_result"],
    })


if __name__ == "__main__":
    print("=" * 50)
    print("  Sales Prediction Web App")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(
        debug=os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true"),
        port=int(os.getenv("FLASK_PORT", "5000")),
    )
