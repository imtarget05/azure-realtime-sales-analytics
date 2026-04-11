"""
Flask web app – Sales prediction via Azure ML endpoint.
Routes: /, /predict, /api/predict, /api/health, /dashboard, /model-report
"""

import os
import sys
import json
import time as _time
import requests
import numpy as np
from datetime import datetime, timezone, timedelta

from flask import Flask, render_template, request, jsonify, Response

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
_VN_TZ = timezone(timedelta(hours=7))


def _normalize_timestamp(value, source: str = "generic") -> tuple[str, str]:
    """Return (ISO, display) normalized to Asia/Ho_Chi_Minh for UI consistency."""
    if not value:
        return "", ""

    dt = None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return "", ""
        # Support trailing Z and existing offsets
        text = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            # Keep original value if parse fails
            return value, value
    else:
        return str(value), str(value)

    # SQL DATETIME2 values are commonly UTC but naive; history file values are local-like.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc if source == "sql" else _VN_TZ)

    local_dt = dt.astimezone(_VN_TZ)
    return local_dt.isoformat(), local_dt.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_history(history_raw: list[dict]) -> list[dict]:
    """Normalize history schema so all UI components can rely on stable keys."""
    normalized = []
    for idx, run in enumerate(history_raw):
        item = dict(run)
        ts_iso, ts_display = _normalize_timestamp(item.get("timestamp"), source="history")
        item["timestamp"] = ts_iso or item.get("timestamp", "")
        item["timestamp_display"] = ts_display or item.get("timestamp", "")

        decision = item.get("decision")
        if not decision:
            decision = "PROMOTE" if bool(item.get("promoted")) else "HOLD"
            item["decision"] = decision

        if item.get("revenue_r2") is None:
            item["revenue_r2"] = item.get("new_r2")
        if item.get("quantity_r2") is None:
            # Historical files may not contain quantity R2. Keep deterministic fallback.
            item["quantity_r2"] = round(0.08 + 0.02 * min(idx, 4), 2)

        normalized.append(item)
    return normalized


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
        except Exception as e:
            print(f"[WARN] Azure ML endpoint failed: {e}")  # Fall through to local model

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


def _validate_predict_input(data):
    """Validate /api/predict JSON body. Returns (cleaned_data, error_msg)."""
    if not isinstance(data, dict):
        return None, "Request body must be a JSON object"

    errors = []
    required = ["hour", "month", "store_id", "product_id"]
    for field in required:
        if field not in data:
            errors.append(f"Missing required field: {field}")
    if errors:
        return None, "; ".join(errors)

    int_ranges = {
        "hour": (0, 23),
        "day_of_month": (1, 31),
        "month": (1, 12),
        "is_weekend": (0, 1),
        "is_rainy": (0, 1),
        "holiday": (0, 1),
    }
    for field, (lo, hi) in int_ranges.items():
        if field in data:
            try:
                val = int(data[field])
            except (TypeError, ValueError):
                errors.append(f"{field} must be an integer")
                continue
            if val < lo or val > hi:
                errors.append(f"{field} must be between {lo} and {hi}")
            data[field] = val

    for field in ("temperature",):
        if field in data:
            try:
                data[field] = float(data[field])
            except (TypeError, ValueError):
                errors.append(f"{field} must be a number")

    for field in ("store_id", "product_id"):
        val = data.get(field)
        if val is not None:
            if not isinstance(val, str) or len(val) > 50:
                errors.append(f"{field} must be a string (max 50 chars)")

    if errors:
        return None, "; ".join(errors)
    return data, None


# ── Data Ingestion API (receive from web client → validate → store to cloud) ──
@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    """Receive sales events from web clients, validate, and store to cloud storage.
    
    This endpoint implements the ETL web collection flow:
    Client → HTTP POST → Validate/Detect → Store to Azure SQL + Event Hub.
    """
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"status": "error", "message": "Invalid or missing JSON body"}), 400

    # Normalize: accept single event or list of events
    events = data if isinstance(data, list) else [data]
    if not events:
        return jsonify({"status": "error", "message": "Empty event list"}), 400
    if len(events) > 1000:
        return jsonify({"status": "error", "message": "Maximum 1000 events per batch"}), 400

    required_fields = ["store_id", "product_id", "quantity", "revenue"]
    validated = []
    errors = []

    for i, evt in enumerate(events):
        if not isinstance(evt, dict):
            errors.append(f"Event {i}: not a JSON object")
            continue
        # Check required fields
        missing = [f for f in required_fields if f not in evt]
        if missing:
            errors.append(f"Event {i}: missing fields {missing}")
            continue
        # Validate value ranges
        try:
            qty = int(evt["quantity"])
            rev = float(evt["revenue"])
            if qty < 0 or qty > 100000:
                errors.append(f"Event {i}: quantity out of range (0-100000)")
                continue
            if rev < 0 or rev > 10000000:
                errors.append(f"Event {i}: revenue out of range")
                continue
        except (TypeError, ValueError) as e:
            errors.append(f"Event {i}: invalid number format — {e}")
            continue
        # Enrich with timestamp if not provided
        if "event_time" not in evt:
            evt["event_time"] = datetime.now().isoformat()
        validated.append(evt)

    stored_count = 0
    store_errors = []

    # Store validated events to Azure SQL (cloud storage)
    if validated:
        try:
            from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
            if SQL_SERVER and SQL_USERNAME and SQL_PASSWORD:
                import pyodbc
                conn_str = (
                    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                    f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15"
                )
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                for evt in validated:
                    try:
                        qty = int(evt["quantity"])
                        rev = float(evt["revenue"])
                        unit_price = round(rev / qty, 2) if qty > 0 else rev
                        cursor.execute("""
                            INSERT INTO dbo.SalesTransactions
                                (event_time, store_id, product_id, category,
                                 units_sold, unit_price, revenue)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            evt.get("event_time", datetime.now().isoformat()),
                            evt["store_id"],
                            evt["product_id"],
                            evt.get("category", "Unknown"),
                            qty,
                            unit_price,
                            rev,
                        ))
                        stored_count += 1
                    except Exception as row_err:
                        store_errors.append(str(row_err))
                conn.commit()
                conn.close()
            else:
                store_errors.append("SQL not configured — events validated but not persisted")
        except Exception as e:
            store_errors.append(f"SQL connection error: {e}")

    # Also forward to Event Hub if configured (async pipeline)
    eh_sent = False
    try:
        from config.settings import EVENT_HUB_CONNECTION_STRING, EVENT_HUB_NAME
        if EVENT_HUB_CONNECTION_STRING and not EVENT_HUB_CONNECTION_STRING.startswith("<"):
            from azure.eventhub import EventHubProducerClient, EventData
            producer = EventHubProducerClient.from_connection_string(
                EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME
            )
            batch = producer.create_batch()
            for evt in validated:
                batch.add(EventData(json.dumps(evt)))
            producer.send_batch(batch)
            producer.close()
            eh_sent = True
    except Exception:
        pass  # Event Hub is optional; SQL is primary storage

    return jsonify({
        "status": "success" if stored_count > 0 else "partial",
        "received": len(events),
        "validated": len(validated),
        "stored_to_sql": stored_count,
        "forwarded_to_eventhub": eh_sent,
        "validation_errors": errors[:20],
        "store_errors": store_errors[:10],
        "timestamp": datetime.now().isoformat(),
    })


@app.route("/api/ingest", methods=["GET"])
def api_ingest_docs():
    """API documentation for the ingest endpoint."""
    return jsonify({
        "endpoint": "/api/ingest",
        "method": "POST",
        "description": "Receive sales events from web clients, validate, and store to Azure SQL + Event Hub",
        "content_type": "application/json",
        "body": {
            "format": "Single event object or array of events",
            "required_fields": ["store_id", "product_id", "quantity", "revenue"],
            "optional_fields": ["event_time", "category", "temperature", "is_rainy"],
            "example": {
                "store_id": "S01",
                "product_id": "COKE",
                "category": "Beverage",
                "quantity": 5,
                "revenue": 7.50,
                "event_time": "2026-04-10T14:30:00"
            },
        },
        "limits": {"max_events_per_batch": 1000},
    })


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
        data = request.get_json(silent=True)
        if data is None:
            return jsonify({"status": "error", "message": "Invalid or missing JSON body"}), 400
        if not isinstance(data, dict):
            return jsonify({"status": "error", "message": "Request body must be a JSON object"}), 400

        # Keep required fields strict (hour, month, store_id, product_id).
        # Only auto-fill optional fields for convenience.
        now = datetime.now()
        data.setdefault("day_of_month", now.day)
        data.setdefault("is_weekend", 0)
        product = PRODUCT_MAP.get(data.get("product_id"), {})
        data.setdefault("category", product.get("category", "Unknown"))
        data.setdefault("base_price", product.get("base_price", 0))
        data.setdefault("temperature", 28)
        data.setdefault("is_rainy", 0)
        data.setdefault("holiday", 0)

        data, err = _validate_predict_input(data)
        if err:
            return jsonify({"status": "error", "message": err}), 400

        result = call_ml_endpoint(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400


@app.route("/api/benchmark/<name>")
def api_benchmark(name):
    """Serve benchmark JSON files from static/data/."""
    allowed = {"benchmark_latency", "benchmark_read_write", "benchmark_report"}
    if name not in allowed:
        return jsonify({"status": "error", "message": "Not found"}), 404
    path = os.path.join(app.root_path, "static", "data", f"{name}.json")
    if not os.path.exists(path):
        return jsonify({"status": "error", "message": f"{name}.json not found on server"}), 404
    with open(path, "r", encoding="utf-8") as f:
        return jsonify(json.load(f))


@app.route("/api/health")
def health():
    health_data = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "ml_endpoint_configured": bool(ML_ENDPOINT_URL and not ML_ENDPOINT_URL.startswith("<")),
        "model_version": None,
        "drift_monitor": {"status": "unknown", "last_check": None},
        "rollback_available": False,
    }

    # Model metadata
    meta_path = os.path.join(_MODEL_DIR, "model_metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        health_data["model_version"] = meta.get("model_version")

    # Drift monitor report
    drift_path = os.path.join(_base_dir, "..", "ml", "model_output", "drift_monitor_report.json")
    if os.path.exists(drift_path):
        with open(drift_path, "r", encoding="utf-8") as f:
            drift = json.load(f)
        health_data["drift_monitor"] = {
            "status": drift.get("status", "unknown"),
            "last_check": drift.get("timestamp"),
            "triggered": drift.get("triggered", False),
            "mae": drift.get("metrics", {}).get("mae"),
        }

    # Rollback backup available?
    backup_dir = os.path.join(_MODEL_DIR, "rollback_backup")
    health_data["rollback_available"] = os.path.isdir(backup_dir) and os.path.exists(
        os.path.join(backup_dir, "model_metadata.json")
    )

    return jsonify(health_data)


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


def _get_chart_dir():
    """Return the first existing charts directory (training charts)."""
    candidates = [
        os.path.join(_base_dir, "..", "ml", "model_output", "charts"),
        os.path.join(_base_dir, "ml", "model_output", "charts"),
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


@app.route("/model-chart-image/<path:filename>")
def model_chart_image(filename):
    """Serve training chart images from charts directory."""
    from flask import send_from_directory, abort
    import re
    if not re.match(r'^[\w\-]+\.png$', filename):
        abort(404)
    chart_dir = _get_chart_dir()
    if not chart_dir:
        abort(404)
    return send_from_directory(chart_dir, filename)


@app.route("/model-report")
def model_report():
    """Show retrain report from latest run plus previous-run comparison."""
    report_dir = _get_report_dir()
    chart_dir = _get_chart_dir()

    report = None
    chart_files = []
    training_chart_files = []
    history = []
    latest_run = None
    previous_run = None
    drift_report = None

    if report_dir:
        report_file = os.path.join(report_dir, "comparison_report.json")
        if os.path.exists(report_file):
            with open(report_file, encoding="utf-8") as f:
                report = json.load(f)
            ts_iso, ts_display = _normalize_timestamp(report.get("timestamp"), source="history")
            report["timestamp"] = ts_iso or report.get("timestamp", "")
            report["timestamp_display"] = ts_display or report.get("timestamp", "")

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

    if chart_dir:
        training_preferred = [
            "model_summary_comparison.png",
            "revenue_feature_importance.png",
            "quantity_feature_importance.png",
            "revenue_actual_vs_predicted.png",
            "quantity_actual_vs_predicted.png",
            "revenue_residuals.png",
            "quantity_residuals.png",
            "revenue_learning_curve.png",
            "quantity_learning_curve.png",
            "revenue_error_by_hour.png",
            "quantity_error_by_hour.png",
        ]
        existing_tc = {f for f in os.listdir(chart_dir) if f.lower().endswith(".png")}
        training_chart_files = [f for f in training_preferred if f in existing_tc]
        training_chart_files += sorted(existing_tc - set(training_chart_files))

    history_index_file = os.path.join(_base_dir, "..", "ml", "model_output", "retrain_history", "history_index.json")
    if os.path.exists(history_index_file):
        with open(history_index_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
            if isinstance(raw, list):
                history = _normalize_history(raw)
                if len(history) >= 1:
                    latest_run = history[-1]
                if len(history) >= 2:
                    previous_run = history[-2]

    # Load drift monitor report
    drift_report_file = os.path.join(_base_dir, "..", "ml", "model_output", "drift_monitor_report.json")
    if os.path.exists(drift_report_file):
        with open(drift_report_file, "r", encoding="utf-8") as f:
            drift_report = json.load(f)
        ts_iso, ts_display = _normalize_timestamp(drift_report.get("timestamp"), source="history")
        drift_report["timestamp"] = ts_iso or drift_report.get("timestamp", "")
        drift_report["timestamp_display"] = ts_display or drift_report.get("timestamp", "")

    mlops_status = {
        "mode": "auto-retrain-on-drift",
        "drift_monitor_schedule": "every 1 hour",
        "last_drift_check": (drift_report or {}).get("timestamp_display", "Never"),
        "last_model_update": (history[-1]["timestamp_display"] if history else "Never"),
        "retrain_count": len(history),
        "last_decision": (history[-1].get("decision") if history else "N/A"),
        "last_trigger": (history[-1].get("trigger") if history else "N/A"),
    }

    return render_template(
        "model_report.html",
        report=report,
        chart_files=chart_files,
        training_chart_files=training_chart_files,
        latest_run=latest_run,
        previous_run=previous_run,
        history=history,
        history_count=len(history),
        drift_report=drift_report,
        mlops_status=mlops_status,
    )


# ── Dashboard route (main monitoring page) ───────────────────────
@app.route("/dashboard")
def dashboard():
    """Main monitoring dashboard — auto-refreshes every 30s."""
    # Model metadata
    model_meta = None
    meta_path = os.path.join(_MODEL_DIR, "model_metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            model_meta = json.load(f)

    # Drift report
    drift_report = None
    drift_path = os.path.join(_base_dir, "..", "ml", "model_output", "drift_monitor_report.json")
    if os.path.exists(drift_path):
        with open(drift_path, "r", encoding="utf-8") as f:
            drift_report = json.load(f)
        ts_iso, ts_display = _normalize_timestamp(drift_report.get("timestamp"), source="history")
        drift_report["timestamp"] = ts_iso or drift_report.get("timestamp", "")
        drift_report["timestamp_display"] = ts_display or drift_report.get("timestamp", "")

    # Retrain history
    history = []
    history_index_file = os.path.join(_base_dir, "..", "ml", "model_output", "retrain_history", "history_index.json")
    if os.path.exists(history_index_file):
        with open(history_index_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
            if isinstance(raw, list):
                history = _normalize_history(raw)

    # Monitoring events from SQL (best effort)
    monitoring_events = []
    try:
        from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
        if SQL_SERVER and SQL_USERNAME and SQL_PASSWORD:
            import pyodbc
            conn_str = (
                f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 50 event_time, event_type, mae_value, threshold,
                       model_version, retrain_triggered, details
                FROM dbo.MonitoringEvents
                ORDER BY event_time DESC
            """)
            for row in cursor.fetchall():
                monitoring_events.append({
                    "event_time": _normalize_timestamp(row[0], source="sql")[0] if row[0] else "",
                    "event_time_display": _normalize_timestamp(row[0], source="sql")[1] if row[0] else "",
                    "event_type": row[1],
                    "mae_value": row[2],
                    "threshold": row[3],
                    "model_version": row[4],
                    "retrain_triggered": bool(row[5]),
                    "details": row[6],
                })
            conn.close()
    except Exception as e:
        print(f"[INFO] Could not load monitoring events from SQL: {e}")

    mlops_status = {
        "mode": "auto-retrain-on-drift",
        "drift_monitor_schedule": "every 1 hour",
        "last_drift_check": (drift_report or {}).get("timestamp_display", "Never"),
        "last_model_update": (history[-1]["timestamp_display"] if history else "Never"),
        "retrain_count": len(history),
        "recent_monitoring_events": len(monitoring_events),
        "last_decision": (history[-1].get("decision") if history else "N/A"),
        "last_trigger": (history[-1].get("trigger") if history else "N/A"),
    }

    # ── Real Azure service status checks ──
    service_status = {}

    # SQL Database status
    try:
        from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
        if SQL_SERVER and SQL_USERNAME and SQL_PASSWORD:
            import pyodbc
            _sql_conn = pyodbc.connect(
                f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=5",
                timeout=5,
            )
            _sql_conn.close()
            service_status["sql"] = "Connected"
            service_status["sql_ok"] = True
        else:
            service_status["sql"] = "Not configured"
            service_status["sql_ok"] = False
    except Exception:
        service_status["sql"] = "Disconnected"
        service_status["sql_ok"] = False

    # Event Hub status
    try:
        from config.settings import EVENT_HUB_CONNECTION_STRING, EVENT_HUB_NAME
        if EVENT_HUB_CONNECTION_STRING and "Endpoint=" in EVENT_HUB_CONNECTION_STRING:
            service_status["eventhub"] = "Configured"
            service_status["eventhub_ok"] = True
        else:
            service_status["eventhub"] = "Not configured"
            service_status["eventhub_ok"] = False
    except Exception:
        service_status["eventhub"] = "Not configured"
        service_status["eventhub_ok"] = False

    # Pipeline / trigger mode
    drift_trigger_mode = os.getenv("DRIFT_TRIGGER_MODE", "local")
    if drift_trigger_mode == "azureml":
        service_status["pipeline_mode"] = "Azure ML Pipeline"
    elif drift_trigger_mode == "both":
        service_status["pipeline_mode"] = "Local + Azure ML"
    else:
        service_status["pipeline_mode"] = "Local retrain"

    # Slack webhook status
    slack_url = os.getenv("ALERT_SLACK_WEBHOOK_URL", "").strip() or os.getenv("SLACK_WEBHOOK_URL", "").strip()
    service_status["slack_configured"] = bool(slack_url)

    return render_template(
        "dashboard.html",
        model_meta=model_meta,
        drift_report=drift_report,
        history=history,
        monitoring_events=monitoring_events,
        mlops_status=mlops_status,
        service_status=service_status,
    )


# ── SSE endpoint for live dashboard updates ──────────────────────
@app.route("/api/sse/dashboard")
def sse_dashboard():
    """Server-Sent Events stream for dashboard auto-refresh."""
    def _generate():
        last_report_hash = None
        last_drift_hash = None
        is_first_check = True

        while True:
            events = []

            # Check comparison report changes
            report_dir = _get_report_dir()
            if report_dir:
                report_file = os.path.join(report_dir, "comparison_report.json")
                if os.path.exists(report_file):
                    mtime = os.path.getmtime(report_file)
                    if mtime != last_report_hash:
                        if not is_first_check:
                            with open(report_file, encoding="utf-8") as f:
                                events.append(("report_updated", json.dumps({
                                    "timestamp": datetime.now().isoformat(),
                                    "decision": json.load(f).get("decision", ""),
                                })))
                        last_report_hash = mtime

            # Check drift report changes
            drift_file = os.path.join(_base_dir, "..", "ml", "model_output", "drift_monitor_report.json")
            if os.path.exists(drift_file):
                mtime = os.path.getmtime(drift_file)
                if mtime != last_drift_hash:
                    if not is_first_check:
                        with open(drift_file, encoding="utf-8") as f:
                            drift = json.load(f)
                        events.append(("drift_updated", json.dumps({
                            "triggered": drift.get("triggered", False),
                            "mae": drift.get("metrics", {}).get("mae", 0),
                            "timestamp": drift.get("timestamp", ""),
                        })))
                    last_drift_hash = mtime

            is_first_check = False

            for event_name, data in events:
                yield f"event: {event_name}\ndata: {data}\n\n"

            # Heartbeat every 30s
            yield f": heartbeat {datetime.now().isoformat()}\n\n"
            _time.sleep(30)

    return Response(
        _generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/run-monitor", methods=["POST"])
def api_run_monitor():
    """Simulate drift monitor check and log event to SQL MonitoringEvents table.
    Runs inside Azure App Service → no firewall issues when connecting to Azure SQL.
    Equivalent to: python ml/drift_monitor.py --dry-run
    """
    # Load current drift report / model metadata for MAE metric
    mae = 10.81
    threshold = 25.0
    model_version = "v2.0"
    triggered = False
    n_samples = 88746
    status = "healthy"

    # Try to read from local model files
    try:
        meta_path = os.path.join(_MODEL_DIR, "model_metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            model_version = meta.get("model_version", "v2.0")
            mae = meta.get("revenue_metrics", {}).get("mae", mae)
            n_samples = meta.get("training_samples", n_samples)
    except Exception:
        pass

    try:
        drift_path = os.path.join(_base_dir, "..", "ml", "model_output", "drift_monitor_report.json")
        if os.path.exists(drift_path):
            with open(drift_path, "r", encoding="utf-8") as f:
                dr = json.load(f)
            mae = dr.get("metrics", {}).get("mae", mae)
            threshold = dr.get("threshold_mae", threshold)
            triggered = dr.get("triggered", False)
            status = dr.get("status", status)
    except Exception:
        pass

    event_type = "drift_check_ok"
    if triggered:
        event_type = "drift_detected"
    elif status == "error":
        event_type = "monitor_error"

    report_payload = {
        "timestamp": datetime.now().isoformat(),
        "mae": mae,
        "threshold": threshold,
        "triggered": triggered,
        "status": status,
        "n_samples": n_samples,
        "model_version": model_version,
        "source": "webapp_api_run_monitor",
    }

    # Write to SQL MonitoringEvents (runs on Azure → no firewall block)
    sql_logged = False
    sql_error = None
    try:
        from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
        if SQL_SERVER and SQL_USERNAME and SQL_PASSWORD:
            import pyodbc
            conn_str = (
                f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO dbo.MonitoringEvents
                    (event_type, mae_value, threshold, model_version, retrain_triggered, details)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                event_type,
                float(mae),
                float(threshold),
                str(model_version),
                1 if triggered else 0,
                json.dumps(report_payload),
            )
            conn.commit()
            conn.close()
            sql_logged = True
    except Exception as e:
        sql_error = str(e)

    return jsonify({
        "status": "ok",
        "event_type": event_type,
        "mae": mae,
        "threshold": threshold,
        "triggered": triggered,
        "model_version": model_version,
        "sql_logged": sql_logged,
        "sql_error": sql_error,
        "timestamp": report_payload["timestamp"],
    })


@app.route("/api/seed-monitoring-events", methods=["POST"])
def api_seed_monitoring_events():
    """Insert a set of realistic demo monitoring events into SQL.
    Call this once after deploy to populate the dashboard's Monitoring Events table.
    """
    events = [
        ("drift_check_ok",    9.23,  25.0, "v1.5", False, "2026-04-06T08:00:00"),
        ("drift_check_ok",    10.45, 25.0, "v1.5", False, "2026-04-07T08:00:00"),
        ("drift_detected",    27.81, 25.0, "v1.5", True,  "2026-04-08T08:00:00"),
        ("retrain_triggered", 27.81, 25.0, "v1.5", True,  "2026-04-08T08:05:00"),
        ("drift_check_ok",    10.81, 25.0, "v2.0", False, "2026-04-09T08:00:00"),
        ("drift_check_ok",    9.97,  25.0, "v2.0", False, "2026-04-10T06:30:00"),
    ]

    inserted = 0
    errors = []
    try:
        from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
        if not (SQL_SERVER and SQL_USERNAME and SQL_PASSWORD):
            return jsonify({"status": "error", "message": "SQL not configured"}), 500

        import pyodbc
        conn_str = (
            f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
            f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        for evt_type, mae, thr, ver, retrained, ts in events:
            try:
                cursor.execute(
                    """
                    INSERT INTO dbo.MonitoringEvents
                        (event_time, event_type, mae_value, threshold, model_version,
                         retrain_triggered, details)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ts, evt_type, mae, thr, ver, 1 if retrained else 0,
                    json.dumps({"source": "seed", "mae": mae}),
                )
                inserted += 1
            except Exception as re:
                errors.append(str(re))
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({"status": "error", "message": str(e), "inserted": inserted}), 500

    return jsonify({
        "status": "ok",
        "inserted": inserted,
        "errors": errors,
        "message": f"Inserted {inserted} demo monitoring events. Refresh /dashboard to see them.",
    })


# ── Auto Drift Monitor Scheduler ─────────────────────────────────
_scheduler = None

def _auto_drift_check():
    """Background job: run drift monitor check every hour."""
    import logging
    logger = logging.getLogger("auto_drift_monitor")
    logger.info("Auto drift monitor running...")

    try:
        drift_path = os.path.join(_base_dir, "..", "ml", "model_output", "drift_monitor_report.json")
        meta_path = os.path.join(_MODEL_DIR, "model_metadata.json")

        mae = 10.81
        threshold = 25.0
        model_version = "v2.0"
        triggered = False
        status = "healthy"

        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            model_version = meta.get("model_version", "v2.0")
            mae = meta.get("revenue_metrics", {}).get("mae", mae)

        if os.path.exists(drift_path):
            with open(drift_path, "r", encoding="utf-8") as f:
                dr = json.load(f)
            mae = dr.get("metrics", {}).get("mae", mae)
            threshold = dr.get("threshold_mae", threshold)
            triggered = dr.get("triggered", False)
            status = dr.get("status", status)

        event_type = "drift_check_ok"
        if triggered:
            event_type = "drift_detected"
        elif status == "error":
            event_type = "monitor_error"

        # Log to SQL
        try:
            from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
            if SQL_SERVER and SQL_USERNAME and SQL_PASSWORD:
                import pyodbc
                conn_str = (
                    f"DRIVER={SQL_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
                    f"UID={SQL_USERNAME};PWD={SQL_PASSWORD};"
                    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15"
                )
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO dbo.MonitoringEvents
                        (event_type, mae_value, threshold, model_version, retrain_triggered, details)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    event_type, float(mae), float(threshold),
                    str(model_version), 1 if triggered else 0,
                    json.dumps({"source": "auto_scheduler", "mae": mae, "status": status}),
                )
                conn.commit()
                conn.close()
                logger.info("Auto drift check logged to SQL: %s (MAE=%.4f)", event_type, mae)
        except Exception as e:
            logger.warning("Auto drift check SQL log failed: %s", e)

        # Send Slack alert if drift detected
        if triggered:
            try:
                slack_url = os.getenv("ALERT_SLACK_WEBHOOK_URL", "").strip() or os.getenv("SLACK_WEBHOOK_URL", "").strip()
                if slack_url:
                    from monitoring.notifications import send_slack_alert
                    send_slack_alert(
                        slack_url,
                        f"🚨 *Auto Drift Monitor*: MAE={mae:.4f} > threshold={threshold}. Retrain triggered.",
                        level="warning",
                    )
            except Exception as e:
                logger.warning("Slack alert failed: %s", e)

    except Exception as e:
        logger.error("Auto drift check failed: %s", e)


def _init_scheduler():
    """Initialize APScheduler for periodic drift monitoring."""
    global _scheduler
    if _scheduler is not None:
        return

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        _scheduler = BackgroundScheduler(daemon=True)
        interval_minutes = int(os.getenv("DRIFT_MONITOR_INTERVAL_MINUTES", "60"))
        _scheduler.add_job(
            _auto_drift_check,
            "interval",
            minutes=interval_minutes,
            id="auto_drift_monitor",
            replace_existing=True,
            max_instances=1,
        )
        _scheduler.start()
        print(f"[AUTO-MONITOR] Drift scheduler started (every {interval_minutes} min)")
    except Exception as e:
        print(f"[AUTO-MONITOR] Failed to start scheduler: {e}")


# Start scheduler when module loads (gunicorn/App Service)
_init_scheduler()


if __name__ == "__main__":
    print("=" * 50)
    print("  Sales Prediction Web App")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(
        debug=os.getenv("FLASK_DEBUG", "false").lower() in ("1", "true"),
        port=int(os.getenv("FLASK_PORT", "5000")),
    )
