"""
Extended tests for webapp/app.py — covers all routes, error paths, and edge cases.
"""

import json
import os
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    """Create Flask test client."""
    from webapp.app import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ── Health & Index ───────────────────────────────────────────────────

def test_health_endpoint(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "ml_endpoint_configured" in data


def test_index_page_loads(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"<!DOCTYPE html>" in resp.data or b"<html" in resp.data


def test_index_contains_stores_and_products(client):
    resp = client.get("/")
    html = resp.data.decode()
    assert "S01" in html or "store" in html.lower()


# ── POST /predict (form) ────────────────────────────────────────────

def test_predict_form_valid(client):
    resp = client.post("/predict", data={
        "hour": "14",
        "day_of_week": "2",
        "month": "6",
        "store_id": "S01",
        "product_id": "COKE",
        "temperature": "28",
        "is_rainy": "0",
        "holiday": "0",
    })
    assert resp.status_code == 200


def test_predict_form_defaults(client):
    """Missing fields should use defaults, not crash."""
    resp = client.post("/predict", data={})
    assert resp.status_code == 200


def test_predict_form_invalid_types(client):
    """Non-numeric values should be handled gracefully."""
    resp = client.post("/predict", data={
        "hour": "abc",
        "month": "xyz",
        "temperature": "not_a_number",
    })
    # Should return 200 with error message in HTML, not 500
    assert resp.status_code == 200


# ── POST /api/predict (JSON) ────────────────────────────────────────

def test_api_predict_valid(client):
    payload = {
        "hour": 14, "day_of_month": 15, "month": 6,
        "is_weekend": 0, "store_id": "S01", "product_id": "COKE",
        "category": "Beverage", "base_price": 1.5,
        "temperature": 28, "is_rainy": 0, "holiday": 0,
    }
    resp = client.post("/api/predict", json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "success"
    assert "prediction" in data
    assert "predicted_revenue" in data["prediction"]
    assert "predicted_quantity" in data["prediction"]
    assert "confidence_interval" in data["prediction"]
    assert "source" in data


def test_api_predict_returns_positive_values(client):
    payload = {
        "hour": 10, "day_of_month": 1, "month": 1,
        "is_weekend": 0, "store_id": "S01", "product_id": "COKE",
        "category": "Beverage", "temperature": 20,
        "is_rainy": 0, "holiday": 0,
    }
    resp = client.post("/api/predict", json=payload)
    data = resp.get_json()
    assert data["prediction"]["predicted_revenue"] >= 0
    assert data["prediction"]["predicted_quantity"] >= 0


def test_api_predict_get_returns_usage(client):
    resp = client.get("/api/predict")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "usage" in data
    assert "example" in data


def test_api_predict_invalid_json(client):
    resp = client.post("/api/predict", data="not-json",
                       content_type="application/json")
    assert resp.status_code == 400


def test_api_predict_empty_body(client):
    resp = client.post("/api/predict", json={})
    assert resp.status_code == 400
    data = resp.get_json()
    assert data["status"] == "error"
    assert "Missing required field" in data["message"]


# ── /api/predict input validation ───────────────────────────────────

def test_api_predict_missing_required_fields(client):
    resp = client.post("/api/predict", json={"hour": 10})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "month" in data["message"]
    assert "store_id" in data["message"]
    assert "product_id" in data["message"]


def test_api_predict_hour_out_of_range(client):
    resp = client.post("/api/predict", json={
        "hour": 25, "month": 6, "store_id": "S01", "product_id": "COKE",
    })
    assert resp.status_code == 400
    data = resp.get_json()
    assert "hour" in data["message"]


def test_api_predict_month_out_of_range(client):
    resp = client.post("/api/predict", json={
        "hour": 10, "month": 13, "store_id": "S01", "product_id": "COKE",
    })
    assert resp.status_code == 400


def test_api_predict_invalid_type(client):
    resp = client.post("/api/predict", json={
        "hour": "abc", "month": 6, "store_id": "S01", "product_id": "COKE",
    })
    assert resp.status_code == 400
    data = resp.get_json()
    assert "integer" in data["message"]


def test_api_predict_non_dict_body(client):
    resp = client.post("/api/predict", json=[1, 2, 3])
    assert resp.status_code == 400
    data = resp.get_json()
    assert "JSON object" in data["message"]


def test_api_predict_valid_minimal(client):
    resp = client.post("/api/predict", json={
        "hour": 14, "month": 6, "store_id": "S01", "product_id": "COKE",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "success"


# ── /model-report ───────────────────────────────────────────────────

def test_model_report_page_loads(client):
    resp = client.get("/model-report")
    assert resp.status_code == 200


def test_model_report_contains_dashboard(client):
    resp = client.get("/model-report")
    html = resp.data.decode()
    assert "Model Report" in html


# ── /dashboard ──────────────────────────────────────────────────────

def test_dashboard_page_loads(client):
    resp = client.get("/dashboard")
    assert resp.status_code == 200


def test_dashboard_contains_monitoring(client):
    resp = client.get("/dashboard")
    html = resp.data.decode()
    assert "Monitoring Dashboard" in html


# ── /model-report-image security ────────────────────────────────────

def test_model_report_image_path_traversal(client):
    """Path traversal attempts must return 404."""
    resp = client.get("/model-report-image/../../../etc/passwd")
    assert resp.status_code == 404


def test_model_report_image_invalid_extension(client):
    resp = client.get("/model-report-image/test.txt")
    assert resp.status_code == 404


def test_model_chart_image_path_traversal(client):
    resp = client.get("/model-chart-image/../../secret.png")
    assert resp.status_code == 404


# ── call_ml_endpoint fallback chain ─────────────────────────────────

def test_call_ml_endpoint_falls_to_mock():
    """When no endpoint and no local model, should return mock prediction."""
    from webapp.app import call_ml_endpoint
    result = call_ml_endpoint({
        "hour": 12, "month": 6, "store_id": "S01",
        "product_id": "COKE", "category": "Beverage",
        "base_price": 1.5, "temperature": 28,
        "is_rainy": 0, "holiday": 0,
    })
    assert result["status"] == "success"
    assert "source" in result


def test_call_ml_endpoint_skips_unconfigured_azure():
    """Should not attempt Azure call when URL not configured."""
    from webapp.app import call_ml_endpoint
    with patch("webapp.app.ML_ENDPOINT_URL", ""), \
         patch("webapp.app.ML_API_KEY", ""):
        result = call_ml_endpoint({"product_id": "COKE"})
        assert result["status"] == "success"


# ── 404 ─────────────────────────────────────────────────────────────

def test_404_for_unknown_route(client):
    resp = client.get("/nonexistent-page")
    assert resp.status_code == 404
