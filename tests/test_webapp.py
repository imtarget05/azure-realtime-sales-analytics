"""
Unit tests cho webapp/app.py — Flask endpoints.
"""

import pytest


@pytest.fixture
def client():
    """Tạo Flask test client."""
    from webapp.app import app
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_health_endpoint(client):
    """GET /api/health phải trả 200 với status healthy."""
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "ml_endpoint_configured" in data


def test_index_page_loads(client):
    """GET / phải trả 200."""
    resp = client.get("/")
    assert resp.status_code == 200


def test_api_predict_returns_json(client):
    """POST /api/predict phải trả JSON response với prediction."""
    payload = {
        "hour": 12,
        "day_of_week": 3,
        "month": 6,
        "is_online": 0,
        "base_price": 50,
        "discount_percent": 10,
        "quantity": 2,
        "store_id": "S01",
        "product_id": "COKE",
    }
    resp = client.post("/api/predict", json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "success"
    assert "prediction" in data


def test_api_predict_invalid_json(client):
    """POST /api/predict với body không hợp lệ phải trả 400."""
    resp = client.post("/api/predict", data="not-json",
                       content_type="application/json")
    assert resp.status_code == 400


def test_predict_form_endpoint(client):
    """POST /predict phải xử lý form data và trả HTML."""
    resp = client.post("/predict", data={
        "hour": "14",
        "day_of_week": "2",
        "month": "3",
        "is_online": "0",
        "category_id": "0",
        "region_id": "0",
        "product_id": "0",
        "base_price": "50",
        "discount_percent": "5",
        "quantity": "1",
    })
    assert resp.status_code == 200
