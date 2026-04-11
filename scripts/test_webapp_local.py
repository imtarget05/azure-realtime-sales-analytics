"""Quick smoke test: hit every webapp endpoint with Flask test client."""
import json, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("KEY_VAULT_URI", "DISABLED")

from webapp.app import app

c = app.test_client()

pages = [
    ("GET  /", "/", "GET", None),
    ("POST /predict", "/predict", "POST_FORM",
     {"hour": "14", "day_of_week": "2", "month": "6", "store_id": "S01",
      "product_id": "COKE", "temperature": "28", "is_rainy": "0", "holiday": "0"}),
    ("GET  /dashboard", "/dashboard", "GET", None),
    ("GET  /model-report", "/model-report", "GET", None),
    ("GET  /api/health", "/api/health", "GET", None),
    ("GET  /api/predict", "/api/predict", "GET", None),
    ("POST /api/predict", "/api/predict", "POST_JSON",
     {"hour": 14, "month": 6, "store_id": "S01", "product_id": "COKE",
      "category": "Beverage", "temperature": 28, "is_rainy": 0, "holiday": 0}),
    ("GET  /api/ingest", "/api/ingest", "GET", None),
    ("POST /api/ingest", "/api/ingest", "POST_JSON",
     [{"store_id": "S01", "product_id": "COKE", "quantity": 5, "revenue": 7.5}]),
]

sep = "=" * 78
print(sep)
print(f"{'Endpoint':<25} {'Code':<6} {'Size':>8}  Checks")
print(sep)

all_ok = True
for name, path, method, data in pages:
    try:
        if method == "POST_FORM":
            r = c.post(path, data=data)
        elif method == "POST_JSON":
            r = c.post(path, json=data, content_type="application/json")
        else:
            r = c.get(path)

        body = r.get_data(as_text=True)
        size = len(body)
        checks = []

        if r.status_code != 200:
            checks.append(f"STATUS={r.status_code}")
            all_ok = False

        # Page-specific checks
        if path == "/" and method == "GET":
            if "store_id" in body:
                checks.append("OK_STORES")
            if "product_id" in body:
                checks.append("OK_PRODUCTS")
        elif path == "/predict":
            if "prediction-value" in body or "predicted_revenue" in body.lower():
                checks.append("OK_PREDICTION")
            else:
                checks.append("MISSING_PREDICTION"); all_ok = False
            if "source-badge" in body:
                checks.append("OK_SOURCE_BADGE")
        elif path == "/dashboard":
            for kw, label in [("MLOps Self-Learning Flow", "MLOPS"),
                               ("UTC+07:00", "TZ"),
                               ("Monitoring Events", "EVENTS"),
                               ("R² Score Trend", "R2_CHART")]:
                if kw in body:
                    checks.append(f"OK_{label}")
                else:
                    checks.append(f"MISS_{label}"); all_ok = False
        elif path == "/model-report":
            for kw, label in [("MLOps Self-Learning Flow", "MLOPS"),
                               ("Retrain History", "HISTORY"),
                               ("Drift Monitor", "DRIFT")]:
                if kw in body:
                    checks.append(f"OK_{label}")
                else:
                    checks.append(f"MISS_{label}"); all_ok = False
        elif path == "/api/health":
            j = json.loads(body)
            checks.append(f"v={j.get('model_version', '?')}")
            checks.append(f"drift={j.get('drift_monitor', {}).get('status', '?')}")
        elif path == "/api/predict" and method == "POST_JSON":
            j = json.loads(body)
            src = j.get("source", "?")
            rev = j.get("prediction", {}).get("predicted_revenue", 0)
            qty = j.get("prediction", {}).get("predicted_quantity", 0)
            checks.append(f"src={src[:35]}")
            if rev > 0:
                checks.append(f"rev=${rev:.2f}")
            if qty > 0:
                checks.append(f"qty={qty}")
        elif path == "/api/ingest" and method == "GET":
            j = json.loads(body)
            checks.append(f"docs={'endpoint' in j}")
        elif path == "/api/ingest" and method == "POST_JSON":
            j = json.loads(body)
            checks.append(f"validated={j.get('validated', 0)}")
            checks.append(f"sql={j.get('stored_to_sql', 0)}")

        check_str = " | ".join(checks) if checks else "OK"
        print(f"{name:<25} {r.status_code:<6} {size:>8}  {check_str}")
    except Exception as e:
        print(f"{name:<25} ERROR         -  {e}")
        all_ok = False

print(sep)
print(f"\nOverall: {'ALL PASS' if all_ok else 'SOME ISSUES'}")
