"""
Gọi Azure ML Endpoint để dự đoán doanh thu theo thời gian thực.
Lưu kết quả vào Azure SQL Database.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import pyodbc

sys.path.insert(0, ".")
from config.settings import (
    AML_ENDPOINT_URL,
    AML_API_KEY,
    SQL_SERVER,
    SQL_DATABASE,
    SQL_USERNAME,
    SQL_PASSWORD,
    SQL_DRIVER,
    REGIONS,
)

CATEGORIES = ["Electronics", "Clothing", "Home", "Accessories"]


def call_ml_endpoint(data: list[dict]) -> dict:
    """Gọi Azure ML Endpoint để dự đoán."""
    body = json.dumps({"data": data}).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AML_API_KEY}",
    }

    req = urllib.request.Request(AML_ENDPOINT_URL, body, headers)

    try:
        response = urllib.request.urlopen(req)
        result = json.loads(response.read().decode("utf-8"))
        return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        print(f"[ERROR] ML Endpoint error: {e.code} - {error_body}")
        return {"error": error_body}


def save_forecast_to_sql(forecasts: list[dict], model_version: str = "v1.0"):
    """Lưu kết quả dự đoán vào Azure SQL Database."""
    conn_string = (
        f"Driver={SQL_DRIVER};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};"
        f"Pwd={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO SalesForecast 
            (forecast_date, forecast_hour, region, category,
             predicted_quantity, predicted_revenue,
             confidence_lower, confidence_upper, model_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for f in forecasts:
        cursor.execute(insert_sql, (
            f["forecast_date"],
            f["forecast_hour"],
            f["region"],
            f["category"],
            f["predicted_quantity"],
            f["predicted_revenue"],
            f["confidence_lower"],
            f["confidence_upper"],
            model_version,
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"[INFO] Đã lưu {len(forecasts)} dự đoán vào SQL Database.")


def run_forecast_cycle():
    """Chạy một chu kỳ dự đoán cho tất cả các vùng và danh mục."""
    now = datetime.now(timezone.utc)

    # Dự đoán cho 24 giờ tới
    forecast_inputs = []
    forecast_metadata = []

    for hours_ahead in range(1, 25):
        future_time = now + timedelta(hours=hours_ahead)
        for region in REGIONS:
            for category in CATEGORIES:
                input_data = {
                    "hour": future_time.hour,
                    "day_of_month": future_time.day,
                    "month": future_time.month,
                    "day_of_week": future_time.strftime("%A"),
                    "is_weekend": 1 if future_time.weekday() >= 5 else 0,
                    "region": region,
                    "category": category,
                    "temperature": 22.0,   # Có thể lấy từ API thời tiết thực
                    "humidity": 60.0,
                    "is_rainy": 0,
                }
                forecast_inputs.append(input_data)
                forecast_metadata.append({
                    "forecast_date": future_time.strftime("%Y-%m-%d"),
                    "forecast_hour": future_time.hour,
                    "region": region,
                    "category": category,
                })

    # Gọi ML endpoint (chia batch nếu nhiều)
    batch_size = 100
    all_predictions = []

    for i in range(0, len(forecast_inputs), batch_size):
        batch = forecast_inputs[i:i + batch_size]
        result = call_ml_endpoint(batch)

        if "predictions" in result:
            all_predictions.extend(result["predictions"])
        elif "error" in result:
            print(f"[ERROR] Batch {i // batch_size}: {result['error']}")

    # Chuẩn bị dữ liệu để lưu
    forecasts_to_save = []
    for idx, pred in enumerate(all_predictions):
        if idx < len(forecast_metadata):
            meta = forecast_metadata[idx]
            ci = pred.get("confidence_interval", {})
            forecasts_to_save.append({
                "forecast_date": meta["forecast_date"],
                "forecast_hour": meta["forecast_hour"],
                "region": meta["region"],
                "category": meta["category"],
                "predicted_quantity": pred.get("predicted_quantity", 0),
                "predicted_revenue": pred.get("predicted_revenue", 0),
                "confidence_lower": ci.get("revenue_lower", 0),
                "confidence_upper": ci.get("revenue_upper", 0),
            })

    if forecasts_to_save:
        save_forecast_to_sql(forecasts_to_save, pred.get("model_version", "v1.0"))

    return len(forecasts_to_save)


def main():
    """Chạy dự đoán liên tục mỗi giờ."""
    print("=" * 60)
    print("  DỰ ĐOÁN NHU CẦU BÁN HÀNG THỜI GIAN THỰC")
    print(f"  ML Endpoint: {AML_ENDPOINT_URL[:50]}...")
    print("  Nhấn Ctrl+C để dừng")
    print("=" * 60)

    while True:
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[{timestamp}] Bắt đầu chu kỳ dự đoán...")
            count = run_forecast_cycle()
            print(f"[{timestamp}] Hoàn thành: {count} dự đoán")
            print(f"[INFO] Chờ 1 giờ cho chu kỳ tiếp theo...")
            time.sleep(3600)
        except KeyboardInterrupt:
            print("\n[INFO] Đã dừng dự đoán.")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            print("[INFO] Thử lại sau 5 phút...")
            time.sleep(300)


if __name__ == "__main__":
    main()
