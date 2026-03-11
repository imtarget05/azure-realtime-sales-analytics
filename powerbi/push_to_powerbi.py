"""
Script tạo Power BI Streaming Dataset và đẩy dữ liệu thời gian thực.
Sử dụng Power BI REST API Push Dataset.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

import pyodbc

sys.path.insert(0, ".")
from config.settings import (
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER,
)

# ========================
# CẤU HÌNH POWER BI
# ========================
# Lấy Push URL từ Power BI Service khi tạo Streaming Dataset
POWERBI_PUSH_URL = "<Your-Power-BI-Push-URL>"


def push_to_powerbi(data: list[dict]):
    """Đẩy dữ liệu đến Power BI Streaming Dataset."""
    if POWERBI_PUSH_URL.startswith("<"):
        print("[WARN] Chưa cấu hình Power BI Push URL. Bỏ qua.")
        return

    body = json.dumps(data).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(POWERBI_PUSH_URL, body, headers)

    try:
        response = urllib.request.urlopen(req)
        if response.status == 200:
            print(f"[INFO] Đã đẩy {len(data)} dòng đến Power BI.")
    except urllib.error.HTTPError as e:
        print(f"[ERROR] Power BI push failed: {e.code}")


def get_realtime_summary_from_sql() -> list[dict]:
    """Lấy dữ liệu tổng hợp mới nhất từ SQL Database."""
    conn_string = (
        f"Driver={SQL_DRIVER};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};"
        f"Pwd={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    try:
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()

        query = """
        SELECT TOP 50
            region,
            category,
            COUNT(*) AS transaction_count,
            SUM(quantity) AS total_quantity,
            SUM(final_amount) AS total_revenue,
            AVG(final_amount) AS avg_order_value,
            AVG(CAST(rating AS FLOAT)) AS avg_rating
        FROM SalesTransactions
        WHERE event_timestamp >= DATEADD(minute, -5, GETUTCDATE())
        GROUP BY region, category
        ORDER BY total_revenue DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        now = datetime.now(timezone.utc).isoformat()
        results = []
        for row in rows:
            results.append({
                "timestamp": now,
                "region": row[0],
                "category": row[1],
                "transaction_count": row[2],
                "total_quantity": row[3],
                "total_revenue": float(row[4]),
                "avg_order_value": float(row[5]),
                "avg_rating": float(row[6]) if row[6] else 0,
            })

        cursor.close()
        conn.close()
        return results

    except Exception as e:
        print(f"[ERROR] SQL query failed: {e}")
        return []


def main():
    """Đẩy dữ liệu tổng hợp đến Power BI mỗi phút."""
    print("=" * 60)
    print("  POWER BI REAL-TIME DATA PUSH")
    print("  Nhấn Ctrl+C để dừng")
    print("=" * 60)

    while True:
        try:
            data = get_realtime_summary_from_sql()
            if data:
                push_to_powerbi(data)
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] Pushed {len(data)} rows to Power BI")
            else:
                print("[INFO] Không có dữ liệu mới.")
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n[INFO] Đã dừng.")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
