"""
Push real-time data from Azure SQL to Power BI Streaming Dataset.
Queries dbo.SalesTransactions (aligned with Stream Analytics output).
"""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

import pyodbc

sys.path.insert(0, ".")
from config.settings import (
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER,
)

POWERBI_PUSH_URL = os.getenv("POWERBI_PUSH_URL", "<Your-Power-BI-Push-URL>")


def push_to_powerbi(data: list[dict]):
    if POWERBI_PUSH_URL.startswith("<"):
        print("[WARN] POWERBI_PUSH_URL not configured. Skipping push.")
        return

    body = json.dumps(data).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(POWERBI_PUSH_URL, body, headers)

    try:
        response = urllib.request.urlopen(req)
        if response.status == 200:
            print(f"[INFO] Pushed {len(data)} rows to Power BI.")
    except urllib.error.HTTPError as e:
        print(f"[ERROR] Power BI push failed: {e.code}")


def get_realtime_summary_from_sql() -> list[dict]:
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
            store_id,
            category,
            COUNT(*)         AS transaction_count,
            SUM(units_sold)  AS total_quantity,
            SUM(revenue)     AS total_revenue,
            AVG(unit_price)  AS avg_unit_price,
            AVG(temperature) AS avg_temperature
        FROM dbo.SalesTransactions
        WHERE event_time >= DATEADD(minute, -5, SYSUTCDATETIME())
        GROUP BY store_id, category
        ORDER BY total_revenue DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        now = datetime.now(timezone.utc).isoformat()
        results = []
        for row in rows:
            results.append({
                "timestamp": now,
                "store_id": row[0],
                "category": row[1],
                "transaction_count": row[2],
                "total_quantity": int(row[3]),
                "total_revenue": round(float(row[4]), 2),
                "avg_unit_price": round(float(row[5]), 2),
                "avg_temperature": round(float(row[6]), 1) if row[6] else None,
            })

        cursor.close()
        conn.close()
        return results

    except Exception as e:
        print(f"[ERROR] SQL query failed: {e}")
        return []


def main():
    print("=" * 60)
    print("  POWER BI REAL-TIME DATA PUSH")
    print("  Press Ctrl+C to stop")
    print("=" * 60)

    while True:
        try:
            data = get_realtime_summary_from_sql()
            if data:
                push_to_powerbi(data)
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] Pushed {len(data)} rows")
            else:
                print("[INFO] No new data.")
            time.sleep(60)
        except KeyboardInterrupt:
            print("\n[INFO] Stopped.")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
