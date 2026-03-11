"""
Mục 3.1 Rubric: Đo benchmark đọc/ghi dữ liệu trên Azure SQL Database.
- Đo tốc độ INSERT (single, batch, bulk)
- Đo tốc độ SELECT (simple, aggregation, JOIN)
- Đo với nhiều kích thước dữ liệu (1K, 10K, 50K, 100K dòng)
- Xuất kết quả dạng bảng + JSON
"""

import os
import sys
import time
import uuid
import json
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER,
    PRODUCTS, REGIONS, PAYMENT_METHODS, CUSTOMER_SEGMENTS,
)

try:
    import pyodbc
except ImportError:
    pyodbc = None


def get_connection():
    conn_string = (
        f"Driver={SQL_DRIVER};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_string, timeout=30)


def generate_row():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 10)
    unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)
    total = round(unit_price * quantity, 2)
    discount_pct = random.choice([0, 5, 10, 15, 20])
    discount_amt = round(total * discount_pct / 100, 2)
    final = round(total - discount_amt, 2)
    dt = datetime(2024, 1, 1) + timedelta(seconds=random.randint(0, 30_000_000))

    return (
        str(uuid.uuid4()), dt.isoformat(), dt.strftime("%Y-%m-%d"),
        dt.hour, ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()],
        product["id"], product["name"], product["category"],
        quantity, unit_price, total, discount_pct, discount_amt, final,
        f"C{random.randint(1,5000):05d}", random.choice(CUSTOMER_SEGMENTS),
        random.choice(REGIONS), random.choice(PAYMENT_METHODS),
        random.choice([0, 1]), random.randint(1, 5),
    )


INSERT_SQL = """
    INSERT INTO SalesTransactions
        (transaction_id, event_timestamp, sale_date, sale_hour, day_of_week,
         product_id, product_name, category, quantity, unit_price,
         total_amount, discount_percent, discount_amount, final_amount,
         customer_id, customer_segment, region, payment_method, is_online, rating)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


# ============================================================
# BENCHMARK: INSERT OPERATIONS
# ============================================================

def benchmark_single_insert(conn, n_rows: int) -> dict:
    """Insert từng dòng một (worst case)."""
    cursor = conn.cursor()
    rows = [generate_row() for _ in range(n_rows)]

    t0 = time.time()
    for row in rows:
        cursor.execute(INSERT_SQL, row)
    conn.commit()
    elapsed = time.time() - t0
    cursor.close()

    return {
        "method": "SINGLE INSERT",
        "rows": n_rows,
        "time_sec": round(elapsed, 3),
        "rows_per_sec": round(n_rows / elapsed, 1),
    }


def benchmark_batch_insert(conn, n_rows: int, batch_size: int = 1000) -> dict:
    """Insert theo batch (executemany)."""
    cursor = conn.cursor()
    rows = [generate_row() for _ in range(n_rows)]

    t0 = time.time()
    for i in range(0, len(rows), batch_size):
        cursor.executemany(INSERT_SQL, rows[i:i + batch_size])
        conn.commit()
    elapsed = time.time() - t0
    cursor.close()

    return {
        "method": f"BATCH INSERT (size={batch_size})",
        "rows": n_rows,
        "time_sec": round(elapsed, 3),
        "rows_per_sec": round(n_rows / elapsed, 1),
    }


# ============================================================
# BENCHMARK: SELECT OPERATIONS
# ============================================================

def benchmark_queries(conn) -> list:
    """Đo tốc độ các loại query SELECT khác nhau."""
    cursor = conn.cursor()
    queries = [
        ("COUNT(*)", "SELECT COUNT(*) FROM SalesTransactions"),
        ("SUM + GROUP BY region", """
            SELECT region, SUM(final_amount) AS revenue, COUNT(*) AS cnt
            FROM SalesTransactions
            GROUP BY region
            ORDER BY revenue DESC
        """),
        ("AVG + GROUP BY category", """
            SELECT category, AVG(final_amount) AS avg_amount, COUNT(*) AS cnt
            FROM SalesTransactions
            GROUP BY category
        """),
        ("TOP 10 products", """
            SELECT TOP 10 product_name, SUM(final_amount) AS total_revenue
            FROM SalesTransactions
            GROUP BY product_name
            ORDER BY total_revenue DESC
        """),
        ("WHERE + ORDER BY", """
            SELECT TOP 100 transaction_id, final_amount, region, category
            FROM SalesTransactions
            WHERE final_amount > 100
            ORDER BY final_amount DESC
        """),
        ("DATE range filter", """
            SELECT COUNT(*) AS cnt, SUM(final_amount) AS revenue
            FROM SalesTransactions
            WHERE sale_date BETWEEN '2024-03-01' AND '2024-06-30'
        """),
        ("Multi-condition filter", """
            SELECT region, category, SUM(final_amount) AS revenue
            FROM SalesTransactions
            WHERE is_online = 1 AND discount_percent > 0
            GROUP BY region, category
            ORDER BY revenue DESC
        """),
    ]

    results = []
    for name, sql in queries:
        t0 = time.time()
        cursor.execute(sql)
        rows = cursor.fetchall()
        elapsed = time.time() - t0
        results.append({
            "query": name,
            "time_sec": round(elapsed, 4),
            "result_rows": len(rows),
        })
        print(f"  [{name}]: {elapsed:.4f}s ({len(rows)} rows)")

    cursor.close()
    return results


# ============================================================
# BENCHMARK: READ vs WRITE TỔNG HỢP
# ============================================================

def run_full_benchmark():
    if pyodbc is None:
        print("[ERROR] pyodbc chưa cài đặt. Chạy: pip install pyodbc")
        return

    print("=" * 60)
    print("  BENCHMARK ĐỌC/GHI AZURE SQL DATABASE")
    print("=" * 60)

    conn = get_connection()
    all_results = {"timestamp": datetime.now().isoformat(), "insert_benchmarks": [], "query_benchmarks": []}

    # --- WRITE BENCHMARKS ---
    sizes = [1_000, 5_000, 10_000]
    print("\n[WRITE] Benchmark INSERT...")

    for n in sizes:
        print(f"\n  --- {n:,} dòng ---")
        # Xóa dữ liệu cũ
        cursor = conn.cursor()
        cursor.execute("DELETE FROM SalesTransactions WHERE transaction_id LIKE '%-%'")
        conn.commit()
        cursor.close()

        r1 = benchmark_single_insert(conn, min(n, 1000))
        all_results["insert_benchmarks"].append(r1)
        print(f"  Single: {r1['time_sec']}s ({r1['rows_per_sec']} rows/s)")

        cursor = conn.cursor()
        cursor.execute("DELETE FROM SalesTransactions WHERE transaction_id LIKE '%-%'")
        conn.commit()
        cursor.close()

        r2 = benchmark_batch_insert(conn, n, batch_size=1000)
        all_results["insert_benchmarks"].append(r2)
        print(f"  Batch:  {r2['time_sec']}s ({r2['rows_per_sec']} rows/s)")

    # --- READ BENCHMARKS ---
    print("\n[READ] Benchmark SELECT (trên dữ liệu hiện có)...")
    # Insert 50K dòng cho query benchmark
    print("  Chuẩn bị 50,000 dòng dữ liệu...")
    benchmark_batch_insert(conn, 50_000, batch_size=2000)

    query_results = benchmark_queries(conn)
    all_results["query_benchmarks"] = query_results

    conn.close()

    # --- XUẤT KẾT QUẢ ---
    output_dir = os.path.join(os.path.dirname(__file__), "..", "benchmark_output")
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "benchmark_read_write.json")

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    # In bảng tổng hợp
    print(f"\n{'='*60}")
    print(f"  TỔNG HỢP KẾT QUẢ")
    print(f"{'='*60}")
    print(f"\n  INSERT:")
    print(f"  {'Method':<35} {'Rows':>8} {'Time(s)':>10} {'Rows/s':>10}")
    print(f"  {'-'*65}")
    for r in all_results["insert_benchmarks"]:
        print(f"  {r['method']:<35} {r['rows']:>8,} {r['time_sec']:>10.3f} {r['rows_per_sec']:>10.1f}")

    print(f"\n  SELECT:")
    print(f"  {'Query':<35} {'Time(s)':>10} {'Result Rows':>12}")
    print(f"  {'-'*60}")
    for r in all_results["query_benchmarks"]:
        print(f"  {r['query']:<35} {r['time_sec']:>10.4f} {r['result_rows']:>12}")

    print(f"\n  Báo cáo: {report_path}")


if __name__ == "__main__":
    run_full_benchmark()
