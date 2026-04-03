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
import json
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER,
    PRODUCTS, STORE_IDS,
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
    revenue = round(unit_price * quantity, 2)
    dt = datetime(2024, 1, 1) + timedelta(seconds=random.randint(0, 30_000_000))
    weather = random.choice(["sunny", "rainy", "cloudy", "stormy"])
    holiday = random.choice([0, 0, 0, 1])

    return (
        dt.isoformat(),
        random.choice(STORE_IDS),
        product["id"],
        quantity,
        unit_price,
        revenue,
        round(random.uniform(15, 40), 1),
        weather,
        holiday,
        product["category"],
    )


INSERT_SQL = """
    INSERT INTO SalesTransactions
        (event_time, store_id, product_id, units_sold, unit_price,
         revenue, temperature, weather, holiday, category)
    VALUES (?,?,?,?,?,?,?,?,?,?)
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
        ("SUM + GROUP BY store_id", """
            SELECT store_id, SUM(revenue) AS total_revenue, COUNT(*) AS cnt
            FROM SalesTransactions
            GROUP BY store_id
            ORDER BY total_revenue DESC
        """),
        ("AVG + GROUP BY category", """
            SELECT category, AVG(revenue) AS avg_revenue, COUNT(*) AS cnt
            FROM SalesTransactions
            GROUP BY category
        """),
        ("TOP 10 products", """
            SELECT TOP 10 product_id, SUM(revenue) AS total_revenue
            FROM SalesTransactions
            GROUP BY product_id
            ORDER BY total_revenue DESC
        """),
        ("WHERE + ORDER BY", """
            SELECT TOP 100 id, revenue, store_id, category
            FROM SalesTransactions
            WHERE revenue > 10
            ORDER BY revenue DESC
        """),
        ("DATE range filter", """
            SELECT COUNT(*) AS cnt, SUM(revenue) AS total_revenue
            FROM SalesTransactions
            WHERE event_time BETWEEN '2024-03-01' AND '2024-06-30'
        """),
        ("Multi-condition filter", """
            SELECT store_id, category, SUM(revenue) AS total_revenue
            FROM SalesTransactions
            WHERE holiday = 1 AND temperature > 30
            GROUP BY store_id, category
            ORDER BY total_revenue DESC
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
        cursor.execute("DELETE FROM SalesTransactions WHERE store_id IN ('S01','S02','S03')")
        conn.commit()
        cursor.close()

        r1 = benchmark_single_insert(conn, min(n, 1000))
        all_results["insert_benchmarks"].append(r1)
        print(f"  Single: {r1['time_sec']}s ({r1['rows_per_sec']} rows/s)")

        cursor = conn.cursor()
        cursor.execute("DELETE FROM SalesTransactions WHERE store_id IN ('S01','S02','S03')")
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
