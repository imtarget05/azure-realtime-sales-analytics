"""
Mục 1.3 Rubric: Kích thước dữ liệu & So sánh tốc độ xử lý local vs cloud.
- Sinh dữ liệu bán hàng lớn (>4GB)
- Đo thời gian xử lý trên local
- Đo thời gian xử lý trên Azure SQL Database
- So sánh và xuất báo cáo
"""

import csv
import json
import os
import sys
import time
import random
import statistics
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


# ============================================================
# 1. SINH DỮ LIỆU LỚN (>4GB)
# ============================================================

def generate_large_dataset(output_path: str, target_size_gb: float = 4.5, batch_log_interval: int = 500_000):
    """
    Sinh file CSV lớn chứa dữ liệu bán hàng giả lập.

    Args:
        output_path: Đường dẫn file CSV đầu ra.
        target_size_gb: Kích thước mục tiêu (GB).
        batch_log_interval: Số dòng giữa mỗi lần log tiến trình.
    """
    target_bytes = int(target_size_gb * 1024 * 1024 * 1024)
    header = [
        "event_time", "store_id", "product_id", "units_sold", "unit_price",
        "revenue", "temperature", "weather", "holiday", "category"
    ]
    weather_choices = ["sunny", "rainy", "cloudy", "stormy"]

    print(f"[INFO] Sinh dữ liệu CSV mục tiêu {target_size_gb} GB...")
    print(f"[INFO] File: {output_path}")

    start_time = time.time()
    row_count = 0
    start_date = datetime(2024, 1, 1)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        while True:
            product = random.choice(PRODUCTS)
            quantity = random.randint(1, 10)
            unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)
            revenue = round(unit_price * quantity, 2)
            dt = start_date + timedelta(seconds=random.randint(0, 60_000_000))

            writer.writerow([
                dt.isoformat(),
                random.choice(STORE_IDS),
                product["id"],
                quantity,
                unit_price,
                revenue,
                round(random.uniform(15, 40), 1),
                random.choice(weather_choices),
                random.choice([0, 0, 0, 1]),
                product["category"],
            ])
            row_count += 1

            if row_count % batch_log_interval == 0:
                current_size = os.path.getsize(output_path)
                elapsed = time.time() - start_time
                pct = current_size / target_bytes * 100
                speed = current_size / (1024 * 1024 * elapsed) if elapsed > 0 else 0
                print(f"  [{pct:.1f}%] {row_count:,} dòng | "
                      f"{current_size / (1024**3):.2f} GB | "
                      f"{speed:.1f} MB/s | {elapsed:.0f}s")

                if current_size >= target_bytes:
                    break

    final_size = os.path.getsize(output_path)
    elapsed = time.time() - start_time
    print(f"\n[DONE] {row_count:,} dòng | {final_size / (1024**3):.2f} GB | {elapsed:.1f}s")
    return row_count, final_size


# ============================================================
# 2. ĐO TỐC ĐỘ XỬ LÝ LOCAL
# ============================================================

def benchmark_local_processing(csv_path: str) -> dict:
    """
    Đo tốc độ xử lý dữ liệu trên máy local (đọc CSV, tính toán tổng hợp).
    """
    print("\n[LOCAL] Bắt đầu benchmark xử lý local...")
    results = {}

    # 2a. Đọc file CSV
    t0 = time.time()
    row_count = 0
    total_revenue = 0.0
    region_revenue = {}
    category_count = {}

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            amount = float(row["revenue"])
            total_revenue += amount

            store = row["store_id"]
            region_revenue[store] = region_revenue.get(store, 0) + amount

            category = row["category"]
            category_count[category] = category_count.get(category, 0) + 1

    t_read = time.time() - t0
    results["local_read_time_sec"] = round(t_read, 2)
    results["local_rows"] = row_count
    results["local_total_revenue"] = round(total_revenue, 2)

    file_size_gb = os.path.getsize(csv_path) / (1024 ** 3)
    results["local_read_throughput_mbps"] = round(file_size_gb * 1024 / t_read, 2)

    print(f"  Đọc CSV: {t_read:.2f}s | {row_count:,} dòng | "
          f"Throughput: {results['local_read_throughput_mbps']:.1f} MB/s")

    # 2b. Tính toán aggregation
    t0 = time.time()
    sorted_regions = sorted(region_revenue.items(), key=lambda x: x[1], reverse=True)
    sorted_categories = sorted(category_count.items(), key=lambda x: x[1], reverse=True)
    avg_revenue = total_revenue / row_count if row_count > 0 else 0
    t_compute = time.time() - t0
    results["local_compute_time_sec"] = round(t_compute, 4)

    print(f"  Tính toán: {t_compute:.4f}s")
    print(f"  Top store: {sorted_regions[0] if sorted_regions else 'N/A'}")
    print(f"  Doanh thu TB: ${avg_revenue:.2f}")

    results["local_total_time_sec"] = round(t_read + t_compute, 2)
    return results


# ============================================================
# 3. ĐO TỐC ĐỘ XỬ LÝ TRÊN CLOUD (Azure SQL)
# ============================================================

def benchmark_cloud_processing(csv_path: str, max_insert_rows: int = 100_000) -> dict:
    """
    Đo tốc độ xử lý dữ liệu trên Azure SQL Database.
    Insert một subset rồi chạy các query aggregation.
    """
    if pyodbc is None:
        print("[WARN] pyodbc chưa cài đặt. Bỏ qua cloud benchmark.")
        return {}

    print(f"\n[CLOUD] Bắt đầu benchmark Azure SQL (insert {max_insert_rows:,} dòng)...")
    results = {}

    conn_string = (
        f"Driver={SQL_DRIVER};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    try:
        conn = pyodbc.connect(conn_string, timeout=30)
    except Exception as e:
        print(f"[ERROR] Không kết nối được Azure SQL: {e}")
        print("[INFO] Đặt giá trị mẫu cho cloud benchmark.")
        results["cloud_insert_time_sec"] = "N/A (chưa kết nối)"
        results["cloud_query_time_sec"] = "N/A (chưa kết nối)"
        return results

    cursor = conn.cursor()

    # 3a. Insert dữ liệu
    insert_sql = """
        INSERT INTO SalesTransactions
            (event_time, store_id, product_id, units_sold, unit_price,
             revenue, temperature, weather, holiday, category)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    t0 = time.time()
    inserted = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            if inserted >= max_insert_rows:
                break
            batch.append((
                row["event_time"], row["store_id"], row["product_id"],
                int(row["units_sold"]), float(row["unit_price"]),
                float(row["revenue"]), float(row["temperature"]),
                row["weather"], int(row["holiday"]),
                row["category"],
            ))
            if len(batch) >= 1000:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                batch = []

        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)

    t_insert = time.time() - t0
    results["cloud_insert_rows"] = inserted
    results["cloud_insert_time_sec"] = round(t_insert, 2)
    results["cloud_insert_rows_per_sec"] = round(inserted / t_insert, 0) if t_insert > 0 else 0

    print(f"  Insert: {inserted:,} dòng trong {t_insert:.2f}s | "
          f"{results['cloud_insert_rows_per_sec']:.0f} rows/s")

    # 3b. Query aggregation trên cloud
    queries = {
        "COUNT_ALL": "SELECT COUNT(*) FROM SalesTransactions",
        "SUM_REVENUE": "SELECT SUM(revenue) FROM SalesTransactions",
        "GROUP_BY_STORE": """
            SELECT store_id, SUM(revenue) as total_revenue, COUNT(*) as cnt
            FROM SalesTransactions GROUP BY store_id ORDER BY total_revenue DESC
        """,
        "GROUP_BY_CATEGORY": """
            SELECT category, SUM(units_sold) as qty, AVG(revenue) as avg_rev
            FROM SalesTransactions GROUP BY category ORDER BY qty DESC
        """,
        "TOP_PRODUCTS": """
            SELECT TOP 10 product_id, SUM(revenue) as total_revenue
            FROM SalesTransactions GROUP BY product_id ORDER BY total_revenue DESC
        """,
    }

    query_times = {}
    for name, sql in queries.items():
        t0 = time.time()
        cursor.execute(sql)
        rows = cursor.fetchall()
        elapsed = time.time() - t0
        query_times[name] = round(elapsed, 4)
        print(f"  Query [{name}]: {elapsed:.4f}s | {len(rows)} kết quả")

    results["cloud_query_times"] = query_times
    results["cloud_total_query_time_sec"] = round(sum(query_times.values()), 4)

    cursor.close()
    conn.close()
    return results


# ============================================================
# 4. SO SÁNH & XUẤT BÁO CÁO
# ============================================================

def generate_comparison_report(local_results: dict, cloud_results: dict,
                                file_size_gb: float, output_path: str):
    """Xuất báo cáo so sánh local vs cloud."""
    report = {
        "benchmark_info": {
            "timestamp": datetime.now().isoformat(),
            "data_size_gb": round(file_size_gb, 2),
            "description": "So sánh tốc độ xử lý dữ liệu bán hàng giữa máy local và Azure SQL Database",
        },
        "local_results": local_results,
        "cloud_results": cloud_results,
        "comparison": {
            "note": "Cloud có ưu thế khi: nhiều query đồng thời, scale-out, "
                    "không giới hạn RAM/Disk. Local nhanh hơn cho batch processing file nhỏ.",
        },
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"  BÁO CÁO SO SÁNH LOCAL vs CLOUD")
    print(f"{'='*60}")
    print(f"  Kích thước dữ liệu: {file_size_gb:.2f} GB")
    print(f"  Local đọc CSV:      {local_results.get('local_read_time_sec', 'N/A')}s")
    print(f"  Local throughput:    {local_results.get('local_read_throughput_mbps', 'N/A')} MB/s")
    print(f"  Cloud insert:        {cloud_results.get('cloud_insert_time_sec', 'N/A')}s")
    print(f"  Cloud tổng query:    {cloud_results.get('cloud_total_query_time_sec', 'N/A')}s")
    print(f"  Báo cáo:             {output_path}")
    print(f"{'='*60}")

    return report


def main():
    output_dir = os.path.join(os.path.dirname(__file__), "..", "benchmark_output")
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, "sales_large_dataset.csv")
    report_path = os.path.join(output_dir, "benchmark_report.json")

    # Bước 1: Sinh dữ liệu lớn (>4GB)
    if not os.path.exists(csv_path):
        row_count, file_size = generate_large_dataset(csv_path, target_size_gb=4.5)
    else:
        file_size = os.path.getsize(csv_path)
        print(f"[INFO] File đã tồn tại: {csv_path} ({file_size / (1024**3):.2f} GB)")

    file_size_gb = file_size / (1024 ** 3)

    # Bước 2: Benchmark local
    local_results = benchmark_local_processing(csv_path)

    # Bước 3: Benchmark cloud
    cloud_results = benchmark_cloud_processing(csv_path, max_insert_rows=100_000)

    # Bước 4: Xuất báo cáo
    generate_comparison_report(local_results, cloud_results, file_size_gb, report_path)


if __name__ == "__main__":
    main()
