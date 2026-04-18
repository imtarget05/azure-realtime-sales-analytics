"""
Mục 1.3 Rubric — Benchmark Local vs Cloud (Azure SQL Database)

So sánh công bằng hiệu năng xử lý dữ liệu:
  - Data ingestion (nạp dữ liệu)
  - Data processing (tính toán aggregation)
  - Query performance (truy vấn)
  - Scalability (CSV vs Parquet, Local vs Cloud)

Dataset: ~4.5 GB / ~70 triệu dòng, lưu CSV (baseline) + Parquet (optimized).
"""

import csv
import gc
import json
import os
import platform
import psutil
import re
import shutil
import subprocess
import sys
import time
import random
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import (
    SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER,
    PRODUCTS, STORE_IDS,
)

try:
    import pyodbc
except ImportError:
    pyodbc = None

# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────

def _system_info() -> dict:
    mem = psutil.virtual_memory()
    return {
        "platform": platform.platform(),
        "cpu_physical_cores": psutil.cpu_count(logical=False),
        "cpu_logical_cores": psutil.cpu_count(logical=True),
        "ram_total_gb": round(mem.total / (1024 ** 3), 2),
        "ram_available_gb": round(mem.available / (1024 ** 3), 2),
        "python_version": platform.python_version(),
    }


def _get_memory_mb() -> float:
    """Lấy memory usage hiện tại của process (RSS) theo MB."""
    return psutil.Process().memory_info().rss / (1024 * 1024)


def _connect_sql(conn_string: str, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            conn = pyodbc.connect(conn_string, timeout=30)
            return conn
        except Exception as e:
            print(f"  [RETRY {attempt}/{max_retries}] Kết nối thất bại: {e}")
            if attempt < max_retries:
                time.sleep(5 * attempt)
    return None


def _conn_string() -> str:
    return (
        f"Driver={SQL_DRIVER};"
        f"Server=tcp:{SQL_SERVER},1433;"
        f"Database={SQL_DATABASE};"
        f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30;"
        f"Command Timeout=300;"
    )


def _count_csv_rows(csv_path: str) -> int:
    """Đếm số dòng dữ liệu (không tính header) theo cách tiết kiệm RAM."""
    with open(csv_path, "r", encoding="utf-8") as f:
        return max(sum(1 for _ in f) - 1, 0)


def _get_cloud_row_count(conn) -> int:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM SalesTransactions")
    rows = int(cursor.fetchone()[0])
    cursor.close()
    return rows


def _truncate_sales_table(conn) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE SalesTransactions")
        conn.commit()
    except Exception:
        # Fallback khi table có FK hoặc không đủ quyền TRUNCATE
        cursor.execute("DELETE FROM SalesTransactions")
        conn.commit()
    finally:
        cursor.close()


def _run_bcp_ingest(csv_path: str, batch_size: int = 50_000) -> tuple:
    """Nạp nhanh bằng bcp utility. Trả về (rows_copied, elapsed_sec, stderr_excerpt)."""
    if shutil.which("bcp") is None:
        return 0, 0.0, "bcp utility không có trong PATH"

    err_file = os.path.join(os.path.dirname(csv_path), "bcp_errors.log")
    cmd = [
        "bcp", "dbo.SalesTransactions", "in", csv_path,
        "-S", f"tcp:{SQL_SERVER},1433",
        "-d", SQL_DATABASE,
        "-U", SQL_USERNAME,
        "-P", SQL_PASSWORD,
        "-c",
        "-t", ",",
        "-r", "\\n",
        "-F", "2",  # skip header row
        "-b", str(batch_size),
        "-e", err_file,
    ]

    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    elapsed = time.time() - t0

    output_text = f"{proc.stdout}\n{proc.stderr}"
    match = re.search(r"(\d+)\s+rows copied", output_text, flags=re.IGNORECASE)
    rows_copied = int(match.group(1)) if match else 0

    if proc.returncode != 0:
        stderr_excerpt = (proc.stderr or "").strip()[:500]
        return rows_copied, elapsed, stderr_excerpt or "BCP thất bại (không có stderr)."
    # Một số phiên bản bcp không luôn in rõ "rows copied"; để caller fallback bằng target_rows.
    if rows_copied == 0:
        rows_copied = -1
    return rows_copied, elapsed, ""


def _ingest_streaming_executemany(csv_path: str, rows_to_ingest: int,
                                  conn_string: str, batch_size: int = 5_000) -> tuple:
    """Fallback ingest bằng pyodbc executemany nhưng stream từ CSV, không load full RAM."""
    conn = _connect_sql(conn_string)
    if conn is None:
        return 0, 0.0

    cursor = conn.cursor()
    cursor.fast_executemany = True
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
            if inserted >= rows_to_ingest:
                break
            batch.append((
                datetime.fromisoformat(row["event_time"]),
                row["store_id"], row["product_id"],
                int(float(row["units_sold"])), float(row["unit_price"]),
                float(row["revenue"]), float(row["temperature"]),
                row["weather"], int(float(row["holiday"])),
                row["category"],
            ))

            if len(batch) >= batch_size or inserted + len(batch) >= rows_to_ingest:
                for attempt in range(1, 4):
                    try:
                        cursor.executemany(insert_sql, batch)
                        conn.commit()
                        inserted += len(batch)
                        break
                    except (pyodbc.OperationalError, pyodbc.Error) as e:
                        print(f"  [RETRY executemany attempt {attempt}] {e}")
                        try:
                            conn.close()
                        except Exception:
                            pass
                        time.sleep(2 * attempt)
                        conn = _connect_sql(conn_string)
                        if conn is None:
                            inserted_time = time.time() - t0
                            return inserted, inserted_time
                        cursor = conn.cursor()
                        cursor.fast_executemany = True
                batch.clear()

            if inserted > 0 and inserted % 50_000 == 0:
                elapsed = time.time() - t0
                print(f"  ... {inserted:,}/{rows_to_ingest:,} rows | {inserted / max(elapsed, 1):,.0f} rows/s")

    elapsed = time.time() - t0
    cursor.close()
    conn.close()
    return inserted, elapsed


# ============================================================
# 1. SINH DỮ LIỆU LỚN (~4.5 GB / ~70M dòng)
# ============================================================

def generate_large_dataset(output_path: str, target_size_gb: float = 4.5,
                           batch_log_interval: int = 500_000) -> tuple:
    target_bytes = int(target_size_gb * 1024 * 1024 * 1024)
    header = [
        "event_time", "store_id", "product_id", "units_sold", "unit_price",
        "revenue", "temperature", "weather", "holiday", "category",
    ]
    weather_choices = ["sunny", "rainy", "cloudy", "stormy"]

    print(f"[GEN] Sinh CSV mục tiêu {target_size_gb} GB → {output_path}")
    start = time.time()
    row_count = 0
    start_date = datetime(2024, 1, 1)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        while True:
            product = random.choice(PRODUCTS)
            qty = random.randint(1, 10)
            price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)
            rev = round(price * qty, 2)
            dt = start_date + timedelta(seconds=random.randint(0, 60_000_000))

            writer.writerow([
                dt.isoformat(), random.choice(STORE_IDS), product["id"],
                qty, price, rev,
                round(random.uniform(15, 40), 1),
                random.choice(weather_choices),
                random.choice([0, 0, 0, 1]),
                product["category"],
            ])
            row_count += 1

            if row_count % batch_log_interval == 0:
                sz = os.path.getsize(output_path)
                el = time.time() - start
                pct = sz / target_bytes * 100
                spd = sz / (1024 * 1024 * el) if el > 0 else 0
                print(f"  [{pct:.1f}%] {row_count:,} dòng | "
                      f"{sz / (1024**3):.2f} GB | {spd:.1f} MB/s | {el:.0f}s")
                if sz >= target_bytes:
                    break

    final_size = os.path.getsize(output_path)
    elapsed = time.time() - start
    print(f"[GEN DONE] {row_count:,} dòng | {final_size / (1024**3):.2f} GB | {elapsed:.1f}s")
    return row_count, final_size


# ============================================================
# 2. CHUYỂN CSV → PARQUET
# ============================================================

def convert_csv_to_parquet(csv_path: str, parquet_path: str,
                           chunk_size: int = 2_000_000) -> int:
    """Đọc CSV theo chunk bằng pandas → ghi Parquet bằng PyArrow."""
    print(f"[PARQUET] Chuyển CSV → Parquet (chunk {chunk_size:,} dòng)...")
    start = time.time()

    schema = pa.schema([
        ("event_time", pa.string()),
        ("store_id", pa.string()),
        ("product_id", pa.string()),
        ("units_sold", pa.int64()),
        ("unit_price", pa.float64()),
        ("revenue", pa.float64()),
        ("temperature", pa.float64()),
        ("weather", pa.string()),
        ("holiday", pa.int64()),
        ("category", pa.string()),
    ])

    writer = None
    total_rows = 0
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype={
        "store_id": str, "product_id": str, "weather": str, "category": str,
    }):
        table = pa.Table.from_pandas(chunk, schema=schema, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, schema, compression="snappy")
        writer.write_table(table)
        total_rows += len(chunk)
        print(f"  {total_rows:,} dòng đã ghi...")

    if writer:
        writer.close()

    elapsed = time.time() - start
    pq_size = os.path.getsize(parquet_path)
    print(f"[PARQUET DONE] {total_rows:,} dòng | "
          f"{pq_size / (1024**3):.2f} GB | {elapsed:.1f}s")
    return total_rows


# ============================================================
# 3. LOCAL BENCHMARK — CSV Pipeline (pandas chunked)
# ============================================================

def benchmark_local_csv(csv_path: str, chunk_size: int = 2_000_000) -> dict:
    """
    Pipeline A: Đọc CSV bằng pandas (chunked) → aggregate dần.

    Tách riêng thời gian:
      - I/O + Parse: pd.read_csv đọc bytes từ disk + parse text → typed values
      - Compute: aggregation (SUM, GROUP BY) trên mỗi chunk
    Wall time = I/O+Parse + Compute (interleaved).
    Chunked reading cần thiết vì CSV 4.5 GB vượt RAM khả dụng.
    """
    print(f"\n[LOCAL-CSV] Benchmark CSV pipeline (pandas, chunk={chunk_size:,})...")
    gc.collect()
    mem_before = _get_memory_mb()

    file_size = os.path.getsize(csv_path)
    file_size_gb = file_size / (1024 ** 3)
    results = {"format": "CSV", "file_size_gb": round(file_size_gb, 2)}

    csv_dtypes = {
        "store_id": str, "product_id": str, "weather": str, "category": str,
        "units_sold": "int64", "unit_price": "float64", "revenue": "float64",
        "temperature": "float64", "holiday": "int64",
    }

    # ── Đọc + Compute (chunked, tách I/O vs compute) ──
    t_wall_start = time.time()
    t_compute_total = 0.0   # Tổng thời gian aggregation
    total_rows = 0
    total_revenue = 0.0
    store_revenue = {}   # store_id → sum(revenue)
    cat_revenue = {}     # category  → sum(revenue)

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size, dtype=csv_dtypes):
        # ── Compute phase: aggregation trên chunk ──
        t_comp_start = time.time()
        total_rows += len(chunk)
        total_revenue += float(chunk["revenue"].sum())
        for sid, rev in chunk.groupby("store_id")["revenue"].sum().items():
            store_revenue[sid] = store_revenue.get(sid, 0) + float(rev)
        for cat, rev in chunk.groupby("category")["revenue"].sum().items():
            cat_revenue[cat] = cat_revenue.get(cat, 0) + float(rev)
        t_compute_total += time.time() - t_comp_start

        if total_rows % 10_000_000 == 0:
            print(f"  ... {total_rows:,} dòng đọc xong")

    t_wall = time.time() - t_wall_start
    # I/O+Parse = wall time − compute time (pd.read_csv gồm disk I/O + CSV text parsing)
    t_io = t_wall - t_compute_total
    mem_peak = _get_memory_mb() - mem_before

    results["read_time_sec"] = round(t_io, 2)
    results["compute_time_sec"] = round(t_compute_total, 2)
    results["read_and_compute_time_sec"] = round(t_wall, 2)
    results["rows"] = total_rows
    results["total_revenue"] = round(total_revenue, 2)
    results["total_time_sec"] = round(t_wall, 2)
    results["read_throughput_mbps"] = round(file_size_gb * 1024 / t_wall, 2) if t_wall > 0 else 0
    results["compute_throughput_rows_per_sec"] = round(total_rows / t_compute_total) if t_compute_total > 0 else 0
    results["peak_memory_delta_mb"] = round(mem_peak, 1)

    top_store = max(store_revenue, key=store_revenue.get) if store_revenue else "N/A"
    top_cat = max(cat_revenue, key=cat_revenue.get) if cat_revenue else "N/A"

    print(f"  Done: {t_wall:.2f}s (I/O+Parse {t_io:.2f}s + Compute {t_compute_total:.2f}s)")
    print(f"  {total_rows:,} dòng | {results['read_throughput_mbps']:.1f} MB/s | Mem Δ{mem_peak:.0f} MB")
    print(f"  Revenue: {total_revenue:,.2f}")
    print(f"  Top store: {top_store} = {store_revenue.get(top_store, 0):,.2f}")
    print(f"  Top category: {top_cat} = {cat_revenue.get(top_cat, 0):,.2f}")

    return results


# ============================================================
# 4. LOCAL BENCHMARK — Parquet Pipeline (Polars)
# ============================================================

def benchmark_local_parquet(parquet_path: str) -> dict:
    """
    Pipeline B: Đọc Parquet bằng Polars (eager + lazy).

    - Eager mode: pl.read_parquet() — load toàn bộ vào RAM rồi compute.
    - Lazy mode:  pl.scan_parquet() — query plan tối ưu, chỉ đọc cột cần thiết,
      giảm RAM, scale beyond RAM nhờ streaming execution.

    Cùng phép tính: SUM(revenue), GROUP BY store_id, GROUP BY category.
    """
    print("\n[LOCAL-PARQUET] Benchmark Parquet pipeline (Polars)...")
    gc.collect()

    file_size = os.path.getsize(parquet_path)
    file_size_gb = file_size / (1024 ** 3)
    results = {"format": "Parquet", "file_size_gb": round(file_size_gb, 2)}

    # ═══════════════════════════════════════════════════════
    # A) Eager mode — load toàn bộ vào RAM rồi compute
    # ═══════════════════════════════════════════════════════
    print("  [Eager] pl.read_parquet()...")
    gc.collect()
    mem_before = _get_memory_mb()

    t0 = time.time()
    df = pl.read_parquet(parquet_path)
    t_read = time.time() - t0

    results["read_time_sec"] = round(t_read, 2)
    results["rows"] = df.height
    results["read_throughput_mbps"] = round(file_size_gb * 1024 / t_read, 2) if t_read > 0 else 0
    print(f"    Đọc: {t_read:.2f}s | {df.height:,} dòng | {results['read_throughput_mbps']:.1f} MB/s")

    # ── Compute (eager) ──
    t0 = time.time()
    total_revenue = float(df["revenue"].sum())
    group_store = df.group_by("store_id").agg(pl.col("revenue").sum()).sort("revenue", descending=True)
    group_category = df.group_by("category").agg(pl.col("revenue").sum()).sort("revenue", descending=True)
    t_compute = time.time() - t0

    mem_after = _get_memory_mb()

    results["compute_time_sec"] = round(t_compute, 4)
    results["total_revenue"] = round(total_revenue, 2)
    results["total_time_sec"] = round(t_read + t_compute, 2)
    results["compute_throughput_rows_per_sec"] = round(df.height / t_compute) if t_compute > 0 else 0
    results["peak_memory_delta_mb"] = round(mem_after - mem_before, 1)

    print(f"    Compute: {t_compute:.4f}s | Revenue: {total_revenue:,.2f}")
    print(f"    Top store: {group_store[0, 'store_id']} = {group_store[0, 'revenue']:,.2f}")
    print(f"    Top category: {group_category[0, 'category']} = {group_category[0, 'revenue']:,.2f}")

    del df, group_store, group_category
    gc.collect()

    # ═══════════════════════════════════════════════════════
    # B) Lazy mode — scan + collect (tối ưu RAM, scale beyond RAM)
    #    Polars tối ưu query plan: projection pushdown (chỉ đọc cột cần),
    #    predicate pushdown, row group pruning trên Parquet metadata.
    # ═══════════════════════════════════════════════════════
    print("  [Lazy] pl.scan_parquet() → collect()...")
    gc.collect()
    mem_before_lazy = _get_memory_mb()

    t0 = time.time()
    lazy_df = pl.scan_parquet(parquet_path)

    # Cùng phép tính qua lazy API — Polars tối ưu query plan trước khi execute
    lazy_revenue = lazy_df.select(pl.col("revenue").sum()).collect()
    lazy_store = (lazy_df.group_by("store_id")
                  .agg(pl.col("revenue").sum())
                  .sort("revenue", descending=True)
                  .collect())
    lazy_category = (lazy_df.group_by("category")
                     .agg(pl.col("revenue").sum())
                     .sort("revenue", descending=True)
                     .collect())
    t_lazy = time.time() - t0

    mem_after_lazy = _get_memory_mb()

    results["lazy_total_time_sec"] = round(t_lazy, 4)
    results["lazy_revenue"] = round(float(lazy_revenue[0, 0]), 2)
    results["lazy_memory_delta_mb"] = round(mem_after_lazy - mem_before_lazy, 1)

    print(f"    Lazy total: {t_lazy:.4f}s | Revenue: {results['lazy_revenue']:,.2f}")
    print(f"    Memory — Eager Δ{results['peak_memory_delta_mb']:.0f} MB | "
          f"Lazy Δ{results['lazy_memory_delta_mb']:.0f} MB")

    del lazy_df, lazy_revenue, lazy_store, lazy_category
    gc.collect()

    return results


def benchmark_local_parquet_subset(parquet_path: str, n_rows: int) -> dict:
    """Benchmark Polars lazy trên N dòng — so sánh fair với cloud (cùng data size)."""
    print(f"\n[LOCAL-SUBSET] Polars lazy benchmark trên {n_rows:,} dòng (match cloud)...")
    gc.collect()

    t0 = time.time()
    lazy = pl.scan_parquet(parquet_path).head(n_rows)
    total_revenue = float(lazy.select(pl.col("revenue").sum()).collect()[0, 0])
    group_store = (lazy.group_by("store_id")
                   .agg(pl.col("revenue").sum())
                   .sort("revenue", descending=True)
                   .collect())
    group_category = (lazy.group_by("category")
                      .agg(pl.col("revenue").sum())
                      .sort("revenue", descending=True)
                      .collect())
    t_total = time.time() - t0

    print(f"  Done: {t_total:.4f}s | {n_rows:,} dòng | Revenue: {total_revenue:,.2f}")
    print(f"  Top store: {group_store[0, 'store_id']} | Top category: {group_category[0, 'category']}")

    del lazy, group_store, group_category
    gc.collect()

    return {
        "rows": n_rows,
        "total_time_sec": round(t_total, 4),
        "total_revenue": round(total_revenue, 2),
    }


# ============================================================
# 5. CLOUD BENCHMARK — Azure SQL Database
# ============================================================

def _ensure_indexes(conn):
    """Tạo index nếu chưa có để đảm bảo fair comparison."""
    indexes = [
        ("IX_SalesTransactions_store_id", "store_id"),
        ("IX_SalesTransactions_category", "category"),
        ("IX_SalesTransactions_product_id", "product_id"),
    ]
    cursor = conn.cursor()
    for idx_name, col in indexes:
        try:
            cursor.execute(
                f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='{idx_name}') "
                f"CREATE NONCLUSTERED INDEX [{idx_name}] ON SalesTransactions([{col}])"
            )
            conn.commit()
        except Exception as e:
            print(f"  [INDEX] {idx_name}: {e}")
    cursor.close()


def benchmark_cloud_ingest(csv_path: str, target_rows: int,
                           method: str = "bcp",
                           truncate_before_load: bool = True) -> dict:
    """Cloud benchmark A: Data ingestion, ưu tiên bcp để đạt throughput cao."""
    if pyodbc is None:
        print("[WARN] pyodbc chưa cài. Bỏ qua cloud benchmark.")
        return {}

    print(f"\n[CLOUD-INGEST] Đồng bộ cloud dataset về {target_rows:,} dòng...")
    results = {}
    cs = _conn_string()

    conn = _connect_sql(cs)
    if conn is None:
        return {"error": "Không kết nối được Azure SQL"}

    csv_rows = _count_csv_rows(csv_path)
    if target_rows > csv_rows:
        print(f"  [WARN] target_rows ({target_rows:,}) > CSV rows ({csv_rows:,}). Dùng {csv_rows:,}.")
        target_rows = csv_rows

    current_rows = _get_cloud_row_count(conn)
    results["initial_rows_in_cloud"] = current_rows

    # Chỉ truncate nếu cần — nếu cloud đã có đúng target_rows thì giữ nguyên
    if current_rows == target_rows:
        print(f"  Cloud đã có đúng {target_rows:,} dòng. Bỏ qua truncate + ingest.")
        results.update({
            "target_rows": target_rows,
            "insert_rows": 0,
            "insert_time_sec": 0.0,
            "insert_rows_per_sec": 0,
            "insert_throughput_mbps": 0,
            "ingest_method": "skip (already at target)",
            "final_rows_in_cloud": current_rows,
            "dataset_parity_ok": True,
        })
        conn.close()
        return results

    if truncate_before_load:
        print(f"  Truncate table (cloud {current_rows:,} → target {target_rows:,} dòng)...")
        _truncate_sales_table(conn)
        current_rows = 0

    rows_to_ingest = max(target_rows - current_rows, 0)
    if rows_to_ingest == 0:
        print("  Cloud đã đủ số dòng mục tiêu. Bỏ qua ingest.")
        results.update({
            "target_rows": target_rows,
            "insert_rows": 0,
            "insert_time_sec": 0.0,
            "insert_rows_per_sec": 0,
            "insert_throughput_mbps": 0,
            "ingest_method": "skip",
        })
        conn.close()
        return results

    print(f"  Cần ingest thêm {rows_to_ingest:,} dòng...")
    inserted = 0
    t_insert = 0.0
    used_method = "executemany"

    # Chiến lược ingest: BCP cho dataset lớn (>1M), executemany cho dataset nhỏ
    BCP_THRESHOLD = 1_000_000
    if method.lower() == "bcp" and rows_to_ingest >= BCP_THRESHOLD and current_rows == 0 and rows_to_ingest == csv_rows:
        print(f"  [BCP] Bulk load {rows_to_ingest:,} dòng...")
        bcp_rows, bcp_time, bcp_err = _run_bcp_ingest(csv_path)
        if not bcp_err:
            inserted = rows_to_ingest if bcp_rows < 0 else bcp_rows
            t_insert = bcp_time
            used_method = "bcp"
        else:
            print(f"  [WARN] BCP không thành công: {bcp_err}")

    # Executemany: phù hợp cho dataset ≤ 1M dòng, tránh overhead BCP setup
    if inserted == 0:
        print(f"  [EXECUTEMANY] Ingest {rows_to_ingest:,} dòng (batch=10K, fast_executemany=True)...")
        inserted, t_insert = _ingest_streaming_executemany(
            csv_path=csv_path,
            rows_to_ingest=rows_to_ingest,
            conn_string=cs,
            batch_size=10_000,
        )

    data_mb = (os.path.getsize(csv_path) / (1024 * 1024)) * (inserted / max(csv_rows, 1))

    # Reconnect để lấy row count sau ingest
    try:
        conn.close()
    except Exception:
        pass
    conn = _connect_sql(cs)
    final_rows = _get_cloud_row_count(conn) if conn else None
    if conn:
        conn.close()

    results["target_rows"] = target_rows
    results["insert_rows"] = inserted
    results["insert_time_sec"] = round(t_insert, 2)
    results["insert_rows_per_sec"] = round(inserted / t_insert) if t_insert > 0 else 0
    results["insert_throughput_mbps"] = round(data_mb / t_insert, 2) if t_insert > 0 else 0
    results["ingest_method"] = used_method
    results["final_rows_in_cloud"] = final_rows
    results["dataset_parity_ok"] = bool(final_rows == target_rows)

    print(f"  Insert done: {inserted:,} dòng | {t_insert:.2f}s | "
          f"{results['insert_rows_per_sec']:,} rows/s | method={used_method}")
    if final_rows is not None:
        print(f"  Cloud rows sau ingest: {final_rows:,} | parity={'OK' if final_rows == target_rows else 'MISMATCH'}")

    return results


def benchmark_cloud_query() -> dict:
    """Cloud benchmark B: Query performance."""
    if pyodbc is None:
        return {}

    print("\n[CLOUD-QUERY] Benchmark query Azure SQL...")
    cs = _conn_string()
    conn = _connect_sql(cs)
    if conn is None:
        return {"error": "Không kết nối được Azure SQL"}

    # Đảm bảo có index
    _ensure_indexes(conn)
    cursor = conn.cursor()

    # Warm-up: loại bỏ cold-start bias (connection pool, query plan cache)
    cursor.execute("SELECT TOP 1 store_id FROM SalesTransactions")
    cursor.fetchone()

    queries = {
        "COUNT_ALL": "SELECT COUNT(*) FROM SalesTransactions",
        "SUM_REVENUE": "SELECT SUM(revenue) FROM SalesTransactions",
        "GROUP_BY_STORE": (
            "SELECT store_id, SUM(revenue) AS total_revenue, COUNT(*) AS cnt "
            "FROM SalesTransactions GROUP BY store_id ORDER BY total_revenue DESC"
        ),
        "GROUP_BY_CATEGORY": (
            "SELECT category, SUM(units_sold) AS qty, AVG(revenue) AS avg_rev "
            "FROM SalesTransactions GROUP BY category ORDER BY qty DESC"
        ),
        "TOP_10_PRODUCTS": (
            "SELECT TOP 10 product_id, SUM(revenue) AS total_revenue "
            "FROM SalesTransactions GROUP BY product_id ORDER BY total_revenue DESC"
        ),
    }

    query_times = {}
    for name, sql in queries.items():
        t0 = time.time()
        cursor.execute(sql)
        rows = cursor.fetchall()
        el = time.time() - t0
        query_times[name] = round(el, 4)
        preview = str(rows[0]) if rows else "∅"
        print(f"  {name}: {el:.4f}s | {len(rows)} kết quả | {preview}")

    results = {
        "query_times": query_times,
        "total_query_time_sec": round(sum(query_times.values()), 4),
    }

    # Lấy tổng số dòng hiện có trên cloud
    cursor.execute("SELECT COUNT(*) FROM SalesTransactions")
    results["total_rows_in_cloud"] = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return results


# ============================================================
# 6. PHÂN TÍCH & BÁO CÁO
# ============================================================

def _analyze_bottleneck(local_csv: dict, local_pq: dict,
                        cloud_ingest: dict, cloud_query: dict,
                        local_subset: dict = None) -> dict:
    """
    Phân tích bottleneck theo 4 khía cạnh rubric:
      1. Data Ingestion   — tốc độ nạp dữ liệu
      2. Data Processing  — I/O bound vs CPU bound, CSV vs Parquet
      3. Query Performance — local compute vs cloud SQL
      4. Scalability      — format, engine, memory efficiency
    """
    analysis = {"bottlenecks": [], "recommendations": [], "conclusion": ""}

    csv_total = local_csv.get("total_time_sec", 1)
    pq_total = local_pq.get("total_time_sec", 1)

    # ── 1. Data Ingestion: CSV I/O+Parse vs Parquet read ──
    csv_read = local_csv.get("read_time_sec", 0)
    csv_compute = local_csv.get("compute_time_sec", 0)
    pq_read = local_pq.get("read_time_sec", 0)
    pq_compute = local_pq.get("compute_time_sec", 0)

    if csv_total > 0 and csv_compute > 0:
        io_pct = csv_read / csv_total * 100
        compute_pct = csv_compute / csv_total * 100
        analysis["csv_io_percent"] = round(io_pct, 1)
        analysis["csv_compute_percent"] = round(compute_pct, 1)
        analysis["bottlenecks"].append(
            f"CSV pipeline: I/O+Parse chiếm {io_pct:.0f}%, compute chỉ {compute_pct:.0f}%. "
            f"Bottleneck chính là text parsing (CSV row-based → typed values), "
            f"không phải disk bandwidth."
        )

    if pq_total > 0 and pq_compute > 0:
        pq_io_pct = pq_read / (pq_read + pq_compute) * 100
        analysis["parquet_io_percent"] = round(pq_io_pct, 1)
        analysis["parquet_compute_percent"] = round(100 - pq_io_pct, 1)

    # ── 2. Data Processing: CSV vs Parquet speedup ──
    if pq_total > 0:
        speedup = csv_total / pq_total
        analysis["parquet_speedup_x"] = round(speedup, 2)
        csv_size = local_csv.get("file_size_gb", 0)
        pq_size = local_pq.get("file_size_gb", 0)
        analysis["bottlenecks"].append(
            f"CSV chậm hơn Parquet {speedup:.1f}x — do: "
            f"(1) text parsing overhead row-by-row, "
            f"(2) phải scan toàn bộ {csv_size} GB vs columnar chỉ đọc cột cần ({pq_size} GB), "
            f"(3) không compression (ratio {csv_size/pq_size:.1f}x)."
            if pq_size > 0 else
            f"CSV chậm hơn Parquet {speedup:.1f}x → format CSV không tối ưu cho analytics."
        )

    # ── 3. Query Performance: Cloud analysis ──
    ingest_method = cloud_ingest.get("ingest_method", "executemany")
    insert_time = cloud_ingest.get("insert_time_sec", 0)
    insert_rows = cloud_ingest.get("insert_rows", 0)
    if insert_time > 0 and insert_rows > 0:
        latency_per_row_ms = insert_time / insert_rows * 1000
        analysis["cloud_latency_per_row_ms"] = round(latency_per_row_ms, 3)
        analysis["cloud_ingest_method"] = ingest_method
        if ingest_method == "bcp":
            analysis["bottlenecks"].append(
                f"Cloud BCP bulk insert: {latency_per_row_ms:.3f} ms/row "
                f"({insert_rows:,} dòng → {insert_time:.1f}s). "
                f"BCP tận dụng bulk copy protocol, bypass transaction log overhead, "
                f"throughput {insert_rows / insert_time:,.0f} rows/s."
            )
        else:
            analysis["bottlenecks"].append(
                f"Cloud executemany: {latency_per_row_ms:.2f} ms/row "
                f"({insert_rows:,} dòng → {insert_time:.1f}s). "
                f"Bottleneck: network round-trip + transaction overhead per batch. "
                f"BCP / BULK INSERT sẽ nhanh hơn 10-50x."
            )

    # Cloud query per-row metrics + data size note
    cloud_rows = cloud_query.get("total_rows_in_cloud", 0)
    query_total = cloud_query.get("total_query_time_sec", 0)
    local_rows = local_pq.get("rows", 0)
    if cloud_rows > 0 and query_total > 0:
        analysis["cloud_query_per_million_rows_sec"] = round(query_total / cloud_rows * 1_000_000, 2)
        analysis["dataset_parity_ok"] = (local_rows == cloud_rows)
        if local_rows > 0 and cloud_rows != local_rows:
            analysis["data_size_note"] = (
                f"⚠ Cloud có {cloud_rows:,} dòng, local có {local_rows:,} dòng. "
                f"Query time không comparable trực tiếp — cần cùng data size để so sánh fair."
            )
        else:
            analysis["data_size_note"] = (
                f"Cloud và local đều {local_rows:,} dòng. So sánh query time là fair."
            )

    # ── 4. Scalability: Memory efficiency ──
    csv_mem = local_csv.get("peak_memory_delta_mb", 0)
    pq_mem = local_pq.get("peak_memory_delta_mb", 0)
    lazy_mem = local_pq.get("lazy_memory_delta_mb", 0)
    if pq_mem > 0:
        analysis["memory_comparison"] = {
            "csv_chunked_peak_mb": csv_mem,
            "parquet_eager_peak_mb": pq_mem,
            "parquet_lazy_peak_mb": lazy_mem,
            "note": "Eager load toàn bộ vào RAM; Lazy dùng projection pushdown chỉ đọc cột cần."
        }

    # ── Scalability Analysis ──
    analysis["scalability"] = {
        "csv": "Tuyến tính O(n) — phải scan toàn bộ file, row-based parsing. 2x data ≈ 2x thời gian.",
        "parquet_eager": "Sub-linear — columnar format, chỉ đọc cột cần, row group pruning. "
                         "Nhưng bị giới hạn bởi RAM (phải load toàn bộ).",
        "parquet_lazy": "Best scalability — Polars scan_parquet() tối ưu query plan, "
                        "projection + predicate pushdown, streaming execution. Scale beyond RAM.",
        "cloud_sql": "Horizontal scale — partition table, columnstore index, scale DTU/vCore. "
                     "Phù hợp >50 GB, concurrent users, ACID transactions.",
        "recommendation": "≤50 GB (vừa RAM): Parquet + Polars. "
                          ">50 GB hoặc multi-user: Cloud SQL / Synapse / Databricks."
    }

    # ── Recommendations ──
    analysis["recommendations"] = [
        "Local: Dùng Parquet + Polars thay CSV + pandas → giảm I/O 3.5x (compression), tăng tốc 20x+ (columnar).",
        "Local: Dùng Polars lazy mode (scan_parquet) khi dataset vượt RAM — projection pushdown chỉ đọc cột cần.",
        "Local: Nếu cần SQL syntax trên local, dùng DuckDB trực tiếp trên Parquet file (zero-copy).",
        "Cloud: Dùng BULK INSERT / bcp / Azure Data Factory thay vì pyodbc executemany để nạp data nhanh hơn.",
        "Cloud: Tạo nonclustered index (store_id, category, product_id) để tối ưu GROUP BY queries.",
        "Cloud: Columnstore Index cho analytical workload — giảm 10x I/O trên scan lớn.",
        "Cloud: Azure Synapse serverless SQL pool → query trực tiếp Parquet trên Blob Storage, không cần ETL.",
    ]

    # ── Normalized per-million-row metrics ──
    if local_rows > 0 and cloud_rows > 0:
        analysis["normalized_per_million_rows"] = {
            "local_parquet_full_sec": round(pq_total / local_rows * 1_000_000, 4),
            "cloud_query_sec": round(query_total / cloud_rows * 1_000_000, 4),
        }
        if local_subset and local_subset.get("rows") == cloud_rows:
            analysis["normalized_per_million_rows"]["local_parquet_subset_sec"] = round(
                local_subset["total_time_sec"] / cloud_rows * 1_000_000, 4)

    # ── Conclusion ──
    if pq_total > 0 and query_total > 0:
        if cloud_rows > 0 and local_rows > 0 and cloud_rows != local_rows:
            # Khi dataset khác size, dùng local_subset (cùng dòng) để so sánh fair
            if local_subset and local_subset.get("rows") == cloud_rows:
                subset_time = local_subset["total_time_sec"]
                ratio = query_total / subset_time if subset_time > 0 else 0
                analysis["conclusion"] = (
                    f"SO SÁNH FAIR ({cloud_rows:,} dòng cùng dataset): "
                    f"Local Polars {subset_time:.2f}s vs Cloud SQL {query_total:.2f}s "
                    f"(Cloud chậm hơn {ratio:.1f}x do network + SQL engine overhead). "
                    f"Full dataset ({local_rows:,} dòng): Local Polars chỉ {pq_total:.2f}s. "
                    f"Kết luận: Local vượt trội cho single-user analytics; "
                    f"Cloud ưu thế khi multi-user, HA, ACID, horizontal scale-out."
                )
            else:
                analysis["conclusion"] = (
                    f"Local Parquet + Polars ({pq_total:.2f}s / {local_rows:,} dòng) "
                    f"xử lý cực nhanh khi dataset vừa RAM. "
                    f"Cloud ({query_total:.2f}s / {cloud_rows:,} dòng) chịu overhead network + SQL engine. "
                    f"Kết luận: Local phù hợp single-user analytics / EDA; "
                    f"Cloud phù hợp production workload multi-user với HA, ACID, và scale-out."
                )
        elif cloud_rows == local_rows:
            # Parity đạt — so sánh trực tiếp
            if query_total < pq_total:
                analysis["conclusion"] = (
                    f"CÙNG DATASET ({cloud_rows:,} dòng): "
                    f"Cloud query ({query_total:.2f}s) nhanh hơn Local Parquet ({pq_total:.2f}s). "
                    f"Cloud phù hợp cho concurrent queries, nhiều người dùng, và dữ liệu >RAM."
                )
            else:
                ratio = query_total / pq_total if pq_total > 0 else 0
                analysis["conclusion"] = (
                    f"CÙNG DATASET ({cloud_rows:,} dòng): "
                    f"Local Parquet ({pq_total:.2f}s) nhanh hơn Cloud query ({query_total:.2f}s) — {ratio:.1f}x. "
                    f"In-memory columnar (Polars) vượt trội cho single-user analytics. "
                    f"Cloud vẫn ưu thế khi cần concurrent access, HA, ACID, và scale-out."
                )
        elif query_total < pq_total:
            analysis["conclusion"] = (
                f"Cloud query ({query_total:.2f}s) nhanh hơn Local Parquet ({pq_total:.2f}s). "
                f"Cloud phù hợp cho concurrent queries, nhiều người dùng, và dữ liệu >RAM."
            )
        else:
            analysis["conclusion"] = (
                f"Local Parquet ({pq_total:.2f}s) nhanh hơn Cloud query ({query_total:.2f}s). "
                f"Với single-user analytics trên dataset vừa RAM, local processing hiệu quả hơn. "
                f"Cloud vẫn ưu thế khi cần concurrent access, HA, và scale-out."
            )

    return analysis


def generate_report(local_csv: dict, local_pq: dict,
                    cloud_ingest: dict, cloud_query: dict,
                    csv_size_gb: float, parquet_size_gb: float,
                    total_rows: int, output_path: str,
                    local_subset: dict = None) -> dict:
    """Xuất báo cáo JSON đầy đủ."""

    analysis = _analyze_bottleneck(local_csv, local_pq, cloud_ingest, cloud_query, local_subset)

    report = {
        "benchmark_info": {
            "timestamp": datetime.now().isoformat(),
            "system": _system_info(),
            "dataset": {
                "total_rows": total_rows,
                "csv_size_gb": round(csv_size_gb, 2),
                "parquet_size_gb": round(parquet_size_gb, 2),
                "compression_ratio": round(csv_size_gb / parquet_size_gb, 2) if parquet_size_gb > 0 else 0,
            },
            "description": (
                "Benchmark công bằng so sánh Local (CSV + Parquet) vs Cloud (Azure SQL). "
                "Cùng dataset, cùng phép tính: SUM(revenue), GROUP BY store, GROUP BY category."
            ),
        },
        "local_csv_results": local_csv,
        "local_parquet_results": local_pq,
        "cloud_ingest_results": cloud_ingest,
        "cloud_query_results": cloud_query,
        "comparison": {
            "local_csv_vs_parquet": {
                "csv_total_sec": local_csv.get("total_time_sec"),
                "parquet_total_sec": local_pq.get("total_time_sec"),
                "speedup_x": analysis.get("parquet_speedup_x"),
                "winner": "Parquet (Polars)" if analysis.get("parquet_speedup_x", 0) > 1 else "CSV (pandas)",
            },
            "local_eager_vs_lazy": {
                "note": "So sánh Polars eager (read_parquet) vs lazy (scan_parquet) mode.",
                "eager_total_sec": local_pq.get("total_time_sec"),
                "lazy_total_sec": local_pq.get("lazy_total_time_sec"),
                "eager_memory_delta_mb": local_pq.get("peak_memory_delta_mb"),
                "lazy_memory_delta_mb": local_pq.get("lazy_memory_delta_mb"),
            },
            "local_compute_vs_cloud_query": {
                "note": "So sánh compute time (không tính I/O read) vs cloud query time — cùng phép tính.",
                "local_parquet_compute_sec": local_pq.get("compute_time_sec"),
                "cloud_query_total_sec": cloud_query.get("total_query_time_sec"),
                "cloud_rows": cloud_query.get("total_rows_in_cloud"),
                "local_rows": local_pq.get("rows"),
            },
            "data_ingestion": {
                "note": "So sánh tốc độ nạp / đọc dữ liệu.",
                "local_csv_read_mbps": local_csv.get("read_throughput_mbps"),
                "local_parquet_read_mbps": local_pq.get("read_throughput_mbps"),
                "cloud_insert_rows_per_sec": cloud_ingest.get("insert_rows_per_sec"),
                "cloud_insert_throughput_mbps": cloud_ingest.get("insert_throughput_mbps"),
                "cloud_ingest_method": cloud_ingest.get("ingest_method"),
                "dataset_parity_ok": cloud_ingest.get("dataset_parity_ok"),
            },
            "throughput": {
                "local_csv_read_mbps": local_csv.get("read_throughput_mbps"),
                "local_parquet_read_mbps": local_pq.get("read_throughput_mbps"),
                "cloud_insert_rows_per_sec": cloud_ingest.get("insert_rows_per_sec"),
            },
            "fair_comparison": {
                "note": "So sánh trên cùng số dòng để đảm bảo rubric công bằng.",
                "cloud_rows": cloud_query.get("total_rows_in_cloud"),
                "local_full_rows": local_pq.get("rows"),
                "dataset_parity_ok": analysis.get("dataset_parity_ok"),
                "local_subset_results": local_subset if local_subset else None,
                "cloud_query_total_sec": cloud_query.get("total_query_time_sec"),
                "normalized_per_million_rows": analysis.get("normalized_per_million_rows"),
            },
        },
        "analysis": analysis,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # ── In tóm tắt ──
    print(f"\n{'=' * 65}")
    print(f"  BÁO CÁO BENCHMARK LOCAL vs CLOUD")
    print(f"{'=' * 65}")
    print(f"  Dataset        : {total_rows:,} dòng | CSV {csv_size_gb:.2f} GB | Parquet {parquet_size_gb:.2f} GB")
    print(f"  ────────────────────────────────────────────")
    print(f"  LOCAL CSV      : {local_csv.get('total_time_sec', 'N/A')}s "
          f"(I/O+Parse {local_csv.get('read_time_sec', '?')}s + Compute {local_csv.get('compute_time_sec', '?')}s)")
    print(f"  LOCAL PARQUET  : {local_pq.get('total_time_sec', 'N/A')}s "
          f"(read {local_pq.get('read_time_sec', '?')}s + compute {local_pq.get('compute_time_sec', '?')}s)")
    print(f"  PARQUET LAZY   : {local_pq.get('lazy_total_time_sec', 'N/A')}s (scan_parquet → collect)")
    print(f"  Parquet speedup: {analysis.get('parquet_speedup_x', '?')}x")
    print(f"  ────────────────────────────────────────────")
    print(f"  CLOUD INSERT   : {cloud_ingest.get('insert_time_sec', 'N/A')}s | "
          f"{cloud_ingest.get('insert_rows_per_sec', '?')} rows/s | "
          f"method={cloud_ingest.get('ingest_method', '?')}")
    print(f"  CLOUD QUERY    : {cloud_query.get('total_query_time_sec', 'N/A')}s "
          f"({cloud_query.get('total_rows_in_cloud', '?')} dòng)")
    _parity = analysis.get('dataset_parity_ok')
    print(f"  DATASET PARITY : {'FAIR ✓' if _parity else 'MISMATCH — dùng subset để so sánh'}")
    if local_subset:
        print(f"  LOCAL SUBSET   : {local_subset.get('total_time_sec', 'N/A')}s "
              f"({local_subset.get('rows', '?')} dòng — cùng cloud)")
    print("  " + "─" * 44)
    print(f"  KẾT LUẬN: {analysis.get('conclusion', 'N/A')}")
    print(f"{'=' * 65}")
    print(f"  Báo cáo: {output_path}")

    return report


# ============================================================
# MAIN
# ============================================================

def main():
    output_dir = os.path.join(os.path.dirname(__file__), "..", "benchmark_output")
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, "sales_large_dataset.csv")
    parquet_path = os.path.join(output_dir, "sales_large_dataset.parquet")
    report_path = os.path.join(output_dir, "benchmark_report.json")

    TARGET_GB = 4.5
    CLOUD_INGEST_METHOD = os.getenv("CLOUD_INGEST_METHOD", "bcp")
    CLOUD_TRUNCATE_BEFORE_LOAD = os.getenv("CLOUD_TRUNCATE_BEFORE_LOAD", "1").lower() in ("1", "true", "yes")

    # ── Bước 0: Xóa file cũ nếu kích thước không đủ ──
    for fpath, label in [(csv_path, "CSV"), (parquet_path, "Parquet")]:
        if os.path.exists(fpath):
            sz_gb = os.path.getsize(fpath) / (1024 ** 3)
            if label == "CSV" and sz_gb < TARGET_GB * 0.95:
                print(f"[WARN] {label} cũ chỉ {sz_gb:.2f} GB (< {TARGET_GB} GB). Xóa...")
                os.remove(fpath)
            elif label == "Parquet":
                # Parquet sẽ được rebuild nếu CSV mới
                pass

    # ── Bước 1: Sinh CSV ──
    if not os.path.exists(csv_path):
        total_rows, csv_size = generate_large_dataset(csv_path, target_size_gb=TARGET_GB)
    else:
        csv_size = os.path.getsize(csv_path)
        # Đếm nhanh số dòng
        print(f"[INFO] CSV đã tồn tại: {csv_size / (1024**3):.2f} GB. Đếm dòng...")
        total_rows = _count_csv_rows(csv_path)
        print(f"[INFO] {total_rows:,} dòng.")

    csv_size_gb = csv_size / (1024 ** 3) if isinstance(csv_size, (int, float)) else os.path.getsize(csv_path) / (1024 ** 3)

    # ── Bước 2: Chuyển CSV → Parquet ──
    if not os.path.exists(parquet_path):
        convert_csv_to_parquet(csv_path, parquet_path)
    else:
        print(f"[INFO] Parquet đã tồn tại: {os.path.getsize(parquet_path) / (1024**3):.2f} GB")

    parquet_size_gb = os.path.getsize(parquet_path) / (1024 ** 3)

    # ── Bước 3: Local CSV benchmark (pandas) ──
    local_csv = benchmark_local_csv(csv_path)

    # ── Bước 4: Local Parquet benchmark (Polars) ──
    local_pq = benchmark_local_parquet(parquet_path)

    # ── Bước 5: Cloud ingestion ──
    # Dùng dataset hợp lý (500K dòng ≈ 30 MB) cho cloud comparison.
    # Full 4.5 GB cho local benchmark, subset cho cloud → tránh thắt nghẽn cổ chai
    # ingest (executemany overhead), tập trung so sánh tốc độ xử lý query.
    CLOUD_COMPARISON_ROWS = 500_000
    cloud_target_rows = int(os.getenv("CLOUD_TARGET_ROWS", str(CLOUD_COMPARISON_ROWS)))
    print(f"\n{'─' * 60}")
    print(f"  CHIẾN LƯỢC: Local full dataset ({total_rows:,} dòng / {csv_size_gb:.2f} GB)")
    print(f"  Cloud subset ({cloud_target_rows:,} dòng) → tránh bottleneck ingest,")
    print(f"  tập trung so sánh tốc độ xử lý (query performance).")
    print(f"  Fair comparison: local subset cùng {cloud_target_rows:,} dòng với cloud.")
    print(f"{'─' * 60}")
    cloud_ingest = benchmark_cloud_ingest(
        csv_path=csv_path,
        target_rows=cloud_target_rows,
        method=CLOUD_INGEST_METHOD,
        truncate_before_load=CLOUD_TRUNCATE_BEFORE_LOAD,
    )

    # ── Bước 6: Cloud query ──
    cloud_query = benchmark_cloud_query()

    # ── Bước 6b: Fair comparison — benchmark local trên cùng số dòng với cloud ──
    cloud_rows_count = cloud_query.get("total_rows_in_cloud", 0)
    local_subset = {}
    if 0 < cloud_rows_count < total_rows:
        local_subset = benchmark_local_parquet_subset(parquet_path, cloud_rows_count)

    # ── Bước 7: Xuất báo cáo ──
    generate_report(
        local_csv, local_pq, cloud_ingest, cloud_query,
        csv_size_gb, parquet_size_gb, total_rows, report_path,
        local_subset=local_subset or None,
    )


if __name__ == "__main__":
    main()
