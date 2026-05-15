"""
Kiểm tra Azure SQL DB size, load thêm dữ liệu nếu < 0.75 GB,
sau đó chạy so sánh 10 tiêu chí Local vs Cloud.
"""
import os, sys, time, socket, threading, statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, ".")

import pyodbc
import polars as pl
from dotenv import load_dotenv
load_dotenv()

# ── Kết nối ──────────────────────────────────────────────────────────────────
conn_str = (
    f"DRIVER={os.getenv('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USERNAME')};"
    f"PWD={os.getenv('SQL_PASSWORD')};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

PARQUET_FILE = "benchmark_output/sales_large_dataset.parquet"
TARGET_GB    = 0.75
BATCH_SIZE   = 5_000

# ─────────────────────────────────────────────────────────────────────────────
def get_conn():
    c = pyodbc.connect(conn_str, timeout=30)
    c.autocommit = True
    return c

def check_db_state(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM SalesTransactions")
    rows = cur.fetchone()[0]
    cur.execute("""
        SELECT SUM(a.total_pages)*8/1024.0/1024.0, SUM(a.used_pages)*8/1024.0/1024.0
        FROM sys.tables t
        JOIN sys.indexes i ON t.OBJECT_ID=i.object_id
        JOIN sys.partitions p ON i.object_id=p.OBJECT_ID AND i.index_id=p.index_id
        JOIN sys.allocation_units a ON p.partition_id=a.container_id
        WHERE t.NAME='SalesTransactions'
    """)
    r = cur.fetchone()
    total_gb = float(r[0] or 0)
    used_gb  = float(r[1] or 0)
    return rows, total_gb, used_gb

def get_columns(conn):
    cur = conn.cursor()
    cur.execute("SELECT TOP 1 * FROM SalesTransactions")
    return [d[0] for d in cur.description]

def insert_batch(rows_data, cols):
    """Insert một batch rows, trả về số rows inserted."""
    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()
    cur.fast_executemany = True
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(cols)
    sql = f"INSERT INTO SalesTransactions ({col_list}) VALUES ({placeholders})"
    cur.executemany(sql, rows_data)
    conn.commit()
    conn.close()
    return len(rows_data)

# ─────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("  CHECK & LOAD — Azure SQL SalesTransactions")
print("=" * 70)

conn = get_conn()
rows_before, total_gb, used_gb = check_db_state(conn)
cols = get_columns(conn)
print(f"\n[DB State] Rows: {rows_before:,} | Total: {total_gb:.4f} GB | Used: {used_gb:.4f} GB")
print(f"[DB State] Columns: {cols}")

if used_gb >= TARGET_GB:
    print(f"\n[OK] DB already >= {TARGET_GB} GB. Không cần load thêm.")
else:
    needed_gb  = TARGET_GB - used_gb
    # Ước tính rows per GB (từ used_gb / rows_before)
    if rows_before > 0:
        gb_per_row = used_gb / rows_before
        rows_needed = int(needed_gb / gb_per_row) + 100_000  # buffer
    else:
        rows_needed = 3_000_000  # fallback
    
    print(f"\n[LOAD] Cần thêm ~{rows_needed:,} rows để đạt {TARGET_GB} GB")
    print(f"       Parquet source: {PARQUET_FILE}")

    # Đọc từ parquet, bỏ qua rows_before đầu tiên (đã có)
    t_load_start = time.time()
    df_iter = (
        pl.scan_parquet(PARQUET_FILE)
        .slice(rows_before, rows_needed + 500_000)
        .collect()
    )
    print(f"[LOAD] Loaded {len(df_iter):,} rows from parquet in {time.time()-t_load_start:.1f}s")

    # Map columns: chỉ lấy cols khớp với table
    df_cols = df_iter.columns
    matched_cols = [c for c in cols if c in df_cols]
    if not matched_cols:
        # Fallback: map by position
        matched_cols = cols[:len(df_cols)]
        df_sub = df_iter.to_pandas()[df_iter.columns[:len(cols)]]
        df_sub.columns = cols[:len(df_sub.columns)]
    else:
        df_sub = df_iter.select(matched_cols).to_pandas()

    insert_cols = list(df_sub.columns)
    total_to_insert = len(df_sub)
    inserted = 0
    errors = 0
    t0 = time.time()

    print(f"[LOAD] Bắt đầu insert {total_to_insert:,} rows (batch={BATCH_SIZE})...")

    for i in range(0, total_to_insert, BATCH_SIZE):
        chunk = df_sub.iloc[i:i+BATCH_SIZE]
        data  = list(chunk.itertuples(index=False, name=None))
        try:
            insert_batch(data, insert_cols)
            inserted += len(data)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  [WARN] Batch {i//BATCH_SIZE} error: {e}")

        # Progress mỗi 50 batches
        if (i // BATCH_SIZE) % 50 == 0 and i > 0:
            elapsed = time.time() - t0
            rate    = inserted / elapsed
            eta     = (total_to_insert - inserted) / rate if rate > 0 else 0
            pct     = inserted / total_to_insert * 100
            print(f"  [{pct:5.1f}%] {inserted:,}/{total_to_insert:,} | {rate:.0f} r/s | ETA {eta:.0f}s")

    elapsed_total = time.time() - t0
    print(f"\n[LOAD] Done: {inserted:,} rows in {elapsed_total:.1f}s | {inserted/elapsed_total:.0f} rows/s")

    # Verify
    rows_after, total_gb2, used_gb2 = check_db_state(conn)
    print(f"[DB State AFTER] Rows: {rows_after:,} | Total: {total_gb2:.4f} GB | Used: {used_gb2:.4f} GB")

    if used_gb2 < TARGET_GB:
        print(f"[WARN] Vẫn < {TARGET_GB} GB. Cần load thêm.")
    else:
        print(f"[OK] DB đạt {used_gb2:.4f} GB >= {TARGET_GB} GB ✓")

conn.close()

# ─────────────────────────────────────────────────────────────────────────────
print("\n")
print("=" * 70)
print("  SO SÁNH 10 TIÊU CHÍ — LOCAL vs CLOUD")
print("=" * 70)

SQL_SERVER_HOST = os.getenv("SQL_SERVER")

# Helpers
def timed(fn):
    t = time.perf_counter()
    r = fn()
    return r, (time.perf_counter() - t) * 1000  # ms

def sql_query(sql_str):
    c = get_conn()
    cur = c.cursor()
    t0 = time.perf_counter()
    cur.execute(sql_str)
    rows = cur.fetchall()
    ms = (time.perf_counter() - t0) * 1000
    c.close()
    return rows, ms

# ── [1] Đọc file local ───────────────────────────────────────────────────────
import psutil
proc = psutil.Process()
mem0 = proc.memory_info().rss
t0 = time.perf_counter()
df_local = pl.scan_parquet(PARQUET_FILE).collect()
local_read_ms = (time.perf_counter() - t0) * 1000
mem1 = proc.memory_info().rss
ram_delta_mb = (mem1 - mem0) / 1024 / 1024
local_rows = len(df_local)
local_gb   = os.path.getsize(PARQUET_FILE) / 1024**3
print(f"\n[1] Đọc file local... {local_rows:,} rows | {local_gb:.2f} GB | {local_read_ms/1000:.2f}s | RAM+{ram_delta_mb:.0f}MB")

# ── [2] COUNT(*) ─────────────────────────────────────────────────────────────
_, local_count_ms = timed(lambda: len(df_local))
(cloud_count_rows,), cloud_count_ms = sql_query("SELECT COUNT(*) FROM SalesTransactions")
print(f"[2] COUNT(*)  Local: {local_rows:,} | {local_count_ms:.2f}ms  |  Cloud: {cloud_count_rows:,} | {cloud_count_ms:.2f}ms")

# ── [3] SUM(revenue) ─────────────────────────────────────────────────────────
_, local_sum_ms = timed(lambda: df_local["revenue"].sum())
_, cloud_sum_ms = sql_query("SELECT SUM(revenue) FROM SalesTransactions")
print(f"[3] SUM(revenue)  Local: {local_sum_ms:.2f}ms  |  Cloud: {cloud_sum_ms:.2f}ms")

# ── [4] GROUP BY store_id ────────────────────────────────────────────────────
_, local_grp_ms = timed(lambda: df_local.group_by("store_id").agg(pl.col("revenue").sum()))
_, cloud_grp_ms = sql_query("SELECT store_id, SUM(revenue) FROM SalesTransactions GROUP BY store_id")
print(f"[4] GROUP BY store_id  Local: {local_grp_ms:.2f}ms  |  Cloud: {cloud_grp_ms:.2f}ms")

# ── [5] GROUP BY phức tạp ────────────────────────────────────────────────────
_, local_cplx_ms = timed(lambda: (
    df_local.group_by(["store_id","category"])
    .agg([pl.col("revenue").sum().alias("total_rev"),
          pl.col("revenue").mean().alias("avg_rev"),
          pl.col("revenue").count().alias("cnt")])
    .filter(pl.col("total_rev") > 1_000_000)
    .sort("total_rev", descending=True)
))
_, cloud_cplx_ms = sql_query("""
    SELECT store_id, category, SUM(revenue) total_rev, AVG(revenue) avg_rev, COUNT(*) cnt
    FROM SalesTransactions
    GROUP BY store_id, category
    HAVING SUM(revenue) > 1000000
    ORDER BY total_rev DESC
""")
print(f"[5] GROUP BY phức tạp  Local: {local_cplx_ms:.2f}ms  |  Cloud: {cloud_cplx_ms:.2f}ms")

# ── [6] Single INSERT ────────────────────────────────────────────────────────
print(f"[6] Single INSERT (100 rows)...")
import random, datetime
def rand_row():
    return (
        random.choice(["STORE_A","STORE_B","STORE_C"]),
        random.choice(["Electronics","Clothing","Food"]),
        round(random.uniform(10, 5000), 2),
        random.randint(1, 50),
        datetime.datetime.now()
    )
c6 = get_conn(); c6.autocommit = False; cur6 = c6.cursor()
N6 = 100
t6 = time.perf_counter()
for _ in range(N6):
    cur6.execute(
        "INSERT INTO SalesTransactions (store_id,category,revenue,quantity,timestamp) VALUES (?,?,?,?,?)",
        rand_row()
    )
c6.commit()
c6.close()
single_ms = (time.perf_counter() - t6) * 1000
print(f"   {N6} rows | {single_ms:.1f}ms | {N6/(single_ms/1000):.1f} rows/s")

# ── [7] Batch INSERT ─────────────────────────────────────────────────────────
print(f"[7] Batch INSERT (1000 rows fast_executemany)...")
c7 = get_conn(); c7.autocommit = False; cur7 = c7.cursor()
cur7.fast_executemany = True
batch_data = [rand_row() for _ in range(1000)]
t7 = time.perf_counter()
cur7.executemany(
    "INSERT INTO SalesTransactions (store_id,category,revenue,quantity,timestamp) VALUES (?,?,?,?,?)",
    batch_data
)
c7.commit(); c7.close()
batch_ms = (time.perf_counter() - t7) * 1000
print(f"   1000 rows | {batch_ms:.1f}ms | {1000/(batch_ms/1000):.1f} rows/s | {batch_ms/single_ms*10:.1f}× faster than single")

# ── [8] Network latency ──────────────────────────────────────────────────────
print("[8] Network latency...")
tcp_times = []
for _ in range(10):
    t = time.perf_counter()
    s = socket.create_connection((SQL_SERVER_HOST, 1433), timeout=10)
    tcp_times.append((time.perf_counter() - t)*1000)
    s.close()
tcp_avg = statistics.mean(tcp_times)

# SQL cold
t = time.perf_counter()
c8 = pyodbc.connect(conn_str, timeout=30)
cold_ms = (time.perf_counter() - t)*1000
# SELECT 1 warm
warm_times = []
cur8 = c8.cursor()
for _ in range(10):
    t = time.perf_counter()
    cur8.execute("SELECT 1"); cur8.fetchone()
    warm_times.append((time.perf_counter()-t)*1000)
c8.close()
warm_avg = statistics.mean(warm_times)
print(f"   TCP avg={tcp_avg:.1f}ms | cold={cold_ms:.1f}ms | SELECT1 avg={warm_avg:.1f}ms")

# ── [9] Concurrent users (5 threads) ────────────────────────────────────────
print("[9] Concurrent users (5 threads)...")
def concurrent_query(_):
    t = time.perf_counter()
    c = get_conn()
    c.cursor().execute("SELECT store_id, SUM(revenue) FROM SalesTransactions GROUP BY store_id").fetchall()
    c.close()
    return (time.perf_counter()-t)*1000

wall0 = time.perf_counter()
with ThreadPoolExecutor(max_workers=5) as ex:
    futs = [ex.submit(concurrent_query, i) for i in range(5)]
    times9 = [f.result() for f in futs]
wall9 = (time.perf_counter()-wall0)*1000
ok9   = len(times9)
print(f"   {ok9}/5 OK | avg={statistics.mean(times9):.1f}ms | wall={wall9:.1f}ms")

# ── [10] Availability ────────────────────────────────────────────────────────
print("[10] Availability (10 connections)...")
avail_times = []
for i in range(10):
    t = time.perf_counter()
    try:
        c = pyodbc.connect(conn_str, timeout=15)
        c.cursor().execute("SELECT 1").fetchone()
        c.close()
        avail_times.append((time.perf_counter()-t)*1000)
        print(f"   [{i+1:2d}] ✓ {avail_times[-1]:.1f}ms")
    except Exception as e:
        print(f"   [{i+1:2d}] ✗ {e}")
avail_pct = len(avail_times)/10*100
print(f"   Availability={avail_pct:.1f}% | avg={statistics.mean(avail_times):.1f}ms")

# ─────────────────────────────────────────────────────────────────────────────
# Final DB size
conn_final = get_conn()
rows_final, tgb, ugb = check_db_state(conn_final)
conn_final.close()

# ── BẢNG KẾT QUẢ ─────────────────────────────────────────────────────────────
print("\n" + "="*72)
print("  BẢNG SO SÁNH: MÁY CHỦ TRUYỀN THỐNG vs HỆ THỐNG CLOUD")
print(f"  Local : {local_rows:,} rows | {local_gb:.2f} GB Parquet")
print(f"  Cloud : {rows_final:,} rows | {ugb:.4f} GB (Azure SQL Standard S2)")
print("="*72)
fmt = "  {:<30} │ {:<35} │ {}"
print(fmt.format("Tiêu chí", "Máy chủ truyền thống (Local)", "Hệ thống Cloud (Azure SQL S2)"))
print("  " + "─"*30 + " │ " + "─"*35 + " │ " + "─"*35)
print(fmt.format("1. Đọc file",            f"{local_gb:.2f} GB Parquet → {local_read_ms/1000:.2f}s",     "N/A (data ở cloud)"))
print(fmt.format("2. COUNT(*)",             f"{local_rows:,} rows | {local_count_ms:.2f} ms",            f"{rows_final:,} rows | {cloud_count_ms:.1f} ms"))
print(fmt.format("3. SUM(revenue)",         f"{local_sum_ms:.2f} ms",                                     f"{cloud_sum_ms:.1f} ms"))
print(fmt.format("4. GROUP BY store_id",    f"{local_grp_ms:.2f} ms",                                     f"{cloud_grp_ms:.1f} ms"))
print(fmt.format("5. GROUP BY phức tạp",    f"{local_cplx_ms:.2f} ms",                                    f"{cloud_cplx_ms:.1f} ms"))
print(fmt.format("6. Single INSERT (100r)", "N/A",                                                         f"{N6/(single_ms/1000):.0f} r/s | {single_ms:.0f}ms"))
print(fmt.format("7. Batch INSERT (1000r)", "N/A",                                                         f"{1000/(batch_ms/1000):.0f} r/s | {batch_ms:.0f}ms"))
print(fmt.format("8. Network latency",      "0 ms (local RAM/SSD)",                                       f"TCP {tcp_avg:.1f}ms | cold {cold_ms:.1f}ms | q {warm_avg:.1f}ms"))
print(fmt.format("9. Concurrent (5 users)", "Không hỗ trợ",                                               f"5/5 OK | avg {statistics.mean(times9):.0f}ms"))
print(fmt.format("10. Availability",        "~99.9% (no HA)",                                              f"{avail_pct:.1f}% | avg {statistics.mean(avail_times):.1f}ms"))
print("="*72)

# Save JSON
import json
result = {
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    "local": {"rows": local_rows, "parquet_gb": local_gb, "read_s": local_read_ms/1000,
              "count_ms": local_count_ms, "sum_ms": local_sum_ms,
              "group_ms": local_grp_ms, "complex_group_ms": local_cplx_ms},
    "cloud": {"rows": rows_final, "db_used_gb": ugb,
              "count_ms": cloud_count_ms, "sum_ms": cloud_sum_ms,
              "group_ms": cloud_grp_ms, "complex_group_ms": cloud_cplx_ms,
              "single_insert_rows_per_s": N6/(single_ms/1000), "single_insert_ms": single_ms,
              "batch_insert_rows_per_s": 1000/(batch_ms/1000), "batch_insert_ms": batch_ms,
              "tcp_avg_ms": tcp_avg, "cold_connect_ms": cold_ms, "select1_avg_ms": warm_avg,
              "concurrent_5_avg_ms": statistics.mean(times9), "concurrent_wall_ms": wall9,
              "availability_pct": avail_pct, "availability_avg_ms": statistics.mean(avail_times)},
}
out = "benchmark_output/local_vs_cloud_v2.json"
with open(out, "w") as f:
    json.dump(result, f, indent=2)
print(f"\n[SAVED] {out}")
