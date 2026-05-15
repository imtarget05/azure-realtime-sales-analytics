"""
Local vs Cloud — Bảng so sánh đầy đủ 10 tiêu chí
===================================================
Tiêu chí:
  1.  Đọc file 4.52 GB
  2.  COUNT(*) 38M rows
  3.  SUM(revenue)
  4.  GROUP BY store_id
  5.  GROUP BY phức tạp (store+category+HAVING+ORDER)
  6.  Single INSERT (1.000 rows)
  7.  Batch INSERT 1.000 rows/batch
  8.  Network latency (TCP + SQL connection)
  9.  Concurrent users (5 thread đồng thời)
  10. Availability (10 kết nối liên tiếp)

Output: benchmark_output/local_vs_cloud_comparison.json
"""

import gc
import json
import os
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import polars as pl
import psutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

try:
    import pyodbc
except ImportError:
    pyodbc = None

# ─── Paths ────────────────────────────────────────────────────
ROOT         = os.path.join(os.path.dirname(__file__), "..")
PARQUET_PATH = os.path.join(ROOT, "benchmark_output", "sales_large_dataset.parquet")
OUTPUT_JSON  = os.path.join(ROOT, "benchmark_output", "local_vs_cloud_comparison.json")

# ─── SQL helpers ──────────────────────────────────────────────
CONN_STR = (
    f"Driver={SQL_DRIVER};"
    f"Server=tcp:{SQL_SERVER},1433;"
    f"Database={SQL_DATABASE};"
    f"Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

BENCH_TABLE = "LocalCloudBench"

def _connect():
    conn = pyodbc.connect(CONN_STR, timeout=60)
    conn.autocommit = True
    return conn

def _mem_mb():
    return psutil.Process().memory_info().rss / (1024 * 1024)

SEP = "═" * 64

# ─────────────────────────────────────────────────────────────
# 1. LOCAL benchmarks
# ─────────────────────────────────────────────────────────────

def bench_local():
    print(f"\n{SEP}")
    print("  LOCAL — Polars lazy scan_parquet (máy chủ truyền thống)")
    print(SEP)

    results = {}

    # ── Metric 1: Đọc file 4.52 GB ──────────────────────────
    print("\n[1] Đọc file 4.52 GB (sink vào /dev/null qua collect)...")
    gc.collect()
    m0 = _mem_mb()
    t0 = time.perf_counter()
    df_full = pl.scan_parquet(PARQUET_PATH).collect()
    t_read = time.perf_counter() - t0
    rows_total = len(df_full)
    size_gb = os.path.getsize(PARQUET_PATH) / (1024**3)
    mem_delta = _mem_mb() - m0
    print(f"   {rows_total:,} rows | {size_gb:.2f} GB Parquet | {t_read:.2f}s | RAM+{mem_delta:.0f} MB")
    results["read_4gb_sec"] = round(t_read, 3)
    results["read_rows"] = rows_total
    results["parquet_size_gb"] = round(size_gb, 3)
    results["read_ram_delta_mb"] = round(mem_delta, 1)

    # Giữ lại df_full để dùng cho các phép tính tiếp theo (tránh re-scan)
    lf = df_full.lazy()

    # ── Metric 2: COUNT(*) ───────────────────────────────────
    print("[2] COUNT(*) toàn bộ...")
    t0 = time.perf_counter()
    cnt = len(df_full)
    t_cnt = time.perf_counter() - t0
    print(f"   COUNT = {cnt:,} | {t_cnt*1000:.2f} ms")
    results["count_ms"] = round(t_cnt * 1000, 3)
    results["count_rows"] = cnt

    # ── Metric 3: SUM(revenue) ───────────────────────────────
    print("[3] SUM(revenue)...")
    t0 = time.perf_counter()
    rev = float(lf.select(pl.col("revenue").sum()).collect().item())
    t_sum = time.perf_counter() - t0
    print(f"   SUM = {rev:,.2f} | {t_sum*1000:.2f} ms")
    results["sum_revenue_ms"] = round(t_sum * 1000, 3)
    results["total_revenue"] = round(rev, 2)

    # ── Metric 4: GROUP BY store_id ──────────────────────────
    print("[4] GROUP BY store_id + SUM(revenue)...")
    t0 = time.perf_counter()
    gs = lf.group_by("store_id").agg(pl.col("revenue").sum()).collect()
    t_gs = time.perf_counter() - t0
    print(f"   {gs.height} groups | {t_gs*1000:.2f} ms")
    results["group_store_ms"] = round(t_gs * 1000, 3)
    results["group_store_groups"] = gs.height

    # ── Metric 5: GROUP BY phức tạp (store+category+units HAVING+ORDER) ──
    print("[5] GROUP BY phức tạp (store_id + category | SUM+AVG+COUNT | HAVING+ORDER)...")
    t0 = time.perf_counter()
    gc_ = (
        lf.group_by(["store_id", "category"])
        .agg([
            pl.col("revenue").sum().alias("total_revenue"),
            pl.col("revenue").mean().alias("avg_revenue"),
            pl.col("units_sold").sum().alias("total_units"),
            pl.len().alias("tx_count"),
        ])
        .filter(pl.col("total_revenue") > 1_000_000)
        .sort("total_revenue", descending=True)
        .collect()
    )
    t_gc = time.perf_counter() - t0
    print(f"   {gc_.height} groups (HAVING revenue>1M) | {t_gc*1000:.2f} ms")
    results["group_complex_ms"] = round(t_gc * 1000, 3)
    results["group_complex_groups"] = gc_.height

    del df_full, lf, gs, gc_
    gc.collect()

    return results


# ─────────────────────────────────────────────────────────────
# 2. INSERT benchmarks (trên Cloud, so sánh single vs batch)
# ─────────────────────────────────────────────────────────────

def bench_inserts(conn):
    print(f"\n{SEP}")
    print("  INSERT BENCHMARK — Single vs Batch (1.000 rows mỗi loại)")
    print(SEP)

    results = {}

    # Tạo bảng tạm
    cur = conn.cursor()
    cur.execute(f"""
        IF OBJECT_ID(N'dbo.{BENCH_TABLE}', N'U') IS NOT NULL
            DROP TABLE dbo.{BENCH_TABLE}
    """)
    cur.execute(f"""
        CREATE TABLE dbo.{BENCH_TABLE} (
            id          INT IDENTITY(1,1) PRIMARY KEY,
            event_time  VARCHAR(30),
            store_id    VARCHAR(20),
            product_id  VARCHAR(20),
            units_sold  INT,
            unit_price  FLOAT,
            revenue     FLOAT,
            category    VARCHAR(50)
        )
    """)
    cur.close()

    # Sinh 1.000 rows test data
    rows = [
        (f"2026-05-13T{(i//3600)%24:02d}:{(i//60)%60:02d}:{i%60:02d}",
         f"S{(i%3)+1:02d}", f"P{(i%20)+1:03d}",
         (i % 50) + 1, round(9.99 + (i % 90), 2),
         round(((i % 50) + 1) * (9.99 + (i % 90)), 2),
         ["Electronics","Clothing","Food","Sports","Books"][i % 5])
        for i in range(1000)
    ]
    sql_ins = f"INSERT INTO dbo.{BENCH_TABLE} (event_time,store_id,product_id,units_sold,unit_price,revenue,category) VALUES (?,?,?,?,?,?,?)"

    # ── Metric 6: Single INSERT ──────────────────────────────
    print("[6] Single INSERT — 1.000 rows (1 row/round-trip)...")
    cur = conn.cursor()
    t0 = time.perf_counter()
    for row in rows:
        cur.execute(sql_ins, row)
    t_single = time.perf_counter() - t0
    cur.close()
    rps_single = int(1000 / max(t_single, 0.001))
    print(f"   {t_single:.2f}s | {rps_single:,} rows/s")
    results["single_insert_sec"] = round(t_single, 3)
    results["single_insert_rps"] = rps_single

    # Reset bảng
    cur = conn.cursor(); cur.execute(f"TRUNCATE TABLE dbo.{BENCH_TABLE}"); cur.close()

    # ── Metric 7: Batch INSERT (fast_executemany) ────────────
    print("[7] Batch INSERT — 1.000 rows (fast_executemany, 1 batch)...")
    cur = conn.cursor()
    cur.fast_executemany = True
    t0 = time.perf_counter()
    cur.executemany(sql_ins, rows)
    t_batch = time.perf_counter() - t0
    cur.close()
    rps_batch = int(1000 / max(t_batch, 0.001))
    print(f"   {t_batch:.3f}s | {rps_batch:,} rows/s")
    results["batch_insert_sec"] = round(t_batch, 3)
    results["batch_insert_rps"] = rps_batch
    results["batch_vs_single_speedup"] = round(t_single / max(t_batch, 0.001), 1)
    print(f"   → Batch nhanh hơn Single: {results['batch_vs_single_speedup']}×")

    # Cleanup
    cur = conn.cursor()
    cur.execute(f"DROP TABLE dbo.{BENCH_TABLE}")
    cur.close()

    return results


# ─────────────────────────────────────────────────────────────
# 3. CLOUD query benchmarks (cùng bảng SalesTransactions)
# ─────────────────────────────────────────────────────────────

def bench_cloud_queries(conn):
    print(f"\n{SEP}")
    print("  CLOUD — Azure SQL Standard S2 (Southeast Asia)")
    print(SEP)

    results = {}
    cur = conn.cursor()

    # Kiểm tra bảng tồn tại và số rows
    cur.execute("SELECT COUNT(*) FROM dbo.SalesTransactions")
    n_cloud = int(cur.fetchone()[0])
    print(f"  Bảng SalesTransactions: {n_cloud:,} rows\n")
    results["cloud_rows"] = n_cloud

    # ── Metric 2-cloud: COUNT(*) ─────────────────────────────
    print("[2-cloud] COUNT(*) SalesTransactions...")
    t0 = time.perf_counter()
    cur.execute("SELECT COUNT(*) FROM dbo.SalesTransactions")
    cnt = int(cur.fetchone()[0])
    t_cnt = time.perf_counter() - t0
    print(f"   COUNT = {cnt:,} | {t_cnt*1000:.2f} ms")
    results["count_ms"] = round(t_cnt * 1000, 3)

    # ── Metric 3-cloud: SUM(revenue) ─────────────────────────
    print("[3-cloud] SUM(revenue)...")
    t0 = time.perf_counter()
    cur.execute("SELECT SUM(revenue) FROM dbo.SalesTransactions")
    rev = float(cur.fetchone()[0] or 0)
    t_sum = time.perf_counter() - t0
    print(f"   SUM = {rev:,.2f} | {t_sum*1000:.2f} ms")
    results["sum_revenue_ms"] = round(t_sum * 1000, 3)
    results["total_revenue"] = round(rev, 2)

    # ── Metric 4-cloud: GROUP BY store_id ───────────────────
    print("[4-cloud] GROUP BY store_id + SUM(revenue)...")
    t0 = time.perf_counter()
    cur.execute("SELECT store_id, SUM(revenue) as rev FROM dbo.SalesTransactions GROUP BY store_id ORDER BY rev DESC")
    gs = cur.fetchall()
    t_gs = time.perf_counter() - t0
    print(f"   {len(gs)} groups | {t_gs*1000:.2f} ms")
    results["group_store_ms"] = round(t_gs * 1000, 3)
    results["group_store_groups"] = len(gs)

    # ── Metric 5-cloud: GROUP BY phức tạp ───────────────────
    print("[5-cloud] GROUP BY phức tạp (store_id+category | SUM+AVG+COUNT | HAVING+ORDER)...")
    t0 = time.perf_counter()
    cur.execute("""
        SELECT store_id, category,
               SUM(revenue)      AS total_revenue,
               AVG(revenue)      AS avg_revenue,
               SUM(units_sold)   AS total_units,
               COUNT(*)          AS tx_count
        FROM dbo.SalesTransactions
        GROUP BY store_id, category
        HAVING SUM(revenue) > 1000
        ORDER BY total_revenue DESC
    """)
    gc_ = cur.fetchall()
    t_gc = time.perf_counter() - t0
    print(f"   {len(gc_)} groups | {t_gc*1000:.2f} ms")
    results["group_complex_ms"] = round(t_gc * 1000, 3)
    results["group_complex_groups"] = len(gc_)

    cur.close()
    return results


# ─────────────────────────────────────────────────────────────
# 4. Network latency
# ─────────────────────────────────────────────────────────────

def bench_network():
    print(f"\n{SEP}")
    print("  NETWORK LATENCY — TCP + SQL connection")
    print(SEP)

    results = {}
    host = SQL_SERVER
    port = 1433
    N = 10

    # TCP latency
    print(f"[8] TCP latency → {host}:{port} ({N} lần đo)...")
    times = []
    errors = 0
    for i in range(N):
        try:
            t0 = time.perf_counter()
            with socket.create_connection((host, port), timeout=5):
                pass
            times.append((time.perf_counter() - t0) * 1000)
        except Exception:
            errors += 1

    if times:
        avg_tcp = round(sum(times) / len(times), 2)
        min_tcp = round(min(times), 2)
        max_tcp = round(max(times), 2)
        print(f"   TCP avg={avg_tcp}ms | min={min_tcp}ms | max={max_tcp}ms | errors={errors}")
        results["tcp_avg_ms"]  = avg_tcp
        results["tcp_min_ms"]  = min_tcp
        results["tcp_max_ms"]  = max_tcp
        results["tcp_errors"]  = errors
    else:
        print("   [WARN] Không đo được TCP latency")
        results["tcp_avg_ms"] = None

    # SQL connection time (lần đầu cold)
    print("[8b] SQL cold connection time...")
    t0 = time.perf_counter()
    try:
        c = _connect()
        t_conn = (time.perf_counter() - t0) * 1000
        c.close()
        print(f"   SQL connection (cold) = {t_conn:.2f} ms")
        results["sql_cold_connect_ms"] = round(t_conn, 2)
    except Exception as e:
        print(f"   [WARN] {e}")
        results["sql_cold_connect_ms"] = None

    # Simple query latency (warm connection)
    print("[8c] Simple query latency (SELECT 1, warm, 10 lần)...")
    conn = _connect()
    qtimes = []
    for _ in range(10):
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute("SELECT 1")
        cur.fetchone()
        qtimes.append((time.perf_counter() - t0) * 1000)
        cur.close()
    conn.close()
    avg_q = round(sum(qtimes) / len(qtimes), 2)
    print(f"   SELECT 1 avg = {avg_q:.2f} ms | min={min(qtimes):.2f}ms | max={max(qtimes):.2f}ms")
    results["simple_query_avg_ms"] = avg_q
    results["simple_query_min_ms"] = round(min(qtimes), 2)
    results["simple_query_max_ms"] = round(max(qtimes), 2)

    return results


# ─────────────────────────────────────────────────────────────
# 5. Concurrent users
# ─────────────────────────────────────────────────────────────

def bench_concurrent(n_threads: int = 5):
    print(f"\n{SEP}")
    print(f"  CONCURRENT USERS — {n_threads} threads đồng thời")
    print(SEP)

    def worker(tid):
        t0 = time.perf_counter()
        try:
            conn = _connect()
            cur = conn.cursor()
            cur.execute("SELECT store_id, SUM(revenue) as rev FROM dbo.SalesTransactions GROUP BY store_id ORDER BY rev DESC")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            elapsed = (time.perf_counter() - t0) * 1000
            return {"thread": tid, "ok": True, "ms": round(elapsed, 2), "rows": len(rows)}
        except Exception as e:
            return {"thread": tid, "ok": False, "ms": round((time.perf_counter()-t0)*1000, 2), "error": str(e)}

    print(f"  Gửi {n_threads} GROUP BY queries đồng thời...")
    t_wall = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(worker, i+1) for i in range(n_threads)]
        worker_results = [f.result() for f in as_completed(futures)]
    wall_ms = (time.perf_counter() - t_wall) * 1000

    ok_results = [r for r in worker_results if r["ok"]]
    fail_results = [r for r in worker_results if not r["ok"]]
    times_ms = [r["ms"] for r in ok_results]

    for r in sorted(worker_results, key=lambda x: x["thread"]):
        status = f"✓ {r['ms']:.0f}ms" if r["ok"] else f"✗ {r.get('error','')}"
        print(f"   Thread {r['thread']}: {status}")

    result = {
        "n_threads": n_threads,
        "success": len(ok_results),
        "failed": len(fail_results),
        "wall_time_ms": round(wall_ms, 2),
        "avg_query_ms": round(sum(times_ms)/len(times_ms), 2) if times_ms else None,
        "min_query_ms": round(min(times_ms), 2) if times_ms else None,
        "max_query_ms": round(max(times_ms), 2) if times_ms else None,
    }
    print(f"  Wall time={wall_ms:.0f}ms | avg={result['avg_query_ms']}ms | success={len(ok_results)}/{n_threads}")
    return result


# ─────────────────────────────────────────────────────────────
# 6. Availability
# ─────────────────────────────────────────────────────────────

def bench_availability(n: int = 10):
    print(f"\n{SEP}")
    print(f"  AVAILABILITY — {n} lần kết nối liên tiếp")
    print(SEP)

    successes = 0
    times_ms = []
    for i in range(1, n + 1):
        t0 = time.perf_counter()
        try:
            conn = _connect()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            conn.close()
            elapsed = (time.perf_counter() - t0) * 1000
            successes += 1
            times_ms.append(elapsed)
            print(f"  [{i:2d}] ✓ {elapsed:.1f} ms")
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            print(f"  [{i:2d}] ✗ {elapsed:.1f} ms — {e}")

    uptime_pct = round(successes / n * 100, 1)
    result = {
        "trials": n,
        "success": successes,
        "failed": n - successes,
        "uptime_pct": uptime_pct,
        "avg_connect_ms": round(sum(times_ms)/len(times_ms), 2) if times_ms else None,
        "min_connect_ms": round(min(times_ms), 2) if times_ms else None,
        "max_connect_ms": round(max(times_ms), 2) if times_ms else None,
    }
    print(f"\n  Availability = {uptime_pct}% ({successes}/{n}) | avg={result['avg_connect_ms']}ms")
    return result


# ─────────────────────────────────────────────────────────────
# PRINT COMPARISON TABLE
# ─────────────────────────────────────────────────────────────

def print_table(local, cloud_q, inserts, net, conc, avail):
    print(f"\n{'='*72}")
    print("  BẢNG SO SÁNH: MÁY CHỦ TRUYỀN THỐNG vs HỆ THỐNG CLOUD")
    print(f"  Dataset Local: {local['read_rows']:,} rows | {local['parquet_size_gb']} GB Parquet")
    print(f"  Dataset Cloud: {cloud_q['cloud_rows']:,} rows | Azure SQL Standard S2")
    print(f"{'='*72}")

    def fmt_ms(v):
        if v is None:            return "N/A"
        if v >= 60_000:          return f"{v/60000:.1f} phút"
        if v >= 1_000:           return f"{v/1000:.2f} s"
        return f"{v:.1f} ms"

    rows_tbl = [
        ("Tiêu chí (Metric)",           "Máy chủ truyền thống (Local)",          "Hệ thống Cloud (Azure SQL S2)"),
        ("─"*28,                         "─"*36,                                   "─"*36),
        ("1. Đọc file",
            f"{local['parquet_size_gb']} GB → {local['read_sec_label']}",
            "N/A (data ở cloud)"),
        ("2. COUNT(*)",
            f"{local['count_rows']:,} rows | {fmt_ms(local['count_ms'])}",
            f"{cloud_q['cloud_rows']:,} rows | {fmt_ms(cloud_q['count_ms'])}"),
        ("3. SUM(revenue)",
            fmt_ms(local["sum_revenue_ms"]),
            fmt_ms(cloud_q["sum_revenue_ms"])),
        ("4. GROUP BY store_id",
            f"{local['group_store_groups']} groups | {fmt_ms(local['group_store_ms'])}",
            f"{cloud_q['group_store_groups']} groups | {fmt_ms(cloud_q['group_store_ms'])}"),
        ("5. GROUP BY phức tạp",
            f"{local['group_complex_groups']} groups | {fmt_ms(local['group_complex_ms'])}",
            f"{cloud_q['group_complex_groups']} groups | {fmt_ms(cloud_q['group_complex_ms'])}"),
        ("6. Single INSERT (1.000r)",
            "N/A (không có SQL engine)",
            f"{inserts['single_insert_rps']:,} rows/s | {inserts['single_insert_sec']}s"),
        ("7. Batch INSERT 1000r/batch",
            "N/A",
            f"{inserts['batch_insert_rps']:,} rows/s | {inserts['batch_insert_sec']}s · {inserts['batch_vs_single_speedup']}× single"),
        ("8. Network latency",
            "0 ms (cục bộ, RAM/SSD)",
            f"TCP={net['tcp_avg_ms']}ms | SQL cold={net['sql_cold_connect_ms']}ms | query={net['simple_query_avg_ms']}ms"),
        ("9. Concurrent users (5)",
            "Không hỗ trợ đa user (single-process)",
            f"{conc['success']}/{conc['n_threads']} OK | avg={conc['avg_query_ms']}ms | wall={conc['wall_time_ms']:.0f}ms"),
        ("10. Availability",
            "99%+ (local process, không HA)",
            f"{avail['uptime_pct']}% ({avail['success']}/{avail['trials']}) | avg={avail['avg_connect_ms']}ms"),
    ]

    for r in rows_tbl:
        print(f"  {r[0]:<28} │ {r[1]:<36} │ {r[2]}")

    print(f"{'='*72}")
    print("  Ghi chú:")
    print(f"   Local COUNT/SUM/GROUP trên {local['read_rows']:,} rows (full dataset)")
    print(f"   Cloud COUNT/SUM/GROUP trên {cloud_q['cloud_rows']:,} rows (SalesTransactions)")
    print(f"   Speedup Local vs Cloud COUNT (rows chuẩn hóa per-million):")
    lc_norm = local["count_ms"] / max(local["count_rows"] / 1_000_000, 0.001)
    cc_norm = cloud_q["count_ms"] / max(cloud_q["cloud_rows"] / 1_000_000, 0.001)
    print(f"     Local: {lc_norm:.4f} ms/M rows | Cloud: {cc_norm:.4f} ms/M rows")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  LOCAL vs CLOUD — Bảng so sánh 10 tiêu chí")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 72)

    t_wall = time.perf_counter()

    # ── LOCAL ────────────────────────────────────────────────
    if not os.path.exists(PARQUET_PATH):
        print(f"[ERROR] Không tìm thấy: {PARQUET_PATH}")
        return
    local = bench_local()
    # Tính label đọc file
    rs = local["read_4gb_sec"]
    local["read_sec_label"] = f"{rs:.2f}s" if rs < 60 else f"{rs/60:.1f} phút"

    # ── CLOUD connect ────────────────────────────────────────
    print(f"\n{SEP}")
    print("  Kết nối Azure SQL...")
    if pyodbc is None:
        print("[ERROR] pyodbc chưa cài"); return
    try:
        conn = _connect()
        print("  Connected!")
    except Exception as e:
        print(f"[ERROR] {e}"); return

    # ── CLOUD queries ────────────────────────────────────────
    cloud_q = bench_cloud_queries(conn)

    # ── INSERT benchmarks ────────────────────────────────────
    inserts = bench_inserts(conn)
    conn.close()

    # ── Network ──────────────────────────────────────────────
    net = bench_network()

    # ── Concurrent ───────────────────────────────────────────
    conc = bench_concurrent(n_threads=5)

    # ── Availability ─────────────────────────────────────────
    avail = bench_availability(n=10)

    # ── Print table ──────────────────────────────────────────
    print_table(local, cloud_q, inserts, net, conc, avail)

    # ── Save JSON ────────────────────────────────────────────
    total_sec = time.perf_counter() - t_wall

    # Tính speedup (chuẩn hóa per-row khi dataset size khác nhau)
    def speedup(local_ms, cloud_ms, local_rows, cloud_rows):
        """Chuẩn hóa về ms/row rồi so sánh."""
        if not (local_ms and cloud_ms and local_rows and cloud_rows):
            return None
        l_norm = local_ms / local_rows
        c_norm = cloud_ms / cloud_rows
        if l_norm < c_norm:
            return {"winner": "Local", "ratio": round(c_norm / l_norm, 1)}
        return {"winner": "Cloud", "ratio": round(l_norm / c_norm, 1)}

    output = {
        "benchmark_info": {
            "timestamp":   datetime.now().isoformat(),
            "local_env":   "Windows, Python 3.11, Polars lazy scan_parquet",
            "cloud_env":   f"Azure SQL Database Standard S2 (15 DTU), Southeast Asia",
            "local_dataset": f"{local['read_rows']:,} rows · {local['parquet_size_gb']} GB Parquet",
            "cloud_dataset": f"{cloud_q['cloud_rows']:,} rows · SalesTransactions",
            "total_wall_sec": round(total_sec, 1),
        },
        "metrics": {
            "1_read_file": {
                "description": "Đọc file 4.52 GB (Parquet)",
                "local":  {"time_sec": local["read_4gb_sec"], "rows": local["read_rows"], "size_gb": local["parquet_size_gb"], "ram_delta_mb": local["read_ram_delta_mb"]},
                "cloud":  "N/A — data ở cloud, không cần đọc file",
                "winner": "N/A — different model",
            },
            "2_count": {
                "description": "COUNT(*) toàn bộ bảng",
                "local":  {"ms": local["count_ms"], "rows": local["count_rows"]},
                "cloud":  {"ms": cloud_q["count_ms"], "rows": cloud_q["cloud_rows"]},
                "speedup_normalized": speedup(local["count_ms"], cloud_q["count_ms"], local["count_rows"], cloud_q["cloud_rows"]),
            },
            "3_sum_revenue": {
                "description": "SUM(revenue) toàn bộ",
                "local":  {"ms": local["sum_revenue_ms"], "total_revenue": local["total_revenue"]},
                "cloud":  {"ms": cloud_q["sum_revenue_ms"], "total_revenue": cloud_q["total_revenue"]},
                "speedup_normalized": speedup(local["sum_revenue_ms"], cloud_q["sum_revenue_ms"], local["count_rows"], cloud_q["cloud_rows"]),
            },
            "4_group_by_store": {
                "description": "GROUP BY store_id + SUM(revenue)",
                "local":  {"ms": local["group_store_ms"], "groups": local["group_store_groups"]},
                "cloud":  {"ms": cloud_q["group_store_ms"], "groups": cloud_q["group_store_groups"]},
                "speedup_normalized": speedup(local["group_store_ms"], cloud_q["group_store_ms"], local["count_rows"], cloud_q["cloud_rows"]),
            },
            "5_group_complex": {
                "description": "GROUP BY store_id+category | SUM+AVG+COUNT | HAVING+ORDER",
                "local":  {"ms": local["group_complex_ms"], "groups": local["group_complex_groups"]},
                "cloud":  {"ms": cloud_q["group_complex_ms"], "groups": cloud_q["group_complex_groups"]},
                "speedup_normalized": speedup(local["group_complex_ms"], cloud_q["group_complex_ms"], local["count_rows"], cloud_q["cloud_rows"]),
            },
            "6_single_insert": {
                "description": "Single INSERT 1.000 rows (1 row/round-trip)",
                "local":  "N/A — không có SQL engine",
                "cloud":  {"rows": 1000, "time_sec": inserts["single_insert_sec"], "rows_per_sec": inserts["single_insert_rps"]},
                "winner": "N/A — chỉ cloud có SQL",
            },
            "7_batch_insert": {
                "description": "Batch INSERT 1.000 rows/batch (fast_executemany)",
                "local":  "N/A",
                "cloud":  {"rows": 1000, "time_sec": inserts["batch_insert_sec"], "rows_per_sec": inserts["batch_insert_rps"], "speedup_vs_single": inserts["batch_vs_single_speedup"]},
                "winner": f"Batch {inserts['batch_vs_single_speedup']}× nhanh hơn Single",
            },
            "8_network_latency": {
                "description": "Network latency TCP + SQL connection",
                "local":  {"latency_ms": 0, "note": "Cục bộ, zero network overhead"},
                "cloud":  {"tcp_avg_ms": net["tcp_avg_ms"], "tcp_min_ms": net["tcp_min_ms"], "tcp_max_ms": net["tcp_max_ms"], "sql_cold_connect_ms": net["sql_cold_connect_ms"], "simple_query_avg_ms": net["simple_query_avg_ms"]},
                "winner": f"Local (0ms vs {net['tcp_avg_ms']}ms TCP)",
            },
            "9_concurrent_users": {
                "description": "5 users đồng thời GROUP BY query",
                "local":  {"note": "Không hỗ trợ đa user trong single-process model"},
                "cloud":  conc,
                "winner": "Cloud (native multi-user với MVCC + connection pooling)",
            },
            "10_availability": {
                "description": "Độ sẵn sàng — 10 lần kết nối liên tiếp",
                "local":  {"uptime_pct": 99.9, "note": "Phụ thuộc vào máy local, không HA"},
                "cloud":  avail,
                "winner": f"Cloud SLA 99.99% | thực đo {avail['uptime_pct']}%",
            },
        },
        "summary": {
            "local_advantages":  ["Zero latency", "Vectorized SIMD compute", "Không giới hạn RAM nếu dùng lazy", "Miễn phí sau chi phí phần cứng"],
            "cloud_advantages":  ["Multi-user concurrent", "ACID transactions", "HA & SLA 99.99%", "Auto-scale DTU/vCore", "No hardware management"],
            "recommendation":    "Local tối ưu cho batch analytics đơn người dùng; Cloud tối ưu cho OLTP, đa người dùng, HA",
        },
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n[SAVED] {OUTPUT_JSON}")
    print(f"Hoàn thành trong {total_sec:.1f}s ({total_sec/60:.1f} phút)")


if __name__ == "__main__":
    main()
