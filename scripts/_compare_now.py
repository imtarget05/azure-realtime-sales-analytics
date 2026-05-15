"""
10-metric comparison: Local Polars parquet vs Azure SQL Cloud
Saves results to benchmark_output/local_vs_cloud_v2.json
"""
import os, sys, time, socket, statistics, json, datetime, random, threading
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, ".")
import pyodbc, polars as pl, psutil
from dotenv import load_dotenv
load_dotenv()

conn_str = (
    f"DRIVER={os.getenv('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USERNAME')};"
    f"PWD={os.getenv('SQL_PASSWORD')};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)
SQL_HOST = os.getenv("SQL_SERVER")
PARQUET  = "benchmark_output/sales_large_dataset.parquet"
INSERT_COLS = ["event_time","store_id","product_id","units_sold","unit_price",
               "revenue","temperature","weather","holiday","category"]
INSERT_SQL = (f"INSERT INTO SalesTransactions ({','.join(INSERT_COLS)}) "
              f"VALUES ({','.join(['?']*len(INSERT_COLS))})")

def get_conn():
    c = pyodbc.connect(conn_str, timeout=30); c.autocommit = True; return c

def sql_q(sql):
    c = get_conn(); cur = c.cursor()
    t = time.perf_counter(); cur.execute(sql); cur.fetchall()
    ms = (time.perf_counter()-t)*1000; c.close(); return ms

def sql_scalar(sql):
    c = get_conn(); cur = c.cursor()
    t = time.perf_counter(); cur.execute(sql); v = cur.fetchone()[0]
    ms = (time.perf_counter()-t)*1000; c.close(); return v, ms

def db_info():
    rows, _ = sql_scalar("SELECT COUNT(*) FROM SalesTransactions")
    c = get_conn()
    gb = float(c.cursor().execute("""SELECT SUM(a.used_pages)*8/1024.0/1024.0
        FROM sys.tables t JOIN sys.indexes i ON t.OBJECT_ID=i.object_id
        JOIN sys.partitions p ON i.object_id=p.OBJECT_ID AND i.index_id=p.index_id
        JOIN sys.allocation_units a ON p.partition_id=a.container_id
        WHERE t.NAME=N'SalesTransactions'""").fetchone()[0] or 0)
    c.close(); return rows, gb

print("="*70)
print("  SO SÁNH 10 TIÊU CHÍ: LOCAL (Polars) vs CLOUD (Azure SQL S2)")
print("="*70)

# DB info
cld_rows, cld_gb = db_info()
print(f"\n  Cloud DB: {cld_rows:,} rows | {cld_gb:.4f} GB")

# ── [1] Đọc file local ────────────────────────────────────────────────────────
proc = psutil.Process()
mem0 = proc.memory_info().rss
t = time.perf_counter()
df_loc = pl.scan_parquet(PARQUET).collect()
read_ms = (time.perf_counter()-t)*1000
ram_mb  = (proc.memory_info().rss - mem0)/1024/1024
loc_rows = len(df_loc)
loc_gb   = os.path.getsize(PARQUET)/1024**3
print(f"\n[1] Đọc toàn bộ file local")
print(f"    {loc_rows:,} rows | {loc_gb:.2f} GB Parquet | {read_ms/1000:.2f}s | RAM+{ram_mb:.0f}MB")

# ── [2] COUNT(*) ─────────────────────────────────────────────────────────────
t = time.perf_counter(); _ = len(df_loc); loc_cnt_ms = (time.perf_counter()-t)*1000
cld_cnt, cld_cnt_ms = sql_scalar("SELECT COUNT(*) FROM SalesTransactions")
print(f"\n[2] COUNT(*)")
print(f"    Local : {loc_rows:,} rows in {loc_cnt_ms:.3f} ms")
print(f"    Cloud : {cld_cnt:,} rows in {cld_cnt_ms:.0f} ms  ({cld_cnt_ms/max(loc_cnt_ms,0.001):.0f}× slower)")

# ── [3] SUM(revenue) ─────────────────────────────────────────────────────────
t = time.perf_counter(); loc_sum = df_loc["revenue"].sum(); loc_sum_ms = (time.perf_counter()-t)*1000
_, cld_sum_ms = sql_scalar("SELECT SUM(CAST(revenue AS FLOAT)) FROM SalesTransactions")
print(f"\n[3] SUM(revenue)")
print(f"    Local : {loc_sum:,.2f}  in {loc_sum_ms:.2f} ms")
print(f"    Cloud : {cld_sum_ms:.0f} ms  ({cld_sum_ms/max(loc_sum_ms,0.001):.0f}× slower)")

# ── [4] GROUP BY store_id ────────────────────────────────────────────────────
t = time.perf_counter()
df_loc.group_by("store_id").agg(pl.col("revenue").sum())
loc_grp_ms = (time.perf_counter()-t)*1000
cld_grp_ms = sql_q("SELECT store_id,SUM(revenue) FROM SalesTransactions GROUP BY store_id")
print(f"\n[4] GROUP BY store_id")
print(f"    Local : {loc_grp_ms:.2f} ms  |  Cloud : {cld_grp_ms:.0f} ms  ({cld_grp_ms/max(loc_grp_ms,0.001):.0f}× slower)")

# ── [5] GROUP BY phức tạp ────────────────────────────────────────────────────
t = time.perf_counter()
(df_loc.group_by(["store_id","category"])
 .agg([pl.col("revenue").sum().alias("rev"),
       pl.col("revenue").mean().alias("avg"),
       pl.col("revenue").count().alias("cnt")])
 .filter(pl.col("rev") > 1_000_000)
 .sort("rev", descending=True))
loc_cplx_ms = (time.perf_counter()-t)*1000
cld_cplx_ms = sql_q("""SELECT store_id,category,SUM(revenue) r,AVG(revenue) a,COUNT(*) c
    FROM SalesTransactions GROUP BY store_id,category
    HAVING SUM(revenue)>1000000 ORDER BY r DESC""")
print(f"\n[5] GROUP BY phức tạp (HAVING + ORDER BY)")
print(f"    Local : {loc_cplx_ms:.2f} ms  |  Cloud : {cld_cplx_ms:.0f} ms  ({cld_cplx_ms/max(loc_cplx_ms,0.001):.0f}× slower)")

# ── [6] Single INSERT ────────────────────────────────────────────────────────
def rand_row():
    return (datetime.datetime.now(),
            random.choice(["S01","S02","S03"]),
            f"P{random.randint(1,50):03d}",
            random.randint(1,20),
            round(random.uniform(10,5000),2),
            round(random.uniform(100,50000),2),
            round(random.uniform(15,40),1),
            random.choice(["sunny","cloudy","rainy"]),
            random.randint(0,1),
            random.choice(["Electronics","Clothing","Food","Stationery"]))

N6 = 100
c6 = pyodbc.connect(conn_str, timeout=30); c6.autocommit = False; cur6 = c6.cursor()
t = time.perf_counter()
for _ in range(N6): cur6.execute(INSERT_SQL, rand_row())
c6.commit(); c6.close()
s_ms = (time.perf_counter()-t)*1000; s_rps = N6/(s_ms/1000)
print(f"\n[6] Single INSERT ({N6} rows 1-by-1)")
print(f"    Cloud : {s_rps:.0f} rows/s | total {s_ms:.0f} ms")

# ── [7] Batch INSERT ─────────────────────────────────────────────────────────
data7 = [rand_row() for _ in range(1000)]
c7 = pyodbc.connect(conn_str, timeout=30); c7.autocommit = False
cur7 = c7.cursor(); cur7.fast_executemany = True
t = time.perf_counter()
cur7.executemany(INSERT_SQL, data7); c7.commit(); c7.close()
b_ms = (time.perf_counter()-t)*1000; b_rps = 1000/(b_ms/1000)
print(f"\n[7] Batch INSERT (1,000 rows, fast_executemany)")
print(f"    Cloud : {b_rps:.0f} rows/s | {b_ms:.0f} ms | {b_rps/s_rps:.1f}× faster vs single")

# ── [8] Network latency ─────────────────────────────────────────────────────
tcp_t = []
for _ in range(10):
    t = time.perf_counter()
    s = socket.create_connection((SQL_HOST, 1433), timeout=10); s.close()
    tcp_t.append((time.perf_counter()-t)*1000)
tcp_avg = statistics.mean(tcp_t)

t = time.perf_counter()
c8 = pyodbc.connect(conn_str, timeout=30)
cold_ms = (time.perf_counter()-t)*1000

warm_t = []
cur8 = c8.cursor()
for _ in range(10):
    t = time.perf_counter(); cur8.execute("SELECT 1"); cur8.fetchone()
    warm_t.append((time.perf_counter()-t)*1000)
c8.close()
warm_avg = statistics.mean(warm_t)
print(f"\n[8] Network latency")
print(f"    Local : 0 ms (RAM/SSD in-process)")
print(f"    Cloud : TCP avg {tcp_avg:.1f} ms | cold conn {cold_ms:.0f} ms | warm query {warm_avg:.2f} ms")

# ── [9] Concurrent 5 users ──────────────────────────────────────────────────
def q_thread(_):
    t = time.perf_counter()
    c = get_conn()
    c.cursor().execute("SELECT store_id,SUM(revenue) FROM SalesTransactions GROUP BY store_id").fetchall()
    c.close()
    return (time.perf_counter()-t)*1000

w0 = time.perf_counter()
with ThreadPoolExecutor(max_workers=5) as ex:
    t9 = list(ex.map(q_thread, range(5)))
wall9 = (time.perf_counter()-w0)*1000
print(f"\n[9] Concurrent 5 users (GROUP BY query)")
print(f"    Local : N/A (single-process in-memory)")
print(f"    Cloud : 5/5 OK | avg {statistics.mean(t9):.0f} ms | wall {wall9:.0f} ms")

# ── [10] Availability ────────────────────────────────────────────────────────
avail = []
for i in range(10):
    t = time.perf_counter()
    try:
        c = pyodbc.connect(conn_str, timeout=15)
        c.cursor().execute("SELECT 1").fetchone(); c.close()
        avail.append((time.perf_counter()-t)*1000)
        print(f"    [{i+1:2d}] ✓ {avail[-1]:.1f} ms")
    except Exception as e:
        print(f"    [{i+1:2d}] ✗ {e}")
avail_pct = len(avail)/10*100
avail_avg = statistics.mean(avail)
print(f"    Availability {avail_pct:.0f}% | avg {avail_avg:.1f} ms")

# ── Refresh DB info ──────────────────────────────────────────────────────────
cld_rows2, cld_gb2 = db_info()

# ── Bảng tóm tắt ────────────────────────────────────────────────────────────
print("\n" + "="*74)
print("  BẢNG TÓM TẮT")
print(f"  Local  : {loc_rows:,} rows | {loc_gb:.2f} GB Parquet (snappy)")
print(f"  Cloud  : {cld_rows2:,} rows | {cld_gb2:.4f} GB — Azure SQL Standard S2 (15 DTU)")
print("="*74)
W = [32, 28, 28]
def row(m, l, c):
    print(f"  {m:<{W[0]}} │ {l:<{W[1]}} │ {c}")
print(f"  {'Tiêu chí':<{W[0]}} │ {'Máy chủ truyền thống':<{W[1]}} │ Hệ thống Cloud")
print("  " + "─"*W[0] + " │ " + "─"*W[1] + " │ " + "─"*W[2])
row("1. Đọc toàn bộ dữ liệu", f"{loc_gb:.2f}GB in {read_ms/1000:.2f}s", "N/A (data lưu cloud)")
row("2. COUNT(*)",            f"{loc_rows:,} rows | {loc_cnt_ms:.3f} ms", f"{cld_rows2:,} rows | {cld_cnt_ms:.0f} ms")
row("3. SUM(revenue)",        f"{loc_sum_ms:.2f} ms",                      f"{cld_sum_ms:.0f} ms")
row("4. GROUP BY đơn giản",   f"{loc_grp_ms:.2f} ms",                      f"{cld_grp_ms:.0f} ms")
row("5. GROUP BY phức tạp",   f"{loc_cplx_ms:.2f} ms",                     f"{cld_cplx_ms:.0f} ms")
row("6. Single INSERT",       "N/A",                                         f"{s_rps:.0f} r/s | {s_ms:.0f} ms")
row("7. Batch INSERT 1000r",  "N/A",                                         f"{b_rps:.0f} r/s | {b_ms:.0f} ms")
row("8. Network latency",     "0 ms (RAM/SSD)",                              f"TCP {tcp_avg:.1f}ms cold {cold_ms:.0f}ms q {warm_avg:.1f}ms")
row("9. Concurrent 5 users",  "Không hỗ trợ (local)",                       f"5/5 OK | avg {statistics.mean(t9):.0f} ms")
row("10. Availability",       "~99.9% (no HA, no SLA)",                     f"{avail_pct:.0f}% | avg {avail_avg:.1f} ms")
print("="*74)

# ── Save JSON ────────────────────────────────────────────────────────────────
out = {
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    "db": {"rows": cld_rows2, "used_gb": cld_gb2},
    "local": {"rows": loc_rows, "parquet_gb": loc_gb, "read_s": read_ms/1000,
              "count_ms": loc_cnt_ms, "sum_ms": loc_sum_ms,
              "group_ms": loc_grp_ms, "complex_ms": loc_cplx_ms},
    "cloud": {"rows": cld_rows2, "count_ms": cld_cnt_ms, "sum_ms": cld_sum_ms,
              "group_ms": cld_grp_ms, "complex_ms": cld_cplx_ms,
              "single_insert_rps": s_rps, "single_ms": s_ms,
              "batch_insert_rps": b_rps, "batch_ms": b_ms,
              "tcp_avg_ms": tcp_avg, "cold_ms": cold_ms, "select1_ms": warm_avg,
              "concurrent5_avg_ms": statistics.mean(t9), "wall_ms": wall9,
              "availability_pct": avail_pct, "avail_avg_ms": avail_avg},
}
path = "benchmark_output/local_vs_cloud_v2.json"
with open(path, "w") as f: json.dump(out, f, indent=2)
print(f"\n  [SAVED] {path}")
