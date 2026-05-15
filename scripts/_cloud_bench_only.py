"""
Re-run cloud-only queries on idle DB (no concurrent inserts).
Measures: COUNT, SUM, GROUP BY, COMPLEX GROUP BY, network latency.
"""
import os, sys, time, socket, statistics, json
sys.path.insert(0, ".")
import pyodbc
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

def get_conn():
    c = pyodbc.connect(conn_str, timeout=30)
    c.autocommit = True
    return c

def sql_q(sql, label=""):
    c = get_conn(); cur = c.cursor()
    t = time.perf_counter(); cur.execute(sql); cur.fetchall()
    ms = (time.perf_counter()-t)*1000
    c.close()
    print(f"  {label}: {ms:.1f} ms")
    return ms

def sql_scalar(sql, label=""):
    c = get_conn(); cur = c.cursor()
    t = time.perf_counter(); cur.execute(sql); v = cur.fetchone()[0]
    ms = (time.perf_counter()-t)*1000
    c.close()
    print(f"  {label}: {v} in {ms:.1f} ms")
    return v, ms

print("="*60)
print("  CLOUD BENCHMARK (idle DB)")
print("="*60)

# Warm-up: 1 throwaway query to establish connection pool
print("\n[warm-up] ...")
sql_q("SELECT TOP 1 1 FROM SalesTransactions", "warmup")

results = {}

# [2] COUNT
print("\n[2] COUNT(*) — x3 runs")
ms_list = []
for i in range(3):
    v, ms = sql_scalar("SELECT COUNT(*) FROM SalesTransactions", f"  run{i+1}")
    ms_list.append(ms)
results["count_ms"] = statistics.median(ms_list)
print(f"  MEDIAN: {results['count_ms']:.1f} ms")

# [3] SUM(revenue)
print("\n[3] SUM(revenue) — x3 runs")
ms_list = []
for i in range(3):
    ms = sql_q("SELECT SUM(CAST(revenue AS FLOAT)) FROM SalesTransactions", f"  run{i+1}")
    ms_list.append(ms)
results["sum_ms"] = statistics.median(ms_list)
print(f"  MEDIAN: {results['sum_ms']:.1f} ms")

# [4] GROUP BY store_id
print("\n[4] GROUP BY store_id — x3 runs")
ms_list = []
for i in range(3):
    ms = sql_q("SELECT store_id, SUM(revenue) FROM SalesTransactions GROUP BY store_id", f"  run{i+1}")
    ms_list.append(ms)
results["group_ms"] = statistics.median(ms_list)
print(f"  MEDIAN: {results['group_ms']:.1f} ms")

# [5] Complex GROUP BY
print("\n[5] Complex GROUP BY (store+category+HAVING) — x2 runs")
CPLX = """
SELECT store_id, category, SUM(revenue) rev, AVG(revenue) avg_rev, COUNT(*) cnt
FROM SalesTransactions
GROUP BY store_id, category
HAVING SUM(revenue) > 1000000
ORDER BY rev DESC
"""
ms_list = []
for i in range(2):
    ms = sql_q(CPLX, f"  run{i+1}")
    ms_list.append(ms)
results["complex_ms"] = statistics.median(ms_list)
print(f"  MEDIAN: {results['complex_ms']:.1f} ms")

# [8] Network latency — TCP ping
print("\n[8] Network latency — 10 TCP pings")
latencies = []
for _ in range(10):
    t = time.perf_counter()
    s = socket.create_connection((SQL_HOST, 1433), timeout=5)
    s.close()
    latencies.append((time.perf_counter()-t)*1000)
results["tcp_avg_ms"] = statistics.mean(latencies)
results["tcp_min_ms"] = min(latencies)
print(f"  avg={results['tcp_avg_ms']:.1f}ms  min={results['tcp_min_ms']:.1f}ms")

# [8b] SELECT 1 round-trip
print("\n[8b] SELECT 1 round-trip — 5 runs")
rt_list = []
for _ in range(5):
    _, ms = sql_scalar("SELECT 1", "  rt")
    rt_list.append(ms)
results["select1_ms"] = statistics.median(rt_list)
print(f"  MEDIAN: {results['select1_ms']:.1f} ms")

# Summary
print("\n" + "="*60)
print("  SUMMARY (idle DB, median)")
print("="*60)
print(f"  COUNT(*)          : {results['count_ms']:.1f} ms")
print(f"  SUM(revenue)      : {results['sum_ms']:.1f} ms")
print(f"  GROUP BY store_id : {results['group_ms']:.1f} ms")
print(f"  Complex GROUP BY  : {results['complex_ms']:.1f} ms")
print(f"  TCP latency avg   : {results['tcp_avg_ms']:.1f} ms")
print(f"  SELECT 1 round-trip: {results['select1_ms']:.1f} ms")

out = "benchmark_output/cloud_bench_idle.json"
with open(out, "w") as f:
    json.dump(results, f, indent=2)
print(f"\n  Saved → {out}")
