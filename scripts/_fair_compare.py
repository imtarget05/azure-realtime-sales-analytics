"""
Fair 1:1 comparison: Local Polars (2.165M rows) vs Cloud Azure SQL (2.165M rows)
Same dataset size for apples-to-apples benchmark.
"""
import os, sys, time, json
sys.path.insert(0, ".")
import polars as pl
from dotenv import load_dotenv
load_dotenv()

PARQUET = "benchmark_output/sales_large_dataset.parquet"
N_ROWS  = 2_165_410   # match cloud row count exactly

print("="*65)
print("  FAIR COMPARE: Local Polars vs Cloud Azure SQL  (2.165M rows)")
print("="*65)

# Load first 2.165M rows from parquet
t0 = time.perf_counter()
df = pl.scan_parquet(PARQUET).head(N_ROWS).collect()
load_s = time.perf_counter() - t0
print(f"\n  Loaded {len(df):,} local rows in {load_s:.3f}s")

results = {}

# [2] COUNT
t = time.perf_counter(); _ = len(df); cnt_ms = (time.perf_counter()-t)*1000
results["local_count_ms"] = cnt_ms
print(f"\n[2] COUNT  :  {cnt_ms:.4f} ms  ({len(df):,} rows)")

# [3] SUM(revenue)
t = time.perf_counter(); s = df["revenue"].sum(); sum_ms = (time.perf_counter()-t)*1000
results["local_sum_ms"] = sum_ms
print(f"[3] SUM    :  {sum_ms:.3f} ms  (total={s:,.2f})")

# [4] GROUP BY store_id
t = time.perf_counter()
df.group_by("store_id").agg(pl.col("revenue").sum())
grp_ms = (time.perf_counter()-t)*1000
results["local_group_ms"] = grp_ms
print(f"[4] GROUP  :  {grp_ms:.2f} ms")

# [5] Complex GROUP BY  (store_id + category + HAVING rev > 1M)
t = time.perf_counter()
(df.group_by(["store_id","category"])
   .agg([pl.col("revenue").sum().alias("rev"),
         pl.col("revenue").mean().alias("avg"),
         pl.col("revenue").count().alias("cnt")])
   .filter(pl.col("rev") > 1_000_000)
   .sort("rev", descending=True))
cplx_ms = (time.perf_counter()-t)*1000
results["local_complex_ms"] = cplx_ms
print(f"[5] COMPLEX:  {cplx_ms:.2f} ms")

print("\n--- Cloud results (from previous run) ---")
cld = {
    "count_ms":   6093.0,
    "sum_ms":     9605.3,
    "group_ms":  29885.6,
    "complex_ms":22083.1,
    "single_rps":   31.7,
    "batch_rps":   735.8,
    "tcp_ms":       36.8,
    "concurrent5": 99649.3,
    "avail_pct":  100.0,
    "avail_ms":   220.1,
}
print(f"  COUNT   : {cld['count_ms']:.0f} ms")
print(f"  SUM     : {cld['sum_ms']:.0f} ms")
print(f"  GROUP   : {cld['group_ms']:.0f} ms")
print(f"  COMPLEX : {cld['complex_ms']:.0f} ms")

print("\n--- Ratio (Cloud / Local) ---")
for label, loc_k, cld_v in [
    ("COUNT  ", "local_count_ms", cld["count_ms"]),
    ("SUM    ", "local_sum_ms",   cld["sum_ms"]),
    ("GROUP  ", "local_group_ms", cld["group_ms"]),
    ("COMPLEX", "local_complex_ms", cld["complex_ms"]),
]:
    r = cld_v / max(results[loc_k], 0.001)
    winner = "Local" if r > 1 else "Cloud"
    print(f"  [{label}]  Local={results[loc_k]:.2f}ms  Cloud={cld_v:.0f}ms  → {winner} nhanh hơn {abs(r) if r>1 else 1/r:.0f}×")

# Save
results.update(cld)
results["n_rows"] = N_ROWS
out = "benchmark_output/fair_compare.json"
with open(out, "w") as f:
    json.dump(results, f, indent=2)
print(f"\n  Saved → {out}")
