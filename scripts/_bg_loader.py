"""Background loader — single connection, no output spam, just loads rows."""
import os, sys, time, datetime
sys.path.insert(0, ".")
import pyodbc, polars as pl
from dotenv import load_dotenv
load_dotenv()

LOG = "benchmark_output/_load_progress.log"

conn_str = (
    f"DRIVER={os.getenv('SQL_DRIVER', '{ODBC Driver 18 for SQL Server}')};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DATABASE')};"
    f"UID={os.getenv('SQL_USERNAME')};"
    f"PWD={os.getenv('SQL_PASSWORD')};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)

INSERT_COLS = ["event_time","store_id","product_id","units_sold","unit_price",
               "revenue","temperature","weather","holiday","category"]
INSERT_SQL  = (f"INSERT INTO SalesTransactions ({','.join(INSERT_COLS)}) "
               f"VALUES ({','.join(['?']*len(INSERT_COLS))})")
PARQUET     = "benchmark_output/sales_large_dataset.parquet"
TARGET_GB   = 0.75
BSIZ        = 10_000

def log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f: f.write(line + "\n")

def db_rows():
    c = pyodbc.connect(conn_str, timeout=30)
    r = c.cursor().execute("SELECT COUNT(*) FROM SalesTransactions").fetchone()[0]
    c.close(); return r

def db_gb():
    c = pyodbc.connect(conn_str, timeout=30)
    v = c.cursor().execute("""SELECT SUM(a.used_pages)*8/1024.0/1024.0
        FROM sys.tables t JOIN sys.indexes i ON t.OBJECT_ID=i.object_id
        JOIN sys.partitions p ON i.object_id=p.OBJECT_ID AND i.index_id=p.index_id
        JOIN sys.allocation_units a ON p.partition_id=a.container_id
        WHERE t.NAME=N'SalesTransactions'""").fetchone()[0]
    c.close(); return float(v or 0)

rows0 = db_rows()
gb0   = db_gb()
log(f"START: {rows0:,} rows | {gb0:.4f} GB")

if gb0 >= TARGET_GB:
    log(f"Already {gb0:.4f} GB >= {TARGET_GB} GB. Done.")
    sys.exit(0)

gb_per_row  = gb0 / rows0
rows_needed = int((TARGET_GB - gb0) / gb_per_row) + 300_000

log(f"Need ~{rows_needed:,} more rows (skip first {rows0:,})")

df = (pl.scan_parquet(PARQUET)
      .slice(rows0, rows_needed)
      .select(INSERT_COLS)
      .collect()
      .with_columns(
          pl.col("event_time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False)
          .fill_null(pl.lit(datetime.datetime(2025, 1, 1)))
      )
      .with_columns([
          pl.col("units_sold").cast(pl.Int32),
          pl.col("holiday").cast(pl.Int32),
      ]))

log(f"Loaded {len(df):,} rows. Converting to tuples...")
all_data = list(df.iter_rows())
log(f"Ready to insert {len(all_data):,} rows, batch={BSIZ}")

c   = pyodbc.connect(conn_str, timeout=60)
c.autocommit = False
cur = c.cursor(); cur.fast_executemany = True

inserted = 0
t0 = time.time()

for i in range(0, len(all_data), BSIZ):
    batch = all_data[i:i+BSIZ]
    try:
        cur.executemany(INSERT_SQL, batch)
        c.commit()
        inserted += len(batch)
    except Exception as e:
        c.rollback()
        log(f"ERR batch {i//BSIZ}: {e}")
        # Reconnect on error
        try: c.close()
        except: pass
        c   = pyodbc.connect(conn_str, timeout=60)
        c.autocommit = False
        cur = c.cursor(); cur.fast_executemany = True

    if (i // BSIZ) % 50 == 0:
        el  = time.time() - t0
        rps = inserted / el if el > 0 else 0
        eta = (len(all_data) - inserted) / rps if rps > 0 else 0
        log(f"{inserted:>7,}/{len(all_data):,}  {rps:.0f} r/s  ETA {eta/60:.1f}min")

c.close()
el = time.time() - t0
rows1 = db_rows(); gb1 = db_gb()
log(f"DONE: inserted {inserted:,} in {el/60:.1f}min ({inserted/el:.0f} r/s)")
log(f"DB final: {rows1:,} rows | {gb1:.4f} GB")
