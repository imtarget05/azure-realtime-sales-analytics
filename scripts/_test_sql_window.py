import sys, time
sys.path.insert(0, ".")
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER
import pyodbc

print("Connecting to SQL...", flush=True)
t0 = time.time()
conn_str = (
    f"Driver={SQL_DRIVER};"
    f"Server=tcp:{SQL_SERVER},1433;"
    f"Database={SQL_DATABASE};"
    f"Uid={SQL_USERNAME};"
    f"Pwd={SQL_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
)
try:
    conn = pyodbc.connect(conn_str)
    print(f"Connected in {time.time()-t0:.1f}s", flush=True)
    cur = conn.cursor()

    # Check SYSUTCDATETIME on SQL server
    cur.execute("SELECT SYSUTCDATETIME()")
    sql_now = cur.fetchone()[0]
    print(f"SQL Server UTC time: {sql_now}", flush=True)

    # Check rows in last 5 min window
    cur.execute("""
        SELECT COUNT(*), MIN(event_time), MAX(event_time)
        FROM dbo.SalesTransactions
        WHERE event_time >= DATEADD(minute, -5, SYSUTCDATETIME())
    """)
    row = cur.fetchone()
    print(f"Rows in last 5min: {row[0]}, min_time: {row[1]}, max_time: {row[2]}", flush=True)

    # Also check last 60 min
    cur.execute("""
        SELECT COUNT(*), MAX(event_time)
        FROM dbo.SalesTransactions
        WHERE event_time >= DATEADD(minute, -60, SYSUTCDATETIME())
    """)
    row2 = cur.fetchone()
    print(f"Rows in last 60min: {row2[0]}, latest: {row2[1]}", flush=True)

    conn.close()
except Exception as e:
    print(f"ERROR: {e}", flush=True)
