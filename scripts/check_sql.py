#!/usr/bin/env python3
"""Check SQL tables and optionally create them."""
import os, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

server = os.getenv("SQL_SERVER", "")
user = os.getenv("SQL_USERNAME", "")
pwd = os.getenv("SQL_PASSWORD", "")

print(f"Connecting to: {server}")

try:
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE=SalesAnalyticsDB;"
        f"UID={user};PWD={pwd};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=10"
    )
    conn = pyodbc.connect(conn_str, timeout=10)
    cursor = conn.cursor()

    # Check tables
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
    rows = cursor.fetchall()
    print(f"\nTables found: {len(rows)}")
    for r in rows:
        # Get row count for each table
        try:
            cursor.execute(f"SELECT COUNT(*) FROM dbo.[{r[0]}]")
            cnt = cursor.fetchone()[0]
            print(f"  - {r[0]:40s} ({cnt:,} rows)")
        except Exception:
            print(f"  - {r[0]}")

    conn.close()
    print("\nSQL connection: OK")

except ImportError:
    print("pyodbc not installed. Run: pip install pyodbc")
except Exception as e:
    err = str(e)
    if "firewall" in err.lower() or "40615" in err or "40914" in err:
        print(f"\nFIREWALL BLOCKED: {err[:200]}")
        print("\nFix: Azure Portal → SQL Server → Security → Networking → Add IPv4 address")
    else:
        print(f"\nError: {err[:300]}")
