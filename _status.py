"""Quick status check + remaining setup actions."""
import pyodbc, os, sys
from dotenv import load_dotenv
load_dotenv()

S = os.getenv("SQL_SERVER")
D = os.getenv("SQL_DATABASE")
U = os.getenv("SQL_USERNAME")
P = os.getenv("SQL_PASSWORD")
conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server=tcp:{S},1433;Database={D};Uid={U};Pwd={P};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

conn = pyodbc.connect(conn_str, timeout=10)
cur = conn.cursor()

# Tables
cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='dbo' AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
tables = [r[0] for r in cur.fetchall()]
print("=== SQL Tables ===")
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM dbo.[{t}]")
    print(f"  {t}: {cur.fetchone()[0]} rows")

# Views
cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='dbo'")
views = [r[0] for r in cur.fetchall()]
print(f"\n=== Views: {views} ===")

# Indexes
cur.execute("SELECT name FROM sys.indexes WHERE object_id IN (SELECT object_id FROM sys.tables) AND name LIKE 'IX_%'")
indexes = [r[0] for r in cur.fetchall()]
print(f"\n=== Indexes: {indexes} ===")

# Stored procs
cur.execute("SELECT name FROM sys.procedures")
procs = [r[0] for r in cur.fetchall()]
print(f"\n=== Stored Procedures: {procs} ===")

conn.close()
print("\nSQL: OK")
