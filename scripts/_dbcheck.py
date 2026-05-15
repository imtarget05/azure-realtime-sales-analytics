import os, sys; sys.path.insert(0, ".")
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

c = pyodbc.connect(conn_str, timeout=30)
cur = c.cursor()
cur.execute("SELECT COUNT(*) FROM SalesTransactions")
rows = cur.fetchone()[0]
print(f"rows: {rows:,}")

cur2 = c.cursor()
cur2.execute("""SELECT SUM(a.used_pages)*8/1024.0/1024.0
    FROM sys.tables t JOIN sys.indexes i ON t.OBJECT_ID=i.object_id
    JOIN sys.partitions p ON i.object_id=p.OBJECT_ID AND i.index_id=p.index_id
    JOIN sys.allocation_units a ON p.partition_id=a.container_id
    WHERE t.NAME=N'SalesTransactions'""")
gb = float(cur2.fetchone()[0] or 0)
print(f"GB used: {gb:.4f}")
print(f"Bytes/row: {gb*1024*1024*1024/rows:.1f}" if rows > 0 else "no rows")

cur3 = c.cursor()
cur3.execute("SELECT @@VERSION")
print(cur3.fetchone()[0][:80])
c.close()
