import sys
sys.path.insert(0, ".")
from config.settings import EVENT_HUB_CONNECTION_STRING, EVENT_HUB_NAME
from azure.eventhub import EventHubProducerClient, EventData
import pyodbc
from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD, SQL_DRIVER

print("=== 1. EVENT HUB TEST ===")
try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )
    batch = producer.create_batch()
    batch.add(EventData('{"test":1}'))
    producer.send_batch(batch)
    producer.close()
    print("OK - Event Hub connected and sent test event")
except Exception as e:
    print("ERROR:", e)

print("\n=== 2. SQL ROW COUNT TEST ===")
try:
    conn_str = (
        f"DRIVER={SQL_DRIVER};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
    )
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM dbo.SalesTransactions")
    count = cur.fetchone()[0]
    print(f"SalesTransactions row count: {count}")
    cur.execute("SELECT TOP 3 id, event_time, store_id, product_id, revenue FROM dbo.SalesTransactions ORDER BY id DESC")
    rows = cur.fetchall()
    print("  Latest rows (by insert order):")
    for r in rows:
        print(" ", r)

    cur.execute("SELECT TOP 3 id, window_start, window_end, store_id, revenue FROM dbo.HourlySalesSummary ORDER BY id DESC")
    rows2 = cur.fetchall()
    print(f"\nHourlySalesSummary latest rows (incl. Power BI window):")
    for r in rows2:
        print(" ", r)
    conn.close()
except Exception as e:
    print("SQL ERROR:", e)
