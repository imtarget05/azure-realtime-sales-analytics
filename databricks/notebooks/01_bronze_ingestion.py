# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion (Structured Streaming / Demo Batch)
# MAGIC
# MAGIC **Mục tiêu**: Đọc real-time events từ Azure Event Hubs, parse JSON,
# MAGIC và ghi nguyên trạng vào Delta Lake tầng **Bronze** (append-only).
# MAGIC
# MAGIC **Demo mode**: Nếu Event Hub không khả dụng, sinh 10,000 sample events
# MAGIC và ghi batch vào Bronze Delta table.
# MAGIC
# MAGIC **Kiến trúc**:
# MAGIC ```
# MAGIC Azure Event Hub ──► Spark Structured Streaming ──► Delta Lake (Bronze)
# MAGIC       OR
# MAGIC Demo Generator  ──► Spark Batch Write ──► Delta Lake (Bronze)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Data (Event Hub or Demo)

# COMMAND ----------

if DEMO_MODE or not EH_CONF:
    # ═══════════════════════════════════════════════════════════════
    # DEMO MODE: Generate sample sales events
    # ═══════════════════════════════════════════════════════════════
    import random
    import uuid
    from datetime import datetime, timedelta

    print("📦 Demo mode: Generating 10,000 sample sales events...")

    DEMO_PRODUCTS = [
        {"id": "P001", "name": "Laptop",       "category": "Electronics",     "base_price": 999.99},
        {"id": "P002", "name": "Smartphone",    "category": "Electronics",     "base_price": 699.99},
        {"id": "P003", "name": "Headphones",    "category": "Electronics",     "base_price": 149.99},
        {"id": "P004", "name": "Tablet",        "category": "Electronics",     "base_price": 499.99},
        {"id": "P006", "name": "T-Shirt",       "category": "Clothing",        "base_price": 29.99},
        {"id": "P007", "name": "Jeans",         "category": "Clothing",        "base_price": 59.99},
        {"id": "P008", "name": "Sneakers",      "category": "Clothing",        "base_price": 89.99},
        {"id": "P009", "name": "Coffee Maker",  "category": "Home",            "base_price": 79.99},
        {"id": "P010", "name": "Blender",       "category": "Home",            "base_price": 49.99},
        {"id": "P016", "name": "Orange Juice",  "category": "Beverage",        "base_price": 2.50},
        {"id": "P017", "name": "Green Tea",     "category": "Beverage",        "base_price": 1.80},
        {"id": "P021", "name": "Chips",         "category": "Snacks",          "base_price": 1.99},
        {"id": "P024", "name": "Sunscreen",     "category": "Health & Beauty", "base_price": 12.99},
        {"id": "P027", "name": "Football",      "category": "Sports",          "base_price": 24.99},
    ]
    DEMO_STORES = ["S01", "S02", "S03"]
    DEMO_REGIONS = ["South", "North", "Central"]
    DEMO_WEATHERS = ["sunny", "rainy", "cloudy", "stormy"]
    DEMO_PAYMENTS = ["Credit Card", "Debit Card", "Cash", "PayPal"]
    DEMO_SEGMENTS = ["Regular", "Premium", "VIP", "New"]

    base_dt = datetime(2026, 4, 1)
    rows = []
    for i in range(10000):
        prod = random.choice(DEMO_PRODUCTS)
        store_idx = random.randint(0, 2)
        qty = random.randint(1, 10)
        up = round(prod["base_price"] * random.uniform(0.85, 1.15), 2)
        disc = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 2)
        dt = base_dt + timedelta(seconds=random.randint(0, 9 * 86400))

        rows.append({
            "transaction_id": str(uuid.uuid4())[:12],
            "event_timestamp_str": dt.isoformat(),
            "store_id": DEMO_STORES[store_idx],
            "region": DEMO_REGIONS[store_idx],
            "product_id": prod["id"],
            "product_name": prod["name"],
            "category": prod["category"],
            "quantity": qty,
            "unit_price": up,
            "price": up,
            "discount": disc,
            "payment_method": random.choice(DEMO_PAYMENTS),
            "customer_id": f"C{random.randint(1, 500):04d}",
            "customer_segment": random.choice(DEMO_SEGMENTS),
            "temperature": round(random.uniform(20, 38), 1),
            "weather": random.choice(DEMO_WEATHERS),
            "holiday": random.choice([0, 0, 0, 0, 1]),
            "_raw_body": "demo_generated",
            "_source": "demo_generator",
        })

    bronze_df = spark.createDataFrame(rows)
    bronze_df = (
        bronze_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_eh_enqueued_time", F.current_timestamp())
        .withColumn("_eh_offset", F.lit("0"))
        .withColumn("_eh_sequence", F.monotonically_increasing_id())
        .withColumn("_eh_partition", F.lit("0"))
    )

    print(f"✓ Generated {bronze_df.count():,} demo events")
    bronze_df.printSchema()

else:
    # ═══════════════════════════════════════════════════════════════
    # PRODUCTION MODE: Read from Event Hub
    # ═══════════════════════════════════════════════════════════════
    raw_stream = (
        spark.readStream
        .format("eventhubs")
        .options(**EH_CONF)
        .load()
    )

    print("✓ Connected to Event Hub stream")
    print(f"  Schema: {raw_stream.schema.fieldNames()}")

    bronze_df = (
        raw_stream
        .withColumn("_raw_body", F.col("body").cast("string"))
        .withColumn(
            "_parsed",
            F.coalesce(
                F.from_json(F.col("_raw_body"), EXTENDED_EVENT_SCHEMA),
                F.from_json(F.col("_raw_body"), RAW_EVENT_SCHEMA),
            )
        )
        .select(
            F.col("enqueuedTime").alias("_eh_enqueued_time"),
            F.col("offset").alias("_eh_offset"),
            F.col("sequenceNumber").alias("_eh_sequence"),
            F.col("partition").alias("_eh_partition"),
            F.col("_raw_body"),
            F.col("_parsed.transaction_id").alias("transaction_id"),
            F.col("_parsed.timestamp").alias("event_timestamp_str"),
            F.col("_parsed.store_id").alias("store_id"),
            F.col("_parsed.region").alias("region"),
            F.col("_parsed.product_id").alias("product_id"),
            F.col("_parsed.product_name").alias("product_name"),
            F.col("_parsed.category").alias("category"),
            F.col("_parsed.quantity").alias("quantity"),
            F.col("_parsed.unit_price").alias("unit_price"),
            F.col("_parsed.price").alias("price"),
            F.col("_parsed.discount").alias("discount"),
            F.col("_parsed.payment_method").alias("payment_method"),
            F.col("_parsed.customer_id").alias("customer_id"),
            F.col("_parsed.customer_segment").alias("customer_segment"),
            F.col("_parsed.temperature").alias("temperature"),
            F.col("_parsed.weather").alias("weather"),
            F.col("_parsed.holiday").alias("holiday"),
            F.current_timestamp().alias("_ingested_at"),
            F.lit("event_hub").alias("_source"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write to Delta Lake (Bronze)

# COMMAND ----------

ensure_databases()

if DEMO_MODE or not EH_CONF:
    # ── Demo: batch write ──
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_DB}.sales_events
        USING DELTA
        LOCATION '{BRONZE_PATH}'
    """)

    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_DB}.sales_events").first()["cnt"]
    print(f"✓ Bronze table written (batch): {BRONZE_DB}.sales_events ({count:,} rows)")

else:
    # ── Production: streaming write ──
    bronze_query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .partitionBy("_eh_partition")
        .trigger(processingTime="30 seconds")
        .option("mergeSchema", "false")
        .start(BRONZE_PATH)
    )

    print(f"✓ Bronze Ingestion Stream STARTED → {BRONZE_PATH}")

    import time
    for i in range(5):
        status = bronze_query.status
        progress = bronze_query.recentProgress
        print(f"\n── Check {i+1}/5 ──")
        print(f"  isActive: {bronze_query.isActive}")
        if progress:
            latest = progress[-1]
            print(f"  numInputRows: {latest.get('numInputRows', 'N/A')}")
        time.sleep(10)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_DB}.sales_events
        USING DELTA
        LOCATION '{BRONZE_PATH}'
    """)

    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_DB}.sales_events").first()["cnt"]
    print(f"✓ Bronze table registered: {BRONZE_DB}.sales_events ({count:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Bronze Ingestion hoàn tất.**
# MAGIC ## 6. Data Quality Check (Bronze)

# COMMAND ----------

# Kiểm tra chất lượng dữ liệu ở Bronze
quality_df = spark.sql(f"""
    SELECT
        COUNT(*)                                    AS total_rows,
        COUNT(DISTINCT store_id)                    AS unique_stores,
        COUNT(DISTINCT product_id)                  AS unique_products,
        SUM(CASE WHEN event_timestamp_str IS NULL THEN 1 ELSE 0 END) AS null_timestamps,
        SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END)             AS null_quantities,
        SUM(CASE WHEN price IS NULL AND unit_price IS NULL THEN 1 ELSE 0 END) AS null_prices,
        SUM(CASE WHEN _raw_body IS NULL THEN 1 ELSE 0 END)            AS null_bodies,
        MIN(_ingested_at)                           AS first_ingested,
        MAX(_ingested_at)                           AS last_ingested
    FROM {BRONZE_DB}.sales_events
""")

quality_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Trạng thái**: Stream đang chạy liên tục. Notebook này được giữ alive bởi Databricks Job.
# MAGIC Tiếp tục với `02_silver_etl.py` để xử lý dữ liệu từ Bronze → Silver.
