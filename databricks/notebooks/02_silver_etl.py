# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver ETL (Bronze → Silver)
# MAGIC
# MAGIC **Mục tiêu**: Đọc raw events từ Bronze, làm sạch, chuẩn hóa,
# MAGIC thêm cột tính toán, và ghi vào Delta Lake tầng **Silver**.
# MAGIC
# MAGIC **Xử lý chính**:
# MAGIC 1. Parse & validate JSON → loại bỏ malformed records
# MAGIC 2. Chuyển đổi Timestamp (ISO 8601 → TimestampType)
# MAGIC 3. Chuẩn hóa giá: `unit_price` = coalesce(unit_price, price)
# MAGIC 4. Tạo cột tính toán: `Total_Amount = Quantity × Unit_Price`
# MAGIC 5. Thêm time dimensions: year, month, day, hour, day_of_week
# MAGIC 6. Deduplicate theo transaction_id (hoặc composite key)
# MAGIC 7. Ghi Delta Lake (Silver) với partitioning tối ưu
# MAGIC
# MAGIC **Kiến trúc**:
# MAGIC ```
# MAGIC Delta (Bronze) ──► Spark ETL ──► Delta (Silver)
# MAGIC   raw events         clean         ├── Total_Amount
# MAGIC   + nulls            validate      ├── time dimensions
# MAGIC   + duplicates       enrich        └── deduped, typed
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType, IntegerType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Đọc dữ liệu từ Bronze (Streaming hoặc Batch)
# MAGIC
# MAGIC Silver ETL hỗ trợ 2 chế độ:
# MAGIC - **Streaming**: readStream từ Bronze Delta → writeStream Silver (near real-time)
# MAGIC - **Batch**: read từ Bronze → overwrite / merge Silver (scheduled mỗi 5 phút)
# MAGIC
# MAGIC Mặc định dùng **Streaming** để duy trì pipeline real-time.

# COMMAND ----------

# Chế độ: "streaming" hoặc "batch"
# Demo mode luôn dùng batch (toàn bộ Bronze)
ETL_MODE = spark.conf.get("pipeline.etl_mode", "streaming")
if DEMO_MODE:
    ETL_MODE = "batch"

if ETL_MODE == "streaming":
    bronze_df = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 100)
        .option("ignoreChanges", "true")
        .load(BRONZE_PATH)
    )
    print("✓ Bronze stream opened (Structured Streaming)")
else:
    if DEMO_MODE:
        # Demo: đọc toàn bộ Bronze (không filter by time)
        bronze_df = spark.read.format("delta").load(BRONZE_PATH)
        print(f"✓ Bronze batch loaded (demo mode, all rows: {bronze_df.count():,})")
    else:
        from datetime import datetime, timedelta
        cutoff = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        bronze_df = (
            spark.read.format("delta").load(BRONZE_PATH)
            .filter(F.col("_ingested_at") >= cutoff)
        )
        print(f"✓ Bronze batch loaded (since {cutoff})")

print(f"  Mode: {ETL_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Làm sạch dữ liệu (Data Cleaning)

# COMMAND ----------

cleaned_df = (
    bronze_df

    # ── 2a. Loại bỏ records có body null hoặc parse thất bại ──
    .filter(F.col("_raw_body").isNotNull())
    .filter(F.col("event_timestamp_str").isNotNull())

    # ── 2b. Parse timestamp: ISO 8601 → TimestampType ──
    # Hỗ trợ cả "2026-03-16T15:44:28Z" và "2026-03-16 15:44:28"
    .withColumn(
        "event_timestamp",
        F.coalesce(
            F.to_timestamp(F.col("event_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp(F.col("event_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ssXXX"),
            F.to_timestamp(F.col("event_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("event_timestamp_str"), "yyyy-MM-dd HH:mm:ss"),
        )
    )
    # Loại bỏ timestamp parse thất bại
    .filter(F.col("event_timestamp").isNotNull())

    # ── 2c. Chuẩn hóa giá ──
    # Dữ liệu có thể có "price" (simple) hoặc "unit_price" (extended)
    .withColumn(
        "unit_price",
        F.coalesce(
            F.col("unit_price"),
            F.col("price")
        ).cast(DoubleType())
    )

    # ── 2d. Validate business rules ──
    .filter(F.col("quantity") > 0)                     # Quantity phải > 0
    .filter(F.col("unit_price") > 0)                   # Price phải > 0
    .filter(F.col("store_id").isNotNull())              # Store phải có
    .filter(F.col("product_id").isNotNull())            # Product phải có

    # ── 2e. Cast types ──
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("temperature", F.col("temperature").cast(DoubleType()))
    .withColumn("holiday", F.col("holiday").cast(IntegerType()))
    .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)).cast(DoubleType()))
)

print("✓ Data cleaning complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cột tính toán & Feature Enrichment

# COMMAND ----------

enriched_df = (
    cleaned_df

    # ── 3a. Total_Amount = Quantity × Unit_Price × (1 - Discount) ──
    .withColumn(
        "total_amount",
        F.round(
            F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")),
            2
        )
    )

    # ── 3b. Time dimensions (tối ưu cho Power BI / aggregate queries) ──
    .withColumn("event_date",    F.to_date(F.col("event_timestamp")))
    .withColumn("event_year",    F.year(F.col("event_timestamp")))
    .withColumn("event_month",   F.month(F.col("event_timestamp")))
    .withColumn("event_day",     F.dayofmonth(F.col("event_timestamp")))
    .withColumn("event_hour",    F.hour(F.col("event_timestamp")))
    .withColumn("day_of_week",   F.dayofweek(F.col("event_timestamp")))  # 1=Sun, 7=Sat
    .withColumn("day_name",      F.date_format(F.col("event_timestamp"), "EEEE"))
    .withColumn("is_weekend",    F.when(F.dayofweek(F.col("event_timestamp")).isin(1, 7), 1).otherwise(0))

    # ── 3c. Revenue category ──
    .withColumn(
        "revenue_tier",
        F.when(F.col("total_amount") >= 500, "High")
         .when(F.col("total_amount") >= 100, "Medium")
         .otherwise("Low")
    )

    # ── 3d. Fill defaults cho nullable fields ──
    .withColumn("region",           F.coalesce(F.col("region"), F.lit("Unknown")))
    .withColumn("category",         F.coalesce(F.col("category"), F.lit("Unknown")))
    .withColumn("product_name",     F.coalesce(F.col("product_name"), F.col("product_id")))
    .withColumn("payment_method",   F.coalesce(F.col("payment_method"), F.lit("Unknown")))
    .withColumn("customer_id",      F.coalesce(F.col("customer_id"), F.lit("ANONYMOUS")))
    .withColumn("customer_segment", F.coalesce(F.col("customer_segment"), F.lit("Regular")))
    .withColumn("weather",          F.coalesce(F.col("weather"), F.lit("unknown")))

    # ── 3e. Tạo surrogate key nếu chưa có transaction_id ──
    .withColumn(
        "transaction_id",
        F.coalesce(
            F.col("transaction_id"),
            F.sha2(
                F.concat_ws("|",
                    F.col("event_timestamp_str"),
                    F.col("store_id"),
                    F.col("product_id"),
                    F.col("quantity"),
                    F.col("unit_price"),
                ),
                256
            )
        )
    )
)

print("✓ Feature enrichment complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Chọn cột Silver & Deduplicate

# COMMAND ----------

# Schema cuối cùng cho Silver layer
silver_columns = [
    # ── Primary Key ──
    "transaction_id",

    # ── Business Fields ──
    "event_timestamp",
    "store_id",
    "region",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount",
    "total_amount",

    # ── Customer ──
    "customer_id",
    "customer_segment",
    "payment_method",

    # ── Context ──
    "temperature",
    "weather",
    "holiday",

    # ── Time Dimensions ──
    "event_date",
    "event_year",
    "event_month",
    "event_day",
    "event_hour",
    "day_of_week",
    "day_name",
    "is_weekend",

    # ── Derived ──
    "revenue_tier",

    # ── Lineage ──
    "_ingested_at",
    "_source",
]

silver_df = enriched_df.select(*silver_columns)

# ── Deduplicate: giữ record mới nhất per transaction_id ──
if ETL_MODE == "batch":
    window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
    silver_df = (
        silver_df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    print("✓ Deduplication complete (batch mode)")
else:
    # Streaming mode: deduplicate bằng watermark + dropDuplicates
    silver_df = (
        silver_df
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(["transaction_id"])
    )
    print("✓ Deduplication with watermark (streaming mode)")

print(f"  Output columns: {len(silver_columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Delta Lake (Silver)

# COMMAND ----------

ensure_databases()

if ETL_MODE == "streaming":
    # ── Streaming: foreachBatch để MERGE (upsert) ──
    def upsert_to_silver(batch_df, batch_id):
        """MERGE micro-batch vào Silver Delta table."""
        if batch_df.isEmpty():
            return

        batch_df.createOrReplaceTempView("silver_updates")

        # Kiểm tra Silver table đã tồn tại chưa
        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            spark.sql(f"""
                MERGE INTO delta.`{SILVER_PATH}` AS target
                USING silver_updates AS source
                ON target.transaction_id = source.transaction_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            # Lần đầu: tạo table mới
            batch_df.write.format("delta") \
                .partitionBy("event_date") \
                .mode("overwrite") \
                .save(SILVER_PATH)

        print(f"  Batch {batch_id}: {batch_df.count()} rows merged to Silver")

    silver_query = (
        silver_df.writeStream
        .foreachBatch(upsert_to_silver)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .trigger(processingTime="1 minute")
        .start()
    )

    print(f"""
╔══════════════════════════════════════════════════════╗
║  Silver ETL Stream STARTED                           ║
╠══════════════════════════════════════════════════════╣
║  Source:      Bronze Delta ({BRONZE_PATH[-35:]})     ║
║  Destination: Silver Delta ({SILVER_PATH[-35:]})     ║
║  Trigger:     processingTime = 1 minute              ║
║  Mode:        MERGE (upsert by transaction_id)       ║
║  Partition:   event_date                             ║
╚══════════════════════════════════════════════════════╝
    """)

else:
    # ── Batch: MERGE vào Silver ──
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("target")
            .merge(silver_df.alias("source"), "target.transaction_id = source.transaction_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("✓ Batch MERGE to Silver complete")
    else:
        (
            silver_df.write.format("delta")
            .partitionBy("event_date")
            .mode("overwrite")
            .save(SILVER_PATH)
        )
        print("✓ Silver table created (first run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Đăng ký Silver Table & Optimize

# COMMAND ----------

# Đăng ký table trong catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DB}.sales_transactions
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print(f"✓ Silver table registered: {SILVER_DB}.sales_transactions")

# OPTIMIZE + Z-ORDER cho query performance
try:
    spark.sql(f"""
        OPTIMIZE delta.`{SILVER_PATH}`
        ZORDER BY (store_id, event_date)
    """)
    print(f"✓ OPTIMIZE + ZORDER complete")
except Exception as e:
    print(f"⚠ OPTIMIZE skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Check (Silver)

# COMMAND ----------

quality_check = spark.sql(f"""
    SELECT
        COUNT(*)                                          AS total_rows,
        COUNT(DISTINCT transaction_id)                    AS unique_transactions,
        COUNT(DISTINCT store_id)                          AS unique_stores,
        COUNT(DISTINCT product_id)                        AS unique_products,
        COUNT(DISTINCT customer_id)                       AS unique_customers,
        ROUND(AVG(total_amount), 2)                       AS avg_total_amount,
        ROUND(SUM(total_amount), 2)                       AS grand_total,
        MIN(event_timestamp)                              AS earliest_event,
        MAX(event_timestamp)                              AS latest_event,
        SUM(CASE WHEN total_amount <= 0 THEN 1 ELSE 0 END) AS invalid_amounts,
        ROUND(
            100.0 * SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) / COUNT(*),
            2
        )                                                 AS pct_null_txn_id
    FROM {SILVER_DB}.sales_transactions
""")

quality_check.display()

# ── Fail-safe: nếu >5% records invalid → raise alert ──
row = quality_check.first()
if row["invalid_amounts"] > 0:
    invalid_pct = 100.0 * row["invalid_amounts"] / max(row["total_rows"], 1)
    if invalid_pct > 5:
        raise ValueError(
            f"DATA QUALITY ALERT: {invalid_pct:.1f}% invalid amounts detected! "
            f"({row['invalid_amounts']} / {row['total_rows']} rows)"
        )

print("✓ Silver data quality check PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sample Data Preview

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT transaction_id, event_timestamp, store_id, region,
               product_id, product_name, category, quantity,
               unit_price, discount, total_amount, revenue_tier,
               customer_id, weather, event_date, day_name
        FROM {SILVER_DB}.sales_transactions
        ORDER BY event_timestamp DESC
        LIMIT 20
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Silver ETL hoàn tất.** Dữ liệu giờ đã sạch, có cấu trúc, sẵn sàng cho:
# MAGIC - `03_gold_aggregation.py` — Tạo bảng tổng hợp cho BI
# MAGIC - Power BI DirectQuery / Serverless SQL
# MAGIC - ML Feature Store
