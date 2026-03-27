# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion (Structured Streaming)
# MAGIC
# MAGIC **Mục tiêu**: Đọc real-time events từ Azure Event Hubs, parse JSON,
# MAGIC và ghi nguyên trạng vào Delta Lake tầng **Bronze** (append-only).
# MAGIC
# MAGIC **Kiến trúc**:
# MAGIC ```
# MAGIC Azure Event Hub ──► Spark Structured Streaming ──► Delta Lake (Bronze)
# MAGIC                     (micro-batch / continuous)       ├── _raw_body
# MAGIC                                                      ├── parsed fields
# MAGIC                                                      └── _ingested_at
# MAGIC ```
# MAGIC
# MAGIC **Chạy**: Notebook này được trigger bởi Databricks Job (xem `job_trigger.json`).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Đọc stream từ Azure Event Hubs

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**EH_CONF)
    .load()
)

# Event Hubs trả về các cột hệ thống:
#   body (binary), partition, offset, sequenceNumber,
#   enqueuedTime, publisher, partitionKey
# Ta cần decode body → string → JSON

print("✓ Connected to Event Hub stream")
print(f"  Schema: {raw_stream.schema.fieldNames()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse JSON payload & thêm metadata

# COMMAND ----------

bronze_df = (
    raw_stream
    # ── Decode binary body → string ──
    .withColumn("_raw_body", F.col("body").cast("string"))

    # ── Parse JSON: thử extended schema trước, fallback sang raw ──
    .withColumn(
        "_parsed",
        F.coalesce(
            F.from_json(F.col("_raw_body"), EXTENDED_EVENT_SCHEMA),
            F.from_json(F.col("_raw_body"), RAW_EVENT_SCHEMA),
        )
    )

    # ── Flatten parsed fields ──
    .select(
        # Metadata từ Event Hubs
        F.col("enqueuedTime").alias("_eh_enqueued_time"),
        F.col("offset").alias("_eh_offset"),
        F.col("sequenceNumber").alias("_eh_sequence"),
        F.col("partition").alias("_eh_partition"),

        # Raw body giữ nguyên cho debug / replay
        F.col("_raw_body"),

        # Parsed fields — lấy tất cả fields từ struct
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

        # Ingestion metadata
        F.current_timestamp().alias("_ingested_at"),
        F.lit("event_hub").alias("_source"),
    )
)

print("✓ Bronze DataFrame schema ready")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write to Delta Lake (Bronze)
# MAGIC
# MAGIC Ghi chế độ **append** — Bronze layer không update/delete,
# MAGIC chỉ append raw events để giữ toàn bộ lịch sử (immutable log).

# COMMAND ----------

# Tạo database Bronze nếu chưa có
ensure_databases()

# ── Start Streaming Query ──
bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)

    # Partition by date → tối ưu query theo ngày
    .partitionBy("_eh_partition")

    # Trigger: micro-batch mỗi 30 giây (cân bằng latency vs throughput)
    # Đổi sang .trigger(continuous="1 second") nếu cần ultra-low latency
    .trigger(processingTime="30 seconds")

    # Không merge schema tự động ở Bronze (strict ingestion)
    .option("mergeSchema", "false")

    .start(BRONZE_PATH)
)

print(f"""
╔══════════════════════════════════════════════════════╗
║  Bronze Ingestion STARTED                            ║
╠══════════════════════════════════════════════════════╣
║  Source:      Azure Event Hub ({EH_NAME})            ║
║  Destination: {BRONZE_PATH[-45:]}                    ║
║  Trigger:     processingTime = 30 seconds            ║
║  Mode:        append (immutable log)                 ║
║  Checkpoint:  {CHECKPOINT_BRONZE[-45:]}              ║
╚══════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monitor Stream

# COMMAND ----------

# Xem tiến trình stream (chạy cell này lặp lại để kiểm tra)
import time

for i in range(5):
    status = bronze_query.status
    progress = bronze_query.recentProgress

    print(f"\n── Check {i+1}/5 ──")
    print(f"  isActive:      {bronze_query.isActive}")
    print(f"  Status:        {status}")

    if progress:
        latest = progress[-1]
        print(f"  numInputRows:  {latest.get('numInputRows', 'N/A')}")
        print(f"  inputRowsPerSecond: {latest.get('inputRowsPerSecond', 'N/A'):.1f}")
        print(f"  processedRowsPerSecond: {latest.get('processedRowsPerSecond', 'N/A'):.1f}")

    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Đăng ký Delta Table trong Catalog

# COMMAND ----------

# Đăng ký Bronze table để query bằng SQL
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_DB}.sales_events
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

# Kiểm tra
count = spark.sql(f"SELECT COUNT(*) as cnt FROM {BRONZE_DB}.sales_events").first()["cnt"]
print(f"✓ Bronze table registered: {BRONZE_DB}.sales_events ({count:,} rows)")

# COMMAND ----------

# MAGIC %md
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
