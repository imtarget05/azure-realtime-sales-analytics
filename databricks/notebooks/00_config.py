# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Configuration — Lakehouse Pipeline
# MAGIC Cấu hình chung cho Bronze / Silver / Gold layers.
# MAGIC Notebook này được `%run` từ các notebook khác.

# COMMAND ----------

# ── Storage Paths (ADLS Gen2 / Databricks Unity Catalog) ────────────
# Thay <storage-account> bằng tên Storage Account thực tế.
# Mount point hoặc abfss:// đều được hỗ trợ.

STORAGE_ACCOUNT = spark.conf.get(
    "pipeline.storage_account", "salesdatalake"
)
CONTAINER = spark.conf.get(
    "pipeline.container", "lakehouse"
)

# Base path — dùng abfss:// để truy cập trực tiếp ADLS Gen2
LAKEHOUSE_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Delta Lake layer paths
BRONZE_PATH = f"{LAKEHOUSE_BASE}/bronze/sales_events"
SILVER_PATH = f"{LAKEHOUSE_BASE}/silver/sales_transactions"
GOLD_PATH   = f"{LAKEHOUSE_BASE}/gold"

# Checkpoint directories cho Structured Streaming
CHECKPOINT_BRONZE = f"{LAKEHOUSE_BASE}/_checkpoints/bronze_ingestion"
CHECKPOINT_SILVER = f"{LAKEHOUSE_BASE}/_checkpoints/silver_etl"

# Gold layer sub-paths
GOLD_HOURLY_PATH      = f"{GOLD_PATH}/hourly_summary"
GOLD_PRODUCT_PATH     = f"{GOLD_PATH}/product_summary"
GOLD_CUSTOMER_PATH    = f"{GOLD_PATH}/customer_summary"
GOLD_VIRAL_PATH       = f"{GOLD_PATH}/viral_predictions"
GOLD_SIMILARITY_PATH  = f"{GOLD_PATH}/similarity_scores"

# MLflow model registry
MLFLOW_MODEL_NAME = spark.conf.get("pipeline.mlflow_model", "sales-viral-classifier")
MLFLOW_MODEL_STAGE = spark.conf.get("pipeline.mlflow_stage", "Production")

# COMMAND ----------

# ── Azure Event Hubs Configuration ──────────────────────────────────
# Connection string lấy từ Azure Key Vault (Databricks Secret Scope)
# Tạo secret scope:  databricks secrets create-scope --scope kv-sales
# Đặt secret:        databricks secrets put --scope kv-sales --key eh-conn-str

EH_CONN_STR = dbutils.secrets.get(scope="kv-sales", key="eh-conn-str")
EH_NAME = spark.conf.get("pipeline.eventhub_name", "sales-events")

# Kafka-compatible connection cho Spark Structured Streaming
EH_KAFKA_BOOTSTRAP = f"{EH_NAME}.servicebus.windows.net:9093"

# Event Hubs config dict (cho connector azure-eventhubs-spark)
EH_CONF = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs
        .EventHubsUtils.encrypt(EH_CONN_STR),
    "eventhubs.eventHubName": EH_NAME,
    "eventhubs.consumerGroup": spark.conf.get(
        "pipeline.consumer_group", "$Default"
    ),
    # Bắt đầu từ đầu stream khi chưa có checkpoint
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    # Giới hạn micro-batch (tránh OOM trên cluster nhỏ)
    "maxEventsPerTrigger": spark.conf.get(
        "pipeline.max_events_per_trigger", "10000"
    ),
}

# COMMAND ----------

# ── Database / Catalog Names ────────────────────────────────────────
CATALOG = spark.conf.get("pipeline.catalog", "sales_analytics")
BRONZE_DB = f"{CATALOG}.bronze"
SILVER_DB = f"{CATALOG}.silver"
GOLD_DB   = f"{CATALOG}.gold"

# COMMAND ----------

# ── Schema dữ liệu Event Hub (JSON payload) ────────────────────────
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Schema khớp với data_generator/sales_generator.py
RAW_EVENT_SCHEMA = StructType([
    StructField("timestamp",   StringType(),  True),
    StructField("store_id",    StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       DoubleType(),  True),
    StructField("temperature", DoubleType(),  True),
    StructField("weather",     StringType(),  True),
    StructField("holiday",     IntegerType(), True),
])

# Schema mở rộng cho events từ sales_generator.py dạng đầy đủ
EXTENDED_EVENT_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("timestamp",      StringType(),  True),
    StructField("store_id",       StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("price",          DoubleType(),  True),
    StructField("discount",       DoubleType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("customer_segment", StringType(), True),
    StructField("temperature",    DoubleType(),  True),
    StructField("weather",        StringType(),  True),
    StructField("holiday",        IntegerType(), True),
])

# COMMAND ----------

# ── Helper: tạo databases nếu chưa tồn tại ─────────────────────────
def ensure_databases():
    """Tạo catalog + databases cho Bronze/Silver/Gold nếu chưa có."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    print(f"✓ Databases ready: {BRONZE_DB}, {SILVER_DB}, {GOLD_DB}")

# COMMAND ----------

print(f"""
╔══════════════════════════════════════════════════════╗
║  Lakehouse Pipeline Config Loaded                    ║
╠══════════════════════════════════════════════════════╣
║  Storage:  {STORAGE_ACCOUNT:<40} ║
║  Bronze:   {BRONZE_PATH[-45:]:<40}   ║
║  Silver:   {SILVER_PATH[-45:]:<40}   ║
║  Gold:     {GOLD_PATH[-45:]:<40}     ║
║  Event Hub: {EH_NAME:<39} ║
╚══════════════════════════════════════════════════════╝
""")
