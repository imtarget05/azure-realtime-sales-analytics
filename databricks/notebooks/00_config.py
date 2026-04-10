# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Configuration — Lakehouse Pipeline
# MAGIC Cấu hình chung cho Bronze / Silver / Gold layers.
# MAGIC Notebook này được `%run` từ các notebook khác.
# MAGIC
# MAGIC Hỗ trợ 2 mode:
# MAGIC - **production**: ADLS Gen2 + Event Hub + Unity Catalog
# MAGIC - **demo**: DBFS + sample data + hive_metastore (không cần external services)

# COMMAND ----------

# ── Pipeline Mode Detection ─────────────────────────────────────────
PIPELINE_MODE = spark.conf.get("pipeline.mode", "auto")

# Auto-detect: thử lấy Event Hub secret, nếu fail → demo mode
if PIPELINE_MODE == "auto":
    try:
        _test = dbutils.secrets.get(scope="kv-sales", key="eh-conn-str")
        if _test and len(_test) > 10:
            PIPELINE_MODE = "production"
        else:
            PIPELINE_MODE = "demo"
    except Exception:
        PIPELINE_MODE = "demo"

DEMO_MODE = (PIPELINE_MODE == "demo")
print(f"Pipeline mode: {PIPELINE_MODE} {'(DBFS + sample data)' if DEMO_MODE else '(ADLS + Event Hub)'}")

# COMMAND ----------

# ── Storage Paths ────────────────────────────────────────────────────
if DEMO_MODE:
    LAKEHOUSE_BASE = "dbfs:/lakehouse"
    STORAGE_ACCOUNT = "dbfs"
    CONTAINER = "lakehouse"
else:
    STORAGE_ACCOUNT = spark.conf.get("pipeline.storage_account", "salesdatalake")
    CONTAINER = spark.conf.get("pipeline.container", "lakehouse")
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
EH_CONN_STR = None
EH_CONF = {}
EH_NAME = spark.conf.get("pipeline.eventhub_name", "sales-events")
EH_KAFKA_BOOTSTRAP = ""

if not DEMO_MODE:
    try:
        EH_CONN_STR = dbutils.secrets.get(scope="kv-sales", key="eh-conn-str")
        EH_KAFKA_BOOTSTRAP = f"{EH_NAME}.servicebus.windows.net:9093"
        EH_CONF = {
            "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs
                .EventHubsUtils.encrypt(EH_CONN_STR),
            "eventhubs.eventHubName": EH_NAME,
            "eventhubs.consumerGroup": spark.conf.get("pipeline.consumer_group", "$Default"),
            "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
            "maxEventsPerTrigger": spark.conf.get("pipeline.max_events_per_trigger", "10000"),
        }
        print("✓ Event Hub configured")
    except Exception as e:
        print(f"⚠ Event Hub secret not found: {e}")
        print("  Falling back to demo mode for bronze ingestion")
else:
    print("ℹ Demo mode — Event Hub skipped")

# COMMAND ----------

# ── Database / Catalog Names ────────────────────────────────────────
if DEMO_MODE:
    CATALOG = "hive_metastore"
    BRONZE_DB = "bronze_sales"
    SILVER_DB = "silver_sales"
    GOLD_DB   = "gold_sales"
else:
    CATALOG = spark.conf.get("pipeline.catalog", "sales_analytics")
    BRONZE_DB = f"{CATALOG}.bronze"
    SILVER_DB = f"{CATALOG}.silver"
    GOLD_DB   = f"{CATALOG}.gold"

# COMMAND ----------

# ── Schema dữ liệu Event Hub (JSON payload) ────────────────────────
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

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
    """Tạo databases cho Bronze/Silver/Gold nếu chưa có."""
    if not DEMO_MODE:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
    print(f"✓ Databases ready: {BRONZE_DB}, {SILVER_DB}, {GOLD_DB}")

# COMMAND ----------

_eh_display = EH_NAME if not DEMO_MODE else "N/A (demo)"
print(f"""
╔══════════════════════════════════════════════════════╗
║  Lakehouse Pipeline Config Loaded                    ║
╠══════════════════════════════════════════════════════╣
║  Mode:     {PIPELINE_MODE:<40} ║
║  Storage:  {STORAGE_ACCOUNT:<40} ║
║  Bronze:   {BRONZE_PATH[-45:]:<40}   ║
║  Silver:   {SILVER_PATH[-45:]:<40}   ║
║  Gold:     {GOLD_PATH[-45:]:<40}     ║
║  Event Hub: {_eh_display:<39} ║
╚══════════════════════════════════════════════════════╝
""")
