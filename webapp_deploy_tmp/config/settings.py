"""
Cấu hình trung tâm cho hệ thống trực quan dữ liệu bán hàng thời gian thực.

Tất cả giá trị được đọc từ biến môi trường (.env). Nếu biến để trống và
có KEY_VAULT_URI, sẽ tự động lấy secret từ Azure Key Vault.

    cp .env.example .env
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

_logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Key Vault integration
# ─────────────────────────────────────────────
KEY_VAULT_URI = os.getenv("KEY_VAULT_URI", "")
KEY_VAULT_NAME = os.getenv("KEY_VAULT_NAME", "")

_kv_client = None


def _get_kv_client():
    """Lazy-init Key Vault client (chỉ tạo khi cần)."""
    global _kv_client
    if _kv_client is not None:
        return _kv_client
    if not KEY_VAULT_URI:
        return None
    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
        credential = DefaultAzureCredential()
        _kv_client = SecretClient(vault_url=KEY_VAULT_URI, credential=credential)
        _logger.info("Key Vault client initialized: %s", KEY_VAULT_URI)
        return _kv_client
    except Exception as e:
        _logger.warning("Không thể kết nối Key Vault: %s", e)
        return None


def _get_secret(env_var: str, kv_secret_name: str, default: str = "") -> str:
    """
    Ưu tiên: env var → Key Vault → default.
    Nếu env var có giá trị (không rỗng, không placeholder) thì dùng luôn.
    Nếu không, thử lấy từ Key Vault.
    """
    value = os.getenv(env_var, "")
    if value and not value.startswith("<"):
        return value
    client = _get_kv_client()
    if client:
        try:
            secret = client.get_secret(kv_secret_name)
            return secret.value
        except Exception as e:
            _logger.debug("Key Vault secret '%s' not found: %s", kv_secret_name, e)
    return default


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return float(value)


# ─────────────────────────────────────────────
# Azure Event Hubs
# ─────────────────────────────────────────────
EVENT_HUB_CONNECTION_STRING = _get_secret(
    "EVENT_HUB_CONNECTION_STRING", "event-hub-connection-string"
)
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "sales-events")

EVENT_HUB_MAX_RETRIES = _get_int("EVENT_HUB_MAX_RETRIES", 5)
EVENT_HUB_RETRY_BACKOFF_FACTOR = _get_float("EVENT_HUB_RETRY_BACKOFF_FACTOR", 0.8)
EVENT_HUB_RETRY_BACKOFF_MAX = _get_int("EVENT_HUB_RETRY_BACKOFF_MAX", 30)
EVENT_HUB_SEND_TIMEOUT = _get_int("EVENT_HUB_SEND_TIMEOUT", 30)

# ─────────────────────────────────────────────
# Azure SQL Database
# ─────────────────────────────────────────────
SQL_SERVER = os.getenv("SQL_SERVER", "sql-sales-analytics-d9bt2m.database.windows.net")
SQL_DATABASE = os.getenv("SQL_DATABASE", "SalesAnalyticsDB")
SQL_USERNAME = _get_secret("SQL_USERNAME", "sql-admin-username")
SQL_PASSWORD = _get_secret("SQL_PASSWORD", "sql-admin-password")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")

# ─────────────────────────────────────────────
# Azure Machine Learning
# ─────────────────────────────────────────────
AML_WORKSPACE_NAME = os.getenv("AML_WORKSPACE_NAME", "<your-aml-workspace>")
AML_SUBSCRIPTION_ID = os.getenv("AML_SUBSCRIPTION_ID", "<your-subscription-id>")
AML_RESOURCE_GROUP = os.getenv("AML_RESOURCE_GROUP", "<your-resource-group>")
AML_ENDPOINT_URL = os.getenv("AML_ENDPOINT_URL", "<your-ml-endpoint-url>")
AML_API_KEY = os.getenv("AML_API_KEY", "<your-ml-api-key>")

ML_ENDPOINT_URL = AML_ENDPOINT_URL
ML_API_KEY = AML_API_KEY

# ─────────────────────────────────────────────
# Azure Blob Storage
# ─────────────────────────────────────────────
BLOB_CONNECTION_STRING = _get_secret(
    "BLOB_CONNECTION_STRING", "blob-connection-string"
)
BLOB_CONTAINER_REFERENCE = os.getenv("BLOB_CONTAINER_REFERENCE", "reference-data")
BLOB_CONTAINER_ARCHIVE = os.getenv("BLOB_CONTAINER_ARCHIVE", "sales-archive")
BLOB_CONTAINER_STAGING = os.getenv("BLOB_CONTAINER_STAGING", "data-factory-staging")

# ─────────────────────────────────────────────
# Azure Data Factory
# ─────────────────────────────────────────────
DATA_FACTORY_NAME = os.getenv("DATA_FACTORY_NAME", "adf-sales-analytics")

# ─────────────────────────────────────────────
# Azure Resource Group & General
# ─────────────────────────────────────────────
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "<your-subscription-id>")
AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "rg-sales-analytics")
AZURE_LOCATION = os.getenv("AZURE_LOCATION", "eastus")

# ─────────────────────────────────────────────
# External APIs
# ─────────────────────────────────────────────
OPENWEATHER_API_KEY = _get_secret("OPENWEATHER_API_KEY", "openweather-api-key")
CALENDARIFIC_API_KEY = _get_secret("CALENDARIFIC_API_KEY", "calendarific-api-key")
DEFAULT_COUNTRY_CODE = os.getenv("DEFAULT_COUNTRY_CODE", "VN")

WEATHER_CACHE_TTL = _get_int("WEATHER_CACHE_TTL", 600)
HOLIDAY_CACHE_TTL = _get_int("HOLIDAY_CACHE_TTL", 86400)

# ─────────────────────────────────────────────
# Data Generator — tốc độ & tham số mô phỏng
# ─────────────────────────────────────────────
SALES_GENERATION_INTERVAL = _get_float("SALES_GENERATION_INTERVAL", 1.0)
BATCH_SIZE = _get_int("BATCH_SIZE", 10)

RATE_PER_MINUTE = _get_int("RATE_PER_MINUTE", 1200)

BURST_ENABLED = _get_bool("BURST_ENABLED", True)
BURST_MULTIPLIER = _get_int("BURST_MULTIPLIER", 3)
BURST_DURATION_SECONDS = _get_int("BURST_DURATION_SECONDS", 15)

REPLAY_MODE = _get_bool("REPLAY_MODE", False)
REPLAY_FILE = os.getenv("REPLAY_FILE", "sample_events.jsonl")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ─────────────────────────────────────────────
# Databricks
# ─────────────────────────────────────────────
DATABRICKS_HOST = _get_secret(
    "DATABRICKS_HOST", "databricks-host",
    default="https://adb-7405619740283965.5.azuredatabricks.net",
)
DATABRICKS_TOKEN = _get_secret("DATABRICKS_TOKEN", "databricks-token")

# ─────────────────────────────────────────────
# Danh mục dữ liệu giả lập
# Nguồn duy nhất (single source of truth) cho tất cả product definitions.
# SALES_PRODUCTS được derive từ PRODUCTS — không hardcode riêng.
# ─────────────────────────────────────────────
PRODUCTS = [
    # ── Sản phẩm Event Hub (data generator → Stream Analytics) ──
    {"id": "COKE",  "name": "Coca-Cola",      "category": "Beverage",     "base_price": 1.5,  "min_price": 1.2, "max_price": 1.8},
    {"id": "PEPSI", "name": "Pepsi",          "category": "Beverage",     "base_price": 1.4,  "min_price": 1.1, "max_price": 1.7},
    {"id": "BREAD", "name": "Bread",          "category": "Bakery",       "base_price": 1.15, "min_price": 0.8, "max_price": 1.5},
    {"id": "MILK",  "name": "Milk",           "category": "Dairy",        "base_price": 1.6,  "min_price": 1.0, "max_price": 2.2},
    # ── Sản phẩm mở rộng (web app, blob reference, Power BI) ──
    {"id": "P001", "name": "Laptop",         "category": "Electronics", "base_price": 999.99},
    {"id": "P002", "name": "Smartphone",     "category": "Electronics", "base_price": 699.99},
    {"id": "P003", "name": "Headphones",     "category": "Electronics", "base_price": 149.99},
    {"id": "P004", "name": "Tablet",         "category": "Electronics", "base_price": 499.99},
    {"id": "P005", "name": "Smart Watch",    "category": "Electronics", "base_price": 299.99},
    {"id": "P006", "name": "T-Shirt",        "category": "Clothing",    "base_price": 29.99},
    {"id": "P007", "name": "Jeans",          "category": "Clothing",    "base_price": 59.99},
    {"id": "P008", "name": "Sneakers",       "category": "Clothing",    "base_price": 89.99},
    {"id": "P009", "name": "Coffee Maker",   "category": "Home",        "base_price": 79.99},
    {"id": "P010", "name": "Blender",        "category": "Home",        "base_price": 49.99},
    {"id": "P011", "name": "Desk Lamp",      "category": "Home",        "base_price": 34.99},
    {"id": "P012", "name": "Backpack",       "category": "Accessories", "base_price": 45.99},
    {"id": "P013", "name": "Sunglasses",     "category": "Accessories", "base_price": 129.99},
    {"id": "P014", "name": "Wireless Mouse", "category": "Electronics", "base_price": 39.99},
    {"id": "P015", "name": "Keyboard",       "category": "Electronics", "base_price": 69.99},
]

# Derive SALES_PRODUCTS từ PRODUCTS (chỉ những sản phẩm có min_price/max_price)
SALES_PRODUCTS = [
    {"product_id": p["id"], "min_price": p["min_price"], "max_price": p["max_price"]}
    for p in PRODUCTS if "min_price" in p and "max_price" in p
]

# Tập hợp tất cả product IDs hợp lệ (dùng cho validation)
VALID_PRODUCT_IDS = {p["id"] for p in PRODUCTS}

STORE_IDS = ["S01", "S02", "S03"]

STORE_LOCATIONS = {
    "S01": {"name": "Ho Chi Minh City", "lat": 10.8231, "lon": 106.6297},
    "S02": {"name": "Ha Noi",           "lat": 21.0278, "lon": 105.8342},
    "S03": {"name": "Da Nang",          "lat": 16.0544, "lon": 108.2022},
}

REGIONS = ["North", "South", "East", "West", "Central"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"]
CUSTOMER_SEGMENTS = ["Regular", "Premium", "VIP", "New"]

# ─────────────────────────────────────────────
# MLOps — Training Pipeline & Drift Detection
# ─────────────────────────────────────────────
ML_TRAINING_COMPUTE = os.getenv("ML_TRAINING_COMPUTE", "training-cluster")
ML_ENDPOINT_NAME = os.getenv("ML_ENDPOINT_NAME", "sales-forecast-endpoint")
ML_MODEL_NAME = os.getenv("ML_MODEL_NAME", "sales-forecast-model")
ML_EXPERIMENT_NAME = os.getenv("ML_EXPERIMENT_NAME", "sales-forecast-training")

# Drift detection thresholds
DRIFT_KS_PVALUE_MIN = _get_float("DRIFT_KS_PVALUE_MIN", 0.01)
DRIFT_PSI_MAX = _get_float("DRIFT_PSI_MAX", 0.2)
DRIFT_R2_DEGRADATION_MAX = _get_float("DRIFT_R2_DEGRADATION_MAX", 0.15)
DRIFT_MAE_INCREASE_MAX = _get_float("DRIFT_MAE_INCREASE_MAX", 0.20)
DRIFT_CHECK_DAYS = _get_int("DRIFT_CHECK_DAYS", 7)

# Auto-retrain
AUTO_RETRAIN_ENABLED = _get_bool("AUTO_RETRAIN_ENABLED", True)
RETRAIN_MIN_SAMPLES = _get_int("RETRAIN_MIN_SAMPLES", 50000)


def _is_missing(value: str) -> bool:
    return (not value) or value.startswith("<")


def validate_required_settings(mode: str = "generator") -> None:
    """
    mode:
    - generator: chạy local live/replay
    - eventhub: chuẩn bị gửi lên Event Hubs
    """
    missing = []

    if mode == "generator":
        required = {
            "OPENWEATHER_API_KEY": OPENWEATHER_API_KEY,
            "CALENDARIFIC_API_KEY": CALENDARIFIC_API_KEY,
        }
    elif mode == "eventhub":
        required = {
            "EVENT_HUB_CONNECTION_STRING": EVENT_HUB_CONNECTION_STRING,
            "EVENT_HUB_NAME": EVENT_HUB_NAME,
            "OPENWEATHER_API_KEY": OPENWEATHER_API_KEY,
            "CALENDARIFIC_API_KEY": CALENDARIFIC_API_KEY,
        }
    else:
        raise ValueError(f"mode không hợp lệ: {mode}")

    for key, value in required.items():
        if _is_missing(value):
            missing.append(key)

    if missing:
        raise ValueError(
            "Thiếu biến môi trường bắt buộc hoặc vẫn để placeholder: "
            + ", ".join(missing)
        )


def get_runtime_config() -> dict:
    """
    Trả về cấu hình runtime đã rút gọn để log/debug.
    Không trả ra secrets.
    """
    return {
        "EVENT_HUB_NAME": EVENT_HUB_NAME,
        "AZURE_RESOURCE_GROUP": AZURE_RESOURCE_GROUP,
        "AZURE_LOCATION": AZURE_LOCATION,
        "BATCH_SIZE": BATCH_SIZE,
        "RATE_PER_MINUTE": RATE_PER_MINUTE,
        "BURST_ENABLED": BURST_ENABLED,
        "BURST_MULTIPLIER": BURST_MULTIPLIER,
        "BURST_DURATION_SECONDS": BURST_DURATION_SECONDS,
        "REPLAY_MODE": REPLAY_MODE,
        "REPLAY_FILE": REPLAY_FILE,
        "LOG_LEVEL": LOG_LEVEL,
        "WEATHER_CACHE_TTL": WEATHER_CACHE_TTL,
        "HOLIDAY_CACHE_TTL": HOLIDAY_CACHE_TTL,
        "DEFAULT_COUNTRY_CODE": DEFAULT_COUNTRY_CODE,
        "STORE_IDS": STORE_IDS,
        "SALES_GENERATION_INTERVAL": SALES_GENERATION_INTERVAL,
    }