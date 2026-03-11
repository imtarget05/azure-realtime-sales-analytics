"""
Cấu hình trung tâm cho hệ thống trực quan dữ liệu bán hàng thời gian thực.

Tất cả giá trị được đọc từ biến môi trường (.env). Nếu chưa có file .env,
hãy copy file .env.example và điền thông tin Azure thực tế của bạn.

    cp .env.example .env
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# Azure Event Hubs
# Dùng để thu thập sự kiện bán hàng / thời tiết / chứng khoán
# ─────────────────────────────────────────────
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING",
    "<Your-Event-Hub-Connection-String>",
)
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "sales-events")

# ─────────────────────────────────────────────
# Azure SQL Database
# Lưu trữ dữ liệu đã qua xử lý ETL
# ─────────────────────────────────────────────
SQL_SERVER   = os.getenv("SQL_SERVER",   "<your-server>.database.windows.net")
SQL_DATABASE = os.getenv("SQL_DATABASE", "SalesAnalyticsDB")
SQL_USERNAME = os.getenv("SQL_USERNAME", "<your-username>")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "<your-password>")
SQL_DRIVER   = os.getenv("SQL_DRIVER",   "{ODBC Driver 18 for SQL Server}")

# ─────────────────────────────────────────────
# Azure Machine Learning
# Workspace cho huấn luyện mô hình & endpoint dự đoán
# ─────────────────────────────────────────────
AML_WORKSPACE_NAME  = os.getenv("AML_WORKSPACE_NAME",  "<your-aml-workspace>")
AML_SUBSCRIPTION_ID = os.getenv("AML_SUBSCRIPTION_ID", "<your-subscription-id>")
AML_RESOURCE_GROUP  = os.getenv("AML_RESOURCE_GROUP",  "<your-resource-group>")
AML_ENDPOINT_URL    = os.getenv("AML_ENDPOINT_URL",    "<your-ml-endpoint-url>")
AML_API_KEY         = os.getenv("AML_API_KEY",         "<your-ml-api-key>")

# Aliases dùng trong webapp
ML_ENDPOINT_URL = AML_ENDPOINT_URL
ML_API_KEY      = AML_API_KEY

# ─────────────────────────────────────────────
# Azure Blob Storage
# Lưu reference data, archive và Data Factory staging
# ─────────────────────────────────────────────
BLOB_CONNECTION_STRING   = os.getenv(
    "BLOB_CONNECTION_STRING",
    "<Your-Blob-Storage-Connection-String>",
)
BLOB_CONTAINER_REFERENCE = os.getenv("BLOB_CONTAINER_REFERENCE", "reference-data")
BLOB_CONTAINER_ARCHIVE   = os.getenv("BLOB_CONTAINER_ARCHIVE",   "sales-archive")
BLOB_CONTAINER_STAGING   = os.getenv("BLOB_CONTAINER_STAGING",   "data-factory-staging")

# ─────────────────────────────────────────────
# Azure Data Factory
# Orchestration pipeline: Blob → SQL, ML scheduling
# ─────────────────────────────────────────────
DATA_FACTORY_NAME = os.getenv("DATA_FACTORY_NAME", "adf-sales-analytics")

# ─────────────────────────────────────────────
# Azure Resource Group & General
# ─────────────────────────────────────────────
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID", "<your-subscription-id>")
AZURE_RESOURCE_GROUP  = os.getenv("AZURE_RESOURCE_GROUP",  "rg-sales-analytics")
AZURE_LOCATION        = os.getenv("AZURE_LOCATION",        "eastus")

# ─────────────────────────────────────────────
# Data Generator — tốc độ & tham số mô phỏng
# ─────────────────────────────────────────────
SALES_GENERATION_INTERVAL = float(os.getenv("SALES_GENERATION_INTERVAL", "1.0"))  # giây
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# Danh mục sản phẩm giả lập
PRODUCTS = [
    {"id": "P001", "name": "Laptop",        "category": "Electronics",  "base_price": 999.99},
    {"id": "P002", "name": "Smartphone",    "category": "Electronics",  "base_price": 699.99},
    {"id": "P003", "name": "Headphones",    "category": "Electronics",  "base_price": 149.99},
    {"id": "P004", "name": "Tablet",        "category": "Electronics",  "base_price": 499.99},
    {"id": "P005", "name": "Smart Watch",   "category": "Electronics",  "base_price": 299.99},
    {"id": "P006", "name": "T-Shirt",       "category": "Clothing",     "base_price":  29.99},
    {"id": "P007", "name": "Jeans",         "category": "Clothing",     "base_price":  59.99},
    {"id": "P008", "name": "Sneakers",      "category": "Clothing",     "base_price":  89.99},
    {"id": "P009", "name": "Coffee Maker",  "category": "Home",         "base_price":  79.99},
    {"id": "P010", "name": "Blender",       "category": "Home",         "base_price":  49.99},
    {"id": "P011", "name": "Desk Lamp",     "category": "Home",         "base_price":  34.99},
    {"id": "P012", "name": "Backpack",      "category": "Accessories",  "base_price":  45.99},
    {"id": "P013", "name": "Sunglasses",    "category": "Accessories",  "base_price": 129.99},
    {"id": "P014", "name": "Wireless Mouse","category": "Electronics",  "base_price":  39.99},
    {"id": "P015", "name": "Keyboard",      "category": "Electronics",  "base_price":  69.99},
]

REGIONS          = ["North", "South", "East", "West", "Central"]
PAYMENT_METHODS  = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"]
CUSTOMER_SEGMENTS = ["Regular", "Premium", "VIP", "New"]
