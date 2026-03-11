#!/bin/bash
# ============================================================
# Script triển khai hạ tầng Azure cho hệ thống phân tích bán hàng thời gian thực
# Chạy bằng Azure CLI (az login trước khi chạy)
# ============================================================

set -e

# ========================
# CẤU HÌNH - Thay đổi các giá trị theo nhu cầu
# ========================
RESOURCE_GROUP="rg-sales-analytics"
LOCATION="eastus"
EVENT_HUB_NAMESPACE="ehns-sales-$(openssl rand -hex 4)"
SQL_SERVER_NAME="sql-sales-$(openssl rand -hex 4)"
SQL_ADMIN_USER="sqladmin"
SQL_ADMIN_PASSWORD="P@ssw0rd$(openssl rand -hex 4)!"
SQL_DB_NAME="SalesAnalyticsDB"
STREAM_ANALYTICS_JOB="sa-sales-analytics"
AML_WORKSPACE="aml-sales-forecast"
STORAGE_ACCOUNT="stsales$(openssl rand -hex 4)"
DATA_FACTORY_NAME="adf-sales-$(openssl rand -hex 4)"

echo "============================================================"
echo "  TRIỂN KHAI HẠ TẦNG AZURE - HỆ THỐNG PHÂN TÍCH BÁN HÀNG"
echo "============================================================"
echo ""
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo ""

# ========================
# 1. Tạo Resource Group
# ========================
echo "[1/8] Tạo Resource Group..."
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION \
  --output table

# ========================
# 2. Tạo Storage Account (Blob Storage)
# ========================
echo "[2/10] Tạo Storage Account (Blob Storage)..."
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --output table

echo "[2a/10] Lấy Blob Connection String..."
BLOB_CONNECTION_STRING=$(az storage account show-connection-string \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --query "connectionString" \
  --output tsv)
echo "  Blob Connection String: $BLOB_CONNECTION_STRING"

echo "[2b/10] Tạo Blob Containers..."
az storage container create --name "reference-data" --connection-string "$BLOB_CONNECTION_STRING" --output table
az storage container create --name "sales-archive" --connection-string "$BLOB_CONNECTION_STRING" --output table
az storage container create --name "data-factory-staging" --connection-string "$BLOB_CONNECTION_STRING" --output table

# ========================
# 3. Tạo Event Hubs Namespace và Event Hubs
# ========================
echo "[3/10] Tạo Event Hubs Namespace..."
az eventhubs namespace create \
  --name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard \
  --capacity 1 \
  --output table

echo "[3a/10] Tạo Event Hub: sales-events..."
az eventhubs eventhub create \
  --name "sales-events" \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --partition-count 4 \
  --message-retention 1 \
  --output table

echo "[3b/10] Tạo Event Hub: weather-events..."
az eventhubs eventhub create \
  --name "weather-events" \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --partition-count 2 \
  --message-retention 1 \
  --output table

echo "[3c/10] Tạo Event Hub: stock-events..."
az eventhubs eventhub create \
  --name "stock-events" \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --partition-count 2 \
  --message-retention 1 \
  --output table

echo "[3d/10] Tạo Consumer Group cho Stream Analytics..."
az eventhubs eventhub consumer-group create \
  --name "stream-analytics-cg" \
  --eventhub-name "sales-events" \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --output table

echo "[3e/10] Lấy Connection String..."
EH_CONNECTION_STRING=$(az eventhubs namespace authorization-rule keys list \
  --name RootManageSharedAccessKey \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --query "primaryConnectionString" \
  --output tsv)

echo "  Event Hub Connection String: $EH_CONNECTION_STRING"

# ========================
# 4. Tạo Azure SQL Server và Database
# ========================
echo "[4/10] Tạo Azure SQL Server..."
az sql server create \
  --name $SQL_SERVER_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --admin-user $SQL_ADMIN_USER \
  --admin-password "$SQL_ADMIN_PASSWORD" \
  --output table

echo "[4a/10] Cấu hình firewall rule (cho phép Azure services)..."
az sql server firewall-rule create \
  --server $SQL_SERVER_NAME \
  --resource-group $RESOURCE_GROUP \
  --name "AllowAzureServices" \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0 \
  --output table

echo "[4b/10] Thêm firewall rule cho IP hiện tại..."
MY_IP=$(curl -s https://api.ipify.org)
az sql server firewall-rule create \
  --server $SQL_SERVER_NAME \
  --resource-group $RESOURCE_GROUP \
  --name "AllowMyIP" \
  --start-ip-address $MY_IP \
  --end-ip-address $MY_IP \
  --output table

echo "[4c/10] Tạo Azure SQL Database..."
az sql db create \
  --server $SQL_SERVER_NAME \
  --resource-group $RESOURCE_GROUP \
  --name $SQL_DB_NAME \
  --service-objective S0 \
  --output table

SQL_CONN_STRING="Server=tcp:${SQL_SERVER_NAME}.database.windows.net,1433;Database=${SQL_DB_NAME};User ID=${SQL_ADMIN_USER};Password=${SQL_ADMIN_PASSWORD};Encrypt=yes;TrustServerCertificate=no;"
echo "  SQL Connection String: $SQL_CONN_STRING"

# ========================
# 5. Tạo Stream Analytics Job
# ========================
echo "[5/10] Tạo Stream Analytics Job..."
az stream-analytics job create \
  --resource-group $RESOURCE_GROUP \
  --name $STREAM_ANALYTICS_JOB \
  --location $LOCATION \
  --output-error-policy "Drop" \
  --events-outoforder-policy "Adjust" \
  --events-outoforder-max-delay 5 \
  --events-late-arrival-max-delay 16 \
  --data-locale "en-US" \
  --output table

# ========================
# 6. Tạo Azure Machine Learning Workspace
# ========================
echo "[6/10] Tạo Azure Machine Learning Workspace..."
az ml workspace create \
  --name $AML_WORKSPACE \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --output table

# ========================
# 7. Tạo Azure Data Factory
# ========================
echo "[7/10] Tạo Azure Data Factory..."
az datafactory create \
  --name $DATA_FACTORY_NAME \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --output table

# ========================
# 8. Cấu hình Event Hub Capture -> Blob Storage
# ========================
echo "[8/10] Cấu hình Event Hub Capture -> Blob Storage..."
STORAGE_ID=$(az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query "id" --output tsv)
az eventhubs eventhub update \
  --name "sales-events" \
  --namespace-name $EVENT_HUB_NAMESPACE \
  --resource-group $RESOURCE_GROUP \
  --enable-capture true \
  --capture-interval 300 \
  --capture-size-limit 314572800 \
  --destination-name "EventHubArchive.AzureBlockBlob" \
  --storage-account $STORAGE_ID \
  --blob-container "sales-archive" \
  --archive-name-format "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}" \
  --output table 2>/dev/null || echo "  [WARN] Event Hub Capture có thể cần cấu hình thủ công trên Portal."

# ========================
# 9. Cấu hình Stream Analytics Input (Event Hub)
# ========================
echo "[9/10] Cấu hình Stream Analytics Input..."

# Tạo file JSON cho input configuration
cat > /tmp/sa-input-sales.json << EOF
{
  "properties": {
    "type": "Stream",
    "datasource": {
      "type": "Microsoft.EventHub/EventHub",
      "properties": {
        "serviceBusNamespace": "$EVENT_HUB_NAMESPACE",
        "sharedAccessPolicyName": "RootManageSharedAccessKey",
        "sharedAccessPolicyKey": "$(az eventhubs namespace authorization-rule keys list --name RootManageSharedAccessKey --namespace-name $EVENT_HUB_NAMESPACE --resource-group $RESOURCE_GROUP --query 'primaryKey' --output tsv)",
        "eventHubName": "sales-events",
        "consumerGroupName": "stream-analytics-cg"
      }
    },
    "serialization": {
      "type": "Json",
      "properties": {
        "encoding": "UTF8"
      }
    }
  }
}
EOF

az stream-analytics input create \
  --resource-group $RESOURCE_GROUP \
  --job-name $STREAM_ANALYTICS_JOB \
  --name "SalesInput" \
  --properties @/tmp/sa-input-sales.json \
  --output table 2>/dev/null || echo "  [WARN] Input có thể cần cấu hình thủ công trên Azure Portal."

# ========================
# 10. Cấu hình Stream Analytics Output (SQL Database)
# ========================
echo "[10/10] Cấu hình Stream Analytics Output..."

cat > /tmp/sa-output-sql.json << EOF
{
  "properties": {
    "datasource": {
      "type": "Microsoft.Sql/Server/Database",
      "properties": {
        "server": "$SQL_SERVER_NAME",
        "database": "$SQL_DB_NAME",
        "user": "$SQL_ADMIN_USER",
        "password": "$SQL_ADMIN_PASSWORD",
        "table": "SalesTransactions"
      }
    }
  }
}
EOF

az stream-analytics output create \
  --resource-group $RESOURCE_GROUP \
  --job-name $STREAM_ANALYTICS_JOB \
  --name "SQLOutput" \
  --properties @/tmp/sa-output-sql.json \
  --output table 2>/dev/null || echo "  [WARN] Output có thể cần cấu hình thủ công trên Azure Portal."

# ========================
# Tổng kết
# ========================
echo ""
echo "============================================================"
echo "  TRIỂN KHAI HOÀN TẤT!"
echo "============================================================"
echo ""
echo "Resource Group:      $RESOURCE_GROUP"
echo "Event Hub Namespace: $EVENT_HUB_NAMESPACE"
echo "Event Hub Conn Str:  $EH_CONNECTION_STRING"
echo "SQL Server:          ${SQL_SERVER_NAME}.database.windows.net"
echo "SQL Database:        $SQL_DB_NAME"
echo "SQL Admin:           $SQL_ADMIN_USER"
echo "SQL Password:        $SQL_ADMIN_PASSWORD"
echo "Stream Analytics:    $STREAM_ANALYTICS_JOB"
echo "ML Workspace:        $AML_WORKSPACE"
echo "Storage Account:     $STORAGE_ACCOUNT"
echo "Blob Conn String:    $BLOB_CONNECTION_STRING"
echo "Data Factory:        $DATA_FACTORY_NAME"
echo ""
echo "BƯỚC TIẾP THEO:"
echo "  1. Cập nhật file .env với các connection strings ở trên"
echo "  2. Upload reference data: python blob_storage/upload_reference_data.py"
echo "  3. Chạy sql/create_tables.sql và sql/stored_procedures.sql trên Azure SQL"
echo "  4. Cấu hình Stream Analytics query trên Azure Portal"
echo "  5. Chạy data generator: python data_generator/sales_generator.py"
echo "  6. Tạo Data Factory pipeline: python data_factory/create_pipeline.py"
echo "  7. Kết nối Power BI với Azure SQL Database"
echo "============================================================"

# Lưu thông tin ra file
cat > deployment_output.txt << EOF
RESOURCE_GROUP=$RESOURCE_GROUP
EVENT_HUB_NAMESPACE=$EVENT_HUB_NAMESPACE
EVENT_HUB_CONNECTION_STRING=$EH_CONNECTION_STRING
SQL_SERVER=${SQL_SERVER_NAME}.database.windows.net
SQL_DATABASE=$SQL_DB_NAME
SQL_ADMIN_USER=$SQL_ADMIN_USER
SQL_ADMIN_PASSWORD=$SQL_ADMIN_PASSWORD
STREAM_ANALYTICS_JOB=$STREAM_ANALYTICS_JOB
AML_WORKSPACE=$AML_WORKSPACE
STORAGE_ACCOUNT=$STORAGE_ACCOUNT
BLOB_CONNECTION_STRING=$BLOB_CONNECTION_STRING
DATA_FACTORY_NAME=$DATA_FACTORY_NAME
EOF

echo "Thông tin đã được lưu vào deployment_output.txt"
