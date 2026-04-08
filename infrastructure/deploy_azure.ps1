# ============================================================
# Script triển khai hạ tầng Azure (PowerShell version cho Windows)
# Chạy: az login trước khi chạy script này
# ============================================================

$ErrorActionPreference = "Stop"

# ========================
# CẤU HÌNH
# ========================
$suffix = -join ((48..57) + (97..102) | Get-Random -Count 8 | ForEach-Object { [char]$_ })
$RESOURCE_GROUP = "rg-sales-analytics"
$LOCATION = "eastus"
$EVENT_HUB_NAMESPACE = "ehns-sales-$suffix"
$SQL_SERVER_NAME = "sql-sales-$suffix"
$SQL_ADMIN_USER = "sqladmin"
$SQL_ADMIN_PASSWORD = "P@ssw0rd$($suffix)!"
$SQL_DB_NAME = "SalesAnalyticsDB"
$STREAM_ANALYTICS_JOB = "sa-sales-analytics"
$AML_WORKSPACE = "aml-sales-forecast"
$STORAGE_ACCOUNT = "stsales$suffix"
$DATA_FACTORY_NAME = "adf-sales-$suffix"
$KEY_VAULT_NAME = "kv-sales-$suffix"
$FUNCTION_APP_NAME = "func-sales-validation-$suffix"
$APP_INSIGHTS_NAME = "appi-sales-analytics"
$LOG_ANALYTICS_NAME = "log-sales-analytics"

function Set-KeyVaultSecretSafe {
  param(
    [Parameter(Mandatory = $true)][string]$VaultName,
    [Parameter(Mandatory = $true)][string]$SecretName,
    [Parameter(Mandatory = $true)][string]$SecretValue
  )

  az keyvault secret set --vault-name $VaultName --name $SecretName --value $SecretValue --output none
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to set secret '$SecretName' in Key Vault '$VaultName'."
  }
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  TRIEN KHAI HA TANG AZURE - HE THONG PHAN TICH BAN HANG" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Resource Group: $RESOURCE_GROUP"
Write-Host "Location: $LOCATION"
Write-Host ""

# 1. Resource Group
Write-Host "[1/8] Tao Resource Group..." -ForegroundColor Yellow
az group create --name $RESOURCE_GROUP --location $LOCATION --output table

# 2. Storage Account (Blob Storage)
Write-Host "[2/10] Tao Storage Account (Blob Storage)..." -ForegroundColor Yellow
az storage account create `
  --name $STORAGE_ACCOUNT `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --sku Standard_LRS `
  --kind StorageV2 `
  --output table

Write-Host "[2a] Lay Blob Connection String..." -ForegroundColor Yellow
$BLOB_CONNECTION_STRING = az storage account show-connection-string `
  --name $STORAGE_ACCOUNT `
  --resource-group $RESOURCE_GROUP `
  --query "connectionString" `
  --output tsv

Write-Host "[2b] Tao Blob Containers..." -ForegroundColor Yellow
az storage container create --name "reference-data" --connection-string $BLOB_CONNECTION_STRING --output table
az storage container create --name "sales-archive" --connection-string $BLOB_CONNECTION_STRING --output table
az storage container create --name "data-factory-staging" --connection-string $BLOB_CONNECTION_STRING --output table
Write-Host "  Blob Connection String: $BLOB_CONNECTION_STRING" -ForegroundColor Green

# 3. Event Hubs
Write-Host "[3/10] Tao Event Hubs Namespace..." -ForegroundColor Yellow
az eventhubs namespace create `
  --name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --sku Standard `
  --capacity 1 `
  --output table

Write-Host "[3a] Tao Event Hub: sales-events..." -ForegroundColor Yellow
az eventhubs eventhub create `
  --name "sales-events" `
  --namespace-name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --partition-count 4 `
  --message-retention 1 `
  --output table

Write-Host "[3b] Tao Event Hub: weather-events..." -ForegroundColor Yellow
az eventhubs eventhub create `
  --name "weather-events" `
  --namespace-name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --partition-count 2 `
  --message-retention 1 `
  --output table

Write-Host "[3c] Tao Event Hub: stock-events..." -ForegroundColor Yellow
az eventhubs eventhub create `
  --name "stock-events" `
  --namespace-name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --partition-count 2 `
  --message-retention 1 `
  --output table

Write-Host "[3d] Tao Consumer Group..." -ForegroundColor Yellow
az eventhubs eventhub consumer-group create `
  --name "stream-analytics-cg" `
  --eventhub-name "sales-events" `
  --namespace-name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --output table

Write-Host "[3e] Lay Connection String..." -ForegroundColor Yellow
$EH_CONNECTION_STRING = az eventhubs namespace authorization-rule keys list `
  --name RootManageSharedAccessKey `
  --namespace-name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --query "primaryConnectionString" `
  --output tsv

Write-Host "  Event Hub Connection String: $EH_CONNECTION_STRING" -ForegroundColor Green

# 4. SQL Server & Database
Write-Host "[4/10] Tao Azure SQL Server..." -ForegroundColor Yellow
az sql server create `
  --name $SQL_SERVER_NAME `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --admin-user $SQL_ADMIN_USER `
  --admin-password $SQL_ADMIN_PASSWORD `
  --output table

Write-Host "[4a] Cau hinh firewall (Allow Azure Services)..." -ForegroundColor Yellow
az sql server firewall-rule create `
  --server $SQL_SERVER_NAME `
  --resource-group $RESOURCE_GROUP `
  --name "AllowAzureServices" `
  --start-ip-address 0.0.0.0 `
  --end-ip-address 0.0.0.0 `
  --output table

Write-Host "[4b] Them firewall rule cho IP hien tai..." -ForegroundColor Yellow
$MY_IP = (Invoke-RestMethod -Uri "https://api.ipify.org")
az sql server firewall-rule create `
  --server $SQL_SERVER_NAME `
  --resource-group $RESOURCE_GROUP `
  --name "AllowMyIP" `
  --start-ip-address $MY_IP `
  --end-ip-address $MY_IP `
  --output table

Write-Host "[4c] Tao Azure SQL Database..." -ForegroundColor Yellow
az sql db create `
  --server $SQL_SERVER_NAME `
  --resource-group $RESOURCE_GROUP `
  --name $SQL_DB_NAME `
  --service-objective S0 `
  --output table

# 5. Stream Analytics Job
Write-Host "[5/10] Tao Stream Analytics Job..." -ForegroundColor Yellow
az stream-analytics job create `
  --resource-group $RESOURCE_GROUP `
  --name $STREAM_ANALYTICS_JOB `
  --location $LOCATION `
  --output-error-policy "Drop" `
  --events-outoforder-policy "Adjust" `
  --events-outoforder-max-delay 5 `
  --events-late-arrival-max-delay 16 `
  --data-locale "en-US" `
  --output table

# 6. Azure Machine Learning
Write-Host "[6/10] Tao Azure Machine Learning Workspace..." -ForegroundColor Yellow
az ml workspace create `
  --name $AML_WORKSPACE `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --output table

# 7. Azure Data Factory
Write-Host "[7/10] Tao Azure Data Factory..." -ForegroundColor Yellow
az datafactory create `
  --name $DATA_FACTORY_NAME `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --output table

# 8. Event Hub Capture -> Blob Storage (archive)
Write-Host "[8/10] Cau hinh Event Hub Capture -> Blob Storage..." -ForegroundColor Yellow
$STORAGE_ID = az storage account show --name $STORAGE_ACCOUNT --resource-group $RESOURCE_GROUP --query "id" --output tsv
az eventhubs eventhub update `
  --name "sales-events" `
  --namespace-name $EVENT_HUB_NAMESPACE `
  --resource-group $RESOURCE_GROUP `
  --enable-capture true `
  --capture-interval 300 `
  --capture-size-limit 314572800 `
  --destination-name "EventHubArchive.AzureBlockBlob" `
  --storage-account $STORAGE_ID `
  --blob-container "sales-archive" `
  --archive-name-format "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}" `
  --output table 2>$null
if ($LASTEXITCODE -ne 0) { Write-Host "  [WARN] Event Hub Capture can cau hinh thu cong tren Portal." -ForegroundColor DarkYellow }

# 9-10. Stream Analytics IO
Write-Host "[9-10/10] Stream Analytics Input/Output can cau hinh tren Azure Portal." -ForegroundColor Yellow

# 11. Key Vault
Write-Host "[11/14] Tao Azure Key Vault..." -ForegroundColor Yellow
az keyvault create `
  --name $KEY_VAULT_NAME `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --sku standard `
  --enable-rbac-authorization true `
  --output table

Write-Host "[11a] Luu Secrets vao Key Vault..." -ForegroundColor Yellow
Set-KeyVaultSecretSafe -VaultName $KEY_VAULT_NAME -SecretName "event-hub-connection-string" -SecretValue $EH_CONNECTION_STRING
Set-KeyVaultSecretSafe -VaultName $KEY_VAULT_NAME -SecretName "sql-connection-string" -SecretValue "Server=$SQL_SERVER_NAME.database.windows.net;Database=$SQL_DB_NAME;User=$SQL_ADMIN_USER;Password=$SQL_ADMIN_PASSWORD"
Set-KeyVaultSecretSafe -VaultName $KEY_VAULT_NAME -SecretName "sql-admin-password" -SecretValue $SQL_ADMIN_PASSWORD
Set-KeyVaultSecretSafe -VaultName $KEY_VAULT_NAME -SecretName "blob-connection-string" -SecretValue $BLOB_CONNECTION_STRING
Write-Host "  Secrets da duoc luu vao Key Vault: $KEY_VAULT_NAME" -ForegroundColor Green

# 12. Application Insights + Log Analytics
Write-Host "[12/14] Tao Log Analytics Workspace..." -ForegroundColor Yellow
az monitor log-analytics workspace create `
  --workspace-name $LOG_ANALYTICS_NAME `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --output table

$LOG_ANALYTICS_ID = az monitor log-analytics workspace show `
  --workspace-name $LOG_ANALYTICS_NAME `
  --resource-group $RESOURCE_GROUP `
  --query "id" --output tsv

Write-Host "[12a] Tao Application Insights..." -ForegroundColor Yellow
az monitor app-insights component create `
  --app $APP_INSIGHTS_NAME `
  --resource-group $RESOURCE_GROUP `
  --location $LOCATION `
  --workspace $LOG_ANALYTICS_ID `
  --output table

$APP_INSIGHTS_KEY = az monitor app-insights component show `
  --app $APP_INSIGHTS_NAME `
  --resource-group $RESOURCE_GROUP `
  --query "connectionString" --output tsv

Set-KeyVaultSecretSafe -VaultName $KEY_VAULT_NAME -SecretName "appinsights-connection-string" -SecretValue $APP_INSIGHTS_KEY
Write-Host "  App Insights Connection String saved to Key Vault" -ForegroundColor Green

# 13. Azure Functions App (Validation Layer)
Write-Host "[13/14] Tao Azure Functions App..." -ForegroundColor Yellow
az functionapp create `
  --name $FUNCTION_APP_NAME `
  --resource-group $RESOURCE_GROUP `
  --storage-account $STORAGE_ACCOUNT `
  --consumption-plan-location $LOCATION `
  --runtime python `
  --runtime-version 3.10 `
  --functions-version 4 `
  --os-type Linux `
  --output table

Write-Host "[13a] Cau hinh App Settings cho Function App..." -ForegroundColor Yellow
az functionapp config appsettings set `
  --name $FUNCTION_APP_NAME `
  --resource-group $RESOURCE_GROUP `
  --settings `
    "EventHubConnectionString=$EH_CONNECTION_STRING" `
    "APPINSIGHTS_INSTRUMENTATIONKEY=$APP_INSIGHTS_KEY" `
    "KEY_VAULT_NAME=$KEY_VAULT_NAME" `
  --output none

Write-Host "[13b] Bat System Managed Identity..." -ForegroundColor Yellow
$FUNC_PRINCIPAL_ID = az functionapp identity assign `
  --name $FUNCTION_APP_NAME `
  --resource-group $RESOURCE_GROUP `
  --query "principalId" --output tsv

Write-Host "[13c] Grant Key Vault access cho Function App..." -ForegroundColor Yellow
$KV_ID = az keyvault show --name $KEY_VAULT_NAME --resource-group $RESOURCE_GROUP --query "id" --output tsv
az role assignment create `
  --assignee $FUNC_PRINCIPAL_ID `
  --role "Key Vault Secrets User" `
  --scope $KV_ID `
  --output none
Write-Host "  Function App: $FUNCTION_APP_NAME (Managed Identity enabled)" -ForegroundColor Green

# 14. Diagnostic Settings (Monitor)
Write-Host "[14/14] Cau hinh Diagnostic Settings..." -ForegroundColor Yellow
$EH_NS_ID = az eventhubs namespace show --name $EVENT_HUB_NAMESPACE --resource-group $RESOURCE_GROUP --query "id" --output tsv
az monitor diagnostic-settings create `
  --name "diag-eventhub" `
  --resource $EH_NS_ID `
  --workspace $LOG_ANALYTICS_ID `
  --logs '[{"category":"OperationalLogs","enabled":true}]' `
  --metrics '[{"category":"AllMetrics","enabled":true}]' `
  --output none 2>$null
if ($LASTEXITCODE -ne 0) { Write-Host "  [WARN] Diagnostic settings co the can cau hinh thu cong." -ForegroundColor DarkYellow }

# ========================
# Tong ket
# ========================
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  TRIEN KHAI HOAN TAT!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Resource Group:      $RESOURCE_GROUP"
Write-Host "Event Hub Namespace: $EVENT_HUB_NAMESPACE"
Write-Host "Event Hub Conn Str:  $EH_CONNECTION_STRING"
Write-Host "SQL Server:          $SQL_SERVER_NAME.database.windows.net"
Write-Host "SQL Database:        $SQL_DB_NAME"
Write-Host "SQL Admin:           $SQL_ADMIN_USER"
Write-Host "SQL Password:        $SQL_ADMIN_PASSWORD"
Write-Host "Stream Analytics:    $STREAM_ANALYTICS_JOB"
Write-Host "ML Workspace:        $AML_WORKSPACE"
Write-Host "Storage Account:     $STORAGE_ACCOUNT"
Write-Host "Blob Conn String:    $BLOB_CONNECTION_STRING"
Write-Host "Data Factory:        $DATA_FACTORY_NAME"
Write-Host "Key Vault:           $KEY_VAULT_NAME"
Write-Host "Function App:        $FUNCTION_APP_NAME"
Write-Host "App Insights:        $APP_INSIGHTS_NAME"
Write-Host "Log Analytics:       $LOG_ANALYTICS_NAME"

# Luu thong tin
$output = @"
RESOURCE_GROUP=$RESOURCE_GROUP
EVENT_HUB_NAMESPACE=$EVENT_HUB_NAMESPACE
EVENT_HUB_CONNECTION_STRING=$EH_CONNECTION_STRING
SQL_SERVER=$SQL_SERVER_NAME.database.windows.net
SQL_DATABASE=$SQL_DB_NAME
SQL_ADMIN_USER=$SQL_ADMIN_USER
SQL_ADMIN_PASSWORD=$SQL_ADMIN_PASSWORD
STREAM_ANALYTICS_JOB=$STREAM_ANALYTICS_JOB
AML_WORKSPACE=$AML_WORKSPACE
STORAGE_ACCOUNT=$STORAGE_ACCOUNT
BLOB_CONNECTION_STRING=$BLOB_CONNECTION_STRING
DATA_FACTORY_NAME=$DATA_FACTORY_NAME
KEY_VAULT_NAME=$KEY_VAULT_NAME
FUNCTION_APP_NAME=$FUNCTION_APP_NAME
APP_INSIGHTS_CONNECTION_STRING=$APP_INSIGHTS_KEY
"@

$output | Out-File -FilePath "deployment_output.txt" -Encoding UTF8
Write-Host ""
Write-Host "Thong tin da duoc luu vao deployment_output.txt" -ForegroundColor Green
