param(
    [switch]$SkipWeb,
    [switch]$SkipGenerator,
    [switch]$SkipDrift
)

$ErrorActionPreference = "Continue"

function Step($msg) {
    Write-Host "`n=== $msg ===" -ForegroundColor Cyan
}

$py = "c:/Users/Admin/azure-realtime-sales-analytics/.venv/Scripts/python.exe"

Step "Pre-check: Azure + ODBC"
az account show --query "{name:name,id:id}" -o table
az group exists --name rg-sales-analytics-dev
Get-OdbcDriver | Where-Object { $_.Name -like "*ODBC Driver 18 for SQL Server*" } | Select-Object Name, Platform

Step "Scenario 1 - Real-time Anomaly (Burst)"
if (-not $SkipGenerator) {
    $env:BURST_ENABLED = "True"
    $env:BURST_MULTIPLIER = "10"
    & $py data_generator/sales_generator.py
}

Step "Scenario 2 - Drift monitor"
if (-not $SkipDrift) {
    & $py ml/drift_monitor.py --threshold-mae 1.0 --trigger-mode local
}

Step "Scenario 3 - Model governance page"
if (-not $SkipWeb) {
    Write-Host "Open: http://127.0.0.1:5000/model-report" -ForegroundColor Yellow
}

Step "Scenario 4 - Fallback demo (force AML key invalid in-process)"
$env:AML_API_KEY = "invalid-demo-key"
& $py -c "from webapp.app import call_ml_endpoint; x={'hour':12,'day_of_week':2,'day_of_month':15,'month':6,'store_id':'S01','product_id':'COKE','category':'Beverage','base_price':1.5,'temperature':28,'is_rainy':0,'holiday':0,'is_weekend':0}; r=call_ml_endpoint(x); print('source=',r.get('source')); print('status=',r.get('status'))"

Step "Scenario 5 - Security code location"
Write-Host "Open config/settings.py and show _get_secret(...) with prefer_key_vault=True" -ForegroundColor Yellow

Write-Host "`nDemo script finished." -ForegroundColor Green
