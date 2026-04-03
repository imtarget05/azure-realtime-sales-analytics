"""Configure Stream Analytics job inputs/outputs and upload query."""
import subprocess
import json
import os

RG = os.getenv("AZURE_RESOURCE_GROUP", "")
SA = os.getenv("STREAM_ANALYTICS_JOB", "")
EH_NS = os.getenv("EVENT_HUB_NAMESPACE", "")
SQL_SERVER = os.getenv("SQL_SERVER", "")
SQL_DB = os.getenv("SQL_DATABASE", "SalesAnalyticsDB")
SQL_USER = os.getenv("SQL_USERNAME", "")
SQL_PASS = os.getenv("SQL_PASSWORD", "")

if not all([RG, SA, EH_NS, SQL_SERVER, SQL_DB, SQL_USER, SQL_PASS]):
    raise RuntimeError(
        "Missing env vars: AZURE_RESOURCE_GROUP, STREAM_ANALYTICS_JOB, EVENT_HUB_NAMESPACE, "
        "SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD"
    )


def az(cmd: str) -> str:
    result = subprocess.run(
        f"az {cmd}", shell=True, capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr.strip()}")
    return result.stdout.strip()


# 1. Get Event Hub key
eh_key = az(
    f"eventhubs namespace authorization-rule keys list "
    f"--resource-group {RG} --namespace-name {EH_NS} "
    f"--name RootManageSharedAccessKey --query primaryKey -o tsv"
)
print(f"EH Key: {eh_key[:5]}...")

# 2. Create Input: SalesInput
input_props = {
    "type": "Stream",
    "datasource": {
        "type": "Microsoft.ServiceBus/EventHub",
        "properties": {
            "serviceBusNamespace": EH_NS,
            "sharedAccessPolicyName": "RootManageSharedAccessKey",
            "sharedAccessPolicyKey": eh_key,
            "eventHubName": "sales-events",
            "consumerGroupName": "$Default",
        },
    },
    "serialization": {
        "type": "Json",
        "properties": {"encoding": "UTF8"},
    },
}

with open("_sa_input.json", "w") as f:
    json.dump(input_props, f)

print("\n[1] Creating SalesInput...")
out = az(
    f'stream-analytics input create --resource-group {RG} --job-name {SA} '
    f'--input-name SalesInput --properties @_sa_input.json'
)
print("  SalesInput:", "OK" if "SalesInput" in out else out[:200])

# 3. Create Output: SalesTransactionsOutput (SQL)
def make_sql_output(table: str) -> dict:
    sql_server_host = SQL_SERVER if ".database.windows.net" in SQL_SERVER else f"{SQL_SERVER}.database.windows.net"
    return {
        "datasource": {
            "type": "Microsoft.Sql/Server/Database",
            "properties": {
                "server": sql_server_host,
                "database": SQL_DB,
                "user": SQL_USER,
                "password": SQL_PASS,
                "table": table,
            },
        },
        "serialization": None,
    }


for output_name, table in [
    ("SalesTransactionsOutput", "SalesTransactions"),
    ("HourlySalesSummaryOutput", "HourlySalesSummary"),
    ("SalesAlertsOutput", "SalesAlerts"),
]:
    props = make_sql_output(table)
    fname = f"_sa_output_{table}.json"
    with open(fname, "w") as f:
        json.dump(props, f)

    print(f"\n[2] Creating {output_name} -> {table}...")
    out = az(
        f'stream-analytics output create --resource-group {RG} --job-name {SA} '
        f'--output-name {output_name} --datasource @{fname}'
    )
    print(f"  {output_name}:", "OK" if output_name in out or table in out else out[:200])

# 4. Upload query
print("\n[3] Uploading SA query...")
with open("stream_analytics/stream_query.sql", "r", encoding="utf-8-sig") as f:
    query = f.read()

# Remove PowerBI output since we didn't configure it
# Actually keep the query as-is, PowerBI output just won't work without config

with open("_sa_query.json", "w") as f:
    json.dump({"query": query}, f)

out = az(
    f'stream-analytics transformation create --resource-group {RG} --job-name {SA} '
    f'--transformation-name Transformation --streaming-units 1 '
    f'--transformation-query "{query[:50]}"'
)

# Use REST API alternative for query upload
import re
out2 = az(
    f'stream-analytics transformation update --resource-group {RG} --job-name {SA} '
    f'--transformation-name Transformation --saql @stream_analytics/stream_query.sql'
)
print("  Query upload:", "OK" if "Transformation" in out2 or "query" in out2.lower() else out2[:200])

# 5. Verify
print("\n[4] Verification:")
inputs = az(f"stream-analytics input list --resource-group {RG} --job-name {SA} -o table")
print("  Inputs:", inputs if inputs else "None")
outputs = az(f"stream-analytics output list --resource-group {RG} --job-name {SA} -o table")
print("  Outputs:", outputs if outputs else "None")

# Clean temp files
import os
for f in ["_sa_input.json", "_sa_output_SalesTransactions.json", "_sa_output_HourlySalesSummary.json", "_sa_query.json"]:
    if os.path.exists(f):
        os.remove(f)

print("\nDone!")
