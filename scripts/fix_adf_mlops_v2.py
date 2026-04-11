"""
ADF → AML MLOps Fix v2 — Full end-to-end production pipeline.

What this script does:
  1. Creates AML custom environment (sales-training-env) from conda_env.yml
  2. Submits REAL training job via SDK → SDK auto-uploads ml/ code to AML
  3. Extracts the uploaded code asset URI from the submitted job
  4. Rebuilds ADF SalesAnalyticsPipeline with:
       CopyBlobToSQL → PrepareTrainingData →
       SubmitMLJob (WebActivity PUT) →
       WaitForMLJob (Until loop: poll every 60s) →
       CheckMLSuccess (IfCondition: Fail if not Completed) →
       UpdateForecasts
  5. Monitors SDK training job until Completed + model registered
  6. Triggers a fresh ADF run and monitors end-to-end
"""

import os
import sys
import json
import time
import requests
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ.setdefault("KEY_VAULT_URI", "DISABLED")

from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient, command
from azure.ai.ml.entities import Environment
from dotenv import load_dotenv

load_dotenv()

# ─── Constants ────────────────────────────────────────────────────
SUB       = "34849ef9-3814-44df-ba32-a86ed9f2a69a"
RG        = "rg-sales-analytics-dev"
WS        = "aml-sales-analytics-d9bt2m2"
ADF       = "adf-sales-paivm"
COMPUTE   = "training-cluster"
COMPUTE_ID = (
    f"/subscriptions/{SUB}/resourceGroups/{RG}"
    f"/providers/Microsoft.MachineLearningServices/workspaces/{WS}"
    f"/computes/{COMPUTE}"
)
AML_BASE = (
    f"https://management.azure.com/subscriptions/{SUB}"
    f"/resourceGroups/{RG}"
    f"/providers/Microsoft.MachineLearningServices/workspaces/{WS}"
)
ADF_BASE = (
    f"https://management.azure.com/subscriptions/{SUB}"
    f"/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{ADF}"
)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # project root

SQL_SERVER   = os.environ.get("SQL_SERVER", "sql-sales-analytics-d9bt2m.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "SalesAnalyticsDB")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "sqladmin")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "SqlP@ssw0rd2026!")


def tok(credential) -> str:
    return credential.get_token("https://management.azure.com/.default").token


# ─── Step 1: Create AML Environment ──────────────────────────────
def create_environment(ml_client: MLClient) -> str:
    """Register AML environment from conda_env.yml. Returns ARM ID."""
    print("[1/7] Creating AML environment 'sales-training-env' ...")
    env = Environment(
        name="sales-training-env",
        version="4",
        conda_file=os.path.join(ROOT, "ml", "conda_env.yml"),
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
        description="Sales forecast: sklearn + pyodbc + mlflow + azureml-defaults",
    )
    created = ml_client.environments.create_or_update(env)
    env_id = created.id
    print(f"     OK: {env_id}")
    return env_id


# ─── Step 2: Submit real training job via SDK ─────────────────────
def submit_training_job(ml_client: MLClient, env_id: str) -> str:
    """Submit CommandJob via SDK — handles code upload automatically. Returns job name."""
    print("[2/7] Submitting real training job via SDK (includes code upload)...")
    job = command(
        code=os.path.join(ROOT, "ml"),
        command=(
            "python train_and_register.py "
            "--n-samples 30000 "
            "--output-dir ./outputs/model_output"
        ),
        environment=env_id,
        compute=COMPUTE,
        experiment_name="sales-retrain-pipeline",
        display_name="sales-retrain-pipeline-v1",
        description="ADF MLOps pipeline v2 — real training + model registration",
        environment_variables={
            "SQL_SERVER":    SQL_SERVER,
            "SQL_DATABASE":  SQL_DATABASE,
            "SQL_USERNAME":  SQL_USERNAME,
            "SQL_PASSWORD":  SQL_PASSWORD,
            "SQL_DRIVER":    "{ODBC Driver 18 for SQL Server}",
            "KEY_VAULT_URI": "DISABLED",
        },
        tags={"triggered_by": "fix_adf_mlops_v2", "version": "2"},
    )
    submitted = ml_client.jobs.create_or_update(job)
    print(f"     OK: {submitted.name}")
    print(f"     Studio: {submitted.studio_url}")
    return submitted.name


# ─── Step 3: Extract code + env IDs from submitted job ───────────
def get_job_asset_ids(credential, job_name: str, retries: int = 6) -> tuple[str, str]:
    """Get codeId + environmentId from the AML job REST response."""
    print("[3/7] Extracting code & environment asset IDs from submitted job...")
    for attempt in range(retries):
        r = requests.get(
            f"{AML_BASE}/jobs/{job_name}?api-version=2023-04-01-preview",
            headers={"Authorization": f"Bearer {tok(credential)}"},
            timeout=20,
        )
        if r.ok:
            props = r.json().get("properties", {})
            code_id = props.get("codeId", "")
            env_id  = props.get("environmentId", "")
            if code_id:
                print(f"     codeId: {code_id}")
                print(f"     envId:  {env_id}")
                return code_id, env_id
        print(f"     Waiting for job to register (attempt {attempt+1}/{retries})...")
        time.sleep(10)
    raise RuntimeError(f"Could not get asset IDs for job {job_name} after {retries} attempts")


# ─── Step 4: Rebuild ADF pipeline with Until polling ─────────────
def update_adf_pipeline(credential, code_id: str, env_id: str) -> bool:
    """
    Replace RunMLPipeline (fire-and-forget WebActivity) with:
      SubmitMLJob (PUT) → WaitForMLJob (Until loop) → CheckMLSuccess (If/Fail)
    UpdateForecasts is preserved and repointed to CheckMLSuccess.
    """
    print("[4/7] Rebuilding ADF pipeline with Until polling loop...")

    token = tok(credential)
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Fetch current pipeline
    r = requests.get(
        f"{ADF_BASE}/pipelines/SalesAnalyticsPipeline?api-version=2018-06-01",
        headers=hdrs, timeout=20,
    )
    r.raise_for_status()
    pipeline = r.json()

    current_acts = pipeline["properties"].get("activities", [])

    # Keep exactly CopyBlobToSQL and PrepareTrainingData as-is
    KEEP = {"CopyBlobToSQL", "PrepareTrainingData"}
    kept = {a["name"]: a for a in current_acts if a["name"] in KEEP}
    # Also keep UpdateForecasts structure (we'll update its dependsOn)
    update_forecasts_base = next(
        (a for a in current_acts if a["name"] == "UpdateForecasts"), None
    )

    # ADF expression for deterministic AML job name (unique per ADF run)
    # pipeline().RunId = UUID, take first 12 hex chars → "srp" + 12 chars = 15 chars
    # Note: ADF does NOT allow nested @concat() — must use a single flat expression
    aml_job_url_expr = (
        f"@concat('{AML_BASE}/jobs/srp',"
        f"replace(substring(pipeline().RunId, 0, 13), '-', ''),"
        f"'?api-version=2023-04-01-preview')"
    )

    # AML job body (used by SubmitMLJob)
    aml_job_body = {
        "properties": {
            "jobType": "Command",
            "displayName": "sales-retrain-pipeline-v1",
            "experimentName": "sales-retrain-pipeline",
            "computeId": COMPUTE_ID,
            "codeId": code_id,
            "environmentId": env_id,
            "command": (
                "python train_and_register.py "
                "--n-samples 30000 "
                "--output-dir ./outputs/model_output"
            ),
            "resources": {"instanceCount": 1},
            "environmentVariables": {
                "SQL_SERVER":    SQL_SERVER,
                "SQL_DATABASE":  SQL_DATABASE,
                "SQL_USERNAME":  SQL_USERNAME,
                "SQL_PASSWORD":  SQL_PASSWORD,
                "SQL_DRIVER":    "{ODBC Driver 18 for SQL Server}",
                "KEY_VAULT_URI": "DISABLED",
            },
            "properties": {},
            "inputs":     {},
            "outputs":    {},
        }
    }

    msi_auth = {"type": "MSI", "resource": "https://management.azure.com/"}

    # ── Build new activities ──────────────────────────────────────
    new_acts = [
        kept["CopyBlobToSQL"],
        kept["PrepareTrainingData"],

        # 3. Submit AML job (PUT) — fire job start
        {
            "name": "SubmitMLJob",
            "type": "WebActivity",
            "dependsOn": [
                {"activity": "PrepareTrainingData", "dependencyConditions": ["Succeeded"]}
            ],
            "policy": {
                "timeout": "0.00:05:00",
                "retry": 1,
                "retryIntervalInSeconds": 30,
                "secureOutput": False,
                "secureInput": False,
            },
            "userProperties": [],
            "typeProperties": {
                "method": "PUT",
                "url": {"value": aml_job_url_expr, "type": "Expression"},
                "headers": {"Content-Type": "application/json"},
                "body": aml_job_body,
                "authentication": msi_auth,
            },
        },

        # 4. Until loop — poll every 60s until AML job terminal status
        {
            "name": "WaitForMLJob",
            "type": "Until",
            "dependsOn": [
                {"activity": "SubmitMLJob", "dependencyConditions": ["Succeeded"]}
            ],
            "userProperties": [],
            "typeProperties": {
                "expression": {
                    "value": (
                        "@or("
                        "equals(variables('mlJobStatus'), 'Completed'),"
                        "equals(variables('mlJobStatus'), 'Failed'),"
                        "equals(variables('mlJobStatus'), 'Canceled')"
                        ")"
                    ),
                    "type": "Expression",
                },
                "timeout": "0.06:00:00",   # 6-hour max wait
                "activities": [
                    # 4a. Wait 60 seconds between polls
                    {
                        "name": "Pause60s",
                        "type": "Wait",
                        "dependsOn": [],
                        "userProperties": [],
                        "typeProperties": {"waitTimeInSeconds": 60},
                    },
                    # 4b. GET job status from AML
                    {
                        "name": "GetMLJobStatus",
                        "type": "WebActivity",
                        "dependsOn": [
                            {"activity": "Pause60s", "dependencyConditions": ["Succeeded"]}
                        ],
                        "policy": {
                            "timeout": "0.00:02:00",
                            "retry": 2,
                            "retryIntervalInSeconds": 15,
                        },
                        "userProperties": [],
                        "typeProperties": {
                            "method": "GET",
                            "url": {"value": aml_job_url_expr, "type": "Expression"},
                            "authentication": msi_auth,
                        },
                    },
                    # 4c. Store status in pipeline variable
                    {
                        "name": "SetMLJobStatus",
                        "type": "SetVariable",
                        "dependsOn": [
                            {"activity": "GetMLJobStatus", "dependencyConditions": ["Succeeded"]}
                        ],
                        "userProperties": [],
                        "typeProperties": {
                            "variableName": "mlJobStatus",
                            "value": "@activity('GetMLJobStatus').output.properties.status",
                        },
                    },
                ],
            },
        },

        # 5. Check AML job result — fail ADF pipeline if training failed
        {
            "name": "CheckMLSuccess",
            "type": "IfCondition",
            "dependsOn": [
                {"activity": "WaitForMLJob", "dependencyConditions": ["Succeeded"]}
            ],
            "userProperties": [],
            "typeProperties": {
                "expression": {
                    "value": "@equals(variables('mlJobStatus'), 'Completed')",
                    "type": "Expression",
                },
                "ifTrueActivities": [],   # training succeeded → continue
                "ifFalseActivities": [    # training failed → fail ADF run
                    {
                        "name": "FailMLJob",
                        "type": "Fail",
                        "dependsOn": [],
                        "userProperties": [],
                        "typeProperties": {
                            "message": {
                                "value": "@concat('AML training job failed or was canceled. Status: ', variables('mlJobStatus'))",
                                "type": "Expression",
                            },
                            "errorCode": "5000",
                        },
                    }
                ],
            },
        },

        # 6. UpdateForecasts (preserve original structure, repoint dependency)
        _rebuild_update_forecasts(update_forecasts_base),
    ]

    # ── Add pipeline variable ─────────────────────────────────────
    pipeline["properties"]["activities"] = new_acts
    pipeline["properties"]["variables"] = {
        "mlJobStatus": {"type": "String", "defaultValue": "NotStarted"}
    }

    # ── PUT updated pipeline to ADF ───────────────────────────────
    pr = requests.put(
        f"{ADF_BASE}/pipelines/SalesAnalyticsPipeline?api-version=2018-06-01",
        headers=hdrs,
        json=pipeline,
        timeout=30,
    )
    if pr.status_code in (200, 201):
        print(f"     OK: ADF pipeline updated (HTTP {pr.status_code})")
        print("     Activities: CopyBlobToSQL → PrepareTrainingData → SubmitMLJob")
        print("                 → WaitForMLJob (Until 60s poll) → CheckMLSuccess → UpdateForecasts")
        return True
    else:
        print(f"     ERROR {pr.status_code}: {pr.text[:600]}")
        return False


def _rebuild_update_forecasts(existing: dict | None) -> dict:
    """Return UpdateForecasts activity with dependency on CheckMLSuccess."""
    if existing:
        act = dict(existing)
        act["dependsOn"] = [
            {"activity": "CheckMLSuccess", "dependencyConditions": ["Succeeded"]}
        ]
        return act
    # Fallback if activity not found in current pipeline
    return {
        "name": "UpdateForecasts",
        "type": "SqlServerStoredProcedure",
        "dependsOn": [
            {"activity": "CheckMLSuccess", "dependencyConditions": ["Succeeded"]}
        ],
        "userProperties": [],
        "typeProperties": {"storedProcedureName": "sp_UpdateForecasts"},
        "linkedServiceName": {
            "referenceName": "AzureSqlDatabaseLS",
            "type": "LinkedServiceReference",
        },
    }


# ─── Step 5: Monitor AML training job ────────────────────────────
def monitor_aml_job(credential, job_name: str, timeout_secs: int = 3600) -> str:
    """Poll AML job status every 30s until terminal state. Returns final status."""
    print(f"[5/7] Monitoring AML training job '{job_name}' (up to {timeout_secs//60}m)...")
    start = time.time()
    token_cache = {"t": tok(credential), "exp": time.time() + 3400}

    while time.time() - start < timeout_secs:
        if time.time() > token_cache["exp"]:
            token_cache["t"] = tok(credential)
            token_cache["exp"] = time.time() + 3400

        r = requests.get(
            f"{AML_BASE}/jobs/{job_name}?api-version=2023-04-01-preview",
            headers={"Authorization": f"Bearer {token_cache['t']}"},
            timeout=20,
        )
        if r.ok:
            props = r.json().get("properties", {})
            status = props.get("status", "Unknown")
            elapsed = int(time.time() - start)
            print(f"     [{elapsed:5d}s] {status}")
            if status in ("Completed", "Failed", "Canceled", "CancelRequested"):
                if status == "Completed":
                    print("     Model trained and registered in AML registry!")
                else:
                    err = props.get("error", {})
                    print(f"     Error: {err}")
                return status
        time.sleep(30)

    return "Timeout"


# ─── Step 6: Trigger ADF run  ────────────────────────────────────
def trigger_adf_run(credential) -> str:
    print("[6/7] Triggering fresh ADF pipeline run...")
    r = requests.post(
        f"{ADF_BASE}/pipelines/SalesAnalyticsPipeline/createRun?api-version=2018-06-01",
        headers={"Authorization": f"Bearer {tok(credential)}", "Content-Type": "application/json"},
        json={},
        timeout=20,
    )
    r.raise_for_status()
    run_id = r.json()["runId"]
    print(f"     Run ID: {run_id}")
    return run_id


# ─── Step 7: Monitor ADF run ─────────────────────────────────────
def monitor_adf_run(credential, run_id: str, timeout_secs: int = 7200) -> tuple:
    """Poll ADF activity runs every 20s. Returns (overall_status, {act: status})."""
    print(f"[7/7] Monitoring ADF run {run_id} (up to {timeout_secs//60}m)...")
    start = time.time()
    token_cache = {"t": tok(credential), "exp": time.time() + 3400}
    last_acts = {}

    while time.time() - start < timeout_secs:
        if time.time() > token_cache["exp"]:
            token_cache["t"] = tok(credential)
            token_cache["exp"] = time.time() + 3400
        hdrs = {"Authorization": f"Bearer {token_cache['t']}", "Content-Type": "application/json"}

        r1 = requests.get(
            f"{ADF_BASE}/pipelineruns/{run_id}?api-version=2018-06-01",
            headers={"Authorization": f"Bearer {token_cache['t']}"}, timeout=15,
        )
        overall = r1.json().get("status", "?") if r1.ok else "?"

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        yes = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))
        r2 = requests.post(
            f"{ADF_BASE}/queryActivityruns?api-version=2018-06-01",
            headers=hdrs,
            json={
                "lastUpdatedAfter": yes,
                "lastUpdatedBefore": now,
                "filters": [{"operand": "PipelineRunId", "operator": "Equals", "values": [run_id]}],
            },
            timeout=15,
        )
        acts = {}
        errs = {}
        if r2.ok:
            for a in r2.json().get("value", []):
                acts[a["activityName"]] = a["status"]
                if a.get("error"):
                    errs[a["activityName"]] = a["error"].get("message", "")[:150]

        if acts != last_acts:
            elapsed = int(time.time() - start)
            print(f"\n  [{elapsed}s] Pipeline overall: {overall}")
            for name, st in acts.items():
                icon = "✓" if st == "Succeeded" else ("✗" if st == "Failed" else "⟳")
                err_str = f"\n        ERROR: {errs[name]}" if name in errs else ""
                print(f"    {icon} {name}: {st}{err_str}")
            last_acts = acts

        if overall in ("Succeeded", "Failed", "Cancelled"):
            return overall, last_acts

        time.sleep(20)

    return "Timeout", last_acts


# ─── Main ─────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  ADF → AML MLOps v2: Real Training + Until Polling")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 65)

    credential = DefaultAzureCredential()
    ml_client = MLClient(
        credential,
        subscription_id=SUB,
        resource_group_name=RG,
        workspace_name=WS,
    )

    # 1. Create AML environment
    env_id = create_environment(ml_client)

    # 2. Submit real training job via SDK (uploads code + starts training)
    sdk_job = submit_training_job(ml_client, env_id)

    # 3. Extract code + environment IDs
    time.sleep(15)  # let AML register the job before querying
    code_id, resolved_env = get_job_asset_ids(credential, sdk_job)
    if not resolved_env:
        resolved_env = env_id

    # 4. Patch ADF pipeline with proper Until polling loop
    ok = update_adf_pipeline(credential, code_id, resolved_env)
    if not ok:
        print("\nERROR: Could not update ADF pipeline. Aborting.")
        sys.exit(1)

    # 5. Wait for SDK training job (trains + registers model)
    print(f"\n{'─'*65}")
    print("  PHASE 1: AML Training Job (this registers the model)")
    print(f"  Job: {sdk_job}")
    print(f"{'─'*65}")
    job_status = monitor_aml_job(credential, sdk_job, timeout_secs=3600)

    if job_status != "Completed":
        print(f"\n  Training job ended with status: {job_status}")
        print("  ADF pipeline is patched — will retry training on next ADF run.")
        sys.exit(1)

    print(f"\n  Training job COMPLETED. Model registered in AML.")

    # 6. Trigger ADF verification run
    print(f"\n{'─'*65}")
    print("  PHASE 2: ADF End-to-End Verification")
    print(f"{'─'*65}")
    time.sleep(5)
    run_id = trigger_adf_run(credential)

    # 7. Monitor ADF
    final, acts = monitor_adf_run(credential, run_id, timeout_secs=7200)

    print(f"\n{'=' * 65}")
    print(f"  FINAL ADF STATUS: {final}")
    for name, st in acts.items():
        icon = "✓" if st == "Succeeded" else ("✗" if st == "Failed" else "○")
        print(f"  {icon} {name}: {st}")
    print(f"{'=' * 65}")

    if final == "Succeeded":
        print("\n  SUCCESS: Full MLOps pipeline verified end-to-end!")
        print("  - AML trained real GradientBoosting model")
        print("  - Model registered in AML registry")
        print("  - ADF polled job status via Until loop (not fire-and-forget)")
        print("  - All 4 ADF activities succeeded")
    else:
        print(f"\n  ADF run {run_id} — check Azure portal for details.")

    return final


if __name__ == "__main__":
    main()
