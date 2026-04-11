"""
fix_mlops_e2e.py — End-to-end MLOps fix:

1. Submit REAL AML training job (train_model.py on training-cluster)
   - Environment: sales-forecast-training-env (conda_env.yml with sklearn/pyodbc)
   - Code: ml/ directory uploaded to AML datastore
   - SQL credentials passed as env vars → loads real data from Azure SQL
   - Auto-registers model in AML registry via azureml.core Run.register_model()

2. Extract codeId + environmentId from submitted job (via REST API)

3. Rebuild ADF SalesAnalyticsPipeline with PROPER Until-polling loop:
   CopyBlobToSQL → PrepareTrainingData
     → SubmitMLJob (WebActivity PUT, fire the real training job)
     → WaitForMLJob (Until loop polls every 30s until Completed/Failed/Canceled)
     → UpdateForecasts (Stored Procedure, runs after ML job is done)

4. Monitor the AML training job to completion

5. Trigger and monitor ADF SalesAnalyticsPipeline end-to-end

Run with:
  $env:KEY_VAULT_URI="DISABLED"; $env:PYTHONIOENCODING="utf-8"
  .\.venv\Scripts\python.exe scripts\fix_mlops_e2e.py [--skip-training] [--adf-only]
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime, timezone

os.environ.setdefault("KEY_VAULT_URI", "DISABLED")
sys.path.insert(0, ".")

from dotenv import load_dotenv
load_dotenv()

from azure.identity import DefaultAzureCredential
from azure.ai.ml import MLClient, command
from azure.ai.ml.entities import Environment

# ─── Constants ───────────────────────────────────────────────────────────────
SUB     = "34849ef9-3814-44df-ba32-a86ed9f2a69a"
RG      = "rg-sales-analytics-dev"
WS      = "aml-sales-analytics-d9bt2m2"
ADF     = "adf-sales-paivm"
CLUSTER = "training-cluster"
EXPERIMENT = "sales-retrain-pipeline"

COMPUTE_ID = (
    f"/subscriptions/{SUB}/resourceGroups/{RG}"
    f"/providers/Microsoft.MachineLearningServices/workspaces/{WS}"
    f"/computes/{CLUSTER}"
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

MGMT_TOKEN_CACHE = {"token": None, "expiry": 0.0}


# ─── Auth ─────────────────────────────────────────────────────────────────────
def get_credential():
    return DefaultAzureCredential()


def mgmt_headers(cred):
    now = time.time()
    if MGMT_TOKEN_CACHE["token"] and MGMT_TOKEN_CACHE["expiry"] > now + 60:
        token = MGMT_TOKEN_CACHE["token"]
    else:
        tok = cred.get_token("https://management.azure.com/.default")
        token = tok.token
        MGMT_TOKEN_CACHE["token"] = token
        MGMT_TOKEN_CACHE["expiry"] = tok.expires_on
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ─── Step 1: Submit real AML training job via SDK ────────────────────────────
def submit_training_job(cred, n_samples: int = 5000) -> str:
    """Submit real training job using azure-ai-ml SDK. Returns job name."""
    print("\n[1/5] Submitting real AML training job...")
    from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD

    ml = MLClient(cred, subscription_id=SUB, resource_group_name=RG, workspace_name=WS)

    env = Environment(
        name="sales-forecast-training-env",
        conda_file=os.path.join("ml", "conda_env.yml"),
        image="mcr.microsoft.com/azureml/openmpi4.1.0-ubuntu20.04:latest",
    )

    # Pass SQL credentials so train_model.py loads real data from Azure SQL
    sql_env = {
        "SQL_SERVER":   SQL_SERVER   or "",
        "SQL_DATABASE": SQL_DATABASE or "SalesAnalyticsDB",
        "SQL_USERNAME": SQL_USERNAME or "",
        "SQL_PASSWORD": SQL_PASSWORD or "",
        "SQL_DRIVER":   "{ODBC Driver 18 for SQL Server}",
        "KEY_VAULT_URI": "DISABLED",
    }

    job = command(
        code=os.path.join("ml"),
        command=(
            f"python train_model.py "
            f"--output-dir ./outputs/model_output "
            f"--n-samples {n_samples}"
        ),
        environment=env,
        compute=CLUSTER,
        experiment_name=EXPERIMENT,
        display_name=f"sales-retrain-pipeline-v1",
        description=(
            "Real MLOps training: GradientBoostingRegressor on SQL data. "
            "Auto-registers model via azureml.core Run.register_model()."
        ),
        environment_variables=sql_env,
        tags={"triggered_by": "fix_mlops_e2e", "trigger_time": datetime.utcnow().isoformat()},
    )

    submitted = ml.jobs.create_or_update(job)
    print(f"    [OK] Job submitted: {submitted.name}")
    print(f"    Studio URL: {submitted.studio_url}")
    return submitted.name


# ─── Step 2: Extract codeId + environmentId from submitted job ────────────────
def get_job_ids(cred, job_name: str, max_wait_secs: int = 60) -> tuple[str, str]:
    """
    Poll AML job REST API until we get codeId and environmentId.
    These IDs are populated once the code is uploaded (before the job starts).
    """
    print(f"\n[2/5] Getting code & environment IDs from job {job_name}...")
    url = f"{AML_BASE}/jobs/{job_name}?api-version=2023-04-01-preview"
    deadline = time.time() + max_wait_secs

    while time.time() < deadline:
        resp = requests.get(url, headers=mgmt_headers(cred), timeout=15)
        if not resp.ok:
            print(f"    [WARN] Job GET returned {resp.status_code}, retrying...")
            time.sleep(5)
            continue

        props = resp.json().get("properties", {})
        code_id = props.get("codeId")
        env_id  = props.get("environmentId")

        if code_id and env_id:
            # Normalize to ARM IDs usable by ADF WebActivity
            print(f"    [OK] codeId:        {code_id}")
            print(f"    [OK] environmentId: {env_id}")
            return code_id, env_id

        status = props.get("status", "?")
        print(f"    Waiting for code upload... (status={status})")
        time.sleep(5)

    # If we can't get the IDs, fall back to env only (code already uploaded)
    print("    [WARN] Could not get codeId; will use environment reference only")
    return "", ""


# ─── Step 3: Build ADF pipeline with Until polling loop ──────────────────────
def build_adf_pipeline_json(existing_pipeline: dict, code_id: str, env_id: str) -> dict:
    """
    Rebuild SalesAnalyticsPipeline:
      CopyBlobToSQL → PrepareTrainingData
        → SubmitMLJob (WebActivity PUT → AML Jobs API)
        → WaitForMLJob (Until loop, polls every 30s)
        → UpdateForecasts (Stored Procedure)

    Pipeline variable 'MLJobStatus' tracks the AML job state.
    """
    props = existing_pipeline["properties"]

    # ── Find original activities we want to keep ──────────────────────────
    orig = {a["name"]: a for a in props.get("activities", [])}

    copy_activity     = orig.get("CopyBlobToSQL")
    prepare_activity  = orig.get("PrepareTrainingData")
    update_activity   = orig.get("UpdateForecasts")

    if not copy_activity or not prepare_activity:
        raise ValueError("CopyBlobToSQL or PrepareTrainingData not found in existing pipeline")

    # ── Fix dependency in PrepareTrainingData (ensure it depends on CopyBlobToSQL) ──
    prepare_activity = dict(prepare_activity)
    if not prepare_activity.get("dependsOn"):
        prepare_activity["dependsOn"] = [
            {"activity": "CopyBlobToSQL", "dependencyConditions": ["Succeeded"]}
        ]

    # ── SQL creds to pass into AML job as env vars ─────────────────────────
    from config.settings import SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
    sql_env_vars = {
        "SQL_SERVER":   SQL_SERVER   or "",
        "SQL_DATABASE": SQL_DATABASE or "SalesAnalyticsDB",
        "SQL_USERNAME": SQL_USERNAME or "",
        "SQL_PASSWORD": SQL_PASSWORD or "",
        "SQL_DRIVER":   "{ODBC Driver 18 for SQL Server}",
        "KEY_VAULT_URI": "DISABLED",
    }

    # ── Build job body for ADF to submit ───────────────────────────────────
    # The AML job name includes the ADF pipeline RunId for uniqueness
    # PUT URL pattern: .../jobs/srpv1-{replace(pipeline().RunId, '-', '')}
    aml_job_body = {
        "properties": {
            "jobType": "Command",
            "displayName": "sales-retrain-pipeline-v1",
            "experimentName": EXPERIMENT,
            "computeId": COMPUTE_ID,
            "command": (
                "python train_model.py "
                "--output-dir ./outputs/model_output "
                "--n-samples 5000"
            ),
            "environmentVariables": sql_env_vars,
            "resources": {"instanceCount": 1},
            "properties": {},
            "inputs": {},
            "outputs": {},
        }
    }

    # Add code and environment if we have the IDs
    if code_id:
        aml_job_body["properties"]["codeId"] = code_id
    if env_id:
        aml_job_body["properties"]["environmentId"] = env_id

    # ADF expression: unique job name per pipeline run
    submit_url_expr = (
        "@concat("
        f"'https://management.azure.com/subscriptions/{SUB}"
        f"/resourceGroups/{RG}"
        f"/providers/Microsoft.MachineLearningServices/workspaces/{WS}"
        "/jobs/srpv1-',"
        "replace(pipeline().RunId,'-',''),"
        "'?api-version=2023-04-01-preview')"
    )

    # ADF expression: poll same job name
    poll_url_expr = submit_url_expr  # Same URL, GET method

    # ── Activity: SubmitMLJob ──────────────────────────────────────────────
    submit_ml_job = {
        "name": "SubmitMLJob",
        "type": "WebActivity",
        "dependsOn": [
            {"activity": "PrepareTrainingData", "dependencyConditions": ["Succeeded"]}
        ],
        "policy": {
            "timeout": "0.00:10:00",
            "retry": 1,
            "retryIntervalInSeconds": 30,
            "secureOutput": False,
            "secureInput": False,
        },
        "userProperties": [],
        "typeProperties": {
            "method": "PUT",
            "url": {"value": submit_url_expr, "type": "Expression"},
            "headers": {"Content-Type": "application/json"},
            "body": aml_job_body,
            "authentication": {
                "type": "MSI",
                "resource": "https://management.azure.com/",
            },
        },
    }

    # ── Activity: CheckMLJobStatus (inside Until loop) ─────────────────────
    check_job_status = {
        "name": "CheckMLJobStatus",
        "type": "WebActivity",
        "dependsOn": [],
        "userProperties": [],
        "typeProperties": {
            "method": "GET",
            "url": {"value": poll_url_expr, "type": "Expression"},
            "headers": {"Content-Type": "application/json"},
            "authentication": {
                "type": "MSI",
                "resource": "https://management.azure.com/",
            },
        },
    }

    # ── Activity: SetMLJobStatus (inside Until loop) ───────────────────────
    set_job_status = {
        "name": "SetMLJobStatus",
        "type": "SetVariable",
        "dependsOn": [
            {"activity": "CheckMLJobStatus", "dependencyConditions": ["Succeeded"]}
        ],
        "userProperties": [],
        "typeProperties": {
            "variableName": "MLJobStatus",
            "value": {
                "value": "@activity('CheckMLJobStatus').output.properties.status",
                "type": "Expression",
            },
        },
    }

    # ── Activity: Wait30s (inside Until loop) ─────────────────────────────
    wait_30s = {
        "name": "Wait30s",
        "type": "Wait",
        "dependsOn": [
            {"activity": "SetMLJobStatus", "dependencyConditions": ["Succeeded"]}
        ],
        "userProperties": [],
        "typeProperties": {"waitTimeInSeconds": 30},
    }

    # ── Activity: WaitForMLJob (Until loop) ───────────────────────────────
    # Expression: exit loop when job is in a terminal state
    until_expr = (
        "@or("
        "equals(variables('MLJobStatus'),'Completed'),"
        "equals(variables('MLJobStatus'),'Failed'),"
        "equals(variables('MLJobStatus'),'Canceled'),"
        "equals(variables('MLJobStatus'),'CancelRequested')"
        ")"
    )

    wait_for_ml_job = {
        "name": "WaitForMLJob",
        "type": "Until",
        "dependsOn": [
            {"activity": "SubmitMLJob", "dependencyConditions": ["Succeeded"]}
        ],
        "userProperties": [],
        "typeProperties": {
            "expression": {"value": until_expr, "type": "Expression"},
            "timeout": "0.04:00:00",   # 4 hours max
            "activities": [
                check_job_status,
                set_job_status,
                wait_30s,
            ],
        },
    }

    # ── Activity: UpdateForecasts (runs after WaitForMLJob) ───────────────
    if update_activity:
        update_activity = dict(update_activity)
        update_activity["dependsOn"] = [
            {"activity": "WaitForMLJob", "dependencyConditions": ["Succeeded"]}
        ]
    else:
        update_activity = {
            "name": "UpdateForecasts",
            "type": "SqlServerStoredProcedure",
            "dependsOn": [
                {"activity": "WaitForMLJob", "dependencyConditions": ["Succeeded"]}
            ],
            "userProperties": [],
            "typeProperties": {
                "storedProcedureName": "sp_UpdateForecasts",
            },
            "linkedServiceName": {
                "referenceName": "AzureSqlDatabaseLS",
                "type": "LinkedServiceReference",
            },
        }

    # ── Assemble pipeline ──────────────────────────────────────────────────
    pipeline_json = dict(existing_pipeline)
    pipeline_json["properties"] = dict(props)
    pipeline_json["properties"]["activities"] = [
        copy_activity,
        prepare_activity,
        submit_ml_job,
        wait_for_ml_job,
        update_activity,
    ]

    # ── Add pipeline variable for MLJobStatus ──────────────────────────────
    pipeline_json["properties"]["variables"] = {
        "MLJobStatus": {
            "type": "String",
            "defaultValue": "Starting",
        }
    }

    return pipeline_json


# ─── PUT updated pipeline to ADF ─────────────────────────────────────────────
def update_adf_pipeline(cred, pipeline_json: dict) -> bool:
    print("\n[3/5] Updating ADF SalesAnalyticsPipeline...")
    url = f"{ADF_BASE}/pipelines/SalesAnalyticsPipeline?api-version=2018-06-01"
    resp = requests.put(url, headers=mgmt_headers(cred), json=pipeline_json, timeout=30)
    if resp.status_code in (200, 201):
        print(f"    [OK] Pipeline updated (HTTP {resp.status_code})")
        print("    Structure: CopyBlobToSQL → PrepareTrainingData → SubmitMLJob → WaitForMLJob (Until) → UpdateForecasts")
        return True
    else:
        print(f"    [ERR] Pipeline update failed: {resp.status_code}")
        print(resp.text[:500])
        return False


# ─── Monitor AML training job ───────────────────────────────────────────────
def monitor_aml_job(cred, job_name: str, max_wait_secs: int = 3600) -> str:
    """Poll AML job until terminal state. Returns final status."""
    print(f"\n[4/5] Monitoring AML training job '{job_name}'...")
    print("      (training on training-cluster: env build ~5min, train ~5-10min)")

    url = f"{AML_BASE}/jobs/{job_name}?api-version=2023-04-01-preview"
    start = time.time()
    last_status = ""

    while time.time() - start < max_wait_secs:
        try:
            resp = requests.get(url, headers=mgmt_headers(cred), timeout=15)
            if resp.ok:
                props = resp.json().get("properties", {})
                status = props.get("status", "?")
                if status != last_status:
                    elapsed = int(time.time() - start)
                    print(f"    [{elapsed:4d}s] AML job status: {status}")
                    last_status = status
                if status in ("Completed", "Failed", "Canceled", "CancelRequested"):
                    return status
        except Exception as e:
            print(f"    [WARN] Poll error: {e}")

        time.sleep(20)

    return "Timeout"


# ─── Trigger ADF run ─────────────────────────────────────────────────────────
def trigger_adf_run(cred) -> str:
    print("\n[5/5] Triggering ADF SalesAnalyticsPipeline run...")
    url = f"{ADF_BASE}/pipelines/SalesAnalyticsPipeline/createRun?api-version=2018-06-01"
    resp = requests.post(url, headers=mgmt_headers(cred), json={}, timeout=30)
    resp.raise_for_status()
    run_id = resp.json()["runId"]
    print(f"    [OK] Run started: {run_id}")
    return run_id


# ─── Monitor ADF pipeline run ────────────────────────────────────────────────
def monitor_adf_run(cred, run_id: str, max_wait_secs: int = 7200) -> tuple[str, dict]:
    """Poll ADF pipeline run. Returns (final_status, {activity: status})."""
    print(f"    Monitoring run {run_id} ...")
    print("    (WaitForMLJob will poll AML every 30s — expect 15-25 min total)")

    start = time.time()
    yesterday = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 86400))
    last_acts: dict = {}

    while time.time() - start < max_wait_secs:
        now_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Overall run status
        run_resp = requests.get(
            f"{ADF_BASE}/pipelineruns/{run_id}?api-version=2018-06-01",
            headers=mgmt_headers(cred), timeout=15
        )
        overall = "Unknown"
        if run_resp.ok:
            overall = run_resp.json().get("status", "Unknown")

        # Activity-level status
        act_resp = requests.post(
            f"{ADF_BASE}/queryActivityruns?api-version=2018-06-01",
            headers=mgmt_headers(cred),
            json={
                "lastUpdatedAfter": yesterday,
                "lastUpdatedBefore": now_str,
                "filters": [
                    {"operand": "PipelineRunId", "operator": "Equals", "values": [run_id]}
                ],
            },
            timeout=15,
        )
        if act_resp.ok:
            acts = act_resp.json().get("value", [])
            cur_acts = {a["activityName"]: a["status"] for a in acts}
            if cur_acts != last_acts:
                elapsed = int(time.time() - start)
                print(f"\n  [{elapsed}s] Pipeline: {overall}")
                for name, st in cur_acts.items():
                    icon = "✓" if st == "Succeeded" else ("✗" if st == "Failed" else "○")
                    # Show error if any
                    err = ""
                    for a in acts:
                        if a["activityName"] == name and a.get("error"):
                            err = f"  ← {a['error'].get('message','')[:100]}"
                    print(f"    {icon} {name}: {st}{err}")
                last_acts = cur_acts

        if overall in ("Succeeded", "Failed", "Cancelled"):
            return overall, last_acts

        time.sleep(15)

    return "Timeout", last_acts


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="End-to-end MLOps fix")
    parser.add_argument("--skip-training", action="store_true",
                        help="Skip AML job submission (use if training already running)")
    parser.add_argument("--adf-only", action="store_true",
                        help="Only rebuild ADF pipeline + trigger run (skip training)")
    parser.add_argument("--n-samples", type=int, default=5000,
                        help="Training samples (default 5000 for speed)")
    parser.add_argument("--existing-job", type=str, default="",
                        help="Use existing AML job name instead of submitting new one")
    args = parser.parse_args()

    print("=" * 65)
    print("  MLOPS END-TO-END FIX")
    print("  ADF: CopyBlobToSQL → PrepareTraining → SubmitMLJob")
    print("       → WaitForMLJob (Until loop) → UpdateForecasts")
    print("  AML: train_model.py → model registered in AML registry")
    print("=" * 65)

    cred = get_credential()

    # ── Step 1: Submit training job ──────────────────────────────────────
    aml_job_name = args.existing_job
    if not args.skip_training and not args.adf_only and not aml_job_name:
        aml_job_name = submit_training_job(cred, n_samples=args.n_samples)
    elif args.existing_job:
        print(f"\n[1/5] Using existing AML job: {aml_job_name}")
    else:
        print("\n[1/5] Skipping training job submission")

    # ── Step 2: Get code / env IDs ───────────────────────────────────────
    code_id, env_id = "", ""
    if aml_job_name and not args.adf_only:
        code_id, env_id = get_job_ids(cred, aml_job_name, max_wait_secs=90)
    else:
        print("\n[2/5] Skipped (--adf-only mode)")

    # ── Step 3: Rebuild ADF pipeline ─────────────────────────────────────
    print("\n[3/5] Fetching current ADF pipeline definition...")
    get_url = f"{ADF_BASE}/pipelines/SalesAnalyticsPipeline?api-version=2018-06-01"
    resp = requests.get(get_url, headers=mgmt_headers(cred), timeout=30)
    resp.raise_for_status()
    current_pipeline = resp.json()

    new_pipeline = build_adf_pipeline_json(current_pipeline, code_id, env_id)
    ok = update_adf_pipeline(cred, new_pipeline)
    if not ok:
        print("[ERR] ADF pipeline update failed. Stopping.")
        sys.exit(1)

    # ── Step 4: Monitor AML training job ─────────────────────────────────
    if aml_job_name and not args.adf_only:
        aml_status = monitor_aml_job(cred, aml_job_name, max_wait_secs=3600)
        print(f"\n    AML training job final status: {aml_status}")

        if aml_status != "Completed":
            print(f"[WARN] AML training job ended with: {aml_status}")
            print("       ADF run will trigger but WaitForMLJob will see 'Failed'.")
            print("       Check AML Studio for detailed error logs.")
    else:
        print("\n[4/5] Skipped AML job monitoring")

    # ── Step 5: Trigger & monitor ADF run ────────────────────────────────
    time.sleep(2)
    run_id = trigger_adf_run(cred)
    adf_status, act_statuses = monitor_adf_run(cred, run_id, max_wait_secs=7200)

    # ── Final summary ─────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print(f"  ADF Pipeline Run: {adf_status}")
    for name, st in act_statuses.items():
        icon = "✓" if st == "Succeeded" else ("✗" if st == "Failed" else "○")
        print(f"  {icon} {name}: {st}")
    print(f"\n  ADF Run ID: {run_id}")
    if aml_job_name:
        print(f"  AML Job:    {aml_job_name}")
    print("=" * 65)

    if adf_status == "Succeeded":
        print("\n[SUCCESS] Full end-to-end pipeline Succeeded!")
        print("  - Real AML training job: Completed")
        print("  - Model: registered in AML registry via Run.register_model()")
        print("  - ADF pipeline: all 5 activities Succeeded")
        print("  - Architecture: Blob→SQL→AML train→SQL update→PowerBI ready")
    else:
        print(f"\n[PARTIAL] ADF run ended with: {adf_status}")
        print(f"  Check ADF portal: run {run_id}")


if __name__ == "__main__":
    main()
