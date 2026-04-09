#!/usr/bin/env python3
"""
Upload Databricks notebooks lên Workspace và cập nhật Job về WORKSPACE source.

Giải quyết lỗi:
    "Unable to access the notebook ... in the repository.
     Either it does not exist, or the identity ... lacks the required permissions."

Nguyên nhân: git_source yêu cầu GitHub credentials trong Databricks User Settings.
Giải pháp này: Upload .py notebooks trực tiếp lên /Shared/ → không cần GitHub.

Yêu cầu:
    DATABRICKS_HOST và DATABRICKS_TOKEN trong .env
    pip install databricks-sdk

Cách lấy token:
    Databricks UI → User Settings (avatar trên cùng phải)
    → Developer → Access tokens → Generate new token

Sử dụng:
    # Upload notebooks + update job
    python databricks/jobs/upload_notebooks.py --upload --update-job --job-id <JOB_ID>

    # Chỉ upload notebooks
    python databricks/jobs/upload_notebooks.py --upload

    # Chỉ update job (sau khi đã upload)
    python databricks/jobs/upload_notebooks.py --update-job --job-id <JOB_ID>
"""

import argparse
import base64
import json
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

# ── Config ────────────────────────────────────────────────────────────────────
NOTEBOOKS_DIR = os.path.join(os.path.dirname(__file__), "..", "notebooks")
WORKSPACE_BASE = "/Shared/azure-realtime-sales-analytics/notebooks"

NOTEBOOK_FILES = [
    "00_config",
    "01_bronze_ingestion",
    "02_silver_etl",
    "03_feature_engineering",
    "04_ml_prediction",
    "05_gold_aggregation",
]


def upload_notebooks(client: WorkspaceClient) -> dict[str, str]:
    """Upload all notebooks to Databricks Workspace. Returns {name: workspace_path}."""
    # Ensure directory exists
    try:
        client.workspace.mkdirs(path=WORKSPACE_BASE)
        print(f"✓ Workspace directory: {WORKSPACE_BASE}")
    except Exception as e:
        print(f"  mkdirs warning (may already exist): {e}")

    uploaded = {}
    for name in NOTEBOOK_FILES:
        local_path = os.path.join(NOTEBOOKS_DIR, f"{name}.py")
        if not os.path.exists(local_path):
            print(f"⚠ Skipped (not found): {local_path}")
            continue

        with open(local_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Databricks workspace import requires base64-encoded content
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        workspace_path = f"{WORKSPACE_BASE}/{name}"

        client.api_client.do(
            "POST",
            "/api/2.0/workspace/import",
            body={
                "path": workspace_path,
                "format": "SOURCE",
                "language": "PYTHON",
                "content": content_b64,
                "overwrite": True,
            },
        )

        uploaded[name] = workspace_path
        print(f"✓ Uploaded: {name} → {workspace_path}")

    return uploaded


def update_job_to_workspace(client: WorkspaceClient, job_id: int, workspace_paths: dict[str, str]):
    """Update job tasks to use WORKSPACE source with uploaded paths."""
    config_path = os.path.join(os.path.dirname(__file__), "job_trigger.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Map task_key → notebook name
    task_notebook_map = {
        "bronze_ingestion": "01_bronze_ingestion",
        "silver_etl": "02_silver_etl",
        "feature_engineering": "03_feature_engineering",
        "ml_prediction": "04_ml_prediction",
        "gold_aggregation": "05_gold_aggregation",
    }

    # Remove git_source — use WORKSPACE instead
    config.pop("git_source", None)
    config.pop("continuous", None)

    for task in config.get("tasks", []):
        task_key = task.get("task_key")
        notebook_name = task_notebook_map.get(task_key)
        if notebook_name and notebook_name in workspace_paths:
            task["notebook_task"]["notebook_path"] = workspace_paths[notebook_name]
            task["notebook_task"]["source"] = "WORKSPACE"

    # Clean up comment fields
    for task in config.get("tasks", []):
        for lib in task.get("libraries", []):
            lib.pop("_comment", None)

    # Remove invalid notification placeholders
    webhook_notifications = config.get("webhook_notifications")
    if webhook_notifications:
        on_failure = webhook_notifications.get("on_failure", [])
        cleaned = [
            item for item in on_failure
            if item.get("id")
            and "placeholder" not in item.get("id", "")
            and "webhook-id" not in item.get("id", "")
        ]
        if cleaned:
            webhook_notifications["on_failure"] = cleaned
        else:
            config.pop("webhook_notifications", None)

    email_notifications = config.get("email_notifications")
    if email_notifications:
        on_failure = email_notifications.get("on_failure", [])
        cleaned_emails = [e for e in on_failure if "@" in e and "yourdomain" not in e]
        if cleaned_emails:
            email_notifications["on_failure"] = cleaned_emails
        else:
            config.pop("email_notifications", None)

    client.api_client.do(
        "POST",
        "/api/2.1/jobs/reset",
        body={"job_id": job_id, "new_settings": config},
    )
    print(f"✓ Job {job_id} updated to WORKSPACE source")
    print(f"  Notebooks at: {WORKSPACE_BASE}/")

    # Print updated task paths
    for task in config.get("tasks", []):
        path = task["notebook_task"]["notebook_path"]
        source = task["notebook_task"]["source"]
        print(f"  {task['task_key']:25s} → {source}: {path}")


def main():
    parser = argparse.ArgumentParser(description="Upload notebooks to Databricks Workspace")
    parser.add_argument("--upload", action="store_true", help="Upload notebooks to workspace")
    parser.add_argument("--update-job", action="store_true", help="Update job to WORKSPACE source")
    parser.add_argument("--job-id", type=int, help="Job ID to update")
    args = parser.parse_args()

    if not args.upload and not args.update_job:
        parser.print_help()
        sys.exit(1)

    # Validate env
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")
    if not host or not token:
        print("ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")
        print()
        print("Lấy token:")
        print("  Databricks UI → User Settings (avatar) → Developer → Access tokens → Generate new token")
        print("  Paste vào .env: DATABRICKS_TOKEN=dapi-xxxxxxxx")
        sys.exit(1)

    client = WorkspaceClient()

    workspace_paths = {name: f"{WORKSPACE_BASE}/{name}" for name in NOTEBOOK_FILES}

    if args.upload:
        print(f"\nUploading notebooks to: {WORKSPACE_BASE}")
        workspace_paths = upload_notebooks(client)
        print(f"\n✓ {len(workspace_paths)} notebooks uploaded")

    if args.update_job:
        if not args.job_id:
            # Try to find job by name
            jobs = list(client.jobs.list(name="Sales_Lakehouse_Pipeline"))
            if jobs:
                args.job_id = jobs[0].job_id
                print(f"Found job: Sales_Lakehouse_Pipeline (ID={args.job_id})")
            else:
                print("ERROR: --job-id required (could not auto-detect job)")
                sys.exit(1)

        print(f"\nUpdating job {args.job_id}...")
        update_job_to_workspace(client, args.job_id, workspace_paths)

    print("\nDone! Now trigger a manual run to verify.")


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    main()
