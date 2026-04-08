#!/usr/bin/env python3
"""
Deploy Databricks Job từ job_trigger.json.

Sử dụng:
    # Tạo job mới
    python deploy_job.py --create

    # Cập nhật job đã tồn tại
    python deploy_job.py --update --job-id 123456

    # Chạy job ngay (manual trigger)
    python deploy_job.py --run-now --job-id 123456

Yêu cầu:
    pip install databricks-sdk
    Đặt biến môi trường:
        DATABRICKS_HOST=https://adb-xxxxx.azuredatabricks.net
        DATABRICKS_TOKEN=dapi-xxxxx
"""

import argparse
import json
import os
import sys

from databricks.sdk import WorkspaceClient


def load_job_config(path: str) -> dict:
    """Load job config từ file JSON."""
    with open(path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Xóa các field comment/private
    for task in config.get("tasks", []):
        for lib in task.get("libraries", []):
            lib.pop("_comment", None)

        # For git_source jobs, Databricks expects repo-relative notebook paths.
        notebook_task = task.get("notebook_task", {})
        notebook_path = notebook_task.get("notebook_path", "")
        if notebook_path.startswith("/"):
            notebook_task["notebook_path"] = notebook_path.lstrip("/")

    config.pop("continuous", None)  # Chỉ dùng schedule

    # Remove placeholders that commonly fail validation in student/demo environments.
    webhook_notifications = config.get("webhook_notifications")
    if webhook_notifications:
        on_failure = webhook_notifications.get("on_failure", [])
        cleaned = [item for item in on_failure if item.get("id") and "placeholder" not in item.get("id", "") and "webhook-id" not in item.get("id", "")]
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

    return config


def create_job(client: WorkspaceClient, config: dict) -> int:
    """Tạo Databricks Job mới."""
    response = client.api_client.do("POST", "/api/2.1/jobs/create", body=config)
    job_id = int(response.get("job_id"))
    print(f"✓ Job created: ID={job_id}, Name={config['name']}")
    print(f"  Schedule: {config['schedule']['quartz_cron_expression']}")
    return job_id


def update_job(client: WorkspaceClient, job_id: int, config: dict):
    """Cập nhật job đã tồn tại."""
    client.api_client.do(
        "POST",
        "/api/2.1/jobs/reset",
        body={"job_id": job_id, "new_settings": config},
    )
    print(f"✓ Job updated: ID={job_id}")


def run_now(client: WorkspaceClient, job_id: int):
    """Chạy job ngay lập tức (manual trigger)."""
    run = client.jobs.run_now(job_id=job_id)
    print(f"✓ Job triggered: Run ID={run.run_id}")
    print(f"  Monitor: {client.config.host}/#job/{job_id}/run/{run.run_id}")


def main():
    parser = argparse.ArgumentParser(description="Deploy Databricks Job")
    parser.add_argument("--create", action="store_true", help="Tạo job mới")
    parser.add_argument("--update", action="store_true", help="Cập nhật job")
    parser.add_argument("--run-now", action="store_true", help="Chạy job ngay")
    parser.add_argument("--job-id", type=int, help="Job ID (cho update/run)")
    parser.add_argument(
        "--config",
        default=os.path.join(os.path.dirname(__file__), "job_trigger.json"),
        help="Path tới file job config JSON",
    )
    args = parser.parse_args()

    if not any([args.create, args.update, args.run_now]):
        parser.print_help()
        sys.exit(1)

    client = WorkspaceClient()
    config = load_job_config(args.config)

    if args.create:
        create_job(client, config)
    elif args.update:
        if not args.job_id:
            print("ERROR: --job-id required for --update")
            sys.exit(1)
        update_job(client, args.job_id, config)
    elif args.run_now:
        if not args.job_id:
            print("ERROR: --job-id required for --run-now")
            sys.exit(1)
        run_now(client, args.job_id)


if __name__ == "__main__":
    main()
