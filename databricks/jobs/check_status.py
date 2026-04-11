#!/usr/bin/env python3
"""Check Databricks pipeline + workspace + Azure ML status."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from dotenv import load_dotenv
load_dotenv()

from databricks.sdk import WorkspaceClient

client = WorkspaceClient(
    host=os.getenv('DATABRICKS_HOST'),
    token=os.getenv('DATABRICKS_TOKEN')
)

me = client.current_user.me()
print(f"Connected as: {me.user_name}")

# Check notebooks uploaded
print("\n=== Workspace Notebooks ===")
try:
    items = list(client.workspace.list('/Shared/azure-realtime-sales-analytics/notebooks'))
    print(f"Found {len(items)} notebooks:")
    for item in items:
        print(f"  {item.path}")
except Exception as e:
    print(f"  ERROR: {e}")

# Check job
print("\n=== Jobs ===")
jobs = list(client.jobs.list(name='Sales_Lakehouse_Pipeline'))
if jobs:
    job = jobs[0]
    print(f"Job: {job.settings.name} (ID={job.job_id})")

    # Check tasks + source
    for task in (job.settings.tasks or []):
        nb = task.notebook_task
        if nb:
            print(f"  Task '{task.task_key}': source={nb.source}, path={nb.notebook_path}")

    # Latest runs
    print("\n  Recent runs:")
    runs = list(client.jobs.list_runs(job_id=job.job_id, limit=5))
    for run in runs:
        st = run.state
        lc = st.life_cycle_state
        rs = st.result_state
        print(f"    Run {run.run_id}: {lc} / {rs}")
else:
    print("  Job 'Sales_Lakehouse_Pipeline' not found!")
    print("  Available jobs:")
    for j in client.jobs.list():
        print(f"    {j.settings.name} (ID={j.job_id})")

# Check clusters (Databricks side)
print("\n=== Clusters ===")
try:
    clusters = list(client.clusters.list())
    if clusters:
        for c in clusters[:5]:
            print(f"  {c.cluster_name}: {c.state}")
    else:
        print("  No running clusters (job clusters are ephemeral, OK)")
except Exception as e:
    print(f"  {e}")
