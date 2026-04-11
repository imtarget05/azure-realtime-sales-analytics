"""
Patch ADF pipeline with fixed expression + trigger run.
Reuses already-uploaded code/env from job brave_stem_d79l7q73pl.
"""
import os, sys, time, requests
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
os.environ["KEY_VAULT_URI"] = "DISABLED"

from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
load_dotenv()

# Import helpers from v2 script
import importlib.util, types

spec = importlib.util.spec_from_file_location(
    "v2",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "fix_adf_mlops_v2.py")
)
v2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v2)

cred = DefaultAzureCredential()

CODE_ID = (
    "/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev"
    "/providers/Microsoft.MachineLearningServices/workspaces/aml-sales-analytics-d9bt2m2"
    "/codes/715ed8df-1366-456a-9fe3-f335fb9eb9f5/versions/1"
)
ENV_ID = (
    "/subscriptions/34849ef9-3814-44df-ba32-a86ed9f2a69a/resourceGroups/rg-sales-analytics-dev"
    "/providers/Microsoft.MachineLearningServices/workspaces/aml-sales-analytics-d9bt2m2"
    "/environments/sales-training-env/versions/4"
)

print("=" * 60)
print("  Re-patch ADF pipeline (fixed expression) + trigger run")
print("=" * 60)

ok = v2.update_adf_pipeline(cred, CODE_ID, ENV_ID)
if not ok:
    print("Failed to update pipeline")
    sys.exit(1)

time.sleep(3)
run_id = v2.trigger_adf_run(cred)
final, acts = v2.monitor_adf_run(cred, run_id, timeout_secs=7200)

print(f"\n{'=' * 60}")
print(f"  ADF Final: {final}")
for name, st in acts.items():
    icon = "ok" if st == "Succeeded" else ("FAIL" if st == "Failed" else "...")
    print(f"  [{icon}] {name}: {st}")
print("=" * 60)
