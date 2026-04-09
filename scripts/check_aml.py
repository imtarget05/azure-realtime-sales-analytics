#!/usr/bin/env python3
import os, sys, json
sys.path.insert(0, ".")
from dotenv import load_dotenv; load_dotenv()

aml_env = {
    "AML_WORKSPACE": os.getenv("AML_WORKSPACE_NAME") or os.getenv("AZURE_ML_WORKSPACE_NAME"),
    "AML_SUBSCRIPTION": os.getenv("AZURE_SUBSCRIPTION_ID"),
    "AML_RESOURCE_GROUP": os.getenv("AZURE_RESOURCE_GROUP"),
}
print("AML Config:")
for k, v in aml_env.items():
    print(f"  {k}: {v}")

reg_path = "ml/model_output/model_metadata.json"
with open(reg_path) as f:
    meta = json.load(f)

print("\n=== Local Trained Models ===")
print(f"Model version: {meta['model_version']}")
print(f"Trained at: {meta['trained_at']}")
print(f"Training samples: {meta['training_samples']:,}")
print(f"Algorithm: GradientBoosting (n_estimators=300)")
print(f"Revenue model: R2={meta['revenue_metrics']['r2_score']}, MAE={meta['revenue_metrics']['mae']}")
print(f"Quantity model: R2={meta['quantity_metrics']['r2_score']}, MAE={meta['quantity_metrics']['mae']}")

# Check pkl files
import glob
models = glob.glob("ml/model_output/*.pkl")
print("\nModel files:")
for m in models:
    sz = os.path.getsize(m)
    print(f"  {os.path.basename(m)}: {sz/1024:.0f} KB")

# Check local pipeline
lp = glob.glob("ml/model_output/local_pipeline*")
ep = glob.glob("ml/model_output/label_encoders*")
print(f"\nLocal pipelines: {len(lp)}, Label encoders: {len(ep)}")

# Check mlops registry
try:
    from mlops.model_registry import ModelRegistry
    reg = ModelRegistry()
    print("\nMLOps registry available")
except Exception as e:
    print(f"\nMLOps registry: {e}")
