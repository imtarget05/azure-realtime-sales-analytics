"""Create a clean deployment ZIP for Azure App Service."""
import zipfile, os

root = r"C:\Users\Admin\azure-realtime-sales-analytics"
zip_path = os.path.join(root, "deploy.zip")

include_dirs = [
    "webapp", "ml", "config", "data_factory", "data_generator",
    "stream_analytics", "benchmark_output", "benchmarks", "docs",
    "sql", "monitoring", "mlops", "scripts", "security",
    "blob_storage", "infrastructure", "powerbi", "terraform",
    "azure_functions", "tests",
]
include_files = ["requirements.txt", "sample_events.jsonl", "validate_env.py"]
skip_dirs = {".venv", "__pycache__", ".git", "node_modules"}
skip_files = {"nul", ".env", "deploy.zip"}

count = 0
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for f in include_files:
        fp = os.path.join(root, f)
        if os.path.exists(fp):
            zf.write(fp, f)
            count += 1

    for d in include_dirs:
        dp = os.path.join(root, d)
        if not os.path.isdir(dp):
            continue
        for dirpath, dirnames, filenames in os.walk(dp):
            dirnames[:] = [x for x in dirnames if x not in skip_dirs]
            for fn in filenames:
                if fn in skip_files or fn.endswith(".pyc"):
                    continue
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, root)
                try:
                    zf.write(full, rel)
                    count += 1
                except Exception as e:
                    print(f"SKIP: {rel} ({e})")

size_mb = os.path.getsize(zip_path) / (1024 * 1024)
print(f"Zipped {count} files => {size_mb:.1f} MB")
