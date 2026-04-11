"""Build a Linux-compatible Azure Functions deployment zip.

Usage:
    python scripts/build_func_zip.py [--out func_deploy.zip]

Zip layout mirrors the Function App root:
  host.json
  requirements.txt
  DriftMonitor/
  ValidateSalesEvent/
  ml/             <- shared modules used by DriftMonitor
  monitoring/
  config/
  mlops/
"""

import argparse
import pathlib
import zipfile

ROOT = pathlib.Path(__file__).resolve().parent.parent
FUNC_DIR = ROOT / "azure_functions"

SHARED_MODULES = ["ml", "monitoring", "config", "mlops"]

SKIP_DIRS = {
    "__pycache__", ".git", ".venv", "node_modules",
    "model_output", "retrain_history", "retrain_comparison",
    "artifacts", ".pytest_cache", "tests", "benchmarks",
    "func_stage", "func_deploy.zip",
}
SKIP_EXTS = {
    ".pyc", ".pyo", ".pkl", ".joblib",
    ".png", ".jpg", ".jpeg", ".gif", ".webp",
    ".lock", ".zip", ".gz", ".tar",
}


def _should_skip(fpath: pathlib.Path) -> bool:
    for part in fpath.parts:
        if part in SKIP_DIRS:
            return True
    return fpath.suffix.lower() in SKIP_EXTS


def add_tree(zf: zipfile.ZipFile, src_dir: pathlib.Path, arc_prefix: str) -> int:
    written = 0
    for fpath in sorted(src_dir.rglob("*")):
        if _should_skip(fpath):
            continue
        if not fpath.is_file():
            continue
        rel = fpath.relative_to(src_dir).as_posix()
        arcname = f"{arc_prefix}/{rel}" if arc_prefix else rel
        zf.write(str(fpath), arcname)
        written += 1
    return written


def build(out: pathlib.Path) -> None:
    out.unlink(missing_ok=True)
    written = 0

    with zipfile.ZipFile(str(out), "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        # host.json and requirements.txt at root
        for name in ("host.json", "requirements.txt"):
            src = FUNC_DIR / name
            if src.exists():
                zf.write(str(src), name)
                written += 1

        # Function folders
        for fn_dir in sorted(FUNC_DIR.iterdir()):
            if not fn_dir.is_dir():
                continue
            written += add_tree(zf, fn_dir, fn_dir.name)

        # Shared modules
        for mod in SHARED_MODULES:
            mod_dir = ROOT / mod
            if mod_dir.exists():
                written += add_tree(zf, mod_dir, mod)
                # Ensure __init__.py exists in archive
                init_arc = f"{mod}/__init__.py"
                if init_arc not in zf.namelist():
                    zf.writestr(init_arc, "")
                    written += 1

    size_mb = out.stat().st_size / 1024 / 1024
    print(f"Written {written} files — {size_mb:.1f} MB -> {out}")

    # Spot-check
    required = [
        "host.json", "requirements.txt",
        "DriftMonitor/__init__.py", "DriftMonitor/function.json",
        "ValidateSalesEvent/__init__.py",
        "ml/drift_monitor.py", "monitoring/notifications.py",
        "monitoring/alerts.py", "config/settings.py",
        "mlops/trigger_training_pipeline.py",
    ]
    with zipfile.ZipFile(str(out)) as zf2:
        names = set(zf2.namelist())
    ok = True
    for r in required:
        if r in names:
            print(f"  OK      {r}")
        else:
            print(f"  MISSING {r}")
            ok = False
    if ok:
        print("All required files present.")
    else:
        print("WARNING: some files missing!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="func_deploy.zip")
    args = parser.parse_args()
    build(pathlib.Path(args.out))
