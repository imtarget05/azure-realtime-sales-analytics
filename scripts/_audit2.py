import sys, os, datetime
sys.path.insert(0, '.')
os.environ.setdefault('KEY_VAULT_URI', 'DISABLED')
from pathlib import Path

print("=== MLOPS FILES ===")
for f in sorted(Path('mlops').glob('*.py')):
    print(f"  {f.name}: {f.stat().st_size} bytes")

print("\n=== ML MODEL OUTPUT ===")
ml_out = Path('ml/model_output')
if ml_out.exists():
    for f in sorted(ml_out.iterdir()):
        if f.is_file():
            mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            print(f"  {f.name}: {f.stat().st_size/1024:.1f} KB  [{mtime}]")
        elif f.is_dir():
            files = list(f.iterdir())
            print(f"  {f.name}/: ({len(files)} files)")

print("\n=== WEBAPP TEMPLATES ===")
for f in sorted(Path('webapp/templates').glob('*.html')):
    print(f"  {f.name}: {f.stat().st_size/1024:.1f} KB")

print("\n=== SCRIPTS FOLDER ===")
for f in sorted(Path('scripts').glob('*.py')):
    print(f"  {f.name}")
