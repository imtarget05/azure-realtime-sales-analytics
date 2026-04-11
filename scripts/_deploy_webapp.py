"""
Deploy webapp to Azure App Service using Azure Identity + REST API.
Bypasses az CLI which may hang due to expired tokens.
Usage: python scripts/_deploy_webapp.py
"""
import io, json, os, sys, zipfile, time
import requests

# ── Config ──
RG = "rg-sales-analytics-dev"
APP = "webapp-sales-analytics-d9bt2m"
SUB_ID = None  # Will be resolved from Azure profile

def get_subscription_id():
    profile_path = os.path.expanduser("~/.azure/azureProfile.json")
    with open(profile_path, "r", encoding="utf-8-sig") as f:
        profile = json.load(f)
    for sub in profile.get("subscriptions", []):
        if sub.get("isDefault"):
            return sub["id"]
    raise RuntimeError("No default subscription found in Azure profile")

def get_access_token():
    """Get access token via DeviceCodeCredential (user enters code in browser)."""
    from azure.identity import DeviceCodeCredential
    credential = DeviceCodeCredential()
    token = credential.get_token("https://management.azure.com/.default")
    return token.token

def get_publish_credentials(token, sub_id):
    """Get Kudu deployment credentials via ARM REST API."""
    url = (
        f"https://management.azure.com/subscriptions/{sub_id}"
        f"/resourceGroups/{RG}/providers/Microsoft.Web/sites/{APP}"
        f"/config/publishingcredentials/list?api-version=2022-03-01"
    )
    r = requests.post(url, headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    creds = r.json()
    return creds["properties"]["publishingUserName"], creds["properties"]["publishingPassword"]

def create_deployment_zip():
    """Create a zip of the project for deployment."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    buf = io.BytesIO()

    # Folders/files to include
    include_dirs = ["webapp", "ml/model_output", "config"]
    include_files = ["requirements.txt"]

    # Folders/files to exclude
    exclude_patterns = {".venv", "__pycache__", ".git", "node_modules", ".pytest_cache",
                        "benchmarks", "terraform", "infrastructure", "data_factory",
                        "databricks", "blob_storage", ".azure", "scripts", "tests",
                        "stream_analytics", "powerbi", "docs", "azure_functions"}

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for d in include_dirs:
            dir_path = os.path.join(root, d)
            if not os.path.isdir(dir_path):
                print(f"  WARN: {d} not found, skipping")
                continue
            for dirpath, dirnames, filenames in os.walk(dir_path):
                # Skip excluded
                dirnames[:] = [dn for dn in dirnames if dn not in exclude_patterns]
                for fn in filenames:
                    if fn.endswith((".pyc", ".pyo")):
                        continue
                    full = os.path.join(dirpath, fn)
                    arcname = os.path.relpath(full, root)
                    zf.write(full, arcname)

        for f in include_files:
            fp = os.path.join(root, f)
            if os.path.isfile(fp):
                zf.write(fp, f)

        # Also include config/__init__.py and config/settings.py
        for f in ["config/__init__.py", "config/settings.py"]:
            fp = os.path.join(root, f)
            if os.path.isfile(fp):
                zf.write(fp, f)

    size_mb = buf.tell() / (1024 * 1024)
    print(f"  Zip created: {size_mb:.1f} MB")
    buf.seek(0)
    return buf

def deploy_to_kudu(username, password, zip_buf):
    """Deploy zip to Azure App Service via Kudu API."""
    url = f"https://{APP}.scm.azurewebsites.net/api/zipdeploy?isAsync=true"
    print(f"  Deploying to {url}...")
    r = requests.post(
        url,
        data=zip_buf.read(),
        auth=(username, password),
        headers={"Content-Type": "application/zip"},
        timeout=300,
    )
    if r.status_code in (200, 202):
        print(f"  Deploy accepted (status {r.status_code})")
        if r.status_code == 202:
            poll_url = r.headers.get("Location")
            if poll_url:
                print(f"  Polling deployment status...")
                for _ in range(30):
                    time.sleep(10)
                    pr = requests.get(poll_url, auth=(username, password), timeout=30)
                    status = pr.json().get("status", "Unknown")
                    print(f"    Status: {status}")
                    if status in ("4", "Failed"):
                        print("  DEPLOY FAILED")
                        print(pr.json())
                        return False
                    if status in ("4", "Success"):
                        break
        return True
    else:
        print(f"  Deploy FAILED: {r.status_code}")
        print(r.text[:500])
        return False

def set_startup_command(token, sub_id):
    """Set the startup command for the webapp."""
    url = (
        f"https://management.azure.com/subscriptions/{sub_id}"
        f"/resourceGroups/{RG}/providers/Microsoft.Web/sites/{APP}"
        f"/config/web?api-version=2022-03-01"
    )
    body = {
        "properties": {
            "appCommandLine": "gunicorn --bind 0.0.0.0:8000 --workers 2 --timeout 120 webapp.app:app"
        }
    }
    r = requests.patch(url, json=body, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    if r.status_code == 200:
        print("  Startup command set: gunicorn --bind 0.0.0.0:8000 --workers 2 --timeout 120 webapp.app:app")
    else:
        print(f"  WARN: Could not set startup command: {r.status_code}")

def main():
    print("=" * 60)
    print("DEPLOY WEBAPP TO AZURE APP SERVICE")
    print("=" * 60)

    # Step 1: Get subscription
    print("\n[1/5] Reading Azure subscription...")
    sub_id = get_subscription_id()
    print(f"  Subscription: {sub_id}")

    # Step 2: Authenticate
    print("\n[2/5] Authenticating (device code flow)...")
    print("  → A code will appear. Enter it at https://microsoft.com/devicelogin")
    token = get_access_token()
    print("  ✓ Authenticated!")

    # Step 3: Get publish credentials
    print("\n[3/5] Getting deployment credentials...")
    username, password = get_publish_credentials(token, sub_id)
    print(f"  ✓ Got credentials for user: {username}")

    # Step 4: Create zip
    print("\n[4/5] Creating deployment package...")
    zip_buf = create_deployment_zip()

    # Step 5: Deploy
    print("\n[5/5] Deploying to Azure...")
    ok = deploy_to_kudu(username, password, zip_buf)

    # Set startup command
    print("\n[BONUS] Setting startup command...")
    set_startup_command(token, sub_id)

    if ok:
        print(f"\n{'=' * 60}")
        print(f"✓ DEPLOYMENT COMPLETE!")
        print(f"  URL: https://{APP}.azurewebsites.net")
        print(f"  Dashboard: https://{APP}.azurewebsites.net/dashboard")
        print(f"  Model Report: https://{APP}.azurewebsites.net/model-report")
        print(f"{'=' * 60}")
    else:
        print("\n✗ Deployment failed. Check errors above.")

if __name__ == "__main__":
    main()
