# Build & Dependency Management Guide

**Last Updated**: May 26, 2026

---

## Overview

This project uses three levels of dependency management:

1. **Root `requirements.txt`** — All shared dependencies (core, ML, Azure, testing)
2. **Subproject requirements** — Component-specific dependencies (webapp, Azure Functions)
3. **Conda environment** — ML training environment (optional, for advanced ML)

---

## Root Dependencies

**File**: `requirements.txt`  
**Used by**:
- Dockerfile (main app image)
- GitHub Actions CI/CD
- Local development
- Scripts

### Key Categories

| Category | Packages | Purpose |
|----------|----------|---------|
| **Web/Framework** | Flask, gunicorn, requests | Web server and API clients |
| **Data Processing** | numpy, pandas, scipy | Data manipulation and analysis |
| **ML/Models** | scikit-learn, joblib, matplotlib | Model training and evaluation |
| **Databases** | pyodbc, sqlalchemy | SQL Server connectivity |
| **Azure** | azure-eventhub, azure-storage-blob, azure-identity | Cloud integrations |
| **Testing** | pytest, flake8, black | Code quality assurance |

---

## Installation

### For Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/macOS
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Optional: install development extras
pip install -r requirements.txt pytest-cov black isort
```

### For Docker Deployment

```bash
# Build image
docker build -t sales-analytics:latest .

# Run container
docker run -p 8000:8000 \
  -e SQL_SERVER=<server> \
  -e SQL_PASSWORD=<password> \
  sales-analytics:latest
```

### For Azure Functions

```bash
cd azure_functions
pip install -r requirements.txt
```

---

## Dependency Updates

### Checking for Updates

```bash
# List outdated packages
pip list --outdated

# Check for security vulnerabilities
pip install safety
safety check
```

### Updating Dependencies

1. **For bugfixes** (e.g., numpy 1.24.2 → 1.24.3):
   - Update `requirements.txt`
   - Run tests: `pytest tests/`
   - Commit and push

2. **For major upgrades** (e.g., pandas 1.x → 2.x):
   - Create feature branch: `git checkout -b deps/upgrade-pandas`
   - Update version in `requirements.txt`
   - Run full test suite
   - Create PR with migration notes
   - Review and merge with approval

### Breaking Changes

Some packages have breaking changes between major versions. Common ones:

| Package | Breaking Change | Mitigation |
|---------|-----------------|-----------|
| **pandas 1.x → 2.x** | Deprecated functions removed | Use new APIs, run migration guide |
| **scikit-learn 0.x → 1.x** | API changes | Update model training code |
| **numpy 1.x → 2.x** | Type system changes | May require ndarray type adjustments |

---

## Docker Build Process

### Build Stages

```dockerfile
FROM python:3.11-slim
  ↓
Install system dependencies (pyodbc, ODBC drivers)
  ↓
COPY webapp/requirements.txt → pip install -r
  ↓
COPY requirements.txt → pip install -r
  ↓
COPY config/, ml/, webapp/ → application code
  ↓
HEALTHCHECK → http://localhost:8000/api/health
  ↓
CMD gunicorn webapp.app:app
```

### Build Examples

```bash
# Development image (no optimization)
docker build -t sales-analytics:dev .

# Production image (with caching optimization)
docker build \
  --cache-from sales-analytics:latest \
  -t sales-analytics:$(git rev-parse --short HEAD) \
  .

# Multi-architecture build (ARM64 for Apple Silicon)
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t sales-analytics:latest .
```

### Troubleshooting Docker Builds

| Error | Cause | Fix |
|-------|-------|-----|
| `No module named 'pyodbc'` | ODBC driver installation failed | Rebuild with `apt-get update` before ODBC install |
| `ModuleNotFoundError: No module named 'numpy'` | pip install failed silently | Check pip install output, ensure wheel available |
| `build timeout` | Large pip install taking too long | Increase timeout: `docker build --timeout 600` |

---

## CI/CD Integration

### GitHub Actions

The `validate` job in `.github/workflows/ci.yml` automatically:

1. Installs dependencies from `requirements.txt`
2. Validates config module imports
3. Checks for missing dependencies

Example:
```yaml
- name: Install dependencies
  run: |
    python -m pip install --upgrade pip
    pip install -r requirements.txt

- name: Validate config module
  run: python -c "from config.settings import PRODUCTS; print('OK')"
```

---

## Managing Azure-Specific Dependencies

### When to Include Azure SDK Packages

**Always include** (in `requirements.txt`):
```
azure-eventhub       # Production code uses Event Hubs
azure-storage-blob   # Production code uses Blob Storage
azure-identity       # For DefaultAzureCredential
```

**Only in Azure Functions** (`azure_functions/requirements.txt`):
```
azure-functions      # Function runtime
azure-keyvault-secrets  # Key Vault access
```

### Running Locally Without Azure

Some dependencies are optional for local dev. Set a fallback:

```python
try:
    from azure.eventhub import EventHubProducerClient
except ImportError:
    EventHubProducerClient = None
    print("Warning: EventHubProducerClient not available")
```

---

## Version Pinning Strategy

### Semantic Versioning

This project uses:
- **>= for compatibility**: `numpy>=1.24.0` (1.24.0 or newer)
- **< for breaking changes**: `pandas<3.0` (prevent major version jump)
- **~= for patch security**: `requests~=2.31` (2.31.x but not 2.32+)

Example:
```
numpy>=1.24.0,<2.0          # Major version stable
pandas>=2.0.0,<2.2          # Minor version range
scikit-learn~=1.3.0         # Patch-level only
requests>=2.31.0            # Latest compatible
```

### When to Pin Exact Versions

Use `==` ONLY for:
- Critical security fixes: `pyodbc==5.0.1  # CVE-2024-XXXX`
- Known incompatibilities: `azure-functions==1.13.0  # Broken in 1.14.0`

---

## Production Checklist

Before deploying to production:

- [ ] All tests pass: `pytest tests/`
- [ ] No security warnings: `safety check`
- [ ] No outdated dependencies: `pip list --outdated`
- [ ] Docker builds successfully: `docker build .`
- [ ] Health check passes: `curl localhost:8000/api/health`
- [ ] Required env vars set in deployment

---

## Support & Issues

### Common Errors

**Error**: `ERROR: Could not find a version that satisfies the requirement`

**Solution**: Check package name spelling, check Python version compatibility

```bash
# List available versions
pip index versions numpy

# Check compatible Python versions
pip search numpy  # (pip search deprecated, use PyPI.org instead)
```

**Error**: `Wheel 'xxx' not supported on this platform`

**Solution**: Mismatch between Python version and package architecture

```bash
# Verify Python installation
python --version
python -c "import struct; print(struct.calcsize('P') * 8)"  # Check if 64-bit

# Upgrade pip/wheel
pip install --upgrade pip wheel setuptools
```

### Contact

For dependency issues, contact: @data-platform-team
