#!/usr/bin/env python3
"""
Validate .env configuration before running any component.

Usage:
    python validate_env.py              # Check all
    python validate_env.py --mode webapp     # Check webapp only
    python validate_env.py --mode generator  # Check generator only
    python validate_env.py --mode ml         # Check ML training only
"""

import os
import sys

from dotenv import load_dotenv

load_dotenv()

# ── Variable definitions per mode ──────────────────────────────────────
REQUIRED = {
    "webapp": [],  # Webapp works with local model fallback, no hard requirement
    "generator": [
        "EVENT_HUB_CONNECTION_STRING",
        "OPENWEATHER_API_KEY",
    ],
    "ml": [
        "SQL_SERVER",
        "SQL_DATABASE",
        "SQL_USERNAME",
        "SQL_PASSWORD",
    ],
    "azure": [
        "AZURE_SUBSCRIPTION_ID",
        "AZURE_RESOURCE_GROUP",
    ],
}

OPTIONAL_WARN = {
    "webapp": [
        ("AML_ENDPOINT_URL", "ML predictions will use local model fallback"),
        ("AML_API_KEY", "ML predictions will use local model fallback"),
    ],
    "generator": [
        ("CALENDARIFIC_API_KEY", "Holiday flag will always be 0"),
    ],
    "ml": [
        ("AML_WORKSPACE_NAME", "Cannot register model to AML"),
        ("BLOB_CONNECTION_STRING", "Cannot upload to blob storage"),
    ],
    "azure": [],
}

_PLACEHOLDER_PREFIXES = ("<", "your-", "xxx")


def _is_set(key: str) -> bool:
    val = os.getenv(key, "")
    if not val:
        return False
    return not any(val.lower().startswith(p) for p in _PLACEHOLDER_PREFIXES)


def validate(mode: str = "all") -> bool:
    modes = list(REQUIRED.keys()) if mode == "all" else [mode]
    errors = []
    warnings = []

    for m in modes:
        for key in REQUIRED.get(m, []):
            if not _is_set(key):
                errors.append(f"  [MISSING] {key}  (required for {m})")

        for key, msg in OPTIONAL_WARN.get(m, []):
            if not _is_set(key):
                warnings.append(f"  [WARN]    {key} — {msg}")

    # Print results
    if warnings:
        print("⚠️  Optional variables not set:")
        for w in warnings:
            print(w)
        print()

    if errors:
        print("❌ Required variables missing:")
        for e in errors:
            print(e)
        print(f"\nCopy .env.example → .env and fill in the values.")
        return False

    print(f"✅ Environment validation passed for mode: {mode}")
    return True


if __name__ == "__main__":
    mode = "all"
    if "--mode" in sys.argv:
        idx = sys.argv.index("--mode")
        if idx + 1 < len(sys.argv):
            mode = sys.argv[idx + 1]

    ok = validate(mode)
    sys.exit(0 if ok else 1)
