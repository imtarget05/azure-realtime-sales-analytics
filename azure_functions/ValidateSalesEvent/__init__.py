"""
Azure Function: ValidateSalesEvent
Trigger: Azure Event Hubs (sales-events)

Validates and cleans incoming sales events before they are processed
by Azure Stream Analytics. Acts as the validation layer in the pipeline:

  Sim → Event Hubs → [THIS FUNCTION] → ASA → SQL

Validation rules:
  - Required fields present (timestamp, store_id, product_id, quantity, price)
  - Types correct (quantity=int, price=float, etc.)
  - Values within acceptable ranges
  - Timestamp not too far in past/future
  - Deduplication via event fingerprint
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import azure.functions as func

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
REQUIRED_FIELDS = {"timestamp", "store_id", "product_id", "quantity", "price"}

VALID_STORES = {"S01", "S02", "S03"}
VALID_PRODUCTS = {"COKE", "PEPSI", "BREAD", "MILK"}

QUANTITY_MIN, QUANTITY_MAX = 1, 100
PRICE_MIN, PRICE_MAX = 0.01, 10000.0
TIMESTAMP_TOLERANCE_HOURS = 24

# In-memory dedup cache (per function instance)
_seen_keys: set[str] = set()
_MAX_CACHE_SIZE = 50_000


def _parse_timestamp(ts_str: str) -> datetime | None:
    """Parse ISO 8601 timestamp string."""
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _validate_event(event: dict[str, Any]) -> tuple[bool, str]:
    """Validate a single sales event. Returns (is_valid, reason)."""
    # 1. Required fields
    missing = REQUIRED_FIELDS - set(event.keys())
    if missing:
        return False, f"missing fields: {missing}"

    # 2. Type checks
    try:
        quantity = int(event["quantity"])
        price = float(event["price"])
    except (ValueError, TypeError) as exc:
        return False, f"type error: {exc}"

    # 3. Range checks
    if not (QUANTITY_MIN <= quantity <= QUANTITY_MAX):
        return False, f"quantity out of range: {quantity}"
    if not (PRICE_MIN <= price <= PRICE_MAX):
        return False, f"price out of range: {price}"

    # 4. Store / product validity
    store_id = str(event.get("store_id", ""))
    product_id = str(event.get("product_id", ""))
    if store_id not in VALID_STORES:
        return False, f"unknown store_id: {store_id}"
    if product_id not in VALID_PRODUCTS:
        return False, f"unknown product_id: {product_id}"

    # 5. Timestamp freshness
    ts = _parse_timestamp(str(event.get("timestamp", "")))
    if ts is None:
        return False, "invalid timestamp format"
    now = datetime.now(timezone.utc)
    if abs((now - ts).total_seconds()) > TIMESTAMP_TOLERANCE_HOURS * 3600:
        return False, f"timestamp too far from now: {ts.isoformat()}"

    # 6. Deduplication
    dedupe_key = f"{event['timestamp']}_{store_id}_{product_id}_{quantity}_{price}"
    if dedupe_key in _seen_keys:
        return False, "duplicate event"
    if len(_seen_keys) >= _MAX_CACHE_SIZE:
        _seen_keys.clear()
    _seen_keys.add(dedupe_key)

    return True, "ok"


def _clean_event(event: dict[str, Any]) -> dict[str, Any]:
    """Normalize and enrich a validated event."""
    cleaned = {
        "timestamp": str(event["timestamp"]),
        "store_id": str(event["store_id"]).strip().upper(),
        "product_id": str(event["product_id"]).strip().upper(),
        "quantity": int(event["quantity"]),
        "price": round(float(event["price"]), 2),
        "revenue": round(int(event["quantity"]) * float(event["price"]), 2),
        "temperature": float(event.get("temperature", 0)),
        "weather": str(event.get("weather", "unknown")).lower(),
        "holiday": int(event.get("holiday", 0)),
        "validated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return cleaned


# ──────────────────────────────────────────────
# Azure Function entry point
# ──────────────────────────────────────────────
def main(events: list[func.EventHubEvent]) -> None:
    """Process a batch of Event Hub messages."""
    total = len(events)
    valid_count = 0
    invalid_count = 0

    for event in events:
        try:
            body = event.get_body().decode("utf-8")
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logging.warning("Failed to parse event body: %s", exc)
            invalid_count += 1
            continue

        is_valid, reason = _validate_event(data)
        if not is_valid:
            logging.warning("Invalid event rejected (%s): %s", reason, json.dumps(data)[:200])
            invalid_count += 1
            continue

        cleaned = _clean_event(data)
        valid_count += 1
        logging.info("Validated event: store=%s product=%s qty=%d revenue=%.2f",
                      cleaned["store_id"], cleaned["product_id"],
                      cleaned["quantity"], cleaned["revenue"])

    logging.info(
        "Batch processed: total=%d valid=%d invalid=%d",
        total, valid_count, invalid_count,
    )
