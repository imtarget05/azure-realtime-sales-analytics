"""
Unit tests cho Azure Function ValidateSalesEvent.

azure.functions không được cài ở local, nên mock nó trước khi import.
"""

import sys
import types
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

# Tạo mock module cho azure.functions (chỉ cần cho import, không dùng runtime)
# Chỉ mock nếu chưa cài — tránh ghi đè azure namespace thật
if "azure.functions" not in sys.modules:
    try:
        import azure.functions  # noqa: F401
    except ImportError:
        _mock_func = types.ModuleType("azure.functions")
        _mock_func.EventHubEvent = MagicMock
        sys.modules["azure.functions"] = _mock_func

from azure_functions.ValidateSalesEvent import (
    _parse_timestamp,
    _validate_event,
    _clean_event,
    _seen_keys,
)


def test_parse_timestamp_valid_formats():
    """Phải parse được các format ISO 8601 phổ biến."""
    assert _parse_timestamp("2026-03-31T10:00:00Z") is not None
    assert _parse_timestamp("2026-03-31T10:00:00.000Z") is not None
    assert _parse_timestamp("2026-03-31T10:00:00+00:00") is not None


def test_parse_timestamp_invalid():
    assert _parse_timestamp("not-a-date") is None
    assert _parse_timestamp("") is None
    assert _parse_timestamp("31/03/2026") is None


def test_validate_event_valid():
    """Event đầy đủ và hợp lệ phải pass validation."""
    _seen_keys.clear()
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": 3,
        "price": 1.5,
    }
    is_valid, reason = _validate_event(event)
    assert is_valid, f"Expected valid but got: {reason}"


def test_validate_event_missing_fields():
    is_valid, reason = _validate_event({"timestamp": "2026-03-31T10:00:00Z"})
    assert not is_valid
    assert "missing fields" in reason


def test_validate_event_quantity_out_of_range():
    _seen_keys.clear()
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": 999,
        "price": 1.5,
    }
    is_valid, reason = _validate_event(event)
    assert not is_valid
    assert "quantity out of range" in reason


def test_validate_event_unknown_store():
    _seen_keys.clear()
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store_id": "INVALID",
        "product_id": "COKE",
        "quantity": 1,
        "price": 1.5,
    }
    is_valid, reason = _validate_event(event)
    assert not is_valid
    assert "unknown store_id" in reason


def test_validate_event_unknown_product():
    _seen_keys.clear()
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store_id": "S01",
        "product_id": "NONEXISTENT",
        "quantity": 1,
        "price": 1.5,
    }
    is_valid, reason = _validate_event(event)
    assert not is_valid
    assert "unknown product_id" in reason


def test_validate_event_dedup():
    """Cùng event gửi 2 lần phải bị reject lần thứ 2."""
    _seen_keys.clear()
    event = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "store_id": "S02",
        "product_id": "MILK",
        "quantity": 2,
        "price": 1.8,
    }
    is_valid_1, _ = _validate_event(event)
    assert is_valid_1
    is_valid_2, reason_2 = _validate_event(event)
    assert not is_valid_2
    assert "duplicate" in reason_2


def test_validate_event_stale_timestamp():
    """Timestamp quá cũ (>24h) phải bị reject."""
    _seen_keys.clear()
    event = {
        "timestamp": "2020-01-01T00:00:00Z",
        "store_id": "S01",
        "product_id": "BREAD",
        "quantity": 1,
        "price": 1.0,
    }
    is_valid, reason = _validate_event(event)
    assert not is_valid
    assert "timestamp too far" in reason


def test_clean_event_computes_revenue():
    """_clean_event phải tính revenue = quantity * price."""
    event = {
        "timestamp": "2026-03-31T10:00:00Z",
        "store_id": "s01",
        "product_id": "coke",
        "quantity": 3,
        "price": 1.5,
        "temperature": 28,
        "weather": "Sunny",
        "holiday": 1,
    }
    cleaned = _clean_event(event)
    assert cleaned["revenue"] == 4.5
    assert cleaned["store_id"] == "S01"  # uppercase
    assert cleaned["product_id"] == "COKE"  # uppercase
    assert cleaned["weather"] == "sunny"  # lowercase
    assert "validated_at" in cleaned
