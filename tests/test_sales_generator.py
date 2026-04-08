"""
Unit tests cho data_generator/sales_generator.py — logic sinh event.
"""

import pytest


def test_utc_now_iso_format():
    """Timestamp phải có format ISO 8601 kết thúc bằng Z."""
    from data_generator.sales_generator import utc_now_iso
    ts = utc_now_iso()
    assert ts.endswith("Z"), f"Timestamp không kết thúc bằng Z: {ts}"
    assert "T" in ts


def test_choose_store_id_valid():
    """Store ID phải nằm trong danh sách cấu hình."""
    from data_generator.sales_generator import choose_store_id
    from config.settings import STORE_IDS
    for _ in range(50):
        sid = choose_store_id()
        assert sid in STORE_IDS, f"store_id '{sid}' không có trong STORE_IDS"


def test_choose_product_valid():
    """Product phải có đúng schema min_price/max_price."""
    from data_generator.sales_generator import choose_product
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    weather = {"weather": "sunny", "temperature": 30}
    for _ in range(50):
        p = choose_product("S01", now, weather, 0)
        assert "product_id" in p
        assert "min_price" in p
        assert "max_price" in p


def test_random_quantity_range():
    """Quantity phải trong khoảng 1-12."""
    from data_generator.sales_generator import random_quantity
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    weather = {"weather": "sunny", "temperature": 30}
    product_meta = {"category": "Beverage"}
    promotion = {"qty_boost": 1.0}
    for _ in range(100):
        q = random_quantity(product_meta, "S01", now, weather, 0, promotion)
        assert 1 <= q <= 12, f"quantity ngoài khoảng: {q}"


def test_random_price_within_bounds():
    """Price phải nằm trong [min_price*0.70, max_price*1.30]."""
    from data_generator.sales_generator import random_price
    product = {"product_id": "TEST", "min_price": 1.0, "max_price": 5.0, "base_price": 3.0}
    promotion = {"discount": 0.0}
    for _ in range(100):
        price = random_price(product, "S01", promotion)
        assert product["min_price"] * 0.70 <= price <= product["max_price"] * 1.30, f"price ngoài khoảng: {price}"
        assert price == round(price, 2)


def test_normalize_weather_condition():
    """Mapping thời tiết phải hoạt động đúng."""
    from data_generator.sales_generator import normalize_weather_condition
    assert normalize_weather_condition("Clear") == "sunny"
    assert normalize_weather_condition("Clouds") == "cloudy"
    assert normalize_weather_condition("Rain") == "rainy"
    assert normalize_weather_condition("Drizzle") == "rainy"
    assert normalize_weather_condition("Thunderstorm") == "stormy"
    assert normalize_weather_condition("Mist") == "foggy"
    assert normalize_weather_condition("") == "unknown"
    assert normalize_weather_condition("SomethingElse") == "unknown"


def test_validate_event_schema_valid():
    """Event hợp lệ không raise exception."""
    from data_generator.sales_generator import validate_event_schema
    event = {
        "timestamp": "2026-03-31T10:00:00Z",
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": 2,
        "price": 1.5,
        "temperature": 30,
        "weather": "sunny",
        "holiday": 0,
    }
    validate_event_schema(event)  # Không raise


def test_validate_event_schema_missing_field():
    """Event thiếu field phải raise ValueError."""
    from data_generator.sales_generator import validate_event_schema
    event = {"timestamp": "2026-03-31T10:00:00Z", "store_id": "S01"}
    with pytest.raises(ValueError, match="Thiếu field"):
        validate_event_schema(event)


def test_validate_event_schema_invalid_holiday():
    """holiday phải là 0 hoặc 1."""
    from data_generator.sales_generator import validate_event_schema
    event = {
        "timestamp": "2026-03-31T10:00:00Z",
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": 2,
        "price": 1.5,
        "temperature": 30,
        "weather": "sunny",
        "holiday": 5,
    }
    with pytest.raises(ValueError, match="holiday"):
        validate_event_schema(event)


def test_validate_event_schema_invalid_quantity():
    """quantity phải > 0."""
    from data_generator.sales_generator import validate_event_schema
    event = {
        "timestamp": "2026-03-31T10:00:00Z",
        "store_id": "S01",
        "product_id": "COKE",
        "quantity": 0,
        "price": 1.5,
        "temperature": 30,
        "weather": "sunny",
        "holiday": 0,
    }
    with pytest.raises(ValueError, match="quantity"):
        validate_event_schema(event)


def test_build_dedupe_key_unique():
    """Hai event khác nhau phải có dedupe key khác nhau."""
    from data_generator.sales_generator import build_dedupe_key
    e1 = {"timestamp": "2026-03-31T10:00:00Z", "store_id": "S01",
          "product_id": "COKE", "quantity": 1, "price": 1.5}
    e2 = {"timestamp": "2026-03-31T10:00:00Z", "store_id": "S01",
          "product_id": "PEPSI", "quantity": 1, "price": 1.5}
    assert build_dedupe_key(e1) != build_dedupe_key(e2)


def test_generate_batch_correct_size():
    """generate_batch phải trả đúng số lượng event yêu cầu."""
    from unittest.mock import patch
    from data_generator.sales_generator import generate_batch

    # Mock API calls để không cần key
    mock_weather = {"temperature": 30, "weather": "sunny"}
    with patch("data_generator.sales_generator.get_weather_for_store", return_value=mock_weather), \
         patch("data_generator.sales_generator.get_holiday_flag", return_value=0):
        batch = generate_batch(5)
    assert len(batch) == 5


def test_generate_batch_zero_raises():
    """batch_size <= 0 phải raise ValueError."""
    from data_generator.sales_generator import generate_batch
    with pytest.raises(ValueError, match="batch_size"):
        generate_batch(0)
