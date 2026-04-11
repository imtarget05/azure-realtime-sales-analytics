"""
Tests for data_generator modules — sales, stock, weather generators.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


# ── Sales Generator ─────────────────────────────────────────────────

class TestSalesGenerator:
    def test_utc_now_iso_format(self):
        from data_generator.sales_generator import utc_now_iso
        ts = utc_now_iso()
        assert ts.endswith("Z")
        assert "T" in ts
        # Verify parseable
        datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def test_choose_store_returns_valid(self):
        from data_generator.sales_generator import choose_store_id
        from config.settings import STORE_IDS
        for _ in range(50):
            assert choose_store_id() in STORE_IDS

    def test_choose_product_schema(self):
        from data_generator.sales_generator import choose_product
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        weather = {"weather": "sunny", "temperature": 30}
        for _ in range(20):
            p = choose_product("S01", now, weather, 0)
            assert "product_id" in p
            assert "min_price" in p
            assert "max_price" in p
            assert p["max_price"] >= p["min_price"]

    def test_random_quantity_bounds(self):
        from data_generator.sales_generator import random_quantity
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        weather = {"weather": "sunny", "temperature": 30}
        product_meta = {"category": "Beverage"}
        promotion = {"qty_boost": 1.0}
        values = [random_quantity(product_meta, "S01", now, weather, 0, promotion) for _ in range(200)]
        assert all(1 <= v <= 12 for v in values)

    def test_random_price_bounds(self):
        from data_generator.sales_generator import random_price
        product = {"product_id": "TEST", "min_price": 2.0, "max_price": 8.0, "base_price": 5.0}
        promotion = {"discount": 0.0}
        for _ in range(100):
            p = random_price(product, "S01", promotion)
            assert product["min_price"] * 0.70 <= p <= product["max_price"] * 1.30
            assert p == round(p, 2)

    def test_normalize_weather_all_conditions(self):
        from data_generator.sales_generator import normalize_weather_condition
        mapping = {
            "Clear": "sunny", "Clouds": "cloudy", "Rain": "rainy",
            "Drizzle": "rainy", "Thunderstorm": "stormy", "Mist": "foggy",
            "Fog": "foggy", "Haze": "foggy",
        }
        for input_val, expected in mapping.items():
            assert normalize_weather_condition(input_val) == expected
        assert normalize_weather_condition("") == "unknown"
        assert normalize_weather_condition("Tornado") == "unknown"

    def test_validate_event_schema_valid(self):
        from data_generator.sales_generator import validate_event_schema
        event = {
            "timestamp": "2026-04-01T10:00:00Z",
            "store_id": "S01", "product_id": "COKE",
            "quantity": 2, "price": 1.5,
            "temperature": 30, "weather": "sunny", "holiday": 0,
        }
        validate_event_schema(event)  # No exception

    def test_validate_event_schema_missing_field(self):
        from data_generator.sales_generator import validate_event_schema
        with pytest.raises(ValueError, match="Thiếu field"):
            validate_event_schema({"timestamp": "2026-04-01T10:00:00Z"})

    def test_validate_event_schema_bad_holiday(self):
        from data_generator.sales_generator import validate_event_schema
        event = {
            "timestamp": "2026-04-01T10:00:00Z",
            "store_id": "S01", "product_id": "COKE",
            "quantity": 2, "price": 1.5,
            "temperature": 30, "weather": "sunny", "holiday": 99,
        }
        with pytest.raises(ValueError, match="holiday"):
            validate_event_schema(event)

    def test_validate_event_schema_zero_quantity(self):
        from data_generator.sales_generator import validate_event_schema
        event = {
            "timestamp": "2026-04-01T10:00:00Z",
            "store_id": "S01", "product_id": "COKE",
            "quantity": 0, "price": 1.5,
            "temperature": 30, "weather": "sunny", "holiday": 0,
        }
        with pytest.raises(ValueError, match="quantity"):
            validate_event_schema(event)

    def test_build_dedupe_key_deterministic(self):
        from data_generator.sales_generator import build_dedupe_key
        event = {"timestamp": "2026-04-01T10:00:00Z", "store_id": "S01",
                 "product_id": "COKE", "quantity": 1, "price": 1.5}
        k1 = build_dedupe_key(event)
        k2 = build_dedupe_key(event)
        assert k1 == k2

    def test_build_dedupe_key_unique(self):
        from data_generator.sales_generator import build_dedupe_key
        e1 = {"timestamp": "2026-04-01T10:00:00Z", "store_id": "S01",
              "product_id": "COKE", "quantity": 1, "price": 1.5}
        e2 = {"timestamp": "2026-04-01T10:00:00Z", "store_id": "S01",
              "product_id": "PEPSI", "quantity": 1, "price": 1.5}
        assert build_dedupe_key(e1) != build_dedupe_key(e2)

    def test_generate_batch_correct_size(self):
        from data_generator.sales_generator import generate_batch
        mock_weather = {"temperature": 30, "weather": "sunny"}
        with patch("data_generator.sales_generator.get_weather_for_store", return_value=mock_weather), \
             patch("data_generator.sales_generator.get_holiday_flag", return_value=0):
            batch = generate_batch(5)
        assert len(batch) == 5
        for event in batch:
            assert "timestamp" in event
            assert "store_id" in event
            assert "product_id" in event

    def test_generate_batch_zero_raises(self):
        from data_generator.sales_generator import generate_batch
        with pytest.raises(ValueError, match="batch_size"):
            generate_batch(0)


# ── Stock Generator ─────────────────────────────────────────────────

class TestStockGenerator:
    def test_stock_generator_instantiates(self):
        from data_generator.stock_generator import StockDataGenerator
        with patch(
            "data_generator.stock_generator.EventHubProducerClient.from_connection_string",
            return_value=MagicMock(),
        ):
            gen = StockDataGenerator()
        assert gen is not None


# ── Weather Generator ───────────────────────────────────────────────

class TestWeatherGenerator:
    def test_weather_generator_instantiates(self):
        from data_generator.weather_generator import WeatherDataGenerator
        with patch(
            "data_generator.weather_generator.EventHubProducerClient.from_connection_string",
            return_value=MagicMock(),
        ):
            gen = WeatherDataGenerator()
        assert gen is not None
