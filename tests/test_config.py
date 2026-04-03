"""
Unit tests cho config/settings.py — kiểm tra config loading và helpers.
"""

import os
import pytest


def test_products_list_loaded():
    """PRODUCTS phải có ít nhất 1 sản phẩm với đúng schema."""
    from config.settings import PRODUCTS
    assert len(PRODUCTS) > 0
    for p in PRODUCTS:
        assert "id" in p, f"Product thiếu 'id': {p}"
        assert "name" in p, f"Product thiếu 'name': {p}"
        assert "category" in p, f"Product thiếu 'category': {p}"
        assert "base_price" in p, f"Product thiếu 'base_price': {p}"
        assert p["base_price"] > 0, f"base_price phải > 0: {p}"


def test_sales_products_derived_from_products():
    """SALES_PRODUCTS phải là tập con của PRODUCTS (product_id khớp id)."""
    from config.settings import PRODUCTS, SALES_PRODUCTS, VALID_PRODUCT_IDS
    product_ids = {p["id"] for p in PRODUCTS}
    for sp in SALES_PRODUCTS:
        assert sp["product_id"] in product_ids, (
            f"SALES_PRODUCTS item '{sp['product_id']}' không có trong PRODUCTS"
        )
        assert sp["min_price"] > 0
        assert sp["max_price"] >= sp["min_price"]
    # VALID_PRODUCT_IDS phải khớp tập id trong PRODUCTS
    assert VALID_PRODUCT_IDS == product_ids


def test_store_ids_and_locations_consistent():
    """STORE_IDS và STORE_LOCATIONS phải khớp nhau."""
    from config.settings import STORE_IDS, STORE_LOCATIONS
    assert set(STORE_IDS) == set(STORE_LOCATIONS.keys()), (
        f"Mismatch: STORE_IDS={STORE_IDS}, STORE_LOCATIONS keys={list(STORE_LOCATIONS.keys())}"
    )


def test_regions_not_empty():
    from config.settings import REGIONS
    assert len(REGIONS) > 0


def test_get_bool_helper():
    """_get_bool phải parse đúng các giá trị boolean."""
    from config.settings import _get_bool

    os.environ["_TEST_BOOL_TRUE"] = "true"
    os.environ["_TEST_BOOL_YES"] = "yes"
    os.environ["_TEST_BOOL_ONE"] = "1"
    os.environ["_TEST_BOOL_FALSE"] = "false"
    os.environ["_TEST_BOOL_ZERO"] = "0"

    assert _get_bool("_TEST_BOOL_TRUE") is True
    assert _get_bool("_TEST_BOOL_YES") is True
    assert _get_bool("_TEST_BOOL_ONE") is True
    assert _get_bool("_TEST_BOOL_FALSE") is False
    assert _get_bool("_TEST_BOOL_ZERO") is False
    assert _get_bool("_TEST_NONEXISTENT", default=True) is True

    # Cleanup
    for k in ("_TEST_BOOL_TRUE", "_TEST_BOOL_YES", "_TEST_BOOL_ONE",
              "_TEST_BOOL_FALSE", "_TEST_BOOL_ZERO"):
        os.environ.pop(k, None)


def test_get_int_helper():
    from config.settings import _get_int
    os.environ["_TEST_INT"] = "42"
    assert _get_int("_TEST_INT", 0) == 42
    assert _get_int("_TEST_INT_MISSING", 99) == 99
    os.environ.pop("_TEST_INT", None)


def test_get_float_helper():
    from config.settings import _get_float
    os.environ["_TEST_FLOAT"] = "3.14"
    assert abs(_get_float("_TEST_FLOAT", 0.0) - 3.14) < 0.001
    assert abs(_get_float("_TEST_FLOAT_MISSING", 2.72) - 2.72) < 0.001
    os.environ.pop("_TEST_FLOAT", None)


def test_validate_required_settings_raises_on_missing():
    """validate_required_settings phải raise ValueError khi thiếu API keys."""
    from config.settings import validate_required_settings, OPENWEATHER_API_KEY
    # Nếu API key thật đã được set thì skip test này
    if OPENWEATHER_API_KEY and not OPENWEATHER_API_KEY.startswith("<"):
        pytest.skip("API keys đã được cấu hình — skip negative test")
    with pytest.raises(ValueError, match="Thiếu biến môi trường"):
        validate_required_settings(mode="generator")


def test_get_runtime_config_no_secrets():
    """get_runtime_config() không được chứa secrets."""
    from config.settings import get_runtime_config
    config = get_runtime_config()
    secret_keywords = ("password", "secret", "key", "token", "connection_string")
    for key in config:
        assert not any(s in key.lower() for s in secret_keywords), (
            f"get_runtime_config() đang leak secret key: {key}"
        )
