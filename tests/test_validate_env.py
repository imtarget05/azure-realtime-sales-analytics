"""
Tests for validate_env.py — environment validation script.
"""

import os
import pytest
from scripts.validate_env import validate, _is_set


class TestIsSet:
    def test_empty_string(self):
        os.environ["_TEST_EMPTY"] = ""
        assert _is_set("_TEST_EMPTY") is False
        os.environ.pop("_TEST_EMPTY")

    def test_placeholder_angle_bracket(self):
        os.environ["_TEST_PH"] = "<your-value>"
        assert _is_set("_TEST_PH") is False
        os.environ.pop("_TEST_PH")

    def test_placeholder_your(self):
        os.environ["_TEST_PH2"] = "your-api-key"
        assert _is_set("_TEST_PH2") is False
        os.environ.pop("_TEST_PH2")

    def test_placeholder_xxx(self):
        os.environ["_TEST_PH3"] = "xxx-placeholder"
        assert _is_set("_TEST_PH3") is False
        os.environ.pop("_TEST_PH3")

    def test_real_value(self):
        os.environ["_TEST_REAL"] = "actual-connection-string"
        assert _is_set("_TEST_REAL") is True
        os.environ.pop("_TEST_REAL")

    def test_missing_var(self):
        assert _is_set("_NONEXISTENT_VAR_12345") is False


class TestValidate:
    def test_webapp_mode_passes(self):
        """Webapp has no hard requirements, should always pass."""
        assert validate("webapp") is True

    def test_all_mode_runs(self):
        """Should run without crash even with missing vars."""
        # May return True or False depending on env, but should not throw
        result = validate("all")
        assert isinstance(result, bool)
