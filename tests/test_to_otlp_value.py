# Copyright (C) 2025 by Posit Software, PBC.

"""Tests for the _to_otlp_value() helper function."""

import base64

from miniotel import _to_otlp_value


def test_string_value():
    """Test that strings are converted to stringValue."""
    result = _to_otlp_value("hello")
    assert result == {"stringValue": "hello"}


def test_int_value():
    """Test that integers are converted to intValue as string."""
    result = _to_otlp_value(42)
    assert result == {"intValue": "42"}


def test_bool_value():
    """Test that booleans are converted to boolValue, not intValue."""
    result = _to_otlp_value(True)  # noqa: FBT003
    assert result == {"boolValue": True}

    result = _to_otlp_value(False)  # noqa: FBT003
    assert result == {"boolValue": False}


def test_float_value():
    """Test that floats are converted to doubleValue."""
    result = _to_otlp_value(3.14)
    assert result == {"doubleValue": 3.14}


def test_bytes_value():
    """Test that bytes are base64 encoded and converted to bytesValue."""
    data = b"hello world"
    result = _to_otlp_value(data)
    expected = {"bytesValue": base64.b64encode(data).decode()}
    assert result == expected


def test_list_value():
    """Test that lists are converted to arrayValue with nested conversion."""
    result = _to_otlp_value([1, 2])
    expected = {"arrayValue": {"values": [{"intValue": "1"}, {"intValue": "2"}]}}
    assert result == expected


def test_dict_value():
    """Test that dicts are converted to kvlistValue with nested conversion."""
    result = _to_otlp_value({"a": 1})
    expected = {"kvlistValue": {"values": [{"key": "a", "value": {"intValue": "1"}}]}}
    assert result == expected


def test_none_value():
    """Test that None is converted to an empty dict."""
    result = _to_otlp_value(None)
    assert result == {}


def test_nested_structures():
    """Test complex nested structures."""
    # List with mixed types
    result = _to_otlp_value(["hello", 42, True, 3.14, None])
    expected = {
        "arrayValue": {
            "values": [
                {"stringValue": "hello"},
                {"intValue": "42"},
                {"boolValue": True},
                {"doubleValue": 3.14},
                {},
            ]
        }
    }
    assert result == expected

    # Dict with mixed types
    result = _to_otlp_value(
        {
            "string": "hello",
            "int": 42,
            "bool": True,
            "float": 3.14,
            "none": None,
            "list": [1, 2],
            "dict": {"nested": "value"},
        }
    )
    values = result["kvlistValue"]["values"]
    assert values[0] == {"key": "string", "value": {"stringValue": "hello"}}
    assert values[1] == {"key": "int", "value": {"intValue": "42"}}
    assert values[2] == {"key": "bool", "value": {"boolValue": True}}
    assert values[3] == {"key": "float", "value": {"doubleValue": 3.14}}
    assert values[4] == {"key": "none", "value": {}}
    assert values[5] == {
        "key": "list",
        "value": {"arrayValue": {"values": [{"intValue": "1"}, {"intValue": "2"}]}},
    }
    assert values[6] == {
        "key": "dict",
        "value": {
            "kvlistValue": {
                "values": [{"key": "nested", "value": {"stringValue": "value"}}]
            }
        },
    }


def test_large_integer():
    """Test that large integers are properly handled as strings."""
    large_int = 9223372036854775807  # Max int64
    result = _to_otlp_value(large_int)
    assert result == {"intValue": "9223372036854775807"}


def test_unknown_type_fallback():
    """Test that unknown types fallback to string representation."""

    class CustomClass:
        def __str__(self):
            return "custom_value"

    obj = CustomClass()
    result = _to_otlp_value(obj)
    assert result == {"stringValue": "custom_value"}
