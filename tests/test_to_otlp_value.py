# Copyright (C) 2025 by Posit Software, PBC.

"""Tests for the _to_otlp_value() helper function."""

from picotel import _to_otlp_value


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
