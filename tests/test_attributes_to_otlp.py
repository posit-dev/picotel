"""Tests for the _attributes_to_otlp() helper function."""

from picotel import _attributes_to_otlp


def test_basic_attributes():
    """Test conversion of basic string and integer attributes."""
    attributes = {"foo": "bar", "count": 5}
    result = _attributes_to_otlp(attributes)

    expected = [
        {"key": "foo", "value": {"stringValue": "bar"}},
        {"key": "count", "value": {"intValue": "5"}},
    ]

    assert result == expected


def test_empty_dict():
    """Test that empty dict returns empty list."""
    result = _attributes_to_otlp({})
    assert result == []


def test_none_value_skipped():
    """Test that None values are skipped and not included in output."""
    attributes = {"a": None, "b": "test", "c": None}
    result = _attributes_to_otlp(attributes)

    # Should only include "b", not "a" or "c"
    expected = [{"key": "b", "value": {"stringValue": "test"}}]
    assert result == expected


def test_various_types():
    """Test conversion of various Python types."""
    attributes = {
        "string": "hello",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "list": [1, 2, 3],
        "dict": {"nested": "value"},
        "none": None,  # Should be skipped
    }

    result = _attributes_to_otlp(attributes)

    # Check that we have 6 items (none should be skipped)
    assert len(result) == 6

    # Check each item directly in the result list
    # Find each attribute by key and verify its value
    for item in result:
        key = item["key"]
        value = item["value"]

        if key == "string":
            assert value == {"stringValue": "hello"}
        elif key == "int":
            assert value == {"intValue": "42"}
        elif key == "float":
            assert value == {"doubleValue": 3.14}
        elif key == "bool":
            assert value == {"boolValue": True}
        elif key == "list":
            assert value == {
                "arrayValue": {
                    "values": [{"intValue": "1"}, {"intValue": "2"}, {"intValue": "3"}]
                }
            }
        elif key == "dict":
            assert value == {
                "kvlistValue": {
                    "values": [{"key": "nested", "value": {"stringValue": "value"}}]
                }
            }

    # Verify "none" is not present
    keys = [item["key"] for item in result]
    assert "none" not in keys


def test_nested_none_in_values():
    """Test that None inside nested structures is handled correctly."""
    # None at top level should be skipped
    attributes = {"top_none": None}
    result = _attributes_to_otlp(attributes)
    assert result == []

    # None inside a list should be converted to empty dict
    attributes = {"list_with_none": [1, None, 2]}
    result = _attributes_to_otlp(attributes)
    expected = [
        {
            "key": "list_with_none",
            "value": {
                "arrayValue": {
                    "values": [
                        {"intValue": "1"},
                        {},  # None becomes empty dict
                        {"intValue": "2"},
                    ]
                }
            },
        }
    ]
    assert result == expected


def test_order_preserved():
    """Test that attribute order is preserved (Python 3.7+ guarantees dict order)."""
    attributes = {"z": 1, "a": 2, "m": 3}
    result = _attributes_to_otlp(attributes)

    keys = [item["key"] for item in result]
    assert keys == ["z", "a", "m"]
