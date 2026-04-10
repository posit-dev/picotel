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


def test_none_value_skipped():
    """Test that None values are skipped and not included in output."""
    attributes = {"a": None, "b": "test", "c": None}
    result = _attributes_to_otlp(attributes)

    # Should only include "b", not "a" or "c"
    expected = [{"key": "b", "value": {"stringValue": "test"}}]
    assert result == expected


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
