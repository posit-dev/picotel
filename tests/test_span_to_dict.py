"""Tests for _span_to_dict() serialization."""

import json

from miniotel import (
    Span,
    _span_to_dict,
    new_span_id,
    new_trace_id,
    now_ns,
)


def test_minimal_span_to_dict():
    """Create minimal span, verify JSON dict has required fields with correct types."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 1000000  # 1ms later

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="test_operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
    )

    result = _span_to_dict(span)

    # Verify required fields are present
    assert result["traceId"] == trace_id
    assert result["spanId"] == span_id
    assert result["name"] == "test_operation"
    assert result["kind"] == 1  # SpanKind.INTERNAL default
    assert result["startTimeUnixNano"] == str(start_time)
    assert result["endTimeUnixNano"] == str(end_time)

    # Verify optional fields are omitted when empty
    assert "parentSpanId" not in result
    assert "attributes" not in result
    assert "events" not in result
    assert "links" not in result
    assert "status" not in result


def test_span_with_parent_and_attributes():
    """Test span with parent span ID and attributes."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    parent_span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 2000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name="child_operation",
        kind=Span.Kind.CLIENT,
        start_time_ns=start_time,
        end_time_ns=end_time,
        attributes={"http.method": "GET", "http.status_code": 200, "user.id": 12345},
    )

    result = _span_to_dict(span)

    assert result["parentSpanId"] == parent_span_id
    assert result["kind"] == 3  # SpanKind.CLIENT
    assert "attributes" in result
    assert len(result["attributes"]) == 3

    # Check attribute format
    attrs_by_key = {attr["key"]: attr["value"] for attr in result["attributes"]}
    assert attrs_by_key["http.method"] == {"stringValue": "GET"}
    assert attrs_by_key["http.status_code"] == {"intValue": "200"}
    assert attrs_by_key["user.id"] == {"intValue": "12345"}


def test_span_with_events():
    """Create span with events, verify events array is present."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()
    event_time1 = start_time + 500000
    event_time2 = start_time + 1000000
    end_time = start_time + 2000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="operation_with_events",
        start_time_ns=start_time,
        end_time_ns=end_time,
        events=[
            Span.Event(
                name="request_started",
                timestamp_ns=event_time1,
                attributes={"url": "https://example.com"},
            ),
            Span.Event(
                name="request_completed",
                timestamp_ns=event_time2,
                attributes={"response_size": 1024},
            ),
        ],
    )

    result = _span_to_dict(span)

    assert "events" in result
    assert len(result["events"]) == 2

    # Check first event
    event1 = result["events"][0]
    assert event1["name"] == "request_started"
    assert event1["timeUnixNano"] == str(event_time1)
    assert "attributes" in event1
    assert len(event1["attributes"]) == 1
    assert event1["attributes"][0] == {
        "key": "url",
        "value": {"stringValue": "https://example.com"},
    }

    # Check second event
    event2 = result["events"][1]
    assert event2["name"] == "request_completed"
    assert event2["timeUnixNano"] == str(event_time2)
    assert event2["attributes"][0] == {
        "key": "response_size",
        "value": {"intValue": "1024"},
    }


def test_span_with_links():
    """Test span with links to other spans."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    linked_trace_id = new_trace_id()
    linked_span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 1000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="operation_with_links",
        start_time_ns=start_time,
        end_time_ns=end_time,
        links=[
            Span.Link(
                trace_id=linked_trace_id,
                span_id=linked_span_id,
                attributes={"link.type": "parent_trace"},
            ),
        ],
    )

    result = _span_to_dict(span)

    assert "links" in result
    assert len(result["links"]) == 1

    link = result["links"][0]
    assert link["traceId"] == linked_trace_id
    assert link["spanId"] == linked_span_id
    assert "attributes" in link
    assert link["attributes"][0] == {
        "key": "link.type",
        "value": {"stringValue": "parent_trace"},
    }


def test_span_status_codes():
    """Test all span status codes are correctly mapped to OTLP format.

    Combines tests for OK, ERROR, and UNSET statuses into a single comprehensive test.
    """
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 1000000

    # Test ERROR status
    span_error = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="failed_operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
        status=Span.Status.ERROR,
    )
    result = _span_to_dict(span_error)
    assert "status" in result
    assert result["status"]["code"] == 2  # Span.Status.ERROR

    # Test OK status
    span_ok = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="successful_operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
        status=Span.Status.OK,
    )
    result = _span_to_dict(span_ok)
    assert "status" in result
    assert result["status"]["code"] == 1  # Span.Status.OK

    # Test UNSET status (should be omitted)
    span_unset = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
        status=Span.Status.UNSET,
    )
    result = _span_to_dict(span_unset)
    assert "status" not in result  # UNSET status should be omitted


def test_span_with_empty_events_and_links():
    """Test that empty events and links lists are omitted."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 1000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
        events=[],  # Empty list
        links=[],  # Empty list
    )

    result = _span_to_dict(span)

    # Empty lists should be omitted
    assert "events" not in result
    assert "links" not in result


def test_round_trip_json_serialization():
    """Round-trip test: span → to_dict → json.dumps → json.loads → verify structure."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    parent_span_id = new_span_id()
    start_time = now_ns()
    event_time = start_time + 500000
    end_time = start_time + 1000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name="complex_operation",
        kind=Span.Kind.SERVER,
        start_time_ns=start_time,
        end_time_ns=end_time,
        attributes={
            "service.name": "test_service",
            "http.method": "POST",
            "http.status_code": 201,
            "user.authenticated": True,
            "request.size": 1024.5,
            "tags": ["important", "production"],
            "metadata": {"version": "1.0", "region": "us-west"},
        },
        events=[
            Span.Event(
                name="processing_started",
                timestamp_ns=event_time,
                attributes={"processor": "main"},
            )
        ],
        status=Span.Status.OK,
    )

    # Convert to dict
    span_dict = _span_to_dict(span)

    # Serialize to JSON and back
    json_str = json.dumps(span_dict)
    parsed = json.loads(json_str)

    # Verify structure matches OTLP spec
    assert parsed["traceId"] == trace_id
    assert parsed["spanId"] == span_id
    assert parsed["parentSpanId"] == parent_span_id
    assert parsed["name"] == "complex_operation"
    assert parsed["kind"] == 2  # SERVER
    assert parsed["startTimeUnixNano"] == str(start_time)
    assert parsed["endTimeUnixNano"] == str(end_time)

    # Verify attributes
    assert "attributes" in parsed
    attrs_by_key = {attr["key"]: attr["value"] for attr in parsed["attributes"]}
    assert attrs_by_key["service.name"]["stringValue"] == "test_service"
    assert attrs_by_key["http.method"]["stringValue"] == "POST"
    assert attrs_by_key["http.status_code"]["intValue"] == "201"
    assert attrs_by_key["user.authenticated"]["boolValue"] is True
    assert attrs_by_key["request.size"]["doubleValue"] == 1024.5
    assert "arrayValue" in attrs_by_key["tags"]
    assert "kvlistValue" in attrs_by_key["metadata"]

    # Verify events
    assert len(parsed["events"]) == 1
    assert parsed["events"][0]["name"] == "processing_started"
    assert parsed["events"][0]["timeUnixNano"] == str(event_time)

    # Verify status
    assert parsed["status"]["code"] == 1  # OK

    # Ensure the JSON is valid and parseable
    assert isinstance(parsed, dict)
