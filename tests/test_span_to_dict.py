"""Tests for _span_to_dict() serialization."""

import json

from miniotel import (
    Event,
    Link,
    Span,
    SpanKind,
    SpanStatus,
    StatusCode,
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
        kind=SpanKind.CLIENT,
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
            Event(
                name="request_started",
                timestamp_ns=event_time1,
                attributes={"url": "https://example.com"},
            ),
            Event(
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
            Link(
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


def test_span_with_error_status():
    """Create span with ERROR status, verify status object is correct."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 1000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="failed_operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
        status=SpanStatus(code=StatusCode.ERROR, message="Connection timeout"),
    )

    result = _span_to_dict(span)

    assert "status" in result
    assert result["status"]["code"] == 2  # StatusCode.ERROR
    assert result["status"]["message"] == "Connection timeout"


def test_span_with_ok_status():
    """Test span with OK status."""
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()
    end_time = start_time + 1000000

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="successful_operation",
        start_time_ns=start_time,
        end_time_ns=end_time,
        status=SpanStatus(code=StatusCode.OK),
    )

    result = _span_to_dict(span)

    assert "status" in result
    assert result["status"]["code"] == 1  # StatusCode.OK
    assert result["status"]["message"] == ""


def test_span_with_unset_status_and_message():
    """Test that UNSET status with a message is included."""
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
        status=SpanStatus(code=StatusCode.UNSET, message="Some info"),
    )

    result = _span_to_dict(span)

    # UNSET with message should be included
    assert "status" in result
    assert result["status"]["code"] == 0
    assert result["status"]["message"] == "Some info"


def test_span_with_unset_status_no_message():
    """Test that UNSET status without message is omitted."""
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
        status=SpanStatus(code=StatusCode.UNSET),
    )

    result = _span_to_dict(span)

    # UNSET without message should be omitted
    assert "status" not in result


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
        kind=SpanKind.SERVER,
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
            Event(
                name="processing_started",
                timestamp_ns=event_time,
                attributes={"processor": "main"},
            )
        ],
        status=SpanStatus(code=StatusCode.OK),
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
    assert parsed["status"]["message"] == ""

    # Ensure the JSON is valid and parseable
    assert isinstance(parsed, dict)
