"""Tests for _span_to_dict() serialization."""

from picotel import (
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
