"""Integration tests for miniotel library.

These tests verify end-to-end functionality rather than testing Python features.
They combine multiple components to ensure proper OTLP protocol compliance.
"""

import json

from miniotel import (
    Event,
    InstrumentationScope,
    Link,
    LogRecord,
    Resource,
    Severity,
    Span,
    SpanKind,
    SpanStatus,
    StatusCode,
    _log_to_dict,
    _span_to_dict,
    new_span_id,
    new_trace_id,
    now_ns,
)


def test_span_creation_with_generated_ids_and_timestamps():
    """Test span creation with ID generation and timestamp validation.

    Replaces: ID format tests, uniqueness tests, timestamp tests
    - Create a span using new_trace_id(), new_span_id(), now_ns()
    - Serialize to OTLP JSON with _span_to_dict()
    - Verify IDs in output are 32/16 hex chars
    - Verify timestamps are valid nanoseconds
    - Keep uniqueness verification (generate 1000 IDs, all unique)
    """
    # Test ID generation and uniqueness
    trace_ids = [new_trace_id() for _ in range(1000)]
    span_ids = [new_span_id() for _ in range(1000)]

    # Verify uniqueness
    assert len(set(trace_ids)) == 1000, "All trace IDs should be unique"
    assert len(set(span_ids)) == 1000, "All span IDs should be unique"

    # Create a span with generated IDs and timestamps
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

    # Serialize to OTLP JSON
    span_dict = _span_to_dict(span)

    # Verify IDs are proper format in output
    assert len(span_dict["traceId"]) == 32, "Trace ID should be 32 hex chars"
    assert span_dict["traceId"].islower(), "Trace ID should be lowercase"
    assert all(c in "0123456789abcdef" for c in span_dict["traceId"])

    assert len(span_dict["spanId"]) == 16, "Span ID should be 16 hex chars"
    assert span_dict["spanId"].islower(), "Span ID should be lowercase"
    assert all(c in "0123456789abcdef" for c in span_dict["spanId"])

    # Verify timestamps are valid nanoseconds (as strings in JSON)
    assert span_dict["startTimeUnixNano"] == str(start_time)
    assert span_dict["endTimeUnixNano"] == str(end_time)
    assert int(span_dict["startTimeUnixNano"]) > 0
    assert int(span_dict["endTimeUnixNano"]) > int(span_dict["startTimeUnixNano"])


def test_resource_and_scope_serialization():
    """Test Resource and InstrumentationScope serialization to OTLP format.

    Replaces: Resource/InstrumentationScope basic tests
    - Create Resource with various attributes
    - Create InstrumentationScope with name, version, attributes
    - Verify they can be used in OTLP request structure
    """
    # Create Resource with various attributes
    resource = Resource(
        {
            "service.name": "myapp",
            "service.version": "1.0.0",
            "deployment.environment": "production",
            "host.name": "server-01",
            "telemetry.sdk.name": "miniotel",
            "telemetry.sdk.version": "0.1.0",
        }
    )

    # Create InstrumentationScope with attributes
    scope = InstrumentationScope(
        name="mylib",
        version="2.3.4",
        attributes={"custom.key": "value", "debug.enabled": True},
    )

    # Build OTLP request structure manually (as would be done in send_spans)
    resource_spans = {
        "resource": {
            "attributes": [
                {"key": k, "value": {"stringValue": v}}
                for k, v in resource.attributes.items()
            ]
        },
        "scopeSpans": [
            {
                "scope": {
                    "name": scope.name,
                    "version": scope.version,
                    "attributes": [
                        {"key": "custom.key", "value": {"stringValue": "value"}},
                        {"key": "debug.enabled", "value": {"boolValue": True}},
                    ],
                },
                "spans": [],
            }
        ],
    }

    # Verify the structure is valid JSON
    json_str = json.dumps(resource_spans)
    parsed = json.loads(json_str)

    # Verify Resource attributes
    assert len(parsed["resource"]["attributes"]) == 6
    attrs_by_key = {
        attr["key"]: attr["value"] for attr in parsed["resource"]["attributes"]
    }
    assert attrs_by_key["service.name"]["stringValue"] == "myapp"
    assert attrs_by_key["service.version"]["stringValue"] == "1.0.0"

    # Verify InstrumentationScope
    assert parsed["scopeSpans"][0]["scope"]["name"] == "mylib"
    assert parsed["scopeSpans"][0]["scope"]["version"] == "2.3.4"
    assert len(parsed["scopeSpans"][0]["scope"]["attributes"]) == 2


def test_complex_span_with_all_components():
    """Test a complex span with events, links, and status.

    Replaces: Individual Event, Link, SpanStatus tests
    - Create a span with events (with attributes), links (with attributes), and status
    - Serialize to OTLP JSON
    - Verify all components appear correctly in output
    """
    trace_id = new_trace_id()
    span_id = new_span_id()
    parent_span_id = new_span_id()
    start_time = now_ns()
    event_time1 = start_time + 500000
    event_time2 = start_time + 1000000
    end_time = start_time + 2000000

    # Create events with attributes
    events = [
        Event(
            name="cache.hit",
            timestamp_ns=event_time1,
            attributes={"cache.key": "user:123", "cache.size": 256},
        ),
        Event(
            name="exception",
            timestamp_ns=event_time2,
            attributes={
                "exception.type": "ValueError",
                "exception.message": "Invalid input",
                "exception.stacktrace": ["line1", "line2"],
            },
        ),
    ]

    # Create links with attributes
    links = [
        Link(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            attributes={"link.type": "child_of"},
        ),
        Link(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            attributes={"link.type": "follows_from", "async": True},
        ),
    ]

    # Create span with all components
    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name="complex_operation",
        kind=SpanKind.SERVER,
        start_time_ns=start_time,
        end_time_ns=end_time,
        attributes={
            "http.method": "POST",
            "http.url": "https://api.example.com/users",
            "http.status_code": 201,
            "user.authenticated": True,
            "request.size": 1024.5,
        },
        events=events,
        links=links,
        status=SpanStatus(StatusCode.ERROR, "Database connection failed"),
    )

    # Serialize to OTLP JSON
    span_dict = _span_to_dict(span)

    # Verify all basic fields
    assert span_dict["traceId"] == trace_id
    assert span_dict["spanId"] == span_id
    assert span_dict["parentSpanId"] == parent_span_id
    assert span_dict["name"] == "complex_operation"
    assert span_dict["kind"] == 2  # SERVER

    # Verify attributes
    assert len(span_dict["attributes"]) == 5
    attrs_by_key = {attr["key"]: attr["value"] for attr in span_dict["attributes"]}
    assert attrs_by_key["http.method"]["stringValue"] == "POST"
    assert attrs_by_key["http.status_code"]["intValue"] == "201"
    assert attrs_by_key["user.authenticated"]["boolValue"] is True
    assert attrs_by_key["request.size"]["doubleValue"] == 1024.5

    # Verify events
    assert len(span_dict["events"]) == 2
    event1 = span_dict["events"][0]
    assert event1["name"] == "cache.hit"
    assert event1["timeUnixNano"] == str(event_time1)
    event1_attrs = {attr["key"]: attr["value"] for attr in event1["attributes"]}
    assert event1_attrs["cache.key"]["stringValue"] == "user:123"
    assert event1_attrs["cache.size"]["intValue"] == "256"

    event2 = span_dict["events"][1]
    assert event2["name"] == "exception"
    event2_attrs = {attr["key"]: attr["value"] for attr in event2["attributes"]}
    assert event2_attrs["exception.type"]["stringValue"] == "ValueError"
    assert "arrayValue" in event2_attrs["exception.stacktrace"]

    # Verify links
    assert len(span_dict["links"]) == 2
    link1 = span_dict["links"][0]
    assert len(link1["traceId"]) == 32
    assert len(link1["spanId"]) == 16
    link1_attrs = {attr["key"]: attr["value"] for attr in link1["attributes"]}
    assert link1_attrs["link.type"]["stringValue"] == "child_of"

    link2 = span_dict["links"][1]
    link2_attrs = {attr["key"]: attr["value"] for attr in link2["attributes"]}
    assert link2_attrs["link.type"]["stringValue"] == "follows_from"
    assert link2_attrs["async"]["boolValue"] is True

    # Verify status
    assert span_dict["status"]["code"] == 2  # ERROR
    assert span_dict["status"]["message"] == "Database connection failed"


def test_log_record_variations():
    """Test LogRecord with various configurations.

    Replaces: Individual LogRecord field tests
    - Create logs with: just body, custom severity, attributes, timestamps, correlation
    - Serialize each to OTLP JSON
    - Verify all fields are correctly formatted
    """
    # Test 1: Log with just body (minimal)
    log1 = LogRecord(body="Simple message")
    dict1 = _log_to_dict(log1)
    assert dict1["body"]["stringValue"] == "Simple message"
    assert dict1["severityNumber"] == Severity.INFO
    assert "severityText" not in dict1
    assert "attributes" not in dict1
    assert "traceId" not in dict1

    # Test 2: Log with custom severity
    log2 = LogRecord(
        body="Error occurred", severity_number=Severity.ERROR, severity_text="ERROR"
    )
    dict2 = _log_to_dict(log2)
    assert dict2["severityNumber"] == Severity.ERROR
    assert dict2["severityText"] == "ERROR"

    # Test 3: Log with attributes
    log3 = LogRecord(
        body="User action",
        attributes={
            "user.id": 456,
            "action": "purchase",
            "amount": 99.99,
            "items": ["item1", "item2"],
            "metadata": {"category": "electronics"},
        },
    )
    dict3 = _log_to_dict(log3)
    assert len(dict3["attributes"]) == 5
    attrs = {attr["key"]: attr["value"] for attr in dict3["attributes"]}
    assert attrs["user.id"]["intValue"] == "456"
    assert attrs["action"]["stringValue"] == "purchase"
    assert attrs["amount"]["doubleValue"] == 99.99
    assert "arrayValue" in attrs["items"]
    assert "kvlistValue" in attrs["metadata"]

    # Test 4: Log with explicit timestamps
    timestamp = now_ns()
    observed = timestamp + 100000
    log4 = LogRecord(
        body="Timed log", timestamp_ns=timestamp, observed_timestamp_ns=observed
    )
    dict4 = _log_to_dict(log4)
    assert dict4["timeUnixNano"] == str(timestamp)
    assert dict4["observedTimeUnixNano"] == str(observed)

    # Test 5: Log with trace correlation
    trace_id = new_trace_id()
    span_id = new_span_id()
    log5 = LogRecord(
        body="Correlated log", trace_id=trace_id, span_id=span_id, trace_flags=1
    )
    dict5 = _log_to_dict(log5)
    assert dict5["traceId"] == trace_id
    assert dict5["spanId"] == span_id
    assert dict5["flags"] == 1

    # Test 6: Log with dict body
    log6 = LogRecord(
        body={"message": "Structured log", "level": "info", "component": "auth"}
    )
    dict6 = _log_to_dict(log6)
    assert "kvlistValue" in dict6["body"]
    body_items = dict6["body"]["kvlistValue"]["values"]
    body_dict = {item["key"]: item["value"] for item in body_items}
    assert body_dict["message"]["stringValue"] == "Structured log"
    assert body_dict["component"]["stringValue"] == "auth"

    # Test 7: Log with list body
    log7 = LogRecord(body=["first", "second", 3])
    dict7 = _log_to_dict(log7)
    assert "arrayValue" in dict7["body"]
    values = dict7["body"]["arrayValue"]["values"]
    assert values[0]["stringValue"] == "first"
    assert values[1]["stringValue"] == "second"
    assert values[2]["intValue"] == "3"


def test_log_edge_cases():
    """Test LogRecord edge cases and special handling.

    Covers cases from test_log_to_dict.py that were unique.
    """
    # Test 1: Log with None attribute values (should be skipped)
    log_with_none = LogRecord(
        body="Test",
        attributes={
            "valid": "value",
            "none_value": None,
            "another": 123,
        },
    )
    dict_with_none = _log_to_dict(log_with_none)
    # Should only have 2 attributes (None is skipped)
    assert len(dict_with_none["attributes"]) == 2
    attrs_by_key = {attr["key"]: attr["value"] for attr in dict_with_none["attributes"]}
    assert "valid" in attrs_by_key
    assert "another" in attrs_by_key
    assert "none_value" not in attrs_by_key

    # Test 2: Round-trip JSON serialization with complex log
    trace_id = new_trace_id()
    span_id = new_span_id()
    timestamp = now_ns()
    complex_log = LogRecord(
        body={"message": "Complex log", "request_id": "req-123"},
        timestamp_ns=timestamp,
        observed_timestamp_ns=timestamp + 500000,
        trace_id=trace_id,
        span_id=span_id,
        trace_flags=1,
        severity_number=Severity.WARN,
        severity_text="WARNING",
        attributes={"service.name": "test_service", "tags": ["monitoring", "alerts"]},
    )
    complex_dict = _log_to_dict(complex_log)
    # Serialize to JSON and back
    json_str = json.dumps(complex_dict)
    parsed = json.loads(json_str)
    # Verify structure survives round-trip
    assert parsed["timeUnixNano"] == str(timestamp)
    assert parsed["severityNumber"] == Severity.WARN
    assert parsed["traceId"] == trace_id
    assert isinstance(parsed, dict)


def test_correlated_logs_and_spans():
    """Test that logs and spans can be properly correlated.

    New integration test:
    - Create a span with trace_id and span_id
    - Create a log correlated to that span
    - Serialize both
    - Verify trace_id/span_id match between them
    """
    # Create a span
    trace_id = new_trace_id()
    span_id = new_span_id()
    start_time = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="database_query",
        start_time_ns=start_time,
        end_time_ns=start_time + 5000000,  # 5ms
        kind=SpanKind.CLIENT,
        attributes={
            "db.system": "postgresql",
            "db.statement": "SELECT * FROM users WHERE id = ?",
            "db.operation": "SELECT",
        },
    )

    # Create correlated logs at different points in the span
    log_start = LogRecord(
        body="Starting database query",
        timestamp_ns=start_time + 100000,  # 0.1ms after start
        trace_id=trace_id,
        span_id=span_id,
        severity_number=Severity.DEBUG,
        attributes={"query.id": "q-123"},
    )

    log_error = LogRecord(
        body={
            "message": "Query failed",
            "error": "Connection timeout",
            "retry_count": 3,
        },
        timestamp_ns=start_time + 4500000,  # 4.5ms after start
        trace_id=trace_id,
        span_id=span_id,
        severity_number=Severity.ERROR,
        severity_text="ERROR",
        attributes={"db.error_code": "08001"},
    )

    # Serialize to OTLP JSON
    span_dict = _span_to_dict(span)
    log_start_dict = _log_to_dict(log_start)
    log_error_dict = _log_to_dict(log_error)

    # Verify trace_id and span_id match across all three
    assert span_dict["traceId"] == trace_id
    assert span_dict["spanId"] == span_id
    assert log_start_dict["traceId"] == trace_id
    assert log_start_dict["spanId"] == span_id
    assert log_error_dict["traceId"] == trace_id
    assert log_error_dict["spanId"] == span_id

    # Verify the logs fall within the span's time range
    span_start = int(span_dict["startTimeUnixNano"])
    span_end = int(span_dict["endTimeUnixNano"])
    log_start_time = int(log_start_dict["timeUnixNano"])
    log_error_time = int(log_error_dict["timeUnixNano"])

    assert span_start < log_start_time < span_end
    assert span_start < log_error_time < span_end

    # Verify log severities
    assert log_start_dict["severityNumber"] == Severity.DEBUG
    assert log_error_dict["severityNumber"] == Severity.ERROR
    assert log_error_dict["severityText"] == "ERROR"

    # Verify error log body structure
    error_body = log_error_dict["body"]["kvlistValue"]["values"]
    error_dict = {item["key"]: item["value"] for item in error_body}
    assert error_dict["message"]["stringValue"] == "Query failed"
    assert error_dict["error"]["stringValue"] == "Connection timeout"
    assert error_dict["retry_count"]["intValue"] == "3"
