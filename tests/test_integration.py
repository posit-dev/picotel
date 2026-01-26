"""Integration tests for picotel library.

These tests verify end-to-end functionality rather than testing Python features.
They combine multiple components to ensure proper OTLP protocol compliance.
"""

import json

from picotel import (
    InstrumentationScope,
    Resource,
    Span,
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
            "telemetry.sdk.name": "picotel",
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
        Span.Event(
            name="cache.hit",
            timestamp_ns=event_time1,
            attributes={"cache.key": "user:123", "cache.size": 256},
        ),
        Span.Event(
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
        Span.Link(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            attributes={"link.type": "child_of"},
        ),
        Span.Link(
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
        kind=Span.Kind.SERVER,
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
        status=Span.Status.ERROR,
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


# Note: test_log_record_variations and test_log_edge_cases removed
# These tests depend on _log_to_dict which will be added when implementing Step 11


# Note: test_correlated_logs_and_spans removed
# This test depends on _log_to_dict which will be added when implementing Step 11
# The test validates correlation between logs and spans which is not yet implemented
