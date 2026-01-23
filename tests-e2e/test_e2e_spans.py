"""E2E tests for send_spans against a real OpenTelemetry Collector."""

from __future__ import annotations

import sys
from pathlib import Path

# Add src to path so we can import miniotel
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from miniotel import (
    Event,
    InstrumentationScope,
    Resource,
    Span,
    SpanKind,
    SpanStatus,
    StatusCode,
    new_span_id,
    new_trace_id,
    now_ns,
    send_spans,
)

from conftest import read_collector_output


def test_send_single_span(collector):
    """Send one span and verify it appears in the collector output."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    trace_id = new_trace_id()
    span_id = new_span_id()
    start = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="test-span",
        start_time_ns=start,
        end_time_ns=start + 1_000_000,
        kind=SpanKind.INTERNAL,
    )

    result = send_spans(collector["endpoint"], resource, [span])
    assert result is True

    output = read_collector_output(collector["output_file"])
    assert len(output) == 1

    # Verify the span data
    resource_spans = output[0]["resourceSpans"]
    assert len(resource_spans) == 1

    scope_spans = resource_spans[0]["scopeSpans"]
    assert len(scope_spans) == 1

    spans = scope_spans[0]["spans"]
    assert len(spans) == 1
    assert spans[0]["traceId"] == trace_id
    assert spans[0]["spanId"] == span_id
    assert spans[0]["name"] == "test-span"


def test_send_span_with_attributes(collector):
    """Verify span attributes are preserved through the collector."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    trace_id = new_trace_id()
    span_id = new_span_id()
    start = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="attributed-span",
        start_time_ns=start,
        end_time_ns=start + 1_000_000,
        attributes={
            "http.method": "GET",
            "http.status_code": 200,
            "custom.flag": True,
            "custom.ratio": 0.95,
        },
    )

    result = send_spans(collector["endpoint"], resource, [span])
    assert result is True

    output = read_collector_output(collector["output_file"])
    spans = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    attrs = {a["key"]: a["value"] for a in spans[0]["attributes"]}

    assert attrs["http.method"]["stringValue"] == "GET"
    assert attrs["http.status_code"]["intValue"] == "200"
    assert attrs["custom.flag"]["boolValue"] is True
    assert attrs["custom.ratio"]["doubleValue"] == 0.95


def test_send_span_with_events(collector):
    """Verify span events are serialized correctly."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    trace_id = new_trace_id()
    span_id = new_span_id()
    start = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="span-with-events",
        start_time_ns=start,
        end_time_ns=start + 2_000_000,
        events=[
            Event(
                name="cache.miss",
                timestamp_ns=start + 500_000,
                attributes={"cache.key": "user:123"},
            ),
            Event(
                name="db.query",
                timestamp_ns=start + 1_000_000,
                attributes={"db.statement": "SELECT * FROM users"},
            ),
        ],
    )

    result = send_spans(collector["endpoint"], resource, [span])
    assert result is True

    output = read_collector_output(collector["output_file"])
    spans = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    events = spans[0]["events"]

    assert len(events) == 2
    assert events[0]["name"] == "cache.miss"
    assert events[1]["name"] == "db.query"


def test_send_multiple_spans(collector):
    """Send a batch of spans and verify all appear in output."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    trace_id = new_trace_id()
    start = now_ns()

    spans = [
        Span(
            trace_id=trace_id,
            span_id=new_span_id(),
            name=f"span-{i}",
            start_time_ns=start + i * 1_000_000,
            end_time_ns=start + (i + 1) * 1_000_000,
        )
        for i in range(5)
    ]

    result = send_spans(collector["endpoint"], resource, spans)
    assert result is True

    output = read_collector_output(collector["output_file"])
    received_spans = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]

    assert len(received_spans) == 5
    names = {s["name"] for s in received_spans}
    assert names == {"span-0", "span-1", "span-2", "span-3", "span-4"}


def test_send_spans_with_scope(collector):
    """Verify instrumentation scope is included in output."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    scope = InstrumentationScope(
        name="my-instrumentation",
        version="1.2.3",
        attributes={"scope.attr": "value"},
    )
    trace_id = new_trace_id()
    start = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        name="scoped-span",
        start_time_ns=start,
        end_time_ns=start + 1_000_000,
    )

    result = send_spans(collector["endpoint"], resource, [span], scope=scope)
    assert result is True

    output = read_collector_output(collector["output_file"])
    scope_spans = output[0]["resourceSpans"][0]["scopeSpans"][0]

    assert "scope" in scope_spans
    assert scope_spans["scope"]["name"] == "my-instrumentation"
    assert scope_spans["scope"]["version"] == "1.2.3"


def test_send_span_with_status(collector):
    """Verify span status is preserved."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    trace_id = new_trace_id()
    start = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        name="error-span",
        start_time_ns=start,
        end_time_ns=start + 1_000_000,
        status=SpanStatus(code=StatusCode.ERROR, message="Something went wrong"),
    )

    result = send_spans(collector["endpoint"], resource, [span])
    assert result is True

    output = read_collector_output(collector["output_file"])
    spans = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]

    assert "status" in spans[0]
    assert spans[0]["status"]["code"] == 2  # ERROR
    assert spans[0]["status"]["message"] == "Something went wrong"


def test_send_span_with_parent(collector):
    """Verify parent-child relationship is preserved."""
    resource = Resource(attributes={"service.name": "e2e-test"})
    trace_id = new_trace_id()
    parent_span_id = new_span_id()
    child_span_id = new_span_id()
    start = now_ns()

    parent_span = Span(
        trace_id=trace_id,
        span_id=parent_span_id,
        name="parent-span",
        start_time_ns=start,
        end_time_ns=start + 2_000_000,
        kind=SpanKind.SERVER,
    )

    child_span = Span(
        trace_id=trace_id,
        span_id=child_span_id,
        parent_span_id=parent_span_id,
        name="child-span",
        start_time_ns=start + 500_000,
        end_time_ns=start + 1_500_000,
        kind=SpanKind.INTERNAL,
    )

    result = send_spans(collector["endpoint"], resource, [parent_span, child_span])
    assert result is True

    output = read_collector_output(collector["output_file"])
    spans = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]

    # Find parent and child spans
    spans_by_id = {s["spanId"]: s for s in spans}
    assert parent_span_id in spans_by_id
    assert child_span_id in spans_by_id

    # Verify parent-child relationship
    # Note: The OTLP spec says parentSpanId is optional, but otelcol's file exporter
    # always includes it (as empty string for root spans).
    assert spans_by_id[parent_span_id]["parentSpanId"] == ""
    assert spans_by_id[child_span_id]["parentSpanId"] == parent_span_id
