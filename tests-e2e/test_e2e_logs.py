"""E2E tests for send_logs against a real OpenTelemetry Collector."""

from __future__ import annotations

import sys
from pathlib import Path

# Add src to path so we can import picotel
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from conftest import read_collector_output

from picotel import (
    InstrumentationScope,
    LogRecord,
    Resource,
    new_span_id,
    new_trace_id,
    now_ns,
    send_logs,
)


def test_send_single_log(collector):
    """Send one log and verify it appears in the collector output."""
    resource = Resource(attributes={"service.name": "e2e-test-logs"})
    timestamp = now_ns()

    log = LogRecord(
        body="Test log message",
        timestamp_ns=timestamp,
        severity_number=LogRecord.Severity.INFO,
        severity_text="INFO",
    )

    result = send_logs(collector["endpoint"], resource, [log])
    assert result is True

    output = read_collector_output(collector["output_file"])
    assert len(output) == 1

    # Verify the log data
    resource_logs = output[0]["resourceLogs"]
    assert len(resource_logs) == 1

    scope_logs = resource_logs[0]["scopeLogs"]
    assert len(scope_logs) == 1

    log_records = scope_logs[0]["logRecords"]
    assert len(log_records) == 1
    assert log_records[0]["body"]["stringValue"] == "Test log message"
    assert log_records[0]["severityNumber"] == LogRecord.Severity.INFO
    assert log_records[0]["severityText"] == "INFO"


def test_send_log_with_attributes(collector):
    """Verify log attributes are preserved through the collector."""
    resource = Resource(attributes={"service.name": "e2e-test-logs"})
    timestamp = now_ns()

    log = LogRecord(
        body="Log with attributes",
        timestamp_ns=timestamp,
        severity_number=LogRecord.Severity.WARN,
        attributes={
            "user.id": "user-123",
            "request.count": 42,
            "feature.enabled": True,
            "latency.ratio": 0.85,
        },
    )

    result = send_logs(collector["endpoint"], resource, [log])
    assert result is True

    output = read_collector_output(collector["output_file"])
    log_records = output[0]["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
    attrs = {a["key"]: a["value"] for a in log_records[0]["attributes"]}

    assert attrs["user.id"]["stringValue"] == "user-123"
    assert attrs["request.count"]["intValue"] == "42"
    assert attrs["feature.enabled"]["boolValue"] is True
    assert attrs["latency.ratio"]["doubleValue"] == 0.85


def test_send_multiple_logs(collector):
    """Send a batch of logs and verify all appear in output."""
    resource = Resource(attributes={"service.name": "e2e-test-logs"})
    start = now_ns()

    logs = [
        LogRecord(
            body=f"Log message {i}",
            timestamp_ns=start + i * 1_000_000,
            severity_number=LogRecord.Severity.INFO,
        )
        for i in range(5)
    ]

    result = send_logs(collector["endpoint"], resource, logs)
    assert result is True

    output = read_collector_output(collector["output_file"])
    received_logs = output[0]["resourceLogs"][0]["scopeLogs"][0]["logRecords"]

    assert len(received_logs) == 5
    bodies = {r["body"]["stringValue"] for r in received_logs}
    assert bodies == {
        "Log message 0",
        "Log message 1",
        "Log message 2",
        "Log message 3",
        "Log message 4",
    }


def test_send_logs_with_scope(collector):
    """Verify instrumentation scope is included in output."""
    resource = Resource(attributes={"service.name": "e2e-test-logs"})
    scope = InstrumentationScope(
        name="my-logger",
        version="2.0.0",
        attributes={"logger.type": "structured"},
    )
    timestamp = now_ns()

    log = LogRecord(
        body="Scoped log message",
        timestamp_ns=timestamp,
        severity_number=LogRecord.Severity.DEBUG,
    )

    result = send_logs(collector["endpoint"], resource, [log], scope=scope)
    assert result is True

    output = read_collector_output(collector["output_file"])
    scope_logs = output[0]["resourceLogs"][0]["scopeLogs"][0]

    assert "scope" in scope_logs
    assert scope_logs["scope"]["name"] == "my-logger"
    assert scope_logs["scope"]["version"] == "2.0.0"


def test_send_log_with_severity_levels(collector):
    """Verify different severity levels are preserved."""
    resource = Resource(attributes={"service.name": "e2e-test-logs"})
    timestamp = now_ns()

    logs = [
        LogRecord(
            body="Trace message",
            timestamp_ns=timestamp,
            severity_number=LogRecord.Severity.TRACE,
            severity_text="TRACE",
        ),
        LogRecord(
            body="Error message",
            timestamp_ns=timestamp + 1_000_000,
            severity_number=LogRecord.Severity.ERROR,
            severity_text="ERROR",
        ),
        LogRecord(
            body="Fatal message",
            timestamp_ns=timestamp + 2_000_000,
            severity_number=LogRecord.Severity.FATAL,
            severity_text="FATAL",
        ),
    ]

    result = send_logs(collector["endpoint"], resource, logs)
    assert result is True

    output = read_collector_output(collector["output_file"])
    received_logs = output[0]["resourceLogs"][0]["scopeLogs"][0]["logRecords"]

    severities = {r["body"]["stringValue"]: r["severityNumber"] for r in received_logs}
    assert severities["Trace message"] == LogRecord.Severity.TRACE
    assert severities["Error message"] == LogRecord.Severity.ERROR
    assert severities["Fatal message"] == LogRecord.Severity.FATAL


def test_send_log_with_trace_correlation(collector):
    """Verify trace correlation (trace_id, span_id) is preserved."""
    resource = Resource(attributes={"service.name": "e2e-test-logs"})
    trace_id = new_trace_id()
    span_id = new_span_id()
    timestamp = now_ns()

    log = LogRecord(
        body="Correlated log message",
        timestamp_ns=timestamp,
        trace_id=trace_id,
        span_id=span_id,
        severity_number=LogRecord.Severity.INFO,
    )

    result = send_logs(collector["endpoint"], resource, [log])
    assert result is True

    output = read_collector_output(collector["output_file"])
    log_records = output[0]["resourceLogs"][0]["scopeLogs"][0]["logRecords"]

    assert len(log_records) == 1
    assert log_records[0]["traceId"] == trace_id
    assert log_records[0]["spanId"] == span_id
