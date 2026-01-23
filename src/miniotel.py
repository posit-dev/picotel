# Copyright (C) 2025 by Posit Software, PBC.

"""miniotel is a minimal, single-file OpenTelemetry client for Python.

It sends spans and logs over HTTP/JSON to any OTLP-compatible collector
(Jaeger, Grafana Tempo, OTEL Collector, etc.) with zero external dependencies.

This makes miniotel ideal for vendoring alongside software that needs
basic observability without pulling in the full OpenTelemetry SDK.

Requires Python 3.8+ for:
- time.time_ns() for nanosecond timestamps
- dataclasses for clean data structures
- from __future__ import annotations for type hint syntax

Version: 0.1.0
Author: Alessandro Molina <alessandro.molina@posit.co>
URL: https://github.com/posit-dev/miniotel
License: MIT
"""

from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class SpanKind(IntEnum):
    """The type of span, indicating its role in a distributed trace."""

    UNSPECIFIED = 0
    INTERNAL = 1
    SERVER = 2
    CLIENT = 3
    PRODUCER = 4
    CONSUMER = 5


class StatusCode(IntEnum):
    """The status of a completed span: unset, ok, or error."""

    UNSET = 0
    OK = 1
    ERROR = 2


class Severity(IntEnum):
    """Log severity levels following OpenTelemetry severity number ranges."""

    TRACE = 1
    DEBUG = 5
    INFO = 9
    WARN = 13
    ERROR = 17
    FATAL = 21


def new_trace_id() -> str:
    """Generate a random 16-byte trace ID as a 32-character lowercase hex string."""
    return os.urandom(16).hex()


def new_span_id() -> str:
    """Generate a random 8-byte span ID as a 16-character lowercase hex string."""
    return os.urandom(8).hex()


def now_ns() -> int:
    """Return the current time in nanoseconds since Unix epoch."""
    return time.time_ns()


@dataclass
class Resource:
    """Resource holds attributes that describe the entity producing telemetry.

    Common attributes include service.name, service.version, and deployment.environment.
    See the OpenTelemetry semantic conventions for standard attribute names.
    """

    attributes: dict[str, Any]


@dataclass
class InstrumentationScope:
    """InstrumentationScope identifies the library that produced the telemetry.

    This is typically the name and version of the instrumentation library,
    not the application being instrumented.
    """

    name: str
    version: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Event:
    """An event represents a notable occurrence during a span's lifetime."""

    name: str
    timestamp_ns: int
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Link:
    """A link associates a span with another span in the same or different trace."""

    trace_id: str
    span_id: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class SpanStatus:
    """The status of a completed span indicating success or error."""

    code: StatusCode = StatusCode.UNSET
    message: str = ""


@dataclass
class Span:
    """A span represents a single operation within a trace.

    Spans can be nested to form a tree structure representing the call hierarchy.
    A root span has no parent_span_id; child spans reference their parent.
    """

    trace_id: str
    span_id: str
    name: str
    start_time_ns: int
    end_time_ns: int
    parent_span_id: str = ""
    kind: SpanKind = SpanKind.INTERNAL
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[Event] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)
    status: SpanStatus | None = None


@dataclass
class LogRecord:
    """A log record represents a single log entry with optional trace correlation.

    Logs can be correlated with traces by setting trace_id and span_id.
    Timestamps default to 0, which means "use current time when sending".
    """

    body: Any
    timestamp_ns: int = 0
    observed_timestamp_ns: int = 0
    trace_id: str = ""
    span_id: str = ""
    trace_flags: int = 0
    severity_number: int = Severity.INFO
    severity_text: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)


def _to_otlp_value(value: Any) -> dict[str, Any]:  # noqa: ANN401, PLR0911
    """Convert a Python value to the typed OTLP attribute format.

    Returns a dict with a single key indicating the type and the value.

    Examples:
        "hello" -> {"stringValue": "hello"}
        42 -> {"intValue": "42"}
        True -> {"boolValue": True}
        3.14 -> {"doubleValue": 3.14}
        None -> {}

    """
    if value is None:
        return {}
    # Check bool before int since bool is a subclass of int
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        # Store as string to avoid JSON precision loss for large integers
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, bytes):
        return {"bytesValue": base64.b64encode(value).decode()}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_to_otlp_value(x) for x in value]}}
    if isinstance(value, dict):
        return {
            "kvlistValue": {
                "values": [
                    {"key": k, "value": _to_otlp_value(v)} for k, v in value.items()
                ]
            }
        }
    # Fallback to string representation for unknown types
    return {"stringValue": str(value)}


def _attributes_to_otlp(attributes: dict[str, Any]) -> list[dict[str, Any]]:
    """Return an OTLP attribute list from a Python dict.

    Converts each key-value pair to {"key": "...", "value": {...}} format,
    where value is converted using _to_otlp_value(). Skips None values.

    :param attributes: Dictionary of attribute key-value pairs

    Examples::

        {"foo": "bar", "count": 5} -> [
            {"key": "foo", "value": {"stringValue": "bar"}},
            {"key": "count", "value": {"intValue": "5"}}
        ]
        {"a": None} -> []  # None values are skipped

    """
    return [
        {"key": k, "value": _to_otlp_value(v)}
        for k, v in attributes.items()
        if v is not None
    ]


def _span_to_dict(span: Span) -> dict[str, Any]:
    """Return the OTLP JSON dict representation of a Span.

    Builds a span dict following the OpenTelemetry Protocol specification.
    Optional fields are omitted when empty to minimize payload size.

    :param span: The Span object to serialize

    """
    # Required fields - always present
    result = {
        "traceId": span.trace_id,
        "spanId": span.span_id,
        "name": span.name,
        "kind": int(span.kind),
        "startTimeUnixNano": str(span.start_time_ns),
        "endTimeUnixNano": str(span.end_time_ns),
    }

    # Optional fields - omit if empty/default
    if span.parent_span_id:
        result["parentSpanId"] = span.parent_span_id

    attrs = _attributes_to_otlp(span.attributes)
    if attrs:
        result["attributes"] = attrs

    if span.events:
        result["events"] = [
            {
                "name": event.name,
                "timeUnixNano": str(event.timestamp_ns),
                **(
                    {"attributes": _attributes_to_otlp(event.attributes)}
                    if event.attributes
                    else {}
                ),
            }
            for event in span.events
        ]

    if span.links:
        result["links"] = [
            {
                "traceId": link.trace_id,
                "spanId": link.span_id,
                **(
                    {"attributes": _attributes_to_otlp(link.attributes)}
                    if link.attributes
                    else {}
                ),
            }
            for link in span.links
        ]

    # Include status only if it's not UNSET or has a message
    if span.status and (span.status.code != StatusCode.UNSET or span.status.message):
        result["status"] = {
            "code": int(span.status.code),
            "message": span.status.message,
        }

    return result


def _log_to_dict(log: LogRecord) -> dict[str, Any]:
    """Return the OTLP JSON dict representation of a LogRecord.

    :param log: The LogRecord object to serialize

    """
    # Use current time for timestamps if not provided (0 means "now")
    result = {
        "timeUnixNano": str(log.timestamp_ns or now_ns()),
        "observedTimeUnixNano": str(log.observed_timestamp_ns or now_ns()),
        "severityNumber": log.severity_number,
        "body": _to_otlp_value(log.body),
    }

    # Optional fields - omit if empty/default
    if log.severity_text:
        result["severityText"] = log.severity_text

    attrs = _attributes_to_otlp(log.attributes)
    if attrs:
        result["attributes"] = attrs

    if log.trace_id:
        result["traceId"] = log.trace_id

    if log.span_id:
        result["spanId"] = log.span_id

    if log.trace_flags:
        result["flags"] = log.trace_flags

    return result
