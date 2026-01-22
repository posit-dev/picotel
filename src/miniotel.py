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
