# Copyright (C) 2026 by Posit Software, PBC.

"""picotel is a minimal, single-file OpenTelemetry client for Python.

It sends spans and logs over HTTP/JSON to any OTLP-compatible collector
(Jaeger, Grafana Tempo, OTEL Collector, etc.) with zero external dependencies.

This makes picotel ideal for vendoring alongside software that needs
basic observability without pulling in the full OpenTelemetry SDK.

Requires Python 3.8+ for:
- time.time_ns() for nanosecond timestamps
- dataclasses for clean data structures
- from __future__ import annotations for type hint syntax

Version: 0.1.2
Author: Alessandro Molina <alessandro.molina@posit.co>
URL: https://github.com/posit-dev/picotel
License: MIT
"""

from __future__ import annotations

import base64
import functools
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

_logger = logging.getLogger("picotel")

# Sentinel to read trace_id/parent_span_id from TRACEPARENT env var (W3C Trace Context)
TRACEPARENT = object()


def new_trace_id() -> str:
    """Generate a random 16-byte trace ID as a 32-character lowercase hex string."""
    return os.urandom(16).hex()


def new_span_id() -> str:
    """Generate a random 8-byte span ID as a 16-character lowercase hex string."""
    return os.urandom(8).hex()


def now_ns() -> int:
    """Return the current time in nanoseconds since Unix epoch.

    It should be preferred over time.time() for higher precision timestamps.
    Otherwise equivalent to int(time.time() * 1_000_000_000).
    """
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


class PicotelConfigError(Exception):
    """Raised when picotel is misconfigured (e.g., no endpoint and not disabled)."""


@dataclass
class Span:
    """A span represents a single operation within a trace.

    Spans can be nested to form a tree structure representing the call hierarchy.
    A root span has no parent_span_id; child spans reference their parent.

    Can be used as a context manager to automatically set times and send the span.
    When using as context manager, start_time_ns and end_time_ns can be 0 and will
    be set automatically::

        with Span(
            trace_id=new_trace_id(),
            name="process_request",
            start_time_ns=0,
            end_time_ns=0,
            endpoint="http://localhost:4318",
            resource=Resource({"service.name": "myapp"}),
        ) as span:
            # do work
            span.attributes["status"] = "success"
        # span is automatically sent on exit

    To continue a trace from environment, pass the TRACEPARENT sentinel::

        span = Span(trace_id=TRACEPARENT, name="child-op", ...)

    When PICOTEL_PREFIX is set (e.g. "PICOTEL"), the prefixed TRACEPARENT
    variable is read instead of the standard one, avoiding interference
    with user-configured OTel propagation.
    """

    class Kind(IntEnum):
        """The type of span, indicating its role in a distributed trace."""

        UNSPECIFIED = 0
        INTERNAL = 1
        SERVER = 2
        CLIENT = 3
        PRODUCER = 4
        CONSUMER = 5

    class Status(IntEnum):
        """The status of a completed span: unset, ok, or error.

        Note: Status message is not currently supported.
        """

        UNSET = 0
        OK = 1
        ERROR = 2

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

    # Required fields (no defaults)
    trace_id: str | object  # Can be TRACEPARENT sentinel
    name: str
    # Optional fields (with defaults) - times default to None for context manager usage
    start_time_ns: int | None = None
    end_time_ns: int | None = None
    span_id: str = field(default_factory=new_span_id)
    parent_span_id: str = ""
    kind: Kind = Kind.INTERNAL
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[Event] = field(default_factory=list)
    links: list[Link] = field(default_factory=list)
    status: Status | None = None
    # Context manager fields - not serialized to OTLP
    endpoint: str = ""
    resource: Resource | None = None
    scope: InstrumentationScope | None = None

    def __post_init__(self) -> None:
        """Handle TRACEPARENT sentinel for trace_id and parent_span_id."""
        if _is_disabled():
            return
        if self.trace_id is TRACEPARENT:
            traceparent = _parse_traceparent()
            if traceparent is None:
                _logger.error("TRACEPARENT requested but env var not set or invalid")
                self.trace_id = ""
                return
            self.trace_id = traceparent[0]
            if not self.parent_span_id:
                self.parent_span_id = traceparent[1]

    def __enter__(self) -> Span:
        """Enter the context manager, setting start_time_ns if not already set."""
        if self.start_time_ns is None:
            self.start_time_ns = now_ns()
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> None:  # noqa: ANN401
        """Exit the context manager, setting end_time_ns and sending the span."""
        if self.end_time_ns is None:
            self.end_time_ns = now_ns()
        # Try to send if we have resource (send_spans handles endpoint/disabled)
        endpoint = self.endpoint or None
        resource = self.resource or _get_resource_from_env()
        if resource:
            send_spans(endpoint, resource, [self], self.scope)

    def _validate(self) -> None:
        """Validate span has required fields set properly."""
        if not self.trace_id:
            raise PicotelConfigError("Span invalid: trace_id is empty")
        if self.start_time_ns is None:
            raise PicotelConfigError("Span invalid: start_time_ns is not set")
        if self.end_time_ns is None:
            raise PicotelConfigError("Span invalid: end_time_ns is not set")

    def send(
        self,
        endpoint: str | None = None,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
        timeout: float = 2.0,
    ) -> bool:
        """Send this span to an OTLP collector over HTTP.

        :param endpoint: OTLP collector URL. If None, uses env vars
        :param resource: Resource attributes. If None, uses env vars
        :param scope: Optional instrumentation scope metadata
        :param timeout: HTTP request timeout in seconds (default 2.0)
        """
        # Use environment variables if not provided
        if endpoint is None:
            endpoint = _get_endpoint("traces")
        if resource is None:
            resource = _get_resource_from_env()

        if endpoint is None or resource is None:
            _logger.warning("span not sent, missing endpoint or resource")
            return False

        return send_spans(endpoint, resource, [self], scope, timeout)


@dataclass
class LogRecord:
    """A log record represents a single log entry with optional trace correlation.

    Logs can be correlated with traces by setting trace_id and span_id.
    Timestamps default to 0, which means "use current time when sending".
    """

    class Severity(IntEnum):
        """Log severity levels following OpenTelemetry severity number ranges."""

        TRACE = 1
        DEBUG = 5
        INFO = 9
        WARN = 13
        ERROR = 17
        FATAL = 21

    body: Any
    timestamp_ns: int = 0
    observed_timestamp_ns: int = 0
    trace_id: str | object = ""  # Can be TRACEPARENT sentinel
    span_id: str = ""
    trace_flags: int = 0
    severity_number: int = Severity.INFO
    severity_text: str = ""
    attributes: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Handle TRACEPARENT sentinel for trace_id and span_id."""
        if _is_disabled():
            return
        if self.trace_id is TRACEPARENT:
            traceparent = _parse_traceparent()
            if traceparent is None:
                _logger.error("TRACEPARENT requested but env var not set or invalid")
                self.trace_id = ""
                return
            self.trace_id = traceparent[0]
            if not self.span_id:
                self.span_id = traceparent[1]

    def send(
        self,
        endpoint: str | None = None,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
        timeout: float = 2.0,
    ) -> bool:
        """Send this log record to an OTLP collector over HTTP.

        :param endpoint: OTLP collector URL. If None, uses env vars
        :param resource: Resource attributes. If None, uses env vars
        :param scope: Optional instrumentation scope metadata
        :param timeout: HTTP request timeout in seconds (default 2.0)
        """
        # Use environment variables if not provided
        if endpoint is None:
            endpoint = _get_endpoint("logs")
        if resource is None:
            resource = _get_resource_from_env()

        if endpoint is None or resource is None:
            _logger.warning("log not sent, missing endpoint or resource")
            return False

        return send_logs(endpoint, resource, [self], scope, timeout)


class OTLPHandler(logging.Handler):
    """Python logging handler that sends logs to an OTLP collector.

    Integrates with Python's standard logging module to automatically export
    logs to an OpenTelemetry collector. Logs are sent immediately (no batching).

    Example usage::

        import logging
        from picotel import OTLPHandler, Resource

        # Configure the handler
        handler = OTLPHandler(
            endpoint="http://localhost:4318",
            resource=Resource({"service.name": "myapp", "service.version": "1.0.0"}),
        )

        # Add to root logger
        logging.getLogger().addHandler(handler)

        # Normal logging now goes to OTLP
        logging.info("Server started on port 8080")

        # With trace correlation via extra dict
        logging.error(
            "Database connection failed",
            extra={"trace_id": trace_id, "span_id": span_id},
        )

        # Or set handler-level defaults that apply to every log record.
        # Per-record extra values override handler defaults for keys in EXTRA_KEYS.
        handler.extra = {
            "trace_id": trace_id,
            "span_id": span_id,
            "attributes": {"worker.id": "w-42"},
        }
        logging.info("This log is automatically correlated with the trace")

        # Per-record attributes merge with handler-level attributes (record wins).
        logging.info("Request handled", extra={"attributes": {"http.status": 200}})
    """

    EXTRA_KEYS = ("trace_id", "span_id", "attributes")

    def __init__(
        self,
        endpoint: str | None = None,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
        level: int = logging.NOTSET,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the OTLP handler.

        :param endpoint: OTLP collector URL. If None, uses env vars
        :param resource: Resource attrs. If None, uses OTEL_SERVICE_NAME
        :param scope: Optional instrumentation scope metadata
        :param level: Minimum log level to export (default: NOTSET exports all)
        :param extra: Default extra dict merged into every log record
        """
        super().__init__(level)
        self.endpoint = endpoint
        self.resource = resource
        self.scope = scope
        self.extra: dict[str, Any] = extra or {}

    def emit(self, record: logging.LogRecord) -> None:
        """Export a log record to the OTLP collector.

        Maps Python logging levels to OTLP severity numbers and automatically
        captures code location attributes. Sends logs immediately without batching.

        On error, prints a message to stderr but doesn't raise to avoid disrupting
        the application.

        :param record: The log record to export
        """
        try:
            # Map Python log level to OTLP severity number
            if record.levelno <= logging.DEBUG:
                severity = LogRecord.Severity.DEBUG
            elif record.levelno <= logging.INFO:
                severity = LogRecord.Severity.INFO
            elif record.levelno <= logging.WARNING:
                severity = LogRecord.Severity.WARN
            elif record.levelno <= logging.ERROR:
                severity = LogRecord.Severity.ERROR
            else:
                severity = LogRecord.Severity.FATAL

            record_extra = {
                key: val
                for key in self.EXTRA_KEYS
                if (val := getattr(record, key, None)) is not None
            }
            merged = {**self.extra, **record_extra}

            trace_id = merged.get("trace_id") or ""
            span_id = merged.get("span_id") or ""

            # "attributes" is merged from both levels separately so that
            # record-level keys extend (not replace) handler-level ones.
            attributes = {
                "code.filepath": record.pathname,
                "code.lineno": record.lineno,
                "code.function": record.funcName,
            }
            attributes.update(self.extra.get("attributes") or {})
            attributes.update(record_extra.get("attributes") or {})

            # Create and send the log record
            log = LogRecord(
                body=record.getMessage(),  # Use interpolated message
                timestamp_ns=int(record.created * 1_000_000_000),
                trace_id=trace_id,
                span_id=span_id,
                severity_number=severity,
                severity_text=record.levelname,
                attributes=attributes,
            )

            # Use environment variables if not set in constructor
            endpoint = self.endpoint or None
            resource = self.resource or _get_resource_from_env()
            if resource:
                send_logs(endpoint, resource, [log], self.scope)
        except Exception:
            # Don't let logging errors crash the application
            sys.stderr.write("failed to send log\n")
            sys.stderr.flush()


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def send_spans(
    endpoint: str | None,
    resource: Resource,
    spans: list[Span],
    scope: InstrumentationScope | None = None,
    timeout: float = 2.0,
) -> bool:
    """Send a batch of spans to an OTLP collector over HTTP.

    Sends spans to the collector's /v1/traces endpoint using the OTLP JSON format.
    Returns True on successful transmission (HTTP 200), False on any error.
    Errors are logged as warnings but not raised to avoid disrupting the application.

    :param str | None endpoint: OTLP collector URL. If None, uses env vars
                                 (OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or OTEL_ENDPOINT)
    :param Resource resource: Resource attributes describing the service
    :param list[Span] spans: List of spans to send
    :param InstrumentationScope scope: Optional instrumentation scope metadata
    :param float timeout: HTTP request timeout in seconds (default 2.0)

    Example tracing an HTTP request with a database query::

        resource = Resource({
            "service.name": "order-service",
            "service.version": "2.1.0",
        })
        trace_id = new_trace_id()
        start = now_ns()

        # Parent span for the HTTP request
        http_span = Span(
            trace_id=trace_id,
            span_id=new_span_id(),
            name="POST /api/orders",
            start_time_ns=start,
            end_time_ns=now_ns(),
            kind=Span.Kind.SERVER,
            attributes={
                "http.method": "POST",
                "http.route": "/api/orders",
                "http.status_code": 201,
            },
            status=Span.Status.OK,
        )

        # Child span for the database insert
        db_span = Span(
            trace_id=trace_id,
            span_id=new_span_id(),
            parent_span_id=http_span.span_id,
            name="INSERT orders",
            start_time_ns=start,
            end_time_ns=now_ns(),
            kind=Span.Kind.CLIENT,
            attributes={
                "db.system": "postgresql",
                "db.operation": "INSERT",
                "db.name": "shop",
            },
        )

        send_spans("http://localhost:4318", resource, [http_span, db_span])

    """
    # Check if picotel is disabled - return immediately without logging
    if _is_disabled():
        return False

    # Build the URL - env vars return full URL, explicit endpoint needs path appended
    if endpoint is None:
        url = _get_endpoint("traces")
        if url is None:
            raise PicotelConfigError(
                "No OTLP endpoint configured."
                f" Set {_env('OTEL_EXPORTER_OTLP_ENDPOINT')}"
                f" or {_env('OTEL_SDK_DISABLED')}=true."
            )
    else:
        url = endpoint.rstrip("/") + "/v1/traces"

    # Validate spans and build the ExportTraceServiceRequest payload
    valid_spans = []
    for span in spans:
        try:
            span._validate()
            valid_spans.append(span)
        except PicotelConfigError as e:
            _logger.error(str(e))  # noqa: TRY400
    span_dicts = [_span_to_dict(s) for s in valid_spans]
    scope_span_dict: dict[str, Any] = {"spans": span_dicts}
    if scope:
        scope_dict: dict[str, Any] = {"name": scope.name, "version": scope.version}
        if scope.attributes:
            scope_dict["attributes"] = _attributes_to_otlp(scope.attributes)
        scope_span_dict["scope"] = scope_dict

    payload = {
        "resourceSpans": [
            {
                "resource": {"attributes": _attributes_to_otlp(resource.attributes)},
                "scopeSpans": [scope_span_dict],
            }
        ]
    }
    headers = {"Content-Type": "application/json"}
    # Add headers from environment if configured
    headers.update(_parse_headers())
    data = json.dumps(payload).encode("utf-8")

    try:
        # urllib is safe here - we're connecting to user-specified telemetry endpoints
        request = urllib.request.Request(  # noqa: S310
            url, data=data, headers=headers, method="POST"
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            # OTLP spec defines only 200 as successful export
            return response.status == 200  # noqa: PLR2004
    except (urllib.error.URLError, OSError) as e:
        # Log the error but don't raise - telemetry shouldn't crash the app
        _logger.error(f"Failed to send spans to {url}: {e}")  # noqa: TRY400
        return False


def send_logs(
    endpoint: str | None,
    resource: Resource,
    logs: list[LogRecord],
    scope: InstrumentationScope | None = None,
    timeout: float = 2.0,
) -> bool:
    """Send a batch of logs to an OTLP collector over HTTP.

    Sends logs to the collector's /v1/logs endpoint using the OTLP JSON format.
    Returns True on successful transmission (HTTP 200), False on any error.
    Errors are logged as warnings but not raised to avoid disrupting the application.

    :param str | None endpoint: OTLP collector URL. If None, uses env vars
                                 (OTEL_EXPORTER_OTLP_LOGS_ENDPOINT or OTEL_ENDPOINT)
    :param Resource resource: Resource attributes describing the service
    :param list[LogRecord] logs: List of log records to send
    :param InstrumentationScope scope: Optional instrumentation scope metadata
    :param float timeout: HTTP request timeout in seconds (default 2.0)

    Example logging a payment processing error with trace correlation::

        resource = Resource({
            "service.name": "payment-service",
            "deployment.environment": "prod",
        })

        # These would come from an active span context
        trace_id = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
        span_id = "1234567890abcdef"

        error_log = LogRecord(
            body="Payment declined: insufficient funds",
            severity_number=LogRecord.Severity.ERROR,
            severity_text="ERROR",
            trace_id=trace_id,
            span_id=span_id,
            attributes={
                "payment.provider": "stripe",
                "payment.amount": 99.99,
                "payment.currency": "USD",
                "error.type": "InsufficientFundsError",
                "customer.id": "cust_12345",
            },
        )

        send_logs("http://localhost:4318", resource, [error_log])

    """
    # Check if picotel is disabled - return immediately without logging
    if _is_disabled():
        return False

    # Build the URL - env vars return full URL, explicit endpoint needs path appended
    if endpoint is None:
        url = _get_endpoint("logs")
        if url is None:
            raise PicotelConfigError(
                "No OTLP endpoint configured."
                f" Set {_env('OTEL_EXPORTER_OTLP_ENDPOINT')}"
                f" or {_env('OTEL_SDK_DISABLED')}=true."
            )
    else:
        url = endpoint.rstrip("/") + "/v1/logs"

    # Build the ExportLogsServiceRequest payload
    scope_log_dict: dict[str, Any] = {"logRecords": [_log_to_dict(log) for log in logs]}
    if scope:
        scope_dict: dict[str, Any] = {"name": scope.name, "version": scope.version}
        if scope.attributes:
            scope_dict["attributes"] = _attributes_to_otlp(scope.attributes)
        scope_log_dict["scope"] = scope_dict

    payload = {
        "resourceLogs": [
            {
                "resource": {"attributes": _attributes_to_otlp(resource.attributes)},
                "scopeLogs": [scope_log_dict],
            }
        ]
    }
    headers = {"Content-Type": "application/json"}
    # Add headers from environment if configured
    headers.update(_parse_headers())
    data = json.dumps(payload).encode("utf-8")

    try:
        # urllib is safe here - we're connecting to user-specified telemetry endpoints
        request = urllib.request.Request(  # noqa: S310
            url, data=data, headers=headers, method="POST"
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            # OTLP spec defines only 200 as successful export
            return response.status == 200  # noqa: PLR2004
    except (urllib.error.URLError, OSError) as e:
        # Log the error but don't raise - telemetry shouldn't crash the app
        _logger.error(f"Failed to send logs to {url}: {e}")  # noqa: TRY400
        return False


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------


@functools.lru_cache(maxsize=None)
def _prefix() -> str:
    """Return the env-var namespace prefix configured via PICOTEL_PREFIX.

    When empty (default), picotel reads standard OTEL_* env vars.
    When set (e.g. "PICOTEL"), it reads PICOTEL_* vars instead.
    """
    return os.environ.get("PICOTEL_PREFIX", "")


@functools.lru_cache(maxsize=None)
def _env(standard_name: str) -> str:
    """Map a standard env-var name to the active namespace.

    Without prefix, returns the name unchanged (e.g. ``OTEL_SDK_DISABLED``).
    With ``PICOTEL_PREFIX="PICOTEL"``, strips the leading ``OTEL_`` and
    prepends the prefix joined by ``_``
    (e.g. ``OTEL_SDK_DISABLED`` -> ``PICOTEL_SDK_DISABLED``).
    """
    p = _prefix()
    if not p:
        return standard_name
    if standard_name.startswith("OTEL_"):
        return p + "_" + standard_name[5:]  # OTEL_X → PICOTEL_X
    return p + "_" + standard_name  # TRACEPARENT → PICOTEL_TRACEPARENT


@functools.lru_cache(maxsize=None)
def _is_disabled() -> bool:
    """Check if picotel is disabled via the SDK_DISABLED environment variable.

    When disabled, all send operations return False immediately without logging.
    """
    return os.environ.get(_env("OTEL_SDK_DISABLED"), "").lower() in ("true", "1")


@functools.lru_cache(maxsize=None)
def _get_endpoint(signal: str = "traces") -> str | None:
    """Get the full OTLP endpoint URL from environment variables.

    Per OTEL spec, signal-specific endpoints are used as-is, while the general
    endpoint has the signal path appended. Returns the full URL ready to use.

    :param signal: The signal type - "traces" or "logs"
    """
    signal_var = _env(f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT")
    if specific := os.environ.get(signal_var):
        return specific
    general_var = _env("OTEL_EXPORTER_OTLP_ENDPOINT")
    if base := os.environ.get(general_var):
        return base.rstrip("/") + f"/v1/{signal}"
    return None


@functools.lru_cache(maxsize=None)
def _parse_headers() -> dict[str, str]:
    """Parse headers from the EXPORTER_OTLP_HEADERS environment variable.

    Format: key1=value1,key2=value2
    Returns empty dict if not set or invalid.
    """
    headers_str = os.environ.get(_env("OTEL_EXPORTER_OTLP_HEADERS"), "")
    if not headers_str:
        return {}

    headers = {}
    for pair in headers_str.split(","):
        if "=" in (pair := pair.strip()):
            key, value = pair.split("=", 1)
            headers[key.strip()] = value.strip()
    return headers


@functools.lru_cache(maxsize=None)
def _get_resource_from_env() -> Resource | None:
    """Create a Resource from environment variables.

    Merges attributes from RESOURCE_ATTRIBUTES with the service name from
    SERVICE_NAME. The RESOURCE_ATTRIBUTES format follows the W3C Baggage spec:
    ``key1=value1,key2=value2`` with percent-encoding for special characters.
    All attribute values are strings.

    SERVICE_NAME takes precedence over any service.name in RESOURCE_ATTRIBUTES.

    Returns None if no resource configuration is found.
    """
    attrs: dict[str, Any] = {}
    if res_attrs_str := os.environ.get(_env("OTEL_RESOURCE_ATTRIBUTES")):
        for pair in res_attrs_str.split(","):
            if "=" in pair:
                key, value = pair.split("=", 1)
                attrs[urllib.parse.unquote(key.strip())] = urllib.parse.unquote(
                    value.strip()
                )
    if service_name := os.environ.get(_env("OTEL_SERVICE_NAME")):
        attrs["service.name"] = service_name
    return Resource(attrs) if attrs else None


@functools.lru_cache(maxsize=None)
def _parse_traceparent() -> tuple[str, str, int] | None:
    """Parse the W3C trace parent from environment.

    Reads the TRACEPARENT variable (or its prefixed variant when PICOTEL_PREFIX
    is set).

    Format: {version}-{trace-id}-{parent-id}-{trace-flags}
    Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01

    Returns (trace_id, parent_id, trace_flags) or None if not set/invalid.
    """
    traceparent = os.environ.get(_env("TRACEPARENT"), "")
    if not traceparent:
        return None

    parts = traceparent.split("-")
    if len(parts) != 4 or parts[0] != "00":  # noqa: PLR2004
        return None

    _, trace_id, parent_id, trace_flags_str = parts
    hex_chars = set("0123456789abcdefABCDEF")

    if not (
        len(trace_id) == 32  # noqa: PLR2004
        and len(parent_id) == 16  # noqa: PLR2004
        and len(trace_flags_str) == 2  # noqa: PLR2004
        and all(c in hex_chars for c in trace_id + parent_id + trace_flags_str)
    ):
        return None

    return (trace_id, parent_id, int(trace_flags_str, 16))


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
    result: dict[str, Any] = {
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

    # Include status only if set (not None and not UNSET)
    if span.status is not None and span.status != Span.Status.UNSET:
        result["status"] = {"code": int(span.status)}

    return result


def _log_to_dict(log: LogRecord) -> dict[str, Any]:
    """Return the OTLP JSON dict representation of a LogRecord.

    Builds a log record dict following the OpenTelemetry Protocol specification.
    Uses current time for timestamps if they are 0. Optional fields are omitted
    when empty to minimize payload size.

    :param log: The LogRecord object to serialize
    """
    # Use current time if timestamps are 0
    result: dict[str, Any] = {
        "timeUnixNano": str(log.timestamp_ns if log.timestamp_ns else now_ns()),
        "observedTimeUnixNano": str(
            log.observed_timestamp_ns if log.observed_timestamp_ns else now_ns()
        ),
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
