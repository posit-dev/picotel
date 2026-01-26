# picotel

Minimal, single-file OpenTelemetry client for Python with zero dependencies.

This is designed so that you can take `src/picotel.py` and copy it into your own project to use it without having to install dependencies.

## Features

- Send traces (spans) to OTLP-compatible collectors
- Send logs correlated with traces
- HTTP/JSON transport (no gRPC/protobuf required)
- Zero external dependencies (stdlib only)
- Python 3.8+ compatible

## Usage

```python
from picotel import (
    Resource,
    Span,
    LogRecord,
    send_spans,
    send_logs,
    new_trace_id,
    new_span_id,
    now_ns,
    SpanKind,
    Severity,
)

# Create a resource identifying your service
resource = Resource({"service.name": "my-app", "service.version": "1.0.0"})

# Create a span
trace_id = new_trace_id()
span_id = new_span_id()
start = now_ns()
# ... do work ...
end = now_ns()

span = Span(
    trace_id=trace_id,
    span_id=span_id,
    name="my-operation",
    start_time_ns=start,
    end_time_ns=end,
    kind=SpanKind.SERVER,
    attributes={"http.method": "GET", "http.url": "/api/data"},
)

# Send to collector
send_spans("http://localhost:4318", resource, [span])

# Create and send a correlated log
log = LogRecord(
    body="Request processed successfully",
    trace_id=trace_id,
    span_id=span_id,
    severity_number=Severity.INFO,
)
send_logs("http://localhost:4318", resource, [log])
```

## Environment Variables

picotel supports standard OpenTelemetry environment variables:

- `OTEL_EXPORTER_OTLP_ENDPOINT` - Base collector URL (e.g., `http://localhost:4318`). Path `/v1/traces` or `/v1/logs` is appended automatically.
- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` - Traces endpoint (used as-is, no path appended)
- `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` - Logs endpoint (used as-is, no path appended)
- `OTEL_EXPORTER_OTLP_HEADERS` - Custom headers (`key1=value1,key2=value2`)
- `OTEL_SERVICE_NAME` - Service name for auto-created Resource

When environment variables are configured, you can omit the endpoint and resource:

```python
# With OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_SERVICE_NAME set:
with Span(trace_id=new_trace_id(), name="my-op", start_time_ns=0, end_time_ns=0):
    pass  # Span sent automatically on exit
```

## Disabling Telemetry

To disable telemetry, simply don't set the OTEL environment variables. When not configured, picotel will log warnings and return `False` from send functions.

To silence the warnings:

```python
import logging
logging.getLogger("picotel").setLevel(logging.ERROR)
```

## Non-Goals

- gRPC/Protobuf support (HTTP/JSON only)
- Auto-instrumentation
- Context propagation
- Metrics
- Sampling
- Batching/buffering
- Full SDK compliance

## License

MIT
