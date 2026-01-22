# miniotel

Minimal, single-file OpenTelemetry client for Python with zero dependencies.

This is designed so that you can take `src/miniotel.py` and copy it into your own project to use it without having to install dependencies.

## Features

- Send traces (spans) to OTLP-compatible collectors
- Send logs correlated with traces
- HTTP/JSON transport (no gRPC/protobuf required)
- Zero external dependencies (stdlib only)
- Python 3.8+ compatible

## Usage

```python
from miniotel import (
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
