# picotel

A minimal, single-file OpenTelemetry client for Python that sends spans and logs over HTTP/JSON to any OTLP-compatible collector (Jaeger, Grafana Tempo, OTEL Collector, etc.) with zero external dependencies.

Designed for:
- **Vendoring** in contexts where installing external dependencies is not possible or desirable (embedded scripts, restricted environments, standalone tools)
- **Isolation** when you need to submit OTLP signals without conflicting with the OpenTelemetry SDK used by other parts of the application

## Installation

Just copy the single `picotel.py` file into your project:

```bash
curl -O https://raw.githubusercontent.com/posit-dev/picotel/main/src/picotel.py
```

That's it! No pip install, no dependency management. Requires Python 3.8+.

## Quick Start

```python
from picotel import Span, Resource, new_trace_id, send_spans

# Configure your service
resource = Resource({"service.name": "my-app", "service.version": "1.0.0"})

# Trace a simple operation
with Span(
    trace_id=new_trace_id(),
    name="process-order",
    endpoint="http://localhost:4318",
    resource=resource,
) as span:
    # Your code here
    span.attributes["order.id"] = "12345"
    span.attributes["order.total"] = 99.99
```

## API Reference

### Core Types

#### `Resource`
Describes the entity producing telemetry (your service):
```python
resource = Resource({
    "service.name": "payment-service",
    "service.version": "2.1.0",
    "deployment.environment": "production"
})
```

#### `Span`
Represents a single operation within a trace:
```python
# Manual span creation
from picotel import now_ns

span = Span(
    trace_id=new_trace_id(),
    span_id=new_span_id(),
    name="database-query",
    start_time_ns=now_ns(),
    end_time_ns=now_ns() + 1000000,  # 1ms later
    kind=Span.Kind.CLIENT,
    attributes={"db.system": "postgresql", "db.operation": "SELECT"}
)

# Context manager (recommended)
with Span(
    trace_id=new_trace_id(),
    name="api-call",
    endpoint="http://localhost:4318",
    resource=resource
) as span:
    # Automatically sets start/end times and sends on exit
    pass
```

#### `LogRecord`
A structured log entry with optional trace correlation:
```python
log = LogRecord(
    body="Payment processed successfully",
    severity_number=LogRecord.Severity.INFO,
    trace_id=span.trace_id,  # Optional: correlate with trace
    span_id=span.span_id,
    attributes={"payment.amount": 99.99, "payment.method": "card"}
)
```

### Helper Functions

- `new_trace_id()` - Generate a 32-char hex trace ID
- `new_span_id()` - Generate a 16-char hex span ID
- `now_ns()` - Current time in nanoseconds since Unix epoch
- `send_spans(endpoint, resource, spans)` - Send spans to collector (raises `PicotelConfigError` if no endpoint configured)
- `send_logs(endpoint, resource, logs)` - Send logs to collector (raises `PicotelConfigError` if no endpoint configured)

### Exceptions

#### `PicotelConfigError`
Raised when picotel is missing required configuration:

```python
from picotel import send_spans, Resource, Span, PicotelConfigError

try:
    # Without endpoint configured and PICOTEL_SDK_DISABLED not set
    send_spans(None, resource, [span])
except PicotelConfigError as e:
    print(e)  # "No OTLP endpoint configured. Set PICOTEL_EXPORTER_OTLP_ENDPOINT..."
```

**Note:** The `Span` context manager and `OTLPHandler` do NOT raise this exception - they silently skip sending if no endpoint is configured, to avoid disrupting application flow.

### Python Logging Integration

Use `OTLPHandler` to automatically export Python logs:
```python
import logging
from picotel import OTLPHandler, Resource

handler = OTLPHandler(
    endpoint="http://localhost:4318",
    resource=Resource({"service.name": "my-app"})
)
logging.getLogger().addHandler(handler)

# Regular logs now go to OTLP
logging.info("Server started", extra={"port": 8080})

# With trace correlation
logging.error("Request failed", extra={
    "trace_id": trace_id,
    "span_id": span_id,
    "http.status": 500
})
```

### Parent-Child Spans

Create nested spans to show operation hierarchy:
```python
trace_id = new_trace_id()
resource = Resource({"service.name": "my-app"})
endpoint = "http://localhost:4318"

# Parent span
with Span(
    trace_id=trace_id,
    name="http-request",
    endpoint=endpoint,
    resource=resource
) as parent:
    # Child span references parent
    with Span(
        trace_id=trace_id,
        parent_span_id=parent.span_id,
        name="database-query",
        endpoint=endpoint,
        resource=resource
    ) as child:
        pass
```

## Environment Variables

Configure endpoints and service name via environment:

```bash
# Endpoint configuration (in order of precedence)
export PICOTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318/v1/traces
export PICOTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://collector:4318/v1/logs
export PICOTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318  # /v1/* appended
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318      # Standard OTEL

# Service name
export PICOTEL_SERVICE_NAME=my-service  # or OTEL_SERVICE_NAME

# Optional headers
export PICOTEL_EXPORTER_OTLP_HEADERS="api-key=secret,x-custom=value"

# Sending mode (see "Sending Modes" below)
export PICOTEL_ASYNC=true  # background thread dispatch; omit for synchronous
```

### Disabling picotel

To completely disable picotel telemetry, set the `PICOTEL_SDK_DISABLED` environment variable:

```bash
export PICOTEL_SDK_DISABLED=true
```

**When to use:** This is useful when:
- Embedding picotel in a library where users might want to disable telemetry
- The main application already uses its own OpenTelemetry SDK and you want to prevent conflicts
- You need to temporarily disable telemetry for debugging or testing

**What happens:** When disabled:
- All telemetry operations silently return `False` without sending data
- No errors or warnings are logged
- No HTTP requests are made to any collector endpoint
- This setting takes precedence over all endpoint configurations

**Note:** When `PICOTEL_SDK_DISABLED=true`, you don't need to configure any endpoints - picotel will simply drop all telemetry data silently.

Then use without explicit configuration:
```python
with Span(name="operation") as span:
    pass  # Uses env vars for endpoint and resource
```

### Trace Context Propagation

Continue traces from parent processes using W3C Trace Context:
```bash
export TRACEPARENT=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
```

```python
from picotel import Span, TRACEPARENT, Resource

resource = Resource({"service.name": "my-app"})

# Continues the trace from TRACEPARENT env var
with Span(
    trace_id=TRACEPARENT,
    name="child-operation",
    endpoint="http://localhost:4318",
    resource=resource
) as span:
    # span.trace_id and span.parent_span_id set from env
    pass
```

### Namespaced mode with PICOTEL_PREFIX

When you need picotel to use its own env-var namespace (e.g. to avoid
conflicting with the OpenTelemetry SDK used by user code), set
`PICOTEL_PREFIX`:

```bash
export PICOTEL_PREFIX=PICOTEL
export PICOTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318
export PICOTEL_SERVICE_NAME=my-service
export PICOTEL_SDK_DISABLED=true       # instead of OTEL_SDK_DISABLED
export PICOTEL_TRACEPARENT=00-...      # instead of TRACEPARENT
```

The prefix replaces the `OTEL_` portion of each standard variable name.
Non-`OTEL_` names like `TRACEPARENT` get the prefix prepended
(`PICOTEL_TRACEPARENT`).

## Sending Modes

picotel supports two sending modes: **synchronous** (default) and **asynchronous**. The mode is selected at import time via the `PICOTEL_ASYNC` environment variable.

### Synchronous (default)

```bash
# No env var needed — sync is the default
unset PICOTEL_ASYNC
```

Telemetry is sent inline on the calling thread. This is the simplest mode and is ideal for short-lived scripts, CLI tools, and environments where background threads are undesirable (some serverless runtimes, `fork()`-heavy process models).

**Error handling — circuit breaker:** If the collector is unreachable, each send blocks until the HTTP timeout expires (default 2 s). After **5 consecutive send failures**, picotel trips an internal circuit breaker and silently drops all subsequent telemetry for the lifetime of the process. This prevents a down collector from adding unbounded latency to every operation.

### Asynchronous

```bash
export PICOTEL_ASYNC=true   # or PICOTEL_ASYNC=1
```

Telemetry is dispatched to a background daemon thread via an internal queue, so the calling thread is never blocked by slow or unreachable collectors. This is the recommended mode for long-running services where latency matters.

**Error handling — queue back-pressure:** If the background thread cannot keep up (e.g. the collector is slow), the internal queue (256 entries) fills up and new signals are silently dropped. A single warning is logged per overflow episode. Once the queue drains, sending resumes normally.

**Fork safety:** The async sender detects `os.fork()` and automatically recreates its thread and queue in the child process. This works even when fork is called from C extensions that bypass `os.register_at_fork`.

### Choosing a mode

| Concern | Synchronous | Asynchronous |
|---|---|---|
| Calling-thread latency | Blocked during HTTP send | Never blocked |
| Error isolation | Circuit breaker after 5 failures | Queue drop on overflow |
| Background threads | None | One daemon thread |
| Fork safety | No special handling needed | Automatic recovery |
| Best for | Scripts, CLIs, short-lived processes | Long-running services |

## Limitations / Non-Goals

This library intentionally does **not** support:

- **gRPC/Protobuf** - HTTP/JSON only
- **Auto-instrumentation** - Manual instrumentation only
- **Metrics** - Traces and logs only
- **Sampling** - All spans are sent
- **Batching** - Each call sends immediately
- **Context propagation** - No automatic W3C TraceContext header injection
- **Full SDK compliance** - Not a complete OpenTelemetry SDK implementation

For these features, use the official [OpenTelemetry Python SDK](https://opentelemetry.io/docs/languages/python/).

## License

MIT - See LICENSE file for details
