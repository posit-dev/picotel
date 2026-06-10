# picotel (Go)

A minimal, zero-dependency single-file OpenTelemetry client for Go that sends
spans and logs over HTTP/JSON to any OTLP-compatible collector (Jaeger, Grafana
Tempo, OTEL Collector, etc.).

Ports **picotel Python v0.3.0** behavior. Requires **Go ≥ 1.21**.

Designed for:
- **Vendoring** in contexts where installing external dependencies is not
  possible or desirable (embedded scripts, restricted environments, standalone
  tools)
- **Isolation** when you need to submit OTLP signals without conflicting with
  the OpenTelemetry SDK used by other parts of the application

## Installation

### Standard Go module

```bash
go get github.com/posit-dev/picotel/go
```

Import as `github.com/posit-dev/picotel/go`.

### Vendoring (single-file copy)

Just copy `picotel.go` into your project — no module management, no dependency
graph:

```bash
curl -O https://raw.githubusercontent.com/posit-dev/picotel/main/go/picotel.go
```

Vendoring users may rewrite the `package` clause at the top of the file to
match their own package name.

> **Note:** Go releases use subdirectory tags (`go/v1.0.0`, `go/v1.1.0`, etc.).

## Quick Start

### Manual span

```go
import (
    "github.com/posit-dev/picotel/go"
)

traceID := picotel.NewTraceID()
resource := picotel.NewResource(map[string]any{
    "service.name":    "my-app",
    "service.version": "1.0.0",
})

span := picotel.NewSpan(traceID, "process-order").Start()
defer span.End()

span.Endpoint = "http://localhost:4318"
span.Resource = resource
span.Attributes["order.id"] = "12345"
span.Attributes["order.total"] = 99.99
```

### Direct send with error handling

```go
import (
    "errors"
    "github.com/posit-dev/picotel/go"
)

err := picotel.SendSpans("http://localhost:4318", resource, []*picotel.Span{span}, nil, 0)
if errors.Is(err, picotel.ErrDisabled) {
    // SDK disabled via OTEL_SDK_DISABLED — safe to ignore
} else if err != nil {
    var cfgErr *picotel.ConfigError
    if errors.As(err, &cfgErr) {
        log.Fatalf("bad config: %v", cfgErr)
    }
    log.Printf("send error: %v", err)
}
```

### Log export via slog

```go
import (
    "log/slog"
    "github.com/posit-dev/picotel/go"
)

logger := slog.New(picotel.NewOTLPHandler(&picotel.OTLPHandlerOptions{
    Endpoint: "http://localhost:4318",
    Resource: picotel.NewResource(map[string]any{"service.name": "my-app"}),
}))

// Regular logs go to OTLP
logger.Info("server started", slog.Int("port", 8080))

// With trace correlation — use "trace_id" / "span_id" slog attributes
logger.Error("request failed",
    slog.String("trace_id", traceID),
    slog.String("span_id", spanID),
    slog.Int("http.status", 500),
)
```

### TRACEPARENT continuation

```go
// Continues a trace from the TRACEPARENT environment variable
span := picotel.NewSpanFromEnv("child-operation").Start()
defer span.End()
span.Endpoint = "http://localhost:4318"
span.Resource = resource

// Or read it yourself:
traceID, parentSpanID, _, ok := picotel.TraceparentFromEnv()
```

### Async mode + Flush before exit

```bash
export PICOTEL_ASYNC=1
```

```go
// ... do work, spans are dispatched to a background goroutine ...

// Before process exit, drain the queue (up to 2 seconds)
picotel.Flush(2 * time.Second)
```

## Environment Variables

Configure endpoints and service name via environment. All standard `OTEL_*`
variables can be remapped to a custom prefix via `PICOTEL_PREFIX` (see
[Namespaced mode](#namespaced-mode-with-picotel_prefix) below).

### Endpoint configuration

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Full URL for traces (used verbatim) |
| `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` | Full URL for logs (used verbatim) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Base URL; `/v1/traces` or `/v1/logs` appended |

Signal-specific variables win over the base variable.

### Service identity

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Sets `service.name` resource attribute |
| `OTEL_RESOURCE_ATTRIBUTES` | Comma-separated `key=value` resource attributes |

### Headers and auth

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_HEADERS` | Comma-separated `key=value` HTTP headers added to every export request |

### TLS

Applies only when the OTLP endpoint is `https://`. Plain `http://` endpoints
ignore all TLS variables.

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_CERTIFICATE` | PEM path for a private CA (bypasses system trust store) |
| `OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE` | Signal-specific CA override (wins over the general variable) |
| `OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE` | Signal-specific CA override (wins over the general variable) |
| `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | PEM path for mTLS client certificate (combined cert+key PEM accepted) |
| `OTEL_EXPORTER_OTLP_CLIENT_KEY` | PEM path for mTLS client private key |
| `PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY` | `true`/`1` — disables cert **and** hostname verification. Dev only, never production. Not remapped by `PICOTEL_PREFIX`. |

Default: no TLS vars set + `https://` endpoint → system trust store is used.

mTLS client cert/key are signal-agnostic (no `_TRACES_CLIENT_CERTIFICATE` or
`_LOGS_CLIENT_CERTIFICATE` variants — a documented deviation from the OTEL spec).

### SDK control and async

| Variable | Description |
|---|---|
| `OTEL_SDK_DISABLED` | `true`/`1` — silently drop all telemetry; takes precedence over all endpoints |
| `PICOTEL_ASYNC` | `true`/`1` — enable background-goroutine dispatch (see [Sending Modes](#sending-modes)) |

### Trace context propagation

| Variable | Description |
|---|---|
| `TRACEPARENT` | W3C traceparent header value; read by `NewSpanFromEnv` / `TraceparentFromEnv` |

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
(`PICOTEL_TRACEPARENT`). `PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY` is
always read under that exact name regardless of `PICOTEL_PREFIX`.

## Sending Modes

picotel supports two sending modes: **synchronous** (default) and
**asynchronous**. The mode is selected at init time via the `PICOTEL_ASYNC`
environment variable.

### Synchronous (default)

```bash
# No env var needed — sync is the default
unset PICOTEL_ASYNC
```

Telemetry is sent inline on the calling goroutine. Ideal for short-lived
scripts, CLI tools, and environments where background goroutines are undesirable.

**Error handling — circuit breaker:** If the collector is unreachable, each
send blocks until the HTTP timeout expires (default 2 s). After **5 consecutive
persistent send failures**, picotel trips an internal circuit breaker and
silently drops all subsequent telemetry for the lifetime of the process. This
prevents a down collector from adding unbounded latency to every operation.

### Asynchronous

```bash
export PICOTEL_ASYNC=true   # or PICOTEL_ASYNC=1
```

Telemetry is dispatched to a background daemon goroutine via an internal queue
(capacity 256), so the calling goroutine is never blocked by slow or unreachable
collectors. This is the recommended mode for long-running services where latency
matters.

**Error handling — queue back-pressure:** If the background goroutine cannot
keep up, the queue fills and new signals are silently dropped. Once the queue
drains, sending resumes normally.

**Flush before exit:** Call `picotel.Flush(timeout)` before process exit to
drain the queue:

```go
picotel.Flush(2 * time.Second) // returns true if queue drained within timeout
```

`Flush` is a no-op (returns true immediately) in synchronous mode.

### Choosing a mode

| Concern | Synchronous | Asynchronous |
|---|---|---|
| Calling-goroutine latency | Blocked during HTTP send | Never blocked |
| Error isolation | Circuit breaker after 5 failures | Queue drop on overflow |
| Background goroutines | None | One daemon goroutine |
| Flush needed before exit | No | Yes |
| Best for | Scripts, CLIs, short-lived processes | Long-running services |

## Differences from the Python Library

| Aspect | Python | Go |
|---|---|---|
| Send errors | `bool` return (`False` = no send) | `error` return (`nil` = success) |
| Disabled indicator | `False` return | `picotel.ErrDisabled` (use `errors.Is`) |
| Config errors | `PicotelConfigError` exception | `*picotel.ConfigError` (use `errors.As`) |
| Log integration | `logging.Handler` (`OTLPHandler`) | `slog.Handler` (`OTLPHandler` + `NewOTLPHandler`) |
| Trace context from env | `Span(trace_id=TRACEPARENT, ...)` sentinel | `NewSpanFromEnv(name)` / `TraceparentFromEnv()` |
| Span timing | `with Span(...) as span:` context manager | `span.Start()` + `defer span.End()` |
| Attribute key order | Dict insertion order | Sorted alphabetically in JSON output |
| Flush | Not applicable | `picotel.Flush(timeout time.Duration) bool` |
| Fork safety | Automatic async-sender recovery after `os.fork()` | Not applicable (Go has no `fork`) |
| NaN / ±Inf float values | Sent as proto3-JSON strings `"NaN"`, `"Infinity"`, `"-Infinity"` | Same behavior |

## Limitations / Non-Goals

This library intentionally does **not** support:

- **gRPC/Protobuf** — HTTP/JSON only
- **Auto-instrumentation** — Manual instrumentation only
- **Metrics** — Traces and logs only
- **Sampling** — All spans are sent
- **Batching** — Each call sends immediately
- **Context propagation** — No automatic W3C TraceContext header injection
- **Full SDK compliance** — Not a complete OpenTelemetry SDK implementation

For these features, use the official [OpenTelemetry Go SDK](https://opentelemetry.io/docs/languages/go/).

## Testing

```bash
cd go && go test -race -cover ./...
```

## License

MIT — See LICENSE file for details
