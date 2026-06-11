# picotel (R)

A minimal, single-file OpenTelemetry client for R that sends spans and logs
over HTTP/JSON to any OTLP-compatible collector (Jaeger, Grafana Tempo, OTEL
Collector, etc.).

Ports **picotel Python v0.3.0** behavior. Requires **R ≥ 4.0** and the
[`curl`](https://cran.r-project.org/package=curl) package at send time.

Designed for:
- **Vendoring** in contexts where installing external dependencies is not
  possible or desirable (embedded scripts, restricted environments, standalone
  tools)
- **Isolation** when you need to submit OTLP signals without conflicting with
  any other telemetry tooling used by the application

## Installation

Just copy the single `picotel.R` file into your project and source it:

```bash
curl -O https://raw.githubusercontent.com/posit-dev/picotel/main/r/picotel.R
```

```r
source("picotel.R")
```

This is **not a CRAN package, by design** — there is no `DESCRIPTION`, no
`NAMESPACE`, no install step. Public functions use plain snake_case names;
internals are dot-prefixed (`.picotel_*`) so they stay out of `ls()`.

### Why the `curl` dependency?

Base R has no HTTP POST and no TLS client. The `curl` package is the
ecosystem floor for HTTP in R: it has zero R dependencies of its own, ships
with practically every R installation that talks to a network, and its
options map one-to-one onto picotel's TLS configuration. `picotel.R` never
loads it at `source()` time — only lazily (via `requireNamespace("curl")`)
when a send is actually attempted. If `curl` is missing, sends log a warning
and return `FALSE`; nothing crashes.

## Quick Start

```r
source("picotel.R")

resource <- picotel_resource(list(
  "service.name"    = "my-app",
  "service.version" = "1.0.0"
))

with_span(
  trace_id = new_trace_id(),
  name     = "process-order",
  endpoint = "http://localhost:4318",
  resource = resource,
  f = function(span) {
    # Your code here
    span$attributes[["order.id"]]    <- "12345"
    span$attributes[["order.total"]] <- 99.99
  }
)
```

`with_span()` is the R analogue of Python's `with Span(...) as span:` — it
sets `start_time_ns` on entry, runs `f(span)`, then sets `end_time_ns` and
sends on exit **even if `f` throws** (the error is re-raised after the span
is submitted; the span's status is *not* automatically set to ERROR, matching
Python).

## API Reference

### Core constructors

#### `picotel_resource(attributes)`
Describes the entity producing telemetry (your service):
```r
resource <- picotel_resource(list(
  "service.name"           = "payment-service",
  "service.version"        = "2.1.0",
  "deployment.environment" = "production"
))
```

#### `picotel_span(trace_id, name, ...)`
A single operation within a trace. Returns a mutable, environment-backed
object of class `picotel_span`:
```r
span <- picotel_span(
  trace_id      = new_trace_id(),
  name          = "database-query",
  start_time_ns = now_ns(),
  end_time_ns   = now_ns() + 1e6,   # 1 ms later
  kind          = SpanKind$CLIENT,
  attributes    = list("db.system" = "postgresql", "db.operation" = "SELECT")
)
```
Other fields: `span_id` (auto-generated), `parent_span_id`, `events`,
`links`, `status` (`SpanStatus$OK` / `SpanStatus$ERROR`), `endpoint`,
`resource`, `scope`.

#### `picotel_log_record(body, ...)`
A structured log entry with optional trace correlation:
```r
log <- picotel_log_record(
  body            = "Payment processed successfully",
  severity_number = Severity$INFO,
  trace_id        = span$trace_id,   # optional: correlate with a trace
  span_id         = span$span_id,
  attributes      = list("payment.amount" = 99.99, "payment.method" = "card")
)
```

#### `picotel_scope(name, version = "", attributes = NULL)`
Optional instrumentation-scope metadata passed to the send functions.

### Sending

- `with_span(trace_id, name, ..., endpoint = "", resource = NULL, scope = NULL, f)` —
  run `f(span)` with automatic timing and submit-on-exit. Resolves
  endpoint/resource from the environment when not given; **silently skips**
  sending when unconfigured.
- `span_send(span, endpoint = NULL, resource = NULL, scope = NULL, timeout = 2)` /
  `log_send(log, ...)` — send one span/log. Logs a warning and returns
  `FALSE` when endpoint or resource cannot be resolved; never raises.
- `send_spans(endpoint, resource, spans, scope = NULL, timeout = 2)` /
  `send_logs(endpoint, resource, logs, ...)` — batch send. Returns `TRUE` on
  HTTP 200, `FALSE` on any send error. These are the **only** functions that
  raise (`picotel_config_error` condition) when no endpoint is configured and
  the SDK is not disabled. Invalid spans (missing `trace_id` or timestamps)
  are logged and dropped individually; the rest of the batch still sends.
- `picotel_flush(timeout = 2)` — drain the async queue (see
  [Sending Modes](#sending-modes)). No-op returning `TRUE` in sync mode.

```r
tryCatch(
  send_spans(NULL, resource, list(span)),   # NULL endpoint → resolve from env
  picotel_config_error = function(e) message("no endpoint configured")
)
```

### Helpers and constants

- `new_trace_id()` — 32-char lowercase hex trace ID
- `new_span_id()` — 16-char lowercase hex span ID
- `now_ns()` — current Unix time in nanoseconds, as a double (see
  [Differences from Python](#differences-from-the-python-library))
- `SpanKind` — `$UNSPECIFIED`, `$INTERNAL`, `$SERVER`, `$CLIENT`,
  `$PRODUCER`, `$CONSUMER`
- `SpanStatus` — `$UNSET`, `$OK`, `$ERROR`
- `Severity` — OTLP severity numbers (`$TRACE`=1 … `$FATAL`=21; `$INFO`=9,
  `$WARN`=13, `$ERROR`=17)
- `TRACEPARENT` — sentinel object for trace continuation (below)

### Parent-child spans

```r
trace_id <- new_trace_id()

with_span(trace_id, "http-request", endpoint = ep, resource = res,
  f = function(parent) {
    with_span(trace_id, "database-query",
      parent_span_id = parent$span_id,
      endpoint = ep, resource = res,
      f = function(child) {
        # ...
      }
    )
  }
)
```

### Trace context propagation

Continue traces from a parent process using W3C Trace Context:

```bash
export TRACEPARENT=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
```

```r
# Pass the TRACEPARENT sentinel as trace_id; picotel parses the env var and
# fills in trace_id + parent_span_id.
with_span(
  trace_id = TRACEPARENT,
  name     = "child-operation",
  endpoint = "http://localhost:4318",
  resource = resource,
  f = function(span) { ... }
)
```

If the env var is unset or invalid, a warning is logged and the span's
`trace_id` is left empty (the span is then dropped at validation, matching
Python).

## Condition Handler (log integration)

`otlp_condition_handler()` is the R analogue of Python's
`logging.Handler`-based `OTLPHandler`, built on R's condition system:

```r
h <- otlp_condition_handler(
  endpoint = "http://localhost:4318",
  resource = picotel_resource(list("service.name" = "my-app"))
)

# Process-wide (typically in .Rprofile or at startup):
globalCallingHandlers(message = h, warning = h)

# Or scoped to a block:
withCallingHandlers({
  message("server started")       # exported as INFO
  warning("cache miss rate high") # exported as WARN
}, message = h, warning = h)
```

Severity mapping (by condition class):

| Condition | Severity |
|---|---|
| `message` (and anything else) | `INFO` (9) |
| `warning` | `WARN` (13) |
| `error` | `ERROR` (17) |

Trace correlation and extra attributes ride on custom condition fields:
`trace_id` and `span_id` become LogRecord fields (not attributes), and a
named list in `picotel.attributes` is merged over any handler-level
`attributes` (condition wins on key conflicts):

```r
cond <- structure(
  class = c("myapp_event", "message", "condition"),
  list(
    message              = "Payment processed\n",
    trace_id             = span$trace_id,
    span_id              = span$span_id,
    picotel.attributes   = list("payment.amount" = 99.99)
  )
)
withCallingHandlers(message(cond), message = h)
```

The handler never throws and never muffles the condition — your `message()` /
`warning()` behavior is unchanged whether or not telemetry is configured.

## Environment Variables

All standard `OTEL_*` variables can be remapped to a custom prefix via
`PICOTEL_PREFIX` (see [Namespaced mode](#namespaced-mode-with-picotel_prefix)).

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
| `OTEL_SERVICE_NAME` | Sets the `service.name` resource attribute |
| `OTEL_RESOURCE_ATTRIBUTES` | Comma-separated `key=value` resource attributes (values percent-decoded per W3C Baggage) |

### Headers and auth

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_HEADERS` | Comma-separated `key=value` HTTP headers added to every export request |

### TLS

Applies only when the OTLP endpoint is `https://`. Plain `http://` endpoints
ignore all TLS variables. Options are passed to `curl` (`cainfo`, `sslcert`,
`sslkey`, `ssl_verifypeer`, `ssl_verifyhost`).

| Variable | Description |
|---|---|
| `OTEL_EXPORTER_OTLP_CERTIFICATE` | PEM path for a private CA (bypasses system trust store) |
| `OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE` | Signal-specific CA override (wins over the general variable) |
| `OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE` | Signal-specific CA override (wins over the general variable) |
| `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | PEM path for mTLS client certificate (combined cert+key PEM accepted) |
| `OTEL_EXPORTER_OTLP_CLIENT_KEY` | PEM path for mTLS client private key (ignored without a client certificate) |
| `PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY` | `true`/`1` — disables cert **and** hostname verification, but still loads any configured client cert (mTLS against self-signed collectors keeps working). Dev only, never production. Not remapped by `PICOTEL_PREFIX`. |

Default: no TLS vars set + `https://` endpoint → system trust store is used.

mTLS client cert/key are signal-agnostic (no `_TRACES_CLIENT_CERTIFICATE` or
`_LOGS_CLIENT_CERTIFICATE` variants — a documented deviation from the OTEL
spec, shared with the Python original).

### SDK control and async

| Variable | Description |
|---|---|
| `OTEL_SDK_DISABLED` | `true`/`1` — silently drop all telemetry; takes precedence over all endpoints |
| `PICOTEL_ASYNC` | `true`/`1` — enable deferred sending (see [Sending Modes](#sending-modes)); read once per process, under this exact name (not prefix-remapped) |

### Trace context propagation

| Variable | Description |
|---|---|
| `TRACEPARENT` | W3C traceparent header value; consumed via the `TRACEPARENT` sentinel |

### Namespaced mode with PICOTEL_PREFIX

When you need picotel to use its own env-var namespace (e.g. to avoid
conflicting with other OpenTelemetry tooling configured by user code), set
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
(`PICOTEL_TRACEPARENT`). `PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY` and
`PICOTEL_ASYNC` are always read under those exact names regardless of
`PICOTEL_PREFIX`.

## Sending Modes

picotel supports two sending modes: **synchronous** (default) and
**deferred** ("async"). The mode is selected once per process, at the first
send, via the `PICOTEL_ASYNC` environment variable.

### Synchronous (default)

Telemetry is sent inline on the calling thread (R has only one). Ideal for
scripts, CLI tools, and short-lived processes.

**Error handling — circuit breaker:** If the collector is unreachable, each
send blocks until the HTTP timeout expires (default 2 s). After **5
consecutive persistent send failures** (HTTP failures or config errors),
picotel trips an internal circuit breaker and silently drops all subsequent
telemetry for the lifetime of the process. A successful send resets the
counter.

### Deferred (`PICOTEL_ASYNC`)

```bash
export PICOTEL_ASYNC=true   # or PICOTEL_ASYNC=1
```

R is single-threaded — there is no background-thread dispatch. In this mode
submitting telemetry **never blocks and never sends**: spans and logs are
enqueued (capacity 256) and delivered when you call:

```r
picotel_flush(timeout = 2)  # TRUE if the queue drained within `timeout` seconds
```

A best-effort finalizer also flushes at session exit, but explicit
`picotel_flush()` before exit is the reliable path. If the queue is full, new
signals are silently dropped with a single warning per overflow episode; once
the queue drains, a future overflow warns again. The circuit breaker applies
to deliveries during flush; once tripped, the remaining queue is discarded.

### Choosing a mode

| Concern | Synchronous | Deferred |
|---|---|---|
| Calling-code latency | Blocked during HTTP send (≤ timeout) | Never blocked |
| Delivery timing | Immediate | At `picotel_flush()` / session exit |
| Error isolation | Circuit breaker after 5 failures | Queue drop on overflow + breaker at flush |
| Flush needed before exit | No | Yes (finalizer is best-effort only) |
| Best for | Scripts, CLIs, anything latency-tolerant | Latency-sensitive code paths |

## Differences from the Python Library

| Aspect | Python | R |
|---|---|---|
| Async mode | Background daemon thread | Deferral queue; delivery only in `picotel_flush()` / exit finalizer |
| Fork safety | Automatic async-sender recovery after `os.fork()` | Not applicable (no fork recovery; avoid sending from `parallel::mclapply` children) |
| Span timing | `with Span(...) as span:` context manager | `with_span(..., f = function(span) ...)` callback |
| Log integration | `logging.Handler` (`OTLPHandler`) | Condition handler for `globalCallingHandlers()` / `withCallingHandlers()` |
| Log severity levels | Python logging levels incl. DEBUG/FATAL | Condition class only: message→INFO, warning→WARN, error→ERROR |
| `code.*` source attributes on logs | filepath/lineno/function from the log record | Not emitted (no cheap caller location in R) |
| Config errors | `PicotelConfigError` exception | `picotel_config_error` condition (use `tryCatch`) |
| `now_ns()` | True ns integer | Double with µs clock precision (256 ns representable step at epoch scale); serialized digit-exact |
| ID generation | `os.urandom` (CSPRNG) | R's Mersenne-Twister PRNG (not cryptographic; fine for telemetry IDs) |
| Scalars vs arrays | `5` is a scalar, `[5]` is an array | Length-1 vector → scalar; length-n vector or unnamed list → array |
| `None`/missing attribute values | `None` → empty OTLP value | `NULL` and scalar `NA` → empty OTLP value |
| Bytes attribute values | base64 `bytesValue` | No bytes type; `raw` vectors become `arrayValue` |
| NaN / ±Inf float values | Sent as proto3-JSON strings `"NaN"`, `"Infinity"`, `"-Infinity"` | Same behavior |
| HTTP timeout granularity | Float seconds | Integer seconds (curl); values < 1 s round up to 1 s |

## Limitations / Non-Goals

This library intentionally does **not** support:

- **gRPC/Protobuf** — HTTP/JSON only
- **Auto-instrumentation** — Manual instrumentation only
- **Metrics** — Traces and logs only
- **Sampling** — All spans are sent
- **Batching** — Each call sends immediately (the deferred queue defers, it does not batch)
- **Context propagation** — No automatic W3C TraceContext header injection
- **Full SDK compliance** — Not a complete OpenTelemetry SDK implementation

For these features, use the official OpenTelemetry SDKs.

## Testing

```bash
Rscript r/tests/run.R
```

Test-only dependencies (never loaded by `picotel.R` itself): `testthat`,
`withr`, `webfakes`. CI runs the suite on R release and oldrel-1 via
`.github/workflows/r.yml`.

## License

MIT — See LICENSE file for details
