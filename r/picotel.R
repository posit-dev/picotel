# picotel.R — minimal single-file OpenTelemetry client for R.
#
# Sends spans and logs over HTTP/JSON to any OTLP-compatible collector
# (Jaeger, Grafana Tempo, OTEL Collector, etc.) with a single runtime
# dependency: the `curl` package (loaded lazily at send time only).
#
# Vendoring: copy this file alongside your project and load it with
#   source("picotel.R")
#
# Requirements:
#   R >= 4.0
#   curl package (runtime, loaded lazily via requireNamespace())
#
# Test-only dependencies (never loaded by picotel.R itself):
#   testthat, withr, webfakes
#
# Version: port of picotel Python v0.3.0
# Author: Posit Software, PBC
# URL: https://github.com/posit-dev/picotel
# License: MIT


# ==== WP1: Constants, IDs, time helpers, env plumbing, state ====


# ----------------------------------------------------------------------------
# Internal logger
# ----------------------------------------------------------------------------

# .picotel_log() is the package-internal message sink.  It is deliberately
# NOT wired into globalCallingHandlers so that picotel errors (e.g. network
# failures) never feed back through an otlp_condition_handler attached to
# messages/warnings, which would create an infinite loop.  cat() to stderr
# is used so the output is never captured by sink() or message(), avoiding
# re-entry.

.picotel_log <- function(msg) {
  cat("picotel:", msg, "\n", file = stderr())
}


# ----------------------------------------------------------------------------
# Error condition
# ----------------------------------------------------------------------------

# picotel_config_error() raises the picotel_config_error condition (plan D3).
# Callers catch it with tryCatch(expr, picotel_config_error = function(e) ...).

picotel_config_error <- function(msg) {
  stop(errorCondition(
    paste0("picotel: ", msg),
    class = c("picotel_config_error", "error", "condition")
  ))
}


# ----------------------------------------------------------------------------
# Mutable process state
# ----------------------------------------------------------------------------

# All mutable process-level state lives in a single env so it can be
# swept cleanly by .picotel_reset_state() in tests (plan D2).
# parent = emptyenv() prevents accidental lookup into the global env.

.picotel_state <- new.env(parent = emptyenv())

# .picotel_reset_state() is a test-only seam.  It clears every slot so that
# the next call to any lazy-init function re-reads the environment.
# WARNING: not safe to call from concurrent code.

.picotel_reset_state <- function() {
  rm(list = ls(.picotel_state, all.names = TRUE), envir = .picotel_state)
}


# ----------------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------------

# SpanKind — type of span in a distributed trace (mirrors Span.Kind).
# Values follow the OTLP protobuf enumeration.
SpanKind <- list(
  UNSPECIFIED = 0L,
  INTERNAL    = 1L,
  SERVER      = 2L,
  CLIENT      = 3L,
  PRODUCER    = 4L,
  CONSUMER    = 5L
)

# SpanStatus — status of a completed span (mirrors Span.Status).
SpanStatus <- list(
  UNSET = 0L,
  OK    = 1L,
  ERROR = 2L
)

# Severity — log severity numbers (mirrors LogRecord.Severity).
# Values are OTLP severity numbers, *not* R message levels.
Severity <- list(
  TRACE = 1L,
  DEBUG = 5L,
  INFO  = 9L,
  WARN  = 13L,
  ERROR = 17L,
  FATAL = 21L
)

# TRACEPARENT sentinel — pass as trace_id or log trace_id to read the
# W3C trace context from the TRACEPARENT environment variable (or its
# prefixed variant when PICOTEL_PREFIX is set).  Detect with identical().
TRACEPARENT <- new.env(parent = emptyenv())


# ----------------------------------------------------------------------------
# ID generators
# ----------------------------------------------------------------------------

# new_trace_id() returns a 32-character lowercase hex string (128 bits of
# random data).
#
# Base R (>= 4.0) has no direct equivalent of Python's os.urandom().  We use
# sample() on the 16 hex-digit alphabet with replacement, which is backed by
# R's internal PRNG (Mersenne Twister by default).  The PRNG is seeded from
# /dev/urandom on startup, making this cryptographically sufficient for trace
# IDs in practice, though it is not a CSPRNG.
#
# Callers that require a stronger guarantee should call openssl::rand_bytes()
# or similar before sourcing picotel.R; for telemetry IDs the MT-based
# approach is universally accepted.

new_trace_id <- function() {
  # 32 hex chars = 128 bits
  paste(sample(c(0:9, letters[1:6]), 32L, replace = TRUE), collapse = "")
}

# new_span_id() returns a 16-character lowercase hex string (64 bits).
# Same PRNG rationale as new_trace_id().

new_span_id <- function() {
  # 16 hex chars = 64 bits
  paste(sample(c(0:9, letters[1:6]), 16L, replace = TRUE), collapse = "")
}


# ----------------------------------------------------------------------------
# Timestamp helpers
# ----------------------------------------------------------------------------

# now_ns() returns the current time as a double of integer nanoseconds since
# the Unix epoch (plan D5).
#
# Precision note: at ~1.75e18 the IEEE 754 double ulp is 256 ns, which is
# below R's actual clock resolution (Sys.time() has microsecond resolution
# at best on most platforms).  Arithmetic like now_ns() + 1e6 is therefore
# exact enough for all practical purposes.  Never format with as.character()
# or default print methods — use .picotel_ns_str() instead.
#
# Implementation: as.numeric(Sys.time()) gives seconds since epoch as a
# double; multiplying by 1e9 and rounding to the nearest integer preserves
# as much precision as the platform provides.

now_ns <- function() {
  round(as.numeric(Sys.time()) * 1e9)
}

# .picotel_ns_str(x) serialises a nanosecond timestamp (or any large integer
# stored as a double) to its exact decimal string without scientific notation.
# This is the ONLY safe way to convert such values to strings for JSON;
# as.character() / format() / print() may produce scientific notation or
# lose precision (plan D5).

.picotel_ns_str <- function(x) {
  sprintf("%.0f", x)
}


# ----------------------------------------------------------------------------
# Env-var plumbing
# ----------------------------------------------------------------------------

# .picotel_prefix() returns the value of PICOTEL_PREFIX (trimmed).
# Mirrors Python's _prefix(), but is not cached here because R env vars can
# change during a session; caching is done at the call site via .picotel_state
# when needed (WP2 owns the cache layer).
#
# The design intent: when PICOTEL_PREFIX is empty, picotel reads standard
# OTEL_* variables; when it is set (e.g. "PICOTEL") the prefix replaces the
# "OTEL_" portion of OTEL_* names, and is prepended (with "_") to non-OTEL
# names such as TRACEPARENT.

.picotel_prefix <- function() {
  trimws(Sys.getenv("PICOTEL_PREFIX", unset = ""))
}

# .picotel_env(standard_name) maps a standard env-var name to the active
# namespace, exactly mirroring Python's _env().
#
# Rules (from py lines 1097-1111):
#   - No prefix  → return standard_name unchanged
#   - With prefix, OTEL_X  → PREFIX_X  (strip leading "OTEL_", prepend prefix)
#   - With prefix, non-OTEL → PREFIX_standard_name  (just prepend)
#
# Special case: "PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY" is NEVER remapped;
# it is read raw from the environment regardless of PICOTEL_PREFIX (plan, WP4).
# This function does NOT handle that exception — callers that need the raw name
# must use Sys.getenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY") directly.

.picotel_env <- function(standard_name) {
  p <- .picotel_prefix()
  if (nchar(p) == 0L) {
    return(standard_name)
  }
  if (startsWith(standard_name, "OTEL_")) {
    # OTEL_X → PREFIX_X (strip "OTEL_", prepend prefix + "_")
    return(paste0(p, "_", substring(standard_name, 6L)))
  }
  # TRACEPARENT → PREFIX_TRACEPARENT
  paste0(p, "_", standard_name)
}

# .picotel_is_disabled() returns TRUE when OTEL_SDK_DISABLED (or its prefixed
# variant) is set to a truthy value.  Truthy means case-insensitive "true" or
# "1" — exactly matching Python's _is_disabled().

.picotel_is_disabled <- function() {
  val <- Sys.getenv(.picotel_env("OTEL_SDK_DISABLED"), unset = "")
  tolower(val) %in% c("true", "1")
}


# ==== WP2: Env config + traceparent ====


# .picotel_endpoint(signal) — resolve full OTLP endpoint URL for "traces" or
# "logs", applying signal-specific > general precedence and appending "/v1/<signal>"
# to the general endpoint.  Caches in .picotel_state.
#
# Mirrors Python's _get_endpoint() which is decorated with @functools.lru_cache —
# i.e. Python also caches.  The result is stored under
# .picotel_state$endpoint_<signal> so .picotel_reset_state() clears it.
#
# Precedence (matches Python exactly):
#   1. OTEL_EXPORTER_OTLP_{SIGNAL}_ENDPOINT  — used verbatim if set.
#   2. OTEL_EXPORTER_OTLP_ENDPOINT           — trailing "/" stripped, then
#      "/v1/<signal>" appended (Python: base.rstrip("/") + "/v1/<signal>").
#   3. Returns NULL when neither is set.
#
# Both variable names are resolved via .picotel_env() to honour PICOTEL_PREFIX.
.picotel_endpoint <- function(signal = "traces") {
  cache_key <- paste0("endpoint_", signal)
  cached <- .picotel_state[[cache_key]]
  if (!is.null(cached)) {
    # cached[[1]] holds the value; cached[[2]] is TRUE when the cached result
    # is a real value vs. a "not-found" sentinel.
    if (isTRUE(cached$found)) {
      return(cached$value)
    } else {
      return(NULL)
    }
  }

  # Signal-specific variable takes precedence and is used verbatim.
  signal_var <- .picotel_env(
    paste0("OTEL_EXPORTER_OTLP_", toupper(signal), "_ENDPOINT")
  )
  specific <- Sys.getenv(signal_var, unset = NA_character_)
  if (!is.na(specific)) {
    .picotel_state[[cache_key]] <- list(found = TRUE, value = specific)
    return(specific)
  }

  # Fall back to the general endpoint with /v1/<signal> appended.
  general_var <- .picotel_env("OTEL_EXPORTER_OTLP_ENDPOINT")
  base <- Sys.getenv(general_var, unset = "")
  if (nchar(base) > 0L) {
    url <- paste0(sub("/+$", "", base), "/v1/", signal)
    .picotel_state[[cache_key]] <- list(found = TRUE, value = url)
    return(url)
  }

  # Neither is set.
  .picotel_state[[cache_key]] <- list(found = FALSE, value = NULL)
  NULL
}

# .picotel_url_decode(s) — lenient percent-decoder mirroring Python's
# urllib.parse.unquote(): decodes %XX sequences but keeps the raw string
# on invalid sequences (rather than erroring).  '+' is NOT decoded as a
# space (path-unescape semantics, not query-unescape).
#
# utils::URLdecode() emits a warning and returns "" for out-of-range bytes;
# we catch that warning and fall back to the raw input.
.picotel_url_decode <- function(s) {
  tryCatch(
    utils::URLdecode(s),
    warning = function(w) s
  )
}

# .picotel_resource_from_env() — build a picotel_resource from OTEL_RESOURCE_ATTRIBUTES
# and OTEL_SERVICE_NAME (W3C Baggage percent-decoding).  Caches result in
# .picotel_state$resource.
#
# Mirrors Python's _get_resource_from_env() (@functools.lru_cache):
#   - OTEL_RESOURCE_ATTRIBUTES: comma-separated "key=value" pairs; keys and
#     values are percent-decoded (W3C Baggage spec).  Malformed pairs (no "=")
#     are silently skipped.  All attribute values are strings.
#   - OTEL_SERVICE_NAME overrides any "service.name" from resource attrs.
#   - Returns NULL (Python: None) when no configuration is found.
#
# Both variable names are resolved via .picotel_env() to honour PICOTEL_PREFIX.
.picotel_resource_from_env <- function() {
  if (exists("resource", envir = .picotel_state, inherits = FALSE)) {
    return(.picotel_state$resource)
  }

  attrs <- list()

  res_attrs_str <- Sys.getenv(
    .picotel_env("OTEL_RESOURCE_ATTRIBUTES"), unset = ""
  )
  if (nchar(res_attrs_str) > 0L) {
    for (pair in strsplit(res_attrs_str, ",", fixed = TRUE)[[1L]]) {
      eq_pos <- regexpr("=", pair, fixed = TRUE)[[1L]]
      if (eq_pos < 1L) {
        # No "=" — malformed pair; skip.
        next
      }
      raw_key <- substring(pair, 1L, eq_pos - 1L)
      raw_val <- substring(pair, eq_pos + 1L)
      key <- .picotel_url_decode(raw_key)
      val <- .picotel_url_decode(raw_val)
      attrs[[key]] <- val
    }
  }

  service_name <- Sys.getenv(
    .picotel_env("OTEL_SERVICE_NAME"), unset = ""
  )
  if (nchar(service_name) > 0L) {
    attrs[["service.name"]] <- service_name
  }

  result <- if (length(attrs) == 0L) NULL else picotel_resource(attrs)
  .picotel_state$resource <- result
  result
}

# .picotel_parse_traceparent() — parse the W3C TRACEPARENT env var (or its
# prefixed variant).  Returns list(trace_id, parent_id, flags) or NULL.
# Caches result in .picotel_state$traceparent.
#
# Mirrors Python's _parse_traceparent() (@functools.lru_cache):
#   - Reads env var via .picotel_env("TRACEPARENT") (honours PICOTEL_PREFIX).
#   - Format: "{version}-{trace_id}-{parent_id}-{trace_flags}"
#   - Exactly 4 "-"-delimited parts; version must be "00".
#   - trace_id:  32 hex chars  [0-9a-fA-F]
#   - parent_id: 16 hex chars  [0-9a-fA-F]
#   - flags:      2 hex chars  [0-9a-fA-F]; parsed as hex integer.
#   - Case is preserved in the returned strings (Python does not normalise).
#   - Returns NULL on any validation failure or when the var is unset/empty.
#
# Note: Python does NOT reject all-zeros trace_id or parent_id; neither do we.
.picotel_parse_traceparent <- function() {
  if (exists("traceparent", envir = .picotel_state, inherits = FALSE)) {
    return(.picotel_state$traceparent)
  }

  tp_var  <- .picotel_env("TRACEPARENT")
  tp_str  <- Sys.getenv(tp_var, unset = "")

  result <- if (nchar(tp_str) == 0L) {
    NULL
  } else {
    parts <- strsplit(tp_str, "-", fixed = TRUE)[[1L]]
    if (length(parts) != 4L || parts[[1L]] != "00") {
      NULL
    } else {
      trace_id  <- parts[[2L]]
      parent_id <- parts[[3L]]
      flags_str <- parts[[4L]]
      hex_re    <- "^[0-9a-fA-F]+$"
      if (
        nchar(trace_id)  == 32L &&
        nchar(parent_id) == 16L &&
        nchar(flags_str) == 2L  &&
        grepl(hex_re, trace_id,  perl = TRUE) &&
        grepl(hex_re, parent_id, perl = TRUE) &&
        grepl(hex_re, flags_str, perl = TRUE)
      ) {
        list(
          trace_id  = trace_id,
          parent_id = parent_id,
          flags     = strtoi(flags_str, base = 16L)
        )
      } else {
        NULL
      }
    }
  }

  .picotel_state$traceparent <- result
  result
}


# ==== WP3: OTLP JSON encoding + validation ====


# .picotel_to_otlp_value(value) — convert an R value to the typed OTLP
# attribute map format (e.g. list(stringValue = "hello")).
# R-specific: length-1 vector → scalar; length-n → arrayValue; NA → skip
# (empty list); factor → string; integer/whole-number double → intValue
# (serialized via .picotel_ns_str to avoid JSON precision loss).
.picotel_to_otlp_value <- function(value) {
  stop("TODO(WP3): .picotel_to_otlp_value")
}

# .picotel_attributes_to_otlp(attributes) — convert a named list of attributes
# to the OTLP [{key, value}] list format, skipping NULL/NA values.
.picotel_attributes_to_otlp <- function(attributes) {
  stop("TODO(WP3): .picotel_attributes_to_otlp")
}

# .picotel_span_to_list(span) — serialize a picotel_span env to the OTLP dict.
.picotel_span_to_list <- function(span) {
  stop("TODO(WP3): .picotel_span_to_list")
}

# .picotel_log_to_list(log) — serialize a picotel_log_record env to OTLP dict.
.picotel_log_to_list <- function(log) {
  stop("TODO(WP3): .picotel_log_to_list")
}

# .picotel_validate_span(span) — check required fields; raise picotel_config_error
# if invalid.
.picotel_validate_span <- function(span) {
  stop("TODO(WP3): .picotel_validate_span")
}

# .picotel_to_json(x) — hand-rolled JSON encoder (plan D1: no jsonlite).
# Handles the OTLP value space: strings (with escaping), booleans, integers,
# doubles, arrays, nested lists.
.picotel_to_json <- function(x) {
  stop("TODO(WP3): .picotel_to_json")
}


# ==== WP4: Transport, TLS, headers ====


# .picotel_parse_headers() — parse OTEL_EXPORTER_OTLP_HEADERS (key=val,key=val)
# returning a named character vector.
.picotel_parse_headers <- function() {
  stop("TODO(WP4): .picotel_parse_headers")
}

# .picotel_tls_options(signal) — return a named list of curl handle options
# for TLS (plan D8: pure function, no side effects on handles).
# CA precedence: signal-specific > general; INSECURE_SKIP_VERIFY never remapped.
.picotel_tls_options <- function(signal = "traces") {
  stop("TODO(WP4): .picotel_tls_options")
}

# .picotel_post_json(url, body, timeout, signal) — POST a JSON string to url
# using curl, applying TLS options for the given signal.
.picotel_post_json <- function(url, body, timeout = 2, signal = "traces") {
  stop("TODO(WP4): .picotel_post_json")
}


# ==== WP5: Senders + flush ====


# .picotel_sync_submit(fn, ...) — execute fn(...) immediately with circuit
# breaker (trips after 5 consecutive failures, plan D6).
.picotel_sync_submit <- function(fn, ...) {
  stop("TODO(WP5): .picotel_sync_submit")
}

# .picotel_async_submit(fn, ...) — enqueue fn(...) in the curl multi pool;
# non-blocking (plan D6).  Capped at 256 pending; single error message per
# overflow episode.
.picotel_async_submit <- function(fn, ...) {
  stop("TODO(WP5): .picotel_async_submit")
}

# picotel_flush(timeout) — pump the async pool until drained or timeout
# (seconds).  No-op for the sync sender.
picotel_flush <- function(timeout = 2) {
  stop("TODO(WP5): picotel_flush")
}

# .picotel_get_sender() — return the process-wide sender function; decides
# once per process based on PICOTEL_ASYNC (lazy init cached in .picotel_state).
.picotel_get_sender <- function() {
  stop("TODO(WP5): .picotel_get_sender")
}


# ==== WP6: Public API ====


# ----------------------------------------------------------------------------
# Constructors — picotel_resource, picotel_scope
# ----------------------------------------------------------------------------

# picotel_resource(attributes) — create a Resource (classed list, plan D3).
# attributes: a named list of service/deployment attributes.
#
# NOTE: trivially implemented here (WP1 scope) to allow helper tests that
# need a Resource object without touching sending.
picotel_resource <- function(attributes) {
  if (!is.list(attributes)) {
    attributes <- as.list(attributes)
  }
  structure(list(attributes = attributes), class = "picotel_resource")
}

# picotel_scope(name, version, attributes) — create an InstrumentationScope.
picotel_scope <- function(name, version = "", attributes = NULL) {
  stop("TODO(WP6): picotel_scope")
}


# ----------------------------------------------------------------------------
# Constructors — picotel_span, picotel_log_record
# ----------------------------------------------------------------------------

# picotel_span(trace_id, name, ...) — create a new span (environment-backed,
# class picotel_span, plan D3).
# Mirrors Python Span defaults: span_id auto-generated, kind = INTERNAL,
# start/end times NULL (set by with_span), status = NULL, etc.

picotel_span <- function(
  trace_id,
  name,
  span_id       = new_span_id(),
  parent_span_id = "",
  kind          = SpanKind$INTERNAL,
  start_time_ns = NULL,
  end_time_ns   = NULL,
  attributes    = list(),
  events        = list(),
  links         = list(),
  status        = NULL,
  endpoint      = "",
  resource      = NULL,
  scope         = NULL
) {
  stop("TODO(WP6): picotel_span")
}

# picotel_log_record(body, ...) — create a new log record (environment-backed,
# class picotel_log_record).
picotel_log_record <- function(
  body,
  timestamp_ns          = 0,
  observed_timestamp_ns = 0,
  trace_id              = "",
  span_id               = "",
  trace_flags           = 0L,
  severity_number       = Severity$INFO,
  severity_text         = "",
  attributes            = list()
) {
  stop("TODO(WP6): picotel_log_record")
}


# ----------------------------------------------------------------------------
# with_span — context-manager analogue (plan D4)
# ----------------------------------------------------------------------------

# with_span(trace_id, name, ..., f) — evaluate f(span) with the span bound,
# automatically recording start/end times and submitting on exit (including on
# error).  This is the callback form: f receives the picotel_span object as
# its sole argument, allowing mutation of span$attributes inside the body.
#
# Design rationale (plan D4, implementer picks the idiom):
#   - Callback form `with_span(..., f = function(span) { ... })` is chosen
#     over the expression form because:
#     (a) It gives f a typed `span` binding without lexical tricks.
#     (b) on.exit() + tryCatch inside with_span cleanly mirrors Python's
#         __enter__/__exit__ lifecycle without needing non-standard evaluation.
#   - f is the *last* argument so callers can write:
#       with_span(tid, "op", f = function(span) { span$attributes$k <- v })
#     which is idiomatic in R (compare lapply/Map/tryCatch).
#
# Behaviour mirrors Python's Span.__exit__:
#   - On exit (normal or error): set end_time_ns, submit via sender.
#   - On error: span status is NOT automatically set to ERROR (matches Python,
#     which does NOT set status in __exit__); caller controls status.
#   - send errors are swallowed (never raised to caller).

with_span <- function(
  trace_id,
  name,
  ...,
  endpoint      = "",
  resource      = NULL,
  scope         = NULL,
  f
) {
  stop("TODO(WP6): with_span")
}


# ----------------------------------------------------------------------------
# span_send / log_send (single-item convenience, silently skips if unconfigured)
# ----------------------------------------------------------------------------

# span_send(span, ...) — send a single span; silently returns FALSE when
# endpoint/resource is unconfigured (only batch senders raise config errors,
# plan WP6).
span_send <- function(
  span,
  endpoint = NULL,
  resource = NULL,
  scope    = NULL,
  timeout  = 2
) {
  stop("TODO(WP6): span_send")
}

# log_send(log, ...) — send a single log record; same skip semantics as span_send.
log_send <- function(
  log,
  endpoint = NULL,
  resource = NULL,
  scope    = NULL,
  timeout  = 2
) {
  stop("TODO(WP6): log_send")
}


# ----------------------------------------------------------------------------
# Batch senders (public API, plan D3)
# ----------------------------------------------------------------------------

# send_spans(endpoint, resource, spans, scope, timeout) — send a batch of spans.
# endpoint = NULL → resolve from env; raises picotel_config_error when
# unconfigured.  Per-span validation: invalid spans are logged and dropped.
send_spans <- function(
  endpoint,
  resource,
  spans,
  scope   = NULL,
  timeout = 2
) {
  stop("TODO(WP6): send_spans")
}

# send_logs(endpoint, resource, logs, scope, timeout) — send a batch of log
# records.  Same endpoint/config semantics as send_spans.
send_logs <- function(
  endpoint,
  resource,
  logs,
  scope   = NULL,
  timeout = 2
) {
  stop("TODO(WP6): send_logs")
}


# ----------------------------------------------------------------------------
# otlp_condition_handler — R condition-system equivalent of OTLPHandler (plan D7)
# ----------------------------------------------------------------------------

# otlp_condition_handler(endpoint, resource, attributes) returns a handler
# function suitable for use with globalCallingHandlers() or
# withCallingHandlers().
#
# Severity mapping (documented deviation from Python — R has no log levels):
#   message condition  → INFO  (9)
#   warning condition  → WARN  (13)
#   error condition    → ERROR (17)  [note: errors also invoke message handlers]
#
# Usage:
#   h <- otlp_condition_handler(resource = picotel_resource(list("service.name" = "app")))
#   globalCallingHandlers(message = h, warning = h)
#
# Or for a scoped block:
#   withCallingHandlers(expr, message = h, warning = h)
otlp_condition_handler <- function(
  endpoint   = NULL,
  resource   = NULL,
  attributes = NULL
) {
  stop("TODO(WP6): otlp_condition_handler")
}
