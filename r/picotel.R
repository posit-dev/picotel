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
#
# Type dispatch (mirrors Python _to_otlp_value(), with R-specific additions):
#
#   NULL              -> list()  (empty map; Python None -> {})
#   NA (any type)     -> list()  (R-specific decision: NA is treated as missing/None.
#                                  NA carries no useful telemetry value and would
#                                  produce misleading strings.  Callers wanting a
#                                  literal "NA" string must call as.character(NA).)
#   logical (len 1)   -> boolValue   (checked before numeric)
#   integer (len 1)   -> intValue    (sprintf "%.0f", no sci notation)
#   whole-number double (len 1) -> intValue  (mirrors Python int path for 42.0)
#   NaN               -> doubleValue "NaN"        (proto3 JSON; mirrors Go f1c4cfb)
#   Inf               -> doubleValue "Infinity"   (proto3 JSON)
#  -Inf               -> doubleValue "-Infinity"  (proto3 JSON)
#   other double(len 1)-> doubleValue             (as numeric, full precision)
#   character (len 1) -> stringValue
#   factor (len 1)    -> stringValue via as.character
#   named list        -> kvlistValue  (mirrors Python dict -> kvlistValue)
#   length > 1 vector/list -> arrayValue  (R has no scalars; length-n is array)
#   unnamed list      -> arrayValue
#   other types       -> stringValue via as.character  (fallback; mirrors Python str())
#
# Empty container convention: bare list() -> "[]" in .picotel_to_json().
# Named empty list is impossible (list() has no names); but if it occurs, it
# also becomes an array.  Upstream serializers always omit empty containers
# before calling to_json, so this edge case is benign.
.picotel_to_otlp_value <- function(value) {
  # NULL -> empty OTLP value map (Python None -> {}).
  # We return a named-but-empty list so the JSON encoder produces "{}".
  # (An unnamed empty list would produce "[]" per the plan D1 convention.)
  if (is.null(value)) {
    return(.picotel_empty_otlp_map())
  }

  # NaN must be checked before is.na() because is.na(NaN) is TRUE.
  # NaN is a valid double, not a missing value.
  if (is.double(value) && length(value) == 1L && is.nan(value)) {
    return(list(doubleValue = "NaN"))
  }

  # Scalar NA of any type -> empty OTLP value map (R-specific: NA treated as None).
  # Must check before type dispatch so NA_real_, NA_integer_, NA_character_ are
  # all caught.  is.na() on a list returns per-element; we guard with !is.list().
  if (!is.list(value) && length(value) == 1L && is.na(value)) {
    return(.picotel_empty_otlp_map())
  }

  # Named list -> kvlistValue  (check before length-n so a named list of length 1
  # is NOT treated as a scalar but as an object with one key)
  if (is.list(value) && !is.null(names(value))) {
    return(.picotel_kvlist_value(value))
  }

  # length > 1 vector or unnamed list -> arrayValue
  if (length(value) > 1L) {
    return(.picotel_array_value(value))
  }

  # Unnamed list of length 0 or 1 -> arrayValue
  if (is.list(value)) {
    return(.picotel_array_value(value))
  }

  # From here on: length-1 atomic vector (scalar).

  # factor -> stringValue  (before is.character since is.character(factor) is FALSE)
  if (is.factor(value)) {
    return(list(stringValue = as.character(value)))
  }

  # logical -> boolValue  (before numeric: logical is not numeric in R)
  if (is.logical(value)) {
    return(list(boolValue = as.logical(value)))
  }

  # integer -> intValue as exact decimal string
  if (is.integer(value)) {
    return(list(intValue = .picotel_ns_str(value)))
  }

  # double: Inf first (proto3 JSON strings), then whole-number doubles, then general.
  # NaN was already handled before the is.na() check above.
  if (is.double(value)) {
    if (is.infinite(value)) {
      return(list(doubleValue = if (value > 0) "Infinity" else "-Infinity"))
    }
    if (value == floor(value)) {
      # Whole-number double -> intValue (mirrors Python's int path for e.g. 42.0).
      # No upper-bound guard: large doubles like max-int64 are representable and
      # must be emitted as intValue strings (plan D5, Go WP3).
      return(list(intValue = .picotel_ns_str(value)))
    }
    # Non-whole double -> doubleValue; store as numeric for JSON encoder
    return(list(doubleValue = value))
  }

  # character -> stringValue
  if (is.character(value)) {
    return(list(stringValue = value))
  }

  # Fallback: as.character() mirrors Python's str(value) for unknown types
  list(stringValue = as.character(value))
}

# .picotel_empty_otlp_map() — return the R representation of an empty OTLP
# value map (Python's {} / None -> {}).  Uses a named-but-empty list so that
# the JSON encoder produces "{}" rather than "[]".
# Internal helper; not exported.
.picotel_empty_otlp_map <- function() {
  setNames(list(), character(0L))
}

# .picotel_array_value(vec) — encode a vector or unnamed list as OTLP arrayValue.
# Internal helper; not exported.
.picotel_array_value <- function(vec) {
  if (length(vec) == 0L) {
    return(list(arrayValue = list(values = list())))
  }
  if (is.list(vec)) {
    vals <- lapply(vec, .picotel_to_otlp_value)
  } else {
    vals <- lapply(seq_along(vec), function(i) .picotel_to_otlp_value(vec[[i]]))
  }
  list(arrayValue = list(values = vals))
}

# .picotel_kvlist_value(x) — encode a named list as OTLP kvlistValue.
# Preserves insertion order (R named lists maintain order, unlike Go maps).
# Internal helper; not exported.
.picotel_kvlist_value <- function(x) {
  nms <- names(x)
  pairs <- lapply(seq_along(x), function(i) {
    list(key = nms[[i]], value = .picotel_to_otlp_value(x[[i]]))
  })
  list(kvlistValue = list(values = pairs))
}

# .picotel_attributes_to_otlp(attributes) — convert a named list of attributes
# to the OTLP [{key, value}] list format.
#
# Skips entries where the value is NULL (mirrors Python: `if v is not None`).
# Skips entries where the value is a scalar NA (R-specific: NA treated as None).
# NULL/empty attributes argument returns list() (empty result).
#
# NOTE: Unlike .picotel_to_otlp_value(), NA inside a nested array is NOT
# skipped here — only top-level NULL/NA entries are omitted.  This mirrors
# Python where None at the top level is skipped but None inside a list
# becomes {} (an empty OTLP value).
.picotel_attributes_to_otlp <- function(attributes) {
  if (is.null(attributes) || length(attributes) == 0L) {
    return(list())
  }
  nms <- names(attributes)
  result <- list()
  for (i in seq_along(attributes)) {
    v <- attributes[[i]]
    # Skip NULL values (mirrors Python's `if v is not None`)
    if (is.null(v)) next
    # Skip scalar NA at the top level (R-specific: NA == missing/None)
    if (!is.list(v) && length(v) == 1L && is.na(v)) next
    result <- c(result, list(list(key = nms[[i]], value = .picotel_to_otlp_value(v))))
  }
  result
}

# .picotel_span_to_list(span) — serialize a picotel_span environment to the
# OTLP JSON dict format.  Ports Python's _span_to_dict() field-by-field,
# including conditional omission of optional fields.
#
# span: environment or list with fields:
#   trace_id, span_id, name, kind, start_time_ns, end_time_ns,
#   parent_span_id, attributes, events (list of lists: name/timestamp_ns/attributes),
#   links (list of lists: trace_id/span_id/attributes), status
.picotel_span_to_list <- function(span) {
  result <- list(
    traceId           = span$trace_id,
    spanId            = span$span_id,
    name              = span$name,
    kind              = as.integer(span$kind),
    startTimeUnixNano = .picotel_ns_str(span$start_time_ns),
    endTimeUnixNano   = .picotel_ns_str(span$end_time_ns)
  )

  # parentSpanId — omit when empty (mirrors Python: `if span.parent_span_id`)
  if (!is.null(span$parent_span_id) && nchar(span$parent_span_id) > 0L) {
    result$parentSpanId <- span$parent_span_id
  }

  # attributes — omit when empty
  attrs <- .picotel_attributes_to_otlp(span$attributes)
  if (length(attrs) > 0L) {
    result$attributes <- attrs
  }

  # events — omit when empty
  if (!is.null(span$events) && length(span$events) > 0L) {
    result$events <- lapply(span$events, function(ev) {
      e <- list(
        name         = ev$name,
        timeUnixNano = .picotel_ns_str(ev$timestamp_ns)
      )
      ev_attrs <- .picotel_attributes_to_otlp(ev$attributes)
      if (length(ev_attrs) > 0L) {
        e$attributes <- ev_attrs
      }
      e
    })
  }

  # links — omit when empty
  if (!is.null(span$links) && length(span$links) > 0L) {
    result$links <- lapply(span$links, function(lk) {
      l <- list(
        traceId = lk$trace_id,
        spanId  = lk$span_id
      )
      lk_attrs <- .picotel_attributes_to_otlp(lk$attributes)
      if (length(lk_attrs) > 0L) {
        l$attributes <- lk_attrs
      }
      l
    })
  }

  # status — omit when NULL or UNSET (SpanStatus$UNSET == 0L)
  # Mirrors Python: `if span.status is not None and span.status != Span.Status.UNSET`
  if (!is.null(span$status) && !identical(as.integer(span$status), SpanStatus$UNSET)) {
    result$status <- list(code = as.integer(span$status))
  }

  result
}

# .picotel_log_to_list(log) — serialize a picotel_log_record environment to the
# OTLP JSON dict format.  Ports Python's _log_to_dict() exactly, including
# timestamp defaulting (0 -> now_ns()) and optional field omission.
#
# log: environment or list with fields:
#   body, timestamp_ns, observed_timestamp_ns, severity_number, severity_text,
#   attributes, trace_id, span_id, trace_flags
.picotel_log_to_list <- function(log) {
  # Use current time when timestamps are 0 (mirrors Python:
  #   str(log.timestamp_ns if log.timestamp_ns else now_ns()))
  ts  <- if (!is.null(log$timestamp_ns) && log$timestamp_ns != 0)
           log$timestamp_ns
         else
           now_ns()
  obs <- if (!is.null(log$observed_timestamp_ns) && log$observed_timestamp_ns != 0)
           log$observed_timestamp_ns
         else
           now_ns()

  result <- list(
    timeUnixNano         = .picotel_ns_str(ts),
    observedTimeUnixNano = .picotel_ns_str(obs),
    severityNumber       = as.integer(log$severity_number),
    body                 = .picotel_to_otlp_value(log$body)
  )

  # severityText — omit when empty
  if (!is.null(log$severity_text) && nchar(log$severity_text) > 0L) {
    result$severityText <- log$severity_text
  }

  # attributes — omit when empty
  attrs <- .picotel_attributes_to_otlp(log$attributes)
  if (length(attrs) > 0L) {
    result$attributes <- attrs
  }

  # traceId — omit when empty
  if (!is.null(log$trace_id) && nchar(log$trace_id) > 0L) {
    result$traceId <- log$trace_id
  }

  # spanId — omit when empty
  if (!is.null(log$span_id) && nchar(log$span_id) > 0L) {
    result$spanId <- log$span_id
  }

  # flags — omit when zero  (mirrors Python: `if log.trace_flags`)
  if (!is.null(log$trace_flags) && log$trace_flags != 0L) {
    result$flags <- as.integer(log$trace_flags)
  }

  result
}

# .picotel_validate_span(span) — check required fields; raise picotel_config_error
# if invalid.  Ports Python's Span._validate() with matching error messages.
#
# Sentinel for "not set": NULL (R) mirrors Python's None.
# In R, 0 could be a legitimate epoch timestamp, so NULL is the correct
# "not set" sentinel — matching picotel_span() constructor defaults.
.picotel_validate_span <- function(span) {
  if (is.null(span$trace_id) || nchar(span$trace_id) == 0L) {
    picotel_config_error("Span invalid: trace_id is empty")
  }
  if (is.null(span$start_time_ns)) {
    picotel_config_error("Span invalid: start_time_ns is not set")
  }
  if (is.null(span$end_time_ns)) {
    picotel_config_error("Span invalid: end_time_ns is not set")
  }
  invisible(NULL)
}

# .picotel_to_json(x) — hand-rolled JSON encoder (plan D1: no jsonlite).
#
# Handles the OTLP value space produced by .picotel_to_otlp_value() and the
# span/log serializers:
#   named list    -> JSON object  {"key": value, ...}
#   unnamed list  -> JSON array   [value, ...]  (bare list() -> "[]")
#   logical TRUE  -> "true";  FALSE -> "false"
#   character     -> JSON string  (with full escaping)
#   numeric (int / double) -> JSON number
#   NULL          -> "null"
#
# Proto3 JSON special values (NaN/Infinity/-Infinity) arrive here as character
# strings from .picotel_to_otlp_value() (e.g. "NaN", "Infinity").  They are
# emitted as JSON strings: {"doubleValue": "NaN"} — matching the proto3 spec.
#
# Convention: bare list() -> "[]" (empty array).  Named empty list cannot occur
# in practice (list() has no names), but would also produce "[]".  Upstream
# serializers always omit empty containers before calling .picotel_to_json, so
# the distinction between empty object and empty array is never load-bearing.
.picotel_to_json <- function(x) {
  if (is.null(x)) {
    return("null")
  }

  # Named list -> JSON object
  if (is.list(x) && !is.null(names(x))) {
    nms <- names(x)
    parts <- character(length(x))
    for (i in seq_along(x)) {
      parts[[i]] <- paste0(
        .picotel_json_string(nms[[i]]),
        ":",
        .picotel_to_json(x[[i]])
      )
    }
    return(paste0("{", paste(parts, collapse = ","), "}"))
  }

  # Unnamed list (including bare list()) -> JSON array
  if (is.list(x)) {
    if (length(x) == 0L) return("[]")
    elems <- vapply(x, .picotel_to_json, character(1L))
    return(paste0("[", paste(elems, collapse = ","), "]"))
  }

  # logical -> true / false  (must come before numeric: is.numeric(TRUE) is FALSE in R)
  if (is.logical(x) && length(x) == 1L) {
    return(if (isTRUE(x)) "true" else "false")
  }

  # character scalar -> JSON string (with escaping)
  if (is.character(x) && length(x) == 1L) {
    return(.picotel_json_string(x))
  }

  # numeric scalar (integer or double)
  if (is.numeric(x) && length(x) == 1L) {
    if (is.nan(x))      return('"NaN"')
    if (is.infinite(x)) return(if (x > 0) '"Infinity"' else '"-Infinity"')
    if (is.integer(x) || (is.double(x) && x == floor(x) && abs(x) < 2^53)) {
      # Whole number: emit without decimal point or scientific notation
      return(sprintf("%.0f", x))
    }
    # Full-precision double (17 sig figs is enough to round-trip any IEEE 754 double)
    return(sprintf("%.17g", x))
  }

  # Fallback: treat as string
  .picotel_json_string(as.character(x))
}

# .picotel_json_string(s) — encode a single R string as a quoted, escaped JSON string.
# Applies all escapes required by RFC 8259:
#   \ -> \\     " -> \"     \b -> \b     \f -> \f
#   \n -> \n    \r -> \r    \t -> \t
#   remaining U+0000-U+001F control chars -> \u00XX
# Internal helper; not exported.
.picotel_json_string <- function(s) {
  # Backslash must be replaced first (otherwise later substitutions
  # would double-escape the backslashes we introduce).
  # With fixed = TRUE, pattern "\" (one backslash as R string "\\") is correct.
  s <- gsub("\\",  "\\\\", s, fixed = TRUE)   # \ -> \\
  s <- gsub('"',   '\\"',  s, fixed = TRUE)   # " -> \"
  s <- gsub("\b",  "\\b",  s, fixed = TRUE)   # backspace       U+0008
  s <- gsub("\f",  "\\f",  s, fixed = TRUE)   # form feed       U+000C
  s <- gsub("\n",  "\\n",  s, fixed = TRUE)   # newline         U+000A
  s <- gsub("\r",  "\\r",  s, fixed = TRUE)   # carriage return U+000D
  s <- gsub("\t",  "\\t",  s, fixed = TRUE)   # tab             U+0009

  # Remaining control characters U+0000-U+001F -> \uXXXX.
  # This handles U+0000-U+0007, U+000B (VT), U+000E-U+001F (i.e. those not
  # already caught by the named-escape substitutions above).
  chars      <- strsplit(s, "", fixed = TRUE)[[1L]]
  codepoints <- utf8ToInt(paste(chars, collapse = ""))
  # Rebuild, replacing any remaining raw control chars with \uXXXX sequences.
  needs_escape <- codepoints <= 0x1FL
  if (any(needs_escape)) {
    chars[needs_escape] <- sprintf("\\u%04X", codepoints[needs_escape])
    s <- paste(chars, collapse = "")
  }

  paste0('"', s, '"')
}


# ==== WP4: Transport, TLS, headers ====


# .picotel_parse_headers() — parse OTEL_EXPORTER_OTLP_HEADERS (key=val,key=val)
# returning a named character vector (possibly empty).
#
# Mirrors Python's _parse_headers() (@functools.lru_cache):
#   - Reads the env var via .picotel_env() so PICOTEL_PREFIX remaps it.
#   - Splits on "," then on "=" (first "=" only, so values may contain "=").
#   - Trims whitespace from both key and value.
#   - Skips pairs that do not contain "=".
#   - Returns a named character vector (names = header keys, values = header
#     values).  Empty character(0) vector when the var is unset or empty.
#
# NOTE: Python's implementation does NOT percent-decode header values (unlike
# resource attributes, which follow the W3C Baggage spec).  Neither does this
# port — values are used verbatim after whitespace trimming.
#
# Caching: mirroring Python's lru_cache, the result is stored in
# .picotel_state$headers and cleared by .picotel_reset_state().
.picotel_parse_headers <- function() {
  if (exists("headers", envir = .picotel_state, inherits = FALSE)) {
    return(.picotel_state$headers)
  }

  headers_str <- Sys.getenv(
    .picotel_env("OTEL_EXPORTER_OTLP_HEADERS"), unset = ""
  )

  result <- if (nchar(headers_str) == 0L) {
    character(0L)
  } else {
    pairs  <- strsplit(headers_str, ",", fixed = TRUE)[[1L]]
    keys   <- character(0L)
    values <- character(0L)
    for (pair in pairs) {
      pair   <- trimws(pair)
      eq_pos <- regexpr("=", pair, fixed = TRUE)[[1L]]
      if (eq_pos < 1L) next
      k    <- trimws(substring(pair, 1L, eq_pos - 1L))
      v    <- trimws(substring(pair, eq_pos + 1L))
      keys   <- c(keys,   k)
      values <- c(values, v)
    }
    if (length(keys) == 0L) character(0L) else setNames(values, keys)
  }

  .picotel_state$headers <- result
  result
}

# .picotel_tls_options(signal) — return a named list of curl handle options
# for TLS (plan D8: pure function, no side effects on handles).
#
# Mirrors Python's _ssl_context() precedence exactly:
#
#   1. PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY (read RAW, never remapped)
#      Truthy = "true" / "True" / "TRUE" / "1" → ssl_verifypeer=0L,
#      ssl_verifyhost=0L, but any configured client cert is STILL loaded.
#      Short-circuits CA lookup entirely.
#
#   2. CA: signal-specific OTEL_EXPORTER_OTLP_{SIGNAL}_CERTIFICATE wins over
#      OTEL_EXPORTER_OTLP_CERTIFICATE (both prefix-remapped via .picotel_env).
#      → curl `cainfo` option (path to PEM file).
#
#   3. mTLS: OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE (+ optional CLIENT_KEY;
#      combined cert+key PEM works with just the cert var, yielding sslkey=NULL
#      which is omitted from the options list).  Signal-agnostic by design
#      (documented deviation — no _TRACES_/_LOGS_ variants, same as Python).
#      Client key without client cert is IGNORED.
#
#   4. Nothing configured → empty list (system trust store / no TLS override).
#
# Scheme gate: this function is scheme-agnostic (pure).  .picotel_post_json
# applies TLS options ONLY for https:// URLs, matching Python's:
#   "context = _ssl_context(signal) if scheme == 'https' else None"
# Keeping the gate in the send path means bad cert paths never break http://.
#
# curl option names used (curl package spellings):
#   cainfo         — path to CA bundle PEM file
#   sslcert        — path to client certificate PEM
#   sslkey         — path to client private key PEM (omitted when NULL)
#   ssl_verifypeer — 0L = skip peer certificate check  (default: on)
#   ssl_verifyhost — 0L = skip hostname check          (default: 2L = verify)
#
# Caching: mirroring Python's lru_cache, the result is stored in
# .picotel_state under key "tls_<signal>" and cleared by .picotel_reset_state().
.picotel_tls_options <- function(signal = "traces") {
  cache_key <- paste0("tls_", signal)
  if (exists(cache_key, envir = .picotel_state, inherits = FALSE)) {
    return(.picotel_state[[cache_key]])
  }

  # --- mTLS client cert (signal-agnostic) ---
  client_cert <- Sys.getenv(
    .picotel_env("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"), unset = ""
  )
  client_key  <- Sys.getenv(
    .picotel_env("OTEL_EXPORTER_OTLP_CLIENT_KEY"), unset = ""
  )
  client_cert <- if (nchar(client_cert) > 0L) client_cert else NULL
  client_key  <- if (nchar(client_key)  > 0L) client_key  else NULL

  # Helper: append client cert options to an existing option list.
  # Key-without-cert is ignored (matches Python: `if client_cert:` guard).
  .with_client_cert <- function(opts) {
    if (!is.null(client_cert)) {
      opts$sslcert <- client_cert
      if (!is.null(client_key)) opts$sslkey <- client_key
    }
    opts
  }

  # --- Skip-verify (raw PICOTEL_ name — never prefix-remapped) ---
  skip_raw <- Sys.getenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", unset = "")
  if (tolower(skip_raw) %in% c("true", "1")) {
    opts <- list(ssl_verifypeer = 0L, ssl_verifyhost = 0L)
    opts <- .with_client_cert(opts)
    .picotel_state[[cache_key]] <- opts
    return(opts)
  }

  # --- CA certificate (signal-specific wins over general) ---
  signal_cert_var <- .picotel_env(
    paste0("OTEL_EXPORTER_OTLP_", toupper(signal), "_CERTIFICATE")
  )
  cafile <- Sys.getenv(signal_cert_var, unset = "")
  if (nchar(cafile) == 0L) {
    cafile <- Sys.getenv(
      .picotel_env("OTEL_EXPORTER_OTLP_CERTIFICATE"), unset = ""
    )
  }
  cafile <- if (nchar(cafile) > 0L) cafile else NULL

  if (!is.null(cafile)) {
    opts <- list(cainfo = cafile)
    opts <- .with_client_cert(opts)
    .picotel_state[[cache_key]] <- opts
    return(opts)
  }

  # --- Client cert only (system trust store for server verification) ---
  if (!is.null(client_cert)) {
    opts <- .with_client_cert(list())
    .picotel_state[[cache_key]] <- opts
    return(opts)
  }

  # Nothing configured → empty list (system defaults)
  .picotel_state[[cache_key]] <- list()
  list()
}

# .picotel_post_json(url, body, timeout, signal) — POST a JSON string to url
# using the curl package, applying TLS options for the given signal.
#
# Parameters:
#   url     — fully-qualified HTTP(S) URL (character scalar)
#   body    — JSON string to POST (character scalar)
#   timeout — request timeout in SECONDS (numeric); curl `timeout` option
#   signal  — OTLP signal type for TLS CA selection: "traces" or "logs"
#
# Returns TRUE on HTTP 200, FALSE on any error or non-200 status.
# NEVER raises — telemetry errors must not crash the application.
#
# Scheme gate: TLS options are applied ONLY for https:// URLs.  A bad cert
# path in OTEL_EXPORTER_OTLP_CERTIFICATE must not break plain http:// sends.
# This mirrors Python's scheme check in send_spans/send_logs:
#   context = _ssl_context(signal) if scheme == "https" else None
#
# curl timeout option: uses `timeout` (seconds) not `timeout_ms` (milliseconds).
# With timeout < 1s round up to 1s (curl `timeout` is integer seconds).
.picotel_post_json <- function(url, body, timeout = 2, signal = "traces") {
  # Lazy-load curl — informative error if unavailable; return FALSE (no raise).
  if (!requireNamespace("curl", quietly = TRUE)) {
    .picotel_log(
      "curl package is required for HTTP sending but is not installed."
    )
    return(FALSE)
  }

  # Build HTTP header vector: Content-Type always present; env headers appended.
  env_headers  <- .picotel_parse_headers()
  http_headers <- "Content-Type: application/json"
  if (length(env_headers) > 0L) {
    http_headers <- c(
      http_headers,
      paste0(names(env_headers), ": ", unname(env_headers))
    )
  }

  # Fresh handle per request (plan hazard: TLS options vary per signal/call).
  h <- curl::new_handle()
  curl::handle_setopt(
    h,
    customrequest = "POST",
    postfields     = body,
    httpheader     = http_headers,
    timeout        = as.integer(max(1L, round(timeout)))
  )

  # Scheme gate: apply TLS options only for https:// endpoints.
  scheme <- tolower(sub("^([a-zA-Z][a-zA-Z0-9+.-]*)://.*$", "\\1", url))
  if (identical(scheme, "https")) {
    tls_opts <- .picotel_tls_options(signal)
    if (length(tls_opts) > 0L) {
      curl::handle_setopt(h, .list = tls_opts)
    }
  }

  # Execute the request; catch all errors (network failure, TLS error, etc.).
  result <- tryCatch(
    curl::curl_fetch_memory(url, handle = h),
    error = function(e) {
      .picotel_log(paste0(
        "Failed to send ", signal, " to ", url, ": ", conditionMessage(e)
      ))
      NULL
    }
  )

  if (is.null(result)) return(FALSE)

  # OTLP spec: only HTTP 200 is a successful export (matches Python exactly).
  if (result$status_code == 200L) return(TRUE)

  .picotel_log(paste0(
    "Failed to send ", signal, " to ", url,
    ": HTTP ", result$status_code
  ))
  FALSE
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
