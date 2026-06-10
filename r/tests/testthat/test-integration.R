# Integration tests for picotel — end-to-end with webfakes HTTP server.
#
# Verifies the full OTLP JSON payload structure for spans and logs, including
# resourceSpans/resourceLogs nesting, attribute encoding, timestamp strings.
#
# Ported from:
#   tests/test_integration.py  (adapted for R: no _span_to_dict unit tests
#    which live in test-encode.R; focus is on HTTP round-trips)
#
# Requires: jsonlite (test-only dep; allowed per plan D1).  It is loaded only
# in this file via requireNamespace.
#
# Skipped cases:
#   test_span_creation_with_generated_ids_and_timestamps (Python) — ID
#     generation and format are covered in test-helpers.R (WP1 scope).
#     The OTLP field format (.picotel_span_to_list) is covered in test-encode.R.
#   Forking / threading async delivery — R async is a deferral queue, not
#     a background thread. Fork-safety is documented as N/A (plan D6).

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

.i_resource <- function(name = "integration-svc", ...) {
  picotel_resource(c(list("service.name" = name), list(...)))
}

.i_log_body <- function(payload, idx = 1) {
  payload$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[idx]]
}

.i_span_body <- function(payload, idx = 1) {
  payload$resourceSpans[[1]]$scopeSpans[[1]]$spans[[idx]]
}

.i_attrs_map <- function(attrs_list) {
  if (is.null(attrs_list) || length(attrs_list) == 0) return(list())
  setNames(
    lapply(attrs_list, function(a) a$value),
    sapply(attrs_list, function(a) a$key)
  )
}

# Webfakes routes run in a SUBPROCESS: `received <<- ...` inside a route only
# mutates the subprocess copy of the closure environment.  Instead the route
# writes the body to a temp file (path captured in the route closure and
# serialised into the subprocess with the app); the test process reads the
# file back.  HTTP POST is synchronous — the file exists when send returns.
.i_make_server <- function(signal = "traces") {
  # Reset state so circuit breaker starts fresh for each test.
  .picotel_reset_state()
  body_file <- tempfile(fileext = ".json")
  app <- webfakes::new_app()
  app$use(webfakes::mw_text(type = "application/json"))
  path <- paste0("/v1/", signal)
  app$post(path, function(req, res) {
    txt <- req[["text"]]
    if (!is.null(txt) && nzchar(txt)) writeLines(txt, body_file)
    res$set_status(200)$send("")
  })
  srv <- webfakes::local_app_process(app)
  list(
    srv      = srv,
    received = function() {
      if (!file.exists(body_file)) return(NULL)
      jsonlite::fromJSON(
        paste(readLines(body_file, warn = FALSE), collapse = "\n"),
        simplifyVector = FALSE
      )
    }
  )
}

# ---------------------------------------------------------------------------
# Full OTLP payload structure — spans
# ---------------------------------------------------------------------------

test_that("send_spans produces correct OTLP resourceSpans nesting", {
  s <- .i_make_server("traces")
  resource <- .i_resource(name = "order-service", "service.version" = "2.1.0")

  trace_id <- new_trace_id()
  start    <- now_ns()
  span     <- picotel_span(
    trace_id      = trace_id,
    name          = "POST /api/orders",
    kind          = SpanKind$SERVER,
    start_time_ns = start,
    end_time_ns   = start + 1e6,
    attributes    = list(
      "http.method"      = "POST",
      "http.route"       = "/api/orders",
      "http.status_code" = 201L
    ),
    status = SpanStatus$OK
  )

  result <- send_spans(s$srv$url(), resource, list(span))
  expect_true(result)

  p <- s$received()
  expect_false(is.null(p))

  # Top-level structure
  expect_true(!is.null(p$resourceSpans))
  expect_length(p$resourceSpans, 1)

  rs <- p$resourceSpans[[1]]
  # resource.attributes is a [{key, value}] list
  res_attr_map <- .i_attrs_map(rs$resource$attributes)
  expect_equal(res_attr_map[["service.name"]]$stringValue,    "order-service")
  expect_equal(res_attr_map[["service.version"]]$stringValue, "2.1.0")

  # scopeSpans nesting
  expect_length(rs$scopeSpans, 1)
  ss <- rs$scopeSpans[[1]]
  expect_null(ss$scope)  # no scope passed

  spans <- ss$spans
  expect_length(spans, 1)
  sp <- spans[[1]]

  expect_equal(sp$traceId, trace_id)
  expect_equal(sp$name,    "POST /api/orders")
  expect_equal(sp$kind,    SpanKind$SERVER)
  expect_null(sp$parentSpanId)  # no parent
  expect_equal(sp$status$code, SpanStatus$OK)

  # Timestamp strings — must be digit-only strings (not scientific notation)
  expect_match(sp$startTimeUnixNano, "^[0-9]+$")
  expect_match(sp$endTimeUnixNano,   "^[0-9]+$")

  # Attribute encoding
  span_attr_map <- .i_attrs_map(sp$attributes)
  expect_equal(span_attr_map[["http.method"]]$stringValue, "POST")
  expect_equal(span_attr_map[["http.route"]]$stringValue,  "/api/orders")
  expect_equal(span_attr_map[["http.status_code"]]$intValue, "201")
})

# ---------------------------------------------------------------------------
# Full OTLP payload structure — logs
# ---------------------------------------------------------------------------

test_that("send_logs produces correct OTLP resourceLogs nesting", {
  s <- .i_make_server("logs")
  resource <- .i_resource(name = "payment-service")

  log <- picotel_log_record(
    body            = "Payment declined",
    severity_number = Severity$ERROR,
    severity_text   = "ERROR",
    trace_id        = "abcdef1234567890abcdef1234567890",
    span_id         = "1234567890abcdef",
    trace_flags     = 1L,
    attributes      = list(
      "payment.provider" = "stripe",
      "payment.amount"   = 99.99
    )
  )

  result <- send_logs(s$srv$url(), resource, list(log))
  expect_true(result)

  p <- s$received()
  expect_false(is.null(p))

  expect_true(!is.null(p$resourceLogs))
  rl <- p$resourceLogs[[1]]

  res_attr_map <- .i_attrs_map(rl$resource$attributes)
  expect_equal(res_attr_map[["service.name"]]$stringValue, "payment-service")

  expect_length(rl$scopeLogs, 1)
  lr <- rl$scopeLogs[[1]]$logRecords[[1]]

  expect_equal(lr$body$stringValue, "Payment declined")
  expect_equal(lr$severityNumber,   Severity$ERROR)
  expect_equal(lr$severityText,     "ERROR")
  expect_equal(lr$traceId, "abcdef1234567890abcdef1234567890")
  expect_equal(lr$spanId,  "1234567890abcdef")
  expect_equal(lr$flags,   1)

  # Timestamps are digit strings
  expect_match(lr$timeUnixNano,         "^[0-9]+$")
  expect_match(lr$observedTimeUnixNano, "^[0-9]+$")

  log_attr_map <- .i_attrs_map(lr$attributes)
  expect_equal(log_attr_map[["payment.provider"]]$stringValue, "stripe")
  # 99.99 is a non-whole double → doubleValue
  expect_equal(log_attr_map[["payment.amount"]]$doubleValue, 99.99)
})

# ---------------------------------------------------------------------------
# with_span — happy path end-to-end
# ---------------------------------------------------------------------------

test_that("with_span happy path delivers correct span payload", {
  s <- .i_make_server("traces")
  resource <- .i_resource(name = "my-service")
  trace_id <- new_trace_id()

  result <- with_span(
    trace_id,
    "my-operation",
    endpoint = s$srv$url(),
    resource = resource,
    f = function(span) {
      span$attributes$key <- "value"
      "done"
    }
  )

  expect_equal(result, "done")
  p <- s$received()
  expect_false(is.null(p))

  sp <- .i_span_body(p)
  expect_equal(sp$traceId, trace_id)
  expect_equal(sp$name,    "my-operation")
  # start/end timestamps must be valid nanosecond digit strings
  expect_match(sp$startTimeUnixNano, "^[0-9]+$")
  expect_match(sp$endTimeUnixNano,   "^[0-9]+$")
  # end >= start
  expect_gte(as.numeric(sp$endTimeUnixNano), as.numeric(sp$startTimeUnixNano))
  # attribute set inside f
  attr_map <- .i_attrs_map(sp$attributes)
  expect_equal(attr_map[["key"]]$stringValue, "value")
})

# ---------------------------------------------------------------------------
# with_span — error path: rethrow + span still sent + endTime set
# ---------------------------------------------------------------------------

test_that("with_span error path: span sent, error rethrown, endTime set", {
  s <- .i_make_server("traces")
  resource <- .i_resource(name = "error-svc")
  result_span <- NULL

  expect_error(
    with_span(
      new_trace_id(),
      "failing-op",
      endpoint = s$srv$url(),
      resource = resource,
      f = function(span) {
        result_span <<- span
        stop("deliberate error")
      }
    ),
    "deliberate error"
  )

  # Span must have been sent.
  p <- s$received()
  expect_false(is.null(p))
  sp <- .i_span_body(p)
  expect_equal(sp$name, "failing-op")
  # endTime was set on exit
  expect_match(sp$endTimeUnixNano, "^[0-9]+$")
  # Status NOT automatically set to ERROR (matches Python __exit__)
  expect_null(sp$status)
})

# ---------------------------------------------------------------------------
# Parent/child spans
# ---------------------------------------------------------------------------

test_that("parent/child spans carry correct traceId and parentSpanId", {
  .picotel_reset_state()
  # Multi-request capture across the subprocess boundary: the route keeps a
  # per-subprocess counter (the <<- copy persists between requests inside the
  # one server process) and writes request N to "<base>.N"; the test process
  # reads the numbered files back.
  body_base     <- tempfile()
  request_count <- 0L
  app <- webfakes::new_app()
  app$use(webfakes::mw_text(type = "application/json"))
  app$post("/v1/traces", function(req, res) {
    request_count <<- request_count + 1L
    txt <- req[["text"]]
    if (!is.null(txt) && nzchar(txt)) {
      writeLines(txt, paste0(body_base, ".", request_count))
    }
    res$set_status(200)$send("")
  })
  srv <- webfakes::local_app_process(app)
  resource <- .i_resource(name = "nested-svc")

  trace_id <- new_trace_id()

  with_span(
    trace_id,
    "parent-op",
    endpoint = srv$url(),
    resource = resource,
    f = function(parent_span) {
      with_span(
        trace_id,
        "child-op",
        parent_span_id = parent_span$span_id,
        endpoint       = srv$url(),
        resource       = resource,
        f = function(child_span) {
          child_span$attributes$level <- "child"
        }
      )
      parent_span$attributes$level <- "parent"
    }
  )

  received_trace <- lapply(
    paste0(body_base, ".", 1:2),
    function(f) {
      if (!file.exists(f)) return(NULL)
      jsonlite::fromJSON(
        paste(readLines(f, warn = FALSE), collapse = "\n"),
        simplifyVector = FALSE
      )
    }
  )

  # Sync sender: each with_span sends immediately on exit (inner first).
  expect_false(is.null(received_trace[[1]]))
  expect_false(is.null(received_trace[[2]]))
  expect_false(file.exists(paste0(body_base, ".3")))  # exactly two requests

  # First request = inner (child) span
  child_sp  <- received_trace[[1]]$resourceSpans[[1]]$scopeSpans[[1]]$spans[[1]]
  parent_sp <- received_trace[[2]]$resourceSpans[[1]]$scopeSpans[[1]]$spans[[1]]

  expect_equal(child_sp$name,    "child-op")
  expect_equal(parent_sp$name,   "parent-op")

  expect_equal(child_sp$traceId,  trace_id)
  expect_equal(parent_sp$traceId, trace_id)

  expect_equal(child_sp$parentSpanId, parent_sp$spanId)
  expect_null(parent_sp$parentSpanId)
})

# ---------------------------------------------------------------------------
# Env-only configuration (no explicit endpoint)
# ---------------------------------------------------------------------------

test_that("send_spans uses env endpoint exclusively (no explicit endpoint)", {
  s <- .i_make_server("traces")

  withr::local_envvar(OTEL_EXPORTER_OTLP_ENDPOINT = s$srv$url())
  .picotel_reset_state()

  resource <- .i_resource(name = "env-only-svc")
  span <- picotel_span(new_trace_id(), "env-span",
                       start_time_ns = now_ns(), end_time_ns = now_ns() + 1e6)

  result <- send_spans(NULL, resource, list(span))
  expect_true(result)
  expect_false(is.null(s$received()))
})

test_that("send_spans and send_logs both work with fully env-configured client", {
  .picotel_reset_state()
  # Both signals resolve from signal-specific env vars (separate servers).
  # Subprocess boundary: each route writes its body to a temp file.
  spans_file <- tempfile(fileext = ".json")
  logs_file  <- tempfile(fileext = ".json")

  spans_app <- webfakes::new_app()
  spans_app$use(webfakes::mw_text(type = "application/json"))
  spans_app$post("/v1/traces", function(req, res) {
    txt <- req[["text"]]
    if (!is.null(txt) && nzchar(txt)) writeLines(txt, spans_file)
    res$set_status(200)$send("")
  })
  logs_app <- webfakes::new_app()
  logs_app$use(webfakes::mw_text(type = "application/json"))
  logs_app$post("/v1/logs", function(req, res) {
    txt <- req[["text"]]
    if (!is.null(txt) && nzchar(txt)) writeLines(txt, logs_file)
    res$set_status(200)$send("")
  })

  # Use a shared base URL pointing to both servers is not possible in webfakes;
  # instead test each signal via its signal-specific env var.
  spans_srv <- webfakes::local_app_process(spans_app)
  logs_srv  <- webfakes::local_app_process(logs_app)

  # Signal-specific endpoint vars are used VERBATIM (no /v1/<signal> appended,
  # matching the OTEL spec and Python picotel) — pass the full path.
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = spans_srv$url("/v1/traces"),
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT   = logs_srv$url("/v1/logs"),
    OTEL_SERVICE_NAME                  = "full-env-svc"
  )
  .picotel_reset_state()

  resource <- .picotel_resource_from_env()
  span <- picotel_span(new_trace_id(), "trace-op",
                       start_time_ns = now_ns(), end_time_ns = now_ns() + 1e6)
  log  <- picotel_log_record("log msg")

  expect_true(send_spans(NULL, resource, list(span)))
  expect_true(send_logs(NULL, resource, list(log)))

  expect_true(file.exists(spans_file))
  expect_true(file.exists(logs_file))
})

# ---------------------------------------------------------------------------
# PICOTEL_ASYNC=true end-to-end:
#   with_span enqueues, nothing hits server until picotel_flush()
# ---------------------------------------------------------------------------

test_that("PICOTEL_ASYNC=true: with_span enqueues; flush delivers to server", {
  # Create server first (which resets state), then set async env and reset again.
  s <- .i_make_server("traces")

  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_reset_state()  # reset again so async mode is picked up fresh

  resource <- .i_resource(name = "async-svc")

  with_span(
    new_trace_id(),
    "async-op",
    endpoint = s$srv$url(),
    resource = resource,
    f = function(span) span$attributes$mode <- "async"
  )

  # Immediately after with_span: nothing should have hit the server yet
  # (async sender only defers — transfers happen in flush).
  expect_null(s$received())

  # After flush: payload should have arrived.
  picotel_flush(timeout = 2)
  expect_false(is.null(s$received()))

  sp <- .i_span_body(s$received())
  expect_equal(sp$name, "async-op")
  attr_map <- .i_attrs_map(sp$attributes)
  expect_equal(attr_map[["mode"]]$stringValue, "async")
})

# ---------------------------------------------------------------------------
# Scope in payload
# ---------------------------------------------------------------------------

test_that("scope name/version appear in scopeSpans payload", {
  s <- .i_make_server("traces")
  resource <- .i_resource()
  scope    <- picotel_scope("io.opentelemetry.r", "1.0.0")

  span <- picotel_span(new_trace_id(), "scoped",
                       start_time_ns = now_ns(), end_time_ns = now_ns() + 1e6)
  send_spans(s$srv$url(), resource, list(span), scope = scope)

  p  <- s$received()
  ss <- p$resourceSpans[[1]]$scopeSpans[[1]]
  expect_equal(ss$scope$name,    "io.opentelemetry.r")
  expect_equal(ss$scope$version, "1.0.0")
})

# ---------------------------------------------------------------------------
# Complex span with events and links
# ---------------------------------------------------------------------------

test_that("complex span with events and links serialises correctly", {
  s <- .i_make_server("traces")
  resource <- .i_resource()

  trace_id   <- new_trace_id()
  start_time <- now_ns()
  event_time <- start_time + 5e5
  end_time   <- start_time + 2e6

  span <- picotel_span(
    trace_id      = trace_id,
    name          = "complex-op",
    kind          = SpanKind$SERVER,
    start_time_ns = start_time,
    end_time_ns   = end_time,
    attributes    = list(
      "http.method"       = "POST",
      "http.status_code"  = 201L,
      "user.authenticated"= TRUE
    ),
    events = list(
      list(name = "cache.hit", timestamp_ns = event_time,
           attributes = list("cache.key" = "user:123"))
    ),
    status = SpanStatus$OK
  )

  result <- send_spans(s$srv$url(), resource, list(span))
  expect_true(result)

  sp <- .i_span_body(s$received())
  expect_equal(sp$kind, SpanKind$SERVER)
  expect_equal(sp$status$code, SpanStatus$OK)

  span_attr_map <- .i_attrs_map(sp$attributes)
  expect_equal(span_attr_map[["http.method"]]$stringValue,        "POST")
  expect_equal(span_attr_map[["http.status_code"]]$intValue,      "201")
  expect_true(span_attr_map[["user.authenticated"]]$boolValue)

  expect_length(sp$events, 1)
  ev <- sp$events[[1]]
  expect_equal(ev$name, "cache.hit")
  expect_match(ev$timeUnixNano, "^[0-9]+$")
  ev_attr_map <- .i_attrs_map(ev$attributes)
  expect_equal(ev_attr_map[["cache.key"]]$stringValue, "user:123")
})
