# Tests for send_spans() and send_logs().
#
# Ported from:
#   tests/test_send_spans.py
#   tests/test_send_logs.py  (TestSendLogs and TestLogToDict sections)
#
# Note: TestLogToDict is covered by test-encode.R (WP3 scope); the cases here
# focus on the HTTP export path.  A few log-to-list cases are repeated here for
# integration clarity where the Python file intermixes them.
#
# Skipped cases:
#   test_send_spans_passes_timeout_to_urlopen — curl `timeout` option is integer
#     seconds (plan D1/WP4); fractional timeouts are rounded up. R's curl
#     transport does not expose the sub-second timeout to assert on; this case
#     is already covered structurally in test-transport.R.

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

.make_resource <- function(name = "test_service") {
  picotel_resource(list("service.name" = name))
}

.make_span <- function(
  trace_id = new_trace_id(),
  name     = "test_operation"
) {
  picotel_span(
    trace_id      = trace_id,
    name          = name,
    start_time_ns = now_ns(),
    end_time_ns   = now_ns() + 1e6
  )
}

# Webfakes routes run in a SUBPROCESS: assigning `received <<- ...` inside a
# route mutates the subprocess copy of the closure environment, never the
# test process.  The reliable pattern: capture the request body to a temp
# file (the path is baked into the route closure at definition time and
# serialised into the subprocess along with the app), then read the file
# back in the test process.  HTTP POST is synchronous — by the time send_*
# returns, the route has run and the file exists.
.read_payload <- function(body_file) {
  if (!file.exists(body_file)) return(NULL)
  jsonlite::fromJSON(
    paste(readLines(body_file, warn = FALSE), collapse = "\n"),
    simplifyVector = FALSE
  )
}

# Make a webfakes server for spans and reset state so circuit breaker is fresh.
.make_traces_server <- function() {
  .picotel_reset_state()
  body_file <- tempfile(fileext = ".json")
  app <- webfakes::new_app()
  app$use(webfakes::mw_text(type = "application/json"))
  app$post("/v1/traces", function(req, res) {
    txt <- req[["text"]]
    if (!is.null(txt) && nzchar(txt)) writeLines(txt, body_file)
    res$set_status(200)$send("")
  })
  srv <- webfakes::local_app_process(app)
  list(srv = srv, received = function() .read_payload(body_file))
}

.make_logs_server <- function() {
  .picotel_reset_state()
  body_file <- tempfile(fileext = ".json")
  app <- webfakes::new_app()
  app$use(webfakes::mw_text(type = "application/json"))
  app$post("/v1/logs", function(req, res) {
    txt <- req[["text"]]
    if (!is.null(txt) && nzchar(txt)) writeLines(txt, body_file)
    res$set_status(200)$send("")
  })
  srv <- webfakes::local_app_process(app)
  list(srv = srv, received = function() .read_payload(body_file))
}

# ---------------------------------------------------------------------------
# send_spans — disabled check
# ---------------------------------------------------------------------------

test_that("send_spans returns FALSE immediately when SDK disabled", {
  withr::local_envvar(OTEL_SDK_DISABLED = "true")
  .picotel_reset_state()
  res <- send_spans("http://localhost:4318", .make_resource(), list(.make_span()))
  expect_false(res)
})

# ---------------------------------------------------------------------------
# send_spans — endpoint resolution
# ---------------------------------------------------------------------------

test_that("send_spans appends /v1/traces to explicit endpoint", {
  s <- .make_traces_server()
  span <- .make_span()
  res <- send_spans(s$srv$url(), .make_resource(), list(span))
  expect_true(res)
})

test_that("send_spans strips trailing slash from explicit endpoint", {
  s <- .make_traces_server()
  res <- send_spans(paste0(s$srv$url(), "/"), .make_resource(), list(.make_span()))
  expect_true(res)
})

test_that("send_spans raises picotel_config_error when no endpoint configured", {
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_ENDPOINT        = NA,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = NA
  )
  .picotel_reset_state()
  expect_error(
    send_spans(NULL, .make_resource(), list(.make_span())),
    class = "picotel_config_error"
  )
})

test_that("send_spans error message mentions env var names", {
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_ENDPOINT        = NA,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = NA
  )
  .picotel_reset_state()
  err <- tryCatch(
    send_spans(NULL, .make_resource(), list(.make_span())),
    picotel_config_error = function(e) e
  )
  expect_match(conditionMessage(err), "OTEL_EXPORTER_OTLP_ENDPOINT")
  expect_match(conditionMessage(err), "OTEL_SDK_DISABLED")
})

test_that("send_spans uses env endpoint when NULL supplied", {
  s <- .make_traces_server()
  withr::local_envvar(OTEL_EXPORTER_OTLP_ENDPOINT = s$srv$url())
  .picotel_reset_state()
  res <- send_spans(NULL, .make_resource(), list(.make_span()))
  expect_true(res)
})

# ---------------------------------------------------------------------------
# send_spans — payload structure (mirrors test_send_spans_basic)
# ---------------------------------------------------------------------------

test_that("send_spans produces correct OTLP payload structure", {
  s <- .make_traces_server()

  trace_id <- new_trace_id()
  span <- picotel_span(
    trace_id      = trace_id,
    name          = "test_operation",
    start_time_ns = now_ns(),
    end_time_ns   = now_ns() + 1e6,
    attributes    = list("test.attribute" = "value")
  )
  res <- send_spans(s$srv$url(), .make_resource(name = "test_service"), list(span))

  expect_true(res)
  received <- s$received()
  expect_false(is.null(received))

  rs <- received$resourceSpans
  expect_length(rs, 1)

  resource_span <- rs[[1]]
  attrs_list <- resource_span$resource$attributes
  attr_map <- setNames(
    lapply(attrs_list, function(a) a$value),
    sapply(attrs_list, function(a) a$key)
  )
  expect_equal(attr_map[["service.name"]]$stringValue, "test_service")

  spans <- resource_span$scopeSpans[[1]]$spans
  expect_length(spans, 1)
  expect_equal(spans[[1]]$traceId, trace_id)
  expect_equal(spans[[1]]$name, "test_operation")
})

test_that("send_spans includes scope when provided", {
  s <- .make_traces_server()

  scope <- picotel_scope("my.library", "2.0.0",
                         attributes = list("library.language" = "r"))
  res <- send_spans(s$srv$url(), .make_resource(), list(.make_span()),
                    scope = scope)

  expect_true(res)
  received <- s$received()
  scope_span <- received$resourceSpans[[1]]$scopeSpans[[1]]
  expect_equal(scope_span$scope$name, "my.library")
  expect_equal(scope_span$scope$version, "2.0.0")
  scope_attrs_list <- scope_span$scope$attributes
  expect_length(scope_attrs_list, 1)
  expect_equal(scope_attrs_list[[1]]$key, "library.language")
  expect_equal(scope_attrs_list[[1]]$value$stringValue, "r")
})

test_that("send_spans sends multiple spans", {
  s <- .make_traces_server()

  trace_id  <- new_trace_id()
  parent_id <- new_span_id()
  parent <- picotel_span(
    trace_id      = trace_id,
    span_id       = parent_id,
    name          = "parent_operation",
    start_time_ns = now_ns(),
    end_time_ns   = now_ns() + 2e6,
    kind          = SpanKind$SERVER
  )
  child <- picotel_span(
    trace_id       = trace_id,
    parent_span_id = parent_id,
    name           = "child_operation",
    start_time_ns  = now_ns() + 5e5,
    end_time_ns    = now_ns() + 1.5e6,
    kind           = SpanKind$CLIENT,
    status         = SpanStatus$OK
  )

  res <- send_spans(s$srv$url(), .make_resource(), list(parent, child))
  expect_true(res)

  received <- s$received()
  spans <- received$resourceSpans[[1]]$scopeSpans[[1]]$spans
  expect_length(spans, 2)

  expect_equal(spans[[1]]$name, "parent_operation")
  expect_equal(spans[[1]]$kind, SpanKind$SERVER)
  expect_null(spans[[1]]$parentSpanId)

  expect_equal(spans[[2]]$name, "child_operation")
  expect_equal(spans[[2]]$kind, SpanKind$CLIENT)
  expect_equal(spans[[2]]$parentSpanId, parent_id)
  expect_equal(spans[[2]]$status$code, SpanStatus$OK)
})

# ---------------------------------------------------------------------------
# send_spans — per-span validation / invalid span drop
# ---------------------------------------------------------------------------

test_that("send_spans skips invalid span but sends valid ones", {
  s <- .make_traces_server()

  valid <- .make_span(name = "valid_span")
  # Construct an invalid span directly (empty trace_id bypasses constructor
  # sentinel logic — mirrors Python Span.__new__(Span)).
  invalid <- picotel_span(
    trace_id      = new_trace_id(),  # will be overwritten
    name          = "invalid_span",
    start_time_ns = now_ns(),
    end_time_ns   = now_ns() + 1e6
  )
  invalid$trace_id <- ""  # make it invalid

  stderr_output <- capture.output({
    res <- send_spans(s$srv$url(), .make_resource(), list(valid, invalid))
  }, type = "message")

  expect_true(res)
  received <- s$received()
  spans <- received$resourceSpans[[1]]$scopeSpans[[1]]$spans
  expect_length(spans, 1)
  expect_equal(spans[[1]]$name, "valid_span")

  expect_true(any(grepl("Span invalid", stderr_output)))
  expect_true(any(grepl("trace_id is empty", stderr_output)))
})

test_that("send_spans sends all-valid payload when no invalid spans", {
  s <- .make_traces_server()
  spans <- list(.make_span(), .make_span())
  expect_true(send_spans(s$srv$url(), .make_resource(), spans))
})

# ---------------------------------------------------------------------------
# send_spans — transport errors return FALSE, not raise
# ---------------------------------------------------------------------------

test_that("send_spans returns FALSE on connection error", {
  .picotel_reset_state()
  res <- send_spans("http://localhost:59999", .make_resource(), list(.make_span()))
  expect_false(res)
})

# ---------------------------------------------------------------------------
# send_logs — disabled check
# ---------------------------------------------------------------------------

test_that("send_logs returns FALSE immediately when SDK disabled", {
  withr::local_envvar(OTEL_SDK_DISABLED = "true")
  .picotel_reset_state()
  log <- picotel_log_record("hello")
  res <- send_logs("http://localhost:4318", .make_resource(), list(log))
  expect_false(res)
})

# ---------------------------------------------------------------------------
# send_logs — endpoint resolution
# ---------------------------------------------------------------------------

test_that("send_logs appends /v1/logs to explicit endpoint", {
  s <- .make_logs_server()
  res <- send_logs(s$srv$url(), .make_resource(), list(picotel_log_record("hi")))
  expect_true(res)
})

test_that("send_logs raises picotel_config_error when no endpoint configured", {
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_ENDPOINT      = NA,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = NA
  )
  .picotel_reset_state()
  expect_error(
    send_logs(NULL, .make_resource(), list(picotel_log_record("hi"))),
    class = "picotel_config_error"
  )
})

# ---------------------------------------------------------------------------
# send_logs — payload structure (mirrors TestSendLogs in test_send_logs.py)
# ---------------------------------------------------------------------------

test_that("send_logs produces correct OTLP payload structure", {
  s <- .make_logs_server()

  logs <- list(
    picotel_log_record("Log 1", severity_number = Severity$INFO),
    picotel_log_record("Log 2", severity_number = Severity$WARN)
  )
  res <- send_logs(s$srv$url(), .make_resource(name = "test-service"), logs)

  expect_true(res)
  received <- s$received()
  expect_false(is.null(received))

  rl <- received$resourceLogs
  expect_length(rl, 1)
  expect_true(!is.null(rl[[1]]$resource))
  expect_true(!is.null(rl[[1]]$scopeLogs))

  log_records <- rl[[1]]$scopeLogs[[1]]$logRecords
  expect_length(log_records, 2)
  expect_equal(log_records[[1]]$body$stringValue, "Log 1")
  expect_equal(log_records[[1]]$severityNumber, Severity$INFO)
})

test_that("send_logs includes scope when provided", {
  s <- .make_logs_server()

  scope <- picotel_scope("my-library", "1.0.0",
                         attributes = list("library.language" = "r"))
  res <- send_logs(s$srv$url(), .make_resource(), list(picotel_log_record("hi")),
                   scope = scope)

  expect_true(res)
  received <- s$received()
  scope_log <- received$resourceLogs[[1]]$scopeLogs[[1]]
  expect_equal(scope_log$scope$name, "my-library")
  expect_equal(scope_log$scope$version, "1.0.0")
})

test_that("send_logs sends trace-correlated log records", {
  s <- .make_logs_server()

  log <- picotel_log_record(
    body        = "Correlated log",
    trace_id    = "abcdef1234567890abcdef1234567890",
    span_id     = "1234567890abcdef",
    trace_flags = 1L
  )
  res <- send_logs(s$srv$url(), .make_resource(), list(log))

  expect_true(res)
  received <- s$received()
  lr <- received$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  expect_equal(lr$traceId, "abcdef1234567890abcdef1234567890")
  expect_equal(lr$spanId, "1234567890abcdef")
  expect_equal(lr$flags, 1)
})

test_that("send_logs returns FALSE on transport error", {
  .picotel_reset_state()
  log <- picotel_log_record("hi")
  res <- send_logs("http://localhost:59999", .make_resource(), list(log))
  expect_false(res)
})

# ---------------------------------------------------------------------------
# prefix-aware error messages in send_spans / send_logs
# ---------------------------------------------------------------------------

test_that("send_spans error message uses prefixed var names when PICOTEL_PREFIX set", {
  withr::local_envvar(
    PICOTEL_PREFIX                        = "PICOTEL",
    PICOTEL_EXPORTER_OTLP_ENDPOINT        = NA,
    PICOTEL_EXPORTER_OTLP_TRACES_ENDPOINT = NA,
    OTEL_EXPORTER_OTLP_ENDPOINT           = NA,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT    = NA
  )
  .picotel_reset_state()
  err <- tryCatch(
    send_spans(NULL, .make_resource(), list(.make_span())),
    picotel_config_error = function(e) e
  )
  expect_match(conditionMessage(err), "PICOTEL_EXPORTER_OTLP_ENDPOINT")
  expect_match(conditionMessage(err), "PICOTEL_SDK_DISABLED")
})
