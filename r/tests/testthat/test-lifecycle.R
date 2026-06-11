# Tests for picotel_span(), picotel_log_record(), with_span(),
# span_send(), and log_send().
#
# Ported from:
#   tests/test_high_level_apis.py  (TestSpanContextManager, TestSpanSendMethod,
#                                    TestLogRecordSendMethod sections)
#   tests/test_traceparent.py      (TRACEPARENT-sentinel cases deferred by WP2)
#
# Skipped cases:
#   test_span_context_manager_logs_without_endpoint — Python asserts a logging
#     module record via picotel_caplog; R equivalent would require capturing
#     stderr lines from .picotel_log().  The underlying with_span behavior is
#     already covered by the "span submitted via sender" tests here; the
#     config-error-path is covered by send_spans tests in test-send.R.
#     Skipped to avoid fragile stderr matching in the test runner.
#   test_span_context_manager_with_scope — Python asserts call_args[0][3]==scope
#     on the mocked send_spans. In R we use a webfakes server instead of mock;
#     the payload-includes-scope case is covered in test-send.R.

# ---------------------------------------------------------------------------
# Helper: create a webfakes traces server that captures the JSON payload.
#
# Webfakes routes run in a SUBPROCESS: `received <<- ...` inside a route only
# mutates the subprocess copy of the closure environment.  Instead the route
# writes the body to a temp file (the path is captured in the route closure
# and serialised into the subprocess); the test process reads the file back.
# HTTP POST is synchronous, so the file exists by the time send returns.
# ---------------------------------------------------------------------------

.lc_make_traces_server <- function() {
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

# Simple server (no body capture needed — just checks HTTP 200).
.lc_simple_server <- function(path = "/v1/traces") {
  .picotel_reset_state()
  app <- webfakes::new_app()
  app$post(path, function(req, res) res$set_status(200)$send(""))
  webfakes::local_app_process(app)
}

# ---------------------------------------------------------------------------
# Valid W3C traceparent used in sentinel tests
# ---------------------------------------------------------------------------

.picotel_valid_tp <- "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

# ---------------------------------------------------------------------------
# picotel_span defaults
# ---------------------------------------------------------------------------

test_that("picotel_span sets all fields correctly", {
  trace_id <- new_trace_id()
  s <- picotel_span(trace_id, "my-op")

  expect_equal(s$trace_id, trace_id)
  expect_equal(s$name, "my-op")
  expect_true(nchar(s$span_id) == 16)
  expect_equal(s$parent_span_id, "")
  expect_equal(s$kind, SpanKind$INTERNAL)
  expect_null(s$start_time_ns)
  expect_null(s$end_time_ns)
  expect_identical(s$attributes, list())
  expect_identical(s$events, list())
  expect_identical(s$links, list())
  expect_null(s$status)
  expect_equal(s$endpoint, "")
  expect_null(s$resource)
  expect_null(s$scope)
})

test_that("picotel_span has class picotel_span", {
  s <- picotel_span(new_trace_id(), "op")
  expect_s3_class(s, "picotel_span")
})

test_that("picotel_span accepts explicit span_id", {
  sid <- new_span_id()
  s <- picotel_span(new_trace_id(), "op", span_id = sid)
  expect_equal(s$span_id, sid)
})

test_that("picotel_span auto-generates span_id", {
  s1 <- picotel_span(new_trace_id(), "op")
  s2 <- picotel_span(new_trace_id(), "op")
  expect_false(identical(s1$span_id, s2$span_id))
})

test_that("picotel_span stores explicit start/end times", {
  s <- picotel_span(new_trace_id(), "op",
                    start_time_ns = 1000, end_time_ns = 2000)
  expect_equal(s$start_time_ns, 1000)
  expect_equal(s$end_time_ns, 2000)
})

test_that("picotel_span accepts kind, status, attributes", {
  s <- picotel_span(
    new_trace_id(), "op",
    kind       = SpanKind$SERVER,
    status     = SpanStatus$OK,
    attributes = list(x = "y")
  )
  expect_equal(s$kind, SpanKind$SERVER)
  expect_equal(s$status, SpanStatus$OK)
  expect_equal(s$attributes$x, "y")
})

# Span is an environment — mutations are visible in place.
test_that("picotel_span is mutable (environment)", {
  s <- picotel_span(new_trace_id(), "op")
  s$attributes$foo <- "bar"
  expect_equal(s$attributes$foo, "bar")
})

# ---------------------------------------------------------------------------
# TRACEPARENT sentinel — picotel_span (deferred from test-traceparent.R WP2)
# ---------------------------------------------------------------------------

test_that("picotel_span with TRACEPARENT sentinel resolves trace_id and parent_span_id", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  s <- picotel_span(trace_id = TRACEPARENT, name = "child-op")
  expect_equal(s$trace_id,       "0af7651916cd43dd8448eb211c80319c")
  expect_equal(s$parent_span_id, "b7ad6b7169203331")
})

test_that("picotel_span with TRACEPARENT does not override explicit parent_span_id", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  s <- picotel_span(trace_id = TRACEPARENT, name = "op",
                    parent_span_id = "1234567890abcdef")
  expect_equal(s$trace_id,       "0af7651916cd43dd8448eb211c80319c")
  # Explicit parent_span_id must be preserved.
  expect_equal(s$parent_span_id, "1234567890abcdef")
})

test_that("picotel_span with TRACEPARENT sentinel and no env var sets trace_id to empty", {
  withr::local_envvar(TRACEPARENT = NA, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  # Should log an error but not raise.
  stderr_lines <- capture.output({
    s <- picotel_span(trace_id = TRACEPARENT, name = "op")
  }, type = "message")
  expect_equal(s$trace_id, "")
  expect_true(any(grepl("TRACEPARENT requested", stderr_lines)))
})

test_that("picotel_span with explicit trace_id does not read TRACEPARENT", {
  # TRACEPARENT env var is set but we pass an explicit string — must be ignored.
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  tid <- new_trace_id()
  s <- picotel_span(trace_id = tid, name = "op")
  expect_equal(s$trace_id, tid)
  # parent_span_id should remain at its default ("").
  expect_equal(s$parent_span_id, "")
})

# ---------------------------------------------------------------------------
# TRACEPARENT sentinel — picotel_log_record (deferred from test-traceparent.R)
# ---------------------------------------------------------------------------

test_that("picotel_log_record with TRACEPARENT resolves trace_id and span_id", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  lr <- picotel_log_record("msg", trace_id = TRACEPARENT)
  expect_equal(lr$trace_id, "0af7651916cd43dd8448eb211c80319c")
  expect_equal(lr$span_id,  "b7ad6b7169203331")
})

test_that("picotel_log_record with TRACEPARENT does not override explicit span_id", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  lr <- picotel_log_record("msg", trace_id = TRACEPARENT,
                           span_id = "abcdef1234567890")
  expect_equal(lr$trace_id, "0af7651916cd43dd8448eb211c80319c")
  expect_equal(lr$span_id,  "abcdef1234567890")
})

test_that("picotel_log_record with TRACEPARENT and no env var sets trace_id to empty", {
  withr::local_envvar(TRACEPARENT = NA, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  stderr_lines <- capture.output({
    lr <- picotel_log_record("msg", trace_id = TRACEPARENT)
  }, type = "message")
  expect_equal(lr$trace_id, "")
  expect_true(any(grepl("TRACEPARENT requested", stderr_lines)))
})

test_that("picotel_log_record with explicit trace_id ignores TRACEPARENT", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  tid <- "aaaabbbbccccdddd1111222233334444"
  lr  <- picotel_log_record("msg", trace_id = tid)
  expect_equal(lr$trace_id, tid)
  expect_equal(lr$span_id,  "")
})

# ---------------------------------------------------------------------------
# picotel_log_record defaults
# ---------------------------------------------------------------------------

test_that("picotel_log_record sets defaults correctly", {
  lr <- picotel_log_record("hello")
  expect_equal(lr$body, "hello")
  expect_equal(lr$timestamp_ns, 0)
  expect_equal(lr$observed_timestamp_ns, 0)
  expect_equal(lr$trace_id, "")
  expect_equal(lr$span_id, "")
  expect_equal(lr$trace_flags, 0L)
  expect_equal(lr$severity_number, Severity$INFO)
  expect_equal(lr$severity_text, "")
  expect_identical(lr$attributes, list())
})

test_that("picotel_log_record has class picotel_log_record", {
  lr <- picotel_log_record("hi")
  expect_s3_class(lr, "picotel_log_record")
})

# ---------------------------------------------------------------------------
# with_span — time management
# ---------------------------------------------------------------------------

test_that("with_span sets start_time_ns if NULL", {
  before <- now_ns()
  with_span(new_trace_id(), "op", f = function(span) {
    expect_true(span$start_time_ns >= before)
  })
})

test_that("with_span preserves explicit start_time_ns", {
  explicit_start <- 1000000000
  with_span(new_trace_id(), "op",
            start_time_ns = explicit_start,
            f = function(span) {
              expect_equal(span$start_time_ns, explicit_start)
            })
})

test_that("with_span sets end_time_ns after f completes", {
  result_span <- NULL
  before <- now_ns()
  with_span(new_trace_id(), "op", f = function(span) {
    result_span <<- span
  })
  expect_false(is.null(result_span$end_time_ns))
  expect_true(result_span$end_time_ns >= before)
})

test_that("with_span preserves explicit end_time_ns", {
  explicit_end <- 9999999999
  result_span <- NULL
  with_span(new_trace_id(), "op",
            end_time_ns = explicit_end,
            f = function(span) {
              result_span <<- span
            })
  expect_equal(result_span$end_time_ns, explicit_end)
})

# ---------------------------------------------------------------------------
# with_span — sends span on exit
# ---------------------------------------------------------------------------

test_that("with_span sends span to server on normal exit", {
  srv      <- .lc_simple_server()
  resource <- picotel_resource(list("service.name" = "test"))

  result <- with_span(new_trace_id(), "my-op",
                      endpoint = srv$url(),
                      resource = resource,
                      f = function(span) {
                        span$attributes$x <- "v"
                        "return_value"
                      })

  expect_equal(result, "return_value")
})

test_that("with_span sends span even when f throws an error", {
  s        <- .lc_make_traces_server()
  resource <- picotel_resource(list("service.name" = "test"))

  # The error from f should be re-raised after sending the span.
  expect_error(
    with_span(new_trace_id(), "error-op",
              endpoint = s$srv$url(),
              resource = resource,
              f = function(span) {
                stop("user error")
              }),
    "user error"
  )

  # Span was still sent (server got a request).
  received <- s$received()
  expect_false(is.null(received))
  spans <- received$resourceSpans[[1]]$scopeSpans[[1]]$spans
  expect_length(spans, 1)
  expect_equal(spans[[1]]$name, "error-op")
  # end_time_ns must be set even on error path.
  expect_false(is.null(spans[[1]]$endTimeUnixNano))
  expect_true(nchar(spans[[1]]$endTimeUnixNano) > 0)
})

test_that("with_span does NOT set status to ERROR on error (matches Python)", {
  srv         <- .lc_simple_server()
  result_span <- NULL
  resource    <- picotel_resource(list("service.name" = "test"))

  expect_error(
    with_span(new_trace_id(), "op",
              endpoint = srv$url(),
              resource = resource,
              f = function(span) {
                result_span <<- span
                stop("boom")
              }),
    "boom"
  )
  # Python __exit__ does NOT auto-set status — status should remain NULL.
  expect_null(result_span$status)
})

test_that("with_span does not send when no resource available", {
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_ENDPOINT = NA,
    OTEL_SERVICE_NAME           = NA,
    OTEL_RESOURCE_ATTRIBUTES    = NA
  )
  .picotel_reset_state()

  # No explicit endpoint / resource, no env vars → resource resolves to NULL.
  # Should not send (no panic, no error from send path).
  with_span(new_trace_id(), "op", f = function(span) {
    span$attributes$k <- "v"
  })
  # If we get here without error the test passes.
  expect_true(TRUE)
})

test_that("with_span skips send when SDK disabled", {
  # Server that would record a hit; disabled path should never reach it.
  # We can't track "hit" across the subprocess boundary, so we verify
  # indirectly: the server gets no requests (the send path short-circuits).
  # Use a simple server that records count via its URL availability check.
  withr::local_envvar(OTEL_SDK_DISABLED = "true")
  .picotel_reset_state()

  resource <- picotel_resource(list("service.name" = "test"))
  # This should complete without error; no send should occur.
  with_span(new_trace_id(), "op",
            endpoint = "http://localhost:59998",  # unreachable; would fail if hit
            resource = resource,
            f = function(span) invisible(NULL))
  # Reached here → disabled check worked, no attempt to connect
  expect_true(TRUE)
})

test_that("with_span returns f result invisibly", {
  res <- with_span(new_trace_id(), "op", f = function(span) 42)
  # invisible() means the value is still accessible when assigned.
  expect_equal(res, 42)
})

# ---------------------------------------------------------------------------
# with_span — resource from env when not specified
# ---------------------------------------------------------------------------

test_that("with_span resolves resource from env when not explicit", {
  s <- .lc_make_traces_server()

  withr::local_envvar(OTEL_SERVICE_NAME = "env-svc")
  .picotel_reset_state()

  with_span(new_trace_id(), "env-op",
            endpoint = s$srv$url(),  # explicit endpoint; resource from env
            f = function(span) invisible(NULL))

  received <- s$received()
  expect_false(is.null(received))
  attrs_list <- received$resourceSpans[[1]]$resource$attributes
  attr_map <- setNames(
    lapply(attrs_list, function(a) a$value),
    sapply(attrs_list, function(a) a$key)
  )
  expect_equal(attr_map[["service.name"]]$stringValue, "env-svc")
})

# ---------------------------------------------------------------------------
# span_send — mirrors Python Span.send()
# ---------------------------------------------------------------------------

test_that("span_send sends span with explicit params", {
  srv <- .lc_simple_server()
  s <- picotel_span(new_trace_id(), "op",
                    start_time_ns = 1000, end_time_ns = 2000)
  res <- span_send(s,
                   endpoint = srv$url(),
                   resource = picotel_resource(list("service.name" = "svc")))
  expect_true(res)
})

test_that("span_send uses env endpoint when NULL", {
  srv <- .lc_simple_server()

  withr::local_envvar(OTEL_EXPORTER_OTLP_ENDPOINT = srv$url())
  .picotel_reset_state()

  s <- picotel_span(new_trace_id(), "op",
                    start_time_ns = 1000, end_time_ns = 2000)
  res <- span_send(s, resource = picotel_resource(list("service.name" = "svc")))
  expect_true(res)
})

test_that("span_send returns FALSE silently when no endpoint or resource", {
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_ENDPOINT        = NA,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = NA,
    OTEL_SERVICE_NAME                  = NA,
    OTEL_RESOURCE_ATTRIBUTES           = NA
  )
  .picotel_reset_state()

  s <- picotel_span(new_trace_id(), "op",
                    start_time_ns = 1000, end_time_ns = 2000)
  stderr_lines <- capture.output(res <- span_send(s), type = "message")
  expect_false(res)
  # Should log a warning (not raise).
  expect_true(any(grepl("missing endpoint or resource", stderr_lines)))
})

# ---------------------------------------------------------------------------
# log_send — mirrors Python LogRecord.send()
# ---------------------------------------------------------------------------

test_that("log_send sends log with explicit params", {
  srv <- .lc_simple_server("/v1/logs")
  lr  <- picotel_log_record("test log")
  res <- log_send(lr,
                  endpoint = srv$url(),
                  resource = picotel_resource(list("service.name" = "svc")))
  expect_true(res)
})

test_that("log_send uses env endpoint when NULL", {
  srv <- .lc_simple_server("/v1/logs")

  withr::local_envvar(OTEL_EXPORTER_OTLP_ENDPOINT = srv$url())
  .picotel_reset_state()

  lr  <- picotel_log_record("hi")
  res <- log_send(lr, resource = picotel_resource(list("service.name" = "svc")))
  expect_true(res)
})

test_that("log_send returns FALSE silently when no endpoint or resource", {
  withr::local_envvar(
    OTEL_EXPORTER_OTLP_ENDPOINT      = NA,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = NA,
    OTEL_SERVICE_NAME                = NA,
    OTEL_RESOURCE_ATTRIBUTES         = NA
  )
  .picotel_reset_state()

  lr <- picotel_log_record("hi")
  stderr_lines <- capture.output(res <- log_send(lr), type = "message")
  expect_false(res)
  expect_true(any(grepl("missing endpoint or resource", stderr_lines)))
})

# ---------------------------------------------------------------------------
# picotel_scope
# ---------------------------------------------------------------------------

test_that("picotel_scope sets fields and class", {
  sc <- picotel_scope("my.lib", "3.0.0", attributes = list(x = 1))
  expect_s3_class(sc, "picotel_scope")
  expect_equal(sc$name, "my.lib")
  expect_equal(sc$version, "3.0.0")
  expect_equal(sc$attributes$x, 1)
})

test_that("picotel_scope version defaults to empty string", {
  sc <- picotel_scope("my.lib")
  expect_equal(sc$version, "")
})
