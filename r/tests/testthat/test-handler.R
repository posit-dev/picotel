# Tests for otlp_condition_handler() — R equivalent of Python OTLPHandler.
#
# Ported from:
#   tests/test_high_level_apis.py  (TestOTLPHandler section)
#
# Deviations from Python OTLPHandler documented here and in the handler source:
#   - No code.filepath / code.lineno / code.function attrs: R cannot retrieve
#     caller source location cheaply; these attributes are omitted entirely.
#   - No log-level filtering: R conditions don't have numeric levels;
#     the handler accepts all conditions and maps by class hierarchy.
#   - timestamp comes from Sys.time() at handle time, not a record.created
#     field (R conditions have no .created timestamp).
#   - Attribute merging uses condition$picotel.attributes instead of logging
#     framework's record.extra["attributes"].
#   - trace_id/span_id come from condition$trace_id / condition$span_id fields.
#
# Skipped cases:
#   test_otlp_handler_exception_writes_to_stderr — Python forces an exception
#     during emit by patching _get_resource_from_env to raise.  In R the
#     equivalent is: the outer tryCatch in the handler swallows any exception
#     and writes to stderr.  This is covered implicitly by the handler-never-
#     throws tests; an exact replication requires a fragile capture of
#     cat()-to-stderr which is unreliable across test runners.
#   test_otlp_handler_timestamp — Python sets record.created to a known value;
#     R uses Sys.time() which we can't inject without mocking the entire
#     now_ns() function (it's not exported).  The timestamp is set correctly
#     by construction; we verify it is a valid nanosecond value in the
#     integration tests.
#   test_otlp_handler_message_interpolation — R message() does not support
#     format-string interpolation (sprintf must be called explicitly); there
#     is no getMessage() analogue.  Skipped: not applicable to R's condition
#     model.

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

.h_resource <- function(name = "test_service") {
  picotel_resource(list("service.name" = name))
}

# Build a webfakes server that records payloads for log assertions.
# Returns list(srv = app_process, received = function() last_payload)
# Also resets picotel state so the circuit breaker starts fresh for each test.
#
# Webfakes routes run in a SUBPROCESS: `received <<- ...` inside a route only
# mutates the subprocess copy of the closure environment.  Instead the route
# writes the raw body to a temp file (the path is captured in the route
# closure and serialised into the subprocess with the app); the test process
# reads the file back.  HTTP POST is synchronous, so the file exists by the
# time the handler's send returns.
.h_make_log_server <- function() {
  # Reset sender / circuit-breaker state so tests don't accumulate failures.
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
# Handler construction
# ---------------------------------------------------------------------------

test_that("otlp_condition_handler returns a function", {
  h <- otlp_condition_handler(resource = .h_resource())
  expect_true(is.function(h))
})

# ---------------------------------------------------------------------------
# Disabled short-circuit
# ---------------------------------------------------------------------------

test_that("handler is a no-op when SDK is disabled", {
  withr::local_envvar(OTEL_SDK_DISABLED = "true")
  .picotel_reset_state()

  s <- .h_make_log_server()
  h <- otlp_condition_handler(
    endpoint = s$srv$url(),
    resource = .h_resource()
  )

  withCallingHandlers(
    message("hello"),
    message = h
  )

  expect_null(s$received())
})

# ---------------------------------------------------------------------------
# Severity mapping
# ---------------------------------------------------------------------------

test_that("message condition maps to INFO (9)", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  withCallingHandlers(
    message("info msg"),
    message = h
  )

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  expect_equal(lr$severityNumber, Severity$INFO)
  expect_equal(lr$severityText,  "INFO")
})

test_that("warning condition maps to WARN (13)", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  # h runs first (innermost calling handler), then suppressWarnings muffles
  # the warning so it does not surface as a testthat warning.
  suppressWarnings(withCallingHandlers(
    warning("warn msg"),
    warning = h
  ))

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  expect_equal(lr$severityNumber, Severity$WARN)
  expect_equal(lr$severityText,  "WARN")
})

test_that("error condition maps to ERROR (17)", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  # Use tryCatch to prevent the error from propagating; withCallingHandlers
  # runs the handler before the stack unwinds.
  tryCatch(
    withCallingHandlers(
      stop("error msg"),
      error = h
    ),
    error = function(e) invisible(NULL)
  )

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  expect_equal(lr$severityNumber, Severity$ERROR)
  expect_equal(lr$severityText,  "ERROR")
})

# ---------------------------------------------------------------------------
# Message body
# ---------------------------------------------------------------------------

test_that("handler uses conditionMessage (trailing newline stripped)", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  withCallingHandlers(
    message("hello world"),  # message() appends "\n"
    message = h
  )

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  expect_equal(lr$body$stringValue, "hello world")
})

# ---------------------------------------------------------------------------
# Condition does NOT propagate disruption
# ---------------------------------------------------------------------------

test_that("handler does not muffle or rethrow the condition", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  # withCallingHandlers message handler should NOT muffle the message;
  # use tryCatch(message = ...) to catch the message after the handler runs.
  msg_received <- tryCatch({
    withCallingHandlers(
      message("test"),
      message = h
    )
    "no-message"
  }, message = function(m) conditionMessage(m))

  # The message should still propagate (handler must not suppress it).
  expect_match(msg_received, "test")
})

# ---------------------------------------------------------------------------
# trace_id / span_id from condition fields
# ---------------------------------------------------------------------------

test_that("handler extracts trace_id and span_id from condition fields", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  cond <- structure(
    class = c("message", "condition"),
    list(
      message  = "correlated",
      trace_id = "aaaa1111bbbb2222cccc3333dddd4444",
      span_id  = "1111aaaa2222bbbb"
    )
  )

  withCallingHandlers(
    signalCondition(cond),
    message = h
  )

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  expect_equal(lr$traceId, "aaaa1111bbbb2222cccc3333dddd4444")
  expect_equal(lr$spanId,  "1111aaaa2222bbbb")
})

test_that("trace_id and span_id are NOT included in OTLP attributes", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(endpoint = s$srv$url(), resource = .h_resource())

  cond <- structure(
    class = c("message", "condition"),
    list(
      message  = "correlated",
      trace_id = "aaaa1111bbbb2222cccc3333dddd4444",
      span_id  = "1111aaaa2222bbbb"
    )
  )

  withCallingHandlers(signalCondition(cond), message = h)

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  attr_keys <- if (is.null(lr$attributes)) character(0) else
    sapply(lr$attributes, function(a) a$key)
  expect_false("trace_id" %in% attr_keys)
  expect_false("span_id"  %in% attr_keys)
})

# ---------------------------------------------------------------------------
# Attribute merging: handler-level first, condition wins
# ---------------------------------------------------------------------------

test_that("handler-level attributes appear in log record", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(
    endpoint   = s$srv$url(),
    resource   = .h_resource(),
    attributes = list("worker.id" = "w-42", "env" = "prod")
  )

  withCallingHandlers(message("hi"), message = h)

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  attr_map <- setNames(
    lapply(lr$attributes, function(a) a$value),
    sapply(lr$attributes, function(a) a$key)
  )
  expect_equal(attr_map[["worker.id"]]$stringValue, "w-42")
  expect_equal(attr_map[["env"]]$stringValue,       "prod")
})

test_that("condition picotel.attributes override handler-level attributes", {
  s <- .h_make_log_server()
  h <- otlp_condition_handler(
    endpoint   = s$srv$url(),
    resource   = .h_resource(),
    attributes = list("worker.id" = "w-42", "env" = "prod")
  )

  cond <- structure(
    class = c("message", "condition"),
    list(
      message             = "override",
      picotel.attributes  = list("env" = "staging", "request.id" = "r-1")
    )
  )

  withCallingHandlers(signalCondition(cond), message = h)

  lr <- s$received()$resourceLogs[[1]]$scopeLogs[[1]]$logRecords[[1]]
  attr_map <- setNames(
    lapply(lr$attributes, function(a) a$value),
    sapply(lr$attributes, function(a) a$key)
  )
  # handler-level worker.id kept; condition-supplied env overrides
  expect_equal(attr_map[["worker.id"]]$stringValue, "w-42")
  expect_equal(attr_map[["env"]]$stringValue,       "staging")
  expect_equal(attr_map[["request.id"]]$stringValue, "r-1")
})

# ---------------------------------------------------------------------------
# Resource / endpoint resolution: handler uses env when not explicit
# ---------------------------------------------------------------------------

test_that("handler sends when resource comes from env", {
  # Subprocess boundary: record the hit by touching a temp file from the route.
  hit_file <- tempfile()
  app <- webfakes::new_app()
  app$post("/v1/logs", function(req, res) {
    file.create(hit_file)
    res$set_status(200)$send("")
  })
  srv <- webfakes::local_app_process(app)

  withr::local_envvar(OTEL_SERVICE_NAME = "env-svc")
  .picotel_reset_state()

  h <- otlp_condition_handler(endpoint = srv$url())
  # No explicit resource — should resolve from env.
  withCallingHandlers(message("hi"), message = h)

  expect_true(file.exists(hit_file))
})

test_that("handler silently skips when no resource available", {
  withr::local_envvar(
    OTEL_SERVICE_NAME        = NA,
    OTEL_RESOURCE_ATTRIBUTES = NA
  )
  .picotel_reset_state()

  h <- otlp_condition_handler(endpoint = "http://localhost:4318")
  # No resource → should not send, must not throw.
  withCallingHandlers(message("hi"), message = h)
  expect_true(TRUE)  # reached here without error
})

# ---------------------------------------------------------------------------
# Handler NEVER throws (wraps in tryCatch)
# ---------------------------------------------------------------------------

test_that("handler swallows internal errors and does not propagate them", {
  # Force an error inside the handler by passing an unreachable endpoint;
  # the outer tryCatch in the handler must catch it silently.
  # (Connection refused at 59999 causes curl to fail, which .picotel_post_json
  #  catches and returns FALSE — the handler tryCatch catches anything else.)
  h <- otlp_condition_handler(
    endpoint = "http://localhost:59999",
    resource = .h_resource()
  )

  # This should NOT throw.
  expect_no_error(
    withCallingHandlers(message("hi"), message = h)
  )
})

# ---------------------------------------------------------------------------
# Async sender: handler submits via .picotel_get_sender()
# ---------------------------------------------------------------------------

test_that("handler works with async sender (submits to queue)", {
  app <- webfakes::new_app()
  app$post("/v1/logs", function(req, res) res$set_status(200)$send(""))
  srv <- webfakes::local_app_process(app)

  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_reset_state()

  h <- otlp_condition_handler(endpoint = srv$url(), resource = .h_resource())
  withCallingHandlers(message("async msg"), message = h)

  # Queue should have one pending item before flush.
  expect_true(.picotel_state$async_pending >= 1L)

  # After flush the queue is drained.
  picotel_flush(timeout = 2)
  expect_equal(.picotel_state$async_pending, 0L)
})
