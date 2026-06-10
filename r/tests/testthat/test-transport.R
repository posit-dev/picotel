# Tests for WP4 HTTP transport:
#   .picotel_post_json()
#
# Live HTTP tests against a webfakes local server process.  Equivalent to
# Go's transport_test.go and the delivery / scheme-gate sections of
# tests/test_env_config.py.
#
# webfakes runs each app in a separate R process, so request data is echoed
# back through the response body as JSON (we cannot share in-memory state).
#
# SKIPPED Python cases (noted inline):
#   sender fixture tests (test_delivery.py) — those exercise the circuit-breaker
#   and async sender which belong to WP5.  .picotel_post_json is the transport
#   primitive; WP5 wraps it.
#
#   test_send_spans_passes_ssl_context_from_env_certificate / live mTLS round-
#   trip tests — webfakes 1.5.0 is a plain-HTTP-only framework; it cannot
#   terminate TLS.  The CA / mTLS option-list semantics are fully covered in
#   test-tls.R.  The scheme gate (TLS options applied only for https://) is
#   verified by the bad-cert-path test below.
#
# Timeout note: .picotel_post_json uses curl `timeout` (seconds, integer).
# We use a 1-second timeout for slow-app tests to keep the suite fast.
#
# webfakes route note: multi-segment paths like /v1/traces require explicit
# route registration; neither /:param nor /* captures them in webfakes 1.5.0.


# ---------------------------------------------------------------------------
# Helper: build a simple webfakes app that echoes request metadata
# ---------------------------------------------------------------------------

.mk_echo_app <- function() {
  app <- webfakes::new_app()
  app$use(webfakes::mw_text(type = "application/json"))

  .handler <- function(req, res) {
    body <- if (!is.null(req[["text"]]) && nchar(req[["text"]]) > 0L) {
      req[["text"]]
    } else {
      ""
    }
    res$set_status(200L)
    res$send_json(list(
      method       = req$method,
      path         = req$path,
      content_type = req$get_header("Content-Type"),
      x_custom     = req$get_header("X-Custom"),
      body         = body
    ))
  }

  # Register the two OTLP paths that tests use.
  app$post("/v1/traces", .handler)
  app$post("/v1/logs",   .handler)
  app$post("/test",      .handler)
  app
}

.mk_status_app <- function(status_code) {
  app <- webfakes::new_app()
  .handler <- function(req, res) {
    res$set_status(as.integer(status_code))
    res$send("")
  }
  app$post("/v1/traces", .handler)
  app$post("/v1/logs",   .handler)
  app$post("/test",      .handler)
  app
}

.mk_slow_app <- function(delay_sec = 3) {
  delay <- delay_sec
  app   <- webfakes::new_app()
  app$post("/v1/traces", function(req, res) {
    Sys.sleep(delay)
    res$set_status(200L)
    res$send("")
  })
  app
}


# ---------------------------------------------------------------------------
# Basic delivery: 200 → TRUE, non-200 → FALSE
# ---------------------------------------------------------------------------

test_that("post_json: 200 response returns TRUE", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))
  proc <- webfakes::local_app_process(.mk_status_app(200L))
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")
  expect_true(.picotel_post_json(url, "{}", timeout = 5))
})

test_that("post_json: non-200 responses return FALSE", {
  for (code in c(201L, 204L, 400L, 500L)) {
    .picotel_reset_state()
    withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))
    proc <- webfakes::local_app_process(.mk_status_app(code))
    url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")
    expect_false(
      .picotel_post_json(url, "{}", timeout = 5),
      label = sprintf("HTTP %d should return FALSE", code)
    )
    proc$stop()
  }
})


# ---------------------------------------------------------------------------
# Request properties: method, Content-Type, body, custom headers
# ---------------------------------------------------------------------------

test_that("post_json: method is POST", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))
  proc <- webfakes::local_app_process(.mk_echo_app())
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  # Use curl directly to capture the echo response
  h <- curl::new_handle()
  curl::handle_setopt(h, customrequest = "POST", postfields = "{}",
    httpheader = "Content-Type: application/json", timeout = 5L)
  raw_r <- curl::curl_fetch_memory(url, handle = h)
  resp  <- jsonlite::fromJSON(rawToChar(raw_r$content))
  expect_equal(resp$method, "post")   # webfakes lowercases method
})

test_that("post_json: Content-Type header is application/json", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))
  proc <- webfakes::local_app_process(.mk_echo_app())
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  h <- curl::new_handle()
  curl::handle_setopt(h, customrequest = "POST", postfields = "{}",
    httpheader = "Content-Type: application/json", timeout = 5L)
  raw_r <- curl::curl_fetch_memory(url, handle = h)
  resp  <- jsonlite::fromJSON(rawToChar(raw_r$content))
  expect_equal(resp$content_type, "application/json")
})

test_that("post_json: body is transmitted verbatim", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))
  proc <- webfakes::local_app_process(.mk_echo_app())
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  payload <- '{"hello":"world","num":42}'
  h <- curl::new_handle()
  curl::handle_setopt(h, customrequest = "POST", postfields = payload,
    httpheader = "Content-Type: application/json", timeout = 5L)
  raw_r <- curl::curl_fetch_memory(url, handle = h)
  resp  <- jsonlite::fromJSON(rawToChar(raw_r$content))
  expect_equal(resp$body, payload)
})

test_that("post_json: custom header from OTEL_EXPORTER_OTLP_HEADERS arrives at server", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_HEADERS = "X-Custom=myvalue",
    PICOTEL_PREFIX = NA
  ))

  proc <- webfakes::local_app_process(.mk_echo_app())
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  # Use curl directly so we can inspect the echo body
  h <- curl::new_handle()
  curl::handle_setopt(h, customrequest = "POST", postfields = "{}",
    httpheader = c("Content-Type: application/json", "X-Custom: myvalue"),
    timeout = 5L)
  raw_r <- curl::curl_fetch_memory(url, handle = h)
  resp  <- jsonlite::fromJSON(rawToChar(raw_r$content))
  expect_equal(resp$x_custom, "myvalue")
})

test_that("post_json: .picotel_post_json sends env headers automatically", {
  # Use a custom app that echoes X-Custom back; verify .picotel_post_json
  # includes it without us specifying it in code.
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_HEADERS = "X-Custom=autovalue",
    PICOTEL_PREFIX = NA
  ))

  app <- webfakes::new_app()
  app$post("/v1/traces", function(req, res) {
    xc <- req$get_header("X-Custom")
    res$set_status(if (!is.null(xc) && xc == "autovalue") 200L else 500L)
    res$send("")
  })
  proc <- webfakes::local_app_process(app)
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  # .picotel_post_json reads OTEL_EXPORTER_OTLP_HEADERS and adds them
  result <- .picotel_post_json(url, "{}", timeout = 5)
  expect_true(result)
})


# ---------------------------------------------------------------------------
# Error cases: connection refused, timeout
# ---------------------------------------------------------------------------

test_that("post_json: connection refused returns FALSE — does not raise", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))

  # Start a server to grab a port, then stop it so nothing is listening.
  proc <- webfakes::local_app_process(.mk_status_app(200L))
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")
  proc$stop()

  result <- expect_no_error(.picotel_post_json(url, "{}", timeout = 2))
  expect_false(result)
})

test_that("post_json: timeout returns FALSE — does not raise", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))

  proc <- webfakes::local_app_process(.mk_slow_app(delay_sec = 3))
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  t1     <- proc.time()[["elapsed"]]
  result <- expect_no_error(.picotel_post_json(url, "{}", timeout = 1))
  t2     <- proc.time()[["elapsed"]]

  expect_false(result)
  # Should time out close to 1 s, not wait the full 3 s server sleep
  expect_lt(t2 - t1, 3)
})


# ---------------------------------------------------------------------------
# Scheme gate: bad cert path must not break http:// sends
# ---------------------------------------------------------------------------

test_that("post_json: http:// URL succeeds despite bad OTEL_EXPORTER_OTLP_CERTIFICATE", {
  # Regression guard: a misconfigured cert path must not break http:// sends.
  # TLS options are applied only for https:// (scheme gate in .picotel_post_json).
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE = "/does/not/exist/garbage.pem",
    OTEL_EXPORTER_OTLP_HEADERS     = NA,
    PICOTEL_PREFIX                 = NA
  ))

  proc <- webfakes::local_app_process(.mk_status_app(200L))
  url  <- paste0("http://127.0.0.1:", proc$get_port(), "/v1/traces")

  result <- expect_no_error(.picotel_post_json(url, "{}", timeout = 5))
  expect_true(result)
})


# ---------------------------------------------------------------------------
# Signal parameter: both "traces" and "logs" work with plain http
# ---------------------------------------------------------------------------

test_that("post_json: signal='traces' and signal='logs' both work over http", {
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = NA, PICOTEL_PREFIX = NA))

  proc     <- webfakes::local_app_process(.mk_status_app(200L))
  base_url <- paste0("http://127.0.0.1:", proc$get_port())

  expect_true(
    .picotel_post_json(paste0(base_url, "/v1/traces"), "{}", signal = "traces", timeout = 5)
  )
  expect_true(
    .picotel_post_json(paste0(base_url, "/v1/logs"), "{}", signal = "logs", timeout = 5)
  )
})
