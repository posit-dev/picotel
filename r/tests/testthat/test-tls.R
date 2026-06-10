# Tests for WP4 TLS / header helpers:
#   .picotel_parse_headers(), .picotel_tls_options()
#
# All assertions are purely on returned values (option lists / named vectors) —
# no live TLS server is required.  Live HTTP tests are in test-transport.R.
#
# Ported from:
#   tests/test_env_config.py — test_parse_headers, TLS sections (lines 144–1286)
#   go/tls_test.go           — computeHeaders and computeTLSConfig unit tests
#
# SKIPPED Python cases (noted inline):
#   test_ssl_context_keeps_client_cert / test_send_spans_passes_* /
#   test_send_logs_* — integration tests that mock urllib.request.urlopen and
#   verify it receives a specific SSLContext object.  In R, the send path is
#   tested live in test-transport.R; the option-list contract is tested here.
#
#   test_ssl_context_skip_verify_keeps_client_cert uses a real openssl-generated
#   PEM to call the actual ssl.load_cert_chain.  In R, .picotel_tls_options()
#   returns option paths without loading certs (curl loads at connect time), so
#   a real PEM is not needed to test the option-list semantics.  Live mTLS is
#   covered by test-transport.R (skipped; see note there).
#
# Cert path fixtures: for pure option-list tests the file need not exist — we
# only check that the returned list contains the expected path string.  Tests
# that do need a readable file (skip-verify + real client cert load) are either
# moved to transport or skipped as noted.


# ---------------------------------------------------------------------------
# Helper: mirrors Python's _prefixed() in test_env_config.py
# ---------------------------------------------------------------------------

.tls_prefixed <- function(env, prefix) {
  if (nchar(prefix) == 0L) return(env)
  result <- c(PICOTEL_PREFIX = prefix)
  for (nm in names(env)) {
    if (startsWith(nm, "OTEL_")) {
      result[[paste0(prefix, "_", substring(nm, 6L))]] <- env[[nm]]
    } else {
      result[[paste0(prefix, "_", nm)]] <- env[[nm]]
    }
  }
  result
}

.tls_prefixes <- c("", "PICOTEL")


# ---------------------------------------------------------------------------
# .picotel_parse_headers() — option-list assertions
# ---------------------------------------------------------------------------

test_that("parse_headers: basic key=value pairs", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(
      c(OTEL_EXPORTER_OTLP_HEADERS = "key1=value1,key2=value2,key3=value with spaces"),
      prefix
    )
    withr::local_envvar(as.list(env))
    h <- .picotel_parse_headers()
    expect_equal(h[["key1"]], "value1",         label = sprintf("prefix='%s' key1", prefix))
    expect_equal(h[["key2"]], "value2",         label = sprintf("prefix='%s' key2", prefix))
    expect_equal(h[["key3"]], "value with spaces", label = sprintf("prefix='%s' key3", prefix))
  }
})

test_that("parse_headers: empty string returns empty vector", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(c(OTEL_EXPORTER_OTLP_HEADERS = ""), prefix)
    withr::local_envvar(as.list(env))
    expect_length(.picotel_parse_headers(), 0L)
  }
})

test_that("parse_headers: unset variable returns empty vector", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(list(), prefix)
    env[["OTEL_EXPORTER_OTLP_HEADERS"]]         <- NA
    env[["PICOTEL_EXPORTER_OTLP_HEADERS"]]      <- NA
    withr::local_envvar(as.list(env))
    expect_length(.picotel_parse_headers(), 0L)
  }
})

test_that("parse_headers: whitespace trimming around key and value", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(
      c(OTEL_EXPORTER_OTLP_HEADERS = " key1 = value1 , key2=value2 "),
      prefix
    )
    withr::local_envvar(as.list(env))
    h <- .picotel_parse_headers()
    expect_equal(h[["key1"]], "value1", label = sprintf("prefix='%s' key1", prefix))
    expect_equal(h[["key2"]], "value2", label = sprintf("prefix='%s' key2", prefix))
  }
})

test_that("parse_headers: pair without '=' is skipped; valid pair still parsed", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_HEADERS = "noequalssign,key=value",
    PICOTEL_PREFIX = NA
  ))
  h <- .picotel_parse_headers()
  expect_false("noequalssign" %in% names(h))
  expect_equal(h[["key"]], "value")
})

test_that("parse_headers: value that contains '=' splits only on first '='", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_HEADERS = "Authorization=Bearer tok==en",
    PICOTEL_PREFIX = NA
  ))
  h <- .picotel_parse_headers()
  expect_equal(h[["Authorization"]], "Bearer tok==en")
})

test_that("parse_headers: prefix remap — PICOTEL_PREFIX replaces OTEL_ portion", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_PREFIX = "PICOTEL",
    PICOTEL_EXPORTER_OTLP_HEADERS = "X-Custom=myval"
  ))
  h <- .picotel_parse_headers()
  expect_equal(h[["X-Custom"]], "myval")
})

test_that("parse_headers: standard name ignored when PICOTEL_PREFIX is set", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_PREFIX = "PICOTEL",
    OTEL_EXPORTER_OTLP_HEADERS = "key=val"  # standard name — ignored
  ))
  expect_length(.picotel_parse_headers(), 0L)
})

test_that("parse_headers: result is cached in .picotel_state", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_HEADERS = "x=y",
    PICOTEL_PREFIX = NA
  ))
  h1 <- .picotel_parse_headers()
  # Change env var — cached result should still be returned
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = "a=b"))
  h2 <- .picotel_parse_headers()
  expect_identical(h1, h2)
})

test_that("parse_headers: .picotel_reset_state() clears the cache", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_HEADERS = "x=y",
    PICOTEL_PREFIX = NA
  ))
  .picotel_parse_headers()  # populate cache
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_HEADERS = "a=b"))
  h <- .picotel_parse_headers()
  expect_equal(h[["a"]], "b")
})


# ---------------------------------------------------------------------------
# .picotel_tls_options() — empty / skip-verify
# ---------------------------------------------------------------------------

test_that("tls_options: empty list when no env vars set", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE             = NA,
    OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE      = NA,
    OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE        = NA,
    OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE      = NA,
    OTEL_EXPORTER_OTLP_CLIENT_KEY              = NA,
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = NA,
    PICOTEL_PREFIX                             = NA
  ))
  expect_length(.picotel_tls_options("traces"), 0L)
})

test_that("tls_options: skip-verify 'true' disables peer and host checks", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "true",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$ssl_verifypeer, 0L)
  expect_equal(opts$ssl_verifyhost, 0L)
})

test_that("tls_options: skip-verify 'TRUE' is truthy", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "TRUE",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$ssl_verifypeer, 0L)
})

test_that("tls_options: skip-verify 'True' is truthy", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "True",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$ssl_verifypeer, 0L)
})

test_that("tls_options: skip-verify '1' is truthy", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "1",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$ssl_verifypeer, 0L)
})

test_that("tls_options: skip-verify falsy values — 'false', '0', 'yes'", {
  for (val in c("false", "0", "yes")) {
    .picotel_reset_state()
    withr::local_envvar(list(
      PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = val,
      PICOTEL_PREFIX = NA
    ))
    opts <- .picotel_tls_options("traces")
    # No skip-verify options; list may be empty (no CA set)
    expect_false(
      isTRUE(opts$ssl_verifypeer == 0L),
      label = sprintf("skip-verify must not trigger for '%s'", val)
    )
  }
})

test_that("tls_options: skip-verify not remapped by PICOTEL_PREFIX (raw PICOTEL_ name)", {
  # PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY is read raw — not remapped.
  # Setting PICOTEL_PREFIX=FOO must still honour the raw PICOTEL_ name.
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_PREFIX = "FOO",
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "true"
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$ssl_verifypeer, 0L)
})

test_that("tls_options: remapped FOO_ name does NOT trigger skip-verify", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_PREFIX = "FOO",
    FOO_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "true"
  ))
  opts <- .picotel_tls_options("traces")
  expect_false(isTRUE(opts$ssl_verifypeer == 0L))
})


# ---------------------------------------------------------------------------
# .picotel_tls_options() — CA certificate precedence
# ---------------------------------------------------------------------------

test_that("tls_options: general CA path appears in cainfo", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE = "/path/to/ca.pem",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$cainfo, "/path/to/ca.pem")
  # ssl_verifypeer must NOT be 0L (we're using CA verification)
  expect_false(isTRUE(opts$ssl_verifypeer == 0L))
})

test_that("tls_options: signal-specific traces cert wins over general", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE        = "/path/general.pem",
    OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE = "/path/signal.pem",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$cainfo, "/path/signal.pem")
})

test_that("tls_options: signal-specific logs cert wins over general", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE      = "/path/general.pem",
    OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE = "/path/logs-signal.pem",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("logs")
  expect_equal(opts$cainfo, "/path/logs-signal.pem")
})

test_that("tls_options: traces-specific CA does not apply to logs signal", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE = "/path/traces.pem",
    OTEL_EXPORTER_OTLP_CERTIFICATE        = NA,
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("logs")
  expect_null(opts$cainfo)
  expect_length(opts, 0L)
})

test_that("tls_options: logs-specific CA does not apply to traces signal", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE = "/path/logs.pem",
    OTEL_EXPORTER_OTLP_CERTIFICATE      = NA,
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_null(opts$cainfo)
  expect_length(opts, 0L)
})

test_that("tls_options: prefix remap applies to CA cert var", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(
      c(OTEL_EXPORTER_OTLP_CERTIFICATE = "/path/ca.pem"), prefix
    )
    withr::local_envvar(as.list(env))
    opts <- .picotel_tls_options("traces")
    expect_equal(opts$cainfo, "/path/ca.pem",
                 label = sprintf("prefix='%s' cainfo", prefix))
  }
})

test_that("tls_options: standard CA name ignored when PICOTEL_PREFIX is set", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_PREFIX = "PICOTEL",
    # Standard name — must be IGNORED under PICOTEL_PREFIX
    OTEL_EXPORTER_OTLP_CERTIFICATE = "/path/should-be-ignored.pem"
  ))
  opts <- .picotel_tls_options("traces")
  expect_null(opts$cainfo)
})

test_that("tls_options: skip-verify short-circuits CA — cainfo not set", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(
      c(OTEL_EXPORTER_OTLP_CERTIFICATE = "/path/to/ca.pem"), prefix
    )
    env[["PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY"]] <- "true"
    withr::local_envvar(as.list(env))
    opts <- .picotel_tls_options("traces")
    # Must have skip-verify flags; cainfo must NOT be present
    expect_equal(opts$ssl_verifypeer, 0L,
                 label = sprintf("prefix='%s' ssl_verifypeer", prefix))
    expect_equal(opts$ssl_verifyhost, 0L,
                 label = sprintf("prefix='%s' ssl_verifyhost", prefix))
    expect_null(opts$cainfo,
                label = sprintf("prefix='%s' cainfo must be absent", prefix))
  }
})


# ---------------------------------------------------------------------------
# .picotel_tls_options() — mTLS client certificate
# ---------------------------------------------------------------------------

test_that("tls_options: client cert only — sslcert set, sslkey absent", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(
      c(OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE = "/path/to/client.pem"), prefix
    )
    withr::local_envvar(as.list(env))
    opts <- .picotel_tls_options("traces")
    expect_equal(opts$sslcert, "/path/to/client.pem",
                 label = sprintf("prefix='%s' sslcert", prefix))
    # keyfile is NULL/absent when CLIENT_KEY is not set
    expect_null(opts$sslkey,
                label = sprintf("prefix='%s' sslkey must be absent", prefix))
    # System trust store — no cainfo
    expect_null(opts$cainfo,
                label = sprintf("prefix='%s' cainfo must be absent", prefix))
  }
})

test_that("tls_options: client cert AND key — both sslcert and sslkey set", {
  for (prefix in .tls_prefixes) {
    .picotel_reset_state()
    env <- .tls_prefixed(c(
      OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE = "/path/to/client.pem",
      OTEL_EXPORTER_OTLP_CLIENT_KEY         = "/path/to/client.key"
    ), prefix)
    withr::local_envvar(as.list(env))
    opts <- .picotel_tls_options("traces")
    expect_equal(opts$sslcert, "/path/to/client.pem",
                 label = sprintf("prefix='%s' sslcert", prefix))
    expect_equal(opts$sslkey,  "/path/to/client.key",
                 label = sprintf("prefix='%s' sslkey", prefix))
  }
})

test_that("tls_options: client key without cert is ignored — empty list", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CLIENT_KEY = "/path/to/client.key",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_length(opts, 0L)
})

test_that("tls_options: client cert applies to both signals (signal-agnostic)", {
  for (signal in c("traces", "logs")) {
    .picotel_reset_state()
    withr::local_envvar(list(
      OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE = "/path/to/client.pem",
      OTEL_EXPORTER_OTLP_CLIENT_KEY         = "/path/to/client.key",
      PICOTEL_PREFIX = NA
    ))
    opts <- .picotel_tls_options(signal)
    expect_equal(opts$sslcert, "/path/to/client.pem",
                 label = sprintf("signal='%s' sslcert", signal))
    expect_equal(opts$sslkey,  "/path/to/client.key",
                 label = sprintf("signal='%s' sslkey", signal))
  }
})

test_that("tls_options: skip-verify still loads client cert (mTLS + self-signed)", {
  .picotel_reset_state()
  withr::local_envvar(list(
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "true",
    OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE      = "/path/to/client.pem",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$ssl_verifypeer, 0L)
  expect_equal(opts$ssl_verifyhost, 0L)
  expect_equal(opts$sslcert, "/path/to/client.pem")
})

test_that("tls_options: CA + client cert — cainfo and sslcert both present", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE        = "/path/to/ca.pem",
    OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE = "/path/to/client.pem",
    OTEL_EXPORTER_OTLP_CLIENT_KEY         = "/path/to/client.key",
    PICOTEL_PREFIX = NA
  ))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$cainfo,   "/path/to/ca.pem")
  expect_equal(opts$sslcert,  "/path/to/client.pem")
  expect_equal(opts$sslkey,   "/path/to/client.key")
  # No skip-verify
  expect_false(isTRUE(opts$ssl_verifypeer == 0L))
})


# ---------------------------------------------------------------------------
# .picotel_tls_options() — caching
# ---------------------------------------------------------------------------

test_that("tls_options: result is cached in .picotel_state$tls_<signal>", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE = "/path/ca.pem",
    PICOTEL_PREFIX = NA
  ))
  opts1 <- .picotel_tls_options("traces")
  # Change env — cached result returned
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_CERTIFICATE = "/other/ca.pem"))
  opts2 <- .picotel_tls_options("traces")
  expect_identical(opts1, opts2)
})

test_that("tls_options: reset_state clears the cache", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_CERTIFICATE = "/path/ca.pem",
    PICOTEL_PREFIX = NA
  ))
  .picotel_tls_options("traces")  # populate cache
  .picotel_reset_state()
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_CERTIFICATE = "/other/ca.pem"))
  opts <- .picotel_tls_options("traces")
  expect_equal(opts$cainfo, "/other/ca.pem")
})

test_that("tls_options: traces and logs cached independently", {
  .picotel_reset_state()
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE = "/path/traces.pem",
    OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE   = "/path/logs.pem",
    PICOTEL_PREFIX = NA
  ))
  traces_opts <- .picotel_tls_options("traces")
  logs_opts   <- .picotel_tls_options("logs")
  expect_equal(traces_opts$cainfo, "/path/traces.pem")
  expect_equal(logs_opts$cainfo,   "/path/logs.pem")
})
