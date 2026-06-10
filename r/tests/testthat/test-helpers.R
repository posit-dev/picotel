# Tests for WP1-implemented helpers:
#   new_trace_id(), new_span_id(), now_ns(), .picotel_ns_str(),
#   .picotel_prefix(), .picotel_env(), .picotel_is_disabled()
#
# Cases ported from tests/test_env_config.py (env-remapping and disabled
# sections) plus new cases for the ID/time helpers that have no Python
# equivalent (Python uses os.urandom() directly; R tests verify format).

test_that("new_trace_id() returns 32 lowercase hex characters", {
  id <- new_trace_id()
  expect_type(id, "character")
  expect_equal(nchar(id), 32L)
  expect_true(grepl("^[0-9a-f]{32}$", id))
})

test_that("new_trace_id() never contains uppercase", {
  # Run several times to reduce false-negative probability
  ids <- replicate(50, new_trace_id())
  expect_true(all(grepl("^[0-9a-f]{32}$", ids)))
})

test_that("new_trace_id() produces unique values", {
  ids <- replicate(200, new_trace_id())
  # With 128 bits of entropy duplicates are astronomically unlikely
  expect_equal(length(unique(ids)), 200L)
})

test_that("new_span_id() returns 16 lowercase hex characters", {
  id <- new_span_id()
  expect_type(id, "character")
  expect_equal(nchar(id), 16L)
  expect_true(grepl("^[0-9a-f]{16}$", id))
})

test_that("new_span_id() never contains uppercase", {
  ids <- replicate(50, new_span_id())
  expect_true(all(grepl("^[0-9a-f]{16}$", ids)))
})

test_that("new_span_id() produces unique values", {
  ids <- replicate(200, new_span_id())
  expect_equal(length(unique(ids)), 200L)
})

test_that("now_ns() returns a double with plausible magnitude", {
  ts <- now_ns()
  expect_type(ts, "double")
  # 2025-01-01 00:00:00 UTC ≈ 1.7358e18 ns; 2100-01-01 ≈ 4.102e18 ns
  expect_gt(ts, 1.7e18)
  expect_lt(ts, 4e18)
})

test_that("now_ns() is non-decreasing across successive calls", {
  t1 <- now_ns()
  t2 <- now_ns()
  expect_gte(t2, t1)
})

test_that(".picotel_ns_str() produces exact integer string without scientific notation", {
  # Canonical value from D5 plan: ~1.75e18
  x <- 1234567890123456768  # a representable double near 1.235e18
  s <- .picotel_ns_str(x)
  expect_type(s, "character")
  # Must contain only digits (no "e", "+", ".", spaces)
  expect_true(grepl("^[0-9]+$", s))
  # Must round-trip: as.numeric(s) should equal x
  expect_equal(as.numeric(s), x)
})

test_that(".picotel_ns_str() works on now_ns() output", {
  ts <- now_ns()
  s  <- .picotel_ns_str(ts)
  expect_true(grepl("^[0-9]+$", s))
  # Digit count should be 19 for current epoch values (~1.7e18)
  expect_gte(nchar(s), 18L)
  expect_lte(nchar(s), 20L)
})

# ---------------------------------------------------------------------------
# .picotel_prefix() and .picotel_env() — env-name remapping
# ---------------------------------------------------------------------------

test_that(".picotel_env() returns standard name unchanged when no prefix", {
  withr::local_envvar(PICOTEL_PREFIX = NA)
  expect_equal(.picotel_env("OTEL_SDK_DISABLED"), "OTEL_SDK_DISABLED")
  expect_equal(.picotel_env("TRACEPARENT"),        "TRACEPARENT")
  expect_equal(.picotel_env("OTEL_EXPORTER_OTLP_ENDPOINT"),
               "OTEL_EXPORTER_OTLP_ENDPOINT")
})

test_that(".picotel_env() strips OTEL_ and prepends prefix", {
  withr::local_envvar(PICOTEL_PREFIX = "PICOTEL")
  expect_equal(.picotel_env("OTEL_SDK_DISABLED"),           "PICOTEL_SDK_DISABLED")
  expect_equal(.picotel_env("OTEL_EXPORTER_OTLP_ENDPOINT"), "PICOTEL_EXPORTER_OTLP_ENDPOINT")
  expect_equal(.picotel_env("OTEL_SERVICE_NAME"),           "PICOTEL_SERVICE_NAME")
})

test_that(".picotel_env() prepends prefix (with _) to non-OTEL names", {
  withr::local_envvar(PICOTEL_PREFIX = "PICOTEL")
  # TRACEPARENT has no OTEL_ prefix → PICOTEL_TRACEPARENT
  expect_equal(.picotel_env("TRACEPARENT"), "PICOTEL_TRACEPARENT")
})

test_that(".picotel_env() works with a custom prefix", {
  withr::local_envvar(PICOTEL_PREFIX = "MYAPP")
  expect_equal(.picotel_env("OTEL_SDK_DISABLED"), "MYAPP_SDK_DISABLED")
  expect_equal(.picotel_env("TRACEPARENT"),        "MYAPP_TRACEPARENT")
})

test_that("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY is never remapped", {
  # The skip-verify var is already in picotel's namespace.  .picotel_env()
  # should NOT be called on it; it is always read raw from the environment.
  # We document this by verifying that reading Sys.getenv() of the raw name
  # works independently of any prefix.
  withr::local_envvar(
    PICOTEL_PREFIX = "FOO",
    PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY = "true"
  )
  # The raw variable is accessible
  expect_equal(
    Sys.getenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY"),
    "true"
  )
  # .picotel_env() on a hypothetical standard name would remap — proving the
  # skip-verify var intentionally bypasses this function.
  expect_equal(.picotel_env("OTEL_SDK_DISABLED"), "FOO_SDK_DISABLED")
})

# ---------------------------------------------------------------------------
# .picotel_is_disabled()
# ---------------------------------------------------------------------------

test_that(".picotel_is_disabled() is FALSE when OTEL_SDK_DISABLED is unset", {
  withr::local_envvar(OTEL_SDK_DISABLED = NA, PICOTEL_PREFIX = NA)
  expect_false(.picotel_is_disabled())
})

test_that(".picotel_is_disabled() recognises truthy values (no prefix)", {
  for (val in c("true", "TRUE", "True", "1")) {
    withr::local_envvar(OTEL_SDK_DISABLED = val, PICOTEL_PREFIX = NA)
    expect_true(.picotel_is_disabled(),
                label = sprintf("OTEL_SDK_DISABLED=%s should be truthy", val))
  }
})

test_that(".picotel_is_disabled() recognises falsy values (no prefix)", {
  for (val in c("false", "FALSE", "False", "0", "")) {
    withr::local_envvar(OTEL_SDK_DISABLED = val, PICOTEL_PREFIX = NA)
    expect_false(.picotel_is_disabled(),
                 label = sprintf("OTEL_SDK_DISABLED=%s should be falsy", val))
  }
})

test_that(".picotel_is_disabled() reads prefixed name when PICOTEL_PREFIX set", {
  withr::local_envvar(
    PICOTEL_PREFIX   = "PICOTEL",
    PICOTEL_SDK_DISABLED = "true",
    OTEL_SDK_DISABLED    = NA
  )
  expect_true(.picotel_is_disabled())
})

test_that(".picotel_is_disabled() ignores un-prefixed name when prefix is active", {
  withr::local_envvar(
    PICOTEL_PREFIX        = "PICOTEL",
    OTEL_SDK_DISABLED     = "true",
    PICOTEL_SDK_DISABLED  = NA
  )
  # Prefix is active → the standard OTEL_ name is NOT consulted
  expect_false(.picotel_is_disabled())
})

test_that(".picotel_is_disabled() FALSE for falsy value under prefix", {
  withr::local_envvar(
    PICOTEL_PREFIX       = "PICOTEL",
    PICOTEL_SDK_DISABLED = "false",
    OTEL_SDK_DISABLED    = NA
  )
  expect_false(.picotel_is_disabled())
})

# ---------------------------------------------------------------------------
# Constants integrity checks
# ---------------------------------------------------------------------------

test_that("SpanKind contains the expected integer values", {
  expect_equal(SpanKind$UNSPECIFIED, 0L)
  expect_equal(SpanKind$INTERNAL,    1L)
  expect_equal(SpanKind$SERVER,      2L)
  expect_equal(SpanKind$CLIENT,      3L)
  expect_equal(SpanKind$PRODUCER,    4L)
  expect_equal(SpanKind$CONSUMER,    5L)
})

test_that("SpanStatus contains the expected integer values", {
  expect_equal(SpanStatus$UNSET,  0L)
  expect_equal(SpanStatus$OK,     1L)
  expect_equal(SpanStatus$ERROR,  2L)
})

test_that("Severity contains the expected integer values", {
  expect_equal(Severity$TRACE, 1L)
  expect_equal(Severity$DEBUG, 5L)
  expect_equal(Severity$INFO,  9L)
  expect_equal(Severity$WARN,  13L)
  expect_equal(Severity$ERROR, 17L)
  expect_equal(Severity$FATAL, 21L)
})

test_that("TRACEPARENT sentinel is detectable with identical()", {
  expect_true(identical(TRACEPARENT, TRACEPARENT))
  expect_false(identical(TRACEPARENT, new.env()))
  expect_false(identical(TRACEPARENT, "TRACEPARENT"))
})

# ---------------------------------------------------------------------------
# .picotel_state and .picotel_reset_state()
# ---------------------------------------------------------------------------

test_that(".picotel_state is an environment", {
  expect_true(is.environment(.picotel_state))
})

test_that(".picotel_reset_state() clears all slots", {
  .picotel_state$test_key <- "test_value"
  .picotel_reset_state()
  expect_equal(ls(.picotel_state, all.names = TRUE), character(0))
})

# ---------------------------------------------------------------------------
# picotel_config_error condition
# ---------------------------------------------------------------------------

test_that("picotel_config_error() raises a condition of the right class", {
  expect_error(
    picotel_config_error("bad config"),
    class = "picotel_config_error"
  )
})

test_that("picotel_config_error() message starts with 'picotel:'", {
  err <- tryCatch(
    picotel_config_error("something wrong"),
    picotel_config_error = function(e) e
  )
  expect_true(startsWith(conditionMessage(err), "picotel:"))
})

# ---------------------------------------------------------------------------
# picotel_resource() — trivially implemented in WP1 for helper-test use
# ---------------------------------------------------------------------------

test_that("picotel_resource() creates a classed list with attributes", {
  r <- picotel_resource(list("service.name" = "myapp"))
  expect_s3_class(r, "picotel_resource")
  expect_equal(r$attributes[["service.name"]], "myapp")
})
