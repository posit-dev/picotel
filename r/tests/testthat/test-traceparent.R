# Tests for WP2 traceparent parsing:
#   .picotel_parse_traceparent()
#
# Ported from tests/test_traceparent.py (all test cases).
#
# Tests for TRACEPARENT sentinel interaction with Span/LogRecord constructors
# are owned by WP6 (those constructors are TODO stubs); they are noted below
# with skip reasons.

# Valid W3C traceparent used throughout tests
.picotel_valid_tp <- "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

# ---------------------------------------------------------------------------
# Basic parsing
# ---------------------------------------------------------------------------

test_that("valid TRACEPARENT is parsed correctly", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  result <- .picotel_parse_traceparent()
  expect_false(is.null(result))
  expect_equal(result$trace_id,  "0af7651916cd43dd8448eb211c80319c")
  expect_equal(result$parent_id, "b7ad6b7169203331")
  expect_equal(result$flags,     1L)
})

test_that("returns NULL when TRACEPARENT is not set", {
  withr::local_envvar(TRACEPARENT = NA, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  expect_null(.picotel_parse_traceparent())
})

# ---------------------------------------------------------------------------
# Invalid format cases (ported from test_parse_traceparent_invalid_format)
# ---------------------------------------------------------------------------

test_that("returns NULL for all invalid TRACEPARENT formats", {
  invalid_cases <- list(
    # Wrong number of parts
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
    "00-0af7651916cd43dd8448eb211c80319c",
    "invalid",
    # Wrong version (not "00")
    "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    "99-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    # Invalid trace_id (not 32 hex chars)
    "00-invalid-b7ad6b7169203331-01",
    "00-0af7651916cd43dd8448eb211c8031-b7ad6b7169203331-01",    # too short (30)
    "00-0af7651916cd43dd8448eb211c80319cX-b7ad6b7169203331-01", # too long (33)
    "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",  # non-hex char
    # Invalid parent_id (not 16 hex chars)
    "00-0af7651916cd43dd8448eb211c80319c-invalid-01",
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333-01",   # too short (15)
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b71692033311-01", # too long (17)
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333g-01",  # non-hex char
    # Invalid trace_flags (not 2 hex chars)
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1",   # too short (1)
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-001", # too long (3)
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0g",  # non-hex char
    # Empty string
    ""
  )

  for (val in invalid_cases) {
    withr::local_envvar(TRACEPARENT = val, PICOTEL_PREFIX = NA)
    .picotel_reset_state()
    expect_null(.picotel_parse_traceparent(),
                label = sprintf("should reject: %s", val))
  }
})

# ---------------------------------------------------------------------------
# Uppercase hex accepted, case preserved
# ---------------------------------------------------------------------------

test_that("uppercase hex is accepted and case is preserved", {
  # Python test: uppercase hex is accepted; returned strings keep original case.
  withr::local_envvar(
    TRACEPARENT = "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-FF",
    PICOTEL_PREFIX = NA
  )
  .picotel_reset_state()
  result <- .picotel_parse_traceparent()
  expect_false(is.null(result))
  expect_equal(result$trace_id,  "0AF7651916CD43DD8448EB211C80319C")
  expect_equal(result$parent_id, "B7AD6B7169203331")
  expect_equal(result$flags,     255L)
})

# ---------------------------------------------------------------------------
# Flags boundary values
# ---------------------------------------------------------------------------

test_that("flags = 0x00 parsed correctly", {
  withr::local_envvar(
    TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
    PICOTEL_PREFIX = NA
  )
  .picotel_reset_state()
  result <- .picotel_parse_traceparent()
  expect_false(is.null(result))
  expect_equal(result$flags, 0L)
})

test_that("flags = 0xff parsed correctly", {
  withr::local_envvar(
    TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-ff",
    PICOTEL_PREFIX = NA
  )
  .picotel_reset_state()
  result <- .picotel_parse_traceparent()
  expect_false(is.null(result))
  expect_equal(result$flags, 255L)
})

# ---------------------------------------------------------------------------
# Prefix remapping
# ---------------------------------------------------------------------------

test_that("reads PICOTEL_TRACEPARENT when prefix is PICOTEL", {
  # Python test_parse_traceparent: env is remapped via _prefixed().
  withr::local_envvar(
    PICOTEL_PREFIX      = "PICOTEL",
    PICOTEL_TRACEPARENT = .picotel_valid_tp,
    TRACEPARENT         = NA
  )
  .picotel_reset_state()
  result <- .picotel_parse_traceparent()
  expect_false(is.null(result))
  expect_equal(result$trace_id,  "0af7651916cd43dd8448eb211c80319c")
  expect_equal(result$parent_id, "b7ad6b7169203331")
  expect_equal(result$flags,     1L)
})

test_that("unprefixed TRACEPARENT is ignored when prefix is active", {
  # With PICOTEL_PREFIX=PICOTEL the standard TRACEPARENT is not consulted.
  withr::local_envvar(
    PICOTEL_PREFIX      = "PICOTEL",
    TRACEPARENT         = .picotel_valid_tp,
    PICOTEL_TRACEPARENT = NA
  )
  .picotel_reset_state()
  expect_null(.picotel_parse_traceparent())
})

# ---------------------------------------------------------------------------
# Caching behaviour (mirrors Go TestComputeTraceparentCached)
# ---------------------------------------------------------------------------

test_that("result is cached in .picotel_state", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  r1 <- .picotel_parse_traceparent()
  expect_false(is.null(r1))

  # Change env var without resetting — cached value must be returned.
  withr::local_envvar(
    TRACEPARENT = "00-aaaabbbbccccdddd1111222233334444-5555666677778888-02"
  )
  r2 <- .picotel_parse_traceparent()
  expect_equal(r1$trace_id, r2$trace_id,
               info = "traceparent should be cached after first call")

  # After reset the new value is picked up.
  .picotel_reset_state()
  r3 <- .picotel_parse_traceparent()
  expect_false(is.null(r3))
  expect_equal(r3$trace_id, "aaaabbbbccccdddd1111222233334444")
})

test_that("cache returns NULL after reset when var is unset", {
  withr::local_envvar(TRACEPARENT = .picotel_valid_tp, PICOTEL_PREFIX = NA)
  .picotel_reset_state()
  r1 <- .picotel_parse_traceparent()
  expect_false(is.null(r1))

  withr::local_envvar(TRACEPARENT = NA)
  .picotel_reset_state()
  expect_null(.picotel_parse_traceparent())
})

# ---------------------------------------------------------------------------
# Skipped Python cases (WP6-owned — Span/LogRecord constructors are stubs)
# ---------------------------------------------------------------------------
#
# The following tests from test_traceparent.py are NOT ported here because
# they test Span() and LogRecord() constructors that belong to WP6:
#
#   test_span_with_traceparent_sentinel         — Span(trace_id = TRACEPARENT)
#   test_span_with_traceparent_sentinel_no_env  — Span + error logging
#   test_span_with_explicit_trace_id            — Span(trace_id = "...")
#   test_span_with_traceparent_and_explicit_parent — Span + explicit parent_span_id
#   test_span_span_id_always_generated          — Span span_id generation
#   test_logrecord_with_traceparent_sentinel    — LogRecord(trace_id = TRACEPARENT)
#   test_logrecord_with_traceparent_sentinel_no_env
#   test_logrecord_without_traceparent_sentinel
#   test_logrecord_with_explicit_trace_id
#   test_logrecord_with_traceparent_and_explicit_span_id
#
# These will be covered in r/tests/testthat/test-lifecycle.R (WP6).
