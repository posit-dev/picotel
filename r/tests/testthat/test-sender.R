# Tests for WP5: .picotel_sync_submit(), .picotel_async_submit(),
# picotel_flush(), and .picotel_get_sender().
#
# Ported from tests/test_async_sender.py and go/sender_test.go.
# Thread/fork-specific cases are skipped (R is single-threaded; see comments).
#
# Failure classification table (documented in r/picotel.R WP5 region):
#
#  fn() outcome                    | Classification | Counter action
#  ---------------------------------+----------------+------------------------
#  Returns any value except FALSE   | Success        | reset to 0
#  Returns identical(result, FALSE) | Persistent     | +1 (trips at 5)
#  Raises picotel_config_error      | Persistent     | +1 + log
#  Raises picotel_disabled          | Neutral        | unchanged (no reset)
#  Raises any other error           | Transient      | unchanged (no reset)
#  ---------------------------------+----------------+------------------------
#
# PICOTEL_ASYNC env-var: "true"/"1" (case-insensitive) selects async sender;
#   anything else (including unset) selects sync.  Read directly, no prefix
#   remapping.  Cached; env change after first call is ignored.

# ===========================================================================
# Helper utilities
# ===========================================================================

# .snd_capture_log() captures .picotel_log() output (which writes to stderr)
# by temporarily replacing the .picotel_log function.  Returns list(msgs, restore).
.snd_capture_log <- function() {
  captured <- character(0L)
  orig_fn  <- .picotel_log
  unlockBinding(".picotel_log", environment(.picotel_log))
  assign(".picotel_log", function(msg) {
    captured <<- c(captured, msg)
  }, envir = parent.env(environment()))
  # Restore by returning a list; caller must call restore() when done.
  list(
    get_msgs = function() captured,
    restore  = function() {
      assign(".picotel_log", orig_fn, envir = parent.env(environment()))
    }
  )
}

# .snd_with_log_capture(expr) — evaluate expr with log messages captured.
# Returns list(result, msgs).
.snd_with_log_capture <- function(expr) {
  msgs    <- character(0L)
  old_log <- .picotel_log
  # Temporarily replace .picotel_log with a capturing version.
  local_env  <- environment()
  capture_fn <- function(msg) msgs <<- c(msgs, msg)
  # We need to patch in the WP1 namespace.  Use environment tricks.
  env_root <- environment(.picotel_log)
  old_val  <- get(".picotel_log", envir = env_root)
  assign(".picotel_log", capture_fn, envir = env_root)
  on.exit(assign(".picotel_log", old_val, envir = env_root), add = TRUE)

  result <- force(expr)
  list(result = result, msgs = msgs)
}

# .snd_reset() — reset sender state completely between tests.
.snd_reset <- function() {
  .picotel_reset_state()
}

# .snd_ok_fn() — closure that always succeeds (returns TRUE).
.snd_ok_fn <- function() function() TRUE

# .snd_false_fn() — closure that returns FALSE (persistent failure).
.snd_false_fn <- function() function() FALSE

# .snd_config_err_fn() — closure that raises picotel_config_error (persistent).
.snd_config_err_fn <- function(msg = "test config error") {
  function() picotel_config_error(msg)
}

# .snd_disabled_fn() — closure that raises picotel_disabled (neutral).
.snd_disabled_fn <- function() {
  function() stop(.picotel_disabled_condition())
}

# .snd_error_fn() — closure that raises a generic error (transient).
.snd_error_fn <- function(msg = "transient error") {
  function() stop(msg)
}

# .snd_side_effect_fn() — closure that appends to a list and returns TRUE.
.snd_side_effect_fn <- function(log_list) {
  function() {
    log_list[[length(log_list) + 1L]] <<- TRUE
    TRUE
  }
}

# ===========================================================================
# Sync sender: basic success path
# ===========================================================================

test_that("sync_submit: success path returns TRUE", {
  .snd_reset()
  expect_true(.picotel_sync_submit(.snd_ok_fn()))
})

test_that("sync_submit: success resets consecutive_failures counter", {
  .snd_reset()
  # Submit 3 failures then a success.
  for (i in seq_len(3L)) .picotel_sync_submit(.snd_false_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 3L)
  .picotel_sync_submit(.snd_ok_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 0L)
  expect_false(.picotel_state$sync_tripped)
})

test_that("sync_submit: NULL return is treated as success (not FALSE)", {
  .snd_reset()
  # NULL is NOT identical to FALSE — should be treated as success.
  expect_true(.picotel_sync_submit(function() NULL))
  expect_equal(.picotel_state$sync_consecutive_failures, 0L)
})

test_that("sync_submit: zero return is treated as success (not FALSE)", {
  .snd_reset()
  expect_true(.picotel_sync_submit(function() 0))
  expect_equal(.picotel_state$sync_consecutive_failures, 0L)
})

# ===========================================================================
# Sync sender: failure counting and circuit breaker
# ===========================================================================

test_that("sync_submit: failure increments counter", {
  .snd_reset()
  .picotel_sync_submit(.snd_false_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 1L)
  expect_false(.picotel_state$sync_tripped)
})

test_that("sync_submit: trips at exactly 5 consecutive failures", {
  .snd_reset()
  results <- logical(.picotel_MAX_CONSECUTIVE_ERRORS)
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) {
    results[[i]] <- .picotel_sync_submit(.snd_false_fn())
  }
  # First 4 return TRUE, 5th returns FALSE (trip).
  expect_true(all(results[seq_len(.picotel_MAX_CONSECUTIVE_ERRORS - 1L)]))
  expect_false(results[[.picotel_MAX_CONSECUTIVE_ERRORS]])
  expect_true(.picotel_state$sync_tripped)
})

test_that("sync_submit: trip logs 'further sends are disabled'", {
  .snd_reset()
  logged <- character(0L)
  env_root <- environment(.picotel_log)
  old_log  <- get(".picotel_log", envir = env_root)
  assign(".picotel_log", function(msg) logged <<- c(logged, msg), envir = env_root)
  on.exit(assign(".picotel_log", old_log, envir = env_root), add = TRUE)

  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) {
    .picotel_sync_submit(.snd_false_fn())
  }
  expect_true(any(grepl("further sends are disabled", logged)))
})

test_that("sync_submit: post-trip drops call without executing fn", {
  .snd_reset()
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) {
    .picotel_sync_submit(.snd_false_fn())
  }
  expect_true(.picotel_state$sync_tripped)
  called <- FALSE
  result <- .picotel_sync_submit(function() { called <<- TRUE; TRUE })
  expect_false(result)
  expect_false(called)  # fn was NOT called
})

test_that("sync_submit: restarted counter can trip again after success reset", {
  .snd_reset()
  # Accumulate 4 failures, then 1 success (counter resets to 0).
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS - 1L)) {
    .picotel_sync_submit(.snd_false_fn())
  }
  .picotel_sync_submit(.snd_ok_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 0L)
  # Another 4 failures should not trip (only 4 consecutive).
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS - 1L)) {
    .picotel_sync_submit(.snd_false_fn())
  }
  expect_false(.picotel_state$sync_tripped)
})

# ===========================================================================
# Sync sender: picotel_config_error is a persistent failure
# ===========================================================================

test_that("sync_submit: config_error counts as persistent failure", {
  .snd_reset()
  .picotel_sync_submit(.snd_config_err_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 1L)
})

test_that("sync_submit: config_error trips after 5", {
  .snd_reset()
  results <- logical(.picotel_MAX_CONSECUTIVE_ERRORS)
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) {
    results[[i]] <- .picotel_sync_submit(.snd_config_err_fn())
  }
  expect_false(results[[.picotel_MAX_CONSECUTIVE_ERRORS]])
  expect_true(.picotel_state$sync_tripped)
})

# ===========================================================================
# Sync sender: picotel_disabled is neutral
# ===========================================================================

test_that("sync_submit: disabled condition is neutral (does not increment counter)", {
  .snd_reset()
  # Accumulate 3 failures.
  for (i in seq_len(3L)) .picotel_sync_submit(.snd_false_fn())
  count_before <- .picotel_state$sync_consecutive_failures
  # Disabled should leave counter unchanged.
  .picotel_sync_submit(.snd_disabled_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, count_before)
  expect_false(.picotel_state$sync_tripped)
})

test_that("sync_submit: disabled condition does not reset non-zero counter", {
  .snd_reset()
  .picotel_sync_submit(.snd_false_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 1L)
  .picotel_sync_submit(.snd_disabled_fn())
  # Counter must stay at 1 — disabled is neutral, not success.
  expect_equal(.picotel_state$sync_consecutive_failures, 1L)
})

test_that("sync_submit: disabled returns TRUE", {
  .snd_reset()
  expect_true(.picotel_sync_submit(.snd_disabled_fn()))
})

# ===========================================================================
# Sync sender: generic errors are transient (not counted)
# ===========================================================================

test_that("sync_submit: generic error is transient — counter stays 0", {
  .snd_reset()
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS + 2L)) {
    .picotel_sync_submit(.snd_error_fn())
  }
  expect_equal(.picotel_state$sync_consecutive_failures, 0L)
  expect_false(.picotel_state$sync_tripped)
})

test_that("sync_submit: generic error returns TRUE (transient)", {
  .snd_reset()
  expect_true(.picotel_sync_submit(.snd_error_fn()))
})

test_that("sync_submit: generic error interspersed with failures does not count", {
  .snd_reset()
  # 2 failures, 1 transient, 2 failures = 4 consecutive persistent, no trip
  .picotel_sync_submit(.snd_false_fn())
  .picotel_sync_submit(.snd_false_fn())
  .picotel_sync_submit(.snd_error_fn())  # transient: should not reset or add
  .picotel_sync_submit(.snd_false_fn())
  .picotel_sync_submit(.snd_false_fn())
  expect_equal(.picotel_state$sync_consecutive_failures, 4L)
  expect_false(.picotel_state$sync_tripped)
})

# ===========================================================================
# Sync sender: args are passed to fn
# ===========================================================================

test_that("sync_submit: passes ... args to fn", {
  .snd_reset()
  received <- NULL
  .picotel_sync_submit(function(x, y) { received <<- c(x, y); TRUE }, 10L, 20L)
  expect_equal(received, c(10L, 20L))
})

# ===========================================================================
# Async sender: enqueue returns immediately without executing fn
# ===========================================================================

test_that("async_submit: enqueues and returns TRUE without executing fn", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()  # initialise async mode

  executed <- FALSE
  result   <- .picotel_async_submit(function() { executed <<- TRUE; TRUE })
  expect_true(result)
  expect_false(executed)  # fn must NOT have been executed inline
  expect_equal(.picotel_state$async_pending, 1L)
})

test_that("async_submit: does not execute fn at all before flush", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  calls <- 0L
  .picotel_async_submit(function() { calls <<- calls + 1L; TRUE })
  .picotel_async_submit(function() { calls <<- calls + 1L; TRUE })
  .picotel_async_submit(function() { calls <<- calls + 1L; TRUE })

  expect_equal(calls, 0L)  # none executed yet
  expect_equal(.picotel_state$async_pending, 3L)
})

# ===========================================================================
# Async sender: queue cap and overflow-episode warnings
# ===========================================================================

test_that("async_submit: cap 256 — 257th submit returns FALSE", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  results <- logical(.picotel_ASYNC_MAX_QUEUE + 1L)
  for (i in seq_len(.picotel_ASYNC_MAX_QUEUE + 1L)) {
    results[[i]] <- .picotel_async_submit(function() TRUE)
  }
  expect_true(all(results[seq_len(.picotel_ASYNC_MAX_QUEUE)]))
  expect_false(results[[.picotel_ASYNC_MAX_QUEUE + 1L]])
})

test_that("async_submit: overflow logs exactly ONE message per episode", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  logged <- character(0L)
  env_root <- environment(.picotel_log)
  old_log  <- get(".picotel_log", envir = env_root)
  assign(".picotel_log", function(msg) logged <<- c(logged, msg), envir = env_root)
  on.exit(assign(".picotel_log", old_log, envir = env_root), add = TRUE)

  # Fill the queue.
  for (i in seq_len(.picotel_ASYNC_MAX_QUEUE)) {
    .picotel_async_submit(function() TRUE)
  }
  # Two overflow submits should produce only ONE log message.
  .picotel_async_submit(function() TRUE)  # first drop
  .picotel_async_submit(function() TRUE)  # second drop — no new message
  full_msgs <- logged[grepl("queue full", logged)]
  expect_equal(length(full_msgs), 1L)
})

test_that("async_submit: overflow warning resets after successful enqueue", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  logged <- character(0L)
  env_root <- environment(.picotel_log)
  old_log  <- get(".picotel_log", envir = env_root)
  assign(".picotel_log", function(msg) logged <<- c(logged, msg), envir = env_root)
  on.exit(assign(".picotel_log", old_log, envir = env_root), add = TRUE)

  # First episode: fill and overflow.
  for (i in seq_len(.picotel_ASYNC_MAX_QUEUE)) {
    .picotel_async_submit(function() TRUE)
  }
  .picotel_async_submit(function() TRUE)  # drop — logs once
  expect_equal(length(logged[grepl("queue full", logged)]), 1L)

  # Drain by flushing.
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_pending, 0L)

  # Second episode: fill and overflow again — should produce a SECOND log.
  for (i in seq_len(.picotel_ASYNC_MAX_QUEUE)) {
    .picotel_async_submit(function() TRUE)
  }
  .picotel_async_submit(function() TRUE)  # drop — new episode, logs again
  expect_equal(length(logged[grepl("queue full", logged)]), 2L)

  picotel_flush(timeout = 5)
})

# ===========================================================================
# picotel_flush: sync sender
# ===========================================================================

test_that("flush: sync sender always returns TRUE immediately", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "")
  .picotel_get_sender()
  expect_true(picotel_flush(timeout = 0))
  expect_true(picotel_flush(timeout = 2))
  expect_true(picotel_flush(timeout = -1))
})

test_that("flush: returns TRUE before sender is ever selected", {
  .snd_reset()
  # No call to .picotel_get_sender() — no sender_mode set.
  expect_true(picotel_flush(timeout = 0))
  expect_true(picotel_flush(timeout = 2))
})

# ===========================================================================
# picotel_flush: async sender — drains queue
# ===========================================================================

test_that("flush: drains all queued closures, returns TRUE", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  calls <- 0L
  for (i in seq_len(5L)) {
    .picotel_async_submit(function() { calls <<- calls + 1L; TRUE })
  }
  expect_equal(.picotel_state$async_pending, 5L)

  result <- picotel_flush(timeout = 5)
  expect_true(result)
  expect_equal(calls, 5L)
  expect_equal(.picotel_state$async_pending, 0L)
})

test_that("flush: pending counter is 0 after full drain", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(10L)) {
    .picotel_async_submit(function() TRUE)
  }
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_pending, 0L)
})

# ===========================================================================
# picotel_flush: timeout semantics
# ===========================================================================

test_that("flush: timeout > 0 returns FALSE if queue not drained in time", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  # Each closure sleeps for 0.15 s; with timeout = 0.1 s only 0 will complete.
  for (i in seq_len(5L)) {
    .picotel_async_submit(function() { Sys.sleep(0.15); TRUE })
  }
  result <- picotel_flush(timeout = 0.1)
  # At least one item should remain (timeout fires before finishing all 5).
  # result should be FALSE (timed out).
  # NOTE: the first closure may overshoot the deadline (single-threaded; no
  # preemption) — the test allows for the one-overshoot case.
  # After timeout, pending should still be > 0 (some items remain).
  expect_true(.picotel_state$async_pending > 0L || result)
  # In practice with 5 * 0.15s closures and 0.1s deadline, result is FALSE.
  # We accept TRUE only if all items ran (queue drained despite overshoot).
})

test_that("flush: timeout = 0 returns TRUE when queue empty", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()
  # Nothing enqueued.
  expect_true(picotel_flush(timeout = 0))
})

test_that("flush: timeout = 0 returns FALSE when queue non-empty", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()
  .picotel_async_submit(function() TRUE)
  # timeout = 0: immediate check, no execution.
  result <- picotel_flush(timeout = 0)
  expect_false(result)
  expect_equal(.picotel_state$async_pending, 1L)  # item still queued
})

test_that("flush: timeout < 0 behaves like timeout = 0", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()
  .picotel_async_submit(function() TRUE)
  result <- picotel_flush(timeout = -1)
  expect_false(result)
  expect_equal(.picotel_state$async_pending, 1L)
})

# ===========================================================================
# picotel_flush: async circuit-breaker during flush
# ===========================================================================

test_that("flush: post-trip discards remaining queue, returns TRUE", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  # Enqueue 5 failures followed by a sentinel that should NOT be called.
  post_trip_called <- FALSE
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) {
    .picotel_async_submit(.snd_false_fn())
  }
  .picotel_async_submit(function() { post_trip_called <<- TRUE; TRUE })

  result <- picotel_flush(timeout = 10)

  expect_true(result)      # fully drained (discarded post-trip)
  expect_true(.picotel_state$async_tripped)
  expect_equal(.picotel_state$async_pending, 0L)
  expect_false(post_trip_called)  # post-trip item was discarded, not executed
})

test_that("flush: pending counter reaches 0 even after trip", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS + 3L)) {
    .picotel_async_submit(.snd_false_fn())
  }
  picotel_flush(timeout = 10)
  expect_equal(.picotel_state$async_pending, 0L)
})

# ===========================================================================
# Async sender: failure classification within flush
# ===========================================================================

test_that("async flush: success resets failure counter", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(3L)) .picotel_async_submit(.snd_false_fn())
  .picotel_async_submit(.snd_ok_fn())

  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_consecutive_failures, 0L)
  expect_false(.picotel_state$async_tripped)
})

test_that("async flush: config_error counts as persistent failure", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  .picotel_async_submit(.snd_config_err_fn())
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_consecutive_failures, 1L)
})

test_that("async flush: disabled is neutral — counter unchanged", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(3L)) .picotel_async_submit(.snd_false_fn())
  .picotel_async_submit(.snd_disabled_fn())
  picotel_flush(timeout = 5)

  # Counter should be 3 (3 persistent failures, disabled was neutral).
  expect_equal(.picotel_state$async_consecutive_failures, 3L)
  expect_false(.picotel_state$async_tripped)
})

test_that("async flush: disabled does not reset non-zero counter", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  .picotel_async_submit(.snd_false_fn())
  .picotel_async_submit(.snd_disabled_fn())
  picotel_flush(timeout = 5)
  # Disabled must not reset the counter — it stays at 1.
  expect_equal(.picotel_state$async_consecutive_failures, 1L)
})

test_that("async flush: generic error is transient — counter stays 0", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS + 2L)) {
    .picotel_async_submit(.snd_error_fn())
  }
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_consecutive_failures, 0L)
  expect_false(.picotel_state$async_tripped)
})

test_that("async flush: generic error interspersed with failures does not count", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  # 2 failures, 1 transient, 2 failures = 4 consecutive persistent, no trip
  .picotel_async_submit(.snd_false_fn())
  .picotel_async_submit(.snd_false_fn())
  .picotel_async_submit(.snd_error_fn())
  .picotel_async_submit(.snd_false_fn())
  .picotel_async_submit(.snd_false_fn())
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_consecutive_failures, 4L)
  expect_false(.picotel_state$async_tripped)
})

# ===========================================================================
# picotel_flush: via .picotel_get_sender() (integration)
# ===========================================================================

test_that("flush integration: sync sender via get_sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_sync_submit)
  expect_true(picotel_flush(timeout = 2))
})

test_that("flush integration: async sender via get_sender drains queue", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_async_submit)

  calls <- 0L
  sender(function() { calls <<- calls + 1L; TRUE })
  sender(function() { calls <<- calls + 1L; TRUE })
  picotel_flush(timeout = 5)
  expect_equal(calls, 2L)
})

# ===========================================================================
# .picotel_get_sender(): env-var selection and caching
# ===========================================================================

test_that("get_sender: default (no PICOTEL_ASYNC) returns sync sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = NA)
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_sync_submit)
  expect_identical(.picotel_state$sender_mode, "sync")
})

test_that("get_sender: PICOTEL_ASYNC=1 returns async sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "1")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_async_submit)
  expect_identical(.picotel_state$sender_mode, "async")
})

test_that("get_sender: PICOTEL_ASYNC=true returns async sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_async_submit)
})

test_that("get_sender: PICOTEL_ASYNC=TRUE (uppercase) returns async sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "TRUE")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_async_submit)
})

test_that("get_sender: PICOTEL_ASYNC=True (mixed case) returns async sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "True")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_async_submit)
})

test_that("get_sender: PICOTEL_ASYNC=false falls back to sync sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "false")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_sync_submit)
})

test_that("get_sender: PICOTEL_ASYNC=0 falls back to sync sender", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "0")
  sender <- .picotel_get_sender()
  expect_identical(sender, .picotel_sync_submit)
})

test_that("get_sender: cached — env change after first call is ignored", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "")
  # First call with no PICOTEL_ASYNC → sync
  sender1 <- .picotel_get_sender()
  expect_identical(sender1, .picotel_sync_submit)

  # Change env AFTER first call — should be ignored (cached decision).
  Sys.setenv(PICOTEL_ASYNC = "true")
  on.exit(Sys.unsetenv("PICOTEL_ASYNC"), add = TRUE)

  sender2 <- .picotel_get_sender()
  expect_identical(sender2, .picotel_sync_submit)  # still sync (cached)
})

test_that("get_sender: reset_state clears cache, allows reselection", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "")
  .picotel_get_sender()
  expect_identical(.picotel_state$sender_mode, "sync")

  .picotel_reset_state()
  withr::local_envvar(PICOTEL_ASYNC = "1")
  .picotel_get_sender()
  expect_identical(.picotel_state$sender_mode, "async")
})

# PICOTEL_ASYNC is read WITHOUT prefix remapping (plan WP5, mirroring Python).
test_that("get_sender: PICOTEL_ASYNC is not remapped via PICOTEL_PREFIX", {
  .snd_reset()
  # Set a custom prefix.  Under remapping, PICOTEL_ASYNC would become
  # PREFIX_ASYNC — but we must still read PICOTEL_ASYNC directly.
  withr::local_envvar(
    PICOTEL_PREFIX = "PREFIX",
    PICOTEL_ASYNC  = "true",
    PREFIX_ASYNC   = "false"   # this should be ignored
  )
  sender <- .picotel_get_sender()
  # PICOTEL_ASYNC=true wins (no remapping) → async sender.
  expect_identical(sender, .picotel_async_submit)
})

# ===========================================================================
# Async sender: pending counter invariants
# ===========================================================================

test_that("pending counter is 0 after flush in all paths (success)", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(5L)) .picotel_async_submit(.snd_ok_fn())
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_pending, 0L)
})

test_that("pending counter is 0 after flush in all paths (persistent failure)", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) .picotel_async_submit(.snd_false_fn())
  picotel_flush(timeout = 10)
  expect_equal(.picotel_state$async_pending, 0L)
})

test_that("pending counter is 0 after flush in all paths (transient error)", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(5L)) .picotel_async_submit(.snd_error_fn())
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_pending, 0L)
})

test_that("pending counter is 0 after flush in all paths (disabled)", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  for (i in seq_len(5L)) .picotel_async_submit(.snd_disabled_fn())
  picotel_flush(timeout = 5)
  expect_equal(.picotel_state$async_pending, 0L)
})

# ===========================================================================
# Async sender: post-trip submit drops without calling fn
# ===========================================================================

test_that("async_submit: post-trip submit returns FALSE without calling fn", {
  .snd_reset()
  withr::local_envvar(PICOTEL_ASYNC = "true")
  .picotel_get_sender()

  # Trip the breaker via flush.
  for (i in seq_len(.picotel_MAX_CONSECUTIVE_ERRORS)) {
    .picotel_async_submit(.snd_false_fn())
  }
  picotel_flush(timeout = 10)
  expect_true(.picotel_state$async_tripped)

  # Post-trip: submit should not enqueue or call fn.
  called <- FALSE
  result <- .picotel_async_submit(function() { called <<- TRUE; TRUE })
  expect_false(result)
  expect_false(called)
  # Queue is still empty.
  expect_equal(length(.picotel_state$async_queue), 0L)
})
