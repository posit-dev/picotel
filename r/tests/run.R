#!/usr/bin/env Rscript
# Standalone testthat runner.  Invoke from the repository root:
#
#   Rscript r/tests/run.R
#
# Exits with a nonzero status on any test failure, so CI pipelines and the
# orchestrator can detect failures reliably.

# Locate the repo root and the testthat directory relative to this script.
# Using normalizePath(sys.frame(0)) is not reliable across all invocation
# styles; instead we use a fixed path relative to the script's own location.

script_dir <- tryCatch(
  dirname(normalizePath(
    if (!is.null(sys.frames()[[1L]])) {
      # Called via source() — find the sourced file
      attr(sys.frames()[[1L]], "srcfile")$filename
    } else {
      # Called via Rscript — commandArgs contains the script path
      sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE))
    }
  )),
  error = function(e) getwd()
)

testthat_dir <- file.path(script_dir, "testthat")

if (!dir.exists(testthat_dir)) {
  # Fallback: assume we are at the repo root
  testthat_dir <- file.path("r", "tests", "testthat")
}

# Run the suite, printing a summary and exiting nonzero on failure.
result <- testthat::test_dir(
  testthat_dir,
  reporter = testthat::SummaryReporter$new(),
  stop_on_failure = TRUE
)

invisible(result)
