# Helper sourced automatically by testthat before each test file.
# Loads picotel.R from the repository root and resets mutable state.

# Locate the repository root robustly: testthat::test_path("../..")
# resolves to the tests/ parent regardless of the working directory when
# testthat is invoked (from Rscript, devtools::test(), or an IDE).

.picotel_root <- function() {
  normalizePath(file.path(testthat::test_path(), "..", ".."))
}

source(file.path(.picotel_root(), "picotel.R"))

# Reset all mutable state so each test file starts from a clean slate.
.picotel_reset_state()
