# Tests for WP2 env-config helpers:
#   .picotel_endpoint(), .picotel_resource_from_env()
#
# Ported from tests/test_env_config.py — endpoint, resource/service-name, and
# disabled sections only.  Header and TLS sections belong to WP4 and are
# intentionally omitted here (see WP4 scope in r/PLAN.md).
#
# NOTE on caching: Python's _get_endpoint, _get_resource_from_env, and
# _parse_traceparent are all decorated with @functools.lru_cache, so they also
# cache.  R caches in .picotel_state; each test that changes env vars calls
# .picotel_reset_state() first to ensure a fresh read.
#
# NOTE on withr::local_envvar(list(VAR = NA)): NA unsets the variable (unlike
# Sys.setenv("") which sets it to "").  This is the correct way to simulate
# "variable not set" in tests per plan D9 / PLAN.md Known Hazards.

# ---------------------------------------------------------------------------
# Helper: .picotel_prefixed() mirrors Python's _prefixed() in test_env_config.py
# ---------------------------------------------------------------------------

.picotel_prefixed <- function(env, prefix) {
  if (nchar(prefix) == 0L) return(env)
  result <- c(PICOTEL_PREFIX = prefix)
  for (nm in names(env)) {
    if (startsWith(nm, "OTEL_")) {
      new_nm <- paste0(prefix, "_", substring(nm, 6L))
    } else {
      new_nm <- paste0(prefix, "_", nm)
    }
    result[[new_nm]] <- env[[nm]]
  }
  result
}

# Prefixes to parametrize over (mirrors Python PREFIXES fixture)
.picotel_test_prefixes <- c("", "PICOTEL")

# ---------------------------------------------------------------------------
# .picotel_endpoint() — endpoint resolution
# ---------------------------------------------------------------------------

test_that("signal-specific traces endpoint takes precedence over general", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(
      OTEL_EXPORTER_OTLP_ENDPOINT        = "http://general:4318",
      OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://traces:4318"
    ), prefix)
    withr::local_envvar(as.list(env))
    .picotel_reset_state()
    expect_equal(.picotel_endpoint("traces"), "http://traces:4318",
                 label = sprintf("prefix='%s' traces specific", prefix))
  }
})

test_that("signal-specific logs endpoint takes precedence over general", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(
      OTEL_EXPORTER_OTLP_ENDPOINT      = "http://general:4318",
      OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = "http://logs:4318"
    ), prefix)
    withr::local_envvar(as.list(env))
    .picotel_reset_state()
    expect_equal(.picotel_endpoint("logs"), "http://logs:4318",
                 label = sprintf("prefix='%s' logs specific", prefix))
  }
})

test_that("fallback to general endpoint appends /v1/<signal>", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(
      OTEL_EXPORTER_OTLP_ENDPOINT = "http://general:4318"
    ), prefix)
    withr::local_envvar(as.list(env))
    .picotel_reset_state()
    expect_equal(.picotel_endpoint("traces"), "http://general:4318/v1/traces",
                 label = sprintf("prefix='%s' traces fallback", prefix))
    .picotel_reset_state()
    expect_equal(.picotel_endpoint("logs"), "http://general:4318/v1/logs",
                 label = sprintf("prefix='%s' logs fallback", prefix))
  }
})

test_that("endpoint returns NULL when no env vars set", {
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_ENDPOINT        = NA,
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = NA,
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT   = NA,
    PICOTEL_PREFIX                     = NA
  ))
  .picotel_reset_state()
  expect_null(.picotel_endpoint("traces"))
  .picotel_reset_state()
  expect_null(.picotel_endpoint("logs"))
})

test_that("trailing slashes on general endpoint are stripped before appending", {
  # Python: base.rstrip("/") strips ALL trailing slashes.
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://general:4318//",
    PICOTEL_PREFIX = NA
  ))
  .picotel_reset_state()
  expect_equal(.picotel_endpoint("traces"), "http://general:4318/v1/traces")
})

test_that("signal-specific endpoint is returned verbatim even with trailing slash", {
  # Signal-specific vars are not modified (Python returns them as-is).
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = "http://traces:4318/v1/traces/",
    PICOTEL_PREFIX = NA
  ))
  .picotel_reset_state()
  expect_equal(.picotel_endpoint("traces"), "http://traces:4318/v1/traces/")
})

# R-specific: empty-string general env var is treated as "not set" because
# Sys.getenv() returns "" for both unset and empty vars (we use nchar() > 0 check
# matching Python's truthiness test on the env-var value).
test_that("empty-string general endpoint is treated as not set", {
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_ENDPOINT = "",
    PICOTEL_PREFIX = NA
  ))
  .picotel_reset_state()
  expect_null(.picotel_endpoint("traces"))
})

test_that("endpoint result is cached in .picotel_state", {
  withr::local_envvar(list(
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://cached:4318",
    PICOTEL_PREFIX = NA
  ))
  .picotel_reset_state()
  r1 <- .picotel_endpoint("traces")
  # Change env var without resetting state — cached result must be returned.
  withr::local_envvar(list(OTEL_EXPORTER_OTLP_ENDPOINT = "http://changed:4318"))
  r2 <- .picotel_endpoint("traces")
  expect_equal(r1, r2, info = "endpoint should be cached after first call")
  # After reset the new value is picked up.
  .picotel_reset_state()
  r3 <- .picotel_endpoint("traces")
  expect_equal(r3, "http://changed:4318/v1/traces")
})

# ---------------------------------------------------------------------------
# .picotel_resource_from_env() — resource from env
# ---------------------------------------------------------------------------

test_that("resource from OTEL_SERVICE_NAME", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(OTEL_SERVICE_NAME = "my-service"), prefix)
    withr::local_envvar(as.list(env))
    .picotel_reset_state()
    r <- .picotel_resource_from_env()
    expect_true(inherits(r, "picotel_resource"),
                label = sprintf("prefix='%s' is picotel_resource", prefix))
    expect_equal(r$attributes[["service.name"]], "my-service",
                 label = sprintf("prefix='%s' service.name", prefix))
    expect_equal(length(r$attributes), 1L,
                 label = sprintf("prefix='%s' only one attr", prefix))
  }
})

test_that("resource returns NULL when no env vars set", {
  for (prefix in .picotel_test_prefixes) {
    env_names <- if (prefix == "") {
      list(OTEL_SERVICE_NAME = NA, OTEL_RESOURCE_ATTRIBUTES = NA,
           PICOTEL_PREFIX = NA)
    } else {
      setNames(
        rep(list(NA), 5L),
        c("OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES",
          paste0(prefix, "_SERVICE_NAME"),
          paste0(prefix, "_RESOURCE_ATTRIBUTES"),
          "PICOTEL_PREFIX")
      )
    }
    if (prefix != "") env_names[["PICOTEL_PREFIX"]] <- prefix
    withr::local_envvar(env_names)
    .picotel_reset_state()
    expect_null(.picotel_resource_from_env(),
                label = sprintf("prefix='%s' NULL when unset", prefix))
  }
})

test_that("resource attributes basic key=value pairs", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(
      OTEL_SERVICE_NAME        = "my-service",
      OTEL_RESOURCE_ATTRIBUTES = "content.guid=abc-123,deployment.env=prod"
    ), prefix)
    withr::local_envvar(as.list(env))
    .picotel_reset_state()
    r <- .picotel_resource_from_env()
    expect_true(inherits(r, "picotel_resource"),
                label = sprintf("prefix='%s'", prefix))
    expect_equal(r$attributes[["service.name"]], "my-service",
                 label = sprintf("prefix='%s' service.name", prefix))
    expect_equal(r$attributes[["content.guid"]], "abc-123",
                 label = sprintf("prefix='%s' content.guid", prefix))
    expect_equal(r$attributes[["deployment.env"]], "prod",
                 label = sprintf("prefix='%s' deployment.env", prefix))
  }
})

test_that("resource attributes without service name", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(
      OTEL_RESOURCE_ATTRIBUTES = "content.guid=abc-123"
    ), prefix)
    unset_svc_key <- if (prefix == "") "OTEL_SERVICE_NAME"
                     else paste0(prefix, "_SERVICE_NAME")
    env_list <- as.list(env)
    env_list[[unset_svc_key]] <- NA
    withr::local_envvar(env_list)
    .picotel_reset_state()
    r <- .picotel_resource_from_env()
    expect_true(inherits(r, "picotel_resource"),
                label = sprintf("prefix='%s'", prefix))
    expect_equal(r$attributes[["content.guid"]], "abc-123",
                 label = sprintf("prefix='%s'", prefix))
  }
})

test_that("OTEL_SERVICE_NAME overrides service.name in resource attributes", {
  for (prefix in .picotel_test_prefixes) {
    env <- .picotel_prefixed(c(
      OTEL_SERVICE_NAME        = "explicit-name",
      OTEL_RESOURCE_ATTRIBUTES = "service.name=from-attrs,other=val"
    ), prefix)
    withr::local_envvar(as.list(env))
    .picotel_reset_state()
    r <- .picotel_resource_from_env()
    expect_equal(r$attributes[["service.name"]], "explicit-name",
                 label = sprintf("prefix='%s' service.name wins", prefix))
    expect_equal(r$attributes[["other"]], "val",
                 label = sprintf("prefix='%s' other preserved", prefix))
  }
})

test_that("resource attributes: percent-encoded comma in value decoded", {
  # "a,b,c" encoded as "a%2Cb%2Cc"
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "tags=a%2Cb%2Cc",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["tags"]], "a,b,c")
})

test_that("resource attributes: percent-encoded equals in value decoded", {
  # "x=1" encoded as "x%3D1"
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "expr=x%3D1",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["expr"]], "x=1")
})

test_that("resource attributes: percent-encoded key decoded", {
  # "my,key" encoded as "my%2Ckey"
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "my%2Ckey=value",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["my,key"]], "value")
})

test_that("resource attributes: percent-encoded spaces and unicode", {
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "msg=hello%20world,place=caf%C3%A9",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["msg"]], "hello world")
  expect_equal(r$attributes[["place"]], "café")
})

test_that("resource attributes: all values are strings", {
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "count=42,enabled=true,ratio=3.14",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["count"]],   "42")
  expect_equal(r$attributes[["enabled"]], "true")
  expect_equal(r$attributes[["ratio"]],   "3.14")
  for (v in r$attributes) {
    expect_type(v, "character")
  }
})

# R-specific: '+' must NOT be decoded as space (path-unescape, not query-unescape).
# Python's urllib.parse.unquote also does NOT decode '+' as space, confirming
# matching semantics.
test_that("resource attributes: plus sign preserved (not decoded as space)", {
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "key=hello+world",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["key"]], "hello+world")
})

# R-specific: invalid percent-encoding kept raw (matches Python's lenient
# urllib.parse.unquote() which keeps raw bytes on decode error).
test_that("resource attributes: invalid percent-encoding kept raw", {
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "key=%zz",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_equal(r$attributes[["key"]], "%zz")
})

# Malformed pair (no '=') is silently skipped; valid pair still parsed.
test_that("resource attributes: malformed pair without '=' is skipped", {
  withr::local_envvar(list(
    OTEL_RESOURCE_ATTRIBUTES = "noequals,good=val",
    PICOTEL_PREFIX = NA,
    OTEL_SERVICE_NAME = NA
  ))
  .picotel_reset_state()
  r <- .picotel_resource_from_env()
  expect_null(r$attributes[["noequals"]])
  expect_equal(r$attributes[["good"]], "val")
})

test_that("resource result is cached in .picotel_state", {
  withr::local_envvar(list(
    OTEL_SERVICE_NAME = "cached-service",
    PICOTEL_PREFIX = NA
  ))
  .picotel_reset_state()
  r1 <- .picotel_resource_from_env()
  withr::local_envvar(list(OTEL_SERVICE_NAME = "changed-service"))
  r2 <- .picotel_resource_from_env()
  expect_equal(r1$attributes[["service.name"]], r2$attributes[["service.name"]],
               info = "resource should be cached after first call")
  .picotel_reset_state()
  r3 <- .picotel_resource_from_env()
  expect_equal(r3$attributes[["service.name"]], "changed-service")
})

# ---------------------------------------------------------------------------
# .picotel_is_disabled() — SDK disabled (already tested in test-helpers.R;
# parametric coverage matching Python's test_is_disabled)
# ---------------------------------------------------------------------------

test_that(".picotel_is_disabled() recognises truthy/falsy values with both prefixes", {
  # truthy
  for (prefix in .picotel_test_prefixes) {
    for (val in c("true", "TRUE", "1")) {
      env <- as.list(.picotel_prefixed(c(OTEL_SDK_DISABLED = val), prefix))
      withr::local_envvar(env)
      expect_true(.picotel_is_disabled(),
                  label = sprintf("prefix='%s' val='%s' should be truthy", prefix, val))
    }
    # falsy
    for (val in c("false", "0")) {
      env <- as.list(.picotel_prefixed(c(OTEL_SDK_DISABLED = val), prefix))
      withr::local_envvar(env)
      expect_false(.picotel_is_disabled(),
                   label = sprintf("prefix='%s' val='%s' should be falsy", prefix, val))
    }
  }
})

test_that(".picotel_is_disabled() is FALSE when SDK_DISABLED unset (both prefixes)", {
  for (prefix in .picotel_test_prefixes) {
    unset_list <- list(
      OTEL_SDK_DISABLED = NA,
      PICOTEL_PREFIX = if (prefix == "") NA else prefix
    )
    if (prefix != "") {
      unset_list[[paste0(prefix, "_SDK_DISABLED")]] <- NA
    }
    withr::local_envvar(unset_list)
    expect_false(.picotel_is_disabled(),
                 label = sprintf("prefix='%s' should be FALSE when unset", prefix))
  }
})
