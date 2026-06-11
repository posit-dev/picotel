# Tests for WP3: OTLP JSON encoding and span validation.
#
# Ported from:
#   tests/test_to_otlp_value.py
#   tests/test_attributes_to_otlp.py
#   tests/test_span_to_dict.py
# Plus R-specific cases for NA/NaN/Inf, factor, length-n vectors, named lists,
# and the hand-rolled .picotel_to_json() encoder.
#
# Python cases not ported (with reason):
#   test_bytes_value_base64_encoded — R has no bytes type analogous to Python
#     bytes; raw vectors are a different use case and OTLP attribute values are
#     not expected to be raw in R.  A raw vector falls through to the as.character
#     fallback (which is documented).

# ---------------------------------------------------------------------------
# Test fixture helpers
# ---------------------------------------------------------------------------

# .make_span() — create a minimal picotel_span-like list for serialisation tests.
# WP6 constructors are not yet implemented (TODO stubs); we build bare lists
# with the required fields directly.
.make_span <- function(
  trace_id       = new_trace_id(),
  span_id        = new_span_id(),
  name           = "test_op",
  kind           = SpanKind$INTERNAL,
  start_time_ns  = 1000000000,
  end_time_ns    = 1001000000,
  parent_span_id = "",
  attributes     = list(),
  events         = list(),
  links          = list(),
  status         = NULL
) {
  list(
    trace_id       = trace_id,
    span_id        = span_id,
    name           = name,
    kind           = kind,
    start_time_ns  = start_time_ns,
    end_time_ns    = end_time_ns,
    parent_span_id = parent_span_id,
    attributes     = attributes,
    events         = events,
    links          = links,
    status         = status
  )
}

# .make_log() — create a minimal picotel_log_record-like list for tests.
.make_log <- function(
  body                  = "test message",
  timestamp_ns          = 1000000000,
  observed_timestamp_ns = 2000000000,
  severity_number       = Severity$INFO,
  severity_text         = "",
  attributes            = list(),
  trace_id              = "",
  span_id               = "",
  trace_flags           = 0L
) {
  list(
    body                  = body,
    timestamp_ns          = timestamp_ns,
    observed_timestamp_ns = observed_timestamp_ns,
    severity_number       = severity_number,
    severity_text         = severity_text,
    attributes            = attributes,
    trace_id              = trace_id,
    span_id               = span_id,
    trace_flags           = trace_flags
  )
}

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — NULL and NA
# ---------------------------------------------------------------------------

test_that(".picotel_to_otlp_value(NULL) returns empty OTLP map (mirrors Python None -> {})", {
  # Decision: NULL -> an empty named list so the JSON encoder produces "{}".
  # Rationale: Python None -> {} (empty dict/object); R list() (unnamed) would
  # produce "[]" per the plan D1 convention, so we use setNames(list(), character(0))
  # to represent the empty-object case.
  result <- .picotel_to_otlp_value(NULL)
  expect_true(is.list(result))
  expect_equal(length(result), 0L)
  # The JSON representation must be "{}" (empty object, not empty array)
  expect_equal(.picotel_to_json(result), "{}")
})

test_that(".picotel_to_otlp_value(NA) returns empty OTLP map (R-specific: NA treated as None)", {
  # Decision: scalar NA of any flavour is treated as missing/None.
  # Callers that want the string "NA" must pass as.character(NA).
  for (na_val in list(NA, NA_integer_, NA_real_, NA_character_)) {
    result <- .picotel_to_otlp_value(na_val)
    expect_true(is.list(result))
    expect_equal(length(result), 0L)
    expect_equal(.picotel_to_json(result), "{}")
  }
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — logical (boolValue)
# ---------------------------------------------------------------------------

test_that(".picotel_to_otlp_value(TRUE) -> boolValue TRUE", {
  expect_equal(.picotel_to_otlp_value(TRUE),  list(boolValue = TRUE))
})

test_that(".picotel_to_otlp_value(FALSE) -> boolValue FALSE", {
  expect_equal(.picotel_to_otlp_value(FALSE), list(boolValue = FALSE))
})

test_that("logical is dispatched before numeric (logical is not numeric in R)", {
  # TRUE and FALSE must produce boolValue, not intValue 1/0
  result_true  <- .picotel_to_otlp_value(TRUE)
  result_false <- .picotel_to_otlp_value(FALSE)
  expect_true("boolValue" %in% names(result_true))
  expect_true("boolValue" %in% names(result_false))
  expect_false("intValue" %in% names(result_true))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — integer (intValue as string)
# ---------------------------------------------------------------------------

test_that(".picotel_to_otlp_value(integer) -> intValue as decimal string", {
  expect_equal(.picotel_to_otlp_value(42L),  list(intValue = "42"))
  expect_equal(.picotel_to_otlp_value(-1L),  list(intValue = "-1"))
  expect_equal(.picotel_to_otlp_value(0L),   list(intValue = "0"))
})

test_that("intValue string has no scientific notation", {
  s <- .picotel_to_otlp_value(2147483647L)$intValue  # .Machine$integer.max
  expect_false(grepl("[eE]", s))
  expect_equal(s, "2147483647")
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — double / whole-number double
# ---------------------------------------------------------------------------

test_that("whole-number double -> intValue (mirrors Python int path for 42.0)", {
  expect_equal(.picotel_to_otlp_value(42.0),  list(intValue = "42"))
  expect_equal(.picotel_to_otlp_value(0.0),   list(intValue = "0"))
  expect_equal(.picotel_to_otlp_value(-5.0),  list(intValue = "-5"))
})

test_that("large integer as double -> intValue string, no sci notation (plan D5)", {
  # 9223372036854775807 == max int64, but R doubles cannot represent this exactly.
  # The nearest representable double is 9223372036854775808 (or 9223372036854777856
  # depending on platform rounding).  We test the structural property: a large whole-
  # number double always produces intValue as a decimal string with no sci notation,
  # and the string round-trips to the same double value.
  big    <- 9223372036854775807  # R stores the nearest representable double
  result <- .picotel_to_otlp_value(big)
  expect_true("intValue" %in% names(result))
  expect_false(grepl("[eE]", result$intValue))
  # String must round-trip: as.numeric of the string == the stored double
  expect_equal(as.numeric(result$intValue), big)
})

test_that("non-whole double -> doubleValue (numeric)", {
  result <- .picotel_to_otlp_value(3.14)
  expect_equal(result, list(doubleValue = 3.14))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — NaN / Inf (proto3 JSON per Go WP3 commit f1c4cfb)
# ---------------------------------------------------------------------------

test_that("NaN -> doubleValue 'NaN' (proto3 JSON string)", {
  expect_equal(.picotel_to_otlp_value(NaN), list(doubleValue = "NaN"))
})

test_that("Inf -> doubleValue 'Infinity' (proto3 JSON string)", {
  expect_equal(.picotel_to_otlp_value(Inf), list(doubleValue = "Infinity"))
})

test_that("-Inf -> doubleValue '-Infinity' (proto3 JSON string)", {
  expect_equal(.picotel_to_otlp_value(-Inf), list(doubleValue = "-Infinity"))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — character (stringValue)
# ---------------------------------------------------------------------------

test_that("character -> stringValue", {
  expect_equal(.picotel_to_otlp_value("hello"), list(stringValue = "hello"))
  expect_equal(.picotel_to_otlp_value(""),      list(stringValue = ""))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — factor (stringValue via as.character)
# ---------------------------------------------------------------------------

test_that("factor -> stringValue via as.character", {
  f <- factor("apple", levels = c("apple", "banana"))
  expect_equal(.picotel_to_otlp_value(f), list(stringValue = "apple"))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — length > 1 vectors -> arrayValue
# (R has no scalars: length-n vector is an OTLP array)
# ---------------------------------------------------------------------------

test_that("length-2 character vector -> arrayValue", {
  result <- .picotel_to_otlp_value(c("a", "b"))
  expect_equal(result, list(arrayValue = list(values = list(
    list(stringValue = "a"),
    list(stringValue = "b")
  ))))
})

test_that("length-3 integer vector -> arrayValue of intValues", {
  result <- .picotel_to_otlp_value(c(1L, 2L, 3L))
  expect_equal(result, list(arrayValue = list(values = list(
    list(intValue = "1"),
    list(intValue = "2"),
    list(intValue = "3")
  ))))
})

test_that("length-3 logical vector -> arrayValue of boolValues", {
  result <- .picotel_to_otlp_value(c(TRUE, FALSE, TRUE))
  expect_equal(result, list(arrayValue = list(values = list(
    list(boolValue = TRUE),
    list(boolValue = FALSE),
    list(boolValue = TRUE)
  ))))
})

test_that("length-n double vector -> arrayValue", {
  result <- .picotel_to_otlp_value(c(1.5, 2.5))
  expect_equal(result, list(arrayValue = list(values = list(
    list(doubleValue = 1.5),
    list(doubleValue = 2.5)
  ))))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — unnamed list -> arrayValue
# (ported from Python test_nested_structures)
# ---------------------------------------------------------------------------

test_that("mixed-type unnamed list -> arrayValue (ported from test_nested_structures)", {
  # Python: ["hello", 42, True, 3.14, None]
  # None -> {} (empty object).  In R, NULL inside a list -> .picotel_empty_otlp_map()
  # which is a named-empty list encoding as JSON "{}".
  result <- .picotel_to_otlp_value(list("hello", 42L, TRUE, 3.14, NULL))
  values <- result$arrayValue$values
  expect_equal(length(values), 5L)
  expect_equal(values[[1L]], list(stringValue = "hello"))
  expect_equal(values[[2L]], list(intValue    = "42"))
  expect_equal(values[[3L]], list(boolValue   = TRUE))
  expect_equal(values[[4L]], list(doubleValue = 3.14))
  # NULL -> empty OTLP map (JSON: "{}")
  expect_equal(.picotel_to_json(values[[5L]]), "{}")
  expect_equal(length(values[[5L]]), 0L)
})

test_that("NA inside list element -> empty OTLP map (R-specific: NA treated as None)", {
  result <- .picotel_to_otlp_value(list(1L, NA, 2L))
  values <- result$arrayValue$values
  # NA -> empty OTLP map (JSON: "{}")
  expect_equal(.picotel_to_json(values[[2L]]), "{}")
  expect_equal(length(values[[2L]]), 0L)
})

test_that("empty unnamed list -> arrayValue with empty values", {
  result <- .picotel_to_otlp_value(list())
  expect_equal(result, list(arrayValue = list(values = list())))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — named list -> kvlistValue
# (ported from Python test_nested_structures dict case)
# ---------------------------------------------------------------------------

test_that("named list -> kvlistValue (ported from test_nested_structures dict)", {
  # Python: {"string": "hello", "int": 42, "bool": True, "float": 3.14,
  #          "none": None, "list": [1, 2], "dict": {"nested": "value"}}
  # R: insertion order is preserved (unlike Go which sorts keys)
  input <- list(
    string = "hello",
    int    = 42L,
    bool   = TRUE,
    float  = 3.14,
    none   = NULL,
    lst    = list(1L, 2L),
    dict   = list(nested = "value")
  )
  result <- .picotel_to_otlp_value(input)
  values <- result$kvlistValue$values
  expect_equal(length(values), 7L)

  # Build lookup by key
  by_key <- setNames(lapply(values, `[[`, "value"), sapply(values, `[[`, "key"))

  expect_equal(by_key[["string"]], list(stringValue = "hello"))
  expect_equal(by_key[["int"]],    list(intValue    = "42"))
  expect_equal(by_key[["bool"]],   list(boolValue   = TRUE))
  expect_equal(by_key[["float"]],  list(doubleValue = 3.14))
  # NULL -> empty OTLP map (JSON: "{}")
  expect_equal(.picotel_to_json(by_key[["none"]]), "{}")
  expect_equal(length(by_key[["none"]]), 0L)
  expect_equal(by_key[["lst"]], list(arrayValue = list(values = list(
    list(intValue = "1"),
    list(intValue = "2")
  ))))
  expect_equal(by_key[["dict"]], list(kvlistValue = list(values = list(
    list(key = "nested", value = list(stringValue = "value"))
  ))))
})

test_that("named list preserves insertion order (R-specific; Go sorts keys)", {
  # In R named lists maintain insertion order; verify the order is preserved
  input  <- list(z = 1L, a = 2L, m = 3L)
  result <- .picotel_to_otlp_value(input)
  keys   <- sapply(result$kvlistValue$values, `[[`, "key")
  expect_equal(keys, c("z", "a", "m"))
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — large integer (Python test_large_integer)
# ---------------------------------------------------------------------------

test_that("large integer (max int64 as double) -> intValue string (test_large_integer)", {
  # Python test_large_integer: 9223372036854775807 (max int64).
  # R stores this as the nearest representable double (9223372036854775808 or similar).
  # The key property: it must produce intValue as a decimal string, not doubleValue.
  large_int <- 9223372036854775807  # R stores nearest representable double
  result    <- .picotel_to_otlp_value(large_int)
  expect_true("intValue" %in% names(result))
  # String should not contain scientific notation
  expect_false(grepl("[eE]", result$intValue))
  # String must round-trip to the same double value
  expect_equal(as.numeric(result$intValue), large_int)
})

# ---------------------------------------------------------------------------
# .picotel_to_otlp_value — fallback for unknown types
# ---------------------------------------------------------------------------

test_that("fallback: unrecognised type -> stringValue via as.character", {
  # Python test_unknown_type_fallback: CustomClass with __str__ -> "custom_value"
  # R: we use a complex scalar as the "unknown type" since R complex has no OTLP
  # mapping and falls through to as.character.
  # NOTE: Python bytes -> bytesValue (base64) is NOT ported; R has no bytes type
  # analogous to Python bytes.  raw() is a length-n atomic vector and becomes
  # an arrayValue in R (each byte as a stringValue hex string via as.character).
  result <- .picotel_to_otlp_value(1 + 2i)
  expect_true("stringValue" %in% names(result))
  # as.character(1+2i) == "1+2i"
  expect_equal(result$stringValue, "1+2i")
})

# ---------------------------------------------------------------------------
# .picotel_attributes_to_otlp
# ---------------------------------------------------------------------------

test_that(".picotel_attributes_to_otlp: basic string and integer attributes", {
  # Ported from test_basic_attributes
  # R preserves insertion order (unlike Go which sorts)
  attributes <- list(foo = "bar", count = 5L)
  result     <- .picotel_attributes_to_otlp(attributes)
  expected   <- list(
    list(key = "foo",   value = list(stringValue = "bar")),
    list(key = "count", value = list(intValue    = "5"))
  )
  expect_equal(result, expected)
})

test_that(".picotel_attributes_to_otlp: NULL values are skipped (test_none_value_skipped)", {
  attributes <- list(a = NULL, b = "test", c = NULL)
  result     <- .picotel_attributes_to_otlp(attributes)
  expected   <- list(list(key = "b", value = list(stringValue = "test")))
  expect_equal(result, expected)
})

test_that(".picotel_attributes_to_otlp: scalar NA at top level is skipped (like NULL)", {
  # R-specific: scalar NA treated as None/missing at the top level -> skip entry
  attributes <- list(top_na = NA)
  result     <- .picotel_attributes_to_otlp(attributes)
  expect_equal(length(result), 0L)
})

test_that(".picotel_attributes_to_otlp: NULL inside list becomes empty OTLP map (test_nested_none_in_values)", {
  # NULL inside a nested list -> becomes empty OTLP map inside arrayValue
  # (mirrors Python: None inside list -> {})
  attributes <- list(list_with_none = list(1L, NULL, 2L))
  result     <- .picotel_attributes_to_otlp(attributes)
  expect_equal(length(result), 1L)
  expect_equal(result[[1L]]$key, "list_with_none")
  values <- result[[1L]]$value$arrayValue$values
  expect_equal(values[[1L]], list(intValue = "1"))
  # NULL -> empty OTLP map (JSON: "{}")
  expect_equal(.picotel_to_json(values[[2L]]), "{}")
  expect_equal(length(values[[2L]]), 0L)
  expect_equal(values[[3L]], list(intValue = "2"))
})

test_that(".picotel_attributes_to_otlp: NULL attributes returns empty list", {
  expect_equal(.picotel_attributes_to_otlp(NULL),  list())
  expect_equal(.picotel_attributes_to_otlp(list()), list())
})

test_that(".picotel_attributes_to_otlp: all-NULL values returns empty list", {
  expect_equal(.picotel_attributes_to_otlp(list(x = NULL)), list())
})

# ---------------------------------------------------------------------------
# .picotel_span_to_list — minimal span (test_minimal_span_to_dict)
# ---------------------------------------------------------------------------

test_that(".picotel_span_to_list: minimal span has required fields", {
  trace_id   <- new_trace_id()
  span_id    <- new_span_id()
  start_time <- 1000000000
  end_time   <- start_time + 1000000

  span   <- .make_span(
    trace_id      = trace_id,
    span_id       = span_id,
    name          = "test_operation",
    kind          = SpanKind$INTERNAL,
    start_time_ns = start_time,
    end_time_ns   = end_time
  )
  result <- .picotel_span_to_list(span)

  expect_equal(result$traceId,           trace_id)
  expect_equal(result$spanId,            span_id)
  expect_equal(result$name,              "test_operation")
  expect_equal(result$kind,              1L)   # SpanKind$INTERNAL
  expect_equal(result$startTimeUnixNano, .picotel_ns_str(start_time))
  expect_equal(result$endTimeUnixNano,   .picotel_ns_str(end_time))

  # Optional fields omitted when empty / default
  expect_null(result$parentSpanId)
  expect_null(result$attributes)
  expect_null(result$events)
  expect_null(result$links)
  expect_null(result$status)
})

# ---------------------------------------------------------------------------
# .picotel_span_to_list — parent and attributes (test_span_with_parent_and_attributes)
# ---------------------------------------------------------------------------

test_that(".picotel_span_to_list: parent span ID and attributes included", {
  trace_id   <- new_trace_id()
  span_id    <- new_span_id()
  parent_id  <- new_span_id()

  span <- .make_span(
    trace_id       = trace_id,
    span_id        = span_id,
    parent_span_id = parent_id,
    name           = "child_operation",
    kind           = SpanKind$CLIENT,
    attributes     = list(
      "http.method"      = "GET",
      "http.status_code" = 200L,
      "user.id"          = 12345L
    )
  )
  result <- .picotel_span_to_list(span)

  expect_equal(result$parentSpanId, parent_id)
  expect_equal(result$kind, 3L)  # SpanKind$CLIENT
  expect_equal(length(result$attributes), 3L)

  by_key <- setNames(
    lapply(result$attributes, `[[`, "value"),
    sapply(result$attributes, `[[`, "key")
  )
  expect_equal(by_key[["http.method"]],      list(stringValue = "GET"))
  expect_equal(by_key[["http.status_code"]], list(intValue    = "200"))
  expect_equal(by_key[["user.id"]],          list(intValue    = "12345"))
})

# ---------------------------------------------------------------------------
# .picotel_span_to_list — events (test_span_with_events)
# ---------------------------------------------------------------------------

test_that(".picotel_span_to_list: events with attributes", {
  start_time  <- 2000000000
  event_time1 <- start_time + 500000
  event_time2 <- start_time + 1000000

  span <- .make_span(
    start_time_ns = start_time,
    end_time_ns   = start_time + 2000000,
    events = list(
      list(name = "request_started",   timestamp_ns = event_time1, attributes = list(url = "https://example.com")),
      list(name = "request_completed", timestamp_ns = event_time2, attributes = list(response_size = 1024L))
    )
  )
  result <- .picotel_span_to_list(span)

  expect_equal(length(result$events), 2L)

  ev1 <- result$events[[1L]]
  expect_equal(ev1$name,         "request_started")
  expect_equal(ev1$timeUnixNano, .picotel_ns_str(event_time1))
  expect_equal(length(ev1$attributes), 1L)
  expect_equal(ev1$attributes[[1L]], list(key = "url", value = list(stringValue = "https://example.com")))

  ev2 <- result$events[[2L]]
  expect_equal(ev2$name,         "request_completed")
  expect_equal(ev2$timeUnixNano, .picotel_ns_str(event_time2))
  expect_equal(ev2$attributes[[1L]], list(key = "response_size", value = list(intValue = "1024")))
})

test_that(".picotel_span_to_list: event with no attributes omits attributes key", {
  span <- .make_span(
    events = list(list(name = "tick", timestamp_ns = 1000L, attributes = list()))
  )
  result <- .picotel_span_to_list(span)
  ev     <- result$events[[1L]]
  expect_null(ev$attributes)
})

# ---------------------------------------------------------------------------
# .picotel_span_to_list — links (test_span_with_links)
# ---------------------------------------------------------------------------

test_that(".picotel_span_to_list: links with attributes", {
  linked_trace <- new_trace_id()
  linked_span  <- new_span_id()

  span <- .make_span(
    links = list(
      list(trace_id = linked_trace, span_id = linked_span, attributes = list("link.type" = "parent_trace"))
    )
  )
  result <- .picotel_span_to_list(span)

  expect_equal(length(result$links), 1L)
  lk <- result$links[[1L]]
  expect_equal(lk$traceId, linked_trace)
  expect_equal(lk$spanId,  linked_span)
  expect_equal(lk$attributes[[1L]], list(key = "link.type", value = list(stringValue = "parent_trace")))
})

test_that(".picotel_span_to_list: link with no attributes omits attributes key", {
  span <- .make_span(
    links = list(list(trace_id = new_trace_id(), span_id = new_span_id(), attributes = list()))
  )
  result <- .picotel_span_to_list(span)
  lk     <- result$links[[1L]]
  expect_null(lk$attributes)
})

# ---------------------------------------------------------------------------
# .picotel_span_to_list — status codes (test_span_status_codes)
# ---------------------------------------------------------------------------

test_that(".picotel_span_to_list: ERROR status included with code 2", {
  span   <- .make_span(status = SpanStatus$ERROR)
  result <- .picotel_span_to_list(span)
  expect_equal(result$status, list(code = 2L))
})

test_that(".picotel_span_to_list: OK status included with code 1", {
  span   <- .make_span(status = SpanStatus$OK)
  result <- .picotel_span_to_list(span)
  expect_equal(result$status, list(code = 1L))
})

test_that(".picotel_span_to_list: UNSET status (0L) is omitted", {
  span   <- .make_span(status = SpanStatus$UNSET)
  result <- .picotel_span_to_list(span)
  expect_null(result$status)
})

test_that(".picotel_span_to_list: NULL status is omitted", {
  span   <- .make_span(status = NULL)
  result <- .picotel_span_to_list(span)
  expect_null(result$status)
})

test_that(".picotel_span_to_list: timestamps encoded as decimal strings without sci notation", {
  start <- now_ns()
  end   <- start + 1000000
  span  <- .make_span(start_time_ns = start, end_time_ns = end)
  result <- .picotel_span_to_list(span)
  expect_false(grepl("[eE]", result$startTimeUnixNano))
  expect_false(grepl("[eE]", result$endTimeUnixNano))
  expect_true(grepl("^[0-9]+$", result$startTimeUnixNano))
})

# ---------------------------------------------------------------------------
# .picotel_log_to_list — minimal log
# ---------------------------------------------------------------------------

test_that(".picotel_log_to_list: minimal log has required fields", {
  log    <- .make_log(body = "Hello world", timestamp_ns = 1000000000, observed_timestamp_ns = 2000000000)
  result <- .picotel_log_to_list(log)

  expect_equal(result$timeUnixNano,         "1000000000")
  expect_equal(result$observedTimeUnixNano, "2000000000")
  expect_equal(result$severityNumber,       9L)   # Severity$INFO
  expect_equal(result$body,                 list(stringValue = "Hello world"))

  # Optional fields absent in minimal log
  expect_null(result$severityText)
  expect_null(result$attributes)
  expect_null(result$traceId)
  expect_null(result$spanId)
  expect_null(result$flags)
})

test_that(".picotel_log_to_list: timestamp 0 is replaced by now_ns()", {
  log    <- .make_log(timestamp_ns = 0, observed_timestamp_ns = 0)
  before <- now_ns()
  result <- .picotel_log_to_list(log)
  after  <- now_ns()

  ts  <- as.numeric(result$timeUnixNano)
  obs <- as.numeric(result$observedTimeUnixNano)
  expect_gte(ts,  before)
  expect_lte(ts,  after + 1e6)  # allow slight clock skew
  expect_gte(obs, before)
  expect_lte(obs, after + 1e6)
})

test_that(".picotel_log_to_list: explicit timestamps are preserved", {
  log    <- .make_log(timestamp_ns = 1111111111, observed_timestamp_ns = 2222222222)
  result <- .picotel_log_to_list(log)
  expect_equal(result$timeUnixNano,         "1111111111")
  expect_equal(result$observedTimeUnixNano, "2222222222")
})

test_that(".picotel_log_to_list: trace correlation fields included when set", {
  log <- .make_log(
    timestamp_ns = 9999999999,
    trace_id     = "abcdef1234567890abcdef1234567890",
    span_id      = "1234567890abcdef",
    trace_flags  = 1L
  )
  result <- .picotel_log_to_list(log)
  expect_equal(result$traceId, "abcdef1234567890abcdef1234567890")
  expect_equal(result$spanId,  "1234567890abcdef")
  expect_equal(result$flags,   1L)
})

test_that(".picotel_log_to_list: traceId/spanId omitted when empty", {
  log    <- .make_log()  # trace_id = "", span_id = "" by default
  result <- .picotel_log_to_list(log)
  expect_null(result$traceId)
  expect_null(result$spanId)
})

test_that(".picotel_log_to_list: flags omitted when zero", {
  log    <- .make_log(trace_flags = 0L)
  result <- .picotel_log_to_list(log)
  expect_null(result$flags)
})

test_that(".picotel_log_to_list: severityText included when non-empty", {
  log    <- .make_log(severity_number = Severity$ERROR, severity_text = "ERROR")
  result <- .picotel_log_to_list(log)
  expect_equal(result$severityNumber, 17L)
  expect_equal(result$severityText,   "ERROR")
})

test_that(".picotel_log_to_list: severityText omitted when empty", {
  log    <- .make_log(severity_text = "")
  result <- .picotel_log_to_list(log)
  expect_null(result$severityText)
})

test_that(".picotel_log_to_list: attributes included when present", {
  log <- .make_log(
    timestamp_ns = 7777777777,
    attributes   = list("user.id" = "user123", "http.status_code" = 500L, success = FALSE)
  )
  result <- .picotel_log_to_list(log)
  by_key <- setNames(
    lapply(result$attributes, `[[`, "value"),
    sapply(result$attributes, `[[`, "key")
  )
  expect_equal(by_key[["user.id"]],          list(stringValue = "user123"))
  expect_equal(by_key[["http.status_code"]], list(intValue    = "500"))
  expect_equal(by_key[["success"]],          list(boolValue   = FALSE))
})

test_that(".picotel_log_to_list: body can be named list (kvlistValue)", {
  log    <- .make_log(body = list(error = "Something went wrong", code = 500L), timestamp_ns = 2000)
  result <- .picotel_log_to_list(log)
  by_key <- setNames(
    lapply(result$body$kvlistValue$values, `[[`, "value"),
    sapply(result$body$kvlistValue$values, `[[`, "key")
  )
  expect_equal(by_key[["error"]], list(stringValue = "Something went wrong"))
  expect_equal(by_key[["code"]],  list(intValue    = "500"))
})

test_that(".picotel_log_to_list: body can be unnamed list (arrayValue)", {
  log    <- .make_log(body = list("item1", "item2", 3L), timestamp_ns = 3000)
  result <- .picotel_log_to_list(log)
  arr    <- result$body$arrayValue$values
  expect_equal(arr[[1L]], list(stringValue = "item1"))
  expect_equal(arr[[2L]], list(stringValue = "item2"))
  expect_equal(arr[[3L]], list(intValue    = "3"))
})

# ---------------------------------------------------------------------------
# .picotel_validate_span
# ---------------------------------------------------------------------------

test_that(".picotel_validate_span: valid span returns invisibly", {
  span <- .make_span(
    trace_id      = new_trace_id(),
    start_time_ns = 1000000000,
    end_time_ns   = 2000000000
  )
  expect_silent(.picotel_validate_span(span))
})

test_that(".picotel_validate_span: empty trace_id raises picotel_config_error", {
  span <- .make_span(trace_id = "", start_time_ns = 1000000000, end_time_ns = 2000000000)
  expect_error(.picotel_validate_span(span), class = "picotel_config_error")
})

test_that(".picotel_validate_span: NULL trace_id raises picotel_config_error", {
  span <- .make_span(start_time_ns = 1000000000, end_time_ns = 2000000000)
  span$trace_id <- NULL
  expect_error(.picotel_validate_span(span), class = "picotel_config_error")
})

test_that(".picotel_validate_span: NULL start_time_ns raises picotel_config_error", {
  span <- .make_span(trace_id = new_trace_id(), end_time_ns = 2000000000)
  span$start_time_ns <- NULL
  expect_error(.picotel_validate_span(span), class = "picotel_config_error")
})

test_that(".picotel_validate_span: NULL end_time_ns raises picotel_config_error", {
  span <- .make_span(trace_id = new_trace_id(), start_time_ns = 1000000000)
  span$end_time_ns <- NULL
  expect_error(.picotel_validate_span(span), class = "picotel_config_error")
})

test_that(".picotel_validate_span: error messages match Python wording", {
  # Python: "Span invalid: trace_id is empty"
  err1 <- tryCatch(
    .picotel_validate_span(.make_span(trace_id = "", start_time_ns = 1, end_time_ns = 2)),
    picotel_config_error = function(e) e
  )
  expect_true(grepl("trace_id is empty", conditionMessage(err1)))

  # Python: "Span invalid: start_time_ns is not set"
  span2 <- .make_span(trace_id = "abc", end_time_ns = 2)
  span2$start_time_ns <- NULL
  err2 <- tryCatch(
    .picotel_validate_span(span2),
    picotel_config_error = function(e) e
  )
  expect_true(grepl("start_time_ns is not set", conditionMessage(err2)))

  # Python: "Span invalid: end_time_ns is not set"
  span3 <- .make_span(trace_id = "abc", start_time_ns = 1)
  span3$end_time_ns <- NULL
  err3 <- tryCatch(
    .picotel_validate_span(span3),
    picotel_config_error = function(e) e
  )
  expect_true(grepl("end_time_ns is not set", conditionMessage(err3)))
})

# ---------------------------------------------------------------------------
# .picotel_to_json — basic types
# ---------------------------------------------------------------------------

test_that(".picotel_to_json: NULL -> 'null'", {
  expect_equal(.picotel_to_json(NULL), "null")
})

test_that(".picotel_to_json: logical -> 'true' / 'false'", {
  expect_equal(.picotel_to_json(TRUE),  "true")
  expect_equal(.picotel_to_json(FALSE), "false")
})

test_that(".picotel_to_json: character -> quoted string", {
  expect_equal(.picotel_to_json("hello"), '"hello"')
  expect_equal(.picotel_to_json(""),      '""')
})

test_that(".picotel_to_json: integer -> unquoted number without decimal", {
  expect_equal(.picotel_to_json(42L),  "42")
  expect_equal(.picotel_to_json(-1L),  "-1")
  expect_equal(.picotel_to_json(0L),   "0")
})

test_that(".picotel_to_json: whole-number double -> unquoted integer string", {
  expect_equal(.picotel_to_json(42.0),  "42")
  expect_equal(.picotel_to_json(0.0),   "0")
})

test_that(".picotel_to_json: non-whole double -> full-precision number string", {
  # Must not contain 'e' / 'E' for small values
  json <- .picotel_to_json(3.14)
  expect_false(grepl("[eE]", json))
  expect_equal(as.numeric(json), 3.14)
})

# ---------------------------------------------------------------------------
# .picotel_to_json — string escaping
# ---------------------------------------------------------------------------

test_that(".picotel_to_json: escapes backslash and double-quote", {
  expect_equal(.picotel_to_json('back\\slash'), '"back\\\\slash"')
  expect_equal(.picotel_to_json('say "hello"'), '"say \\"hello\\""')
})

test_that(".picotel_to_json: escapes \\n, \\r, \\t, \\b, \\f", {
  expect_equal(.picotel_to_json("new\nline"),   '"new\\nline"')
  expect_equal(.picotel_to_json("car\rriage"),  '"car\\rriage"')
  expect_equal(.picotel_to_json("ta\tb"),       '"ta\\tb"')
  expect_equal(.picotel_to_json("back\bspace"), '"back\\bspace"')
  expect_equal(.picotel_to_json("form\ffeed"),  '"form\\ffeed"')
})

test_that(".picotel_to_json: control chars U+0000-U+001F escaped as \\uXXXX", {
  # U+0001 (SOH) and U+001F (US) — not covered by named escapes
  json_soh <- .picotel_to_json("\x01")
  expect_true(grepl("\\\\u", json_soh))

  json_us <- .picotel_to_json("\x1f")
  expect_true(grepl("\\\\u", json_us))
})

# ---------------------------------------------------------------------------
# .picotel_to_json — containers
# ---------------------------------------------------------------------------

test_that(".picotel_to_json: empty unnamed list -> '[]'", {
  expect_equal(.picotel_to_json(list()), "[]")
})

test_that(".picotel_to_json: unnamed list -> JSON array", {
  expect_equal(.picotel_to_json(list(1L, "two", TRUE)), '[1,"two",true]')
})

test_that(".picotel_to_json: named list -> JSON object", {
  result <- .picotel_to_json(list(a = 1L, b = "two"))
  expect_equal(result, '{"a":1,"b":"two"}')
})

test_that(".picotel_to_json: nested lists produce valid nested JSON", {
  # list(arrayValue = list(values = list(list(intValue = "42"))))
  x <- list(arrayValue = list(values = list(list(intValue = "42"))))
  json <- .picotel_to_json(x)
  expect_equal(json, '{"arrayValue":{"values":[{"intValue":"42"}]}}')
})

# ---------------------------------------------------------------------------
# .picotel_to_json — round-trip through .picotel_to_otlp_value
# ---------------------------------------------------------------------------

test_that(".picotel_to_json round-trips OTLP scalar values correctly", {
  expect_equal(.picotel_to_json(.picotel_to_otlp_value("hello")),  '{"stringValue":"hello"}')
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(TRUE)),     '{"boolValue":true}')
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(42L)),      '{"intValue":"42"}')
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(3.14)),     '{"doubleValue":3.1400000000000001}')
  # NaN/Inf are proto3 strings -> JSON string in the value
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(NaN)),      '{"doubleValue":"NaN"}')
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(Inf)),      '{"doubleValue":"Infinity"}')
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(-Inf)),     '{"doubleValue":"-Infinity"}')
  # NULL -> empty OTLP map -> JSON "{}" (empty object, mirrors Python None -> {})
  expect_equal(.picotel_to_json(.picotel_to_otlp_value(NULL)),     '{}')
})

test_that(".picotel_to_json round-trips a minimal span to JSON", {
  trace_id <- "aabbccddeeff00112233445566778899"
  span_id  <- "0011223344556677"
  span     <- .make_span(
    trace_id      = trace_id,
    span_id       = span_id,
    name          = "op",
    kind          = SpanKind$INTERNAL,
    start_time_ns = 1000000000,
    end_time_ns   = 2000000000
  )
  result <- .picotel_span_to_list(span)
  json   <- .picotel_to_json(result)

  # Must be valid JSON-looking output (basic structure checks)
  expect_true(grepl('"traceId"', json))
  expect_true(grepl(trace_id,    json))
  expect_true(grepl('"spanId"',  json))
  expect_true(grepl(span_id,     json))
  expect_true(grepl('"name"',    json))
  expect_true(grepl('"op"',      json))
  expect_true(grepl('"1000000000"', json))
})
