"""Tests for Resource, InstrumentationScope, and LogRecord dataclasses."""

from miniotel import InstrumentationScope, LogRecord, Resource, Severity


def test_resource_with_attributes():
    """Resource stores service metadata attributes."""
    r = Resource({"service.name": "test"})
    assert r.attributes["service.name"] == "test"


def test_resource_with_multiple_attributes():
    """Resource can hold multiple attributes."""
    r = Resource(
        {
            "service.name": "myapp",
            "service.version": "1.0.0",
            "deployment.environment": "production",
        }
    )
    assert r.attributes["service.name"] == "myapp"
    assert r.attributes["service.version"] == "1.0.0"
    assert r.attributes["deployment.environment"] == "production"


def test_resource_with_keyword_argument():
    """Resource can be instantiated with keyword argument."""
    r = Resource(attributes={"service.name": "kwarg-test"})
    assert r.attributes["service.name"] == "kwarg-test"


def test_instrumentation_scope_basic():
    """InstrumentationScope requires name, optional version and attributes."""
    s = InstrumentationScope("mylib", "1.0")
    assert s.name == "mylib"
    assert s.version == "1.0"


def test_instrumentation_scope_defaults():
    """InstrumentationScope has sensible defaults for version and attributes."""
    s = InstrumentationScope("lib-only")
    assert s.name == "lib-only"
    assert s.version == ""
    assert s.attributes == {}


def test_instrumentation_scope_with_attributes():
    """InstrumentationScope can store attributes."""
    s = InstrumentationScope("lib", "2.0", {"custom.key": "value"})
    assert s.name == "lib"
    assert s.version == "2.0"
    assert s.attributes == {"custom.key": "value"}


def test_instrumentation_scope_keyword_arguments():
    """InstrumentationScope can be instantiated with keyword arguments."""
    s = InstrumentationScope(name="kwlib", version="3.0", attributes={"debug": True})
    assert s.name == "kwlib"
    assert s.version == "3.0"
    assert s.attributes == {"debug": True}


def test_instrumentation_scope_default_attributes_isolation():
    """Each InstrumentationScope instance has its own attributes dict."""
    s1 = InstrumentationScope("lib1")
    s2 = InstrumentationScope("lib2")
    s1.attributes["key"] = "value"
    assert "key" not in s2.attributes


def test_logrecord_with_just_body():
    """LogRecord can be created with just body, has correct defaults."""
    log = LogRecord("hello")
    assert log.body == "hello"
    assert log.timestamp_ns == 0
    assert log.observed_timestamp_ns == 0
    assert log.trace_id == ""
    assert log.span_id == ""
    assert log.trace_flags == 0
    assert log.severity_number == Severity.INFO
    assert log.severity_text == ""
    assert log.attributes == {}


def test_logrecord_with_trace_correlation():
    """LogRecord stores trace_id and span_id for correlation."""
    log = LogRecord(
        body="correlated log",
        trace_id="abcd1234567890abcd1234567890abcd",
        span_id="1234567890abcdef",
    )
    assert log.body == "correlated log"
    assert log.trace_id == "abcd1234567890abcd1234567890abcd"
    assert log.span_id == "1234567890abcdef"


def test_logrecord_severity_defaults():
    """LogRecord defaults to INFO severity with empty severity_text."""
    log = LogRecord("test message")
    assert log.severity_number == 9  # Severity.INFO value
    assert log.severity_number == Severity.INFO
    assert log.severity_text == ""


def test_logrecord_with_custom_severity():
    """LogRecord can have custom severity number and text."""
    log = LogRecord(
        body="error occurred",
        severity_number=Severity.ERROR,
        severity_text="ERROR",
    )
    assert log.body == "error occurred"
    assert log.severity_number == Severity.ERROR
    assert log.severity_text == "ERROR"


def test_logrecord_with_attributes():
    """LogRecord can have custom attributes."""
    log = LogRecord(
        body="structured log",
        attributes={"user.id": 123, "request.method": "GET"},
    )
    assert log.body == "structured log"
    assert log.attributes["user.id"] == 123
    assert log.attributes["request.method"] == "GET"


def test_logrecord_body_can_be_any_type():
    """LogRecord body can be any type, not just string."""
    # Body as dict
    log1 = LogRecord({"message": "structured", "level": "info"})
    assert log1.body == {"message": "structured", "level": "info"}

    # Body as list
    log2 = LogRecord(["item1", "item2"])
    assert log2.body == ["item1", "item2"]

    # Body as number
    log3 = LogRecord(42)
    assert log3.body == 42


def test_logrecord_with_timestamps():
    """LogRecord can have custom timestamps."""
    log = LogRecord(
        body="timed log",
        timestamp_ns=1234567890000000000,
        observed_timestamp_ns=1234567891000000000,
    )
    assert log.timestamp_ns == 1234567890000000000
    assert log.observed_timestamp_ns == 1234567891000000000


def test_logrecord_default_attributes_isolation():
    """Each LogRecord instance has its own attributes dict."""
    log1 = LogRecord("log1")
    log2 = LogRecord("log2")
    log1.attributes["key"] = "value"
    assert "key" not in log2.attributes
