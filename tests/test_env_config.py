# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for environment variable configuration."""

import os
from typing import Dict
from unittest.mock import patch

import pytest

import picotel
from picotel import (
    PicotelConfigError,
    Resource,
    send_logs,
    send_spans,
)


class MockResponse:
    """Mock response for urllib.request.urlopen."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def _prefixed(env: Dict[str, str], prefix: str) -> Dict[str, str]:
    """Remap standard OTEL_* env var names for the given prefix.

    When prefix is empty, returns env unchanged (standard OTEL mode).
    When prefix is "PICOTEL", OTEL_X becomes PICOTEL_X and
    TRACEPARENT becomes PICOTEL_TRACEPARENT — same logic as _env() in picotel.py.
    """
    if not prefix:
        return env
    result = {"PICOTEL_PREFIX": prefix}
    for key, value in env.items():
        if key.startswith("OTEL_"):
            result[prefix + "_" + key[5:]] = value
        else:
            result[prefix + "_" + key] = value
    return result


PREFIXES = pytest.mark.parametrize("prefix", ["", "PICOTEL"])


# ---------------------------------------------------------------------------
# Endpoint resolution
# ---------------------------------------------------------------------------


@PREFIXES
def test_get_endpoint_traces_specific(prefix):
    """Test that trace-specific endpoint takes precedence."""
    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        assert picotel._get_endpoint("traces") == "http://traces:4318"


@PREFIXES
def test_get_endpoint_logs_specific(prefix):
    """Test that logs-specific endpoint takes precedence."""
    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs:4318",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        assert picotel._get_endpoint("logs") == "http://logs:4318"


@PREFIXES
def test_get_endpoint_fallback_to_general(prefix):
    """Test fallback to general endpoint when specific not set.

    Per OTEL spec, general endpoint has signal path appended.
    """
    env = _prefixed({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318"}, prefix)
    with patch.dict(os.environ, env):
        assert picotel._get_endpoint("traces") == "http://general:4318/v1/traces"
        picotel._get_endpoint.cache_clear()
        assert picotel._get_endpoint("logs") == "http://general:4318/v1/logs"


def test_get_endpoint_none_when_not_set():
    """Test that get_endpoint returns None when no env vars set."""
    with patch.dict(os.environ, {}, clear=True):
        assert picotel._get_endpoint("traces") is None
        assert picotel._get_endpoint("logs") is None


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


@PREFIXES
def test_parse_headers(prefix):
    """Test parsing EXPORTER_OTLP_HEADERS environment variable."""
    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "key1=value1,key2=value2,key3=value with spaces"
            )
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        headers = picotel._parse_headers()
        assert headers == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value with spaces",
        }

    # Clear cache between sub-tests
    picotel._parse_headers.cache_clear()

    # Empty headers
    env = _prefixed({"OTEL_EXPORTER_OTLP_HEADERS": ""}, prefix)
    with patch.dict(os.environ, env):
        assert picotel._parse_headers() == {}

    picotel._parse_headers.cache_clear()

    # Not set
    with patch.dict(os.environ, _prefixed({}, prefix), clear=True):
        assert picotel._parse_headers() == {}

    picotel._parse_headers.cache_clear()

    # Whitespace handling
    env = _prefixed(
        {"OTEL_EXPORTER_OTLP_HEADERS": " key1 = value1 , key2=value2 "}, prefix
    )
    with patch.dict(os.environ, env):
        headers = picotel._parse_headers()
        assert headers == {"key1": "value1", "key2": "value2"}


# ---------------------------------------------------------------------------
# Service name / Resource from env
# ---------------------------------------------------------------------------


@PREFIXES
def test_get_resource_from_env(prefix):
    """Test creating Resource from SERVICE_NAME."""
    env = _prefixed({"OTEL_SERVICE_NAME": "my-service"}, prefix)
    with patch.dict(os.environ, env):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"service.name": "my-service"}

    picotel._get_resource_from_env.cache_clear()

    # Not set
    with patch.dict(os.environ, _prefixed({}, prefix), clear=True):
        assert picotel._get_resource_from_env() is None


# ---------------------------------------------------------------------------
# SDK disabled
# ---------------------------------------------------------------------------


@PREFIXES
def test_is_disabled(prefix):
    """Test _is_disabled honours the SDK_DISABLED env var."""
    env = _prefixed({"OTEL_SDK_DISABLED": "true"}, prefix)
    with patch.dict(os.environ, env, clear=True):
        assert picotel._is_disabled() is True


# ---------------------------------------------------------------------------
# Traceparent
# ---------------------------------------------------------------------------


@PREFIXES
def test_parse_traceparent(prefix):
    """Test _parse_traceparent reads the (possibly prefixed) TRACEPARENT var."""
    env = _prefixed(
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
        prefix,
    )
    with patch.dict(os.environ, env):
        result = picotel._parse_traceparent()
        assert result is not None
        assert result[0] == "0af7651916cd43dd8448eb211c80319c"
        assert result[1] == "b7ad6b7169203331"
        assert result[2] == 1


# ---------------------------------------------------------------------------
# OTEL_RESOURCE_ATTRIBUTES (W3C Baggage format: key=value,key=value)
# ---------------------------------------------------------------------------


@PREFIXES
def test_resource_attributes_basic(prefix):
    """Test RESOURCE_ATTRIBUTES with simple key=value pairs."""
    env = _prefixed(
        {
            "OTEL_SERVICE_NAME": "my-service",
            "OTEL_RESOURCE_ATTRIBUTES": "content.guid=abc-123,deployment.env=prod",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {
            "service.name": "my-service",
            "content.guid": "abc-123",
            "deployment.env": "prod",
        }


@PREFIXES
def test_resource_attributes_without_service_name(prefix):
    """Test RESOURCE_ATTRIBUTES works without a service name."""
    env = _prefixed({"OTEL_RESOURCE_ATTRIBUTES": "content.guid=abc-123"}, prefix)
    with patch.dict(os.environ, env, clear=True):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"content.guid": "abc-123"}


@PREFIXES
def test_resource_attributes_service_name_wins_over_attr(prefix):
    """Test SERVICE_NAME overrides service.name in resource attrs."""
    env = _prefixed(
        {
            "OTEL_SERVICE_NAME": "explicit-name",
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=from-attrs,other=val",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes["service.name"] == "explicit-name"
        assert resource.attributes["other"] == "val"


def test_resource_attributes_percent_encoded_comma_in_value():
    """Test that percent-encoded comma (%2C) in value is decoded correctly."""
    # value "a,b" is encoded as "a%2Cb"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "tags=a%2Cb%2Cc"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"tags": "a,b,c"}


def test_resource_attributes_percent_encoded_equals_in_value():
    """Test that percent-encoded equals (%3D) in value is decoded correctly."""
    # value "x=1" is encoded as "x%3D1"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "expr=x%3D1"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"expr": "x=1"}


def test_resource_attributes_percent_encoded_key():
    """Test that percent-encoded characters in the key are decoded."""
    # key "my,key" is encoded as "my%2Ckey"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "my%2Ckey=value"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"my,key": "value"}


def test_resource_attributes_spaces_and_special_chars():
    """Test percent-encoded spaces and unicode in values."""
    # "hello world" -> "hello%20world", "café" -> "caf%C3%A9"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "msg=hello%20world,place=caf%C3%A9"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"msg": "hello world", "place": "café"}


def test_resource_attributes_all_values_are_strings():
    """Test that all attribute values are strings per the W3C Baggage spec."""
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "count=42,enabled=true,ratio=3.14"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {
            "count": "42",
            "enabled": "true",
            "ratio": "3.14",
        }
        for v in resource.attributes.values():
            assert isinstance(v, str)


# ---------------------------------------------------------------------------
# Integration tests: send_spans / send_logs with env vars
# ---------------------------------------------------------------------------


@PREFIXES
def test_send_spans_with_env_endpoint(prefix, monkeypatch):
    """Test send_spans uses environment variable when endpoint is None."""
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://env-test:4318"}, prefix)
    with patch.dict(os.environ, env):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        result = send_spans(None, resource, [span])

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://env-test:4318/v1/traces"


@PREFIXES
def test_send_logs_with_env_endpoint(prefix, monkeypatch):
    """Test send_logs uses environment variable when endpoint is None.

    Per OTEL spec, signal-specific endpoints are used as-is (include full path).
    """
    import urllib.request  # noqa: PLC0415

    from picotel import LogRecord  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs-env:4318/v1/logs"}, prefix
    )
    with patch.dict(os.environ, env):
        resource = Resource({"service.name": "test"})
        log = LogRecord(body="test log")

        result = send_logs(None, resource, [log])

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://logs-env:4318/v1/logs"


@PREFIXES
def test_send_spans_with_headers_from_env(prefix, monkeypatch):
    """Test that headers from environment are included in requests."""
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://test:4318",
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "Authorization=Bearer token123,X-Custom=value"
            ),
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        result = send_spans(None, resource, [span])

        assert result is True
        assert captured_request is not None
        assert captured_request.headers["Authorization"] == "Bearer token123"
        assert captured_request.headers["X-custom"] == "value"
        assert captured_request.headers["Content-type"] == "application/json"


def test_send_without_endpoint_raises_config_error():
    """Test that send functions raise PicotelConfigError when no endpoint."""
    from picotel import (  # noqa: PLC0415
        LogRecord,
        Span,
        new_span_id,
        new_trace_id,
        now_ns,
    )

    with patch.dict(os.environ, {}, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test")

        with pytest.raises(PicotelConfigError):
            send_spans(None, resource, [span])
        with pytest.raises(PicotelConfigError):
            send_logs(None, resource, [log])


@PREFIXES
def test_send_returns_false_when_disabled(prefix):
    """Test that send functions return False when picotel is disabled."""
    from picotel import (  # noqa: PLC0415
        LogRecord,
        Span,
        new_span_id,
        new_trace_id,
        now_ns,
    )

    env = _prefixed({"OTEL_SDK_DISABLED": "true"}, prefix)
    with patch.dict(os.environ, env, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test")

        assert send_spans(None, resource, [span]) is False
        assert send_logs(None, resource, [log]) is False


@PREFIXES
def test_span_context_manager_with_env(prefix, monkeypatch):
    """Test Span context manager uses environment variables."""
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://env:4318",
            "OTEL_SERVICE_NAME": "env-service",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
        ):
            pass

        assert captured_request is not None
        assert captured_request.get_full_url() == "http://env:4318/v1/traces"


@PREFIXES
def test_otlp_handler_with_env(prefix, monkeypatch):
    """Test OTLPHandler uses environment variables.

    Uses general endpoint which gets /v1/logs appended per OTEL spec.
    """
    import logging  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import OTLPHandler  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://logs:4318",
            "OTEL_SERVICE_NAME": "logging-service",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        handler = OTLPHandler()
        logger = logging.getLogger(f"test_env_{prefix or 'otel'}")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        logger.info("Test message")

        assert captured_request is not None
        assert captured_request.get_full_url() == "http://logs:4318/v1/logs"


def test_explicit_endpoint_still_works(monkeypatch):
    """Test that providing explicit endpoint works even without env vars."""
    import urllib.request  # noqa: PLC0415

    from picotel import (  # noqa: PLC0415
        LogRecord,
        Span,
        new_span_id,
        new_trace_id,
        now_ns,
    )

    captured_requests = []

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        captured_requests.append(request.get_full_url())
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(os.environ, {}, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test log")

        assert send_spans("http://explicit:4318", resource, [span]) is True
        assert send_logs("http://explicit:4318", resource, [log]) is True

        assert captured_requests[0] == "http://explicit:4318/v1/traces"
        assert captured_requests[1] == "http://explicit:4318/v1/logs"
