# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for environment variable configuration."""

import os
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


def test_get_endpoint_traces_specific():
    """Test that trace-specific endpoint takes precedence."""
    # Clear cache before test
    picotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318",
        },
    ):
        assert picotel._get_endpoint("traces") == "http://traces:4318"


def test_get_endpoint_logs_specific():
    """Test that logs-specific endpoint takes precedence."""
    # Clear cache before test
    picotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs:4318",
        },
    ):
        assert picotel._get_endpoint("logs") == "http://logs:4318"


def test_get_endpoint_fallback_to_general():
    """Test fallback to general endpoint when specific not set.

    Per OTEL spec, general endpoint has signal path appended.
    """
    # Clear cache before test
    picotel._get_endpoint.cache_clear()

    with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318"}):
        # General endpoint gets /v1/{signal} appended per OTEL spec
        assert picotel._get_endpoint("traces") == "http://general:4318/v1/traces"
        picotel._get_endpoint.cache_clear()
        assert picotel._get_endpoint("logs") == "http://general:4318/v1/logs"


def test_get_endpoint_none_when_not_set():
    """Test that get_endpoint returns None when no env vars set."""
    # Clear cache before test
    picotel._get_endpoint.cache_clear()

    with patch.dict(os.environ, {}, clear=True):
        assert picotel._get_endpoint("traces") is None
        assert picotel._get_endpoint("logs") is None


def test_parse_headers():
    """Test parsing OTEL_EXPORTER_OTLP_HEADERS environment variable."""
    # Clear cache before test
    picotel._parse_headers.cache_clear()

    # Test valid headers
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "key1=value1,key2=value2,key3=value with spaces"
            )
        },
    ):
        headers = picotel._parse_headers()
        assert headers == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value with spaces",
        }

    # Clear cache between tests
    picotel._parse_headers.cache_clear()

    # Test empty headers
    with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_HEADERS": ""}):
        assert picotel._parse_headers() == {}

    # Clear cache between tests
    picotel._parse_headers.cache_clear()

    # Test not set
    with patch.dict(os.environ, {}, clear=True):
        assert picotel._parse_headers() == {}

    # Clear cache between tests
    picotel._parse_headers.cache_clear()

    # Test whitespace handling
    with patch.dict(
        os.environ, {"OTEL_EXPORTER_OTLP_HEADERS": " key1 = value1 , key2=value2 "}
    ):
        headers = picotel._parse_headers()
        assert headers == {"key1": "value1", "key2": "value2"}


def test_get_resource_from_env():
    """Test creating Resource from OTEL_SERVICE_NAME."""
    # Clear cache before test
    picotel._get_resource_from_env.cache_clear()

    # Test when set
    with patch.dict(os.environ, {"OTEL_SERVICE_NAME": "my-service"}):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"service.name": "my-service"}

    # Clear cache between tests
    picotel._get_resource_from_env.cache_clear()

    # Test when not set
    with patch.dict(os.environ, {}, clear=True):
        assert picotel._get_resource_from_env() is None


def test_picotel_endpoint_takes_precedence_over_otel():
    """Test that PICOTEL_EXPORTER_OTLP_ENDPOINT takes precedence over OTEL."""
    picotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel:4318",
            "PICOTEL_EXPORTER_OTLP_ENDPOINT": "http://picotel:4318",
        },
    ):
        assert picotel._get_endpoint("traces") == "http://picotel:4318/v1/traces"
        picotel._get_endpoint.cache_clear()
        assert picotel._get_endpoint("logs") == "http://picotel:4318/v1/logs"


def test_picotel_traces_endpoint_takes_precedence():
    """Test PICOTEL_EXPORTER_OTLP_TRACES_ENDPOINT wins over all OTEL variants."""
    picotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-general:4318",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://otel-traces:4318",
            "PICOTEL_EXPORTER_OTLP_ENDPOINT": "http://picotel-general:4318",
            "PICOTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://picotel-traces:4318",
        },
    ):
        assert picotel._get_endpoint("traces") == "http://picotel-traces:4318"


def test_picotel_logs_endpoint_takes_precedence():
    """Test PICOTEL_EXPORTER_OTLP_LOGS_ENDPOINT wins over all OTEL variants."""
    picotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-general:4318",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://otel-logs:4318",
            "PICOTEL_EXPORTER_OTLP_ENDPOINT": "http://picotel-general:4318",
            "PICOTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://picotel-logs:4318",
        },
    ):
        assert picotel._get_endpoint("logs") == "http://picotel-logs:4318"


def test_picotel_general_wins_over_otel_specific():
    """Test PICOTEL general endpoint wins over OTEL signal-specific.

    All PICOTEL vars take precedence over all OTEL vars.
    """
    picotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://otel-traces:4318",
            "PICOTEL_EXPORTER_OTLP_ENDPOINT": "http://picotel:4318",
        },
    ):
        assert picotel._get_endpoint("traces") == "http://picotel:4318/v1/traces"


def test_picotel_headers_takes_precedence():
    """Test PICOTEL_EXPORTER_OTLP_HEADERS wins over OTEL."""
    picotel._parse_headers.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_HEADERS": "X-Otel=otel-value",
            "PICOTEL_EXPORTER_OTLP_HEADERS": "X-Picotel=picotel-value",
        },
    ):
        headers = picotel._parse_headers()
        assert headers == {"X-Picotel": "picotel-value"}
        assert "X-Otel" not in headers


def test_picotel_headers_fallback_to_otel():
    """Test fallback to OTEL headers when PICOTEL not set."""
    picotel._parse_headers.cache_clear()

    with patch.dict(
        os.environ,
        {"OTEL_EXPORTER_OTLP_HEADERS": "X-Otel=otel-value"},
    ):
        headers = picotel._parse_headers()
        assert headers == {"X-Otel": "otel-value"}


def test_picotel_service_name_takes_precedence():
    """Test PICOTEL_SERVICE_NAME wins over OTEL_SERVICE_NAME."""
    picotel._get_resource_from_env.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_SERVICE_NAME": "otel-service",
            "PICOTEL_SERVICE_NAME": "picotel-service",
        },
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"service.name": "picotel-service"}


def test_picotel_service_name_fallback_to_otel():
    """Test fallback to OTEL_SERVICE_NAME when PICOTEL not set."""
    picotel._get_resource_from_env.cache_clear()

    with patch.dict(os.environ, {"OTEL_SERVICE_NAME": "otel-service"}):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"service.name": "otel-service"}


def test_send_spans_with_env_endpoint(monkeypatch):
    """Test send_spans uses environment variable when endpoint is None."""
    # Clear caches before test
    picotel._get_endpoint.cache_clear()
    picotel._get_resource_from_env.cache_clear()
    picotel._is_disabled.cache_clear()

    # Mock urlopen to capture the request
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment
    with patch.dict(
        os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://env-test:4318"}
    ):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        # Send with None endpoint - should use env var
        result = send_spans(None, resource, [span])

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://env-test:4318/v1/traces"


def test_send_logs_with_env_endpoint(monkeypatch):
    """Test send_logs uses environment variable when endpoint is None.

    Per OTEL spec, signal-specific endpoints are used as-is (include full path).
    """
    # Clear caches before test
    picotel._get_endpoint.cache_clear()
    picotel._get_resource_from_env.cache_clear()
    picotel._is_disabled.cache_clear()

    # Mock urlopen to capture the request
    import urllib.request  # noqa: PLC0415

    from picotel import LogRecord  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment - signal-specific endpoint includes full path per OTEL spec
    with patch.dict(
        os.environ, {"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs-env:4318/v1/logs"}
    ):
        resource = Resource({"service.name": "test"})
        log = LogRecord(body="test log")

        # Send with None endpoint - should use env var as-is
        result = send_logs(None, resource, [log])

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://logs-env:4318/v1/logs"


def test_send_spans_with_headers_from_env(monkeypatch):
    """Test that headers from environment are included in requests."""
    # Clear caches before test
    picotel._get_endpoint.cache_clear()
    picotel._parse_headers.cache_clear()
    picotel._is_disabled.cache_clear()

    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment with headers
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://test:4318",
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "Authorization=Bearer token123,X-Custom=value"
            ),
        },
    ):
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
        # Check headers were added
        assert captured_request.headers["Authorization"] == "Bearer token123"
        # urllib lowercases header names
        assert captured_request.headers["X-custom"] == "value"
        assert captured_request.headers["Content-type"] == "application/json"


def test_send_without_endpoint_raises_error():
    """Test that send functions raise PicotelConfigError when no endpoint."""
    # Clear caches before test
    picotel._get_endpoint.cache_clear()
    picotel._is_disabled.cache_clear()

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

        # Test send_spans raises error
        with pytest.raises(PicotelConfigError) as exc_info:
            send_spans(None, resource, [span])
        assert "No OTLP endpoint configured" in str(exc_info.value)
        assert "PICOTEL_SDK_DISABLED=true" in str(exc_info.value)

        # Test send_logs raises error
        with pytest.raises(PicotelConfigError) as exc_info:
            send_logs(None, resource, [log])
        assert "No OTLP endpoint configured" in str(exc_info.value)
        assert "PICOTEL_SDK_DISABLED=true" in str(exc_info.value)


def test_span_context_manager_with_env(monkeypatch):
    """Test Span context manager uses environment variables."""
    # Clear caches before test
    picotel._get_endpoint.cache_clear()
    picotel._get_resource_from_env.cache_clear()

    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Test with both endpoint and service name from env
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://env:4318",
            "OTEL_SERVICE_NAME": "env-service",
        },
    ):
        # Span with no explicit endpoint or resource - timestamps optional
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
        ):
            pass

        # Should have sent the span
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://env:4318/v1/traces"


def test_otlp_handler_with_env(monkeypatch):
    """Test OTLPHandler uses environment variables.

    Uses general endpoint which gets /v1/logs appended per OTEL spec.
    """
    # Clear caches before test
    picotel._get_endpoint.cache_clear()
    picotel._get_resource_from_env.cache_clear()

    import logging  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import OTLPHandler  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Use general endpoint - path gets appended per OTEL spec
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://logs:4318",
            "OTEL_SERVICE_NAME": "logging-service",
        },
    ):
        # Create handler without explicit endpoint or resource
        handler = OTLPHandler()
        logger = logging.getLogger("test_env")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        # Log a message
        logger.info("Test message")

        # Should have sent the log
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://logs:4318/v1/logs"


def test_explicit_endpoint_still_works(monkeypatch):
    """Test that providing explicit endpoint works even without env vars."""
    picotel._is_disabled.cache_clear()
    picotel._get_endpoint.cache_clear()

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

    with patch.dict(os.environ, {}, clear=True):  # No env vars set
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test log")

        # Should work with explicit endpoint
        assert send_spans("http://explicit:4318", resource, [span]) is True
        assert send_logs("http://explicit:4318", resource, [log]) is True

        # Check URLs were properly constructed
        assert captured_requests[0] == "http://explicit:4318/v1/traces"
        assert captured_requests[1] == "http://explicit:4318/v1/logs"
