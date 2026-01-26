# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for environment variable configuration."""

import os
from unittest.mock import patch

import miniotel
from miniotel import (
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
    miniotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318",
        },
    ):
        assert miniotel._get_endpoint("traces") == "http://traces:4318"


def test_get_endpoint_logs_specific():
    """Test that logs-specific endpoint takes precedence."""
    # Clear cache before test
    miniotel._get_endpoint.cache_clear()

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs:4318",
        },
    ):
        assert miniotel._get_endpoint("logs") == "http://logs:4318"


def test_get_endpoint_fallback_to_general():
    """Test fallback to general endpoint when specific not set.

    Per OTEL spec, general endpoint has signal path appended.
    """
    # Clear cache before test
    miniotel._get_endpoint.cache_clear()

    with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318"}):
        # General endpoint gets /v1/{signal} appended per OTEL spec
        assert miniotel._get_endpoint("traces") == "http://general:4318/v1/traces"
        miniotel._get_endpoint.cache_clear()
        assert miniotel._get_endpoint("logs") == "http://general:4318/v1/logs"


def test_get_endpoint_none_when_not_set():
    """Test that get_endpoint returns None when no env vars set."""
    # Clear cache before test
    miniotel._get_endpoint.cache_clear()

    with patch.dict(os.environ, {}, clear=True):
        assert miniotel._get_endpoint("traces") is None
        assert miniotel._get_endpoint("logs") is None


def test_parse_headers():
    """Test parsing OTEL_EXPORTER_OTLP_HEADERS environment variable."""
    # Clear cache before test
    miniotel._parse_headers.cache_clear()

    # Test valid headers
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "key1=value1,key2=value2,key3=value with spaces"
            )
        },
    ):
        headers = miniotel._parse_headers()
        assert headers == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value with spaces",
        }

    # Clear cache between tests
    miniotel._parse_headers.cache_clear()

    # Test empty headers
    with patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_HEADERS": ""}):
        assert miniotel._parse_headers() == {}

    # Clear cache between tests
    miniotel._parse_headers.cache_clear()

    # Test not set
    with patch.dict(os.environ, {}, clear=True):
        assert miniotel._parse_headers() == {}

    # Clear cache between tests
    miniotel._parse_headers.cache_clear()

    # Test whitespace handling
    with patch.dict(
        os.environ, {"OTEL_EXPORTER_OTLP_HEADERS": " key1 = value1 , key2=value2 "}
    ):
        headers = miniotel._parse_headers()
        assert headers == {"key1": "value1", "key2": "value2"}


def test_get_resource_from_env():
    """Test creating Resource from OTEL_SERVICE_NAME."""
    # Clear cache before test
    miniotel._get_resource_from_env.cache_clear()

    # Test when set
    with patch.dict(os.environ, {"OTEL_SERVICE_NAME": "my-service"}):
        resource = miniotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"service.name": "my-service"}

    # Clear cache between tests
    miniotel._get_resource_from_env.cache_clear()

    # Test when not set
    with patch.dict(os.environ, {}, clear=True):
        assert miniotel._get_resource_from_env() is None


def test_send_spans_with_env_endpoint(monkeypatch):
    """Test send_spans uses environment variable when endpoint is None."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    # Mock urlopen to capture the request
    import urllib.request  # noqa: PLC0415

    from miniotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

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
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    # Mock urlopen to capture the request
    import urllib.request  # noqa: PLC0415

    from miniotel import LogRecord  # noqa: PLC0415

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
    miniotel._get_endpoint.cache_clear()
    miniotel._parse_headers.cache_clear()

    import urllib.request  # noqa: PLC0415

    from miniotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

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


def test_send_without_endpoint_returns_false():
    """Test that send functions return False when no endpoint is available."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()

    from miniotel import (  # noqa: PLC0415
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

        assert send_spans(None, resource, [span]) is False
        assert send_logs(None, resource, [log]) is False


def test_span_context_manager_with_env(monkeypatch):
    """Test Span context manager uses environment variables."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    import urllib.request  # noqa: PLC0415

    from miniotel import Span, new_span_id, new_trace_id  # noqa: PLC0415

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
        # Span with no explicit endpoint or resource
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=0,  # Will be set by context manager
            end_time_ns=0,  # Will be set by context manager
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
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    import logging  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from miniotel import OTLPHandler  # noqa: PLC0415

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
