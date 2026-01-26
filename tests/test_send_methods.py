# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for Span.send() and LogRecord.send() methods with optional parameters."""

import os
from unittest.mock import patch

import miniotel
from miniotel import LogRecord, Resource, Span, new_span_id, new_trace_id, now_ns


class MockResponse:
    """Mock response for urllib.request.urlopen."""

    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def test_span_send_with_env_vars(monkeypatch):
    """Test Span.send() uses environment variables when parameters are None."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    import urllib.request  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318",
            "OTEL_SERVICE_NAME": "test-service",
        },
    ):
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        # Call send() without parameters - should use env vars
        result = span.send()

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://traces:4318/v1/traces"


def test_span_send_with_explicit_params(monkeypatch):
    """Test Span.send() uses explicit parameters over env vars."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    import urllib.request  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment (will be overridden)
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://env:4318",
            "OTEL_SERVICE_NAME": "env-service",
        },
    ):
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        # Call send() with explicit parameters - should use these, not env vars
        resource = Resource({"service.name": "explicit-service"})
        result = span.send(endpoint="http://explicit:4318", resource=resource)

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://explicit:4318/v1/traces"


def test_span_send_fails_without_config():
    """Test Span.send() returns False when no config is available."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    # Clear environment
    with patch.dict(os.environ, {}, clear=True):
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        # Call send() without parameters and no env vars
        result = span.send()

        assert result is False


def test_log_send_with_env_vars(monkeypatch):
    """Test LogRecord.send() uses environment variables when parameters are None."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    import urllib.request  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs:4318",
            "OTEL_SERVICE_NAME": "test-service",
        },
    ):
        log = LogRecord(body="test log message")

        # Call send() without parameters - should use env vars
        result = log.send()

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://logs:4318/v1/logs"


def test_log_send_with_explicit_params(monkeypatch):
    """Test LogRecord.send() uses explicit parameters over env vars."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    import urllib.request  # noqa: PLC0415

    captured_request = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_request
        captured_request = request
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Set up environment (will be overridden)
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://env:4318",
            "OTEL_SERVICE_NAME": "env-service",
        },
    ):
        log = LogRecord(body="test log message")

        # Call send() with explicit parameters - should use these, not env vars
        resource = Resource({"service.name": "explicit-service"})
        result = log.send(endpoint="http://explicit:4318", resource=resource)

        assert result is True
        assert captured_request is not None
        assert captured_request.get_full_url() == "http://explicit:4318/v1/logs"


def test_log_send_fails_without_config():
    """Test LogRecord.send() returns False when no config is available."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()

    # Clear environment
    with patch.dict(os.environ, {}, clear=True):
        log = LogRecord(body="test log message")

        # Call send() without parameters and no env vars
        result = log.send()

        assert result is False
