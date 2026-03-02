# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for SDK_DISABLED functionality.

This module tests the behavior of picotel when the OTEL_SDK_DISABLED
environment variable is set to disable telemetry. When disabled, picotel
should silently drop all telemetry without errors or warnings.
"""

import os
from unittest.mock import patch

import picotel
from picotel import (
    TRACEPARENT,
    LogRecord,
    Resource,
    Span,
    new_span_id,
    new_trace_id,
    now_ns,
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


def test_is_disabled_true():
    """Test _is_disabled returns True when OTEL_SDK_DISABLED is set."""
    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}):
        assert picotel._is_disabled() is True

    picotel._is_disabled.cache_clear()

    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "TRUE"}):
        assert picotel._is_disabled() is True

    picotel._is_disabled.cache_clear()

    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "1"}):
        assert picotel._is_disabled() is True


def test_is_disabled_false():
    """Test _is_disabled returns False when not set or set to other values."""
    with patch.dict(os.environ, {}, clear=True):
        assert picotel._is_disabled() is False

    picotel._is_disabled.cache_clear()

    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "false"}):
        assert picotel._is_disabled() is False

    picotel._is_disabled.cache_clear()

    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "0"}):
        assert picotel._is_disabled() is False


def test_send_spans_disabled_no_send(monkeypatch):
    """Test send_spans returns False immediately when disabled."""
    import urllib.request  # noqa: PLC0415

    request_made = False

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal request_made
        request_made = True
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(
        os.environ,
        {
            "OTEL_SDK_DISABLED": "true",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
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

        assert result is False
        assert request_made is False  # No HTTP request should be made


def test_send_logs_disabled_no_send(monkeypatch):
    """Test send_logs returns False immediately when disabled."""
    import urllib.request  # noqa: PLC0415

    request_made = False

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal request_made
        request_made = True
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(
        os.environ,
        {
            "OTEL_SDK_DISABLED": "true",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
        },
    ):
        resource = Resource({"service.name": "test"})
        log = LogRecord(body="test log")

        result = send_logs(None, resource, [log])

        assert result is False
        assert request_made is False  # No HTTP request should be made


def test_disabled_does_not_use_otel_vars(monkeypatch):
    """Test that when disabled, picotel doesn't use user's OTEL_* variables.

    This is critical: when OTEL_SDK_DISABLED=true, picotel must not
    send telemetry to any OTEL endpoint.
    """
    import urllib.request  # noqa: PLC0415

    captured_url = None

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal captured_url
        captured_url = request.get_full_url()
        return MockResponse()

    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    # Simulate: Connect disabled picotel, but user set their own OTEL endpoint
    with patch.dict(
        os.environ,
        {
            "OTEL_SDK_DISABLED": "true",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://user-collector:4318",
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

        # Should NOT send to user's collector
        assert result is False
        assert captured_url is None  # No request made at all


def test_disabled_no_warning_logged(caplog):
    """Test that when disabled, no warning is logged about missing endpoint."""
    import logging  # noqa: PLC0415

    # Clear any existing log records
    caplog.clear()

    with caplog.at_level(logging.WARNING, logger="picotel"):
        with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}, clear=True):
            resource = Resource({"service.name": "test"})
            span = Span(
                trace_id=new_trace_id(),
                span_id=new_span_id(),
                name="test-span",
                start_time_ns=now_ns(),
                end_time_ns=now_ns(),
            )
            log = LogRecord(body="test log")

            send_spans(None, resource, [span])
            send_logs(None, resource, [log])

            # Should NOT log "endpoint not configured" warnings
            assert "endpoint not configured" not in caplog.text


def test_disabled_returns_false_no_exception():
    """Test that when disabled, functions return False without exception."""
    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test log")

        # Should return False without raising
        assert send_spans(None, resource, [span]) is False
        assert send_logs(None, resource, [log]) is False


def test_disabled_no_traceparent_error():
    """When disabled, TRACEPARENT sentinel must not log errors for missing env var."""
    with patch.dict(
        os.environ, {"OTEL_SDK_DISABLED": "true"}, clear=True
    ), patch.object(picotel._logger, "error") as mock_error:
        Span(trace_id=TRACEPARENT, name="test", start_time_ns=1000, end_time_ns=2000)
        LogRecord(body="test", trace_id=TRACEPARENT)

        mock_error.assert_not_called()
