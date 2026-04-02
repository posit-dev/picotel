"""Test that Span context manager logs config error when no endpoint configured."""

import logging
import os
from unittest.mock import patch

import picotel
from picotel import Resource, Span, new_trace_id


def test_span_context_manager_logs_without_endpoint(caplog):
    """Test that Span context manager logs PicotelConfigError when no endpoint."""
    picotel._logger.addHandler(caplog.handler)
    try:
        with patch.dict(os.environ, {}, clear=True):
            with caplog.at_level(logging.ERROR, logger="picotel"):
                with Span(
                    trace_id=new_trace_id(),
                    name="test-span",
                    resource=Resource({"service.name": "test"}),
                ):
                    pass

            assert any(
                "No OTLP endpoint configured" in r.message for r in caplog.records
            )
    finally:
        picotel._logger.removeHandler(caplog.handler)


def test_span_context_manager_works_with_endpoint():
    """Test that Span context manager works when endpoint is provided."""
    # Mock the send to avoid actual network call
    import urllib.request  # noqa: PLC0415

    class MockResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    original_urlopen = urllib.request.urlopen
    request_made = False

    def mock_urlopen(request, timeout=None):  # noqa: ARG001
        nonlocal request_made
        request_made = True
        return MockResponse()

    try:
        urllib.request.urlopen = mock_urlopen

        # Should work with explicit endpoint
        with Span(
            trace_id=new_trace_id(),
            name="test-span",
            endpoint="http://test:4318",
            resource=Resource({"service.name": "test"}),
        ) as span:
            assert span.start_time_ns > 0

        assert span.end_time_ns > 0
        assert request_made

    finally:
        urllib.request.urlopen = original_urlopen


def test_span_context_manager_without_timestamps():
    """Test that timestamps are optional in context manager."""
    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}):
        # Should work without providing timestamps
        with Span(
            trace_id=new_trace_id(),
            name="test",
            resource=Resource({"service.name": "test"}),
        ) as span:
            assert span.start_time_ns > 0  # Set automatically

        assert span.end_time_ns > 0  # Set automatically
        assert span.end_time_ns >= span.start_time_ns
