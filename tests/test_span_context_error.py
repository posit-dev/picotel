#!/usr/bin/env python3
"""Test that Span context manager raises error when no endpoint configured."""

import os
from unittest.mock import patch

import pytest

import picotel
from picotel import PicotelConfigError, Resource, Span, new_trace_id


def test_span_context_manager_raises_without_endpoint():
    """Test that Span context manager raises PicotelConfigError when no endpoint."""
    # Clear caches
    picotel._get_endpoint.cache_clear()
    picotel._get_resource_from_env.cache_clear()
    picotel._is_disabled.cache_clear()

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(PicotelConfigError) as exc_info:
            with Span(
                trace_id=new_trace_id(),
                name="test-span",
                resource=Resource({"service.name": "test"}),
            ):
                pass

        assert "No OTLP endpoint configured" in str(exc_info.value)
        assert "PICOTEL_SDK_DISABLED=true" in str(exc_info.value)


def test_span_context_manager_works_with_endpoint():
    """Test that Span context manager works when endpoint is provided."""
    # Clear caches
    picotel._get_endpoint.cache_clear()
    picotel._is_disabled.cache_clear()

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
    picotel._is_disabled.cache_clear()

    with patch.dict(os.environ, {"PICOTEL_SDK_DISABLED": "true"}):
        # Should work without providing timestamps
        with Span(
            trace_id=new_trace_id(),
            name="test",
            resource=Resource({"service.name": "test"}),
        ) as span:
            assert span.start_time_ns > 0  # Set automatically

        assert span.end_time_ns > 0  # Set automatically
        assert span.end_time_ns >= span.start_time_ns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])