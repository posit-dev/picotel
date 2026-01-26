# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for W3C Trace Context via TRACEPARENT environment variable."""

import os
from unittest.mock import patch

import miniotel
from miniotel import TRACEPARENT, LogRecord, Span


def test_parse_traceparent_valid():
    """Test parsing valid TRACEPARENT format."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        result = miniotel._parse_traceparent()
        assert result is not None
        trace_id, parent_id, trace_flags = result
        assert trace_id == "0af7651916cd43dd8448eb211c80319c"
        assert parent_id == "b7ad6b7169203331"
        assert trace_flags == 1


def test_parse_traceparent_not_set():
    """Test parsing when TRACEPARENT is not set."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(os.environ, {}, clear=True):
        assert miniotel._parse_traceparent() is None


def test_parse_traceparent_invalid_format():
    """Test parsing malformed TRACEPARENT values."""
    test_cases = [
        # Wrong number of parts
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
        "00-0af7651916cd43dd8448eb211c80319c",
        "invalid",
        # Wrong version
        "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "99-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        # Invalid trace_id (not 32 hex chars)
        "00-invalid-b7ad6b7169203331-01",
        "00-0af7651916cd43dd8448eb211c8031-b7ad6b7169203331-01",  # Too short
        "00-0af7651916cd43dd8448eb211c80319cX-b7ad6b7169203331-01",  # Too long
        "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",  # Non-hex char
        # Invalid parent_id (not 16 hex chars)
        "00-0af7651916cd43dd8448eb211c80319c-invalid-01",
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333-01",  # Too short
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b71692033311-01",  # Too long
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333g-01",  # Non-hex char
        # Invalid trace_flags (not 2 hex chars)
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1",  # Too short
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-001",  # Too long
        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0g",  # Non-hex char
        # Empty string
        "",
    ]

    for invalid_value in test_cases:
        miniotel._parse_traceparent.cache_clear()
        with patch.dict(os.environ, {"TRACEPARENT": invalid_value}):
            assert miniotel._parse_traceparent() is None, (
                f"Should reject: {invalid_value}"
            )


def test_span_with_traceparent_sentinel():
    """Test Span with TRACEPARENT sentinel reads from env."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        span = Span(
            trace_id=TRACEPARENT, name="test", start_time_ns=1000, end_time_ns=2000
        )
        assert span.trace_id == "0af7651916cd43dd8448eb211c80319c"
        assert span.parent_span_id == "b7ad6b7169203331"


def test_span_with_traceparent_sentinel_no_env():
    """Test Span with TRACEPARENT sentinel and no env var logs error."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(os.environ, {}, clear=True), patch.object(
        miniotel._logger, "error"
    ) as mock_error:
        span = Span(
            trace_id=TRACEPARENT, name="test", start_time_ns=1000, end_time_ns=2000
        )
        assert span.trace_id == ""
        mock_error.assert_called_once()
        assert "TRACEPARENT requested" in mock_error.call_args[0][0]


def test_span_with_explicit_trace_id():
    """Test Span with explicit trace_id does not read from TRACEPARENT."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        span = Span(
            trace_id="abcdef1234567890abcdef1234567890",
            name="test",
            start_time_ns=1000,
            end_time_ns=2000,
        )
        # Should use explicit trace_id, not TRACEPARENT
        assert span.trace_id == "abcdef1234567890abcdef1234567890"
        # Should NOT get parent_span_id from TRACEPARENT (explicit behavior)
        assert span.parent_span_id == ""


def test_span_with_traceparent_and_explicit_parent():
    """Test Span with TRACEPARENT sentinel but explicit parent_span_id."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        span = Span(
            trace_id=TRACEPARENT,
            name="test",
            parent_span_id="1234567890abcdef",
            start_time_ns=1000,
            end_time_ns=2000,
        )
        # Should use trace_id from TRACEPARENT
        assert span.trace_id == "0af7651916cd43dd8448eb211c80319c"
        # Should use explicit parent_span_id
        assert span.parent_span_id == "1234567890abcdef"


def test_span_without_traceparent_explicit_trace_required():
    """Test Span without TRACEPARENT sentinel requires explicit trace_id."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(os.environ, {}, clear=True):
        # With explicit trace_id, should work fine
        span = Span(
            trace_id="abcdef1234567890abcdef1234567890",
            name="test",
            start_time_ns=1000,
            end_time_ns=2000,
        )
        assert span.trace_id == "abcdef1234567890abcdef1234567890"


def test_span_span_id_always_generated():
    """Test Span without span_id always generates a new span_id."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        span = Span(
            trace_id=TRACEPARENT, name="test", start_time_ns=1000, end_time_ns=2000
        )
        assert span.span_id
        assert len(span.span_id) == 16
        # Should NOT be the parent-id from TRACEPARENT
        assert span.span_id != "b7ad6b7169203331"


def test_logrecord_with_traceparent_sentinel():
    """Test LogRecord with TRACEPARENT sentinel populates trace_id and span_id."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        log = LogRecord(body="test log", trace_id=TRACEPARENT)
        assert log.trace_id == "0af7651916cd43dd8448eb211c80319c"
        assert log.span_id == "b7ad6b7169203331"


def test_logrecord_with_traceparent_sentinel_no_env():
    """Test LogRecord with TRACEPARENT sentinel and no env var logs error."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(os.environ, {}, clear=True), patch.object(
        miniotel._logger, "error"
    ) as mock_error:
        log = LogRecord(body="test log", trace_id=TRACEPARENT)
        assert log.trace_id == ""
        assert log.span_id == ""
        mock_error.assert_called_once()
        assert "TRACEPARENT requested" in mock_error.call_args[0][0]


def test_logrecord_without_traceparent_sentinel():
    """Test LogRecord without TRACEPARENT sentinel has empty trace_id and span_id."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        # Without sentinel, should NOT read from TRACEPARENT
        log = LogRecord(body="test log")
        assert log.trace_id == ""
        assert log.span_id == ""


def test_logrecord_with_explicit_trace_id():
    """Test LogRecord with explicit trace_id does not read from TRACEPARENT."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        log = LogRecord(
            body="test log",
            trace_id="abcdef1234567890abcdef1234567890",
        )
        assert log.trace_id == "abcdef1234567890abcdef1234567890"
        assert log.span_id == ""


def test_logrecord_with_traceparent_and_explicit_span_id():
    """Test LogRecord with TRACEPARENT sentinel but explicit span_id."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        log = LogRecord(
            body="test log",
            trace_id=TRACEPARENT,
            span_id="1234567890abcdef",
        )
        # Should get trace_id from TRACEPARENT
        assert log.trace_id == "0af7651916cd43dd8448eb211c80319c"
        # But should use explicit span_id
        assert log.span_id == "1234567890abcdef"


def test_traceparent_caching():
    """Test that _parse_traceparent result is cached."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
    ):
        result1 = miniotel._parse_traceparent()
        result2 = miniotel._parse_traceparent()
        assert result1 is result2  # Same object reference due to caching


def test_traceparent_with_uppercase_hex():
    """Test that TRACEPARENT with uppercase hex is accepted."""
    miniotel._parse_traceparent.cache_clear()

    with patch.dict(
        os.environ,
        {"TRACEPARENT": "00-0AF7651916CD43DD8448EB211C80319C-B7AD6B7169203331-FF"},
    ):
        result = miniotel._parse_traceparent()
        assert result is not None
        trace_id, parent_id, trace_flags = result
        assert trace_id == "0AF7651916CD43DD8448EB211C80319C"
        assert parent_id == "B7AD6B7169203331"
        assert trace_flags == 255


def test_traceparent_sentinel_identity():
    """Test that TRACEPARENT sentinel can be checked with 'is'."""
    assert TRACEPARENT is miniotel.TRACEPARENT
