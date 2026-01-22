"""Tests for ID generation and timestamp helper functions."""

import time

from miniotel import new_span_id, new_trace_id, now_ns

TRACE_ID_LENGTH = 32  # 16 bytes as hex
SPAN_ID_LENGTH = 16  # 8 bytes as hex
UNIQUENESS_SAMPLE_SIZE = 1000
ONE_SECOND_NS = 1e9


def test_new_trace_id_format():
    """Trace ID must be 32 lowercase hex characters (16 bytes)."""
    trace_id = new_trace_id()
    assert len(trace_id) == TRACE_ID_LENGTH
    assert trace_id.islower()
    assert all(c in "0123456789abcdef" for c in trace_id)


def test_new_span_id_format():
    """Span ID must be 16 lowercase hex characters (8 bytes)."""
    span_id = new_span_id()
    assert len(span_id) == SPAN_ID_LENGTH
    assert span_id.islower()
    assert all(c in "0123456789abcdef" for c in span_id)


def test_new_trace_id_uniqueness():
    """1000 trace IDs should all be unique."""
    ids = [new_trace_id() for _ in range(UNIQUENESS_SAMPLE_SIZE)]
    assert len(set(ids)) == UNIQUENESS_SAMPLE_SIZE


def test_new_span_id_uniqueness():
    """1000 span IDs should all be unique."""
    ids = [new_span_id() for _ in range(UNIQUENESS_SAMPLE_SIZE)]
    assert len(set(ids)) == UNIQUENESS_SAMPLE_SIZE


def test_now_ns_returns_nanoseconds():
    """now_ns() should return nanoseconds within 1 second of time.time()."""
    ns = now_ns()
    expected = time.time() * ONE_SECOND_NS
    assert abs(ns - expected) < ONE_SECOND_NS


def test_now_ns_is_integer():
    """now_ns() should return an integer."""
    assert isinstance(now_ns(), int)
