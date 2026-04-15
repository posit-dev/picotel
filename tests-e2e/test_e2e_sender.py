"""E2E tests confirming both sync and async senders deliver identical data."""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from conftest import read_collector_output

import picotel
from picotel import (
    Resource,
    Span,
    _AsyncSender,
    _SyncSender,
    new_span_id,
    new_trace_id,
)


@pytest.fixture(params=["sync", "async"])
def sender(request, monkeypatch):
    """Patch picotel._sender with the parametrized sender type."""
    instance = _SyncSender() if request.param == "sync" else _AsyncSender()
    monkeypatch.setattr(picotel, "_sender", instance)
    return instance


def test_span_context_manager_delivers(collector, sender):
    """Span context manager delivers correct data through both sender types."""
    resource = Resource(attributes={"service.name": "e2e-sender-test"})
    trace_id = new_trace_id()
    span_id = new_span_id()

    with Span(
        trace_id=trace_id,
        span_id=span_id,
        name="sender-test-span",
        endpoint=collector["endpoint"],
        resource=resource,
        kind=Span.Kind.CLIENT,
        attributes={"test.sender": type(sender).__name__},
    ):
        pass

    # For async, drain the queue: the sentinel fires only after the
    # send_spans call queued before it has completed.
    if isinstance(sender, _AsyncSender):
        done = threading.Event()
        sender.submit(done.set)
        done.wait(timeout=5)

    output = read_collector_output(collector["output_file"])
    assert len(output) == 1

    spans = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 1
    assert spans[0]["traceId"] == trace_id
    assert spans[0]["spanId"] == span_id
    assert spans[0]["name"] == "sender-test-span"
    assert spans[0]["kind"] == 3  # CLIENT
