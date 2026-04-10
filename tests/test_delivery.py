"""Parametrized delivery tests verifying data flows through both sender types.

Each test runs twice: once with _SyncSender (immediate execution) and once
with _AsyncSender (background thread dispatch). Mocking urllib.request.urlopen
and using threading.Event makes both modes testable with the same assertions.
"""

import json
import logging
import os
import threading
import urllib.error
from unittest.mock import Mock, patch

import pytest

import picotel
from picotel import (
    Resource,
    Span,
    _AsyncSender,
    _SyncSender,
    new_span_id,
    new_trace_id,
    now_ns,
)

_mock_response = Mock(status=200)
_mock_response.__enter__ = Mock(return_value=_mock_response)
_mock_response.__exit__ = Mock(return_value=False)


@pytest.fixture(params=["sync", "async"])
def sender(request, monkeypatch):
    """Provide a sender instance and patch it into picotel._sender."""
    instance = _SyncSender() if request.param == "sync" else _AsyncSender()
    monkeypatch.setattr(picotel, "_sender", instance)
    return instance


def _mock_urlopen(monkeypatch):
    """Patch urlopen and return (mock, event) where event fires on call."""
    called = threading.Event()

    def side_effect(*_args, **_kwargs):
        called.set()
        return _mock_response

    mock = Mock(side_effect=side_effect)
    monkeypatch.setattr(picotel.urllib.request, "urlopen", mock)
    return mock, called


# ---------------------------------------------------------------------------
# Span.__exit__ delivery
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("sender")
def test_span_exit_delivers(monkeypatch):
    """Span context manager delivers to the correct /v1/traces URL."""
    mock_urlopen, called = _mock_urlopen(monkeypatch)

    resource = Resource(attributes={"service.name": "test"})
    with Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="delivery-test",
        start_time_ns=now_ns(),
        resource=resource,
        endpoint="http://collector:4318",
    ):
        pass

    assert called.wait(timeout=5), "urlopen was not called"
    request = mock_urlopen.call_args[0][0]
    assert request.get_full_url() == "http://collector:4318/v1/traces"


@pytest.mark.usefixtures("sender")
def test_span_exit_disabled_skips(monkeypatch):
    """Span.__exit__ skips delivery when OTEL_SDK_DISABLED=true."""
    mock_urlopen, _called = _mock_urlopen(monkeypatch)

    with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}):
        picotel._is_disabled.cache_clear()
        resource = Resource(attributes={"service.name": "test"})
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="disabled-test",
            start_time_ns=now_ns(),
            resource=resource,
            endpoint="http://collector:4318",
        ):
            pass

    mock_urlopen.assert_not_called()


@pytest.mark.usefixtures("sender")
def test_span_exit_payload_structure(monkeypatch):
    """Verify the OTLP JSON payload structure sent through the sender."""
    mock_urlopen, called = _mock_urlopen(monkeypatch)

    trace_id = new_trace_id()
    span_id = new_span_id()
    resource = Resource(attributes={"service.name": "payload-test"})
    with Span(
        trace_id=trace_id,
        span_id=span_id,
        name="structured-span",
        start_time_ns=now_ns(),
        resource=resource,
        endpoint="http://collector:4318",
        attributes={"test.key": "test-value"},
    ):
        pass

    assert called.wait(timeout=5), "urlopen was not called"
    request = mock_urlopen.call_args[0][0]
    payload = json.loads(request.data.decode("utf-8"))

    resource_spans = payload["resourceSpans"]
    assert len(resource_spans) == 1

    attrs = resource_spans[0]["resource"]["attributes"]
    attrs_dict = {a["key"]: a["value"] for a in attrs}
    assert attrs_dict["service.name"]["stringValue"] == "payload-test"

    spans = resource_spans[0]["scopeSpans"][0]["spans"]
    assert len(spans) == 1
    assert spans[0]["traceId"] == trace_id
    assert spans[0]["spanId"] == span_id
    assert spans[0]["name"] == "structured-span"


# ---------------------------------------------------------------------------
# OTLPHandler.emit delivery
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("sender")
def test_otlp_handler_delivers(monkeypatch):
    """OTLPHandler.emit delivers to the correct /v1/logs URL."""
    mock_urlopen, called = _mock_urlopen(monkeypatch)

    handler = picotel.OTLPHandler(
        resource=Resource(attributes={"service.name": "test"}),
        endpoint="http://collector:4318",
    )
    logger = logging.getLogger("test_delivery_handler")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    try:
        logger.info("delivery test message")
        assert called.wait(timeout=5), "urlopen was not called"
        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://collector:4318/v1/logs"
    finally:
        logger.removeHandler(handler)


@pytest.mark.usefixtures("sender")
def test_otlp_handler_disabled_skips(monkeypatch):
    """OTLPHandler.emit skips delivery when OTEL_SDK_DISABLED=true."""
    mock_urlopen, _called = _mock_urlopen(monkeypatch)

    handler = picotel.OTLPHandler(
        resource=Resource(attributes={"service.name": "test"}),
        endpoint="http://collector:4318",
    )
    logger = logging.getLogger("test_delivery_disabled")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    try:
        with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}):
            picotel._is_disabled.cache_clear()
            logger.info("should not be sent")
    finally:
        logger.removeHandler(handler)

    mock_urlopen.assert_not_called()


# ---------------------------------------------------------------------------
# _SyncSender circuit breaker integration
# ---------------------------------------------------------------------------


def test_sync_sender_circuit_breaker_trips_on_network_errors(
    monkeypatch, picotel_caplog
):
    """Circuit breaker trips after repeated real send_spans failures via urllib."""
    sender = _SyncSender()
    monkeypatch.setattr(picotel, "_sender", sender)

    # Make urlopen raise a network error every time
    mock_urlopen = Mock(side_effect=urllib.error.URLError("connection refused"))
    monkeypatch.setattr(picotel.urllib.request, "urlopen", mock_urlopen)

    resource = Resource(attributes={"service.name": "test"})

    # Send spans until the circuit breaker trips
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS):
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="failing-span",
            start_time_ns=now_ns(),
            resource=resource,
            endpoint="http://unreachable:4318",
        ):
            pass

    assert sender._tripped is True
    assert any(
        "further sends are disabled" in r.message for r in picotel_caplog.records
    )

    # Next send should be dropped entirely -- urlopen not called again
    mock_urlopen.reset_mock()
    with Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="dropped-span",
        start_time_ns=now_ns(),
        resource=resource,
        endpoint="http://unreachable:4318",
    ):
        pass
    mock_urlopen.assert_not_called()


def test_sync_sender_circuit_breaker_resets_on_success(monkeypatch):
    """A successful send after failures resets the circuit breaker counter."""
    sender = _SyncSender()
    monkeypatch.setattr(picotel, "_sender", sender)

    error_urlopen = Mock(side_effect=urllib.error.URLError("timeout"))
    monkeypatch.setattr(picotel.urllib.request, "urlopen", error_urlopen)

    resource = Resource(attributes={"service.name": "test"})

    # Accumulate failures just below threshold
    for _ in range(sender._MAX_CONSECUTIVE_ERRORS - 1):
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="failing",
            start_time_ns=now_ns(),
            resource=resource,
            endpoint="http://unreachable:4318",
        ):
            pass

    assert sender._consecutive_errors == sender._MAX_CONSECUTIVE_ERRORS - 1

    # One successful send resets the counter
    ok_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(picotel.urllib.request, "urlopen", ok_urlopen)

    with Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="success",
        start_time_ns=now_ns(),
        resource=resource,
        endpoint="http://collector:4318",
    ):
        pass

    assert sender._consecutive_errors == 0
    assert sender._tripped is False
