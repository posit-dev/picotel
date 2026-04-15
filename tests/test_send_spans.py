"""Tests for send_spans() function."""

import json
import urllib.error
from unittest.mock import Mock, patch

import picotel
from picotel import (
    InstrumentationScope,
    Resource,
    Span,
    new_span_id,
    new_trace_id,
    now_ns,
    send_spans,
)

_mock_response = Mock(status=200)
_mock_response.__enter__ = Mock(return_value=_mock_response)
_mock_response.__exit__ = Mock(return_value=False)


def test_send_spans_basic():
    """Test sending spans produces correct OTLP payload."""
    resource = Resource({"service.name": "test_service", "service.version": "1.0.0"})

    trace_id = new_trace_id()
    span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
        attributes={"test.attribute": "value"},
    )

    with patch(
        "picotel.urllib.request.urlopen", return_value=_mock_response
    ) as mock_urlopen:
        result = send_spans("http://localhost:4318", resource, [span])

    assert result is True

    request = mock_urlopen.call_args[0][0]
    assert request.get_full_url() == "http://localhost:4318/v1/traces"
    assert request.headers["Content-type"] == "application/json"

    payload = json.loads(request.data.decode("utf-8"))
    assert len(payload["resourceSpans"]) == 1

    resource_span = payload["resourceSpans"][0]
    attrs = {a["key"]: a["value"] for a in resource_span["resource"]["attributes"]}
    assert attrs["service.name"]["stringValue"] == "test_service"
    assert attrs["service.version"]["stringValue"] == "1.0.0"

    spans = resource_span["scopeSpans"][0]["spans"]
    assert len(spans) == 1
    assert spans[0]["traceId"] == trace_id
    assert spans[0]["name"] == "test_operation"


def test_send_spans_with_scope():
    """Test sending spans with instrumentation scope."""
    resource = Resource({"service.name": "test_service"})
    scope = InstrumentationScope(
        name="my.library",
        version="2.0.0",
        attributes={"library.language": "python"},
    )

    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="scoped_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    with patch(
        "picotel.urllib.request.urlopen", return_value=_mock_response
    ) as mock_urlopen:
        result = send_spans("http://localhost:4318", resource, [span], scope=scope)

    assert result is True

    payload = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
    scope_span = payload["resourceSpans"][0]["scopeSpans"][0]

    assert scope_span["scope"]["name"] == "my.library"
    assert scope_span["scope"]["version"] == "2.0.0"
    scope_attrs = scope_span["scope"]["attributes"]
    assert len(scope_attrs) == 1
    assert scope_attrs[0]["key"] == "library.language"
    assert scope_attrs[0]["value"]["stringValue"] == "python"


def test_send_spans_multiple():
    """Test sending multiple spans in one request."""
    resource = Resource({"service.name": "test_service"})

    trace_id = new_trace_id()
    parent_span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        name="parent_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 2000000,
        kind=Span.Kind.SERVER,
    )

    child_span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        parent_span_id=parent_span.span_id,
        name="child_operation",
        start_time_ns=now_ns() + 500000,
        end_time_ns=now_ns() + 1500000,
        kind=Span.Kind.CLIENT,
        status=Span.Status.OK,
    )

    with patch(
        "picotel.urllib.request.urlopen", return_value=_mock_response
    ) as mock_urlopen:
        result = send_spans(
            "http://localhost:4318", resource, [parent_span, child_span]
        )

    assert result is True

    payload = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 2

    assert spans[0]["name"] == "parent_operation"
    assert spans[0]["kind"] == 2  # SERVER
    assert "parentSpanId" not in spans[0]

    assert spans[1]["name"] == "child_operation"
    assert spans[1]["kind"] == 3  # CLIENT
    assert spans[1]["parentSpanId"] == parent_span.span_id
    assert spans[1]["status"]["code"] == 1  # OK


def test_send_spans_nonexistent_endpoint():
    """Test sending to unreachable endpoint returns False without raising."""
    resource = Resource({"service.name": "test_service"})
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    with patch(
        "picotel.urllib.request.urlopen",
        side_effect=urllib.error.URLError("connection refused"),
    ):
        result = send_spans("http://localhost:59999", resource, [span])

    assert result is False


def test_send_spans_with_trailing_slash():
    """Test that endpoint with trailing slash is handled correctly."""
    resource = Resource({"service.name": "test_service"})
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    with patch(
        "picotel.urllib.request.urlopen", return_value=_mock_response
    ) as mock_urlopen:
        result = send_spans("http://localhost:4318/", resource, [span])

    assert result is True
    assert (
        mock_urlopen.call_args[0][0].get_full_url() == "http://localhost:4318/v1/traces"
    )


def test_send_spans_passes_timeout_to_urlopen():
    """Test that the timeout parameter is forwarded to urlopen."""
    resource = Resource({"service.name": "test_service"})
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    with patch(
        "picotel.urllib.request.urlopen", return_value=_mock_response
    ) as mock_urlopen:
        send_spans("http://localhost:4318", resource, [span], timeout=0.75)

    assert mock_urlopen.call_args[1]["timeout"] == 0.75


def test_send_spans_skips_invalid_trace_id():
    """Test that spans without trace_id are skipped but valid ones are sent."""
    resource = Resource({"service.name": "test_service"})

    valid_span = Span(
        trace_id=new_trace_id(),
        name="valid_span",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )
    invalid_span = Span.__new__(Span)  # Bypass __post_init__
    invalid_span.trace_id = ""
    invalid_span.span_id = new_span_id()
    invalid_span.name = "invalid_span"
    invalid_span.start_time_ns = now_ns()
    invalid_span.end_time_ns = now_ns() + 1000000
    invalid_span.parent_span_id = ""
    invalid_span.kind = Span.Kind.INTERNAL
    invalid_span.attributes = {}
    invalid_span.events = []
    invalid_span.links = []
    invalid_span.status = None

    with patch(
        "picotel.urllib.request.urlopen", return_value=_mock_response
    ) as mock_urlopen, patch.object(picotel._logger, "error") as mock_error:
        result = send_spans(
            "http://localhost:4318", resource, [valid_span, invalid_span]
        )

    assert result is True
    mock_error.assert_called_once()
    assert "Span invalid" in mock_error.call_args[0][0]
    assert "trace_id is empty" in mock_error.call_args[0][0]

    payload = json.loads(mock_urlopen.call_args[0][0].data.decode("utf-8"))
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 1
    assert spans[0]["name"] == "valid_span"
