"""Tests for send_spans() function."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from miniotel import (
    InstrumentationScope,
    Resource,
    Span,
    SpanKind,
    SpanStatus,
    StatusCode,
    new_span_id,
    new_trace_id,
    now_ns,
    send_spans,
)


class MockOTLPHandler(BaseHTTPRequestHandler):
    """Mock OTLP collector handler that captures requests."""

    captured_requests = []

    def do_POST(self):
        """Handle POST requests to capture the payload."""
        if self.path == "/v1/traces":
            content_length = int(self.headers["Content-Length"])
            body = self.rfile.read(content_length)

            # Store the request details
            MockOTLPHandler.captured_requests.append(
                {
                    "path": self.path,
                    "headers": dict(self.headers),
                    "body": body.decode("utf-8"),
                }
            )

            # Send success response
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"partialSuccess":{}}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, _format, *_args):
        """Suppress log messages from the test server."""


def test_send_spans_basic():
    """Test sending spans to a mock HTTP server."""
    # Clear any previous captured requests
    MockOTLPHandler.captured_requests = []

    # Start mock server in a thread
    server = HTTPServer(("localhost", 0), MockOTLPHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()

    # Create test data
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

    # Send spans
    result = send_spans(f"http://localhost:{port}", resource, [span])

    # Wait for server thread to complete
    server_thread.join()
    server.server_close()

    # Verify success
    assert result is True

    # Verify request was captured
    assert len(MockOTLPHandler.captured_requests) == 1
    request = MockOTLPHandler.captured_requests[0]

    # Verify request details
    assert request["path"] == "/v1/traces"
    assert request["headers"]["Content-Type"] == "application/json"

    # Verify JSON payload structure
    payload = json.loads(request["body"])
    assert "resourceSpans" in payload
    assert len(payload["resourceSpans"]) == 1

    resource_span = payload["resourceSpans"][0]
    assert "resource" in resource_span
    assert "attributes" in resource_span["resource"]

    # Check resource attributes
    attrs = resource_span["resource"]["attributes"]
    attrs_dict = {attr["key"]: attr["value"] for attr in attrs}
    assert attrs_dict["service.name"]["stringValue"] == "test_service"
    assert attrs_dict["service.version"]["stringValue"] == "1.0.0"

    # Check scopeSpans
    assert "scopeSpans" in resource_span
    assert len(resource_span["scopeSpans"]) == 1
    scope_span = resource_span["scopeSpans"][0]

    # Check spans array
    assert "spans" in scope_span
    assert len(scope_span["spans"]) == 1
    span_data = scope_span["spans"][0]
    assert span_data["traceId"] == trace_id
    assert span_data["name"] == "test_operation"


def test_send_spans_with_scope():
    """Test sending spans with instrumentation scope."""
    MockOTLPHandler.captured_requests = []

    server = HTTPServer(("localhost", 0), MockOTLPHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()

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

    result = send_spans(f"http://localhost:{port}", resource, [span], scope=scope)

    server_thread.join()
    server.server_close()

    assert result is True

    # Verify scope in payload
    payload = json.loads(MockOTLPHandler.captured_requests[0]["body"])
    scope_span = payload["resourceSpans"][0]["scopeSpans"][0]

    assert "scope" in scope_span
    assert scope_span["scope"]["name"] == "my.library"
    assert scope_span["scope"]["version"] == "2.0.0"
    assert "attributes" in scope_span["scope"]

    scope_attrs = scope_span["scope"]["attributes"]
    assert len(scope_attrs) == 1
    assert scope_attrs[0]["key"] == "library.language"
    assert scope_attrs[0]["value"]["stringValue"] == "python"


def test_send_spans_empty_list():
    """Test sending empty spans list still makes valid request."""
    MockOTLPHandler.captured_requests = []

    server = HTTPServer(("localhost", 0), MockOTLPHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()

    resource = Resource({"service.name": "test_service"})

    # Send empty spans list
    result = send_spans(f"http://localhost:{port}", resource, [])

    server_thread.join()
    server.server_close()

    assert result is True

    # Verify request structure is still valid
    payload = json.loads(MockOTLPHandler.captured_requests[0]["body"])
    assert "resourceSpans" in payload
    assert len(payload["resourceSpans"]) == 1
    assert "scopeSpans" in payload["resourceSpans"][0]
    assert "spans" in payload["resourceSpans"][0]["scopeSpans"][0]
    assert payload["resourceSpans"][0]["scopeSpans"][0]["spans"] == []


def test_send_spans_multiple():
    """Test sending multiple spans in one request."""
    MockOTLPHandler.captured_requests = []

    server = HTTPServer(("localhost", 0), MockOTLPHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()

    resource = Resource({"service.name": "test_service"})

    trace_id = new_trace_id()
    parent_span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        name="parent_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 2000000,
        kind=SpanKind.SERVER,
    )

    child_span = Span(
        trace_id=trace_id,
        span_id=new_span_id(),
        parent_span_id=parent_span.span_id,
        name="child_operation",
        start_time_ns=now_ns() + 500000,
        end_time_ns=now_ns() + 1500000,
        kind=SpanKind.CLIENT,
        status=SpanStatus(code=StatusCode.OK),
    )

    result = send_spans(f"http://localhost:{port}", resource, [parent_span, child_span])

    server_thread.join()
    server.server_close()

    assert result is True

    # Verify both spans in payload
    payload = json.loads(MockOTLPHandler.captured_requests[0]["body"])
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 2

    # Check parent span
    assert spans[0]["name"] == "parent_operation"
    assert spans[0]["kind"] == 2  # SERVER
    assert "parentSpanId" not in spans[0]

    # Check child span
    assert spans[1]["name"] == "child_operation"
    assert spans[1]["kind"] == 3  # CLIENT
    assert spans[1]["parentSpanId"] == parent_span.span_id
    assert spans[1]["status"]["code"] == 1  # OK


def test_send_spans_nonexistent_endpoint():
    """Test sending to non-existent endpoint returns False without raising."""
    resource = Resource({"service.name": "test_service"})
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    # Use a port that's unlikely to have a server
    result = send_spans("http://localhost:59999", resource, [span], timeout=0.5)

    # Should return False, not raise an exception
    assert result is False


def test_send_spans_with_trailing_slash():
    """Test that endpoint with trailing slash is handled correctly."""
    MockOTLPHandler.captured_requests = []

    server = HTTPServer(("localhost", 0), MockOTLPHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.handle_request)
    server_thread.start()

    resource = Resource({"service.name": "test_service"})
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    # Endpoint with trailing slash
    result = send_spans(f"http://localhost:{port}/", resource, [span])

    server_thread.join()
    server.server_close()

    assert result is True
    assert MockOTLPHandler.captured_requests[0]["path"] == "/v1/traces"


def test_send_spans_timeout():
    """Test that timeout parameter is respected."""

    class SlowHandler(BaseHTTPRequestHandler):
        """Handler that responds slowly."""

        def do_POST(self):
            """Delay response to trigger timeout."""
            time.sleep(2)  # Sleep longer than timeout
            self.send_response(200)
            self.end_headers()

        def log_message(self, _format, *_args):
            """Suppress log messages."""

    server = HTTPServer(("localhost", 0), SlowHandler)
    port = server.server_address[1]
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    resource = Resource({"service.name": "test_service"})
    span = Span(
        trace_id=new_trace_id(),
        span_id=new_span_id(),
        name="test_operation",
        start_time_ns=now_ns(),
        end_time_ns=now_ns() + 1000000,
    )

    # Use short timeout
    result = send_spans(f"http://localhost:{port}", resource, [span], timeout=0.1)

    # Should timeout and return False
    assert result is False

    # Clean up server
    server.shutdown()
    server.server_close()
