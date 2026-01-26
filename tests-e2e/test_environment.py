# Copyright (C) 2026 by Posit Software, PBC.

"""End-to-end tests for environment variable configuration.

These tests verify that spans and logs can be sent using environment
variables for configuration, and that lru_cache works correctly.
"""

import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from time import sleep
from unittest.mock import patch

import miniotel
from miniotel import (
    LogRecord,
    OTLPHandler,
    Span,
    new_span_id,
    new_trace_id,
    send_logs,
    send_spans,
)


class MockCollector(BaseHTTPRequestHandler):
    """Mock OTLP collector that captures requests."""

    captured_requests = []

    def do_POST(self):
        """Handle POST requests and capture the data."""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)

        # Store the request
        MockCollector.captured_requests.append(
            {
                "path": self.path,
                "headers": dict(self.headers),
                "body": json.loads(post_data.decode("utf-8")),
            }
        )

        # Send success response
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A002
        """Suppress log messages from the HTTP server."""


def run_mock_collector(port=4318):
    """Run a mock collector in a separate thread."""
    server = HTTPServer(("localhost", port), MockCollector)
    thread = Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    sleep(0.1)  # Give server time to start
    return server


def test_span_send_with_env_vars():
    """Test that Span.send() works with environment configuration."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()

    # Clear any previous captured requests
    MockCollector.captured_requests = []

    # Start mock collector
    server = run_mock_collector(port=4319)

    try:
        # Set up environment
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://localhost:4319",
                "OTEL_SERVICE_NAME": "test-service-spans",
                "OTEL_EXPORTER_OTLP_HEADERS": (
                    "Authorization=Bearer token123,X-Custom=value"
                ),
            },
        ):
            # Create and send a span using environment configuration
            span = Span(
                trace_id=new_trace_id(),
                span_id=new_span_id(),
                name="test-span",
                start_time_ns=1000000000,
                end_time_ns=2000000000,
                attributes={"test.attribute": "value"},
            )

            # Send without explicit endpoint/resource - should use env vars
            result = span.send()

            assert result is True
            sleep(0.1)  # Give time for request to be processed

            # Verify request was received
            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Check endpoint
            assert request["path"] == "/v1/traces"

            # Check headers from environment
            assert request["headers"]["Authorization"] == "Bearer token123"
            assert request["headers"]["X-Custom"] == "value"

            # Check resource from environment
            body = request["body"]
            assert "resourceSpans" in body
            resource = body["resourceSpans"][0]["resource"]
            assert len(resource["attributes"]) == 1
            assert resource["attributes"][0]["key"] == "service.name"
            attr = resource["attributes"][0]
            assert attr["value"]["stringValue"] == "test-service-spans"

            # Check span data
            spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
            assert len(spans) == 1
            assert spans[0]["name"] == "test-span"
    finally:
        server.shutdown()


def test_send_spans_with_env_vars():
    """Test that send_spans() works with environment configuration."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()

    # Clear any previous captured requests
    MockCollector.captured_requests = []

    # Start mock collector
    server = run_mock_collector(port=4320)

    try:
        # Set up environment with general endpoint (not traces-specific)
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4320",
                "OTEL_SERVICE_NAME": "test-service-batch",
            },
        ):
            # Create spans
            spans = [
                Span(
                    trace_id=new_trace_id(),
                    span_id=new_span_id(),
                    name="span1",
                    start_time_ns=1000000000,
                    end_time_ns=2000000000,
                ),
                Span(
                    trace_id=new_trace_id(),
                    span_id=new_span_id(),
                    name="span2",
                    start_time_ns=3000000000,
                    end_time_ns=4000000000,
                ),
            ]

            # Send without explicit endpoint - should use env vars for endpoint
            # But we need to get resource from env manually for send_spans
            resource = miniotel._get_resource_from_env()
            result = send_spans(None, resource, spans)

            assert result is True
            sleep(0.1)  # Give time for request to be processed

            # Verify request was received
            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Check endpoint (should include /v1/traces)
            assert request["path"] == "/v1/traces"

            # Check that both spans were sent
            body = request["body"]
            sent_spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
            assert len(sent_spans) == 2
            assert sent_spans[0]["name"] == "span1"
            assert sent_spans[1]["name"] == "span2"
    finally:
        server.shutdown()


def test_log_send_with_env_vars():
    """Test that LogRecord.send() works with environment configuration."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()

    # Clear any previous captured requests
    MockCollector.captured_requests = []

    # Start mock collector
    server = run_mock_collector(port=4321)

    try:
        # Set up environment
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://localhost:4321",
                "OTEL_SERVICE_NAME": "test-service-logs",
            },
        ):
            # Create and send a log using environment configuration
            log = LogRecord(
                body="Test log message",
                severity_number=LogRecord.Severity.INFO,
                attributes={"log.attribute": "value"},
            )

            # Send without explicit endpoint/resource - should use env vars
            result = log.send()

            assert result is True
            sleep(0.1)  # Give time for request to be processed

            # Verify request was received
            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Check endpoint
            assert request["path"] == "/v1/logs"

            # Check resource from environment
            body = request["body"]
            assert "resourceLogs" in body
            resource = body["resourceLogs"][0]["resource"]
            assert len(resource["attributes"]) == 1
            assert resource["attributes"][0]["key"] == "service.name"
            attr = resource["attributes"][0]
            assert attr["value"]["stringValue"] == "test-service-logs"

            # Check log data
            logs = body["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
            assert len(logs) == 1
            assert logs[0]["body"]["stringValue"] == "Test log message"
    finally:
        server.shutdown()


def test_otlp_handler_with_env_vars():
    """Test that OTLPHandler works with environment configuration."""
    import logging  # noqa: PLC0415

    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()

    # Clear any previous captured requests
    MockCollector.captured_requests = []

    # Start mock collector
    server = run_mock_collector(port=4322)

    try:
        # Set up environment
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4322",
                "OTEL_SERVICE_NAME": "test-service-handler",
            },
        ):
            # Create handler without explicit endpoint/resource
            handler = OTLPHandler()
            logger = logging.getLogger("test_env_e2e")
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

            # Log a message
            logger.info("Test handler message")

            sleep(0.1)  # Give time for request to be processed

            # Verify request was received
            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Check endpoint
            assert request["path"] == "/v1/logs"

            # Check resource from environment
            body = request["body"]
            resource = body["resourceLogs"][0]["resource"]
            found_service_name = False
            for attr in resource["attributes"]:
                if attr["key"] == "service.name":
                    assert attr["value"]["stringValue"] == "test-service-handler"
                    found_service_name = True
            assert found_service_name

            # Check log message
            logs = body["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
            assert len(logs) == 1
            assert logs[0]["body"]["stringValue"] == "Test handler message"

            # Clean up logger
            logger.removeHandler(handler)
    finally:
        server.shutdown()


def test_send_logs_with_env_vars():
    """Test that send_logs() works with environment configuration."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()

    # Clear any previous captured requests
    MockCollector.captured_requests = []

    # Start mock collector
    server = run_mock_collector(port=4323)

    try:
        # Set up environment with general endpoint (not logs-specific)
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4323",
                "OTEL_SERVICE_NAME": "test-service-batch-logs",
            },
        ):
            # Create logs
            logs = [
                LogRecord(
                    body="Log message 1",
                    severity_number=LogRecord.Severity.INFO,
                ),
                LogRecord(
                    body="Log message 2",
                    severity_number=LogRecord.Severity.WARN,
                ),
            ]

            # Send without explicit endpoint - should use env vars for endpoint
            # But we need to get resource from env manually for send_logs
            resource = miniotel._get_resource_from_env()
            result = send_logs(None, resource, logs)

            assert result is True
            sleep(0.1)  # Give time for request to be processed

            # Verify request was received
            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Check endpoint (should include /v1/logs)
            assert request["path"] == "/v1/logs"

            # Check that both logs were sent
            body = request["body"]
            sent_logs = body["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
            assert len(sent_logs) == 2
            assert sent_logs[0]["body"]["stringValue"] == "Log message 1"
            assert sent_logs[1]["body"]["stringValue"] == "Log message 2"
    finally:
        server.shutdown()


def test_cache_clearing():
    """Test that cache clearing works correctly.

    Per OTEL spec, general endpoint has /v1/{signal} appended.
    """
    # Clear caches
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()

    # Set initial environment
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://first:4318",
            "OTEL_SERVICE_NAME": "first-service",
            "OTEL_EXPORTER_OTLP_HEADERS": "X-First=value1",
        },
    ):
        # Call functions to cache values
        endpoint1 = miniotel._get_endpoint("traces")
        resource1 = miniotel._get_resource_from_env()
        headers1 = miniotel._parse_headers()

        # General endpoint gets path appended per OTEL spec
        assert endpoint1 == "http://first:4318/v1/traces"
        assert resource1.attributes["service.name"] == "first-service"
        assert headers1["X-First"] == "value1"

    # Change environment without clearing cache
    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://second:4318",
            "OTEL_SERVICE_NAME": "second-service",
            "OTEL_EXPORTER_OTLP_HEADERS": "X-Second=value2",
        },
    ):
        # Should still get cached values
        endpoint2 = miniotel._get_endpoint("traces")
        resource2 = miniotel._get_resource_from_env()
        headers2 = miniotel._parse_headers()

        assert endpoint2 == "http://first:4318/v1/traces"  # Cached
        assert resource2.attributes["service.name"] == "first-service"  # Cached
        assert headers2["X-First"] == "value1"  # Cached

        # Now clear cache
        miniotel._get_endpoint.cache_clear()
        miniotel._get_resource_from_env.cache_clear()
        miniotel._parse_headers.cache_clear()

        # Should get new values
        endpoint3 = miniotel._get_endpoint("traces")
        resource3 = miniotel._get_resource_from_env()
        headers3 = miniotel._parse_headers()

        assert endpoint3 == "http://second:4318/v1/traces"  # New value
        assert resource3.attributes["service.name"] == "second-service"  # New value
        assert headers3["X-Second"] == "value2"  # New value


def test_span_with_traceparent_sends_correct_ids():
    """Test that Span with TRACEPARENT sends correct trace_id and parent_span_id."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()
    miniotel._parse_traceparent.cache_clear()

    MockCollector.captured_requests = []
    server = run_mock_collector(port=4324)

    try:
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4324",
                "OTEL_SERVICE_NAME": "traceparent-test",
                "TRACEPARENT": (
                    "00-abcdef1234567890abcdef1234567890-fedcba0987654321-01"
                ),
            },
        ):
            span = miniotel.Span(
                trace_id=miniotel.TRACEPARENT,
                name="child-span",
                start_time_ns=1000000000,
                end_time_ns=2000000000,
            )
            result = span.send()

            assert result is True
            sleep(0.1)

            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Verify the span has the correct trace_id from TRACEPARENT
            body = request["body"]
            spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
            assert len(spans) == 1
            assert spans[0]["traceId"] == "abcdef1234567890abcdef1234567890"
            assert spans[0]["parentSpanId"] == "fedcba0987654321"
            assert spans[0]["name"] == "child-span"
            # span_id should be auto-generated, not the parent from TRACEPARENT
            assert spans[0]["spanId"] != "fedcba0987654321"
    finally:
        server.shutdown()


def test_logrecord_with_traceparent_sends_correct_ids():
    """Test that LogRecord with TRACEPARENT sends correct trace correlation."""
    # Clear caches before test
    miniotel._get_endpoint.cache_clear()
    miniotel._get_resource_from_env.cache_clear()
    miniotel._parse_headers.cache_clear()
    miniotel._parse_traceparent.cache_clear()

    MockCollector.captured_requests = []
    server = run_mock_collector(port=4325)

    try:
        with patch.dict(
            os.environ,
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4325",
                "OTEL_SERVICE_NAME": "traceparent-log-test",
                "TRACEPARENT": (
                    "00-11111111222222223333333344444444-aaaabbbbccccdddd-00"
                ),
            },
        ):
            log = miniotel.LogRecord(
                body="Log correlated via TRACEPARENT",
                trace_id=miniotel.TRACEPARENT,
                severity_number=miniotel.LogRecord.Severity.INFO,
            )
            result = log.send()

            assert result is True
            sleep(0.1)

            assert len(MockCollector.captured_requests) == 1
            request = MockCollector.captured_requests[0]

            # Verify the log has the correct trace correlation from TRACEPARENT
            body = request["body"]
            logs = body["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
            assert len(logs) == 1
            assert logs[0]["traceId"] == "11111111222222223333333344444444"
            assert logs[0]["spanId"] == "aaaabbbbccccdddd"
            assert logs[0]["body"]["stringValue"] == "Log correlated via TRACEPARENT"
    finally:
        server.shutdown()


if __name__ == "__main__":
    # Run tests if executed directly
    test_span_send_with_env_vars()
    test_send_spans_with_env_vars()
    test_log_send_with_env_vars()
    test_otlp_handler_with_env_vars()
    test_send_logs_with_env_vars()
    test_cache_clearing()
    test_span_with_traceparent_sends_correct_ids()
    test_logrecord_with_traceparent_sends_correct_ids()
    print("All e2e environment tests passed!")  # noqa: T201
