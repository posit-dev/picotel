"""Tests for send_logs() and _log_to_dict() functions."""

import json
import urllib.error
from unittest.mock import Mock, patch

from miniotel import (
    InstrumentationScope,
    LogRecord,
    Resource,
    _log_to_dict,
    send_logs,
)


class TestLogToDict:
    """Tests for _log_to_dict() serialization."""

    def test_minimal_log_record(self):
        """Test converting a minimal LogRecord to OTLP dict."""
        log = LogRecord(body="Hello world")

        with patch("miniotel.now_ns", return_value=1234567890):
            result = _log_to_dict(log)

        assert result["timeUnixNano"] == "1234567890"
        assert result["observedTimeUnixNano"] == "1234567890"
        assert result["severityNumber"] == LogRecord.Severity.INFO
        assert result["body"] == {"stringValue": "Hello world"}
        # Optional fields should be omitted
        assert "severityText" not in result
        assert "attributes" not in result
        assert "traceId" not in result
        assert "spanId" not in result
        assert "flags" not in result

    def test_log_with_explicit_timestamps(self):
        """Test that explicit timestamps are used when provided."""
        log = LogRecord(
            body="Test",
            timestamp_ns=1111111111,
            observed_timestamp_ns=2222222222,
        )

        result = _log_to_dict(log)

        assert result["timeUnixNano"] == "1111111111"
        assert result["observedTimeUnixNano"] == "2222222222"

    def test_log_with_trace_correlation(self):
        """Test log with trace and span IDs for correlation."""
        log = LogRecord(
            body="Correlated log",
            trace_id="abcdef1234567890abcdef1234567890",
            span_id="1234567890abcdef",
            trace_flags=1,
        )

        with patch("miniotel.now_ns", return_value=9999999999):
            result = _log_to_dict(log)

        assert result["traceId"] == "abcdef1234567890abcdef1234567890"
        assert result["spanId"] == "1234567890abcdef"
        assert result["flags"] == 1

    def test_log_with_severity(self):
        """Test log with custom severity level and text."""
        log = LogRecord(
            body="Error occurred",
            severity_number=LogRecord.Severity.ERROR,
            severity_text="ERROR",
        )

        with patch("miniotel.now_ns", return_value=5555555555):
            result = _log_to_dict(log)

        assert result["severityNumber"] == LogRecord.Severity.ERROR
        assert result["severityText"] == "ERROR"

    def test_log_with_attributes(self):
        """Test log with attributes."""
        log = LogRecord(
            body="Log with attrs",
            attributes={
                "user.id": "user123",
                "http.status_code": 500,
                "success": False,
            },
        )

        with patch("miniotel.now_ns", return_value=7777777777):
            result = _log_to_dict(log)

        assert "attributes" in result
        attrs = {attr["key"]: attr["value"] for attr in result["attributes"]}
        assert attrs["user.id"] == {"stringValue": "user123"}
        assert attrs["http.status_code"] == {"intValue": "500"}
        assert attrs["success"] == {"boolValue": False}

    def test_log_body_types(self):
        """Test that different body types are converted correctly."""
        # String body
        log = LogRecord(body="String message")
        with patch("miniotel.now_ns", return_value=1000):
            result = _log_to_dict(log)
        assert result["body"] == {"stringValue": "String message"}

        # Dict body
        log = LogRecord(body={"error": "Something went wrong", "code": 500})
        with patch("miniotel.now_ns", return_value=2000):
            result = _log_to_dict(log)
        assert result["body"]["kvlistValue"]["values"] == [
            {"key": "error", "value": {"stringValue": "Something went wrong"}},
            {"key": "code", "value": {"intValue": "500"}},
        ]

        # List body
        log = LogRecord(body=["item1", "item2", 3])
        with patch("miniotel.now_ns", return_value=3000):
            result = _log_to_dict(log)
        assert result["body"]["arrayValue"]["values"] == [
            {"stringValue": "item1"},
            {"stringValue": "item2"},
            {"intValue": "3"},
        ]


class TestSendLogs:
    """Tests for send_logs() HTTP export."""

    def test_send_logs_success(self):
        """Test successful log export to collector."""
        resource = Resource({"service.name": "test-service"})
        logs = [
            LogRecord(body="Log 1", severity_number=LogRecord.Severity.INFO),
            LogRecord(body="Log 2", severity_number=LogRecord.Severity.WARN),
        ]

        mock_response = Mock()
        mock_response.status = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)

        with patch(
            "miniotel.urllib.request.urlopen", return_value=mock_response
        ) as mock_urlopen, patch("miniotel.now_ns", return_value=1234567890):
            result = send_logs("http://localhost:4318", resource, logs)

        assert result is True

        # Verify the request was made correctly
        mock_urlopen.assert_called_once()
        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://localhost:4318/v1/logs"
        assert request.get_method() == "POST"
        assert request.headers["Content-type"] == "application/json"

        # Verify the payload structure
        payload = json.loads(request.data.decode("utf-8"))
        assert "resourceLogs" in payload
        assert len(payload["resourceLogs"]) == 1

        resource_logs = payload["resourceLogs"][0]
        assert "resource" in resource_logs
        assert "scopeLogs" in resource_logs

        scope_logs = resource_logs["scopeLogs"][0]
        assert "logRecords" in scope_logs
        assert len(scope_logs["logRecords"]) == 2

        # Check first log record
        log_record = scope_logs["logRecords"][0]
        assert log_record["body"] == {"stringValue": "Log 1"}
        assert log_record["severityNumber"] == LogRecord.Severity.INFO

    def test_send_logs_with_scope(self):
        """Test sending logs with instrumentation scope."""
        resource = Resource({"service.name": "test-service"})
        scope = InstrumentationScope(
            name="my-library",
            version="1.0.0",
            attributes={"library.language": "python"},
        )
        logs = [LogRecord(body="Scoped log")]

        mock_response = Mock()
        mock_response.status = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)

        with patch(
            "miniotel.urllib.request.urlopen", return_value=mock_response
        ) as mock_urlopen, patch("miniotel.now_ns", return_value=9999999999):
            result = send_logs("http://localhost:4318", resource, logs, scope=scope)

        assert result is True

        # Verify scope is in the payload
        # Get the request from the urlopen mock
        request = mock_urlopen.call_args[0][0]
        payload = json.loads(request.data.decode("utf-8"))
        scope_logs = payload["resourceLogs"][0]["scopeLogs"][0]
        assert "scope" in scope_logs
        assert scope_logs["scope"]["name"] == "my-library"
        assert scope_logs["scope"]["version"] == "1.0.0"

    def test_send_logs_with_trace_correlation(self):
        """Test sending logs correlated with traces."""
        resource = Resource({"service.name": "traced-service"})
        logs = [
            LogRecord(
                body="Trace correlated log",
                trace_id="abcdef1234567890abcdef1234567890",
                span_id="1234567890abcdef",
                trace_flags=1,
            )
        ]

        mock_response = Mock()
        mock_response.status = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)

        with patch(
            "miniotel.urllib.request.urlopen", return_value=mock_response
        ) as mock_urlopen, patch("miniotel.now_ns", return_value=5555555555):
            result = send_logs("http://localhost:4318", resource, logs)

        assert result is True

        # Verify trace correlation in payload
        request = mock_urlopen.call_args[0][0]
        payload = json.loads(request.data.decode("utf-8"))
        log_record = payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
        assert log_record["traceId"] == "abcdef1234567890abcdef1234567890"
        assert log_record["spanId"] == "1234567890abcdef"
        assert log_record["flags"] == 1

    def test_send_logs_http_error(self):
        """Test handling of HTTP errors during log export."""
        resource = Resource({"service.name": "test-service"})
        logs = [LogRecord(body="Test log")]

        with patch("miniotel.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.HTTPError(
                "http://localhost:4318/v1/logs", 500, "Internal Server Error", {}, None
            )

            with patch("miniotel.logging.warning") as mock_warning:
                result = send_logs("http://localhost:4318", resource, logs)

        assert result is False
        mock_warning.assert_called_once()
        assert "Failed to send logs" in mock_warning.call_args[0][0]

    def test_send_logs_network_error(self):
        """Test handling of network errors during log export."""
        resource = Resource({"service.name": "test-service"})
        logs = [LogRecord(body="Test log")]

        with patch("miniotel.urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = OSError("Connection refused")

            with patch("miniotel.logging.warning") as mock_warning:
                result = send_logs("http://localhost:4318", resource, logs)

        assert result is False
        mock_warning.assert_called_once()
        assert "Failed to send logs" in mock_warning.call_args[0][0]

    def test_send_empty_logs_list(self):
        """Test sending an empty list of logs."""
        resource = Resource({"service.name": "test-service"})
        logs = []

        mock_response = Mock()
        mock_response.status = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=None)

        with patch(
            "miniotel.urllib.request.urlopen", return_value=mock_response
        ) as mock_urlopen:
            result = send_logs("http://localhost:4318", resource, logs)

        assert result is True

        # Verify empty logRecords array is sent
        request = mock_urlopen.call_args[0][0]
        payload = json.loads(request.data.decode("utf-8"))
        log_records = payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
        assert log_records == []
