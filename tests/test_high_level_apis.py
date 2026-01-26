# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for high-level APIs: Span context manager and OTLPHandler."""

import logging
from unittest.mock import patch

from picotel import (
    InstrumentationScope,
    LogRecord,
    OTLPHandler,
    Resource,
    Span,
    new_span_id,
    new_trace_id,
)


class TestSpanContextManager:
    """Tests for Span as a context manager."""

    def test_span_context_manager_sets_times(self):
        """Test that context manager sets start and end times."""
        trace_id = new_trace_id()
        span_id = new_span_id()

        with Span(
            trace_id=trace_id,
            span_id=span_id,
            name="test_span",
            start_time_ns=0,
            end_time_ns=0,
        ) as span:
            # Start time should be set on enter
            assert span.start_time_ns > 0
            start_time = span.start_time_ns

        # End time should be set on exit
        assert span.end_time_ns > 0
        assert span.end_time_ns >= start_time

    def test_span_context_manager_preserves_explicit_times(self):
        """Test that explicit times are not overwritten."""
        trace_id = new_trace_id()
        span_id = new_span_id()
        start_time = 1000000000
        end_time = 2000000000

        with Span(
            trace_id=trace_id,
            span_id=span_id,
            name="test_span",
            start_time_ns=start_time,
            end_time_ns=end_time,
        ) as span:
            # Explicit times should be preserved
            assert span.start_time_ns == start_time

        assert span.end_time_ns == end_time

    def test_span_context_manager_sends_on_exit(self):
        """Test that span is sent when context manager exits."""
        resource = Resource({"service.name": "test_service"})
        trace_id = new_trace_id()
        span_id = new_span_id()

        with patch("picotel.send_spans") as mock_send:
            mock_send.return_value = True

            with Span(
                trace_id=trace_id,
                span_id=span_id,
                name="test_span",
                start_time_ns=0,
                end_time_ns=0,
                endpoint="http://localhost:4318",
                resource=resource,
            ) as span:
                span.attributes["test.key"] = "test_value"

            # Verify send_spans was called with the right arguments
            mock_send.assert_called_once()
            call_args = mock_send.call_args
            assert call_args[0][0] == "http://localhost:4318"  # endpoint
            assert call_args[0][1] == resource  # resource
            assert len(call_args[0][2]) == 1  # spans list
            sent_span = call_args[0][2][0]
            assert sent_span.name == "test_span"
            assert sent_span.trace_id == trace_id
            assert sent_span.span_id == span_id
            assert sent_span.attributes["test.key"] == "test_value"

    def test_span_context_manager_without_endpoint(self):
        """Test that span works without endpoint (no sending)."""
        trace_id = new_trace_id()
        span_id = new_span_id()

        with patch("picotel.send_spans") as mock_send:
            with Span(
                trace_id=trace_id,
                span_id=span_id,
                name="test_span",
                start_time_ns=0,
                end_time_ns=0,
            ) as span:
                span.attributes["test"] = "value"

            # Should not call send_spans without endpoint
            mock_send.assert_not_called()

    def test_span_context_manager_with_scope(self):
        """Test that scope is passed when sending."""
        resource = Resource({"service.name": "test_service"})
        scope = InstrumentationScope("test_lib", "1.0.0")
        trace_id = new_trace_id()
        span_id = new_span_id()

        with patch("picotel.send_spans") as mock_send:
            mock_send.return_value = True

            with Span(
                trace_id=trace_id,
                span_id=span_id,
                name="test_span",
                start_time_ns=0,
                end_time_ns=0,
                endpoint="http://localhost:4318",
                resource=resource,
                scope=scope,
            ):
                pass

            # Verify scope was passed to send_spans
            mock_send.assert_called_once()
            call_args = mock_send.call_args
            assert call_args[0][3] == scope  # scope is 4th positional arg

    def test_nested_spans_with_parent_child(self):
        """Test nested spans with explicit parent-child relationship."""
        resource = Resource({"service.name": "test_service"})
        trace_id = new_trace_id()

        with patch("picotel.send_spans") as mock_send:
            mock_send.return_value = True

            with Span(
                trace_id=trace_id,
                span_id=new_span_id(),
                name="parent_span",
                start_time_ns=0,
                end_time_ns=0,
                endpoint="http://localhost:4318",
                resource=resource,
            ) as parent:
                parent_span_id = parent.span_id

                with Span(
                    trace_id=trace_id,
                    span_id=new_span_id(),
                    parent_span_id=parent_span_id,
                    name="child_span",
                    start_time_ns=0,
                    end_time_ns=0,
                    endpoint="http://localhost:4318",
                    resource=resource,
                ) as child:
                    child.attributes["child.attr"] = "value"

            # Should have sent two spans
            assert mock_send.call_count == 2

            # Check child span (first call)
            child_call = mock_send.call_args_list[0]
            child_span = child_call[0][2][0]  # First span in the list
            assert child_span.name == "child_span"
            assert child_span.trace_id == trace_id
            assert child_span.parent_span_id == parent_span_id
            assert child_span.attributes["child.attr"] == "value"

            # Check parent span (second call)
            parent_call = mock_send.call_args_list[1]
            parent_span = parent_call[0][2][0]  # First span in the list
            assert parent_span.name == "parent_span"
            assert parent_span.trace_id == trace_id
            assert parent_span.parent_span_id == ""  # Default is empty string, not None


class TestOTLPHandler:
    """Tests for OTLPHandler logging integration."""

    def test_otlp_handler_basic_logging(self):
        """Test basic logging through OTLPHandler."""
        resource = Resource({"service.name": "test_service"})

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            # Create logger with OTLPHandler
            logger = logging.getLogger("test_logger")
            logger.setLevel(logging.DEBUG)
            handler = OTLPHandler("http://localhost:4318", resource)
            logger.addHandler(handler)

            try:
                # Log a message
                logger.info("Test log message")

                # Verify send_logs was called
                mock_send.assert_called_once()
                call_args = mock_send.call_args
                assert call_args[0][0] == "http://localhost:4318"  # endpoint
                assert call_args[0][1] == resource  # resource
                assert len(call_args[0][2]) == 1  # logs list

                log_record = call_args[0][2][0]
                assert log_record.body == "Test log message"
                assert log_record.severity_number == LogRecord.Severity.INFO
                assert log_record.severity_text == "INFO"

                # Check code location attributes
                assert "code.filepath" in log_record.attributes
                assert "code.lineno" in log_record.attributes
                assert "code.function" in log_record.attributes

            finally:
                logger.removeHandler(handler)

    def test_otlp_handler_severity_mapping(self):
        """Test that Python log levels map correctly to OTLP severity."""
        resource = Resource({"service.name": "test_service"})

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            logger = logging.getLogger("test_severity")
            logger.setLevel(logging.DEBUG)
            handler = OTLPHandler("http://localhost:4318", resource)
            logger.addHandler(handler)

            try:
                # Test different log levels
                test_cases = [
                    (logging.DEBUG, "Debug message", LogRecord.Severity.DEBUG),
                    (logging.INFO, "Info message", LogRecord.Severity.INFO),
                    (logging.WARNING, "Warning message", LogRecord.Severity.WARN),
                    (logging.ERROR, "Error message", LogRecord.Severity.ERROR),
                    (logging.CRITICAL, "Critical message", LogRecord.Severity.FATAL),
                ]

                for level, message, expected_severity in test_cases:
                    mock_send.reset_mock()
                    logger.log(level, message)

                    mock_send.assert_called_once()
                    log_record = mock_send.call_args[0][2][0]
                    assert log_record.severity_number == expected_severity
                    assert log_record.body == message

            finally:
                logger.removeHandler(handler)

    def test_otlp_handler_with_trace_correlation(self):
        """Test logging with trace and span IDs."""
        resource = Resource({"service.name": "test_service"})
        trace_id = new_trace_id()
        span_id = new_span_id()

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            logger = logging.getLogger("test_trace")
            logger.setLevel(logging.INFO)
            handler = OTLPHandler("http://localhost:4318", resource)
            logger.addHandler(handler)

            try:
                # Log with trace correlation
                logger.info(
                    "Correlated log", extra={"trace_id": trace_id, "span_id": span_id}
                )

                mock_send.assert_called_once()
                log_record = mock_send.call_args[0][2][0]
                assert log_record.trace_id == trace_id
                assert log_record.span_id == span_id

            finally:
                logger.removeHandler(handler)

    def test_otlp_handler_with_scope(self):
        """Test OTLPHandler with instrumentation scope."""
        resource = Resource({"service.name": "test_service"})
        scope = InstrumentationScope("test_instrumentation", "2.0.0")

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            logger = logging.getLogger("test_scope")
            logger.setLevel(logging.INFO)
            handler = OTLPHandler("http://localhost:4318", resource, scope)
            logger.addHandler(handler)

            try:
                logger.info("Scoped log")

                mock_send.assert_called_once()
                call_args = mock_send.call_args
                assert call_args[0][3] == scope  # scope is 4th positional arg

            finally:
                logger.removeHandler(handler)

    def test_otlp_handler_error_handling(self):
        """Test that handler errors don't crash the application."""
        # Use invalid endpoint
        resource = Resource({"service.name": "test_service"})

        logger = logging.getLogger("test_error")
        logger.setLevel(logging.ERROR)  # Set level so messages are processed
        handler = OTLPHandler("http://invalid.endpoint:9999", resource)
        logger.addHandler(handler)

        try:
            # This should not raise an exception - the key test is that it doesn't crash
            logger.error("This should not crash")

            # If we get here without an exception, the test passed
            assert True

        finally:
            logger.removeHandler(handler)

    def test_otlp_handler_message_interpolation(self):
        """Test that log messages are properly interpolated."""
        resource = Resource({"service.name": "test_service"})

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            logger = logging.getLogger("test_interpolation")
            logger.setLevel(logging.INFO)
            handler = OTLPHandler("http://localhost:4318", resource)
            logger.addHandler(handler)

            try:
                # Log with format arguments
                logger.info("User %s logged in from %s", "alice", "192.168.1.1")

                mock_send.assert_called_once()
                log_record = mock_send.call_args[0][2][0]
                # getMessage() should interpolate the message
                assert log_record.body == "User alice logged in from 192.168.1.1"

            finally:
                logger.removeHandler(handler)

    def test_otlp_handler_timestamp(self):
        """Test that timestamps are correctly set from log record."""
        resource = Resource({"service.name": "test_service"})

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            logger = logging.getLogger("test_timestamp")
            handler = OTLPHandler("http://localhost:4318", resource)
            logger.addHandler(handler)

            try:
                # Create a log record with known timestamp
                record = logging.LogRecord(
                    name="test",
                    level=logging.INFO,
                    pathname="test.py",
                    lineno=42,
                    msg="Test message",
                    args=(),
                    exc_info=None,
                )
                record.created = 1234567890.123  # Known timestamp in seconds

                handler.emit(record)

                mock_send.assert_called_once()
                log_record = mock_send.call_args[0][2][0]
                # Should convert to nanoseconds (close due to float precision)
                assert abs(log_record.timestamp_ns - 1234567890123000000) < 1000

            finally:
                logger.removeHandler(handler)

    def test_otlp_handler_exception_writes_to_stderr(self):
        """Test that exceptions during emit are caught and written to stderr."""
        import picotel
        resource = Resource({"service.name": "test_service"})

        with patch.object(picotel, "send_logs", side_effect=Exception("Network error")) as mock_send:
            with patch.object(picotel.sys, "stderr") as mock_stderr:
                logger = logging.getLogger("test_exception_stderr")
                logger.setLevel(logging.INFO)
                handler = OTLPHandler("http://localhost:4318", resource)
                logger.addHandler(handler)

                try:
                    logger.info("This will fail")
                    assert mock_send.called, "send_logs was not called"
                    mock_stderr.write.assert_called_with("failed to send log\n")
                finally:
                    logger.removeHandler(handler)


class TestSpanSendMethod:
    """Tests for Span.send() method."""

    def test_span_send_with_explicit_params(self):
        """Test Span.send() with explicit parameters."""
        resource = Resource({"service.name": "test_service"})
        trace_id = new_trace_id()
        span_id = new_span_id()

        with patch("picotel.send_spans") as mock_send:
            mock_send.return_value = True

            span = Span(
                trace_id=trace_id,
                span_id=span_id,
                name="test_span",
                start_time_ns=1000,
                end_time_ns=2000,
            )

            result = span.send(
                endpoint="http://localhost:4318",
                resource=resource,
                scope=InstrumentationScope("test", "1.0"),
                timeout=5.0,
            )

            assert result is True
            mock_send.assert_called_once()
            call_args = mock_send.call_args
            assert call_args[0][0] == "http://localhost:4318"
            assert call_args[0][1] == resource
            assert len(call_args[0][2]) == 1
            assert call_args[0][2][0] == span

    def test_span_send_with_env_vars(self):
        """Test Span.send() uses environment variables when parameters are None."""
        import picotel

        # Clear caches before test
        picotel._get_endpoint.cache_clear()
        picotel._get_resource_from_env.cache_clear()

        with patch("picotel.send_spans") as mock_send:
            mock_send.return_value = True

            with patch("picotel._get_endpoint") as mock_endpoint:
                mock_endpoint.return_value = "http://env:4318"
                with patch("picotel._get_resource_from_env") as mock_resource:
                    test_resource = Resource({"service.name": "env_service"})
                    mock_resource.return_value = test_resource

                    span = Span(
                        trace_id=new_trace_id(),
                        span_id=new_span_id(),
                        name="test_span",
                        start_time_ns=1000,
                        end_time_ns=2000,
                    )

                    result = span.send()

                    assert result is True
                    mock_send.assert_called_once()
                    call_args = mock_send.call_args
                    assert call_args[0][0] == "http://env:4318"
                    assert call_args[0][1] == test_resource

    def test_span_send_fails_without_config(self):
        """Test Span.send() returns False when no config is available."""
        import picotel

        # Clear caches before test
        picotel._get_endpoint.cache_clear()
        picotel._get_resource_from_env.cache_clear()

        with patch("picotel._get_endpoint") as mock_endpoint:
            mock_endpoint.return_value = None
            with patch("picotel._get_resource_from_env") as mock_resource:
                mock_resource.return_value = None

                span = Span(
                    trace_id=new_trace_id(),
                    span_id=new_span_id(),
                    name="test_span",
                    start_time_ns=1000,
                    end_time_ns=2000,
                )

                result = span.send()

                assert result is False


class TestLogRecordSendMethod:
    """Tests for LogRecord.send() method."""

    def test_log_send_with_explicit_params(self):
        """Test LogRecord.send() with explicit parameters."""
        resource = Resource({"service.name": "test_service"})

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            log = LogRecord(body="test log message")

            result = log.send(
                endpoint="http://localhost:4318",
                resource=resource,
                scope=InstrumentationScope("test", "1.0"),
                timeout=5.0,
            )

            assert result is True
            mock_send.assert_called_once()
            call_args = mock_send.call_args
            assert call_args[0][0] == "http://localhost:4318"
            assert call_args[0][1] == resource
            assert len(call_args[0][2]) == 1
            assert call_args[0][2][0] == log

    def test_log_send_with_env_vars(self):
        """Test LogRecord.send() uses environment variables when parameters are None."""
        import picotel

        # Clear caches before test
        picotel._get_endpoint.cache_clear()
        picotel._get_resource_from_env.cache_clear()

        with patch("picotel.send_logs") as mock_send:
            mock_send.return_value = True

            with patch("picotel._get_endpoint") as mock_endpoint:
                mock_endpoint.return_value = "http://env:4318"
                with patch("picotel._get_resource_from_env") as mock_resource:
                    test_resource = Resource({"service.name": "env_service"})
                    mock_resource.return_value = test_resource

                    log = LogRecord(body="test log message")

                    result = log.send()

                    assert result is True
                    mock_send.assert_called_once()
                    call_args = mock_send.call_args
                    assert call_args[0][0] == "http://env:4318"
                    assert call_args[0][1] == test_resource

    def test_log_send_fails_without_config(self):
        """Test LogRecord.send() returns False when no config is available."""
        import picotel

        # Clear caches before test
        picotel._get_endpoint.cache_clear()
        picotel._get_resource_from_env.cache_clear()

        with patch("picotel._get_endpoint") as mock_endpoint:
            mock_endpoint.return_value = None
            with patch("picotel._get_resource_from_env") as mock_resource:
                mock_resource.return_value = None

                log = LogRecord(body="test log message")

                result = log.send()

                assert result is False
