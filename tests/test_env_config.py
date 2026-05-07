# Copyright (C) 2026 by Posit Software, PBC.

"""Tests for environment variable configuration."""

import os
from typing import Dict
from unittest.mock import Mock, patch

import pytest

import picotel
from picotel import (
    PicotelConfigError,
    Resource,
    send_logs,
    send_spans,
)

_mock_response = Mock(status=200)
_mock_response.__enter__ = Mock(return_value=_mock_response)
_mock_response.__exit__ = Mock(return_value=False)


def _prefixed(env: Dict[str, str], prefix: str) -> Dict[str, str]:
    """Remap standard OTEL_* env var names for the given prefix.

    When prefix is empty, returns env unchanged (standard OTEL mode).
    When prefix is "PICOTEL", OTEL_X becomes PICOTEL_X and
    TRACEPARENT becomes PICOTEL_TRACEPARENT — same logic as _env() in picotel.py.
    """
    if not prefix:
        return env
    result = {"PICOTEL_PREFIX": prefix}
    for key, value in env.items():
        if key.startswith("OTEL_"):
            result[prefix + "_" + key[5:]] = value
        else:
            result[prefix + "_" + key] = value
    return result


PREFIXES = pytest.mark.parametrize("prefix", ["", "PICOTEL"])


# ---------------------------------------------------------------------------
# Endpoint resolution
# ---------------------------------------------------------------------------


@PREFIXES
def test_get_endpoint_traces_specific(prefix):
    """Test that trace-specific endpoint takes precedence."""
    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        assert picotel._get_endpoint("traces") == "http://traces:4318"


@PREFIXES
def test_get_endpoint_logs_specific(prefix):
    """Test that logs-specific endpoint takes precedence."""
    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs:4318",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        assert picotel._get_endpoint("logs") == "http://logs:4318"


@PREFIXES
def test_get_endpoint_fallback_to_general(prefix):
    """Test fallback to general endpoint when specific not set.

    Per OTEL spec, general endpoint has signal path appended.
    """
    env = _prefixed({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://general:4318"}, prefix)
    with patch.dict(os.environ, env):
        assert picotel._get_endpoint("traces") == "http://general:4318/v1/traces"
        picotel._get_endpoint.cache_clear()
        assert picotel._get_endpoint("logs") == "http://general:4318/v1/logs"


def test_get_endpoint_none_when_not_set():
    """Test that get_endpoint returns None when no env vars set."""
    with patch.dict(os.environ, {}, clear=True):
        assert picotel._get_endpoint("traces") is None
        assert picotel._get_endpoint("logs") is None


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------


@PREFIXES
def test_parse_headers(prefix):
    """Test parsing EXPORTER_OTLP_HEADERS environment variable."""
    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "key1=value1,key2=value2,key3=value with spaces"
            )
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        headers = picotel._parse_headers()
        assert headers == {
            "key1": "value1",
            "key2": "value2",
            "key3": "value with spaces",
        }

    # Clear cache between sub-tests
    picotel._parse_headers.cache_clear()

    # Empty headers
    env = _prefixed({"OTEL_EXPORTER_OTLP_HEADERS": ""}, prefix)
    with patch.dict(os.environ, env):
        assert picotel._parse_headers() == {}

    picotel._parse_headers.cache_clear()

    # Not set
    with patch.dict(os.environ, _prefixed({}, prefix), clear=True):
        assert picotel._parse_headers() == {}

    picotel._parse_headers.cache_clear()

    # Whitespace handling
    env = _prefixed(
        {"OTEL_EXPORTER_OTLP_HEADERS": " key1 = value1 , key2=value2 "}, prefix
    )
    with patch.dict(os.environ, env):
        headers = picotel._parse_headers()
        assert headers == {"key1": "value1", "key2": "value2"}


# ---------------------------------------------------------------------------
# Service name / Resource from env
# ---------------------------------------------------------------------------


@PREFIXES
def test_get_resource_from_env(prefix):
    """Test creating Resource from SERVICE_NAME."""
    env = _prefixed({"OTEL_SERVICE_NAME": "my-service"}, prefix)
    with patch.dict(os.environ, env):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"service.name": "my-service"}

    picotel._get_resource_from_env.cache_clear()

    # Not set
    with patch.dict(os.environ, _prefixed({}, prefix), clear=True):
        assert picotel._get_resource_from_env() is None


# ---------------------------------------------------------------------------
# SDK disabled
# ---------------------------------------------------------------------------


@PREFIXES
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("TRUE", True),
        ("1", True),
        ("false", False),
        ("0", False),
    ],
)
def test_is_disabled(prefix, value, expected):
    """Test _is_disabled honours the SDK_DISABLED env var with various values."""
    env = _prefixed({"OTEL_SDK_DISABLED": value}, prefix)
    with patch.dict(os.environ, env, clear=True):
        assert picotel._is_disabled() is expected


@PREFIXES
def test_is_disabled_unset(prefix):
    """Test _is_disabled returns False when SDK_DISABLED is not set."""
    with patch.dict(os.environ, _prefixed({}, prefix), clear=True):
        assert picotel._is_disabled() is False


# ---------------------------------------------------------------------------
# Traceparent
# ---------------------------------------------------------------------------


@PREFIXES
def test_parse_traceparent(prefix):
    """Test _parse_traceparent reads the (possibly prefixed) TRACEPARENT var."""
    env = _prefixed(
        {"TRACEPARENT": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
        prefix,
    )
    with patch.dict(os.environ, env):
        result = picotel._parse_traceparent()
        assert result is not None
        assert result[0] == "0af7651916cd43dd8448eb211c80319c"
        assert result[1] == "b7ad6b7169203331"
        assert result[2] == 1


# ---------------------------------------------------------------------------
# OTEL_RESOURCE_ATTRIBUTES (W3C Baggage format: key=value,key=value)
# ---------------------------------------------------------------------------


@PREFIXES
def test_resource_attributes_basic(prefix):
    """Test RESOURCE_ATTRIBUTES with simple key=value pairs."""
    env = _prefixed(
        {
            "OTEL_SERVICE_NAME": "my-service",
            "OTEL_RESOURCE_ATTRIBUTES": "content.guid=abc-123,deployment.env=prod",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {
            "service.name": "my-service",
            "content.guid": "abc-123",
            "deployment.env": "prod",
        }


@PREFIXES
def test_resource_attributes_without_service_name(prefix):
    """Test RESOURCE_ATTRIBUTES works without a service name."""
    env = _prefixed({"OTEL_RESOURCE_ATTRIBUTES": "content.guid=abc-123"}, prefix)
    with patch.dict(os.environ, env, clear=True):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"content.guid": "abc-123"}


@PREFIXES
def test_resource_attributes_service_name_wins_over_attr(prefix):
    """Test SERVICE_NAME overrides service.name in resource attrs."""
    env = _prefixed(
        {
            "OTEL_SERVICE_NAME": "explicit-name",
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=from-attrs,other=val",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes["service.name"] == "explicit-name"
        assert resource.attributes["other"] == "val"


def test_resource_attributes_percent_encoded_comma_in_value():
    """Test that percent-encoded comma (%2C) in value is decoded correctly."""
    # value "a,b" is encoded as "a%2Cb"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "tags=a%2Cb%2Cc"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"tags": "a,b,c"}


def test_resource_attributes_percent_encoded_equals_in_value():
    """Test that percent-encoded equals (%3D) in value is decoded correctly."""
    # value "x=1" is encoded as "x%3D1"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "expr=x%3D1"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"expr": "x=1"}


def test_resource_attributes_percent_encoded_key():
    """Test that percent-encoded characters in the key are decoded."""
    # key "my,key" is encoded as "my%2Ckey"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "my%2Ckey=value"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"my,key": "value"}


def test_resource_attributes_spaces_and_special_chars():
    """Test percent-encoded spaces and unicode in values."""
    # "hello world" -> "hello%20world", "café" -> "caf%C3%A9"
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "msg=hello%20world,place=caf%C3%A9"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {"msg": "hello world", "place": "café"}


def test_resource_attributes_all_values_are_strings():
    """Test that all attribute values are strings per the W3C Baggage spec."""
    with patch.dict(
        os.environ,
        {"OTEL_RESOURCE_ATTRIBUTES": "count=42,enabled=true,ratio=3.14"},
        clear=True,
    ):
        resource = picotel._get_resource_from_env()
        assert resource is not None
        assert resource.attributes == {
            "count": "42",
            "enabled": "true",
            "ratio": "3.14",
        }
        for v in resource.attributes.values():
            assert isinstance(v, str)


# ---------------------------------------------------------------------------
# Integration tests: send_spans / send_logs with env vars
# ---------------------------------------------------------------------------


@PREFIXES
def test_send_spans_with_env_endpoint(prefix, monkeypatch):
    """Test send_spans uses environment variable when endpoint is None."""
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://env-test:4318"}, prefix)
    with patch.dict(os.environ, env):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        result = send_spans(None, resource, [span])

        assert result is True
        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://env-test:4318/v1/traces"


@PREFIXES
def test_send_logs_with_env_endpoint(prefix, monkeypatch):
    """Test send_logs uses environment variable when endpoint is None.

    Per OTEL spec, signal-specific endpoints are used as-is (include full path).
    """
    import urllib.request  # noqa: PLC0415

    from picotel import LogRecord  # noqa: PLC0415

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "http://logs-env:4318/v1/logs"}, prefix
    )
    with patch.dict(os.environ, env):
        resource = Resource({"service.name": "test"})
        log = LogRecord(body="test log")

        result = send_logs(None, resource, [log])

        assert result is True
        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://logs-env:4318/v1/logs"


@PREFIXES
def test_send_spans_with_headers_from_env(prefix, monkeypatch):
    """Test that headers from environment are included in requests."""
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://test:4318",
            "OTEL_EXPORTER_OTLP_HEADERS": (
                "Authorization=Bearer token123,X-Custom=value"
            ),
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        result = send_spans(None, resource, [span])

        assert result is True
        request = mock_urlopen.call_args[0][0]
        assert request.headers["Authorization"] == "Bearer token123"
        assert request.headers["X-custom"] == "value"
        assert request.headers["Content-type"] == "application/json"


def test_send_without_endpoint_raises_config_error():
    """Test that send functions raise PicotelConfigError when no endpoint."""
    from picotel import (  # noqa: PLC0415
        LogRecord,
        Span,
        new_span_id,
        new_trace_id,
        now_ns,
    )

    with patch.dict(os.environ, {}, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test")

        with pytest.raises(PicotelConfigError):
            send_spans(None, resource, [span])
        with pytest.raises(PicotelConfigError):
            send_logs(None, resource, [log])


@PREFIXES
def test_send_returns_false_when_disabled(prefix, monkeypatch):
    """Test that send functions return False and make no HTTP request when disabled."""
    import urllib.request  # noqa: PLC0415

    from picotel import (  # noqa: PLC0415
        LogRecord,
        Span,
        new_span_id,
        new_trace_id,
        now_ns,
    )

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed({"OTEL_SDK_DISABLED": "true"}, prefix)
    with patch.dict(os.environ, env, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test")

        assert send_spans(None, resource, [span]) is False
        assert send_logs(None, resource, [log]) is False
        mock_urlopen.assert_not_called()


def test_disabled_no_traceparent_error():
    """When disabled, TRACEPARENT sentinel must not log errors for missing env var."""
    from picotel import (  # noqa: PLC0415
        TRACEPARENT,
        LogRecord,
        Span,
    )

    with patch.dict(
        os.environ, {"OTEL_SDK_DISABLED": "true"}, clear=True
    ), patch.object(picotel._logger, "error") as mock_error:
        Span(trace_id=TRACEPARENT, name="test", start_time_ns=1000, end_time_ns=2000)
        LogRecord(body="test", trace_id=TRACEPARENT)

        mock_error.assert_not_called()


@PREFIXES
def test_span_context_manager_with_env(prefix, monkeypatch):
    """Test Span context manager uses environment variables."""
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id  # noqa: PLC0415

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://env:4318",
            "OTEL_SERVICE_NAME": "env-service",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        with Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
        ):
            pass

        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://env:4318/v1/traces"


@PREFIXES
def test_otlp_handler_with_env(prefix, monkeypatch):
    """Test OTLPHandler uses environment variables.

    Uses general endpoint which gets /v1/logs appended per OTEL spec.
    """
    import logging  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import OTLPHandler  # noqa: PLC0415

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://logs:4318",
            "OTEL_SERVICE_NAME": "logging-service",
        },
        prefix,
    )
    with patch.dict(os.environ, env):
        handler = OTLPHandler()
        logger = logging.getLogger(f"test_env_{prefix or 'otel'}")
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        logger.info("Test message")

        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://logs:4318/v1/logs"


def test_explicit_endpoint_still_works(monkeypatch):
    """Test that providing explicit endpoint works even without env vars."""
    import urllib.request  # noqa: PLC0415

    from picotel import (  # noqa: PLC0415
        LogRecord,
        Span,
        new_span_id,
        new_trace_id,
        now_ns,
    )

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(os.environ, {}, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )
        log = LogRecord(body="test log")

        assert send_spans("http://explicit:4318", resource, [span]) is True
        assert send_logs("http://explicit:4318", resource, [log]) is True

        urls = [c[0][0].get_full_url() for c in mock_urlopen.call_args_list]
        assert urls == [
            "http://explicit:4318/v1/traces",
            "http://explicit:4318/v1/logs",
        ]


# ---------------------------------------------------------------------------
# TLS CA certificate (OTEL_EXPORTER_OTLP_CERTIFICATE)
# ---------------------------------------------------------------------------


@PREFIXES
def test_ssl_context_reads_certificate_via_env(prefix, monkeypatch):
    """_ssl_context() routes OTEL_EXPORTER_OTLP_CERTIFICATE through _env().

    Proves that setting PICOTEL_EXPORTER_OTLP_CERTIFICATE works under
    PICOTEL_PREFIX=PICOTEL, matching how all other OTEL_EXPORTER_OTLP_*
    vars are remapped. Without a prefix, the standard OTEL_ name is still
    honoured.
    """
    import ssl  # noqa: PLC0415

    sentinel = object()
    mock_create = Mock(return_value=sentinel)
    monkeypatch.setattr(ssl, "create_default_context", mock_create)

    env = _prefixed({"OTEL_EXPORTER_OTLP_CERTIFICATE": "/path/to/ca.pem"}, prefix)
    with patch.dict(os.environ, env, clear=True):
        result = picotel._ssl_context()

    assert result is sentinel
    mock_create.assert_called_once_with(cafile="/path/to/ca.pem")


@PREFIXES
def test_ssl_context_returns_none_when_certificate_unset(prefix, monkeypatch):
    """_ssl_context() returns None when the (prefixed) cert var is unset.

    Under PICOTEL_PREFIX=PICOTEL an unprefixed OTEL_EXPORTER_OTLP_CERTIFICATE
    must be ignored — only the prefixed name counts.
    """
    import ssl  # noqa: PLC0415

    mock_create = Mock()
    monkeypatch.setattr(ssl, "create_default_context", mock_create)

    # Put the *wrong* name in the environment: standard when prefixed,
    # prefixed when unprefixed. Either way, _ssl_context() should ignore it.
    stray = (
        {"OTEL_EXPORTER_OTLP_CERTIFICATE": "/should/be/ignored.pem"}
        if prefix
        else {"PICOTEL_EXPORTER_OTLP_CERTIFICATE": "/should/be/ignored.pem"}
    )
    env = {**_prefixed({}, prefix), **stray}
    with patch.dict(os.environ, env, clear=True):
        assert picotel._ssl_context() is None
    mock_create.assert_not_called()


@PREFIXES
def test_send_spans_passes_ssl_context_from_env_certificate(prefix, monkeypatch):
    """send_spans hands the SSL context built from the (prefixed) cert var to urlopen.

    This closes the integration-level gap for OTEL_EXPORTER_OTLP_CERTIFICATE:
    every other prefix-sensitive env var is exercised through send_spans with
    a mocked urlopen, proving end-to-end that the prefixed name is honoured
    and reaches the transport layer. Without the _env() wrap on the cert
    lookup, the PICOTEL-prefixed case would fail to find the cert, and the
    context kwarg seen by urlopen would be None instead of our sentinel.
    """
    import ssl  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    sentinel = object()
    mock_create = Mock(return_value=sentinel)
    monkeypatch.setattr(ssl, "create_default_context", mock_create)

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    env = _prefixed(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://secure:4318",
            "OTEL_EXPORTER_OTLP_CERTIFICATE": "/some/path",
        },
        prefix,
    )
    with patch.dict(os.environ, env, clear=True):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        result = send_spans(None, resource, [span])

    assert result is True
    assert mock_urlopen.call_args.kwargs["context"] is sentinel
    mock_create.assert_called_once_with(cafile="/some/path")


@PREFIXES
@pytest.mark.parametrize(
    ("signal", "signal_var"),
    [
        ("traces", "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE"),
        ("logs", "OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE"),
    ],
)
def test_ssl_context_signal_specific_certificate_wins(
    prefix, signal, signal_var, monkeypatch
):
    """Per-signal CA var overrides the general OTEL_EXPORTER_OTLP_CERTIFICATE.

    Mirrors the _get_endpoint precedence: OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE
    / OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE win over OTEL_EXPORTER_OTLP_CERTIFICATE,
    and both names are remapped by PICOTEL_PREFIX via _env().
    """
    import ssl  # noqa: PLC0415

    sentinel = object()
    mock_create = Mock(return_value=sentinel)
    monkeypatch.setattr(ssl, "create_default_context", mock_create)

    env = _prefixed(
        {
            signal_var: "/path/signal.pem",
            "OTEL_EXPORTER_OTLP_CERTIFICATE": "/path/general.pem",
        },
        prefix,
    )
    with patch.dict(os.environ, env, clear=True):
        result = picotel._ssl_context(signal)

    assert result is sentinel
    mock_create.assert_called_once_with(cafile="/path/signal.pem")


def test_ssl_context_signal_specific_does_not_apply_to_other_signal():
    """A per-signal CA must not leak into the other signal's context.

    With no general OTEL_EXPORTER_OTLP_CERTIFICATE configured, setting only
    the traces-specific cert must leave _ssl_context("logs") at None, and
    vice versa. Prefix behavior is covered by
    test_ssl_context_signal_specific_certificate_wins; this test focuses on
    cross-signal isolation.
    """
    with patch.dict(
        os.environ,
        {"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": "/traces.pem"},
        clear=True,
    ):
        assert picotel._ssl_context("logs") is None
    with patch.dict(
        os.environ,
        {"OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE": "/logs.pem"},
        clear=True,
    ):
        assert picotel._ssl_context("traces") is None


def test_send_spans_uses_traces_specific_certificate(monkeypatch):
    """send_spans threads "traces" down to _ssl_context → create_default_context.

    With both the traces-specific and general CA set, the cafile handed to
    ssl.create_default_context must be the traces-specific one. This catches
    a regression where send_spans passed the wrong signal (e.g. "logs") to
    _ssl_context — the general cert would be picked up instead of the
    per-signal one, breaking the send_spans → _ssl_context("traces") contract.
    """
    import ssl  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    sentinel = object()
    mock_create = Mock(return_value=sentinel)
    monkeypatch.setattr(ssl, "create_default_context", mock_create)
    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://secure:4318",
            "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": "/traces.pem",
            "OTEL_EXPORTER_OTLP_CERTIFICATE": "/general.pem",
        },
        clear=True,
    ):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        assert send_spans(None, resource, [span]) is True

    assert mock_urlopen.call_args.kwargs["context"] is sentinel
    mock_create.assert_called_once_with(cafile="/traces.pem")


def test_send_logs_uses_logs_specific_certificate(monkeypatch):
    """send_logs threads "logs" down to _ssl_context → create_default_context.

    Symmetric to test_send_spans_uses_traces_specific_certificate: with both
    the logs-specific and general CA set, the cafile handed to
    ssl.create_default_context must be the logs-specific one. Catches a
    regression where send_logs passed the wrong signal (e.g. "traces").
    """
    import ssl  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import LogRecord  # noqa: PLC0415

    sentinel = object()
    mock_create = Mock(return_value=sentinel)
    monkeypatch.setattr(ssl, "create_default_context", mock_create)
    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://secure:4318",
            "OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE": "/logs.pem",
            "OTEL_EXPORTER_OTLP_CERTIFICATE": "/general.pem",
        },
        clear=True,
    ):
        resource = Resource({"service.name": "test"})
        log = LogRecord(body="test log")

        assert send_logs(None, resource, [log]) is True

    assert mock_urlopen.call_args.kwargs["context"] is sentinel
    mock_create.assert_called_once_with(cafile="/logs.pem")


# ---------------------------------------------------------------------------
# TLS insecure skip-verify (PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected_skip"),
    [
        ("true", True),
        ("TRUE", True),
        ("True", True),
        ("1", True),
        ("false", False),
        ("0", False),
        ("", False),
        # Unrecognised values fall through to the CA branch; with no CA
        # configured the helper returns None.
        ("yes", False),
    ],
)
def test_ssl_context_skip_verify_truthy_values(value, expected_skip):
    """PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY honours only the documented truthy set.

    Truthy values ("true"/"TRUE"/"True"/"1") yield an unverified SSLContext
    with both certificate and hostname verification disabled, so operators
    can hit self-signed TLS endpoints on trusted networks without
    configuring a CA. Falsy or unrecognised values fall through to the CA
    branch; with no CA configured the helper returns None — verifying the
    skip-verify short-circuit is not accidentally too lenient.
    """
    import ssl  # noqa: PLC0415

    with patch.dict(
        os.environ,
        {"PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY": value},
        clear=True,
    ):
        ctx = picotel._ssl_context()

    if expected_skip:
        assert ctx is not None
        assert ctx.verify_mode == ssl.CERT_NONE
        assert ctx.check_hostname is False
    else:
        # Falsy value falls through to the CA branch; with no CA configured
        # the helper returns None.
        assert ctx is None


@PREFIXES
def test_ssl_context_skip_verify_wins_over_ca(prefix, monkeypatch):
    """Skip-verify short-circuits before any CA lookup.

    When both OTEL_EXPORTER_OTLP_CERTIFICATE and the skip-verify var are
    set, the returned context has verification fully disabled and
    ``ssl.create_default_context`` is never called.
    """
    import ssl  # noqa: PLC0415

    mock_create = Mock()
    monkeypatch.setattr(ssl, "create_default_context", mock_create)

    env = _prefixed({"OTEL_EXPORTER_OTLP_CERTIFICATE": "/path/to/ca.pem"}, prefix)
    env["PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY"] = "true"
    with patch.dict(os.environ, env, clear=True):
        ctx = picotel._ssl_context()

    assert ctx is not None
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False
    mock_create.assert_not_called()


def test_ssl_context_skip_verify_not_remapped_by_prefix():
    """PICOTEL_PREFIX does NOT remap PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY.

    The var is already in picotel's namespace (it starts with PICOTEL_),
    so setting PICOTEL_PREFIX=FOO must NOT require a FOO_ variant.
    The raw PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY name continues to
    work, and a FOO-prefixed variant has no effect.
    """
    import ssl  # noqa: PLC0415

    # Raw PICOTEL_ name still honoured under an arbitrary PICOTEL_PREFIX.
    with patch.dict(
        os.environ,
        {
            "PICOTEL_PREFIX": "FOO",
            "PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY": "true",
        },
        clear=True,
    ):
        ctx = picotel._ssl_context()
    assert ctx is not None
    assert ctx.verify_mode == ssl.CERT_NONE

    picotel._ssl_context.cache_clear()

    # A FOO_-prefixed variant must NOT trigger skip-verify.
    with patch.dict(
        os.environ,
        {
            "PICOTEL_PREFIX": "FOO",
            "FOO_EXPORTER_OTLP_INSECURE_SKIP_VERIFY": "true",
        },
        clear=True,
    ):
        assert picotel._ssl_context() is None


def test_send_spans_passes_skip_verify_context_to_urlopen(monkeypatch):
    """send_spans hands the unverified SSL context through to urlopen.

    Closes the integration-level gap for PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY
    mirroring the cert-based integration test above: with skip-verify enabled,
    the SSLContext built by _ssl_context() must actually reach the transport
    layer. The real stdlib ssl._create_unverified_context is used (not mocked),
    so asserting verify_mode == CERT_NONE on the context seen by urlopen proves
    the whole env -> helper -> sender -> urlopen(context=...) wiring.

    Skip-verify is prefix-invariant by design (already covered by
    test_ssl_context_skip_verify_not_remapped_by_prefix), so this test does
    not parametrize over PREFIXES.
    """
    import ssl  # noqa: PLC0415
    import urllib.request  # noqa: PLC0415

    from picotel import Span, new_span_id, new_trace_id, now_ns  # noqa: PLC0415

    mock_urlopen = Mock(return_value=_mock_response)
    monkeypatch.setattr(urllib.request, "urlopen", mock_urlopen)

    with patch.dict(
        os.environ,
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://self-signed:4318",
            "PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY": "true",
        },
        clear=True,
    ):
        resource = Resource({"service.name": "test"})
        span = Span(
            trace_id=new_trace_id(),
            span_id=new_span_id(),
            name="test-span",
            start_time_ns=now_ns(),
            end_time_ns=now_ns(),
        )

        result = send_spans(None, resource, [span])

    assert result is True
    ctx = mock_urlopen.call_args.kwargs["context"]
    assert ctx is not None
    assert ctx.verify_mode == ssl.CERT_NONE
    assert ctx.check_hostname is False


# ---------------------------------------------------------------------------
# _get_sender() factory
# ---------------------------------------------------------------------------


def test_get_sender_default_is_sync():
    """_get_sender() returns _SyncSender by default (no PICOTEL_ASYNC)."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("PICOTEL_ASYNC", None)
        assert isinstance(picotel._get_sender(), picotel._SyncSender)


def test_get_sender_returns_async_when_enabled():
    """_get_sender() returns _AsyncSender when PICOTEL_ASYNC is set."""
    with patch.dict(os.environ, {"PICOTEL_ASYNC": "true"}):
        assert isinstance(picotel._get_sender(), picotel._AsyncSender)
