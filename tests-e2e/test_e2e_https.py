# Copyright (C) 2026 by Posit Software, PBC.

"""E2E test for HTTPS signal submission against a TLS-enabled collector.

This is the probe slice for GitHub issue #11 ("Add support for HTTPS
signals"). It drives picotel's `send_spans` against a real otelcol
instance with a TLS-enabled OTLP receiver fronted by a self-signed cert,
and proves that the client trusts the server when
`OTEL_EXPORTER_OTLP_CERTIFICATE` points at the CA PEM.

Intent ledger for this feature (see also TODO(EVO-...) markers in
`src/picotel.py` and `tests-e2e/conftest.py`):

* EVO-010  cache `_ssl_context` with @lru_cache (env-driven like the other
           env helpers) and wire `cache_clear()` into conftest.
* EVO-020  honour signal-specific overrides
           (`OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE`, `..._LOGS_CERTIFICATE`).
* EVO-030  honour `PICOTEL_PREFIX` for all new TLS env vars via `_env()`.
* EVO-040  `PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY` escape hatch, plus
           an e2e negative control proving the handshake fails without
           either a CA or skip-verify.
* EVO-050  openssl-free cert generation fallback (probe-level TODO in
           conftest).
* EVO-060  mTLS: `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` and
           `..._CLIENT_KEY` plus signal-specific variants, and an otelcol
           config that requires client auth.
* EVO-070  unit coverage in `tests/test_https.py` mirroring
           `tests/test_env_config.py` patterns (mock urlopen, assert the
           SSL context is built and passed correctly).
* EVO-080  README "HTTPS / TLS" subsection under "Environment Variables"
           documenting the new vars, precedence, and the picotel-specific
           skip-verify flag.
* EVO-090  e2e coverage for `send_logs` over TLS (trivially mirrors the
           trace test once the signal-specific vars land in EVO-020).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from conftest import read_collector_output

from picotel import (
    Resource,
    Span,
    new_span_id,
    new_trace_id,
    now_ns,
    send_spans,
)


@pytest.mark.parametrize("collector", [{"tls": True}], indirect=True)
def test_send_span_over_https_with_ca_cert(collector):
    """A span sent to an HTTPS collector is accepted when the CA is trusted.

    This is the single probe-level happy path. It exercises the full
    chain: env-var-driven CA resolution -> SSLContext construction ->
    urlopen with context -> TLS handshake -> span delivered.

    The shared ``collector`` fixture grows TLS via indirect params rather
    than a parallel fixture; that's the whole architectural question this
    probe answers.
    """
    resource = Resource(attributes={"service.name": "e2e-tls"})
    trace_id = new_trace_id()
    span_id = new_span_id()
    start = now_ns()

    span = Span(
        trace_id=trace_id,
        span_id=span_id,
        name="tls-span",
        start_time_ns=start,
        end_time_ns=start + 1_000_000,
    )

    # The probe reads the CA path from OTEL_EXPORTER_OTLP_CERTIFICATE, the
    # standard OTEL env var. Connect will set this when it spawns picotel
    # subprocesses alongside its otelcol sidecar (see issue #11 discussion).
    with patch.dict(
        os.environ,
        {"OTEL_EXPORTER_OTLP_CERTIFICATE": str(collector["ca_cert"])},
    ):
        result = send_spans(collector["endpoint"], resource, [span])

    assert result is True

    output = read_collector_output(collector["output_file"])
    assert len(output) == 1
    received = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(received) == 1
    assert received[0]["traceId"] == trace_id
    assert received[0]["spanId"] == span_id
    assert received[0]["name"] == "tls-span"
