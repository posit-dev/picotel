# Copyright (C) 2026 by Posit Software, PBC.

"""E2E test for HTTPS signal submission against a TLS-enabled collector.

Probe slice for GitHub issue #11 ("Add support for HTTPS signals"):
drives picotel's ``send_spans`` against a real otelcol instance with a
TLS-enabled OTLP receiver fronted by a self-signed cert, and proves that
the client trusts the server when ``OTEL_EXPORTER_OTLP_CERTIFICATE``
points at the CA PEM.

The full graduation plan lives as evolution markers placed at the code
boundaries where each change belongs; run ``probedev list`` to enumerate.
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

# TODO(EVO-080): Document the new TLS env vars in README.md — add a
#     "HTTPS / TLS" subsection under "Environment Variables" covering
#     OTEL_EXPORTER_OTLP_CERTIFICATE (and signal-specific variants),
#     PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY, mTLS vars, precedence
#     rules, and PICOTEL_PREFIX interaction.
# TODO(EVO-090): Add an e2e test that sends a LogRecord over the same
#     TLS collector fixture. Trivially mirrors the span test below once
#     EVO-020 lands signal-specific CA vars.


@pytest.mark.parametrize(
    ("collector", "tls_env"),
    [
        ({"tls": True}, "ca_cert"),
        ({"tls": True}, "skip_verify"),
    ],
    indirect=["collector"],
)
def test_send_span_over_https(collector, tls_env):
    """A span sent to an HTTPS collector is accepted via either TLS escape hatch.

    Two routes to the same happy path exercise the full chain
    (env-var-driven SSLContext -> urlopen with context -> TLS handshake
    -> span delivered):

    - ``ca_cert``: OTEL_EXPORTER_OTLP_CERTIFICATE points at the self-signed
      CA PEM; the client builds a context that trusts only that cert.
    - ``skip_verify``: PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY=true
      returns an unverified context, proving the client can talk to a TLS
      server with an untrusted self-signed cert without any CA config.

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

    # Each case sets exactly the env var that activates its TLS path; the
    # ca_cert case uses the standard OTEL_ name, the skip-verify case uses
    # the picotel-specific escape hatch (no CA configured).
    tls_env_vars = (
        {"OTEL_EXPORTER_OTLP_CERTIFICATE": str(collector["ca_cert"])}
        if tls_env == "ca_cert"
        else {"PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY": "true"}
    )
    with patch.dict(os.environ, tls_env_vars):
        result = send_spans(collector["endpoint"], resource, [span])

    assert result is True

    output = read_collector_output(collector["output_file"])
    assert len(output) == 1
    received = output[0]["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(received) == 1
    assert received[0]["traceId"] == trace_id
    assert received[0]["spanId"] == span_id
    assert received[0]["name"] == "tls-span"
