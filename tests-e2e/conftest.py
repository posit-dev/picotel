"""Pytest fixtures for E2E testing with a real OpenTelemetry Collector."""

from __future__ import annotations

import json
import os
import shutil
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

TESTS_E2E_DIR = Path(__file__).parent
CONFIG_PATH = TESTS_E2E_DIR / "config" / "otelcol.yaml"
TLS_CONFIG_PATH = TESTS_E2E_DIR / "config" / "otelcol-tls.yaml"
OTELCOL_BINARY = TESTS_E2E_DIR / "infra" / "otelcol"


@pytest.fixture(scope="session")
def otelcol_binary() -> Path:
    """Return path to otelcol binary, skip if not found."""
    if not OTELCOL_BINARY.exists():
        pytest.skip(
            f"otelcol binary not found at {OTELCOL_BINARY}. "
            "Run ./tests-e2e/bootstrap.sh first."
        )
    return OTELCOL_BINARY


@pytest.fixture
def otelcol_output_file(tmp_path: Path) -> Path:
    """Create a temp file for collector output."""
    return tmp_path / "otelcol_output.json"


@pytest.fixture
def collector(
    request, otelcol_binary: Path, otelcol_output_file: Path, tmp_path: Path
):
    """Start otelcol and yield endpoint metadata; stop it on teardown.

    Opt into TLS via indirect parametrization::

        @pytest.mark.parametrize("collector", [{"tls": True}], indirect=True)
        def test_something(collector): ...

    Yields dict: ``endpoint``, ``output_file``, and (when TLS) ``ca_cert``.
    """
    params = getattr(request, "param", {}) or {}
    tls = params.get("tls", False)

    # Clear any cached environment variable parsers to avoid test contamination
    import picotel  # noqa: PLC0415

    for func in [
        picotel._parse_traceparent,
        picotel._get_endpoint,
        picotel._parse_headers,
        picotel._get_resource_from_env,
        # _ssl_context joins this list when EVO-010 lands (see picotel.py).
    ]:
        if hasattr(func, "cache_clear"):
            func.cache_clear()

    env = os.environ.copy()
    env["OUTPUT_FILE"] = str(otelcol_output_file)

    ca_cert: Path | None = None
    if tls:
        cert_path, key_path = _generate_self_signed_cert(tmp_path)
        env["TLS_CERT_FILE"] = str(cert_path)
        env["TLS_KEY_FILE"] = str(key_path)
        config_path = TLS_CONFIG_PATH
        scheme = "https"
        ca_cert = cert_path
    else:
        config_path = CONFIG_PATH
        scheme = "http"

    process = subprocess.Popen(
        [str(otelcol_binary), "--config", str(config_path)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    endpoint = f"{scheme}://localhost:4318"
    try:
        _wait_for_collector(endpoint, ca_cert=ca_cert, timeout=10)
        result = {"endpoint": endpoint, "output_file": otelcol_output_file}
        if ca_cert is not None:
            result["ca_cert"] = ca_cert
        yield result
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _wait_for_collector(
    endpoint: str, ca_cert: Path | None = None, timeout: float = 10
) -> None:
    """Wait for collector to accept connections.

    When ``ca_cert`` is given, build an SSL context that trusts that PEM —
    required for TLS endpoints with self-signed certs, otherwise the
    handshake failure would be indistinguishable from "not ready yet".
    """
    context = ssl.create_default_context(cafile=str(ca_cert)) if ca_cert else None
    start = time.time()
    url = endpoint + "/v1/traces"
    while time.time() - start < timeout:
        try:
            req = urllib.request.Request(  # noqa: S310
                url,
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=1, context=context)  # noqa: S310
            return
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    raise RuntimeError(f"Collector at {endpoint} did not become ready in {timeout}s")


def _generate_self_signed_cert(target_dir: Path) -> tuple[Path, Path]:
    """Generate a self-signed cert + key valid for localhost via openssl."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl binary not found; required for TLS e2e fixtures")

    cert_path = target_dir / "otelcol-cert.pem"
    key_path = target_dir / "otelcol-key.pem"
    subprocess.run(  # noqa: S603
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key_path), "-out", str(cert_path),
            "-days", "1", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return cert_path, key_path


def read_collector_output(output_file: Path, wait_time: float = 0.3) -> list[dict]:
    """Read and parse collector output file.

    :param Path output_file: Path to the collector output file
    :param float wait_time: Time to wait for flush before reading

    The file exporter writes JSON lines (one JSON object per line).
    Returns list of parsed JSON objects.
    """
    time.sleep(wait_time)

    if not output_file.exists():
        return []

    results = []
    with output_file.open() as f:
        for raw_line in f:
            line = raw_line.strip()
            if line:
                results.append(json.loads(line))
    return results
