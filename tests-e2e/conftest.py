"""Pytest fixtures for E2E testing with a real OpenTelemetry Collector."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

TESTS_E2E_DIR = Path(__file__).parent
CONFIG_PATH = TESTS_E2E_DIR / "config" / "otelcol.yaml"
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
def collector(otelcol_binary: Path, otelcol_output_file: Path):
    """Start otelcol process and yield, then stop it.

    Yields a dict with endpoint and output_file path.
    """
    # Clear any cached environment variable parsers to avoid test contamination
    import picotel  # noqa: PLC0415

    for func in [
        picotel._parse_traceparent,
        picotel._get_endpoint,
        picotel._parse_headers,
        picotel._get_resource_from_env,
    ]:
        if hasattr(func, "cache_clear"):
            func.cache_clear()

    env = os.environ.copy()
    env["OUTPUT_FILE"] = str(otelcol_output_file)

    process = subprocess.Popen(
        [str(otelcol_binary), "--config", str(CONFIG_PATH)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for collector to be ready by polling the OTLP endpoint
    endpoint = "http://localhost:4318"
    _wait_for_collector(endpoint, timeout=10)

    yield {"endpoint": endpoint, "output_file": otelcol_output_file}

    # Stop the collector
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _wait_for_collector(endpoint: str, timeout: float = 10) -> None:
    """Wait for collector to be ready to accept connections."""
    start = time.time()
    url = endpoint + "/v1/traces"
    while time.time() - start < timeout:
        try:
            # Send empty request to check if collector is up
            req = urllib.request.Request(  # noqa: S310
                url,
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=1)  # noqa: S310
            return
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    raise RuntimeError(f"Collector at {endpoint} did not become ready in {timeout}s")


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
