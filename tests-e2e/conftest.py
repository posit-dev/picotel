"""Pytest fixtures for E2E testing with a real OpenTelemetry Collector."""

from __future__ import annotations

import json
import os
import subprocess
import time
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
    import urllib.request
    import urllib.error

    start = time.time()
    url = endpoint + "/v1/traces"
    while time.time() - start < timeout:
        try:
            # Send empty request to check if collector is up
            req = urllib.request.Request(
                url,
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=1)
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
    with open(output_file) as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(json.loads(line))
    return results
