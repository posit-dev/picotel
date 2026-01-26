# E2E Tests

End-to-end tests that validate picotel against a real OpenTelemetry Collector.

## Setup

Download the collector binary:

```bash
./tests-e2e/bootstrap.sh
```

This downloads `otelcol-contrib` to `tests-e2e/infra/otelcol`. The binary is gitignored.

## Running Tests

```bash
pytest tests-e2e/ -v
```

## How It Works

1. The pytest fixtures in `conftest.py` start a real otelcol process before each test
2. The collector is configured to receive OTLP/HTTP on `localhost:4318` and write to a temp file
3. Tests call `send_spans()` and verify the data appears correctly in the collector output
4. The collector is stopped after each test

## Supported Platforms

- Linux (amd64)
- macOS (amd64, arm64)
