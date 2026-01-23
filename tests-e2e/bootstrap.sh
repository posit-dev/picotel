#!/bin/bash
# Downloads the OpenTelemetry Collector binary for E2E testing.
set -e

VERSION="0.116.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${SCRIPT_DIR}/infra"
BINARY_PATH="${INFRA_DIR}/otelcol"

# Skip if binary already exists
if [ -f "$BINARY_PATH" ]; then
    echo "otelcol binary already exists at ${BINARY_PATH}"
    exit 0
fi

# Detect OS
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$OS" in
    linux)
        OS="linux"
        ;;
    darwin)
        OS="darwin"
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

# Detect architecture
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64)
        ARCH="amd64"
        ;;
    aarch64|arm64)
        ARCH="arm64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Build download URL
FILENAME="otelcol-contrib_${VERSION}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${VERSION}/${FILENAME}"

echo "Downloading otelcol-contrib v${VERSION} for ${OS}/${ARCH}..."
echo "URL: ${URL}"

mkdir -p "$INFRA_DIR"
cd "$INFRA_DIR"

# Download and extract
curl -fsSL "$URL" -o otelcol.tar.gz
tar -xzf otelcol.tar.gz otelcol-contrib
mv otelcol-contrib otelcol
rm otelcol.tar.gz

chmod +x "$BINARY_PATH"
echo "Successfully installed otelcol to ${BINARY_PATH}"
