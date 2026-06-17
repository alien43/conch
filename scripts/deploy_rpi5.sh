#!/usr/bin/env bash

set -e

echo "============================================="
echo "  Deploying Conch to rpi5 (Raspberry Pi OS)  "
echo "============================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONCH_DIR="$PROJECT_ROOT/tools/conch"

echo "[Conch] Building statically-linked binary for linux/arm64..."
mkdir -p "$CONCH_DIR/bin"
(
  cd "$CONCH_DIR"
  env GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/conch cmd/conch/main.go
)

echo "[Conch] Copying conch binary to rpi5 (10.0.1.4)..."
scp "$CONCH_DIR/bin/conch" andrey@10.0.1.4:/tmp/conch

echo "[Conch] Moving binary to /usr/local/bin/conch and setting executable..."
ssh andrey@10.0.1.4 "sudo mv /tmp/conch /usr/local/bin/conch && sudo chmod +x /usr/local/bin/conch"

echo "[Conch] Verifying conch deployment on rpi5..."
VERSION=$(ssh andrey@10.0.1.4 "/usr/local/bin/conch version")
echo "[Conch] Success! Conch version on rpi5 is: $VERSION"

echo "============================================="
