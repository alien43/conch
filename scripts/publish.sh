#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GITEA_API="https://gitea.landos.win/api/v1"
REPO="andrey/conch"
TAG="latest"

TOKEN="${GITEA_TOKEN:-}"
if [[ -z "$TOKEN" ]]; then
  echo "Error: GITEA_TOKEN not set" >&2
  exit 1
fi

echo "[conch] Building linux/amd64..."
(cd "$CONCH_DIR" && GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/conch-amd64 cmd/conch/main.go)

echo "[conch] Building linux/arm64..."
(cd "$CONCH_DIR" && GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/conch-arm64 cmd/conch/main.go)

echo "[conch] Deleting existing '$TAG' release if present..."
release_id=$(curl -sf "$GITEA_API/repos/$REPO/releases/tags/$TAG" \
  -H "Authorization: token $TOKEN" | jq -r '.id // empty')
if [[ -n "$release_id" ]]; then
  curl -sf -X DELETE "$GITEA_API/repos/$REPO/releases/$release_id" \
    -H "Authorization: token $TOKEN"
  git -C "$CONCH_DIR" push origin ":refs/tags/$TAG" 2>/dev/null || true
fi

echo "[conch] Creating '$TAG' release..."
release_id=$(curl -sf -X POST "$GITEA_API/repos/$REPO/releases" \
  -H "Authorization: token $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"tag_name\":\"$TAG\",\"name\":\"latest\",\"body\":\"$(git -C "$CONCH_DIR" log -1 --format='%h %s')\",\"draft\":false,\"prerelease\":false}" \
  | jq -r '.id')

echo "[conch] Uploading binaries..."
for arch in amd64 arm64; do
  curl -sf -X POST "$GITEA_API/repos/$REPO/releases/$release_id/assets?name=conch-$arch" \
    -H "Authorization: token $TOKEN" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@$CONCH_DIR/bin/conch-$arch"
  echo "  uploaded conch-$arch"
done

echo "[conch] Published: https://gitea.landos.win/$REPO/releases/tag/$TAG"
