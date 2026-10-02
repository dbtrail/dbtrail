#!/usr/bin/env bash
# build-mcpb.sh — package a bintrail-mcp binary as an MCP Bundle (.mcpb).
#
# An .mcpb is a zip archive with a manifest.json that Claude Desktop installs
# on double-click (spec: https://github.com/modelcontextprotocol/mcpb). The
# bundle runs the binary in bridge mode:
#   bintrail-mcp --connect <console_url> --token <token>
#
# Invoked by GoReleaser as a per-artifact post-build hook on the bintrail-mcp
# build (see build/.goreleaser.yaml), and usable standalone:
#
#   scripts/build-mcpb.sh <binary-path> <goos> <goarch> <version>
#
# Output: dist/mcpb/dbtrail-<goos>-<goarch>.mcpb (validated before exiting).
set -euo pipefail

if [ $# -ne 4 ]; then
    echo "usage: $0 <binary-path> <goos> <goarch> <version>" >&2
    exit 2
fi

BINARY="$1"
GOOS="$2"
GOARCH="$3"
VERSION="$4"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMPLATE="$REPO_ROOT/build/packaging/mcpb/manifest.template.json"
OUT_DIR="$REPO_ROOT/dist/mcpb"
OUT="$OUT_DIR/dbtrail-$GOOS-$GOARCH.mcpb"

[ -f "$BINARY" ] || { echo "build-mcpb: binary not found: $BINARY" >&2; exit 1; }
[ -f "$TEMPLATE" ] || { echo "build-mcpb: manifest template not found: $TEMPLATE" >&2; exit 1; }

# Map GOOS to the mcpb platform identifier (node process.platform names).
case "$GOOS" in
    linux)   PLATFORM=linux ;;
    darwin)  PLATFORM=darwin ;;
    windows) PLATFORM=win32 ;;
    *) echo "build-mcpb: unsupported GOOS: $GOOS" >&2; exit 1 ;;
esac

# The manifest version must be bare semver (no leading v).
VERSION="${VERSION#v}"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "$STAGE/server"
ENTRY="bintrail-mcp"
[ "$GOOS" = "windows" ] && ENTRY="bintrail-mcp.exe"
cp "$BINARY" "$STAGE/server/$ENTRY"
chmod 0755 "$STAGE/server/$ENTRY"

sed -e "s/__MCPB_VERSION__/$VERSION/" \
    -e "s/__MCPB_PLATFORM__/$PLATFORM/" \
    -e "s/__MCPB_ENTRY__/$ENTRY/" \
    "$TEMPLATE" > "$STAGE/manifest.json"

mkdir -p "$OUT_DIR"
rm -f "$OUT"
(cd "$STAGE" && zip -q -r -X "$OUT" manifest.json server)

"$REPO_ROOT/scripts/validate-mcpb.sh" "$OUT"
echo "build-mcpb: wrote $OUT"
