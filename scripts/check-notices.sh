#!/usr/bin/env bash
#
# CI staleness guard for THIRD-PARTY-NOTICES.
#
# Regenerating the notices file in CI would mean a slow CGO build on every PR
# AND cross-platform nondeterminism (the per-OS duckdb binding modules differ
# between the macOS dev machine and the linux runner) → flaky false failures.
#
# Instead we key off the dependency GRAPH, which is what actually drives notice
# rot: if `go list -m all` changes, a module was added/removed/bumped and the
# notices may be stale. The hash is deterministic and platform-independent (no
# CGO, no build), so this is cheap and reliable.
#
# Exits non-zero with an actionable message when the committed marker no longer
# matches the current module graph. Fix: `make notices` and commit the result.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MARKER="$REPO_ROOT/scripts/THIRD-PARTY-NOTICES.deps.sha256"
NOTICES="$REPO_ROOT/build/THIRD-PARTY-NOTICES"

fail() { echo "ERROR: $*" >&2; exit 1; }

[ -f "$NOTICES" ] || fail "THIRD-PARTY-NOTICES is missing. Run: make notices"
[ -f "$MARKER" ]  || fail "scripts/THIRD-PARTY-NOTICES.deps.sha256 is missing. Run: make notices"

want="$(cat "$MARKER")"
have="$(go list -m all | shasum -a 256 | awk '{print $1}')"

if [ "$want" != "$have" ]; then
  echo "THIRD-PARTY-NOTICES is stale: the dependency graph has changed." >&2
  echo "  committed graph hash: $want" >&2
  echo "  current   graph hash: $have" >&2
  echo "" >&2
  echo "A dependency was added, removed, or bumped. Regenerate the notices:" >&2
  echo "  make notices" >&2
  echo "and commit build/THIRD-PARTY-NOTICES + scripts/THIRD-PARTY-NOTICES.deps.sha256." >&2
  exit 1
fi

# Content pin for the manually-maintained OFL-1.1 web-fonts section (#1360).
# The graph hash above cannot see it — the vendored console fonts are not Go
# modules — and a regeneration reassembles the file from
# scripts/notices-header.txt, so if that section were ever trimmed the marker
# would stay valid while every artifact quietly stopped carrying the license
# text OFL-1.1 §2 requires. Pin the section heading and each family's
# copyright line in the ASSEMBLED file.
while IFS= read -r needle; do
  grep -qF "$needle" "$NOTICES" || fail "THIRD-PARTY-NOTICES lost its OFL-1.1 web-fonts section (missing: \"$needle\").
Restore the section in scripts/notices-header.txt and run: make notices"
done <<'NEEDLES'
SIL OPEN FONT LICENSE
Bricolage Grotesque
Geist
IBM Plex Mono
Reserved Font Name "Plex"
NEEDLES

# Note: this only confirms the dependency GRAPH is unchanged since the notices
# were generated; it does not re-hash the THIRD-PARTY-NOTICES body, so a manual
# corruption of that file with an intact marker is not caught here (beyond the
# OFL needles pinned above).
echo "dependency graph unchanged since THIRD-PARTY-NOTICES was generated."
echo "OFL-1.1 web-fonts section present."
