#!/usr/bin/env bash
#
# Regenerate build/THIRD-PARTY-NOTICES.
#
# Run via `make notices`. Produces a single file with two parts:
#   PART 1 (manual)    — scripts/notices-header.txt, checked in by hand. Covers
#                        the C/C++ libraries vendored inside the precompiled
#                        libduckdb static library (which go-licenses cannot see)
#                        plus the MPL-2.0 note for go-sql-driver/mysql.
#   PART 2 (generated) — `go-licenses save` over the FOUR published binary
#                        mains (bintrail, bintrail-mcp, bintrail-console,
#                        bintrail-pg), reproducing each linked Go module's
#                        LICENSE and, where
#                        present, NOTICE file.
#
# Output is deterministic (modules walked in sorted order, host-arch-only
# duckdb binding) so re-running with an unchanged dependency graph yields a
# byte-identical file. The staleness guard (scripts/check-notices.sh) keys off
# the module graph, not this file, so generating on macOS vs linux is fine.
#
# Requires: go-licenses (go install github.com/google/go-licenses@latest) and
# CGO_ENABLED=1 (DuckDB links a precompiled static C library).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OUT="$REPO_ROOT/build/THIRD-PARTY-NOTICES"
HEADER="$REPO_ROOT/scripts/notices-header.txt"
MARKER="$REPO_ROOT/scripts/THIRD-PARTY-NOTICES.deps.sha256"

MAINS=(./cmd/bintrail ./cmd/bintrail-mcp ./cmd/bintrail-console ./cmd/bintrail-pg)

if ! command -v go-licenses >/dev/null 2>&1; then
  echo "error: go-licenses not on PATH. Install it with:" >&2
  echo "  go install github.com/google/go-licenses@latest" >&2
  exit 1
fi

SAVE_DIR="$(mktemp -d)"
# mktemp -d created it; go-licenses save requires the save_path NOT to exist.
rm -rf "$SAVE_DIR"
trap 'rm -rf "$SAVE_DIR"' EXIT

echo "Running go-licenses save over: ${MAINS[*]}" >&2
# --ignore our own module so the tool does not try to vendor the repo itself.
# Warnings about non-Go (.s/.c/.h) files are expected and harmless — the Go
# module licenses are still captured.
CGO_ENABLED=1 go-licenses save "${MAINS[@]}" \
  --save_path="$SAVE_DIR" \
  --ignore github.com/dbtrail/dbtrail \
  --force

# Assemble PART 2 into a temp file first, then concatenate header + body into
# the final file at the very end — so go-licenses failing mid-run aborts before
# THIRD-PARTY-NOTICES is touched at all. (The final cat truncates-then-streams,
# so it is not itself atomic against an interruption during that last write.)
BODY="$(mktemp)"
trap 'rm -rf "$SAVE_DIR" "$BODY"' EXIT

# Each module's license files land under $SAVE_DIR/<module/import/path>/. Walk
# the directories that actually contain a license-ish file, sorted, and emit a
# "## <module path>" header followed by every LICENSE/COPYING/NOTICE file in it.
find "$SAVE_DIR" -type f \
  \( -iname 'LICENSE*' -o -iname 'COPYING*' -o -iname 'NOTICE*' \) -print0 \
  | xargs -0 -n1 dirname \
  | LC_ALL=C sort -u \
  | while IFS= read -r dir; do
      module="${dir#"$SAVE_DIR"/}"
      {
        printf '\n'
        printf '================================================================================\n'
        printf '%s\n' "$module"
        printf '================================================================================\n'
      } >>"$BODY"
      # LICENSE/COPYING first, then NOTICE, each sorted for determinism.
      for f in $(LC_ALL=C find "$dir" -maxdepth 1 -type f \
                   \( -iname 'LICENSE*' -o -iname 'COPYING*' \) | LC_ALL=C sort); do
        printf '\n' >>"$BODY"
        cat "$f" >>"$BODY"
      done
      for f in $(LC_ALL=C find "$dir" -maxdepth 1 -type f \
                   -iname 'NOTICE*' | LC_ALL=C sort); do
        printf '\n----- NOTICE -----\n\n' >>"$BODY"
        cat "$f" >>"$BODY"
      done
    done

# header + generated body → final file (single cat; not a rename).
cat "$HEADER" "$BODY" >"$OUT"

# The OFL-1.1 web-fonts section (the vendored console fonts, #1360) lives in
# the MANUAL header — go-licenses cannot see it. A regeneration must never
# publish a notices file without it; fail loudly if the header lost it.
for needle in "SIL OPEN FONT LICENSE" "Bricolage Grotesque" "Geist" "IBM Plex Mono"; do
  grep -qF "$needle" "$OUT" || {
    echo "error: assembled THIRD-PARTY-NOTICES is missing the OFL-1.1 web-fonts section (missing: \"$needle\")." >&2
    echo "Restore it in scripts/notices-header.txt before regenerating." >&2
    exit 1
  }
done

# Stamp the dependency-graph hash so the CI staleness guard can detect drift
# without re-running the (slow, CGO) go-licenses pipeline. `go list -m all` is
# deterministic and platform-independent (the module graph, not the build).
go list -m all | shasum -a 256 | awk '{print $1}' >"$MARKER"

echo "Wrote $OUT and $MARKER" >&2
