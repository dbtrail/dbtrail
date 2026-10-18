#!/usr/bin/env bash
# Keeps the PostgreSQL source container every workflow starts on the same
# server settings. Run from the repository root, or pass the root as the
# first argument. Exits 1 and says what diverged.
#
# Every "Start PostgreSQL source" step in ci.yml and release.yaml starts the
# container with one line of `-c` settings, and those lines must be the same.
# A test that needs a setting (track_commit_timestamp, #2453) passes in CI and
# fails the release when only ci.yml gets it, and a failed release publishes
# a tag with no binaries: v0.108.0.
set -euo pipefail

root="${1:-.}"
status=0

fail() {
  echo "postgres source lockstep: $1" >&2
  status=1
}

# settings <file>: prints, per "Start PostgreSQL source" step, the first line
# of `-c` settings within the step's next 12 lines, trimmed.
settings() {
  awk '
    /name: Start PostgreSQL source/ { left = 12; next }
    left > 0 {
      left--
      if ($0 ~ /^[[:space:]]*-c[[:space:]]/) {
        sub(/^[[:space:]]+/, ""); sub(/[[:space:]]+$/, "")
        print; left = 0
      }
    }
  ' "$1"
}

ci="$root/.github/workflows/ci.yml"
rel="$root/.github/workflows/release.yaml"
all=""
for f in "$ci" "$rel"; do
  if [ ! -f "$f" ]; then
    fail "$(basename "$f") is missing"
    continue
  fi
  s="$(settings "$f")"
  [ -n "$s" ] || fail "$(basename "$f") has no 'Start PostgreSQL source' step with a '-c' settings line"
  while IFS= read -r line; do
    [ -n "$line" ] && all="$all$(basename "$f"): $line"$'\n'
  done <<< "$s"
done

distinct="$(printf '%s' "$all" | sed -E 's/^[^:]+: //' | sort -u | grep -c . || true)"
if [ "$distinct" -gt 1 ]; then
  fail "the PostgreSQL source settings differ between steps:"$'\n'"$all"
fi

exit "$status"
