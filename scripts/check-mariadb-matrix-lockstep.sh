#!/usr/bin/env bash
# Keeps the MariaDB versions CI tests and the versions a release tests the
# same. Run from the repository root, or pass the root as the first argument.
# Exits 1 and says what diverged.
#
# 1. ci.yml and release.yaml each carry exactly one `mariadb: [...]` matrix
#    line, not empty, and the two lines list the same versions in the same
#    order. What the release runs is what CI proves.
# 2. The oldest version in that list is the minimum doctor warns below
#    (internal/doctor/mariadb_version.go), so raising one without the other
#    fails here.
set -euo pipefail

root="${1:-.}"
status=0

fail() {
  echo "mariadb matrix lockstep: $1" >&2
  status=1
}

# matrix <file>: prints the versions of the one `mariadb: [...]` line, one per
# line. Prints nothing when the file has zero lines or more than one.
matrix() {
  local lines
  lines="$(grep -E '^[[:space:]]*mariadb:[[:space:]]*\[' "$1" || true)"
  [ "$(printf '%s' "$lines" | grep -c . || true)" = 1 ] || return 0
  printf '%s\n' "$lines" | sed -E 's/^[^[]*\[//; s/\].*$//' | tr ',' '\n' \
    | tr -d ' "'"'" | grep . || true
}

# flat <lines>: the same values on one line, for messages.
flat() { printf '%s\n' "$1" | paste -sd' ' -; }

ci="$root/.github/workflows/ci.yml"
rel="$root/.github/workflows/release.yaml"
for f in "$ci" "$rel"; do
  [ -f "$f" ] || fail "$(basename "$f") is missing"
done

ci_m=""
rel_m=""
[ -f "$ci" ] && ci_m="$(matrix "$ci")"
[ -f "$rel" ] && rel_m="$(matrix "$rel")"
[ -n "$ci_m" ] || fail "ci.yml needs exactly one non-empty 'mariadb: [...]' matrix line"
[ -n "$rel_m" ] || fail "release.yaml needs exactly one non-empty 'mariadb: [...]' matrix line"
if [ -n "$ci_m" ] && [ -n "$rel_m" ] && [ "$ci_m" != "$rel_m" ]; then
  fail "versions differ: ci.yml tests [$(flat "$ci_m")], release.yaml tests [$(flat "$rel_m")]"
fi

gofile="$root/internal/doctor/mariadb_version.go"
major="$(sed -nE 's/^[[:space:]]*mariaDBMinMajor[[:space:]]*=[[:space:]]*([0-9]+)[[:space:]]*$/\1/p' "$gofile" 2>/dev/null || true)"
minor="$(sed -nE 's/^[[:space:]]*mariaDBMinMinor[[:space:]]*=[[:space:]]*([0-9]+)[[:space:]]*$/\1/p' "$gofile" 2>/dev/null || true)"
if [ "$(printf '%s' "$major" | grep -c . || true)" != 1 ] || [ "$(printf '%s' "$minor" | grep -c . || true)" != 1 ]; then
  fail "could not read mariaDBMinMajor/mariaDBMinMinor from internal/doctor/mariadb_version.go"
elif [ -n "$ci_m" ]; then
  lowest="$(printf '%s\n' "$ci_m" | sort -t. -k1,1n -k2,2n | head -n 1)"
  [ "$lowest" = "$major.$minor" ] \
    || fail "the oldest version CI tests is $lowest, but doctor's minimum is $major.$minor"
fi

[ "$status" = 0 ] && echo "mariadb matrix lockstep: ok ($(flat "$ci_m"))"
exit "$status"
