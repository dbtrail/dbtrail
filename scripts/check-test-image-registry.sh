#!/usr/bin/env bash
# Keeps the test images of CI, of a release and of the managed-PostgreSQL
# smoke test off Docker Hub. Run from the repository root, or pass the root
# as the first argument. Exits 1 and says what it found.
#
# Docker Hub limits how many images an address or an account may pull, and
# on 2026-10-10 that limit failed 20 of 26 jobs, three runs in a row (#2299).
# The workflows pull from a mirror instead, named once per file:
#
#   env:
#     TEST_IMAGES: mirror.gcr.io
#
# 1. ci.yml, release.yaml and managed-pg-smoke.yml each set TEST_IMAGES
#    exactly once, to the same registry host, and not to Docker Hub.
# 2. In those files every `docker run` and `docker pull` names its image
#    under "${TEST_IMAGES}/". A bare `mysql:8.4` is a pull from Docker Hub,
#    and so is the probe container a readiness loop starts.
# 3. None of them logs in to Docker Hub: with no pull from it left, a login
#    step is one that can only fail.
set -euo pipefail

root="${1:-.}"
status=0

fail() {
  echo "test image registry: $1" >&2
  status=1
}

# commands <file>: the file's `docker run` / `docker pull` commands, one per
# line as "<line number>: <command>", with a command continued over several
# lines (trailing backslash) joined into one. Comment lines are left out.
commands() {
  awk '
    function flush() {
      if (cmd ~ /docker[[:space:]]+(run|pull)([[:space:]]|$)/) print start ": " cmd
      cmd = ""
    }
    {
      line = $0
      sub(/^[[:space:]]+/, "", line)
      if (line ~ /^#/) next
      if (cmd == "") start = NR
      continued = (line ~ /\\$/)
      sub(/[[:space:]]*\\$/, "", line)
      cmd = (cmd == "" ? line : cmd " " line)
      if (!continued) flush()
    }
    END { if (cmd != "") flush() }
  ' "$1"
}

hosts=""
for name in ci.yml release.yaml managed-pg-smoke.yml; do
  f="$root/.github/workflows/$name"
  if [ ! -f "$f" ]; then
    fail "$name is missing"
    continue
  fi

  set_lines="$(grep -E '^[[:space:]]*TEST_IMAGES:' "$f" || true)"
  if [ "$(printf '%s' "$set_lines" | grep -c . || true)" != 1 ]; then
    fail "$name must set TEST_IMAGES exactly once (the registry its test images come from)"
  else
    host="$(printf '%s\n' "$set_lines" | sed -E 's/^[^:]*:[[:space:]]*//; s/[[:space:]]+#.*$//; s/[[:space:]]*$//' | tr -d "\"'" | tr '[:upper:]' '[:lower:]')"
    case "$host" in
      "") fail "$name sets TEST_IMAGES to nothing" ;;
      */* | *[[:space:]]*) fail "$name: TEST_IMAGES must be a registry host alone, got '$host'" ;;
      docker.io | index.docker.io | registry-1.docker.io | registry.hub.docker.com)
        fail "$name: TEST_IMAGES is Docker Hub ($host), the registry this check keeps the tests off" ;;
      *.*) hosts="$hosts$host"$'\n' ;;
      *) fail "$name: TEST_IMAGES '$host' is not a registry host" ;;
    esac
  fi

  while IFS= read -r c; do
    [ -n "$c" ] || continue
    case "$c" in
      *'"${TEST_IMAGES}/'*) ;;
      *) fail "$name line ${c%%:*}: this command does not take its image from \"\${TEST_IMAGES}/...\":${c#*:}" ;;
    esac
  done <<<"$(commands "$f")"

  if grep -vE '^[[:space:]]*#' "$f" | grep -E 'docker[[:space:]]+login' | grep -vE 'ghcr\.io' >/dev/null; then
    fail "$name logs in to a registry that is not ghcr.io; the test images need no account"
  fi
  if grep -vE '^[[:space:]]*#' "$f" | grep -E 'DOCKERHUB_' >/dev/null; then
    fail "$name reads a DOCKERHUB_ secret; the test images need no account"
  fi
done

distinct="$(printf '%s' "$hosts" | grep . | sort -u || true)"
if [ "$(printf '%s' "$distinct" | grep -c . || true)" -gt 1 ]; then
  fail "the workflows name different registries: $(printf '%s\n' "$distinct" | paste -sd' ' -)"
fi

exit "$status"
