#!/bin/sh
# dbtrail installer for PostgreSQL servers.
#
#   curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install-postgres.sh | sh
#
# The same installer as install.sh, the same stack, with its words and its
# next steps for PostgreSQL alone (DBTRAIL_FOR=postgres). The installer itself
# comes from main; its knobs (DBTRAIL_DIR, DBTRAIL_PORT, DBTRAIL_REF for the
# stack's version, ...) are set before the pipe, as for install.sh.
#
# DBTRAIL_INSTALLER (optional): a local install.sh to run instead of
# downloading it, for a checkout or a test.

set -eu

# Everything runs from main(), called on the last line, so a download of
# this file cut short by the pipe runs nothing at all.
main() {
  URL="https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh"
  DBTRAIL_FOR=postgres
  export DBTRAIL_FOR

  if [ -n "${DBTRAIL_INSTALLER:-}" ]; then
    is_installer "$DBTRAIL_INSTALLER" || fail "$DBTRAIL_INSTALLER is not the DBTrail installer."
    exec sh "$DBTRAIL_INSTALLER"
  fi

  tmp=$(mktemp "${TMPDIR:-/tmp}/dbtrail-install.XXXXXX") || fail "could not make a temporary file for the installer."
  trap 'rm -f "$tmp"' EXIT
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$URL" -o "$tmp" || fail "could not download $URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$tmp" "$URL" || fail "could not download $URL"
  else
    fail "neither curl nor wget is installed; one of them is needed to download the installer."
  fi
  # A proxy or a captive portal can answer with a page and a 200: what came
  # back must be the installer before it runs.
  is_installer "$tmp" || fail "what $URL sent back is not the DBTrail installer (a proxy or a sign-in page in the way?)."
  sh "$tmp"
}

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# is_installer FILE: not empty, and a POSIX sh script that is install.sh.
is_installer() {
  [ -s "$1" ] && head -n 1 "$1" | grep -q '^#!/bin/sh' && grep -q 'DBTRAIL_FOR' "$1"
}

main "$@"
