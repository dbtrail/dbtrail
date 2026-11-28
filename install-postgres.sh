#!/bin/sh
# dbtrail installer for PostgreSQL servers.
#
#   curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install-postgres.sh | sh
#
# The same installer as install.sh, the same stack, with its words and its
# next steps for PostgreSQL alone (DBTRAIL_FOR=postgres). Every knob of
# install.sh works here too, set before the pipe in the same way.
#
# DBTRAIL_INSTALLER (optional): a local install.sh to run instead of
# downloading it, for a checkout or a test.

set -eu

URL="https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh"
DBTRAIL_FOR=postgres
export DBTRAIL_FOR

if [ -n "${DBTRAIL_INSTALLER:-}" ]; then
  exec sh "$DBTRAIL_INSTALLER"
fi

tmp=$(mktemp "${TMPDIR:-/tmp}/dbtrail-install.XXXXXX") || {
  printf 'ERROR: could not make a temporary file for the installer.\n' >&2; exit 1; }
trap 'rm -f "$tmp"' EXIT
if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$URL" -o "$tmp" || { printf 'ERROR: could not download %s\n' "$URL" >&2; exit 1; }
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$tmp" "$URL" || { printf 'ERROR: could not download %s\n' "$URL" >&2; exit 1; }
else
  printf 'ERROR: neither curl nor wget is installed; one of them is needed to download the installer.\n' >&2
  exit 1
fi
sh "$tmp"
