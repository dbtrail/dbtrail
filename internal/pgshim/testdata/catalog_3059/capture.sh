#!/usr/bin/env bash
# Collects what each psql major sends for the backslash commands the
# PostgreSQL port answers (#3059), into psql<major>.sql beside this script.
#
#   capture.sh [out-dir]        (default: this script's directory)
#
# It needs docker. It starts a PostgreSQL 14 of its own (the port says it is a
# 14, and psql writes its statements for the server version it is told),
# creates the tables consoleapp's catalog test creates, and runs every
# command with `psql -E` from each postgres:<major> image, which echoes the
# statements psql sends. The container is removed at the end.
#
# The files are fixtures of internal/pgshim's tests: each statement in them
# must be recognized. Run this again when a new psql major comes out, and add
# its number to MAJORS.
set -euo pipefail

MAJORS="${MAJORS:-13 14 15 16 17 18}"
OUT="${1:-$(cd "$(dirname "$0")" && pwd)}"
NAME="catalog3059-pg14"
PORT="${PORT:-15514}"

# One command per line. What the port's documentation promises (\l, \dn, \dt
# with and without a pattern, \d, \d <table>, \d+ <table>) and what
# consoleapp's TestIntegrationPGRouterCatalog runs. Not \d with a pattern that
# matches an index too (\d c2769*): PostgreSQL then describes the index,
# which the port refuses by design.
COMMANDS='\l
\dn
\dn c2769*
\dt
\dt c2769*
\dt c2769s.*
\dt *.*
\d
\d c2769_orders
\d c2769s.c2769_items
\d c2769_text
\d+ c2769_orders
\d+ c2769s.c2769_items'

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup
docker run -d --name "$NAME" -e POSTGRES_PASSWORD=testpg -e POSTGRES_DB=pgtest -p "127.0.0.1:$PORT:5432" postgres:14 >/dev/null
for _ in $(seq 60); do
  if docker exec "$NAME" psql -U postgres -d pgtest -XAtc 'select 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec -i "$NAME" psql -U postgres -d pgtest -X -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE SCHEMA c2769s;
CREATE TABLE c2769_orders (id int PRIMARY KEY, note text, amount numeric(10,2), created timestamptz, d date, code text NOT NULL DEFAULT 'x', big bigint, flags bool[], u uuid, ok boolean, r real);
CREATE TABLE c2769s.c2769_items (sku text, line int, qty int NOT NULL, PRIMARY KEY (sku, line));
CREATE TABLE c2769_text (id int PRIMARY KEY, vc varchar(20), doc jsonb, iv interval);
SQL

for major in $MAJORS; do
  image="postgres:$major"
  run() { docker run --rm --network host -e PGPASSWORD=testpg "$image" psql -X -h 127.0.0.1 -p "$PORT" -U postgres -d pgtest "$@"; }
  version="$(run --version)"
  file="$OUT/psql$major.sql"
  {
    echo "-- What $version sends to a PostgreSQL 14 for each backslash command"
    echo "-- (psql -E, collected by capture.sh), one statement per block, blocks"
    echo "-- separated by a line of ====. The first line of a block is the command."
  } >"$file"
  while IFS= read -r c; do
    run -E -c "$c" 2>&1 | CMD="$c" awk '
      /^\/?\*+ QUERY \*+\/?$/ { inq = 1; print "===="; print "-- " ENVIRON["CMD"]; next }
      inq && /^\/?\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*\*+\/?$/ { inq = 0; next }
      inq { print }
    ' >>"$file"
  done <<<"$COMMANDS"
  echo "$file: $(grep -c '^====$' "$file") statements, $version"
done
