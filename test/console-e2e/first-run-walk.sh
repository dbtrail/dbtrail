#!/usr/bin/env bash
# The first-run walk (#1800): a scoreboard of a person's first hour with
# DBTrail, measured in a real browser. It starts a fresh `watch` daemon (empty
# server list, no login yet) and a stock mysql:8.4 container as the database
# to capture, then runs first_run_walk.mjs, which walks sign in -> add the
# server -> first change -> Undo -> first snapshot, plus four shorter runs,
# and compares the numbers with first_run_baseline.json.
#
# Usage:  test/console-e2e/first-run-walk.sh      (or: make console-first-run-walk)
# Env:
#   FIRST_RUN_WALK_MODE        ratchet (default: fail when a number is worse
#                              than the baseline), target (fail unless every
#                              number meets the issue's target), or
#                              write-baseline (record the measured numbers)
#   FIRST_RUN_WALK_ALLOW_SKIP  1 turns a skip (no Docker, no test MySQL, no
#                              mydumper) into exit 0 for a local run. Without
#                              it a skip exits 77: it is never a pass.
#   FIRST_RUN_WALK_SHOTS       a folder to save a screenshot of every step
#   FIRST_RUN_WALK_SKIP_UNIT   1 skips the unit tests that run first
#   FIRST_RUN_WALK_KEEP        1 keeps the scratch folder for a look afterwards
#   BINTRAIL_TEST_DSN          index server DSN, default root:testroot@tcp(127.0.0.1:13306)
#   MYSQL_CONTAINER            the test MySQL container, default bintrail-test-mysql
#   CONSOLE_BIN                a prebuilt bintrail-console (else it is built)
#   DOCKER                     the docker command, default docker
#   PW_CHANNEL                 playwright browser channel (e.g. "chrome")
#
# Everything it creates carries one random id: the source container
# frw-src-<id>, the daemon's own index database frw_boot_<id>, and the scratch
# folder. On exit it removes exactly those, plus the per-server index
# databases named in its own server list, by exact name.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
DOCKER="${DOCKER:-docker}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-bintrail-test-mysql}"
BASE_DSN="${BINTRAIL_TEST_DSN:-root:testroot@tcp(127.0.0.1:13306)}"
SOURCE_IMAGE="mysql:8.4"
export E2E_ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-${RUNNER_TEMP:-/tmp}}"

skip() {
  cat <<EOF
============================================================================
 FIRST-RUN WALK SKIPPED: $1
 This is not a pass. Nothing was measured, so nothing was compared with the
 baseline. Start what is missing and run it again.
============================================================================
EOF
  if [ "${FIRST_RUN_WALK_ALLOW_SKIP:-}" = "1" ]; then exit 0; fi
  exit 77
}

# 1. The unit tests: the scoreboard arithmetic and the in-browser counting
#    rules, on pages where the answer is known. No Docker needed.
if [ "${FIRST_RUN_WALK_SKIP_UNIT:-}" != "1" ]; then
  echo "==> node dependencies"
  (cd "$HERE" && npm install --no-audit --no-fund --silent)
  if [ -z "${PW_CHANNEL:-}" ]; then (cd "$HERE" && npx --yes playwright install chromium >/dev/null); fi
  echo "==> unit tests"
  node --test "$HERE/first_run_walk.test.mjs"
fi

# 2. What the walk needs from this machine. A missing piece skips loudly.
command -v "$DOCKER" >/dev/null 2>&1 || skip "docker is not installed ($DOCKER)"
"$DOCKER" info >/dev/null 2>&1 || skip "docker is not running"
"$DOCKER" exec "$MYSQL_CONTAINER" mysql -uroot -ptestroot -e "SELECT 1" >/dev/null 2>&1 \
  || skip "the test MySQL container $MYSQL_CONTAINER is not running (see test/console-e2e/README.md)"
command -v mydumper >/dev/null 2>&1 || skip "mydumper is not on PATH; the first snapshot runs it (1.0.3-1, as the console image ships)"

RUN_ID="$(node -e 'console.log(require("crypto").randomBytes(4).toString("hex"))')"
BOOT_DB="frw_boot_${RUN_ID}"
SRC_CONTAINER="frw-src-${RUN_ID}"
SRC_ROOT_PW="frw-root-${RUN_ID}"
SCRATCH="$(mktemp -d -t first-run-walk.XXXXXX)"
SERVERS_FILE="$SCRATCH/console-servers.yaml"
read -r SRC_PORT CONSOLE_PORT <<EOF
$(node -e '
const net = require("net");
const free = () => new Promise((r) => { const s = net.createServer(); s.listen(0, "127.0.0.1", () => { const p = s.address().port; s.close(() => r(p)); }); });
(async () => console.log((await free()) + " " + (await free())))();')
EOF

mysql_idx() { "$DOCKER" exec -i "$MYSQL_CONTAINER" mysql -uroot -ptestroot "$@" 2>/dev/null; }

DAEMON_PID=""
cleanup() {
  status=$?
  if [ -n "$DAEMON_PID" ]; then kill "$DAEMON_PID" 2>/dev/null || true; wait "$DAEMON_PID" 2>/dev/null || true; fi
  "$DOCKER" rm -f "$SRC_CONTAINER" >/dev/null 2>&1 || true
  # The per-server index databases are bintrail_idx_<entry id>, one per
  # server this daemon added: read the ids from its own server list and drop
  # exactly those names, never a pattern (other sessions share this MySQL).
  dbs="$BOOT_DB"
  if [ -f "$SERVERS_FILE" ]; then
    for id in $(sed -n 's/^[[:space:]-]*id:[[:space:]]*"\{0,1\}\([0-9a-f]\{8,64\}\)"\{0,1\}[[:space:]]*$/\1/p' "$SERVERS_FILE"); do
      dbs="$dbs bintrail_idx_$id"
    done
  fi
  for db in $dbs; do mysql_idx -e "DROP DATABASE IF EXISTS \`$db\`;" >/dev/null 2>&1 || true; done
  if [ "${FIRST_RUN_WALK_KEEP:-}" = "1" ]; then echo "scratch kept: $SCRATCH"; else rm -rf "$SCRATCH"; fi
  exit "$status"
}
trap cleanup EXIT

echo "==> build bintrail-console"
CONSOLE_BIN="${CONSOLE_BIN:-}"
if [ -z "$CONSOLE_BIN" ]; then
  CONSOLE_BIN="$SCRATCH/bintrail-console"
  (cd "$ROOT" && go build -o "$CONSOLE_BIN" ./cmd/bintrail-console)
fi

echo "==> stock $SOURCE_IMAGE as the database to capture ($SRC_CONTAINER on 127.0.0.1:$SRC_PORT)"
# No server flags at all: the "zero yellow or red" column is about a MySQL
# as it ships, so its defaults must be the image's own.
"$DOCKER" run -d --name "$SRC_CONTAINER" -e MYSQL_ROOT_PASSWORD="$SRC_ROOT_PW" \
  -p "127.0.0.1:$SRC_PORT:3306" "$SOURCE_IMAGE" >/dev/null
# The image's first start runs a temporary server with networking off, then
# restarts; three TCP answers in a row mean the real one is up.
ok=0
for _ in $(seq 1 90); do
  if "$DOCKER" exec -e MYSQL_PWD="$SRC_ROOT_PW" "$SRC_CONTAINER" mysql --protocol=TCP -h127.0.0.1 -uroot -e "SELECT 1" >/dev/null 2>&1; then
    ok=$((ok + 1)); [ "$ok" -ge 3 ] && break
  else ok=0; fi
  sleep 2
done
[ "$ok" -ge 3 ] || { echo "the source MySQL did not come up" >&2; "$DOCKER" logs "$SRC_CONTAINER" >&2 || true; exit 1; }
"$DOCKER" exec -i -e MYSQL_PWD="$SRC_ROOT_PW" "$SRC_CONTAINER" mysql --protocol=TCP -h127.0.0.1 -uroot <<'SQL'
CREATE DATABASE shop;
CREATE TABLE shop.customers (id INT PRIMARY KEY, name VARCHAR(64), email VARCHAR(128));
INSERT INTO shop.customers VALUES (1,'Ana','ana@example.com'),(2,'Bo','bo@example.com'),(3,'Cy','cy@example.com'),(4,'Di','di@example.com');
CREATE TABLE shop.orders (id INT PRIMARY KEY, customer_id INT, status VARCHAR(16), total DECIMAL(10,2));
INSERT INTO shop.orders VALUES (1,1,'new',12.00),(2,2,'new',20.00),(3,3,'shipped',7.50),(4,1,'new',3.25),(5,4,'new',9.99);
SQL

echo "==> fresh watch daemon on 127.0.0.1:$CONSOLE_PORT (no servers, no login yet)"
mkdir -p "$SCRATCH/home" "$SCRATCH/tmp" "$SCRATCH/staging" "$SCRATCH/archives"
# A clean environment and a scratch HOME: nothing from this machine's own
# DBTrail setup (a config.env, a saved password, a server list) leaks in. The
# BINTRAIL_CONSOLE_* values are the ones docker-compose.yml sets.
(cd "$SCRATCH" && exec env -i \
  HOME="$SCRATCH/home" PATH="$PATH" TMPDIR="$SCRATCH/tmp" DO_NOT_TRACK=1 \
  BINTRAIL_CONSOLE_BASELINE_TRIGGER=1 \
  BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=ftwrl \
  BINTRAIL_CONSOLE_BASELINE_STAGING="$SCRATCH/staging" \
  BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 \
  BINTRAIL_CONSOLE_ALLOW_SETUP=1 \
  "$CONSOLE_BIN" watch \
    --index-dsn "${BASE_DSN}/${BOOT_DB}" \
    --console-listen "127.0.0.1:$CONSOLE_PORT" \
    --console-servers-file "$SERVERS_FILE" \
    --console-auth-file "$SCRATCH/console-auth.yaml" \
    --console-mcp-token-file "$SCRATCH/console-mcp-token.yaml" \
    --archive-staging-dir "$SCRATCH/archives" \
    --telemetry off) >"$E2E_ARTIFACT_DIR/first-run-walk-daemon.log" 2>&1 &
DAEMON_PID=$!
for _ in $(seq 1 60); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$CONSOLE_PORT/api/healthz" 2>/dev/null; then break; fi
  if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
    echo "the daemon exited early; its log:" >&2; cat "$E2E_ARTIFACT_DIR/first-run-walk-daemon.log" >&2; exit 1
  fi
  sleep 1
done

echo "==> walk"
cd "$HERE"
CONSOLE_URL="http://127.0.0.1:$CONSOLE_PORT" \
FRW_SRC_CONTAINER="$SRC_CONTAINER" FRW_SRC_ROOT_PASSWORD="$SRC_ROOT_PW" FRW_SRC_PORT="$SRC_PORT" \
FRW_SCRATCH="$SCRATCH" FRW_REPO_ROOT="$ROOT" DOCKER="$DOCKER" \
  node first_run_walk.mjs
