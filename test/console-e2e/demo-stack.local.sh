#!/usr/bin/env bash
# Orchestrates the console frontend E2E: build the binaries, point them at the
# shared test MySQL, launch a source-less `watch` daemon, seed a monitored
# source whose per-source index is intentionally NOT provisioned (the
# lifecycle state that exercises the regression guards), then drive headless
# Chrome (console_e2e.mjs). Tears everything down on exit.
#
# The boot index ($IDX_DB) is additionally provisioned as a REAL read fixture
# (#970/#686/#619): `bintrail init` creates the production schema, seeded row
# events + a cascade fixture land in real hourly partitions, and a baseline
# snapshot is produced by the real `bintrail baseline` converter over a
# hand-written mydumper-format dump. The scenarios reach it through the
# "byo-idx" registry server; the daemon-level --baseline-dir makes that server
# reconstruct-capable (Time-travel).
#
# Usage:  test/console-e2e/run.sh
# Env:
#   BINTRAIL_TEST_DSN   base DSN, default root:testroot@tcp(127.0.0.1:13306)
#   MYSQL_CONTAINER     docker container running test MySQL, default bintrail-test-mysql
#   CONSOLE_BIN         path to a prebuilt bintrail-console (else it is built)
#   PW_CHANNEL          playwright browser channel (e.g. "chrome"); default bundled chromium
#   E2E_FLASHBACK_PORT  port for the daemon's embedded time-travel SQL port, default 13308
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

BASE_DSN="${BINTRAIL_TEST_DSN:-root:testroot@tcp(127.0.0.1:13306)}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-bintrail-test-mysql}"
PORT="${E2E_PORT:-8090}"
# The daemon's embedded time-travel SQL port (#996), opened so scenario 17g
# photographs the Connect page's SQL client panel in its enabled shape (#1446).
FLASHBACK_PORT="${E2E_FLASHBACK_PORT:-13308}"
TOKEN="${E2E_TOKEN:-e2e-console-token}"
IDX_DB="bintrail_e2e_idx"
ARC_DB="bintrail_e2e_arc"
FIX_SCHEMA="e2eshop"
SERVERS_FILE="$(mktemp -t console-e2e-servers.XXXXXX.yaml)"
DUMP_DIR="$(mktemp -d -t console-e2e-dump.XXXXXX)"
BASELINE_ROOT="$(mktemp -d -t console-e2e-baseline.XXXXXX)"
ARC_DIR="$(mktemp -d -t console-e2e-arc.XXXXXX)"
export E2E_ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-${RUNNER_TEMP:-/tmp}}"

mysql_exec() { docker exec -i "$MYSQL_CONTAINER" mysql -uroot -ptestroot "$@" 2>/dev/null; }

DAEMON_PID=""
cleanup() {
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null || true
  mysql_exec -e "DROP DATABASE IF EXISTS $IDX_DB;" >/dev/null 2>&1 || true
  mysql_exec -e "DROP DATABASE IF EXISTS $ARC_DB;" >/dev/null 2>&1 || true
  rm -f "$SERVERS_FILE" 2>/dev/null || true
  rm -rf "$DUMP_DIR" "$BASELINE_ROOT" "$ARC_DIR" 2>/dev/null || true
}
# stack kept alive on purpose; see the banner at the end

echo "==> build bintrail-console"
CONSOLE_BIN="${CONSOLE_BIN:-}"
if [ -z "$CONSOLE_BIN" ]; then
  CONSOLE_BIN="$ROOT/bintrail-console"
  (cd "$ROOT" && go build -o "$CONSOLE_BIN" ./cmd/bintrail-console)
fi

echo "==> build bintrail (fixture provisioning: init + baseline conversion)"
CLI_BIN="$ROOT/bintrail"
(cd "$ROOT" && go build -o "$CLI_BIN" ./cmd/bintrail)

echo "==> fresh index database $IDX_DB"
mysql_exec -e "DROP DATABASE IF EXISTS $IDX_DB; CREATE DATABASE $IDX_DB;"

# Fixture timestamps, anchored in the PREVIOUS UTC hour so the whole event
# window (baseline anchor -> reconstruct `at`) lives inside one hour whose
# partition exists — the planner classifies hours without a named partition as
# gaps (p_future doesn't count), and a gap 422s the strict Time-travel path.
# Computed with node (already required by this harness) because BSD and GNU
# `date` disagree on relative-time flags.
IFS='|' read -r P_HOUR H_HOUR H_NEXT PN_P PN_H TS_SNAP TS_INS TS_UPD TS_DEL TT_AT <<EOF
$(node -e '
const H = new Date(); H.setUTCMinutes(0, 0, 0);
const P = new Date(H.getTime() - 3600e3), N = new Date(H.getTime() + 3600e3);
const f = (d) => d.toISOString().slice(0, 19).replace("T", " ");
const pn = (d) => "p_" + d.toISOString().slice(0, 13).replace(/[-T]/g, "");
const m = (min) => f(new Date(P.getTime() + min * 60e3));
console.log([f(P), f(H), f(N), pn(P), pn(H), m(5), m(10), m(20), m(30), m(35)].join("|"));')
EOF

echo "==> provision the boot index with the production schema (bintrail init)"
"$CLI_BIN" init --index-dsn "${BASE_DSN}/${IDX_DB}" --partitions 4 >/dev/null

# init partitions forward from the current hour; the fixture events live in the
# PREVIOUS hour, so split the first partition to give that hour a named one.
mysql_exec "$IDX_DB" -e "ALTER TABLE binlog_events REORGANIZE PARTITION $PN_H INTO (
  PARTITION $PN_P VALUES LESS THAN (TO_SECONDS('$H_HOUR')),
  PARTITION $PN_H VALUES LESS THAN (TO_SECONDS('$H_NEXT')));"

echo "==> seed row events + cascade fixture into $IDX_DB"
# Mirrors internal/console's seedCascadeConsole plus an orders lifecycle. The
# UPDATE carries query_text/query_hash (a canary the frontend must NEVER see —
# they are redacted at the DTO layer) and connection_id=777 (which MUST pass
# through since #701 D1). The cascade child deletes are deliberately NOT
# indexed — that blind spot is what /api/recover's cascade synthesis repairs.
# The two schema_changes rows share ONE second, CREATE inserted first with the
# lower binlog position: the Schema changes view must list the ALTER on top
# (the #1441 tiebreak), which a detected_at-only order would not.
mysql_exec "$IDX_DB" <<SQL
INSERT INTO schema_snapshots (snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable) VALUES
  (1,'$P_HOUR','$FIX_SCHEMA','orders','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','$FIX_SCHEMA','orders','status',2,'','varchar','YES'),
  (1,'$P_HOUR','$FIX_SCHEMA','orders','email',3,'','varchar','YES'),
  (1,'$P_HOUR','$FIX_SCHEMA','parent','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','$FIX_SCHEMA','child','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','$FIX_SCHEMA','child','pid',2,'','int','YES'),
  (1,'$P_HOUR','e2estock','inventory','id',1,'PRI','int','NO'),
  (1,'$P_HOUR','e2estock','orders','id',1,'PRI','int','NO');
INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, connection_id, schema_name, table_name, event_type, pk_values, changed_columns, row_before, row_after, query_text, query_hash) VALUES
  ('binlog.000001',100,200,'$TS_INS',NULL,'$FIX_SCHEMA','orders',1,'3',NULL,NULL,'{"id":3,"status":"new","email":"c@example.com"}',NULL,NULL),
  ('binlog.000001',200,300,'$TS_UPD',777,'$FIX_SCHEMA','orders',2,'1','["status"]','{"id":1,"status":"new","email":"a@example.com"}','{"id":1,"status":"shipped","email":"a@example.com"}','UPDATE orders SET status = ''shipped'' /* e2e-canary-query-text */',SHA2('e2e-canary-query-text',256)),
  ('binlog.000001',300,400,'$TS_DEL',NULL,'$FIX_SCHEMA','orders',3,'2',NULL,'{"id":2,"status":"new","email":"b@example.com"}',NULL,NULL,NULL),
  ('binlog.000001',400,500,'$TS_INS',NULL,'$FIX_SCHEMA','child',1,'10',NULL,NULL,'{"id":10,"pid":1}',NULL,NULL),
  ('binlog.000001',500,600,'$TS_INS',NULL,'$FIX_SCHEMA','child',1,'11',NULL,NULL,'{"id":11,"pid":1}',NULL,NULL),
  ('binlog.000001',600,700,'$TS_UPD',NULL,'$FIX_SCHEMA','parent',3,'1',NULL,'{"id":1}',NULL,NULL,NULL);
INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query) VALUES
  ('$TS_INS','binlog.000001',120,'$FIX_SCHEMA','orders','CREATE TABLE','CREATE TABLE orders (id INT PRIMARY KEY) /* e2e-ddl-create */'),
  ('$TS_INS','binlog.000001',150,'$FIX_SCHEMA','orders','ALTER TABLE','ALTER TABLE orders ADD COLUMN note VARCHAR(64) /* e2e-ddl-alter */');
INSERT INTO fk_constraints (snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position, referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule) VALUES
  (1,'fk_child_parent','$FIX_SCHEMA','child','pid',1,'$FIX_SCHEMA','parent','id','CASCADE','RESTRICT');
SQL

echo "==> build a baseline snapshot (real 'bintrail baseline' over a mydumper-format dump)"
# Baseline rows 1 and 2 predate the events above (anchor binlog.000001:50 at
# TS_SNAP); row 4 is never touched by any event — Time-travel resolving it
# proves the baseline half of baseline+deltas, not just the event fold.
# The tab before Log:/Pos: is load-bearing (ParseMetadata cuts "\tLog: ").
printf 'Started dump at: %s\nSHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 50\nFinished dump at: %s\n' "$TS_SNAP" "$TS_SNAP" > "$DUMP_DIR/metadata"
cat > "$DUMP_DIR/$FIX_SCHEMA.orders-schema.sql" <<'SQL'
CREATE TABLE `orders` (
  `id` int NOT NULL,
  `status` varchar(32) DEFAULT NULL,
  `email` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
SQL
cat > "$DUMP_DIR/$FIX_SCHEMA.orders.00000.sql" <<'SQL'
INSERT INTO `orders` VALUES (1,"new","a@example.com"),(2,"new","b@example.com"),(4,"new","d@example.com");
SQL
"$CLI_BIN" baseline --input "$DUMP_DIR" --output "$BASELINE_ROOT" >/dev/null

echo "==> provision the archive fixture index $ARC_DB (#1365: info note vs alert register)"
# A second index whose history is mostly ARCHIVED: six past hourly partitions,
# 8 events each, run through the REAL rotation (archive to Parquet + drop past
# retention). The live remnant fills a small Events page, so the default
# newest-first browse takes the #1353 short-circuit and the response carries
# the archive-elision NOTE (info register). Deleting the OLDEST hour's
# archive_state row afterwards manufactures a real "rotated and not archived"
# coverage gap for the warning-register state — the archives that remain
# registered stay strictly older than the live remnant, so the elision
# premise is untouched.
mysql_exec -e "DROP DATABASE IF EXISTS $ARC_DB; CREATE DATABASE $ARC_DB;"
"$CLI_BIN" init --index-dsn "${BASE_DSN}/${ARC_DB}" --partitions 4 >/dev/null
# The anchor hour is computed ONCE and passed to both node scripts below:
# independent `new Date()` calls could straddle an hour boundary, leaving the
# gap bounds pointing one hour off the seeded partitions — a loud flake.
ARC_EPOCH_MS="$(node -e 'const H = new Date(); H.setUTCMinutes(0, 0, 0); console.log(H.getTime());')"
ARC_EPOCH_MS="$ARC_EPOCH_MS" node -e '
const H = new Date(Number(process.env.ARC_EPOCH_MS));
const f = (d) => d.toISOString().slice(0, 19).replace("T", " ");
const pn = (d) => "p_" + d.toISOString().slice(0, 13).replace(/[-T]/g, "");
const hours = [];
for (let h = 6; h >= 1; h--) hours.push(new Date(H.getTime() - h * 3600e3));
// Give each past hour a named partition (init only partitions forward from
// the current hour): split the current-hour partition open, keeping its own
// upper bound last. Double-quoted SQL strings on purpose — this script is
// single-quoted in the shell.
const parts = hours.map((d, i) => {
  const upper = i + 1 < hours.length ? hours[i + 1] : H;
  return `PARTITION ${pn(d)} VALUES LESS THAN (TO_SECONDS("${f(upper)}"))`;
});
parts.push(`PARTITION ${pn(H)} VALUES LESS THAN (TO_SECONDS("${f(new Date(H.getTime() + 3600e3))}"))`);
console.log(`ALTER TABLE binlog_events REORGANIZE PARTITION ${pn(H)} INTO (${parts.join(", ")});`);
const values = [];
let pk = 0;
for (const hour of hours) {
  for (let i = 0; i < 8; i++) {
    const ts = f(new Date(hour.getTime() + i * 5 * 60e3));
    pk++;
    values.push(`("binlog.000001", ${1000 + pk}, ${1100 + pk}, "${ts}", "arcshop", "widgets", 1, "${pk}")`);
  }
}
console.log(`INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values) VALUES ${values.join(", ")};`);
' | mysql_exec "$ARC_DB"
"$CLI_BIN" rotate --index-dsn "${BASE_DSN}/${ARC_DB}" --retain 2h \
  --archive-dir "$ARC_DIR" --bintrail-id e2e-arc >/dev/null
# The manufactured gap: the oldest archived hour loses its registration, so a
# time-ranged read covering it reports a REAL coverage-gap warning.
mysql_exec "$ARC_DB" -e "DELETE FROM archive_state ORDER BY partition_name ASC LIMIT 1;"
IFS='|' read -r ARC_GAP_SINCE ARC_GAP_UNTIL <<EOF
$(ARC_EPOCH_MS="$ARC_EPOCH_MS" node -e '
const H = new Date(Number(process.env.ARC_EPOCH_MS));
const f = (d) => d.toISOString().slice(0, 19).replace("T", " ");
console.log([f(new Date(H.getTime() - 6 * 3600e3)), f(new Date(H.getTime() - 5 * 3600e3))].join("|"));')
EOF
# Premises, asserted loudly (a broken fixture must fail the run, not skip the
# scenario): the live remnant must fill a 5-event page plus its probe row, and
# registered archives must remain behind it.
ARC_LIVE="$(mysql_exec "$ARC_DB" -N -e "SELECT COUNT(*) FROM binlog_events;")"
ARC_ARCH="$(mysql_exec "$ARC_DB" -N -e "SELECT COUNT(*) FROM archive_state;")"
if [ "${ARC_LIVE:-0}" -lt 6 ] || [ "${ARC_ARCH:-0}" -lt 1 ]; then
  echo "arc fixture premise broken: live=$ARC_LIVE (want >=6), registered archives=$ARC_ARCH (want >=1)" >&2
  exit 1
fi

echo "==> launch source-less watch daemon on :$PORT"
BINTRAIL_CONSOLE_TOKEN="$TOKEN" BINTRAIL_CONSOLE_BASELINE_TRIGGER=1 BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 "$CONSOLE_BIN" watch \
  --index-dsn "${BASE_DSN}/${IDX_DB}" \
  --console-listen "127.0.0.1:$PORT" \
  --console-token "$TOKEN" \
  --console-servers-file "$SERVERS_FILE" \
  --baseline-dir "$BASELINE_ROOT" \
  --flashback-listen "127.0.0.1:$FLASHBACK_PORT" \
  --console-allow-setup >"$E2E_ARTIFACT_DIR/console-e2e-daemon.log" 2>&1 &
DAEMON_PID=$!

echo "==> wait for /api/healthz"
for i in $(seq 1 30); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/api/healthz" 2>/dev/null; then break; fi
  if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
    echo "daemon exited early; log:" >&2; cat "$E2E_ARTIFACT_DIR/console-e2e-daemon.log" >&2; exit 1
  fi
  sleep 1
done

echo "==> seed a monitored source (per-source index left unprovisioned)"
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"wp","source_host":"127.0.0.1","source_port":"13306","source_user":"dbtrail","source_password":"x"}' \
  "http://127.0.0.1:$PORT/api/servers" >/dev/null

echo "==> install node deps"
(cd "$HERE" && npm install --no-audit --no-fund --silent)
if [ "${PW_CHANNEL:-}" = "" ]; then
  echo "==> install playwright chromium"
  (cd "$HERE" && npx --yes playwright install chromium >/dev/null)
fi

echo "==> drive headless Chrome"
cd "$HERE"
CONSOLE_URL="http://127.0.0.1:$PORT" E2E_ARC_DB="$ARC_DB" \
  E2E_ARC_GAP_SINCE="$ARC_GAP_SINCE" E2E_ARC_GAP_UNTIL="$ARC_GAP_UNTIL" CONSOLE_TOKEN="$TOKEN" \
  E2E_FIX_SCHEMA="$FIX_SCHEMA" E2E_TT_AT="$TT_AT" E2E_BASELINE_DIR="$BASELINE_ROOT" \
  true

# The server that actually HAS backups. run.sh does not create it (the
# Playwright driver did), so the demo makes it here: its index is the boot db
# run.sh already provisioned, and the daemon-wide --baseline-dir exposes the
# snapshot to it.
echo "==> seed the server that has backups"
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"name\":\"byo-idx\",\"host\":\"127.0.0.1\",\"port\":\"13306\",\"user\":\"root\",\"password\":\"testroot\",\"dbname\":\"$IDX_DB\",\"baseline_dir\":\"$BASELINE_ROOT\"}" \
  "http://127.0.0.1:$PORT/api/servers" >/dev/null

# ── the stack stays up ────────────────────────────────────────────────────────
# Seed a second and third backup so the pager and the newest-row treatment have
# something to show, and so the two locations differ (which is what #1571 is
# about: the coverage verdict has to read both).
for age in 2 3 4 5 6 7 8; do
  NEWDIR="$BASELINE_ROOT/$(date -u -v-${age}d +%Y-%m-%dT%H-%M-%SZ 2>/dev/null || date -u -d "$age days ago" +%Y-%m-%dT%H-%M-%SZ)"
  mkdir -p "$NEWDIR/$FIX_SCHEMA"
  cp -R "$BASELINE_ROOT"/*/"$FIX_SCHEMA"/*.parquet "$NEWDIR/$FIX_SCHEMA/" 2>/dev/null || true
  touch "$NEWDIR/_SUCCESS"
done

cat <<BANNER

────────────────────────────────────────────────────────────────────────
  Console is up:  http://127.0.0.1:$PORT/?token=$TOKEN

  Pick the "byo-idx" server in the switcher, then open Backups.
  What is new:
    - "Take a copy with you": two lanes, DuckDB and MySQL
    - the backups list pages five at a time, rows open with the chevron
    - the newest row keeps its mark on hover now

  Backups live in:  $BASELINE_ROOT
  Stop it with:     kill $DAEMON_PID
────────────────────────────────────────────────────────────────────────
BANNER
wait $DAEMON_PID
