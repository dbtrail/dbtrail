# Time-Travel SQL Setup

This walkthrough takes you from zero to running a working time-travel query against your MySQL:

```sql
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

The query is answered by `bintrail shim`, an in-process MySQL-protocol server (a subcommand of the `bintrail` binary) that intercepts the virtual `_flashback`, `_diff`, and `_snapshot` schemas and resolves them against your DBTrail index plus any rotated archives (local directory or `s3://` prefix). ProxySQL sits in front of both your real MySQL and the shim, routing each query to the right backend. The shim only cares that the index exists and `archive_state` is current — whatever keeps `binlog_events` populated (typically `bintrail stream`).

```
┌─────────────┐     :6033       ┌──────────┐    real query     ┌────────────┐
│ your app    ├────────────────►│ ProxySQL ├──────────────────►│ MySQL      │
└─────────────┘                 │          │                   └────────────┘
                                │          │  _flashback.*     ┌────────────┐
                                │          ├──────────────────►│ bintrail   │
                                │          │  _diff.*          │ shim       │
                                │          │  _snapshot.*      │ (:3308)    │
                                └──────────┘                   └────────────┘
```

## Three ways to run time-travel SQL

There are three ways to put `AS OF` SQL in front of your data; they differ only
in *how the client connects*:

1. **Embedded in `bintrail-console watch` — one port for every monitored server
   (multi-source).** If you already run the daemon, add `--flashback-listen`
   and it serves `_flashback` / `_snapshot` / `_diff` for *every* server in the
   console, routed by the connection username. No separate `bintrail shim`
   process, no hand-built index DSN. See [the embedded port](#the-embedded-port-multi-source)
   below. Start here if you run `watch`.
2. **A dedicated terminal — point `mysql` straight at a standalone shim (no
   ProxySQL).** Simplest for a single index. The shim already speaks the MySQL
   protocol, so an analyst connects a `mysql` client directly to it and runs
   `_flashback` / `_snapshot` / `_diff` queries. Trade-off: that connection
   answers *only* time-travel queries (a normal `SELECT` against a real table
   returns `ER_NOT_SUPPORTED_YET`, 1235). Use it when a person or tool just needs
   to read historical state from one index.
3. **Transparent routing — ProxySQL in front (the rest of this guide).** Needed
   only when an application's *normal* connection must mix live queries and
   `AS OF` queries on the same endpoint. ProxySQL routes virtual-schema queries
   to the shim and everything else to your real MySQL.

All three speak the **MySQL** protocol. A **PostgreSQL** operator can instead run
single-row `AS OF` over the **PostgreSQL wire protocol** from `psql` with
`bintrail-pg flashback` — same grammar, no MySQL client required. See
[Interactive `AS OF` from `psql`](postgres.md#interactive-as-of-from-psql).

### The embedded port (multi-source)

`bintrail-console watch` already holds the time-travel engine and, through its
control plane, a resolved connection to every monitored server's *own* per-source
index (`bintrail_idx_<id>`). `--flashback-listen` exposes a MySQL-protocol port
wired to that same map, so one endpoint time-travels any server you monitor —
no `bintrail shim` container, no `--profile flashback`, no per-source DSN to
discover:

```sh
bintrail-console watch \
  --index-dsn 'root:pw@tcp(127.0.0.1:3306)/bintrail_index' \
  --console-token "$BINTRAIL_CONSOLE_TOKEN" \
  --flashback-listen 127.0.0.1:3308            # or env BINTRAIL_CONSOLE_FLASHBACK_LISTEN
```

**Routing is by username, auth is the console token.** Connect as the target
server — its registry **ID** (robust; the `X-Bintrail-Server` value shown in the
console) or its display **name** — with the console token as the password:

```sh
# the console shows each server's id/name in the switcher
mysql -h 127.0.0.1 -P 3308 -u 7f4d577430b48821 -p"$BINTRAIL_CONSOLE_TOKEN"
mysql> USE myapp;   -- optional: seeded from the server's source DSN when known
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

Servers added in the console mid-session are reachable immediately (the registry
is read live). A token is **required** — MySQL-protocol auth cannot use the
console's password store — so set `--console-token` / `BINTRAIL_CONSOLE_TOKEN`;
`watch` refuses to open the port otherwise. The default `127.0.0.1` bind keeps
it host-local; do not expose it to untrusted networks.

The console shows all of this on **Settings → Connect AI**, in the **Connect a
SQL client** panel: whether the port is on, its address, the user and password
rules, and a ready-to-copy `mysql` line for the server picked in the sidebar
(the token itself is never displayed). When the port is off, the panel names
`--flashback-listen` / `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` as the daemon
setting that opens it.

`_snapshot.*` parity: each server reads the baseline configured on its registry
entry (or the daemon's `--baseline-dir` / `--baseline-s3`), exactly as the
console's Time-travel tab does — with one edge: a server configured with *both*
a local `--baseline-dir` **and** an `--baseline-s3` copy reads `_snapshot` only
from the local dir on this port. If local baselines have been pruned (retention)
while a durable S3 copy remains, use the console's Time-travel tab or a standalone
shim pointed at the S3 prefix for those tables. Single-source baseline configs —
the common case — have full parity.

### The dedicated terminal

**With the Docker Compose stack**, the shim ships as the opt-in `flashback`
profile. It serves the boot `SOURCE_DSN` source, so set `SOURCE_DSN` in `.env`
and bring it up *with* the full stack (`up -d`, not `up -d shim`, so the
streaming `bintrail` service comes along — see [docker.md](./docker.md)):

```sh
SHIM_USER=analyst SHIM_PASSWORD='pick-a-strong-one' \
  docker compose --profile flashback up -d
```

**Standalone** (the shim is a subcommand of the core `bintrail` binary), run it
against your index — no ProxySQL, and the source MySQL need not even be
reachable (the shim reads the index). `init-shim` reads `BINTRAIL_SOURCE_DSN`
and `BINTRAIL_SERVER_ID` from the environment (or `.bintrail.env`):

```sh
export BINTRAIL_SOURCE_DSN='user:pass@tcp(your-db:3306)/yourdb' BINTRAIL_SERVER_ID=prod-1
bintrail init-shim --out shim.yaml          # then fill in mysql_user + mysql_password
bintrail shim --shim-config shim.yaml \
  --index-dsn 'user:pass@tcp(127.0.0.1:3306)/bintrail_index'
```

Either way, connect a plain `mysql` client to the shim's port (default `3308`)
and query the virtual schemas:

```sh
mysql -h 127.0.0.1 -P 3308 -u analyst -p
mysql> USE myapp;
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
mysql> SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';   -- full table (needs a baseline)
```

`_snapshot.*` (the complete table as it was) requires a baseline configured with
`--baseline-dir` / `--baseline-s3` (or `BASELINE_DIR` on the compose profile);
without one, `_flashback.*` returns only rows with binlog activity in the
retained window. The statement shapes and their semantics are identical on both
paths — see **Step 6 — Run a time-travel query** below.

---

The ProxySQL walkthrough below takes about 10 minutes on a fresh Ubuntu 22.04 or Amazon Linux 2023 host that already has a populated DBTrail index.

---

## Prerequisites

Before starting, you need:

- **A populated DBTrail index.** Some process is keeping `binlog_events` current — typically `bintrail stream`. If rotated hours have been archived, `archive_state` points at the local directory or `s3://` prefix where the Parquet files live. If you haven't set any of this up yet, see [`docs/streaming.md`](streaming.md) and [`docs/rotation-and-status.md`](rotation-and-status.md).
- **A `.bintrail.env` file** with `BINTRAIL_SOURCE_DSN`, `BINTRAIL_INDEX_DSN`, and `BINTRAIL_SERVER_ID` set. `bintrail config init` scaffolds one.
- **The `bintrail` binary** on the host. The shim is a subcommand — there is no second binary to download.
- **Root or `sudo` access** on the host.
- **A writable bintrail config directory.** The generator commands below (`bintrail init-shim`, `bintrail proxysql-config`) and the `mysql … < proxysql-setup.sql` redirect both run as the operator (not root), so the directory holding `.bintrail.env`, `shim.yaml`, and `proxysql-setup.sql` must be owned by the operator. If you keep these in a root-owned `/etc/bintrail`, run `sudo chown $(whoami):$(whoami) /etc/bintrail` once at install time.
- **The `mysql` client** installed on the host (used to apply ProxySQL config below).
- **A MySQL user your application will use to connect through ProxySQL.** This is *not* the replication user the streamer uses — it's a regular application user that ProxySQL authenticates against. Pick a username and a strong password; you'll need both below.

---

## Step 1 — Generate `shim.yaml`

`bintrail init-shim` scaffolds the file from your existing `.bintrail.env`:

```sh
cd /etc/bintrail   # or wherever your .bintrail.env lives
bintrail init-shim --out shim.yaml
```

The generated file has one tenant block populated from your `.bintrail.env`, plus two TODO lines for the application credentials:

```yaml
listen: '127.0.0.1:3308'

tenants:
  - server_id: '...'        # from BINTRAIL_SERVER_ID
    source_dsn: '...'       # from BINTRAIL_SOURCE_DSN
    # TODO: fill in your application's MySQL credentials
    # mysql_user: app_user
    # mysql_password: '<cleartext>'
```

Edit `shim.yaml`, uncomment the two TODO lines, and paste the values:

```yaml
    mysql_user: app_user
    mysql_password: 'your-app-password'
```

`bintrail proxysql-config` recomputes the SHA1 hash ProxySQL needs from `mysql_password` automatically — you do not need to run a manual SHA1 recipe.

> **Auth note**: both `bintrail shim` and ProxySQL validate the application's password against the same `mysql_password`. The default is `mysql_native_password`; `caching_sha2_password` is opt-in via `--auth-method` (see Step 4). The shim's listen address defaults to `127.0.0.1:3308` so it is not reachable from the network. Treat `shim.yaml` as you'd treat `.bintrail.env` — it contains a password and ships at 0o600.

### Isolating tenants with `allowed_schemas`

Authentication alone does not scope what a tenant can *query*: the shim answers
every time-travel query from one shared index, so in a multi-tenant `shim.yaml`
any authenticated tenant could `USE` (or fully qualify) another tenant's schema
and read its entire indexed history — including deleted rows.

Add the optional `allowed_schemas` list to each tenant to enforce isolation:

```yml
tenants:
  - mysql_user: app_a
    mysql_password: '...'
    source_dsn: 'a:pw@tcp(db-a:3306)/app_a'
    allowed_schemas: [app_a]
  - mysql_user: app_b
    mysql_password: '...'
    source_dsn: 'b:pw@tcp(db-b:3306)/app_b'
    allowed_schemas: [app_b, app_b_audit]
```

With `allowed_schemas` set, a `USE` of — or any time-travel query resolving to
— a schema outside the list is rejected with MySQL error 1044
(`ER_DBACCESS_DENIED_ERROR`), the same code a real mysqld returns for a
database the user has no grants on. The check applies to every statement
shape, including fully qualified forms that never issue `USE`
(`SELECT /*+ DBTRAIL_AT='…' */ * FROM other_schema.t`). Schema names are
matched case-insensitively.

The field is **opt-in**: a tenant without it keeps the historical
any-schema behaviour, so existing configs are unaffected. When `shim.yaml`
has more than one tenant and any of them lacks `allowed_schemas`, the shim
logs a startup warning that cross-schema isolation is not enforced.

**Both wire front-ends enforce it.** `bintrail-pg flashback` reads the same
`shim.yaml` and applies the same allowlist to the same resolved target schema,
including a fully qualified query that never selected a database; there the
refusal is `SQLSTATE 42501` (`insufficient_privilege`) rather than MySQL 1044,
and it is a query error — the connection stays usable. It emits the same
multi-tenant startup warning.

---

## Step 2 — Install ProxySQL

ProxySQL 2.6 (LTS) is the recommended release.

### Ubuntu / Debian

```sh
sudo apt-get update
sudo apt-get install -y wget lsb-release gnupg ca-certificates
sudo install -d -m 0755 /etc/apt/keyrings
wget -qO- https://repo.proxysql.com/ProxySQL/repo_pub_key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/proxysql.gpg
echo "deb [signed-by=/etc/apt/keyrings/proxysql.gpg] https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/$(lsb_release -sc)/ ./" \
  | sudo tee /etc/apt/sources.list.d/proxysql.list
sudo apt-get update
sudo apt-get install -y proxysql=2.6.*
sudo systemctl enable --now proxysql
```

### RHEL / Amazon Linux 2023

```sh
sudo tee /etc/yum.repos.d/proxysql.repo >/dev/null <<'EOF'
[proxysql_repo]
name=ProxySQL 2.6.x repository
baseurl=https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/centos/9
gpgcheck=1
gpgkey=https://repo.proxysql.com/ProxySQL/repo_pub_key
EOF
sudo dnf install -y proxysql-2.6.*
sudo systemctl enable --now proxysql
```

After install, ProxySQL listens on:
- **`:6032`** — admin port (used to apply config). Default credentials are `admin / admin`. Change them in `/etc/proxysql.cnf` before exposing this port to anything other than localhost.
- **`:6033`** — MySQL protocol port your application connects to.

---

## Step 3 — Apply the ProxySQL config

`bintrail proxysql-config` reads `BINTRAIL_SOURCE_DSN` from `.bintrail.env` and `shim.yaml` from the previous step and emits a deterministic SQL script:

```sh
bintrail proxysql-config --out proxysql-setup.sql
```

The script tells you exactly how to apply it:

```text
ProxySQL setup SQL written to proxysql-setup.sql
Apply it: mysql -u admin -P 6032 -h <proxysql-host> < proxysql-setup.sql
```

If ProxySQL is on the same host (typical):

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

The script wraps its DML in `BEGIN`/`COMMIT` and finishes with `LOAD ... TO RUNTIME` and `SAVE ... TO DISK`, so the new routing is live immediately and survives a ProxySQL restart. **Re-running the script is safe** — it scopes its DELETEs to dbtrail-owned hostgroups (990, 991) and rule IDs (990001-990006), so it never touches operator-managed config.

Verify ProxySQL accepted the config — you should see exactly two rows, one for hostgroup 990 (your real MySQL — `hostname` reflects whatever you have in `BINTRAIL_SOURCE_DSN`) and one for hostgroup 991 (the shim, always `127.0.0.1:3308`):

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 -e \
  "SELECT hostgroup_id, hostname, port FROM runtime_mysql_servers WHERE hostgroup_id IN (990,991);"
```

---

## Step 4 — Run `bintrail shim` under systemd

Create `/etc/systemd/system/bintrail-shim.service`:

```ini
[Unit]
Description=bintrail shim - time-travel SQL backend for ProxySQL
Documentation=https://github.com/dbtrail/dbtrail/blob/main/docs/time-travel-sql.md
After=network-online.target proxysql.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/etc/bintrail
EnvironmentFile=/etc/bintrail/.bintrail.env
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml
Restart=on-failure
RestartSec=5s

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

> A copy of this unit ships at `deploy/bintrail-shim.service` in the DBTrail repo.

The unit reads `BINTRAIL_INDEX_DSN` from `/etc/bintrail/.bintrail.env` (the same file your other `bintrail` commands use) so the shim can answer queries against your index. The DSN must include the index database name (e.g. `…/bintrail_index`) — the shim refuses to start otherwise. Append `--allow-gaps` to `ExecStart` to warn-and-continue on archive failures or coverage gaps instead of returning a MySQL error to the client; the default is strict because the wire protocol has no warning channel.

**Auth method on MySQL 8.4+.** If your MySQL has `mysql_native_password` disabled (the default since 8.4), append `--auth-method=caching_sha2_password` to `ExecStart` (or `Environment=BINTRAIL_AUTH_METHOD=caching_sha2_password`):

```ini
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml --auth-method=caching_sha2_password
```

Requires ProxySQL **2.7+** between the application and the shim — the LTS 2.6 line isn't verified to negotiate SHA2 against backends, so operators on 2.6 keep the default (`mysql_native_password`). The application user used by ProxySQL must match the chosen scheme: `IDENTIFIED WITH mysql_native_password BY '<password>'` for the default path, `IDENTIFIED WITH caching_sha2_password BY '<password>'` for the opt-in. `sha256_password` is also accepted by `--auth-method` if your environment requires it. The same 2.7+ requirement applies when ProxySQL fronts an **8.4 source** directly (its `caching_sha2_password` backend), set via `proxysql-config --backend-auth-plugin caching_sha2_password`.

> **DBTrail's own connections to MySQL 8.4 need no auth flag.** The ProxySQL requirement above is only about ProxySQL negotiating `caching_sha2_password` to a backend. DBTrail's *index* connection (`--index-dsn`) and its *source replication* handshake (`bintrail stream`/`up`) both complete `caching_sha2_password` over a plaintext network on their own — with no flag, no TLS, and no ProxySQL in the path. This is what the bundled MySQL 8.4 index uses by default.

Enable and start:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now bintrail-shim
sudo systemctl status bintrail-shim
```

You should see `active (running)`. Tail the log if not:

```sh
journalctl -u bintrail-shim -f
```

The shim should report `shim listening addr=127.0.0.1:3308 tenants=N` once it has loaded `shim.yaml`.

### Resource limits

The shim is a long-running daemon shared by every forensic session, so heavy or abandoned queries are bounded by three flags (all opt-out; the defaults are safe for a typical deployment):

- **`--query-timeout`** (default `5m`, env `BINTRAIL_SHIM_QUERY_TIMEOUT`) — per-query deadline covering the index fetch, the archive/DuckDB fetch, and the wait for a full-table slot. A query that exceeds it fails with MySQL error **1317** (`ER_QUERY_INTERRUPTED`). `0` disables the deadline.
- **`--max-connections`** (default `100`, env `BINTRAIL_SHIM_MAX_CONNECTIONS`) — concurrent client connections; a connection past the cap is refused with MySQL error **1040** (`Too many connections`), exactly like a real mysqld. `0` removes the cap.
- **`--max-fulltable-queries`** (default `4`, env `BINTRAIL_SHIM_MAX_FULLTABLE_QUERIES`) — concurrent full-table reconstructions (the heaviest path — a buffered `_flashback` or `LIMIT`ed query holds up to the 100,000-row cap; an uncapped streaming `_snapshot` holds only one row plus the changed-since-baseline set). Excess full-table queries wait for a slot; a waiter that outlives `--query-timeout` fails with MySQL error **1203** (`ER_TOO_MANY_USER_CONNECTIONS`). `0` removes the cap. PK point-lookups and `_diff` are never gated.

A client that disconnects mid-query (an ORM timeout, Ctrl+C in the mysql CLI) cancels the in-flight fetch immediately — the shim stops the index/S3 work instead of finishing a resultset nobody will read.

---

## Step 5 — Point your application at ProxySQL

Change your application's MySQL connection string from the real MySQL port (`:3306`) to ProxySQL's MySQL port (`:6033`). The credentials are the `mysql_user` / `mysql_password` pair from `shim.yaml` (cleartext — same value the shim and ProxySQL both validate against).

For example, with the Go MySQL driver:

```go
// before:
db, _ := sql.Open("mysql", "app_user:your-app-password@tcp(127.0.0.1:3306)/myapp")

// after:
db, _ := sql.Open("mysql", "app_user:your-app-password@tcp(127.0.0.1:6033)/myapp")
```

Normal queries (`SELECT * FROM orders WHERE id = 1`) still go to your real MySQL, transparently. Only queries that reference `_flashback.*`, `_diff.*`, or `_snapshot.*` are routed to the shim.

---

## Step 6 — Run a time-travel query

Connect through ProxySQL:

```sh
mysql -u app_user -p -h 127.0.0.1 -P 6033 myapp
```

Six statement shapes are recognised:

```sql
-- Row state at a point in time (point-lookup, fast):
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;

-- The same, on the REAL table name — the AS OF clause must END the
-- statement (#385). Rewritten internally to _flashback (binlog-only):
SELECT * FROM orders WHERE id = 12345 AS OF '2026-05-02 10:00:00';

-- Optimizer-hint form on the real table name (ORM-friendly: survives query
-- builders that would reject AS OF syntax). `*`-only — a column list here
-- is a parse error:
SELECT /*+ DBTRAIL_AT='2026-05-02 10:00:00' */ * FROM orders WHERE id = 12345;

-- Full-table reconstruction at AS OF (no WHERE). Against _snapshot (with a
-- baseline configured) this is every row that existed at that instant;
-- against _flashback it is only rows with binlog activity in the retained
-- window (see the _snapshot vs _flashback note under Limitations):
SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00';

-- Browse the first N rows of a full-table reconstruction. LIMIT lets a
-- full-table query succeed under the row cap; results come back in the
-- merge's order (no implicit ORDER BY):
SELECT * FROM _snapshot.orders AS OF '2026-05-02 10:00:00' LIMIT 100;

-- All events for one row in a time window:
SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345;
```

> **The hint form is the only one that degrades silently.** `/*+ DBTRAIL_AT=... */`
> is 100% valid vanilla MySQL (an unknown optimizer hint raises a warning, not an
> error), so if the query never reaches the shim — the client points at MySQL's
> port instead of ProxySQL's `:6033`, rules 990001-990006 are missing (e.g.
> ProxySQL restarted without `SAVE MYSQL QUERY RULES TO DISK`), or a
> lower-`rule_id` operator rule intercepts first — MySQL executes it against the
> live table and returns **present-day data with no error**. You believe you are
> reading the past; you are reading the present. The other shapes fail loud on
> the same misroute (`_flashback.*` → `ER_BAD_DB` 1049; bare `AS OF` → 1064), so
> prefer them whenever the client can emit them — reserve the hint form for
> ORM/query-builder paths that reject `AS OF` syntax, and verify routing after
> any ProxySQL change:
>
> ```sh
> bintrail doctor --source-dsn "$SRC" --proxysql-admin 'admin:<pass>@tcp(127.0.0.1:6032)/'
> ```
>
> The check confirms all six DBTrail rules are live in
> `runtime_mysql_query_rules` (advisory `WARN` when they aren't — it never
> changes doctor's exit code).

Every quoted time literal — in `AS OF`, `DBTRAIL_AT`, and the `BETWEEN` bounds — accepts four absolute formats (`'2026-05-02 10:00:00'`, RFC 3339 `'2026-05-02T10:00:00Z'`, zone-less `'2026-05-02T10:00:00'`, and date-only `'2026-05-02'`), plus `'now'` and relative forms `'<n> seconds|minutes|hours|days ago'` (e.g. `AS OF '5 minutes ago'`), resolved against the wall clock at parse time. Larger units (weeks, months) are deliberately not parsed — spell them as days.

**All three zone-less forms (`'2026-05-02 10:00:00'`, the zone-less RFC-3339-shaped literal, and date-only) are interpreted as UTC** — only the `Z`-suffixed RFC 3339 form is unambiguous by construction. There is no per-session override: a `SET time_zone = ...` sent by the client is accepted (returned `OK`, matching a real MySQL server's handshake noise) but has **no effect** on how the shim parses `AS OF` literals — it is treated as connection setup noise and silently ignored. If your monitoring or incident timeline is in local time, convert to UTC before writing the literal, or use the unambiguous `Z`-suffixed form.

**1-second granularity.** Timestamps are compared and stored at one-second resolution; a literal with sub-second precision has no finer effect than truncating to the second.

**Server-side prepared statements are not supported.** The shim has no `COM_STMT_PREPARE` handling and returns error 1105 ("not supported now") for it. Drivers/ORMs that prepare statements by default against the shim's port — MySQL Connector/J with `useServerPrepStmts=true`, .NET's `MySqlConnector`, Perl's `DBD::mysql` with `mysql_server_prepare` — fail on the very first query unless configured to use client-side (text-protocol) statements instead.

On the `_flashback` / `_snapshot` shapes a column list may replace `*` and the optional `TIMESTAMP` keyword may follow `AS OF` (Oracle / SQL Server convention):

```sql
SELECT id, email, name FROM _flashback.users AS OF TIMESTAMP '2026-05-02 10:00:00' WHERE id = 1;
```

The column list accepts bare identifiers only; backticks, schema-qualified columns (`users.id`), and aliases (`id AS user_id`) are not yet parsed and surface as `ER_PARSE_ERROR`. Columns the row image is missing (e.g. dropped post-event) come back as `NULL` — matching MySQL's behaviour after an `ALTER TABLE DROP COLUMN`.

For a **full-table** `SELECT *` (no column list), DBTrail unions the columns present across the reconstructed rows, so a column that existed at the queried time but was **dropped afterward** still appears — with its historical values — rather than being silently hidden by the current (narrower) schema. (The one exception is the uncapped streaming `_snapshot` path described below: it fixes the column set from the table's *current* schema before the first row is sent, so a since-dropped column is not surfaced there — the shim logs a warning naming the omitted column, and adding a `LIMIT` forces the buffered path that surfaces it.)

The WHERE column must match the table's primary key. A WHERE on a non-PK column is rejected with a parser error rather than silently returning the wrong row.

Single-row `_flashback` / `_snapshot` point-lookups cut at the **transaction boundary**, not the individual event: if the row's most recent change belongs to a multi-statement transaction whose other statements commit *after* the AS OF instant, that whole transaction is excluded and the row resolves to its state *before* it — never a half-applied image that never existed at any real instant. (Full-table `AS OF` still cuts per row; see the transaction-boundary note in `query-and-recovery.md`.)

`SHOW TABLES FROM _flashback / _diff / _snapshot` returns every table in the current schema that DBTrail has schema knowledge of (the newest snapshot per table, so PostgreSQL-source indexes — which record one table per snapshot — list all their tables too). A table that was since dropped at the source still appears: its indexed history remains queryable with `AS OF`. This lets an interactive `mysql>` session explore the virtual schemas:

```sql
USE myapp;
SHOW TABLES FROM _flashback;
```

`_diff` returns the full per-PK event history within the requested window — there is no implicit row cap. If a single hot row produced thousands of events, you'll get all of them in one response; if that's too much for one query, narrow the `BETWEEN` range.

`LIMIT <n>` bounds a full-table `AS OF` resultset (`SELECT * FROM _snapshot.orders AS OF '…' LIMIT 100`). It composes with a `WHERE` clause and a column list, and it is the quickest way to *browse* a large table: a `LIMIT` at or below the row cap lets the query succeed instead of tripping the cap. Rows come back in the merge's internal order (roughly primary-key order for `_snapshot`), **not** a sorted order — add your own `ORDER BY` downstream if you need one — and a `LIMIT n` can return fewer than `n` rows when some of the first `n` were deleted by the AS OF instant. A `LIMIT` never *raises* the cap.

Full-table `_snapshot` with **no** `LIMIT`, run over a live shim (`bintrail shim`, or `bintrail-console watch --flashback-listen`), **streams** its resultset row-by-row over the wire and is **not** row-capped: the baseline flows through the merge cursor one row at a time, so peak memory is proportional to the rows *changed* since the baseline, not the table size — a multi-million-row table dumps without `ER_TOO_BIG_SELECT`. If the merge fails mid-stream, the connection receives an error packet in place of the next row (no clean end-of-result), so a failed dump is never mistaken for a complete one. The 100,000-row cap still applies to the binlog-only full-table `_flashback` path (whose fetch is buffered) and to any `LIMIT`ed query; exceeding it surfaces as `ER_TOO_BIG_SELECT` (code 1104) with a hint to add a `LIMIT`, narrow the AS OF range, or add a PK filter.

DELETE events are correctly suppressed — rows that did not exist at the AS OF instant don't appear in the resultset (same semantic as Oracle's `AS OF`). For ad-hoc filtering, joins, or aggregations, pipe the resultset to `duckdb`, `pandas`, or any tool that consumes a `SELECT *` stream — the shim deliberately stays a forensic point-lookup + full-table tool, not a SQL planner.

The shim resolves the row by replaying the relevant binlog events from your DBTrail MySQL index. If the timestamp falls outside the index's retention (because hourly partitions have been rotated to S3), the shim auto-discovers the Parquet archives via `archive_state` and merges results from both sources — same machinery `bintrail query` and `bintrail recover` already use.

---

## Troubleshooting

### `ERROR 1045: Access denied for user 'app_user'@'…'`

ProxySQL is rejecting your credentials. Confirm your app is connecting with the cleartext value of `mysql_password` from `shim.yaml`. If `shim.yaml` was edited, re-apply the ProxySQL config so the regenerated SHA1 reaches the live `mysql_users` table:

```sh
rm -f proxysql-setup.sql
bintrail proxysql-config --out proxysql-setup.sql
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

If the username comes through but the connection still fails, check `bintrail shim`'s log: it logs which usernames are in the allowlist at startup, and a connection from an unknown username is rejected.

### `_flashback.t doesn't exist` (or query goes to MySQL instead of the shim)

The query rule isn't matching. Inspect the routing:

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 \
  -e "SELECT rule_id, match_pattern, destination_hostgroup FROM runtime_mysql_query_rules WHERE rule_id BETWEEN 990001 AND 990006;"
```

You should see six rows targeting hostgroup 991 (one each for `_flashback.*`, `_diff.*`, `_snapshot.*`, the `/*+ DBTRAIL_AT=... */` hint-comment shape, `SHOW TABLES FROM` the virtual schemas, and the end-anchored bare `... AS OF '<ts>'` shape). If they're missing, re-apply `proxysql-setup.sql`. If they're present but the query still goes to MySQL, double-check that no operator rule with a smaller `rule_id` is intercepting `_flashback.*` first (ProxySQL evaluates rules in `rule_id` order). `bintrail doctor --proxysql-admin '<admin-dsn>'` runs the six-rules check for you (advisory `WARN` when any is missing or inactive).

Note this failure mode is only *visible* for the `_flashback.*`/`_diff.*`/`_snapshot.*` and bare `AS OF` shapes, which error out when misrouted to MySQL. The `/*+ DBTRAIL_AT */` hint form produces **no error at all** on the same misroute — MySQL treats the hint as unknown-and-ignorable and returns current data (see the warning under Step 6). If time-travel results ever look suspiciously like the present, check the rules above before trusting them.

### `connection refused` on the shim's port

`bintrail shim` isn't running, or it's listening on a different port than `shim.yaml`'s `listen` directive.

```sh
systemctl status bintrail-shim
ss -tlnp | grep 3308
```

If `bintrail-shim` is dead, `journalctl -u bintrail-shim -n 100` shows why. Common causes: missing or unreadable `shim.yaml`, missing `BINTRAIL_INDEX_DSN`, a `mysql_password` value that's not a valid YAML string (quote it).

### MySQL error codes the shim returns

The shim emits typed wire codes so ORMs and monitoring can distinguish *user input* errors from *server fault* errors — a 1105 spike no longer means "any time-travel query failed". Codes you may see:

- **1064 `ER_PARSE_ERROR`** — a query mentions `_flashback` / `_snapshot` / `_diff` but doesn't match any supported shape (missing `AS OF`, missing `BETWEEN`, missing `USE <db>`, unparseable timestamp). Same code MySQL itself returns for any SQL syntax error.
- **1235 `ER_NOT_SUPPORTED_YET`** — a non-virtual-schema query reached the shim (typically a direct connection to `:3308` bypassing ProxySQL). Hostgroup routing is misconfigured.
- **1526 `ER_NO_PARTITION_FOR_GIVEN_VALUE`** — two causes, distinguished by the message. Either the AS OF or BETWEEN range falls outside what this index retains (rotated out of MySQL with no archive coverage) — narrow the time range or check `archive_state` and the shim's `--allow-gaps` flag. Or a **full-table `_snapshot`** query found the configured baseline unusable (no baseline snapshot at-or-before the AS OF instant, or a primary key the baseline merge can't canonicalize) and refused rather than return a partial table — create or re-run `bintrail baseline` so a snapshot covers the AS OF, or query `_flashback` for a binlog-only view.
- **1045 `ER_ACCESS_DENIED_ERROR`** — credential mismatch (see the section above).
- **1317 `ER_QUERY_INTERRUPTED`** — the query exceeded `--query-timeout` (message names the flag), or its client disconnected / the shim shut down mid-query. Narrow the AS OF range, filter by PK, or raise the timeout.
- **1203 `ER_TOO_MANY_USER_CONNECTIONS`** — too many concurrent full-table time-travel queries; the query waited for a slot until `--query-timeout`. Retry later or raise `--max-fulltable-queries`.
- **1040 `ER_CON_COUNT_ERROR`** — the `--max-connections` cap was reached; the connection was refused before the handshake.
- **1104 `ER_TOO_BIG_SELECT`** — full-table `_flashback` / `_snapshot` returned more than 100,000 rows. Narrow the AS OF or add a `WHERE <pk> = <value>` to fall back to the point-lookup path.
- **1105 `ER_UNKNOWN_ERROR`** — real internal failure (DB timeout, archive S3 outage, build-resultset bug). This is the catch-all "the server is broken, retry" signal; persistent 1105s warrant inspecting the shim log.

### Time-travel query returns empty

Three causes produce an empty `_flashback` / `_snapshot` resultset:

1. The row had no event at-or-before the requested timestamp.
2. The latest event at-or-before the timestamp is a DELETE — the row did not exist at AS OF (Oracle `AS OF` semantic; matches the full-table path).
3. A coverage gap or archive-fetch failure under `--allow-gaps`. Without that flag the shim returns a typed MySQL error instead — `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526) for coverage gaps, `ER_UNKNOWN_ERROR` (1105) for archive-fetch failures — so on the default strict configuration the empty resultset never indicates a gap or an archive outage.

To distinguish cases 1 and 2, query `_diff` for the per-PK history: it returns every event (including the DELETE's `row_before`), so a row that was deleted produces at least one row in the diff resultset while a row that never existed produces zero. Or check the indexer is keeping up:

```sh
journalctl -u bintrail-stream -n 200
```

The DBTrail index retains the most recent hours via partition rotation; older data is in S3 (auto-discovered via `archive_state`). See [`docs/rotation-and-status.md`](rotation-and-status.md) for rotation and archive cadence.

### Operator already has users in hostgroup 990

`bintrail proxysql-config` scopes its DELETE to `mysql_users WHERE default_hostgroup = 990` — any pre-existing user in that hostgroup will be removed when the script is applied. If you have application users you want to keep separate from DBTrail-managed routing, place them in a different hostgroup before running the script. Hostgroup 990 is reserved for DBTrail; see the comment header at the top of the generated `proxysql-setup.sql` for the full list of resources the script manages.

---

## Limitations

- **Single source MySQL per shim.** The current `bintrail shim` is one-tenant-per-instance. If you have multiple source MySQLs you want time-travel SQL against, run one shim per instance with separate listen ports and separate ProxySQL hostgroups.
- **No TLS termination on the shim port.** `bintrail shim` accepts plain MySQL protocol on `127.0.0.1:3308` by default. If you need TLS between ProxySQL and the shim, terminate at ProxySQL or via an `stunnel` sidecar.
- **`_snapshot` is baseline-aware; `_flashback` is binlog-only.** Start `bintrail shim` with `--baseline-dir <dir>` or `--baseline-s3 s3://bucket/prefix/` (the snapshots produced by `bintrail baseline`) to enable it. Both the **single-row** (`WHERE <pk> = <value>`) and **full-table** (no WHERE) `_snapshot` shapes then seed row state from the baseline at-or-before the AS OF instant and apply post-snapshot binlog events on top — so a row that existed at AS OF but was never touched within the retained binlog window still resolves, and full-table `_snapshot` returns the table's complete row state at AS OF (never-touched baseline rows, rows updated/inserted after the baseline, with rows deleted after it dropped). `_flashback` deliberately stays binlog-only — its full-table form returns only rows with binlog activity in the retained window — so the two schemas have distinct, observable semantics. With no baseline source configured, `_snapshot` degrades to the binlog-only `_flashback` behaviour (a `Debug` log notes the fallback). The baseline match is supported for **integer, `YEAR`, `DECIMAL`/`NUMERIC`, string (`CHAR`/`VARCHAR`/`TEXT`/`ENUM`/`SET`), and `DATETIME`/`TIMESTAMP`/`DATE` PKs** (the shim pins the DuckDB session to UTC so temporal-PK matches resolve deterministically on any host timezone). **`FLOAT`/`DOUBLE`, `TIME`, `BIT`, `JSON`, and spatial PK types can't be matched against the baseline** (their round-trip between the stored key and the Parquet column is unverified, so the match is refused rather than risked), and neither can a table whose primary key can't be resolved from the indexed snapshot. **`BINARY`/`VARBINARY`/`BLOB` PKs are full-table only**: the full-table shape canonicalizes keys through the offline merge and handles them since [#1155](https://github.com/dbtrail/dbtrail/issues/1155), but the single-row shape has no width to pad a fixed `BINARY(n)` key to — that key is stored padded and captured stripped — so it keeps falling back to binlog-only for the whole family (a `Warn` notes it) rather than risk silently missing a row. Use `bintrail reconstruct --pk`, which reconciles both spellings, for a single binary-keyed row. The **single-row** shape still falls back to binlog-only in these cases (a `Warn` notes the unsupported PK type). The **full-table** shape does not: with a baseline source configured, it **refuses with `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526)** instead of silently returning a partial table (only rows with binlog activity in the window, indistinguishable from a complete one) — the error message points at `_flashback` for a binlog-only view. Full-table `_snapshot` also refuses with the same code when **no baseline snapshot exists at-or-before the AS OF instant**: take a baseline covering that instant (`bintrail baseline`), or query `_flashback`. For tables keyed by an unsupported PK type, or to write the reconstructed table to mydumper-format files, use the offline `bintrail reconstruct` command (its full-table mode streams the window a page at a time and keeps a per-touched-row change map, warning past `--warn-event-threshold` — see [Memory Footprint](query-and-recovery.md#memory-footprint)). Full-table `_snapshot` buffers the reconstructed table in memory and is bounded by the same row cap as `_flashback` (see the next limitation).
- **Full-table reconstruction is buffered, not streamed.** The MVP buffers up to 100,000 rows per query and surfaces overflow as `ER_TOO_BIG_SELECT` (1104). A streaming wire-protocol path (no row cap) is deferred until an operator reports the cap as a real bottleneck. PK-filtered point-lookups are unaffected.
- **`_snapshot` refuses across a TRUNCATE/DROP/RENAME.** `TRUNCATE TABLE`/`DROP TABLE`/`RENAME TABLE` and MariaDB's `CREATE OR REPLACE TABLE` emit no row events, so a baseline merge spanning one of these statements would silently resurrect pre-DDL rows as if they still existed at AS OF. Both the single-row and full-table `_snapshot` paths check `schema_changes` for such a statement between the baseline snapshot and AS OF and return `ER_UNKNOWN_ERROR` (1105) naming the DDL type and timestamp instead ([#764](https://github.com/dbtrail/dbtrail/issues/764)); re-baseline the table after the DDL to resume. `_flashback` is unaffected — it never reads a baseline.
- **No JOINs, aggregations, or non-PK WHERE filters inside the shim.** Run them outside on the resultset (`duckdb`, `pandas`, `awk`). The shim's job is to deliver correct historical row state; SQL execution against that state is the operator's tool of choice.
- **ENUM/SET labels are decoded with the snapshot in effect at each event.** Binlog row images store ENUMs as ordinals and SETs as bitmasks; the shim (and the console Time-travel / `bintrail reconstruct` surfaces) map them back to labels using the schema snapshot whose capture time most recently precedes the event — so an enum reshaped between two events renders each event under its own definition. Remaining caveats: events older than the *first* snapshot decode with that first snapshot, and a change made between an ALTER and the next snapshot decodes with the pre-ALTER definition (stream mode auto-snapshots on DDL, so that window is normally seconds). An ordinal beyond the selected definition is returned as the raw number — the forensic ground truth, also visible in `bintrail query`'s JSON output, which is deliberately left unmapped.
- **ProxySQL itself is not provisioned by DBTrail.** `bintrail proxysql-config` only writes routing rules; you install and harden ProxySQL itself (admin password, frontend TLS, monitoring) using the standard ProxySQL docs.
- **The bare `AS OF` rule (990006) has a small residual false-positive surface.** The rule is end-anchored — only statements that *finish* with `AS OF '<text>'` route to the shim, so `AS OF` inside a string literal mid-query stays on passthrough (covered by an e2e guard test against real ProxySQL). The irreducible residue: a benign statement whose **final token** is a string literal of the exact form `AS OF '<text>'` would route to the shim and fail (the shim has no passthrough). If you hit that in practice, parenthesise or reorder the predicate — or delete rule 990006 from `mysql_query_rules` and use the `_flashback.`/hint forms instead. Note ProxySQL's `$` anchor assumes the default `re_modifiers` (CASELESS, no multiline); adding `GLOBAL`/multiline modifiers to the rule weakens the anchor to end-of-line.
- **The bare `AS OF` form is `*`-only and trailing-only.** Column lists stay on the `_flashback`/`_snapshot` virtual schemas, and the AS OF clause must end the statement (an AS-OF-before-WHERE variant would forfeit the end anchor — the false-positive defense above). The bare form rewrites to `_flashback` (binlog-only); for baseline-aware lookups use `_snapshot`.
