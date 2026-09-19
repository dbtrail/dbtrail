# PostgreSQL as a source

bintrail can capture from a **PostgreSQL** server while the index database stays
MySQL. PostgreSQL capture lives in its own binary, **`bintrail-pg`**. This is a
**GA** capability: capture is type-faithful,
`REPLICA IDENTITY FULL`-enforced, replication-slot/WAL-retention-monitored, and
DDL-drift-safe, all verified end-to-end against real PostgreSQL (14–17 in CI)
and smoke-validated on managed PostgreSQL (RDS / Aurora). It still has
documented limitations (below) — notably full-table `reconstruct` /
time-travel, which stays out of scope for PostgreSQL — so read them before
pointing it at production.

**Scope:** PostgreSQL is supported as a **source** (the database you capture
changes from). The **index** — where bintrail stores the indexed events — stays
**MySQL** (one index schema for every source family). Pointing the index at
PostgreSQL is not supported.

**Nothing is installed in your PostgreSQL server.** bintrail-pg connects as an
ordinary logical-replication **client** using PostgreSQL's built-in `pgoutput`
plugin. It does **not** install an output plugin, a `CREATE EXTENSION`, an
event trigger, or any other server-side component. You create one publication
and set REPLICA IDENTITY; bintrail reads — it never writes to your source.

---

## Requirements at a glance

| On the PostgreSQL source | Required? | Notes |
|---|---|---|
| **PostgreSQL 14+** | Yes (declared) | 14/15/16/17 tested in CI. 13 may work but is EOL — best-effort only. |
| **`wal_level = logical`** | **Yes** | Server-wide; needs a **restart**. Validated at startup — bintrail-pg refuses to start otherwise. |
| **A role with `REPLICATION`** | **Yes** | Used for the replication stream and to create the slot. |
| **`REPLICA IDENTITY FULL`** on each captured table | **Yes** | The PostgreSQL analog of MySQL's `binlog_row_image = FULL`. Validated per table; refuses to start otherwise. |
| **A `PUBLICATION`** covering those tables | **Yes — you create it** | bintrail-pg validates it exists and covers your tables; it does **not** create it. |
| **A replication slot** | No — created for you | bintrail-pg creates the logical slot on first run and reuses it on restart. |
| `max_replication_slots` ≥ 1, `max_wal_senders` ≥ 1 | Yes | Defaults (10) are usually fine. A slot consumes one of each. |
| **`max_slot_wal_keep_size`** set (not `-1`) | **Strongly recommended** | The production safety valve (PG 13+). Left unlimited (`-1`, the default), a stalled slot pins WAL until the source disk fills — an outage. Set a bound (e.g. `'10GB'`); `bintrail-pg doctor` WARNs while it's unlimited. See [the slot section](#5-the-replication-slot--created-for-you). |

The index MySQL has the same requirements as for any source — see
[Streaming → index requirements](streaming.md).

---

## Install

The console captures PostgreSQL as a **first-class source** — the same
`bintrail-console watch` stack the [Compose quickstart](install.md) stands up.
There are two ways in: the **web console** (recommended — zero binaries to place)
or the standalone **`bintrail-pg`** binary (for an existing index or a headless
CLI deployment).

### The web console — "+ Add server" (recommended)

If you brought up the [Docker Compose stack](install.md) — the *same*
`install.sh` MySQL uses — you already have everything. Do the [source-side
setup](#postgresql-side-setup) below first, then:

1. Open the console (**http://127.0.0.1:8090**) and sign in.
2. **+ Add server** → set **Source type** to **PostgreSQL**. The PostgreSQL-only
   fields appear: **Database**, **Replication slot**, and **Publication**.
3. Fill in host, port, user, password, the database, the slot name (created for
   you on first run), and the publication (the one you created above).
   Optionally restrict **Schemas**.
4. **Save.** DBTrail provisions a dedicated MySQL index for that source and
   starts capturing **in-process** (the console runs the same PostgreSQL
   preflight as `bintrail-pg doctor` — `wal_level`, publication coverage,
   `REPLICA IDENTITY FULL`, slot health — and surfaces any failure as a
   remediation card). Capture resumes automatically on restart.

The **index stays MySQL**; only the *source* is PostgreSQL. The source type is
fixed once saved — to change a server's capture engine, delete and re-create it.
The form lives in `bintrail-console watch` (the Compose default); the read-only
`serve` console browses an existing PostgreSQL-sourced index but does not add
sources.

### Standalone binary / image

Prefer a headless deployment, or capturing into an index you already run?
`bintrail-pg` ships as its own artifact alongside the core `bintrail` binary.

**Docker image:**

```bash
docker pull ghcr.io/dbtrail/bintrail-pg:latest
docker run --rm ghcr.io/dbtrail/bintrail-pg:latest --version
```

**Linux packages** (`.deb` / `.rpm`, amd64 + arm64) are attached to every
[release](https://github.com/dbtrail/dbtrail/releases) as `bintrail-pg_*`:

```bash
# Debian/Ubuntu
sudo dpkg -i bintrail-pg_*_linux_amd64.deb
# RHEL/Fedora
sudo rpm -i bintrail-pg_*_linux_amd64.rpm
```

**From source:**

```bash
git clone https://github.com/dbtrail/dbtrail && cd dbtrail
make build-pg        # produces ./bintrail-pg (requires CGO — DuckDB is embedded)
```

`bintrail-pg` carries the PostgreSQL capture command (`stream`) plus the shared
read/recovery commands (`query`, `recover`, `reconstruct`, `status`, `shim`) —
the same ones the core binary exposes, working over the same index.

---

## PostgreSQL-side setup

Run these once on the source, as a superuser (or the managed-database master
user). Replace `shop` / table names with yours.

### 1. Enable logical decoding (`wal_level = logical`)

Logical replication is impossible without it, and bintrail-pg refuses to start
if it is not set.

```sql
SHOW wal_level;        -- want: logical
ALTER SYSTEM SET wal_level = 'logical';
-- wal_level only takes effect after a RESTART (not a reload):
--   self-hosted:  restart the postgres service
--   managed (RDS/Aurora/Cloud SQL): set it in the parameter group and reboot
```

While you are there, make sure there is slot/sender headroom (defaults are
usually fine; a slot uses one of each):

```sql
SHOW max_replication_slots;   -- ≥ 1
SHOW max_wal_senders;         -- ≥ 1
```

### 2. Create a replication user

bintrail-pg connects with a role that has the **`REPLICATION`** attribute (this
is what lets it open a replication connection and create the slot). It only ever
reads.

```sql
CREATE ROLE dbtrail WITH LOGIN REPLICATION PASSWORD 'change-me';
GRANT CONNECT ON DATABASE shop TO dbtrail;
```

- The role does **not** need to be a superuser, and does **not** need `SELECT`
  on your tables to stream changes (logical replication delivers the row data
  over the WAL). It does read system catalogs (publications, replica identity,
  primary keys) — readable by any role by default.
- **Managed PostgreSQL:** the master user already has the replication privilege
  (e.g. AWS grants `rds_replication`); you can capture as the master user, or
  create a dedicated role and grant it the provider's replication role
  (`GRANT rds_replication TO dbtrail;` on RDS/Aurora). See
  [Managed PostgreSQL](#managed-postgresql) below.

### 3. Set `REPLICA IDENTITY FULL` on every captured table

This is the single most important step. By default PostgreSQL only puts the
**primary-key** columns in the WAL for UPDATE/DELETE (`REPLICA IDENTITY
DEFAULT`). bintrail needs the **full before-image** to generate correct reversal
SQL — and, critically, an unchanged out-of-line **TOAST** value (a large
text/bytea/json column) is only present in the before-image under
`REPLICA IDENTITY FULL`. Under a weaker identity it is silently absent, and
recovery would be wrong.

```sql
ALTER TABLE shop.orders   REPLICA IDENTITY FULL;
ALTER TABLE shop.customers REPLICA IDENTITY FULL;
-- repeat for every table you want to capture
```

bintrail-pg validates this for every table in the publication at startup **and**
re-checks each table live as it appears in the stream, so a table added later
without `REPLICA IDENTITY FULL` fails loud rather than silently losing data.

> This is the direct counterpart of the MySQL/MariaDB `binlog_row_image = FULL`
> requirement.

### 4. Create a publication

A **publication** is PostgreSQL's list of which tables get streamed. You create
it; bintrail-pg validates that it exists and that it covers the tables you ask
for, but **does not create it for you** (we never run DDL on your source).
The publication must publish **all** of insert/update/delete (the default when
`publish` is left unset) and must not attach a PG15+ row filter (`WHERE (...)`)
or column list to any table — bintrail-pg refuses a partial publication at
startup and in `doctor`, because pgoutput would silently drop the excluded
changes.

```sql
-- Specific tables (recommended — you control exactly what is captured):
CREATE PUBLICATION bintrail_pub FOR TABLE shop.orders, shop.customers;

-- …or everything (requires superuser):
CREATE PUBLICATION bintrail_pub FOR ALL TABLES;
```

To add a table later: `ALTER PUBLICATION bintrail_pub ADD TABLE shop.items;`
(then set its `REPLICA IDENTITY FULL` too).

### 5. The replication slot — created for you

You do **not** need to create a replication slot. bintrail-pg creates a logical
slot (named by `--slot`) on first run and resumes from it on every restart.

> **Operational note (important):** a replication slot *retains WAL on the
> source* until its consumer confirms it. If bintrail-pg is stopped for a long
> time, the slot pins WAL and the source disk can fill. On PostgreSQL 13+ set
> `max_slot_wal_keep_size` as a safety valve so an abandoned slot is capped
> (it becomes `lost` instead of filling the disk; bintrail-pg then fails loud on
> resume rather than silently skipping data). To decommission a capture, use
> `bintrail-pg reset` (below) rather than dropping the slot by hand.

#### Checking slot health

`bintrail-pg doctor` reports the slot's live WAL-retention state on demand,
alongside the capture prerequisites (`wal_level`, publication coverage, REPLICA
IDENTITY FULL):

```bash
bintrail-pg doctor --query-dsn "$PG" --slot bintrail_shop --publication bintrail_shop_pub
```

- **`max_slot_wal_keep_size`** → WARN while unlimited (`-1`); set a bound to clear it.
- **Replication slot health** → shows `wal_status` and how much WAL the slot is
  retaining. The status progresses `reserved` (PASS) → `extended` / `unreserved`
  (**WARN** — retaining WAL and approaching the limit; doctor still exits 0) →
  `lost` (a loud **FAIL** with the re-baseline recovery path).
- **No UNLOGGED tables** → WARN if any captured table is `UNLOGGED` (under a
  `FOR ALL TABLES` publication, and under a `FOR TABLES IN SCHEMA` — PostgreSQL
  15+ — publication for its member schemas). UNLOGGED tables write no WAL, so
  their changes are never captured — `ALTER TABLE <t> SET LOGGED` if you need
  them, or ignore if the data is intentionally ephemeral.
- **FK cascade-child coverage** → WARN if a published table has a foreign-key
  `ON DELETE CASCADE` / `SET NULL` **child** that is *not* in the publication. A
  delete on the parent would rewrite that child, and the rewrite would not be
  captured — add the child to the publication (and set its `REPLICA IDENTITY FULL`).

These coverage checks are also emitted as warnings by `bintrail-pg stream` at
startup, so capture is never silently incomplete even if you skip `doctor`.

A permanently-lost stream is also recorded durably: once a slot is invalidated or
dropped out from under a running capture, `bintrail status` shows a loud
**`EVENTS PERMANENTLY LOST`** banner even after the process has exited. In every
case the index up to the gap is still fully usable for recovery — recovery never
needs the slot; only *resuming capture* requires a re-baseline.

---

## Running it

PostgreSQL needs **two connection strings** — this is a protocol constraint, not
a quirk:

- **`--repl-dsn`** — a **replication** connection. Its connection string must
  include `replication=database`. A replication connection runs in *walsender*
  mode and cannot run ordinary SQL, which is why a second connection is needed.
- **`--query-dsn`** — an ordinary connection, used for primary-key lookups in
  the catalog and the startup validations.

Both are standard PostgreSQL/libpq URLs (so TLS is configured the usual way, via
`sslmode=` in the DSN).

```bash
bintrail-pg stream \
  --index-dsn   'user:pw@tcp(index-host:3306)/binlog_index' \
  --repl-dsn    'postgres://dbtrail:pw@pg-host:5432/shop?replication=database&sslmode=require' \
  --query-dsn   'postgres://dbtrail:pw@pg-host:5432/shop?sslmode=require' \
  --slot        bintrail_shop \
  --publication bintrail_pub \
  --server-id   201 \
  --schemas     shop
```

That command: validates `wal_level`, the publication, and per-table
`REPLICA IDENTITY FULL`; creates the slot `bintrail_shop` if absent; bootstraps
the index tables if needed; then streams every row change into `binlog_events`.
Send `SIGINT`/`SIGTERM` for a graceful shutdown (it flushes the batch and writes
a final checkpoint).

### Flags

| Flag | Required | Meaning |
|---|---|---|
| `--index-dsn` | yes | The index MySQL database (env `BINTRAIL_INDEX_DSN`). |
| `--repl-dsn` | yes | PostgreSQL replication DSN, must carry `replication=database` (env `BINTRAIL_PG_REPL_DSN`). |
| `--query-dsn` | yes | PostgreSQL ordinary DSN for catalog/PK lookups (env `BINTRAIL_PG_QUERY_DSN`). |
| `--slot` | yes | Logical replication slot name; created if absent (env `BINTRAIL_PG_SLOT`). |
| `--publication` | yes | Publication name covering the tables to capture (env `BINTRAIL_PG_PUBLICATION`). |
| `--server-id` | yes | A unique number identifying this source in the index — must differ from every other source (env `BINTRAIL_SERVER_ID`). |
| `--schemas` | no | Only index these schemas (comma-separated). |
| `--tables` | no | Only index these tables (comma-separated, e.g. `shop.orders`). |
| `--start-lsn` | no | Explicit start LSN; **first run only**, ignored once a checkpoint exists. |
| `--batch-size` | no | Events per batch insert (default 1000). |
| `--checkpoint` | no | Checkpoint interval in seconds (default 5). |
| `--partitions` | no | Index partitions for the one-time bootstrap (default 48). |
| `--rotate-retain` | no | Built-in rotation: drop index partitions older than this (`Nd`/`Nh`; `off` disables). Default `48h` for a new index; an index created before that default changed keeps the window it was created under (env `BINTRAIL_ROTATE_RETAIN`). |
| `--rotate-interval` | no | Built-in rotation: how often a rotation cycle runs (default `1h`, env `BINTRAIL_ROTATE_INTERVAL`). |
| `--rotate-add-future` | no | Built-in rotation: keep at least N future hourly partitions ready (default `3`, env `BINTRAIL_ROTATE_ADD_FUTURE`). |

Every flag has a `BINTRAIL_*` environment equivalent and can be set in a
`.bintrail.env` file (see [the env-file convention](install.md)).

### Resuming and re-seeding

The durable checkpoint is the last committed **LSN**, stored in the index's
`stream_state`. On restart bintrail-pg resumes from it automatically — re-running
the same command is idempotent, and `--start-lsn` is ignored once a checkpoint
exists.

To start over from scratch you must drop the slot **and** clear the checkpoint —
because in PostgreSQL the *slot* governs the resume position, so clearing the
checkpoint alone does not rewind. `bintrail-pg reset` does both (it drops the slot
first, so an interrupted reset fails safe — "slot gone, checkpoint stale" rather
than "checkpoint cleared, slot live"):

```bash
bintrail-pg reset --query-dsn "$PG" --index-dsn "$IDX" --slot bintrail_shop --force
```

`--force` confirms the destructive teardown. If the slot is already gone or `lost`
(e.g. after a `max_slot_wal_keep_size` invalidation), pass `--index-only` to clear
just the checkpoint:

```bash
bintrail-pg reset --index-dsn "$IDX" --index-only --force
```

This removes only capture-resume state; the index's recovery data is untouched.
Under the hood it is the two-system teardown that otherwise has to be done by hand
(`SELECT pg_drop_replication_slot('bintrail_shop')` on the source + clearing the
position/counter columns of the `stream_state` row on the index — the row is
never DELETEd, so a recorded `gap_lost_at`/`gap_lost_detail` continuity-loss
record is never erased by a reset; a real-checkpoint reset replaces the
record's detail with the reset's own, a no-checkpoint reset preserves it
verbatim).

**A reset that discards a real checkpoint is itself recorded as a permanent
continuity loss**: dropping and recreating the slot skips whatever the old slot
had not yet streamed, so the discarded LSN is stamped into
`gap_lost_at`/`gap_lost_detail` — `bintrail status` shows the
`EVENTS PERMANENTLY LOST` banner and `status --fail-on-gap` exits non-zero,
exactly as after a slot invalidation. After a reset, re-seed the baseline and
re-run `bintrail-pg stream`.

### Index rotation and retention

The index table `binlog_events` is split into **hourly partitions**. New hours
need new partitions or every event eventually lands in the catch-all `p_future`
partition, which can never be dropped — so without rotation the index grows until
the disk fills.

`bintrail-pg stream` runs a **built-in rotation loop** so this is handled for you:
by default it drops partitions older than **30 days** and keeps a few future
partitions ready, running one cycle at startup and then every hour. Tune it with
`--rotate-retain` / `--rotate-interval` / `--rotate-add-future` (or the
`BINTRAIL_ROTATE_*` env vars), or turn it off with `--rotate-retain off` if you
prefer to run retention separately.

The built-in loop is **drop-only** — it does not archive partitions to S3 before
dropping them. For an archive-then-drop retention policy, or for manual/offline
maintenance, run the standalone command against the same index (it is the same
index-side command the core `bintrail` binary exposes, now available on
`bintrail-pg` too):

```bash
# One-off: drop partitions older than 7 days
bintrail-pg rotate --index-dsn "$IDX" --retain 7d

# Daemon: archive each expired partition to S3, then drop it
bintrail-pg rotate --index-dsn "$IDX" --retain 30d --daemon \
  --archive-dir /var/lib/bintrail/archives --archive-s3 s3://my-bucket/archives/

# Re-sync the archive registry with the files actually on disk / in S3
bintrail-pg archive reconcile --index-dsn "$IDX" \
  --archive-dir /var/lib/bintrail/archives --archive-s3 s3://my-bucket/archives/
```

> **Upgrading an existing PostgreSQL install that predates built-in rotation.**
> If you ran `bintrail-pg stream` before this feature, its index has no future
> partitions and everything has piled into `p_future`. No data is lost — the loop
> refuses to drop deep history until you choose a retention explicitly, and it
> back-fills the missing partitions — but the first catch-up can be slow because
> each cycle rewrites the large `p_future`. To catch up in one pass instead, run a
> standalone rotation with enough headroom before (or alongside) the stream, e.g.
> `bintrail-pg rotate --index-dsn "$IDX" --add-future <hours-since-first-event>`,
> then set `--rotate-retain` explicitly (e.g. `30d`) so the loop begins pruning.

---

## Adding more servers

Capture each PostgreSQL source by running its **own** `bintrail-pg stream`
process with a **unique `--server-id`** and a unique `--slot`. There is no
shared daemon for PostgreSQL sources in this release — one process per source
(systemd unit, container, etc.).

You can **view and recover** PostgreSQL-captured data in the read-only web
console (`bintrail-console`), which reads the shared index. The console presents
PostgreSQL sources natively — LSN/slot vocabulary, a lost-slot badge, a
connection-id note, and a live replication-health panel (slot WAL-retention, lag, RI-FULL); see
[PostgreSQL sources](console.md#postgresql-sources). What it does **not** drive is
*capture*: the "+ Add server" / `watch` control plane is MySQL-oriented, so run
`bintrail-pg stream` for the capture and use the console (or the `query`/`recover`
CLI) to browse and recover.

---

## Querying and recovering

Once events are flowing, the read/recovery commands work the same as for a MySQL
source — they read the flavor-agnostic index:

```bash
bintrail-pg query   --index-dsn '…' --schema shop --table orders --limit 20
bintrail-pg recover --index-dsn '…' --schema shop --table orders --pk 42 --dry-run
bintrail-pg status  --index-dsn '…'
```

(The core `bintrail` binary works against the same index too — the read plane is
identical across binaries.)

**What's recoverable:** all INSERT/UPDATE/DELETE row changes with full
before/after images. Under `REPLICA IDENTITY FULL`, unchanged out-of-line TOAST
values are present in the before-image, so a reversal of a large-column row is
correct. FK `ON DELETE CASCADE` / `SET NULL` cascades are visible in the stream
(PostgreSQL performs them as ordinary row changes), so `recover` undoes them
directly.

> **Baseline snapshots exist for PostgreSQL, with a narrower scope than
> MySQL.** `bintrail-pg baseline` takes a consistent Parquet snapshot directly
> from a live PostgreSQL source, anchored to the replication slot's own LSN
> floor. It feeds **single-row** `reconstruct` and the shim's **single-row**
> `_snapshot` — both fold a PG baseline with binlog deltas. This fold is
> **validated end-to-end for scalar types**: the canonical set (`int2/4/8`,
> `numeric`, `bool`, `uuid`, `text`/`varchar`/`char`, `json`, `jsonb`, enums)
> **and** the GUC-sensitive set (`timestamptz`, `timestamp`/`date`/`time`,
> `real`/`double precision`, `bytea`, `interval`), whose text rendering is
> pinned identically on the baseline COPY connections and the logical-decoding
> session (`TimeZone=UTC`, `DateStyle=ISO`, `extra_float_digits=3`,
> `bytea_output=hex`, `IntervalStyle=postgres`) — and user-defined
> **composite** types. Container types beyond that (arrays beyond the tested
> `integer[]`/`text[]`, range/multirange, `hstore`, geometric) remain
> untested through the fold — validate your own round-trip. **Re-baseline after upgrading:** baselines taken before the
> rendering-GUC pin were rendered under your server's session defaults; on a
> server whose defaults differ from the pinned values, re-run
> `bintrail-pg baseline` — the baseline↔delta merge is an exact text join
> (the reader warns when it meets a pre-pin baseline). Events indexed
> **before** the upgrade also keep their pre-pin rendering: on such a server a
> GUC-sensitive primary key (`timestamptz`, `float`) has two text identities
> across the upgrade, so `--pk` lookups and `--history` can split there;
> re-baselining resolves this going forward (deltas before the new anchor are
> not replayed).
> **Full-table** `reconstruct` (`--output-format mydumper`) remains
> deliberately out of scope: a PG baseline does not carry the `CREATE TABLE`
> metadata it needs. `verify` is not bound by that limit — it digests row
> content without emitting a dump, so both baseline-anchored and live-source
> `verify` are supported for PostgreSQL (see
> [verify — Notes per source](verify.md#notes-per-source)). `query` and
> `recover` (which work from the indexed deltas alone, no baseline required)
> round out the recovery surface — see [Limitations](#limitations) for the
> complete picture.

### Interactive `AS OF` from `psql`

The `_flashback` / `_snapshot` / `AS OF` time-travel grammar is also available
over the **PostgreSQL wire protocol**, so you can run single-row time-travel
straight from `psql` (or any PostgreSQL driver) rather than the MySQL-wire shim:

```bash
bintrail-pg flashback \
  --index-dsn 'root:…@tcp(127.0.0.1:3306)/bintrail_index' \
  --shim-config shim.yaml \
  --baseline-dir ./baselines      # optional; enables _snapshot
  # --listen 127.0.0.1:5433 is the default
```

Then connect and query. **Set `dbname` to your PostgreSQL schema** (e.g.
`public`); choose flashback-vs-snapshot with the `_flashback.` / `_snapshot.`
table prefix in the query, exactly like the MySQL shim:

```
psql "host=127.0.0.1 port=5433 user=<tenant> dbname=public"

-- binlog-only view of one row at a past instant:
SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42;

-- baseline-aware view (also resolves rows untouched in the retained window):
SELECT * FROM _snapshot.orders  AS OF '2026-07-01 09:00:00' WHERE id = 42;

-- the bare form on the real table works too:
SELECT * FROM orders AS OF '5 minutes ago' WHERE id = 42;
```

Scope and behaviour track PostgreSQL's current time-travel maturity:

- **Single-row `AS OF` only** — a `WHERE <primary-key> = <value>` predicate.
  Full-table `AS OF` is refused with remediation: a PG baseline does not carry
  the `CREATE TABLE` metadata full-table reconstruct needs (the same limit as
  `reconstruct --output-format mydumper`).
- Columns render as **text** (the conservative first cut): read each one as a
  string, or let your driver text-parse it into a typed target. One caveat: a
  binary/`bytea` value containing a `0x00` byte truncates at the first NUL in
  `psql`/libpq (their text protocol uses NUL-terminated C strings); a driver that
  reads the raw value (e.g. `pgx` scanning into `[]byte`) round-trips it intact.
  Per-column type OIDs (so `bytea` arrives as `bytea`) are a follow-up.
- Use the **simple query protocol** — `psql` does this by default; a `pgx`
  client sets `QueryExecModeSimpleProtocol`. The extended (prepared-statement)
  protocol is not yet implemented.
- **Auth** mirrors the shim: a cleartext password per tenant from
  `--shim-config` (`shim.yaml`). The default listen address is loopback; for a
  non-loopback bind, front a TLS terminator — the same posture as the MySQL
  shim behind ProxySQL.

`bintrail-pg` still ships the MySQL-wire `shim` command as well, if you prefer
to reach the index from MySQL tooling. See
[Time-Travel SQL Setup](time-travel-sql.md) for the full grammar.

---

## Sequences after recovery

Restoring **row data** does not restore the **sequence cursor** behind a
`SERIAL` or `IDENTITY` column. This is the classic PostgreSQL dump/restore
gotcha, and it applies to bintrail-recovered data too — read this section if
your tables use auto-generated ids.

### Why this happens

PostgreSQL's logical decoding does not replicate sequence state. In the words
of the PostgreSQL docs: *"Sequence data is not replicated. The data in serial
or identity columns backed by sequences will of course be replicated as part
of the table, but the sequence itself would still show the start value on the
subscriber."*

For bintrail that means:

- The **id values in your rows are fully captured** — the materialized id
  rides in the row image, and `recover` puts it back verbatim. No data is lost.
- The sequence's own `last_value` is a **separate catalog object** bintrail
  never sees. Nothing in the index knows how far the sequence had advanced.

So after loading recovered rows into a target, the sequence can lag behind the
ids that are now in the table — and the next `INSERT` that draws from it fails
with a duplicate-key error on the primary key.

This is the same boundary MySQL has with `AUTO_INCREMENT`, with one twist:
in PostgreSQL, an INSERT that supplies an **explicit id** (which is exactly
what `recover`'s reversal SQL does) never advances the sequence.

### When you need to fix the sequence

- **Recovering into a fresh or restored target** (a different database, or the
  same database rebuilt from a backup): **always**. The target's sequence
  reflects the backup — or the start value — not the history you just replayed.
- **After the sequence was reset on the source** (`TRUNCATE … RESTART
  IDENTITY`, a manual `setval`, `ALTER SEQUENCE … RESTART`): **yes** — the
  recovered ids are ahead of the rewound cursor.
- **Applying `recover` output on the same live database where the rows were
  deleted**: usually **not needed**. The sequence advanced when the rows were
  first inserted, and a sequence never goes backward on its own — the cursor is
  still past the re-inserted ids. Running the fix anyway is cheap and harmless,
  so when in doubt, run it.

### The fix: `setval` to MAX(id)

After the recovered rows are loaded, point each sequence just past the highest
id actually in the table. `pg_get_serial_sequence` finds the sequence name for
you (it works for both `SERIAL` and `IDENTITY` columns):

```sql
SELECT setval(
  pg_get_serial_sequence('shop.orders', 'id'),
  COALESCE((SELECT MAX(id) FROM shop.orders), 0) + 1,
  false
);
```

The `+ 1, false` form makes the next `nextval` return exactly `MAX(id) + 1`,
and handles an empty table (next value is 1). Run one `setval` per
sequence-backed column — a table can have more than one.

Avoid running it while writers are actively inserting into the table (the
`MAX` and the `setval` are not atomic together); recovery windows are normally
quiesced anyway.

To fix **every owned sequence in a schema** at once, generate the statements
from the catalog (covers `SERIAL` and `IDENTITY` columns; run the output it
prints):

```sql
SELECT format(
  'SELECT setval(%L, COALESCE((SELECT MAX(%I) FROM %I.%I), 0) + 1, false);',
  quote_ident(sn.nspname) || '.' || quote_ident(seq.relname),
  att.attname, tn.nspname, tbl.relname
)
FROM pg_class seq
JOIN pg_namespace sn ON sn.oid = seq.relnamespace
JOIN pg_depend dep ON dep.objid = seq.oid AND dep.deptype IN ('a', 'i')
JOIN pg_class tbl ON tbl.oid = dep.refobjid
JOIN pg_namespace tn ON tn.oid = tbl.relnamespace
JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = dep.refobjsubid
WHERE seq.relkind = 'S'
  AND tn.nspname = 'shop';   -- your schema
```

### Out of scope: standalone sequences

A **standalone sequence** — one whose `nextval` values are used directly
(counters, ticket numbers) and never land in a captured table — is
**irrecoverable from bintrail**. Its values never entered the row history, so
there is nothing to derive a cursor from. If such sequences matter to you,
snapshot them by other means (e.g. include them in your regular dumps —
`pg_dump` records sequence state).

---

## Type support

bintrail-pg captures every value as its PostgreSQL text representation (the
`pgoutput` text format) and, on recovery, regenerates it as a standard-conforming
quoted literal that the target column's input function coerces back. Because the
value is stored verbatim as text — never reparsed through a numeric type in the
index — the `numeric`-via-`float64` precision class that the MySQL path had to guard
against simply cannot occur here.

The following types are **round-trip tested** two ways — insert → capture → index →
`recover` → re-execute the reversal against live PostgreSQL, asserting the column's
canonical `::text` is byte-for-byte identical (`TestOne_PGTypeRoundTripMatrix`), and
through the **baseline+delta reconstruct fold** under deliberately skewed session
GUCs (`TestOne_PGTypeMatrixThroughReconstructFold`) — in `internal/pgstreamrun`,
run across the PG 14/15/16/17 CI matrix:

| Category | Types | Notes |
|---|---|---|
| Integer | `smallint`, `integer`, `bigint` | exact |
| Arbitrary precision | `numeric` (`decimal` is its alias) | full **precision and scale** preserved — values > 2^53 and trailing zeros (`1.50`) survive |
| Floating point | `real`, `double precision` | rendered under pinned `extra_float_digits=3` (shortest-precise) on both capture and baseline sessions |
| Character | `text`, `varchar(n)`, `char(n)` | single quotes and backslashes escaped correctly (`standard_conforming_strings`); `char(n)` round-trips (trailing blanks are insignificant in `bpchar` — PostgreSQL trims them on cast to text) |
| Boolean | `boolean` | |
| UUID | `uuid` | |
| Binary | `bytea` | hex (`\x…`) form — guaranteed by the pinned `bytea_output=hex`, no longer default-dependent |
| JSON | `json`, `jsonb` | `jsonb` tested with an embedded quote |
| Date / time | `date`, `time`, `timestamp`, `timestamptz`, `interval` | rendered under pinned `TimeZone=UTC` / `DateStyle=ISO` / `IntervalStyle=postgres` on both capture and baseline sessions — no longer dependent on server defaults |
| Network | `inet`, `cidr`, `macaddr` | |
| Bit string | `bit(n)`, `varbit` | |
| Range | `int4range` | the other built-in range types share the same text mechanism |
| Array | `integer[]`, `text[]` | element quoting/escaping (e.g. a comma inside an element) preserved |
| Geometric | `point` | |
| Enum | user `ENUM` types | recovered **by label** — the target must declare the same enum type |
| Composite | user-defined composite types | field quoting preserved (embedded comma, doubled quote); the target must declare the same composite type |
| Money | `money` | round-trips, but its text form is **locale-dependent** (`$`-prefixed); recover into a target with the same `lc_monetary` |

The generated reversal SQL is PostgreSQL dialect (double-quoted identifiers,
standard-conforming string escaping, a `SET LOCAL standard_conforming_strings = on`
guard) — the dialect is selected automatically from the source recorded in the index,
so `bintrail-pg recover`, the console, and the MCP server all emit valid PostgreSQL
for a PostgreSQL source.

**Extension types** — PostGIS `geometry`/`geography` and pgvector `vector` — are
round-trip tested the same insert → capture → `recover` → re-execute way, in
dedicated CI cells running extension-bearing images
(`TestOne_PGExtensionTypeRoundTripMatrix`); see
[Extensions and custom types](#extensions-and-custom-types) below.

**Untested / best-effort:** `hstore` and other extension-provided types are covered
under [Extensions and custom types](#extensions-and-custom-types) below. Range
types beyond `int4range`, multirange, geometric types beyond `point`,
arrays beyond the tested `integer[]`/`text[]` (multi-dimensional, `NULL`-containing)
are not yet in the round-trip suite — they are captured as text and are expected to
coerce, but verify your own round-trip.

---

## Extensions and custom types

- **bintrail installs nothing in your database.** Capture is via the built-in
  `pgoutput` plugin only — no output plugin, no `CREATE EXTENSION`, no event
  trigger. This is a deliberate red line, and it is what lets bintrail-pg work
  against managed PostgreSQL (which forbids custom extensions).
- **PostGIS is round-trip tested.** `geometry` and `geography` columns stream
  as hex-EWKB, which embeds the SRID and round-trips losslessly back into
  PostgreSQL — proven empirically by `TestOne_PGExtensionTypeRoundTripMatrix`
  against the `postgis/postgis` image in CI. The target just needs PostGIS
  installed. (Set `REPLICA IDENTITY FULL` as for any table; large geometries
  are TOASTed, so FULL matters.)
- **pgvector is round-trip tested.** `vector` columns stream as their bracketed
  text form (`[0.5,-1.25,3.75]`) and round-trip byte-for-byte — same test,
  against the `pgvector/pgvector` image in CI. The target needs the extension
  installed.
- **Other extension/custom types** (`hstore`, …) are captured **as their text
  representation**. Recovery into a target that has the same extension installed
  is expected to work; treat exotic types as best-effort and test your
  round-trip. (Enums, ranges and user-defined composite types are round-trip
  tested — see the [Type support](#type-support) matrix.)
- **TimescaleDB hypertables are out of scope.** Logical decoding emits the
  underlying *chunk* tables (`_timescaledb_internal._hyper_*`), not the
  hypertable, so a hypertable is not captured coherently in this release.
  bintrail-pg detects a chunk relation in the stream and **warns once** (so you
  are never silently indexing raw chunks under their physical names) — but it does
  not synthesize the logical hypertable.

---

## Limitations

- **`UNLOGGED` tables are not captured.** They bypass the WAL by design, so logical
  decoding never sees them. bintrail-pg **warns** when an UNLOGGED table is in
  capture scope (at `stream` startup and in `bintrail-pg doctor`) — under a
  `FOR ALL TABLES` publication and under a `FOR TABLES IN SCHEMA` (PostgreSQL
  15+) publication's member schemas alike — but the changes are still not
  captured, so don't rely on bintrail for UNLOGGED data.
- **A cascade child must be in the publication.** A foreign-key `ON DELETE CASCADE`
  / `SET NULL` is captured (PostgreSQL performs it as ordinary row changes) **only
  if the child table is published**. If a published parent has an unpublished
  cascade child, the cascade rewrites are not captured — bintrail-pg warns (startup
  + doctor); add the child to the publication.
- **`TRUNCATE` is recorded as an audit marker, not a reversible change.** A
  PostgreSQL `TRUNCATE` emits no per-row `DELETE`, so left uncaptured it would be
  indistinguishable from "nothing happened". bintrail-pg records it as a
  `schema_changes` row (`ddl_type = TRUNCATE TABLE`) — the same durable audit
  trail MySQL's DDL leaves — surfaced by the `list_schema_changes` MCP tool and in
  the `schema_changes` table. It is a *record that the truncate happened*, not a
  reversible event: `recover` has no per-row before-images to put back, and a
  `reconstruct` that crosses the truncate does not yet consume the marker (it can
  still resurrect the pre-truncate rows from baseline + deltas). A multi-table
  `TRUNCATE a, b` records one marker per published table; a truncate of an
  out-of-scope table records nothing.
- **Sequence cursors are not captured.** The materialized id values in your rows
  *are* captured; the sequence's own `last_value` (a separate catalog object) is
  not — logical decoding does not replicate sequences. After a restore, re-point
  each `SERIAL`/`IDENTITY` sequence past `MAX(id)` — recipe and when it matters
  in [Sequences after recovery](#sequences-after-recovery).
- **`GENERATED ... AS IDENTITY` / generated columns** can need care on recovery
  (`GENERATED ALWAYS AS IDENTITY` rejects an explicit insert; `STORED` generated
  columns are absent from the stream before PostgreSQL 18). Treat as best-effort.
- **Full-table `reconstruct` is not wired for PostgreSQL** — a PG baseline
  deliberately omits the `CREATE TABLE` metadata it needs (see above).
  `verify` is not bound by that limit: baseline-anchored, live-source, and
  recover-input modes are all supported for PostgreSQL sources (see
  [verify — Notes per source](verify.md#notes-per-source), including how the
  live-source coverage check differs). **Single-row** `reconstruct` and
  the shim's single-row `_snapshot` work against a PG baseline and are
  validated end-to-end for scalar types — including the GUC-sensitive set,
  rendered under session GUCs pinned identically at capture and baseline;
  container types remain best-effort (see
  [Querying and recovering](#querying-and-recovering)). (`recover` and
  `recover-cascade` work unconditionally, with no baseline needed; cascades
  are captured as ordinary row changes — see
  [Querying and recovering](#querying-and-recovering).)
- **No connection attribution.** `pgoutput` does not carry the backend PID, so
  the `connection_id` column is empty for PostgreSQL sources. Forensic
  attribution ("which session made this change") is therefore a MySQL-only
  capability today, and this is a **settled decision, not a pending gap**
  ([#1212](https://github.com/dbtrail/dbtrail/issues/1212)): the only way to
  recover the backend PID is to install something into your database, and
  bintrail installs nothing into a source. Everything else forensics offers —
  the full before/after row images, the change timeline, `--query-hash`
  grouping where the source logs statements — works identically on PostgreSQL.
- **One database per slot.** A logical slot is scoped to a single database; to
  capture multiple databases on one cluster, run one `bintrail-pg stream` (and
  slot/publication) per database.
- **No BYOS agent.** The web console captures PostgreSQL sources (**+ Add
  server** → PostgreSQL); `bintrail agent` (BYOS) does not support PostgreSQL.

The gates that took PostgreSQL from beta to **GA** are closed
([#597](https://github.com/dbtrail/dbtrail/issues/597)): the data-safety set
(type fidelity, identity/generated recovery, slot/WAL monitoring, RI-FULL
validation, DDL-drift handling, the silent-loss coverage guards above),
baseline-anchored single-row `reconstruct` / time-travel with the
baseline↔delta rendering-GUC identity and its fold-validated type matrix
(#593), the managed-PostgreSQL smoke matrix (RDS and Aurora — see
[Managed PostgreSQL](#managed-postgresql)), and source-aware console
presentation including the live replication-health panel (v0.20.1).
**Full-table** `reconstruct` remains documented out of scope (above);
`verify` — baseline-anchored, live-source, and recover-input — is supported.

---

## Managed PostgreSQL

Logical replication works on managed offerings that expose it; the only setup
difference is *how* you set `wal_level` and grant replication.

| Provider | Validated? | `wal_level = logical` | Replication privilege |
|---|---|---|---|
| **Amazon RDS for PostgreSQL** | **Smoke-validated** (PostgreSQL 16) | Set `rds.logical_replication = 1` in the parameter group, then **reboot**. (A parameter group set at *instance creation* applies at the initial boot — no reboot needed.) | Master user has it (`rds_replication`); grant others with `GRANT rds_replication TO <role>;`. |
| **Amazon Aurora PostgreSQL** | **Smoke-validated** (16-compatible, Serverless v2) | Same, in the **cluster** parameter group. | Same as RDS. |
| **Google Cloud SQL** | Documented only — **not validated** | Set the `cloudsql.logical_decoding` flag on, then restart. | Use a user with `cloudsqlsuperuser` / the replication grant. |

**Smoke-validated** means the full pipeline was exercised end-to-end against a
real instance of that flavor: `bintrail-pg doctor` passes, `stream` creates its
slot and captures INSERT/UPDATE/DELETE into `binlog_events` with full
before/after images, and `recover` generates a correct reversal. The smoke is
`scripts/managed-pg-smoke.sh` (it runs against *any* PostgreSQL DSN — the
managed part is the provisioning), and a gated CI job
(`.github/workflows/managed-pg-smoke.yml`, `workflow_dispatch`-only) provisions
an ephemeral RDS or Aurora instance, runs it, and tears everything down.

Cloud SQL is *expected* to work — bintrail-pg is an ordinary
logical-replication client — but it has **not** been smoke-validated, so its
row is setup documentation, not a support claim. Extending the validated
matrix follows demand ([#535](https://github.com/dbtrail/dbtrail/issues/535)).

Two observations from the RDS/Aurora validation runs: the master user captures
out of the box (it already holds `rds_replication`), and both flavors default
`max_slot_wal_keep_size` to `-1` (unlimited) — `doctor` WARNs about it; set a
bound as you would for any production source.

In all cases bintrail-pg connects as a **client** — there is nothing to install
on the managed instance beyond the publication and `REPLICA IDENTITY FULL`,
which are plain SQL.

---

## Troubleshooting

> Tip: run `bintrail-pg doctor` to catch most of the rows below *before* they cost
> you a debugging cycle — it checks `wal_level`, publication coverage, REPLICA
> IDENTITY FULL, `max_slot_wal_keep_size`, and live slot health in one shot.

| Symptom | Cause / fix |
|---|---|
| `wal_level is "replica", must be 'logical'` | Set `wal_level = logical` and **restart** the server (a reload is not enough). |
| `publication "…" does not exist — create it` | Create the publication first (step 4) — bintrail-pg never creates it. |
| `publication "…" does not cover requested table(s) […]` | Your `--schemas`/`--tables` include tables not in the publication; `ALTER PUBLICATION … ADD TABLE …` (or widen the publication). |
| `table(s) not at REPLICA IDENTITY FULL […]` | Run `ALTER TABLE <t> REPLICA IDENTITY FULL` for the listed tables (step 3). |
| `publication "…" applies a row filter or column list to table(s) […]` | A PG15+ `WHERE (...)` row filter or column list makes pgoutput emit only a subset of changes (silent data loss). Recreate the publication — or `ALTER PUBLICATION … SET TABLE …` re-listing the tables — without `WHERE (...)` / column lists. |
| `publication "…" does not publish […] — those changes would be silently lost` | The publication was created with a narrowed `publish` option (e.g. `WITH (publish = 'insert')`). Run `ALTER PUBLICATION … SET (publish = 'insert, update, delete')`, or recreate it leaving `publish` unset (the default publishes all operations). |
| `replication slot "…" is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded)` | The source dropped WAL the slot still needed (slot was stopped too long). The data since the checkpoint is gone — `bintrail-pg reset` (drops the slot + clears the checkpoint), re-seed, and raise `max_slot_wal_keep_size` / keep the consumer running. `bintrail status` shows this as a loud `EVENTS PERMANENTLY LOST` banner. |
| `resuming from a saved checkpoint but replication slot "…" no longer exists` | The slot was dropped while a checkpoint still pointed at it. Creating a fresh slot would skip data, so bintrail-pg refuses — `bintrail-pg reset --index-only` to clear the checkpoint and start fresh if the gap is acceptable. |
| `must include replication=database` (connection error on `--repl-dsn`) | Add `replication=database` to the `--repl-dsn` connection string. |
| Permission denied opening replication / creating slot | The role needs the `REPLICATION` attribute (`ALTER ROLE <r> REPLICATION;`), or on managed PG the provider's replication grant. |

---

## See also

- [Install](install.md) — all install methods and the env-file convention.
- [Streaming](streaming.md) — index requirements and the streaming model.
- [Query & Recovery](query-and-recovery.md) — querying history and generating
  reversal SQL (flavor-agnostic — same for PostgreSQL, MySQL, and MariaDB).
- [MariaDB as a source](mariadb.md) — the sibling alpha source.
