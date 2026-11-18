# PostgreSQL as a source

bintrail can capture from a **PostgreSQL** server while the index database stays
MySQL. PostgreSQL capture lives in its own binary, **`bintrail-pg`**. This is a
**beta** capability: capture is type-faithful,
`REPLICA IDENTITY FULL`-enforced, replication-slot/WAL-retention-monitored, and
DDL-drift-safe, all verified end-to-end against real PostgreSQL (15–18 in CI)
and smoke-validated on managed PostgreSQL (RDS / Aurora). It still has
documented limitations (below) — notably whole-table time-travel in the shim
and full-table `reconstruct` to a mydumper dump, which stay out of scope for
PostgreSQL — so read them before pointing it at production.

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
| **PostgreSQL 15+** | Yes (declared) | 15/16/17/18 tested in CI. 13 and 14 may work but are past end of life, best-effort only. On 18, a table with a `VIRTUAL` generated column cannot be captured, and a table with a `STORED` one needs `publish_generated_columns = stored` on the publication ([why](#3-set-replica-identity-full-on-every-captured-table)). |
| **`wal_level = logical`** | **Yes** | Server-wide; needs a **restart**. Validated at startup — bintrail-pg refuses to start otherwise. |
| **A role with `REPLICATION`** | **Yes** | Used for the replication stream and to create the slot. |
| **`REPLICA IDENTITY FULL`** on each captured table | **Yes** | The PostgreSQL analog of MySQL's `binlog_row_image = FULL`. Validated per table; refuses to start otherwise. |
| **Primary keys that are not `DEFERRABLE`** | **Yes** | The copy is kept by key, and a deferrable key lets two rows share one inside a transaction. Validated at startup and in `doctor`: a captured table with one is refused, with what to do. |
| **A `PUBLICATION`** covering those tables | **Yes — you create it** | bintrail-pg validates it exists and covers your tables; it does **not** create it. |
| **A replication slot** | No — created for you | bintrail-pg creates the logical slot on first run and reuses it on restart. |
| `max_replication_slots` ≥ 1, `max_wal_senders` ≥ 1 | Yes | Defaults (10) are usually fine. A slot consumes one of each. |
| **`max_slot_wal_keep_size`** set (not `-1`) | **Strongly recommended** | The production safety valve (PG 13+). Left unlimited (`-1`, the default), a stalled slot pins WAL until the source disk fills — an outage. Set a bound (e.g. `'10GB'`); `bintrail-pg doctor` WARNs while it's unlimited. See [the slot section](#5-the-replication-slot--created-for-you). |

The index MySQL has the same requirements as for any source — see
[Streaming → index requirements](streaming.md).

---

## Install

DBTrail captures PostgreSQL in the same `bintrail-console watch` stack the
[Compose quickstart](install.md) stands up, in an installation of its own: an
installation serves PostgreSQL servers, or MySQL and MariaDB servers, not both
([one database family per installation](install.md#one-database-family-per-installation)).
There are two ways in: the **web interface** (recommended: zero binaries to place)
or the standalone **`bintrail-pg`** binary (for an existing index or a headless
CLI deployment).

### The web interface: "+ Add server" (recommended)

If you brought up the [Docker Compose stack](install.md) — the *same*
`install.sh` MySQL uses — you already have everything. Do the [source-side
setup](#postgresql-side-setup) below first, then:

1. Open the web interface (**http://127.0.0.1:8090**) and sign in.
2. **+ Add server** → **Open the full form** (the link at the foot of the
   Connect screen) → set **Source type** to **PostgreSQL**. The PostgreSQL-only
   fields appear: **Database**, **Replication slot**, and **Publication**.
3. Fill in host, port, user, password, the database, the slot name (created for
   you on first run), and the publication (the one you created above).
   Optionally restrict **Schemas**.
4. **Save.** DBTrail provisions a dedicated MySQL index for that source and
   starts capturing **in-process** (the daemon runs the same PostgreSQL
   preflight as `bintrail-pg doctor` — `wal_level`, publication coverage,
   `REPLICA IDENTITY FULL`, slot health — and surfaces any failure as a
   remediation card). Capture resumes automatically on restart.

From a terminal, the same thing is one command (the publication is the one
you created above; the slot is created for you and named `bintrail_<database>`
unless you give `--slot`):

```bash
dbtrail server add --flavor postgres -h pg1.example.com -u dbtrail -p -d shop --publication bintrail_pub
```

It runs the same checks, prints each one that fails with its fix, and saves
nothing until they all pass. `--flavor postgres` has to be said: the question
DBTrail asks an address to learn what answers there is in MySQL's protocol.
What it prints at the end is the `psql` line for the PostgreSQL port, with the
port's password when this was the first server.

The **index stays MySQL**; only the *source* is PostgreSQL. The source type is
fixed once saved — to change a server's capture engine, delete and re-create it.
The form lives in `bintrail-console watch` (the Compose default); the read-only
`serve` mode browses an existing PostgreSQL-sourced index but does not add
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
CREATE ROLE dbtrail WITH LOGIN REPLICATION PASSWORD <choose a password>;
GRANT CONNECT ON DATABASE shop TO dbtrail;
```

Put a password of your own in quotes where it says `<choose a password>`. As
written, PostgreSQL refuses the line on purpose, so no role is created with a
password copied from this page.

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

> **PostgreSQL 18 and generated columns: read this before you run step 3 or 4.**
> On PostgreSQL 18, a table that is at `REPLICA IDENTITY FULL`, belongs to a
> publication, and has a generated column that the publication does not
> publish **refuses every `UPDATE` and `DELETE`**, from your own application
> too, with `Replica identity must not contain unpublished generated columns`.
> `INSERT` still works. It takes effect the moment both settings are in place,
> whether or not DBTrail is running.
>
> Find your generated columns first:
>
> ```sql
> SELECT c.relnamespace::regnamespace AS schema, c.relname AS "table", a.attname AS "column",
>        CASE a.attgenerated WHEN 's' THEN 'STORED' ELSE 'VIRTUAL' END AS kind
> FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
> WHERE a.attgenerated <> '' AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p');
> ```
>
> - **`STORED`**: create the publication with
>   `WITH (publish_generated_columns = stored)` (step 4), and the table works.
>   For a publication that exists:
>   `ALTER PUBLICATION bintrail_pub SET (publish_generated_columns = stored);`
> - **`VIRTUAL`** (new in 18, and what `GENERATED ALWAYS AS (...)` creates there
>   when neither word is given): PostgreSQL cannot publish it, so that table
>   cannot be captured. Leave it out of the publication and do not set
>   `REPLICA IDENTITY FULL` on it. A `FOR ALL TABLES` publication cannot leave
>   a table out: list the tables instead.
> - A table that belongs to **another** publication as well is refused if that
>   one does not publish the column, whatever DBTrail's publication does.
> - **If your application is already refused**, this lets it write again at
>   once: `ALTER TABLE shop.orders REPLICA IDENTITY DEFAULT;`
>
> `bintrail-pg doctor`, the web interface's checks when a server is added, and
> the capture when it starts all look for this and stop with the statements to
> run. PostgreSQL 17 and older have no such rule.

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

-- On PostgreSQL 18, when a captured table has a STORED generated column
-- (see the note in step 3):
CREATE PUBLICATION bintrail_pub FOR TABLE shop.orders, shop.customers
  WITH (publish_generated_columns = stored);

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

#### When capture was last caught up

With each of those health polls the capture also asks the source how far its
WAL is flushed. If the capture has received the source's changes up to that
point within a second of asking, it held everything the source had at that
moment, and it records the moment in the index at its next checkpoint. The web
interface and the router read it to say how far behind the source an answer
is. A capture that keeps up records one about every 30 seconds; one that is
more than a second behind records none, and shows as behind. It is one
`SELECT` on the health poll's connection.

#### A large transaction arrives whole, after it commits

PostgreSQL sends a transaction to capture only once it has committed (capture
asks for version 1 of the `pgoutput` protocol, which does not stream a
transaction that is still open). Nothing is missing from the copy while it is
open: its changes are not visible on the source either. The cost comes at the
commit. The whole transaction is then sent in one piece and indexed, and every
change committed after it waits behind it. A transaction that changed a few
million rows can leave the copy minutes behind, where the same rows in small
transactions would have gone through as they were made.

So what sets the delay is how MUCH one transaction changed, not how long it
stayed open: a transaction open for an hour that changed ten rows costs
nothing. While the large one goes through, the web interface shows the capture
as behind and the copy stays where it was; both recover by themselves once it
is in. If a job on the source changes rows in
bulk, committing in batches keeps the copy close.

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
| `--metrics-addr` | no | Address to expose Prometheus metrics on, e.g. `:9090`; empty (the default) exposes none (env `BINTRAIL_METRICS_ADDR`). See [observability.md](observability.md#postgresql-capture-bintrail_pg_). |
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
record's detail with the reset's own and withdraws any acknowledgement of the
earlier loss, a no-checkpoint reset preserves it verbatim).

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
interface (`bintrail-console`), which reads the shared index. The web interface presents
PostgreSQL sources natively — LSN/slot vocabulary, a lost-slot badge, a
connection-id note, and a live replication-health panel (slot WAL-retention, lag, RI-FULL); see
[PostgreSQL sources](console.md#postgresql-sources). What it does **not** drive is
*capture*: the "+ Add server" / `watch` control plane is MySQL-oriented, so run
`bintrail-pg stream` for the capture and use the web interface (or the `query`/`recover`
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
> **Where the changes after a snapshot start.** A PostgreSQL snapshot records
> an LSN floor read just before it opens, and reads start there, not at the
> snapshot's time: a transaction committing at the instant the snapshot opens
> can be missing from it, and is then applied from the index. Such a commit
> is looked for at least a minute before the snapshot's time; one held longer
> than that across the snapshot's start (a synchronous standby that stopped
> answering) is not covered. A primary-key-changing UPDATE in that minute
> makes the shim's whole-table `_snapshot` and `verify` refuse, as one after
> the snapshot does.
> **Full-table** `reconstruct --output-format parquet` and `bintrail baseline
> refresh` work for PostgreSQL: they fold a snapshot forward to a point in
> time, and the result is a snapshot the next fold starts from (see
> [Full-table snapshots](#full-table-snapshots)). `--output-format psql`
> writes SQL files psql loads, and `bintrail-pg drill` rehearses a restore with
> them into a scratch PostgreSQL (see
> [Restore drills](drill.md#postgresql-sources)). `--output-format mydumper`
> stays out of scope: a mydumper dump is MySQL's. `verify` digests row content without emitting a dump, so
> both baseline-anchored and live-source `verify` are supported for PostgreSQL
> (see
> [verify — Notes per source](verify.md#notes-per-source)). `query` and
> `recover` (which work from the indexed deltas alone, no baseline required)
> round out the recovery surface — see [Limitations](#limitations) for the
> complete picture.

### Full-table snapshots
`bintrail reconstruct --output-format parquet --tables public.orders --at …`
and `bintrail baseline refresh` take a PostgreSQL snapshot (from
`bintrail-pg baseline`, or a previous fold) and the indexed changes after it,
and write a snapshot of the whole table at the requested point. Each one
records where the next fold starts (an LSN) and how far back in time the
changes after it can reach, so a chain of folds applies every transaction
exactly once.

A snapshot schedule set on a PostgreSQL server in the web interface does the
same on its own. The first snapshot is a full read of the source; each slot
after it builds the next snapshot from the previous one and the changes
captured since, without reading the source again. An update holds the changes
up to capture's last saved position, which capture saves every 10 seconds. A
table added to the publication after the previous snapshot has no file to
update, so the update publishes without it and names it, and the schedule then
takes one full read of the source to bring it into the copy, as it does for a
new table on a MySQL source: where the schedule may take full reads, and no
more than a few times for the same table.

What to know:

- **The output is a snapshot, or SQL psql loads.** A snapshot's values are
  PostgreSQL's own text, as `bintrail-pg baseline` writes them.
  `--output-format psql` instead writes one file per table,
  `<schema>.<table>.sql`, that `psql -v ON_ERROR_STOP=1 -f <file>` loads in one
  transaction: the settings the values were rendered under, a `CREATE TABLE`
  built from the snapshot's recorded shape (columns, types, `NOT NULL`,
  defaults, identity and `serial` sequences, `STORED` generated columns, the
  primary key), the rows as `COPY ... FROM stdin`, and each sequence moved past
  the loaded rows. Not made: other indexes and constraints, collations,
  comments, grants, and any type or sequence a default refers to that the table
  does not own; a snapshot taken before these facts were recorded is refused
  (take a new one). `--output-format mydumper` is refused.
- **A change to the table's columns or primary key refuses**, a type-only
  change included: the fold compares the shape the snapshot recorded with every
  shape the stream saw up to the point, and the key the changes were captured
  under with the one rows are joined by. Take a new snapshot with
  `bintrail-pg baseline`.
- **A TRUNCATE, a DROP or a rename refuses**, including one that committed at
  the instant the snapshot opened: it writes no row changes, so the fold
  would bring the rows back.
- **A table taken out of the publication and added back refuses** across that
  stretch: its changes while it was out were never captured. This is seen only
  for a `FOR TABLE` publication with `track_commit_timestamp = on` (see
  [Schema changes on PostgreSQL](#schema-changes-on-postgresql)); otherwise
  the fold cannot tell, and the chain's "every transaction exactly once" holds
  only for what capture received.
- **An UPDATE that changes the primary key is applied as a delete of the old
  key and an insert of the new one**, the way MySQL and MariaDB do it, each
  ordered against every other change of that key by the commit LSN (and,
  inside one transaction, by the order the changes were made in). This covers
  a key changed twice, changed and changed back, moved onto a key that was
  deleted, and two rows swapping keys through a third value, each change in
  its own transaction, and several rows moved by one statement when no row
  lands on a key that changes again in that statement's transaction. One such change still refuses the
  table, with the reason in the message, when it cannot be proven whole: the
  change has no commit LSN in the index, its old row was not captured with
  every column (the table was not at `REPLICA IDENTITY FULL`), a key column
  is missing from either row, or the key the index stores for it is not the
  one its old row spells. One more case refuses out of caution: the old row
  has NULL in every column outside the key and the new row has a NULL outside
  the key too. Such an old row looks the same as one captured with the key
  only, where a large value the UPDATE did not touch would have been stored
  as NULL. And one case refuses because the changes alone cannot say what
  happened: inside one transaction, a key receives a row (an INSERT, or a
  row whose key changed to it) and is changed again later in that
  transaction, with a key change among those changes. When a primary key is
  `DEFERRABLE`, two rows can hold the same key value inside a transaction, so
  `UPDATE t SET id = id + 1` over rows 1, 2 and 3 is captured as "1 to 2, 2
  to 3, 3 to 4", the same as one row moved three times, and the two leave
  different rows. This also refuses a row whose key is changed twice in one
  transaction, and a row inserted and re-keyed in the same transaction. The
  same statements in separate transactions are applied. A refused change refuses even when a later change
  overwrote it, and one in the minute before the snapshot, which the fold
  reads again (see above), counts too.
- **A transaction whose changes of one key do not describe one row
  refuses.** A `DEFERRABLE` primary key is checked at the end of the
  statement or of the transaction, so until then two rows can hold the same
  key value, with no key change involved: a transaction inserts a new row 1
  while the old row 1 is still there and then deletes the old one. The
  captured changes are an INSERT and a DELETE of key 1, the last one is the
  DELETE, and a copy that kept the last change would have no row 1 where the
  source has the new one. The fold therefore checks, for the changes one
  transaction makes to one key, that each starts from the row the one before
  it left: an INSERT from no row, an UPDATE or a DELETE from exactly the row
  the previous change wrote. When that does not hold, or the rows cannot be
  compared, the table is refused with a message that names the key and the
  transaction's commit LSN. The ordinary transactions are applied as before:
  a row inserted and deleted again, inserted and updated, updated twice,
  deleted and replaced under the same key.
  A transaction that capture delivered twice after a restart is recognized
  and applied once, when it looks exactly like one: the key's first change
  of the transaction comes back, and the changes after it repeat the changes
  before it, one by one, for as long as those ran (the first delivery may
  have been cut short; the last one is whole). A first change that comes back
  followed by anything else refuses. Two limits: when a snapshot update holds
  so many changed rows that it moves them to disk and a transaction delivered
  twice has its changes on both sides of that move, it is refused, not
  checked; and a transaction delivered twice that holds a key-changing
  UPDATE is refused by the rule of the previous point (the new key receives
  the row twice).
  What is not seen, in every case because the rows are equal in every column,
  the key included: (1) a row inserted as an exact copy of one that already
  existed, followed by a delete of the original, which reads like inserting
  a row and deleting it again; (2) the same row inserted twice and one of the
  two deleted, or any series of changes made identically to two identical
  rows, which reads like a transaction cut short and then delivered whole:
  the source keeps one row and the copy none. Both need a `DEFERRABLE` key;
  the index does not record whether a key is deferrable. (3) On a database
  whose encoding is `SQL_ASCII`, text that is not valid UTF-8 is stored with
  replacement characters, so two rows that differ only in such bytes compare
  equal.
- **An unchanged-TOAST marker refuses** on any change the fold reads, even
  one a later change overwrote. Under `REPLICA IDENTITY FULL`, which capture
  requires, there is no marker to meet: capture replaces an unchanged large
  value with the real one from the old row of the same change, for an
  ordinary UPDATE and for one that changes the key. A marker is only in
  changes captured without the whole old row, where the value was never
  captured.
- **A table with no change is carried forward** under
  `--carry-forward-unchanged`, as for MySQL: the new snapshot gets the previous
  file (a hard link where the filesystem allows one) and the table is not read
  or written again. Two cases still rewrite it. The first update after a full
  read rewrites every table once, because a full read's files do not record
  how far back their missing changes reach. And a table that has gone
  unchanged for long is rewritten before the index drops the changes its file
  starts from. Only the scheduled updates of the web console carry tables
  forward: `bintrail baseline refresh` on the command line does not know how
  long the index keeps its changes, and rewrites. A changed table is rewritten
  in full: `--table-deltas` applies to MySQL only.
- **Capture that is behind is safe for the chain, not for one file.** A
  snapshot folded while capture is behind holds the table as of the newest
  change the index has, under the requested point's name, and the run warns.
  The next fold applies what capture delivers later: the folded snapshot's time
  bound is set from the newest change the index holds, not from the point.
- **A transaction the stream has not checkpointed** is left whole to the next
  fold, never applied in part.
- One index database follows one PostgreSQL source: the fold reads where it
  stops from the whole index.

### SQL on the copy from `psql`

`bintrail-console pg-router` opens a PostgreSQL-protocol port, so a PostgreSQL
client runs read-only SQL on the copy of a PostgreSQL server:

```bash
bintrail-console pg-router --listen 127.0.0.1:5433
```

```
psql "host=127.0.0.1 port=5433 dbname=<server id or name> user=app"

SELECT status, count(*) AS n FROM orders GROUP BY status ORDER BY n DESC;
```

It is a process of its own. It runs beside `bintrail-console watch`, reads the
servers the web interface manages and never writes them, and shares nothing
at run time with the MySQL port (`bintrail-console router`): its own
listener, its own workers, its own memory.

- **The database name picks the server**: its id or its name, as the web
  interface shows them. The username is free, except that a server's
  application login, used as the username, opens that server whatever database
  is named.
- **The password** is the one created for SQL clients in the web interface
  (`dbtrail port on`), the access token, or the application login's own. The
  port is open while the port for SQL clients is turned on in the web
  interface, and closes with every connection when it is turned off or its
  password is replaced. Switching Engine Routing, which is the MySQL port's,
  does not disconnect anyone here.
- **PostgreSQL servers only.** A MySQL or MariaDB server is refused with an
  error that names it; its copy is read through the MySQL port.
- **Fresh or fast, per server, as on the MySQL port.** In fresh mode, the
  default, each table's newest changes captured from PostgreSQL are laid over
  its snapshot, so a row written a moment ago shows without waiting for the
  next snapshot update: answers are about as far behind as capture. In fast
  mode an answer is as old as the copy's last snapshot update. Choose a
  server's mode on its Snapshots page or with
  `bintrail-console port mode <server> fresh|fast`; `BINTRAIL_SQL_TAILS=on`
  or `=off` where the PostgreSQL router starts (`SQL_FRESH` in the compose
  `.env`) decides for every server instead. `port mode` says what is saved
  for a PostgreSQL server; it cannot see what the PostgreSQL router applies.
  The recent changes are held in this process's memory, on top of each
  statement's own: up to a fifth of what it may use for each server in fresh
  mode (`BINTRAIL_SQL_TAIL_MAX_BYTES` sets another bound per server), so with
  several servers in fresh mode give the process the memory for all of them;
  `--workers` is not lowered for them.
  They are also kept as small files in the temporary directory, which the
  MySQL router on the same machine can share: neither removes the other's.
  What it gives and what it costs, as measured on 2026-11-17 (a table of 2
  and of 10 million rows of about 100 bytes, 400 transactions a second at the
  source, a snapshot update every 5 minutes, 8 statements at once):
  - **How far behind an answer is:** about 1.2 seconds at the median and
    never more than 3.2, at both sizes, with 10, 50 and 200 clients, and
    while a snapshot update ran.
  - **What a statement costs:** 2 to 3 times what it costs in fast mode. The
    size of the table counts for more than the mode: a statement on the
    10-million-row table took 0.4 to 0.6 seconds in fresh mode (0.13 to 0.35
    in fast), and 0.12 to 0.17 on the 2-million-row one (0.03 to 0.09).
  - **A statement that reads one row by its key reads the whole table too**:
    the copy has no index. With a forwarding user such a statement can be
    answered by the source (see
    [Which side answers a statement](#which-side-answers-a-statement-on-the-postgresql-port)).
  - **How many statements it answers** is set by `--workers`, which is 4
    out of the box, with 256 clients connected at most
    (`--max-connections`); in the compose `.env`, `PG_ROUTER_WORKERS` and
    `PG_ROUTER_MAX_CONNECTIONS`. The measurement above ran 8 at once.
  - **Switching a server from fresh to fast** makes the connections already
    open read the snapshot from their next statement on: an answer older
    than the one before it on the same connection, which is told once by a
    NOTICE.
  What fresh mode does not cover yet, where the answer comes from the
  snapshot alone. The client is told only by a NOTICE that comes with the
  answer (drivers that drop notices, or `client_min_messages` above
  `notice`, hide it), and the statement is not sent to the source for it:
  - **Right after a snapshot update**, each table's recent changes are
    gathered again from the new snapshot on, which takes a few seconds; until
    then statements on that table are answered from the snapshot.
  - **The first statement on a table** after the router starts, or after the
    server turns to fresh mode, starts gathering that table's changes: it,
    and any statement in the next moments, may still answer from the
    snapshot.
  - **A table with no write since capture started** has no recent changes to
    lay over it, which is the right answer for that table alone. But a
    statement answers fresh only when every table it reads does, so a join
    with such a table is answered from the snapshot for all of its tables.
  - **Waiting for the source** (`BINTRAIL_SQL_FRESH_WAIT`) does not apply to
    PostgreSQL yet: a statement does not wait for the newest changes to
    arrive before it reads.
- **The copy answers what it answers as PostgreSQL does.** With the server's
  forwarding user, each statement is decided on its own: the copy answers it
  when everything in it is known to give PostgreSQL's answer there, and the
  source otherwise, read-only (see
  [Which side answers a statement](#which-side-answers-a-statement-on-the-postgresql-port)).
  A transaction block the client opens, and a catalog statement the copy does
  not answer, go to the source too (see
  [Transactions on the source](#transactions-on-the-source-the-forwarding-user)).
  A server without a forwarding user is answered from the copy alone, and a
  statement the copy cannot answer gets its error. A statement that is not a
  read is refused with an error. Unqualified names
  resolve in `public` until the client sets `search_path` (see
  [Session settings](#session-settings-and-query-cancel-on-the-postgresql-port)). NULL order, integer division and text comparison follow
  PostgreSQL, and each type reads as [the table below](#how-each-type-reads-in-sql-on-the-copy)
  says; the differences listed there apply here too.
- **Each column carries its PostgreSQL type**, and each value is written the
  way PostgreSQL writes a value of that type (`t`/`f`, `{a,b}`,
  `2026-03-04 05:06:07+00`, `\xdead`, `-0`, `NaN`). A column of the table
  keeps the type, type modifier and size the source announces for it:
  `integer`, `bigint`, `smallint`, `numeric(p,s)`, `real`, `double precision`,
  `boolean`, `date`, `timestamp`, `timestamptz`, `time`, `timetz`, `uuid`,
  `bytea`, `text`, and arrays of one dimension of those. So `pgx` returns an
  `int64` or a `time.Time`, psycopg an `int`, a `Decimal` or a `datetime`, and
  JDBC's `getObject` a `Long`, a `BigDecimal` or a `Timestamp`, as they do from
  the source. When a driver asks for results in binary (`pgx` and JDBC do for
  common types, psycopg on a binary cursor), they are sent in binary, the
  same bytes the source sends. Some columns are announced as `text`, with the
  text PostgreSQL writes:
  - `json`, `jsonb`, `varchar(n)`, `char(n)`, enums and the other types the
    copy keeps as text (the [table below](#how-each-type-reads-in-sql-on-the-copy)
    lists them);
  - `interval`: JDBC reads a column of that type by asking the server's
    catalog about the type, which this port answers only for the types it
    announces, so `getObject` would fail; as text every driver reads it;
  - a value of a type the copy has and PostgreSQL does not (a struct, a map,
    an array of arrays);
  - time travel (`_flashback`, `_snapshot`) and `SHOW`.

  `infinity` and `-infinity` in a `date`, `timestamp` or `timestamptz` column
  read as the source writes them: the words in text, and in binary the ends of
  the count PostgreSQL sends (the largest and smallest 32-bit day for a `date`,
  64-bit microsecond for a timestamp), in arrays too.

  An expression's type is the one the copy computes it with, which is not
  always PostgreSQL's: `avg()` of whole numbers is a `double precision` here
  and a `numeric` there, and an aggregate or a literal of a `numeric` keeps its
  precision and scale here (`numeric(10,2)`) where PostgreSQL reports none. A
  `numeric` declared without precision is a `numeric(38,18)` here and is
  written with its 18 decimals (`12345.678000000000000000` where the source
  writes `12345.678`). A value with no binary form for its type (a date past
  year 9999) is refused with error `0A000` when a client asks for binary, and
  sent as text when it asks for text.
- **A column with no name of its own is headed as PostgreSQL heads it**:
  `count(*)` is `count`, a function call takes the function's name, a cast of
  an expression takes the type's name (`int4`, `numeric`, `timestamptz`), and
  an operator or a constant is `?column?`. A few cases keep the copy's own
  heading, so give the column a name (`AS n`) when the heading matters there:
  a statement whose select list holds a `*` next to an unnamed expression, a
  cast of an expression to `text`, `varchar`, `char`, `real` or `float`, a
  column written with upper-case letters inside a cast, and `VALUES` lists.
- **Prepared statements and parameters work**: the port speaks the simple and
  the extended query protocol, so `psql`, JDBC, `pgx` in its default mode
  and psycopg 3 connect as they are. Each argument is written
  into the statement as a value before the copy runs it, and the statement then
  runs exactly as if it had been typed with that value; whatever the simple
  protocol refuses (a write, `SET ROLE`) is refused here with the same
  error. Three things to know:
  - The port does not work out a parameter's type from the statement, as
    PostgreSQL does: it tells the driver the type is unknown. An argument the
    driver sends without a type is read as the type its place needs
    (`id = $1` compares as a number), but where its place does not decide, as
    in `$1 + 1`, give it one: `$1::int + 1`.
  - When a driver asks for a statement's columns before running it (pgx does,
    JDBC sometimes), the port asks the copy for them without reading a row.
    A statement the copy cannot ask that way (`DESCRIBE`, the copy's own
    `SHOW TABLES`) is described as returning no rows; with `pgx` run those
    with `QueryExecModeSimpleProtocol`. `SHOW` of a setting is answered by
    the port, and described.
    The columns said this way keep the copy's own heading where the statement
    gives one no name (`count(*)` is `count_star()`), so name such a column
    (`AS n`) when the heading matters to a driver that describes first.
  - `BEGIN`, `COMMIT` and `ROLLBACK` move the transaction status the driver
    sees, and after an error inside a block every statement is refused until
    `ROLLBACK`, as on PostgreSQL. They do not make a snapshot: each statement
    reads the copy as it is when it runs. A driver that fetches a few rows at a
    time (JDBC with `setFetchSize` and autocommit off) works; the port still
    computes the whole answer first, under the same row cap.
- **The login is SCRAM-SHA-256.** The client proves it knows the password and
  the password itself never crosses the network, with TLS or without. The port
  never asks for the password in clear text, so a client needs SCRAM support:
  libpq 10 or later (`psql`, psycopg), JDBC 42.2 or later, or any `pgx`. Over
  TLS the port also offers SCRAM-SHA-256-PLUS, which binds the login to the
  certificate the client saw, so a client with `channel_binding=require`
  refuses anything that relays between it and the port.
- **TLS with the MySQL port's certificate.** Give the same files the MySQL port
  takes (`router --tls-cert`) with `--tls-cert` and `--tls-key`, and a client
  that trusts their CA connects with `sslmode=verify-ca`, or
  `sslmode=verify-full` with the name the certificate holds:

  ```
  psql "host=db.example.com port=5433 dbname=<server> user=app sslmode=verify-full sslrootcert=ca.pem"
  ```

  With no certificate set, a client that asks for TLS still gets it, with a
  certificate made up at start: `sslmode=require` works, and `verify-ca` and
  `verify-full` refuse it. The files are read when the process starts, so a
  renewed certificate is served after a restart; one that expired is served
  all the same, with a warning at start, and a file that cannot be read stops
  the process before it listens, with the reason. Clients that start TLS
  directly (`sslnegotiation=direct`, PostgreSQL 17) are taken too.
- **TLS is offered, never required**, as on the MySQL port: a client that asks
  for no TLS (`sslmode=disable` or `allow`) is answered in clear text. Its password still does not cross the
  network, but its rows do. A client that must not read rows in clear text
  sets `sslmode=require` or stricter; that is also what keeps something in
  the middle from turning TLS off.
- **Any address.** `--listen` takes any host:port, `0.0.0.0:5433` included. An
  address other machines reach is named at start, since whoever reaches it can
  try the port's password; keep it behind a firewall as you would the MySQL
  port.
- **Session settings and query cancel** are taken as PostgreSQL takes them;
  [below](#session-settings-and-query-cancel-on-the-postgresql-port).
- A result with more rows than the port's row cap, or with a cell longer than
  the cap on one cell, is refused, not cut: add a `LIMIT`, or select part of
  the long column.
- **Results being sent are held in memory up to 256 MB in all.** The port
  builds a result whole and keeps it until the client has read it. While it
  holds 256 MB of results for clients that have not read theirs yet, the next
  statement is refused with error `53000` before it runs; run it again once
  the other clients have read their answers. Time travel answers are counted
  too. A client that asks for a result and reads nothing of it for 60 seconds
  is disconnected, and what the port held for it is freed. There is no flag
  for this size; the MySQL port holds the same amount.
- **Time travel on the same connection**: one row as it was at a past moment,
  from the server's change history and snapshots.

  ```sql
  SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42;
  SELECT * FROM _snapshot.orders  AS OF '2026-07-01 09:00:00' WHERE id = 42;
  ```

  A whole table as of a moment is refused here (use `bintrail-pg reconstruct`
  or the web interface). A server that hides rows or columns refuses time
  travel on this port, with the reason: time travel reads the history
  directly, past the views that hide them.

What the process may use is said where it is started, with defaults that fit
beside capture on the recommended machine (8 cores). Each statement may take
the SQL memory, so four at once can take 8GB, plus the recent changes fresh
mode holds (up to a fifth of the process's memory for each server in fresh
mode): on a 16GB machine, set `--workers 2`.

| Flag | Environment variable | Default | What it does |
|---|---|---|---|
| `--listen` | `BINTRAIL_PG_ROUTER_LISTEN` | `127.0.0.1:5433` | Where the port listens |
| `--tls-cert` | `BINTRAIL_PG_ROUTER_TLS_CERT` | none: a certificate made up at start | The certificate (PEM, its chain after it) a client that asks for TLS gets. Requires `--tls-key` |
| `--tls-key` | `BINTRAIL_PG_ROUTER_TLS_KEY` | none | Its private key (PEM, not encrypted with a passphrase) |
| `--workers` | `BINTRAIL_PG_ROUTER_WORKERS` | 4 | How many statements run on the copy at once (1 to 16). The rest wait their turn |
| `--threads` | `BINTRAIL_PG_ROUTER_THREADS` | 2 | Threads each statement may use (1 to 16) |
| `--sql-memory` | `BINTRAIL_CONSOLE_SQL_MEMORY` | 2GB, or the value saved in the web interface | Memory each statement may use |
| `--max-connections` | `BINTRAIL_PG_ROUTER_MAX_CONNECTIONS` | 256 | Clients connected at once (1 to 1024). One more is refused with an error |
| `--max-source-connections` | `BINTRAIL_PG_ROUTER_MAX_SOURCE_CONNECTIONS` | 16 | Connections to the servers' own PostgreSQL held at once, every server together (1 to 1024). A client holds one while its transaction block (or a catalog statement the copy does not answer) runs there; past the limit the statement that needed one is refused (`53300`) |
| `--max-wait` | `BINTRAIL_PG_ROUTER_MAX_WAIT` | none | How long a statement waits for its turn before it is refused |
| `--sql-port-max-rows` | `BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS` | the port's row cap | Most rows one statement returns |
| `--token` | `BINTRAIL_CONSOLE_TOKEN` | none | The access token, one more password. It does not open the port |
| `--servers-file` | `BINTRAIL_CONSOLE_SERVERS` | `~/.config/bintrail/console-servers.yaml` | The servers file the web interface manages. Read, never written |


### Session settings and query cancel on the PostgreSQL port

What a driver sets for its session, with `SET` or in its connection
parameters, is answered here as PostgreSQL answers it: the same `SET` and
`SHOW`, the same values, and a `ParameterStatus` to the client when a reported
setting changes. Nothing that would change an answer is taken and ignored:

| Setting | On this port |
|---|---|
| `TimeZone` (`SET TIME ZONE`) | **Applied.** The statement runs in the zone (a cast to `date`, `date_trunc`, a literal without an offset), and a `timestamptz` is written in it (`2026-01-01 05:00:00-05`). A name of the time zone database (`America/Bogota`; `america/bogota` is taken too), a name of UTC, or `GMT` with a whole-hour offset as JDBC writes one (`GMT-05:00`). An offset as a number (`SET TIME ZONE -5`) or an interval is refused: name the zone (`Etc/GMT+5` is five hours behind UTC). A zone the copy's engine does not know, or knows with other offsets than this host, is refused. One difference is known: the engine reads `Europe/Amsterdam` before 1937 without its local mean time (+00:19:32), so a cast or `date_trunc` of such an instant there is off by those minutes; the `timestamptz` itself is written right. |
| `search_path` (`SET SCHEMA`) | **Applied**, to one schema of the copy: unqualified names resolve there, `current_schema()` says it, time travel reads it. `"$user"` and `pg_catalog` may be in the path; a second schema, or one the copy does not have, is refused. |
| `statement_timeout` | **Applied.** A statement that runs past it stops with `57014 canceling statement due to statement timeout`. The port's own cap (five minutes) still applies. |
| `application_name`, `client_min_messages` | **Applied.** |
| `extra_float_digits` 1 to 3, `DateStyle` `ISO, MDY`, `client_encoding` `UTF8` (or `SQL_ASCII`), `lock_timeout`, `IntervalStyle` `postgres`, `bytea_output` `hex`, `standard_conforming_strings` `on`, read committed, read only | **Taken; they change nothing this port answers.** PostgreSQL 12 and later write the same digits for an `extra_float_digits` of 1, 2 or 3; the copy takes no lock. |
| `extra_float_digits` 0 or less, another `DateStyle`, `IntervalStyle`, `bytea_output` or client encoding, `standard_conforming_strings` off, `REPEATABLE READ`, `SERIALIZABLE`, `READ WRITE`, `SET ROLE`, `SET SESSION AUTHORIZATION` | **Refused, with the reason** (`0A000`): the port would answer otherwise than the setting says. Each statement reads the copy as it is when it runs, so no snapshot is kept across statements. |
| A name the port does not take | **Refused** (`42704`); `SHOW ALL` lists the ones it takes. |

A setting in the connection parameters (JDBC sends `TimeZone`, `DateStyle`
and `extra_float_digits` there; libpq sends `PGOPTIONS` as `-c name=value`) is
taken the same way, and is what `RESET` goes back to; a value refused there
ends the connection with the reason. In a transaction block a `SET` is undone
by `ROLLBACK` and by an error in the block, and a `SET LOCAL` ends with the
block, as on PostgreSQL. `SELECT version()` answers `PostgreSQL 14.0 ...`,
which is what SQLAlchemy (and Superset) read on connect.

**Query cancel works**: each connection is given a process number and a secret
key (`BackendKeyData`), and a cancel request with both, from `psql`'s Ctrl-C,
JDBC's `setQueryTimeout` or `cancel()`, psycopg's `cancel()` or `pgx`, stops
the statement the copy is running for that connection. It answers
`57014 canceling statement due to user request` and the connection stays
open. The request is taken in clear text and over TLS (`psql` 16 and psycopg
send it in clear text even on a TLS connection; it carries no password), and
also when the port has all the connections it takes.

Time travel reads its `AS OF` moment in UTC, so under another `TimeZone` it
is refused with the reason: `SET TimeZone = 'UTC'` first. Its columns are text,
written as the change history holds them. On `bintrail-pg flashback`, which
answers time travel only, `TimeZone` is UTC only, at connect too: start a JDBC
client with `-Duser.timezone=UTC`.

### Transactions on the source: the forwarding user

A transaction block a client opens on the PostgreSQL port (`BEGIN` or
`START TRANSACTION` to `COMMIT` or `ROLLBACK`) runs on the server's own
PostgreSQL, every statement in it, and so does a statement on the system
catalog that the port does not answer from the copy
([below](#listing-schemas-tables-and-columns-on-the-postgresql-port)), and
every statement outside a block that the copy is not known to answer as
PostgreSQL does
([below](#which-side-answers-a-statement-on-the-postgresql-port)). It needs the
server's **forwarding user**, a role of its own on the source:

```sql
CREATE ROLE app_ro LOGIN PASSWORD <a password>;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_ro;
```

```bash
bintrail-console server forward-user <server> -u app_ro -p
```

or in the web interface, edit the server (Forwarding user and password). It is
laid over the source's own address, database and parameters (`sslmode`
included), and must be another user than the one DBTrail captures with: the
port never runs anything as that one, which holds `REPLICATION` and reads every
published table. Without a forwarding user, every statement, a transaction
block's too, is answered by the copy alone, as before. Changing the forwarding user, or removing the
server, closes the connections that use it; clients reconnect.

- **Read-only, three ways.** A statement that is not a read is refused before
  it is sent (`25006`, naming `INSERT`, `UPDATE`, `CREATE`...). The connection
  to the source is opened with `default_transaction_read_only = on`, so
  PostgreSQL itself refuses what the text does not show (a function that
  writes, a `WITH` that deletes, `SELECT ... INTO`, `nextval`). The port watches
  that setting, which PostgreSQL 14 and later report, and closes the connection
  if anything turns it off. A source older than 14 is not forwarded to.
  `BEGIN READ WRITE` and `SET default_transaction_read_only = off` are refused.
- **Refused by name**: `COPY` (`0A000`; read rows with `SELECT`), `LISTEN` (a
  notification would reach the client only when it next sent a statement),
  `NOTIFY` and `pg_notify()`, `DO`, `PREPARE`/`EXECUTE`, advisory locks,
  `set_config()`, `dblink`, and names written with `U&"..."` escapes. A
  transaction statement inside a longer text (`SELECT 1; COMMIT`) is refused:
  send it on its own.
- **The source's answers as it gives them**: its rows as they come, never held
  whole in this process; its errors with their code, detail and position, which
  fail the block as on PostgreSQL; its notices (`RAISE NOTICE`). Savepoints
  (`ROLLBACK TO SAVEPOINT` brings a failed block back, and the session's
  settings to what they were at the savepoint, as psql's `ON_ERROR_ROLLBACK`
  and JDBC's `autosave` expect),
  cursors and the extended protocol work as on the source: a statement a driver
  prepared outside the block (pgx, JDBC) is prepared on the source the first
  time it runs inside one. When the copy had described its columns with
  another type than the source sends in binary, that execution is refused with
  `cached plan must not change result type` (`0A000`), the words drivers
  prepare again on, never sent as the wrong value.
- **The session goes with it**: `TimeZone`, `search_path`, `statement_timeout`,
  `lock_timeout`, `extra_float_digits`, `client_min_messages` and the default
  isolation are set on the source before a statement is sent, and the formats
  the port writes in (`DateStyle` `ISO, MDY`, `IntervalStyle`, `bytea_output`,
  UTF8) are the source connection's from the start.
- **Cancel and disconnect reach the source.** A cancel request stops the
  statement there (`57014`); a client that leaves in the middle of a
  statement has it cancelled there. A source that does not stop a cancelled
  statement within ten seconds has its connection closed.
- **At most one connection to the source per client**, opened when a
  statement needs it and given back as soon as nothing of the client's lives
  there (the block ended, the batch synced, no portal open), so a connection
  pool's clients do not hold one each for good; at most
  `--max-source-connections` across every server. The next one is given the
  session's settings again, and a prepared statement is prepared there again
  the first time it runs; a cursor `WITH HOLD`, which would outlive the block,
  is refused. A connection that is lost, or whose read-only guard was turned
  off, is not replaced behind the client's back: a statement that needs the
  source answers `08006 ... reconnect` (a block open at that moment fails, and
  only its `ROLLBACK` is answered), while reads on the copy go on.
- **Every wait for the source is bounded** by the port's five-minute cap on a
  statement, besides the client's `statement_timeout` and cancel.
- **When the source cannot be reached**: `08001 cannot connect to the source`;
  when it refuses the forwarding user, `08004` with PostgreSQL's reason (not
  `28P01`, which would read as the client's own password being wrong). The
  `BEGIN` fails and the connection stays open on the copy.
- **TLS to the source** follows the forwarding user's URL: the source's own
  `sslmode` when the user was laid over it, `prefer` when none is written.
- `REPEATABLE READ` and `SERIALIZABLE` are still refused, as on the copy:
  inside a block the port still answers some statements itself (`SHOW`,
  the catalog statements it recognizes, time travel).

### Which side answers a statement on the PostgreSQL port

With a forwarding user, a statement outside a transaction block is answered by
the copy only when everything in it is something the copy is known to answer
the way PostgreSQL does. What "known" means is measured, not assumed: the
comparison harness (`internal/pgcompare`) runs the same statements on a real
PostgreSQL and on its copy, and only what it found answering the same is let
through. When in doubt, the source answers: the cost is a slower answer, never
a different one.

The source answers:

- **A function, operator, cast or construct the copy answers differently or
  not at all.** The copy takes `count`, `sum`, `min`, `max`, `coalesce`,
  `nullif`, `greatest`, `least`, `length`, `substring`, `trim`, `position`,
  `concat`, `abs`, `bool_and`, `string_agg`, `row_number`, `rank`, `lag`, and
  `extract`, `date_part` and `date_trunc` of a year, month or day; the
  operators `=`, `<>`, `<`, `>`, `<=`, `>=`, `+`, `-`, `*`, `||`, `LIKE`
  (a pattern without a backslash), `IN`, `BETWEEN`, `IS [NOT] DISTINCT FROM`,
  `ANY`, `EXISTS`, an array subscript by a positive whole number; `::date` and `CAST(... AS date)`; `DATE`, `TIMESTAMP`, `TIME` and
  `INTERVAL` literals; joins, `GROUP BY`, `HAVING`, `DISTINCT [ON]`, `UNION`,
  `INTERSECT`, `EXCEPT`, `WITH`, window functions, `FILTER`, `LIMIT` and
  `OFFSET`. Anything else goes to the source. Some of what the harness found
  answering otherwise, and why each one goes there: `avg()` and division of a
  `numeric` (PostgreSQL keeps more decimals), `round()` of a
  `double precision` (it rounds a half to even), a division by a column or by
  zero (the copy answers NULL where PostgreSQL fails), `~` and `SIMILAR TO`
  (the copy matched no row), `->` and `->>` (PostgreSQL answers `json`, and
  writes a nested value with spaces), a number with an exponent (a `numeric`
  on PostgreSQL), `::text` of a
  float, `upper`, `lower` and `ILIKE` (PostgreSQL folds case by the database's
  character type), `COLLATE`, `AT TIME ZONE`, `FOR UPDATE`, `jsonb` operators,
  `generate_series` and every function the list does not name.
- **The clock and the session**: `now()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`,
  `LOCALTIMESTAMP`, `current_user` and the like. The copy's data is as old as
  its last update, and its clock and session are not the source's.
- **Text sorted or compared by order** (`ORDER BY` a text column, `min()` of
  one, `name > 'B'`): PostgreSQL orders text by the database's collation, the
  copy by bytes. Text compared for equality, grouped or matched with `LIKE`
  stays on the copy.
- **A session `TimeZone` other than UTC**, which the harness has not measured.
- **What the copy cannot run, before the client is told anything**: a table
  the copy does not hold, a statement the copy refuses (a reserved word of its
  own dialect, a value it does not read, such as `NaN` in a numeric or
  `24:00:00`), a copy that is busy or behind the server's lag limit. A
  result over the port's row or size cap is still refused, as before: the
  source would send it whole, past the caps. A column of a
  `numeric` declared without precision, which the copy writes with 18
  decimals, is sent to the source too.
- **What is not a query**: `EXPLAIN`, cursors and savepoints outside a block,
  `VALUES`, `TABLE`. A statement that writes is refused (`25006`). `SHOW
  TABLES` and `DESCRIBE`, the copy's own, stay on the copy.

**A server that hides tables or columns sends nothing to its source**, not a
statement the copy refuses, not a transaction block, not a catalog statement:
the rules are applied by the copy's views, and a statement run on the source
reads past them, whatever the forwarding user's grants (a view there runs with
its owner's rights, as a `SECURITY DEFINER` function does). Such a server is
answered by the copy alone, as without a forwarding user; a rule saved while a
block is open on the source fails that block. `/status` says so for the server
(`forwarding_off`), and the log once.

A prepared statement keeps the side it was given once its columns were
described: a driver that prepared it once (pgx, JDBC) is not told other
columns later. One that the copy refused at its first execution runs on the
source from then on.

#### The port's status

```bash
bintrail-console pg-router --status-addr 127.0.0.1:9092 [--status-statements]
curl -s http://127.0.0.1:9092/status
```

`/status` is JSON: per server (by its id), how many statements each side
answered (`copy`, `source`, `refused`), why (`reasons`), and per side how many
finished, how many got an error and how long they took (`answers`), and its
mode: `sql_mode` (`fresh` or `fast`) and where that comes from,
`sql_mode_from` (`environment`, `saved` or `default`). Every PostgreSQL server
is listed with its mode, whether or not a statement reached it yet. The
document also says `fresh` (true when `BINTRAIL_SQL_TAILS` puts every server in
fresh mode) and `sql_mode_forced` (`fresh` or `fast` when that setting decides
for every server), as the MySQL router's does. How old the data of a server's
last answer was is `data_as_of` (every change capture had recorded up to
this instant, a commit time on the source, was in what it read) and
`data_age_seconds` (how long before the answer that was, by this machine's
clock), with `data_age_max_seconds` the worst since the process started. Only
an answer the client got counts, never a statement the source answered or one
that ended in an error. An answer that read a table without its newest
changes (fast mode, among others) has no age: `data_age_unknown` counts them,
so with any the worst is a floor, and `data_age_unknown_reason` says why the
last one has none. On a source with no write since, the age grows though
nothing is missing; this machine's clock ahead of the source's makes it
larger, and behind it smaller, down to 0. With
`--status-statements` each server also carries `statements`: a row per
statement shape and side, values blanked and table and column names kept. That
address asks for no password: whoever reaches it reads the counts, and with
`--status-statements` the names of the tables and columns your clients query.
`/metrics` on the same address is this process's Prometheus metrics.

The reasons, with the MySQL port's names where they mean the same:

| Side | Reason | Why |
|---|---|---|
| copy | `answers_the_same` | everything in it answers on the copy as on PostgreSQL |
| copy | `copy_command` | a command of the copy's own (`SHOW TABLES`, `DESCRIBE`) |
| copy | `no_forward_account` | the server has no forwarding user: the copy answers alone |
| copy | `hidden_rules` | the server hides tables or columns: nothing is sent to its source |
| source | `in_transaction` | the client's transaction block |
| source | `veto` | a construct the copy answers differently, or is not known to answer the same |
| source | `session_differs` | a session setting the copy is not known to reproduce (`TimeZone`) |
| source | `not_a_select` | `EXPLAIN`, a cursor, a savepoint, `VALUES` |
| source | `catalog_not_on_copy` | a catalog statement the copy does not answer |
| source | `table_not_on_copy` | a table the copy does not hold |
| source | `copy_refused` | the copy could not run it |
| source | `copy_columns_differ` | a column the copy writes otherwise (a `numeric` without precision) |
| source | `copy_behind_source`, `copy_age_unknown` | the server's lag limit |
| source | `copy_queue_full`, `copy_wait_timeout` | the copy was busy |
| refused | `read_only` | not a read |

What the copy would answer and the source did not make faster is not decided
here yet: every statement the copy can answer goes to the copy, however cheap
it would be on the source. Deciding by the source's plan, as the MySQL port
does, would cost a login to the source for each statement, since a connection
holds one there only while something of its own lives on it.

### Listing schemas, tables and columns on the PostgreSQL port

The statements tools send to list what a server holds are answered by the
port from the copy's real tables: psql's `\l`, `\dn`, `\dt` (with or without
a pattern), `\d`, `\d <table>` and `\d+ <table>`; JDBC's `getSchemas`,
`getCatalogs`, `getTables`, `getColumns` and `getPrimaryKeys` (what DBeaver and
Metabase call); SQLAlchemy's inspector (`get_schema_names`, `get_table_names`,
`get_view_names`, `get_columns`, `get_pk_constraint`, `has_table`,
`get_foreign_keys`, `get_indexes`, which Superset calls); and Grafana's table
and column lists. They were checked against PostgreSQL 16 with psql 16, JDBC
42.7, SQLAlchemy 1.4 with psycopg2 and Grafana's queries: for the copy's tables
each prints what it prints for the source's, but for what follows.

- **What the copy can be read for, and only that**: the tables and columns a
  statement on the copy reads. A table or a column the server hides is not
  listed, and neither is a primary key one of whose columns it hides. The
  copy's own schema (`main`, where the change log is) is not listed.
- **Each column has the type a result announces for it** (see
  [above](#sql-on-the-copy-from-psql)), so
  what a tool lists is what it then reads: a `varchar(n)`, `jsonb`, enum or
  `interval` column is listed as `text`.
- **`NOT NULL`, the default and the primary key are the source's**, as the
  snapshot recorded them, and so is the table's OID. The primary key is named
  as PostgreSQL names one by default (`<table>_pkey`): the snapshot does not
  record its name. A snapshot taken before DBTrail recorded a table's shape
  lists the table without them (a warning in the log names it); the next
  snapshot records them.
- **What the copy does not have is not listed**: no other index, no foreign
  key, view, sequence, policy, publication, comment, enum or domain, and no
  access method. The owner of everything is the user the client connected
  as, and the one database is the one it connected to, with collation `C`
  (text compares byte by byte here).
- **Any other statement on the system catalog is refused** (`0A000`) by name:
  `pg_catalog`, `information_schema` and the `pg_` catalog tables are not run
  on the copy, whose engine has a catalog of its own that would answer with
  its internal schemas and types. `SHOW TABLES` and `DESCRIBE <table>` list
  the copy's tables and columns in its engine's terms. A client version that
  writes these statements otherwise than the versions above is refused the
  same way.
- `\d` of an index is refused: the copy has no index; `\d <table>` shows the
  primary key. A connection reads the catalog once for a burst of statements:
  a table hidden while it is connected leaves its listing within 10 seconds,
  and is never readable.

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
  Full-table `AS OF` is refused with remediation: the shim's whole-table read
  needs the `CREATE TABLE` metadata a PG baseline does not carry. A whole
  table at a point in time is `reconstruct --output-format parquet` (see
  [Full-table snapshots](#full-table-snapshots)).
- Columns render as **text**, on purpose: a row as it was can carry other
  columns, and other types, than the table has now. Read each one as a string,
  or let your driver text-parse it into a typed target. One caveat: a
  binary/`bytea` value containing a `0x00` byte truncates at the first NUL in
  `psql`/libpq (their text protocol uses NUL-terminated C strings); a driver that
  reads the raw value (e.g. `pgx` scanning into `[]byte`) round-trips it intact.
- Both the **simple and the extended query protocol** are served, so a driver
  in its default mode can prepare `SELECT * FROM _flashback.orders AS OF $1
  WHERE id = $2` and run it with arguments. An argument cannot hold a single
  quote (`'`) here: write such a value in the statement's text.
- **Login** is SCRAM-SHA-256 against each tenant's password in
  `--shim-config` (`shim.yaml`), as on `pg-router`: the password never crosses
  the network. A client that asks for TLS gets it, with `--tls-cert` and
  `--tls-key` (`BINTRAIL_PG_FLASHBACK_TLS_CERT`, `_KEY`) or with a certificate
  made up at start; one that asks for none is answered in clear text. The
  default listen address is loopback, and a bind other machines reach is
  warned about at start.

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
run across the PG 15/16/17/18 CI matrix:

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
so `bintrail-pg recover`, the web interface, and the MCP server all emit valid PostgreSQL
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

### How each type reads in SQL on the copy

A snapshot of a PostgreSQL source holds every value as the text PostgreSQL
wrote, which is what keeps recovery exact. SQL on the copy (the web
interface's SQL page, `bintrail views`, the views file inside each snapshot)
reads each column as the type the table declares, in the table's own column
order, from the shape the snapshot records beside the data. Nothing is
rewritten: a snapshot that records the shape reads this way whichever
version took it. One taken before the shape was recorded reads as text, in
alphabetical column order, until the next full snapshot; the log names such
tables once.

| PostgreSQL type | Reads as |
|---|---|
| `smallint`, `integer`, `bigint` | whole numbers of the same width |
| `numeric(p,s)` up to 38 digits | exact decimal with that precision and scale |
| `numeric` with no precision | exact decimal with 20 digits before the point and 18 after |
| `real`, `double precision` | floating point, `NaN` and `Infinity` included |
| `boolean` | boolean |
| `date`, `timestamp`, `timestamptz` | date and timestamps; `infinity` included |
| `time`, `timetz` | time of day, with its offset for `timetz` |
| `interval` | interval, with its months, days and time kept apart as PostgreSQL keeps them |
| `uuid` | uuid |
| `bytea` | bytes |
| `text`, `varchar(n)`, `char(n)` | text compared byte by byte, so `'Lima'` is not `'lima'` |
| an array of any type above but `bytea`, `numeric` with no precision, `time`, `timetz` and `interval` (`text[]`, `integer[]`, `date[]`, ...) | a list of that type: `labels[1]` is the first element, `'red' = ANY(labels)` works |
| everything else | the text PostgreSQL wrote |

"Everything else" is `json` and `jsonb`, enums, domains, ranges, a `numeric`
wider than 38 digits, with a negative scale or with a scale above its
precision, every extension type, and an array of any of these, of `bytea`,
of `numeric` with no precision, of `time`, `timetz` or `interval`.
`->>` reads a `json` column as it is; an array left as text is the text
`{a,b}`, so `labels[1]` is its first character, not its first element. A
column left as text still compares byte by byte. Each view names the columns
it left as text.

Two kinds of table are read as text throughout: one with two columns whose
names differ only in case (`"Id"` and `"id"`), which the copy cannot tell
apart, and any table in a views file that follows later snapshots
(`bintrail views` without `--pin-snapshot`), because a type written into
such a file would outlive a change of the column's type at the source.

A value its type here cannot hold fails the statement that reads it, with an
error that says so. It is never rounded and never read as NULL:

- `NaN` or `Infinity` in a `numeric` column.
- A `numeric` with no precision holding more than 18 decimals or more than
  20 digits before the point.
- A `date`, `timestamp` or `timestamptz` before the common era, or beyond the
  years the copy's types reach.
- A `bytea` not written as `\x` and hex digits.
- The `time` `24:00:00`, which the copy would show as midnight. A `timetz`
  of `24:00:00` reads as it is.
- An `interval` of `infinity` (PostgreSQL 17).
- An array of more than one dimension (`'{{1,2},{3,4}}'`), or one whose
  first element is not numbered 1 (`'[0:1]={1,2}'`).

The elements of a text array compare as the session compares text. On the
SQL card of a PostgreSQL server that is byte by byte. In a DuckDB of your
own with `default_collation = 'nocase'`, `labels[1] = 'RED'` matches `red`,
where a text column of the same views does not.

A statement that does not read that column is not affected, and neither is
recovery, which works from the text.

In the console's SQL card and the `sql` tool, a statement on a PostgreSQL
server runs by PostgreSQL's rules where the two engines differ in silence:
NULL is the largest value (last in `ORDER BY x`, first in `ORDER BY x DESC`),
`/` between two integers gives an integer cut toward zero (`9 / 2` is `4`,
`-9 / 2` is `-4`), and text compares byte by byte (`'a' = 'A'` is false). A
views file you run in a DuckDB of your own runs under that DuckDB's settings;
these three lines give it the same rules:

```sql
SET default_null_order = 'nulls_last_on_asc_first_on_desc';
SET integer_division = true;
SET default_collation = '';
```

Three things still answer differently from PostgreSQL, with no error:

- A division by zero is NULL, where PostgreSQL fails the statement.
- Text sorts by bytes. PostgreSQL sorts by the collation the database was
  created with, which for anything but `C` orders letters differently.
- `char(n)` compares with its padding, where PostgreSQL ignores trailing
  blanks.

A numeric with no precision prints with all 18 decimals (`12345.678` as
`12345.678000000000000000`); it compares, sorts and sums as the same number.

---

### Names the copy reads as its own words

The copy's SQL has words PostgreSQL does not, and a table or column with one of
them as its name is refused by the copy with a syntax error when the name is
written without quotes. PostgreSQL takes the same statement. Measured against a
real PostgreSQL with the comparison harness (`internal/pgcompare`), 169
statements: 73 refused, none answered differently.

- **Tables** named `pivot`, `unpivot`, `qualify`, `asof`, `anti`, `show`,
  `describe` or `summarize`: `SELECT id, v FROM pivot` is refused, and so is
  `FROM pivot AS t`.
- **Columns** named `at`, `by`, `anti`, `semi`, `asof`, `positional`,
  `qualify`, `pivot`, `pivot_longer`, `pivot_wider`, `unpivot`, `unpack`,
  `glob`, `lambda`, `show`, `describe` or `summarize`: `SELECT id, at FROM t`,
  `WHERE describe IS NOT NULL` and `sum(by)` are refused.

Written another way, each of them answers as PostgreSQL does: the name in
double quotes (`"at"`, `FROM "pivot"`), the table with its schema
(`FROM public.pivot`), or the column with its table (`t.at`). `SELECT *` is not
affected.

On the PostgreSQL port, a statement with `qualify`, `asof`, `anti`, `semi` or
`positional` anywhere in it goes to your PostgreSQL when the server has a
forwarding user, which answers it; without one, and for the other names, the
client gets the copy's syntax error (SQLSTATE `42601`).

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
  `reconstruct` whose window crosses the truncate refuses instead of bringing the
  removed rows back. A multi-table `TRUNCATE a, b` records one marker per
  published table; a truncate of an out-of-scope table records nothing.
- **Other schema changes are inferred, not read.** See
  [Schema changes on PostgreSQL](#schema-changes-on-postgresql).
- **Sequence cursors are not captured.** The materialized id values in your rows
  *are* captured; the sequence's own `last_value` (a separate catalog object) is
  not — logical decoding does not replicate sequences. After a restore, re-point
  each `SERIAL`/`IDENTITY` sequence past `MAX(id)` — recipe and when it matters
  in [Sequences after recovery](#sequences-after-recovery).
- **`GENERATED ... AS IDENTITY` / generated columns** can need care on recovery
  (`GENERATED ALWAYS AS IDENTITY` rejects an explicit insert; `STORED` generated
  columns are absent from the stream before PostgreSQL 18). Treat as best-effort.
  The copy never holds a generated column: the database computes it. On
  PostgreSQL 18 a table with a `VIRTUAL` generated column cannot be captured,
  and one with a `STORED` one needs `publish_generated_columns = stored` on the
  publication (see step 3 of the setup).
- **Full-table `reconstruct` writes snapshots** (`--output-format
  parquet`, and `baseline refresh`) **or SQL psql loads** (`--output-format
  psql`); a mydumper dump stays MySQL's (see
  [Full-table snapshots](#full-table-snapshots)). `bintrail-pg drill`
  rehearses the restore into a scratch PostgreSQL from the psql files.
  `verify` is supported too: baseline-anchored, live-source, and
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
- **No BYOS agent.** The web interface captures PostgreSQL sources (**+ Add
  server** → PostgreSQL); `bintrail agent` (BYOS) does not support PostgreSQL.

The data-safety gates for PostgreSQL are closed
([#597](https://github.com/dbtrail/dbtrail/issues/597)): the data-safety set
(type fidelity, identity/generated recovery, slot/WAL monitoring, RI-FULL
validation, DDL-drift handling, the silent-loss coverage guards above),
baseline-anchored single-row `reconstruct` / time-travel with the
baseline↔delta rendering-GUC identity and its fold-validated type matrix
(#593), the managed-PostgreSQL smoke matrix (RDS and Aurora — see
[Managed PostgreSQL](#managed-postgresql)), and source-aware web interface
presentation including the live replication-health panel (v0.20.1).
**Full-table** `reconstruct` writes snapshots (`--output-format parquet`, and
`baseline refresh`) or SQL psql loads (`--output-format psql`); a mydumper dump
stays out of scope (above). `verify`
(baseline-anchored, live-source and recover-input) is supported.

---

## Schema changes on PostgreSQL

PostgreSQL's change stream carries row changes, not schema statements: no
`CREATE TABLE`, `ALTER TABLE` or `DROP TABLE` ever reaches bintrail-pg as text.
What the stream does carry is a description of each table (its name and its
columns with their types) sent before the first change made under a new shape.
bintrail-pg compares each description with the last one it saw for the same
table, identified by the table's internal number (its OID, which survives a
rename), and records what changed in `schema_changes`:

| What the stream shows | Recorded as |
|---|---|
| Same table, new name or new schema | `RENAME TABLE` (one row under each name) |
| Same table, other columns | `ALTER TABLE`, listing the columns added, dropped and retyped (a renamed column reads as one dropped plus one added, and the text says so) |
| A new table under a name another table held, the old one gone | `DROP TABLE` then `CREATE TABLE` |
| A new table under a name another table held, the old one still there under another name | `RENAME TABLE` for the old one, then `CREATE TABLE` |
| A table that was not in the publication when capture started | `CREATE TABLE` (labelled when it is a partition), or `ALTER PUBLICATION` when the catalog shows it was added to the publication after it existed |
| A table taken out of the publication and added back | `ALTER PUBLICATION`, marked `gap=uncaptured`: its changes while it was out were never captured, and `reconstruct` and the snapshot-pair `verify` refuse a window across it |
| A `TRUNCATE` | `TRUNCATE TABLE` |

Nothing is installed in your database for this; it is read from the stream
alone.

**Which rows belong to which shape is exact.** PostgreSQL decodes every
change with the table as it was when that change was made, so a row is never
assigned to the wrong set of columns.

**When the change happened is a range, not an instant.** The stream says the
change happened after the last change made under the old shape and at or
before the first one under the new shape, nothing finer. Each inferred row
starts with a marker carrying that range, for example:

```
/* dbtrail: inferred from relation shape; oid=16411; instant=at-or-before; low=0/16B3748@2026-10-17T10:00:00Z; high=0/16B9A20@2026-10-17T10:20:00Z */ ALTER TABLE shop.orders: added note (text)
```

**The exact instant, when the server keeps commit times.** With
`track_commit_timestamp = on` (a server setting that needs a restart; on
managed PostgreSQL, check that your provider lets you set it; `bintrail-pg
doctor` says whether it is on), bintrail-pg reads from the catalog when the
statement that gave the table the shape the stream shows committed, and the
marker says `instant=exact; at=<time>`, the range still beside it. When several
changes fall in one range (a column added, then retyped), that is the last of
them. It does so only where the catalog can prove it:

- an added column, or a retyped one whose change rewrote the table, when the
  column's catalog row and the table's still agree and the column has no
  column-level grant (a later statement on that column, such as `SET
  STATISTICS` or `SET NOT NULL`, or on the table, such as a `GRANT`, moves one
  of them; a retype that does not rewrite the table, such as `varchar(10)` to
  `varchar(20)`, is never exact);
- a dropped column, whose catalog row is never written again;
- a rename or a schema move, from the table's row type, which a later `GRANT`
  or column change does not move;
- a new table, from its row type, when the table's earliest column carries the
  same time.

Not caught, and read as the change's time: an `ALTER ... OWNER TO` later in the
range for a rename, and one later statement that rewrites both a changed
column and the table (a multi-part `ALTER`). The catalog must also show the
table with the columns the stream does (when bintrail-pg is catching up past a
later change, it does not), and the time must lie inside the range. Anything
else stays "at or before". A renamed column is never exact. Reading the times
uses a connection of its own and never stops capture: if the catalog cannot
answer, bintrail-pg logs a warning (at most every ten minutes, with how many
reads failed) and dates the rows by range.

The row's `detected_at` is the exact instant when there is one; otherwise the
start of the range (the end, when the range has no start). Its stream position
is the end. `reconstruct` places an inferred row by its range either way. `reconstruct` refuses a point in
time whose window overlaps the range of an inferred `DROP TABLE` or `RENAME
TABLE`, since the table may or may not have changed yet at that point; the
refusal says so and says which points in time can still be rebuilt. A
`TRUNCATE TABLE` is reported by PostgreSQL with its exact transaction, so it
has no range and refuses exactly where it happened. A table that was not written
to for a while after the change has a wide range.

**Tables added to the publication.** A table added to a `FOR TABLE`
publication has a row in the catalog written when it was added, and with
`track_commit_timestamp = on` bintrail-pg reads its time. A table first seen
after capture started whose catalog row is later than the table itself is
recorded as added (`ALTER PUBLICATION`), not created; its changes from before
are outside what capture covers, like the changes before capture started. A
known table seen again after it was added back is recorded with the gap, so a
rebuild that would need the changes made while it was out refuses instead of
missing them. A table that no change has shown yet is caught too when it was
in the publication when capture started and its row is later than that. When
the catalog shows an add later than the change being read (bintrail-pg is
behind), the stretch is refused as well, since the table may also have been
out before. Changing a table's row filter or column list (PostgreSQL 15+)
rewrites the same row and reads the same way; the row says so. Not seen: a
table added back while `track_commit_timestamp` is off, or while the catalog
cannot be read (bintrail-pg warns); a table no change had shown, taken out and
added back while bintrail-pg was stopped; a table taken out again and not yet
added back; tables covered by a `FOR ALL TABLES` or `FOR TABLES IN SCHEMA`
publication or through a published parent (they are not added one by one); and
a server clock stepped back across the add, since the add is placed by time.
The live-source `verify` compares with the source as it is now and does not
read these rows: a gap shows there as a difference.

**What cannot be seen.** Anything that changes neither a column's name or type
nor the table's name or schema: indexes, constraints, defaults, `NOT NULL`,
ownership, grants, comments, `SET LOGGED` / `UNLOGGED`. A `DROP TABLE` that is
not followed by a new table of the same name leaves no row: the table simply
stops appearing. A table created while bintrail-pg was stopped is taken as
already existing when capture starts again, so it gets no `CREATE TABLE` row.
On the first restart after upgrading to a version that records table
identities, a table whose stored shape predates that is matched by name: if
its columns differ, the change is recorded as an `ALTER TABLE` whose text says
it could also have been a drop and a create, and `reconstruct` does not refuse
on it the way it does on a `DROP TABLE`. The statement text and the role that
ran it are never available.

**If detection cannot start.** What detection needs is read when capture
starts. If that read fails (for example a very slow index database),
bintrail-pg captures rows as usual and does not detect schema changes until
its next restart, and logs that at ERROR level. The table shapes seen during
that run are stored without their position in the stream, so the next start
compares against the last shapes detection did see, and a change made in
between is still found then (its range covers the whole run).

**After a restart.** PostgreSQL resends changes from the point it last
confirmed, which can be earlier than what bintrail-pg had already recorded.
Each table description is compared with the shape that table had just before
it in the stream, so a resent stretch is read exactly as it was the first time,
and a change already recorded is not recorded again.

**Changes that look like DDL but are not.** Narrowing a publication's column
list (`ALTER PUBLICATION ... SET TABLE t (a, b)`, PostgreSQL 15+), or on
PostgreSQL 18 turning `publish_generated_columns` on or off, changes the
columns the stream carries without changing the table. It is recorded as an
`ALTER TABLE`.

**Where names come from during catch-up.** When bintrail-pg is behind and a
name moved to a new table, whether the old table was dropped or renamed (and
to what) is read from the database as it is now, which can be later than the
point being processed. The row says so.

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
- [MariaDB as a source](mariadb.md) — the sibling source.
