# Support scope

This document is the canonical statement of what the DBTrail project does
and does not support. Issue triage links here; reports outside this scope
are closed with a pointer to this file.

## The contract

DBTrail's contract with your infrastructure is one line:

> **A reachable MySQL 8.0+ via `--index-dsn`.**

DBTrail installs and versions **its own schema** on that server — databases
(`CREATE DATABASE IF NOT EXISTS`), tables, and idempotent migrations, via
`init`, `up`, and the console control plane. The **bintrail binary** never
installs, supervises, or operates a mysqld **on the host** (no apt/yum
mysql-server, no managed daemon) — that boundary is architectural and
permanent.

The project does **ship** a packaged MySQL: the bundled `docker-compose.yml`
includes a pinned, containerized MySQL 8.4 as the default index store (BYO via
`--index-dsn` stays co-equal). Shipping it is **not** operating it — see the
ship-vs-operate boundary below.

## In scope (we own this)

- The bintrail binaries and their commands, flags, and documented behavior.
- The index **schema**: its tables, migrations, and data correctness
  (every row event, full before/after images).
- DBTrail's own tooling for operating the index *data*: `rotate`, `status`,
  `doctor`, `archive reconcile`, Parquet archives and their queries.
- The recovery and consistency tooling: `query`, `recover`, `recover-cascade`,
  `reconstruct`, `baseline`, and `verify` (the data-consistency check that
  proves a recovery would reproduce the source).
- The web console, the MCP server, the time-travel shim.
- The Docker images we publish, **including the pinned MySQL 8.4 index image
  bundled in docker-compose**: its build, tuned defaults, and documented
  upgrade path. We ship and version that image; we do not operate your running
  instance of it (see ship-vs-operate below).

## Out of scope (the operator owns this)

**Operating the running index MySQL** is the operator's responsibility in the
free core — and this holds whether you bring your own server OR run the MySQL
8.4 image we bundle. Shipping the image is not operating it. All of it is
yours:

- Sizing, InnoDB tuning, and capacity planning — the math is documented
  in [Capacity Planning](./capacity.md); running the numbers and the
  disk is yours.
- Backups and restore of the index server, and replication of the index
  itself.
- Disk-full conditions, corruption, and crash recovery of the server.
- Executing MySQL version upgrades (including the bundled 8.0→8.4 path; the
  8.4 datadir is non-downgradable), and distribution/managed-flavor quirks
  (RDS, Aurora, Cloud SQL, MariaDB) of the **index** server.

The **supported surface is two cells, both tested in CI** (the 8.0/8.4
integration matrix): (1) any **MySQL 8.0+ reached via `--index-dsn`** (BYO),
and (2) the **pinned MySQL 8.4 image we bundle**. We own those two images'
builds and the schema/migrations on them; the *operation* of whatever server
you actually run — BYO or bundled — is not part of our defect matrix.

> Want the index **operated** for you — sized, backed up, upgraded, kept
> alive on-call? That is exactly what the managed service at
> [dbtrail.com](https://dbtrail.com) is for.

## The bundled MySQL 8.4 index: we ship it, you operate it

The root `docker-compose.yml` bundles a pinned, containerized **MySQL 8.4** as
the default index store so the four-line quickstart works with zero
prerequisites. It holds the forensic record (`binlog_events` with full
before/after images) — **it is your system of record, not a throwaway.**
The boundary triage cites:

- **We ship it.** We package, pin, and tune the image, generate its
  credentials (no static default), and document its `8.0→8.4` upgrade path.
  Bugs in *that* — the image build, the tuned defaults, the migration runbook
  — are ours.
- **You operate it.** The running instance is yours: disk and disk-full,
  backups and restore, corruption and crash recovery, and *executing* the
  upgrades we document. **Volume loss = re-index** (back up the
  `bintrail-index-data` + `bintrail-index-secret` volumes together). The
  managed tier at [dbtrail.com](https://dbtrail.com) operates it for you.
- **BYO stays co-equal.** Set `INDEX_DSN` in `.env` to a MySQL 8.0+ you run
  and remove the bundled `index-init` + `index-mysql` services (see
  [docker.md](./docker.md)) — same ship-vs-operate split, your server.

## Source server configuration (required for correct capture)

DBTrail captures changes from your **source** database — **MySQL**, **MariaDB**
(alpha), or **PostgreSQL**; see [Supported source families](#supported-source-families)
below. The requirements here cover a **MySQL** (and MariaDB) source's ROW-format
binary logs; PostgreSQL's capture requirements (logical replication, `wal_level`,
`REPLICA IDENTITY`) live in [docs/postgres.md](./postgres.md). Faithful capture
requires the source to be configured **server-wide** (not just per-session):

- `binlog_format = ROW`.
- `binlog_row_image = FULL`. Partial images (`MINIMAL`/`NOBLOB`) — **including from
  a per-session `SET SESSION binlog_row_image = MINIMAL`** while the global is `FULL`
  — are **out of scope**: DBTrail indexes incomplete before/after images as if
  complete, so `recover` emits NULLs for unchanged columns and its `WHERE` clause
  matches nothing.
- `binlog_row_value_options` must **not** include `PARTIAL_JSON` — partial JSON
  updates log only a diff, leaving no complete after-image to recover from.

DBTrail's startup preflight and `bintrail doctor` validate the `binlog_row_image`
they see on bintrail's own connection; bintrail can't observe what other
application sessions set, so preventing per-session overrides is the operator's
responsibility. Data captured while the source violated these requirements is out
of scope — configure the source (see
[deployment.md → Source MySQL Requirements](./deployment.md#2-source-mysql-requirements))
and re-index.

## Supported source families

DBTrail captures from three source families. **All of them index into the same
MySQL index** — there is no per-source index store, and the index schema is
identical across sources (that portability is deliberate). Pointing `--index-dsn`
at a non-MySQL server is **not** supported.

| Source | Status | Capture mechanism | Guide |
|---|---|---|---|
| **MySQL** 8.0+ (incl. Percona, RDS, Aurora, Cloud SQL) | Supported | ROW-format binlog over the replication protocol | [streaming.md](./streaming.md) |
| **MariaDB** (target 11.4) | Alpha | ROW-format binlog (MariaDB GTID) | [mariadb.md](./mariadb.md) |
| **PostgreSQL** | Supported — via the separate `bintrail-pg` binary | Logical replication (`pgoutput`) | [postgres.md](./postgres.md) |

**We install nothing in your source database.** DBTrail connects as an ordinary
read-only replication client. For PostgreSQL specifically, capture uses the
**built-in `pgoutput`** logical-decoding plugin only — DBTrail never installs a
custom output plugin, runs `CREATE EXTENSION`, adds an event trigger, or places
any other server-side component in your source, and it never writes to the
source. This keeps managed PostgreSQL (RDS/Aurora/Cloud SQL) in scope. We
**validate** the source's configuration (`wal_level`, `REPLICA IDENTITY`, the
publication); we never create it for you.

**Operating the source-side capture prerequisites is the operator's
responsibility** — the same ship-vs-operate split as the index. In particular, a
PostgreSQL logical replication **slot retains WAL** until DBTrail consumes it: if
`bintrail-pg` is stopped long enough, the slot can fill the source's disk. Sizing
`max_slot_wal_keep_size`, monitoring slot lag, and the source's own disk are
yours (`bintrail-pg doctor` reports slot/WAL health — see
[postgres.md](./postgres.md)). Capturing from a misconfigured source
(non-`FULL` row image, missing `REPLICA IDENTITY`, `PARTIAL_JSON`) is out of scope.

## Reporting issues

Bugs in DBTrail's binaries, schema, tooling, console, or docs: please open
an issue with reproduction steps — those are always in scope. If your report
is about the index MySQL server's own operation (disk, backups, upgrades,
corruption), or about data captured under an unsupported source configuration
(non-`FULL` row image, `PARTIAL_JSON`), see the lists above first.
