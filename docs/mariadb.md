# MariaDB as a source (beta)

bintrail can capture from a **MariaDB** server while the index database stays
MySQL. This is a **beta** capability: the happy path is verified end-to-end
against real MariaDB, but it has documented limitations (below) and narrower
version/topology coverage than the MySQL path. Read the limitations before
pointing it at production.

**Scope:** MariaDB is supported as a **source** (the database you capture
changes from). The **index** — where bintrail stores the indexed events — stays
**MySQL**. Pointing the index at MariaDB is not supported. Because the index is
MySQL, the index-side tooling works unchanged: `query`, `recover`,
`recover-cascade` (FK cascade recovery), and `verify` (consistency check) all
apply to a MariaDB source.

---

## Quickstart

No flag is needed. On start, bintrail asks the source `SELECT VERSION()` and
captures as MariaDB when the answer says MariaDB, and as MySQL otherwise
(Percona and Aurora MySQL count as MySQL). This applies to `stream`, `up`,
`agent`, `bintrail-console watch` and servers added in the web interface.

`--source-flavor mysql|mariadb` (or `BINTRAIL_SOURCE_FLAVOR`) is optional. If you
set it and the server says otherwise, bintrail refuses to start and names both.
If the server cannot be asked, bintrail refuses unless you set it; with it set,
it starts with a warning.

**Live streaming** (the common case — works against managed MariaDB too):

```bash
bintrail stream \
  --source-dsn 'dbtrail:pw@tcp(mariadb-host:3306)/' \
  --index-dsn 'user:pw@tcp(index-host:3306)/binlog_index' \
  --server-id 200 --schemas shop
```

**File-based backfill** of on-disk MariaDB binlogs:

```bash
bintrail index \
  --binlog-dir /var/lib/mysql --files mariadb-bin.000042 \
  --source-dsn 'dbtrail:pw@tcp(mariadb-host:3306)/shop' \
  --index-dsn 'user:pw@tcp(index-host:3306)/binlog_index'
```

`query`, `recover`, and `reconstruct` then work exactly as they do for a MySQL
source — they read the index, which is flavor-agnostic.

---

## Requirements

Identical to a MySQL source (see [Streaming → The Source MySQL User](streaming.md#the-source-mysql-user)):

| Requirement | Notes |
|---|---|
| `binlog_format = ROW` | Validated at preflight; `bintrail` refuses to start otherwise. |
| `binlog_row_image = FULL` | Set it **server-wide** (`SHOW VARIABLES LIKE 'binlog_row_image';`). MariaDB defaults to `FULL`, but verify. |
| `log_bin = ON` | Binary logging must be enabled. |
| Source user grants | `REPLICATION SLAVE, REPLICATION CLIENT, SELECT` — the same set as MySQL. Add `RELOAD, SHOW VIEW` for baselines (`LOCK TABLES, SHOW VIEW` with `lock-all` on managed MariaDB). MariaDB has no `BACKUP_ADMIN` and never issues `LOCK INSTANCE FOR BACKUP`, so on a self-hosted MariaDB the default `ftwrl` mode needs only `RELOAD`/`FLUSH_TABLES`. RDS for MariaDB is different: its `RELOAD` does not allow `FLUSH TABLES WITH READ LOCK`, so use `lock-all` there. |

> MariaDB does not have a `server_uuid` system variable. bintrail detects this
> and emits a benign `WARN … MariaDB source has no @@server_uuid; synthesized a
> stable bintrail_id anchor …` at startup — expected, not an error. (You will
> *not* see a raw `Unknown system variable 'server_uuid'` driver error: bintrail
> swallows it and synthesizes instead.) Because MySQL's stable identity anchor is
> absent, bintrail **synthesizes a stable `bintrail_id` from the source address**
> (`host:port`) instead. See [Server identity on MariaDB](#server-identity-on-mariadb)
> below — it matters when you capture **more than one** MariaDB server.

---

## Server identity on MariaDB

On MySQL, bintrail derives each server's stable `bintrail_id` from `@@server_uuid`.
MariaDB has no such variable, so bintrail **synthesizes the identity anchor from
the source address** (`host:port`) and runs it through the same registration
logic. The result is a normal `bintrail_id` recorded in `bintrail_servers` and
`stream_state` — identical downstream behavior to MySQL.

Two consequences worth understanding:

- **Stable across restarts.** The same MariaDB server (same address) always
  resolves to the same `bintrail_id`, so resume and archive paths are stable.
- **Distinct per server — this is what keeps two servers apart in S3.** Parquet
  archives are written under `bintrail_id=<id>/event_date=…/`. Two MariaDB
  servers reached at distinct addresses synthesize **distinct** `bintrail_id`s
  and land in **distinct** S3 prefixes automatically — no manual bookkeeping, no
  collision.

> **The address must actually differ per server.** Because the anchor is
> `host:port`, two *different* MariaDB servers reached through the **same**
> address (a shared proxy/VIP, or both via a `127.0.0.1` tunnel) synthesize the
> **same** anchor and would collide under one prefix. Give each server a distinct
> address, or pass an explicit `--bintrail-id` per server.

When you archive with `bintrail rotate --archive-dir … [--archive-s3 …]`, you no
longer need to pass `--bintrail-id` by hand: it defaults to the `bintrail_id`
recorded in `stream_state` (the synthesized one for MariaDB). Precedence is
**explicit `--bintrail-id` (typed on the CLI) > the `stream_state` id > a global
`BINTRAIL_ID` env var** — so one `BINTRAIL_ID` in a shared `config.env` can never
silently become the write key for every server. Two caveats:

- A **file-based `bintrail index`** backfill records identity in `index_state`,
  not `stream_state`, so `rotate` has nothing to fall back to — pass an explicit
  `--bintrail-id` when archiving an index-only (never-streamed) source. It fails
  loud ("no bintrail_id recorded in stream_state") rather than guessing.
- A `BINTRAIL_ID` env var is used only as a last resort (no `stream_state` id),
  and `rotate` warns when it does, since reusing one across servers collides them.

> **Address changes split history.** Because the anchor is `host:port`, moving a
> MariaDB server to a new address (or capturing the same server through two
> different hostnames) yields a *new* `bintrail_id`, so its archives continue
> under a new S3 prefix. This is the documented trade-off for MariaDB having no
> migration-stable identity; pin a stable address, or pass an explicit
> `--bintrail-id` to keep one identity across an address change.

---

## Version support

| Version | Status |
|---|---|
| **MariaDB 10.6 LTS** | **Tested** in CI. |
| **MariaDB 10.11 LTS** | **Tested** in CI. |
| **MariaDB 11.4 LTS** | **Tested** in CI. |
| Other versions from 10.6 up | Expected to work; not covered by CI. |
| MariaDB < 10.6 | Not supported. |

---

## What works

- **Live capture** in both **position mode** and **GTID mode** (MariaDB
  `domain-server-seq` GTIDs, e.g. `0-1-100`).
- **GTID resume with gap detection** — restart and bintrail re-reads the saved
  MariaDB GTID set and continues where it left off. On resume it verifies the
  source still retains the binlogs needed: MariaDB has no `@@gtid_purged`, so the
  purge floor is derived from `BINLOG_GTID_POS` over the oldest surviving binlog.
  A purged-binlog gap raises the data-loss alarm (or, with `--no-gap-fill`,
  refuses to start) in **both position and GTID mode**, and multi-domain GTID
  sets are compared per domain.
- **Multi-domain GTID: per-domain resume.** This is the supported stance, not a
  best-effort side effect: the checkpoint's MariaDB GTID set carries every
  domain's position independently, resume hands the source the full per-domain
  map, and gap detection compares each domain against the purge floor
  separately. Validated live against an interleaved multi-domain stream
  (per-session `gtid_domain_id`): stop mid-stream, write more under every
  domain, resume — no event lost, none double-indexed, each domain's sequence
  advancing from its own position, and no false alarm from the per-domain
  purge-floor comparison.
- **File-based `bintrail index`** over MariaDB binlog files.
- All row events (INSERT/UPDATE/DELETE) with before/after images, including
  `UNSIGNED`, `DECIMAL`, and DDL detection / auto-snapshot.
- **Compressed row events** (`log_bin_compress = ON`): MariaDB's
  `WRITE/UPDATE/DELETE_ROWS_COMPRESSED_V1` events are decompressed and indexed
  exactly like their uncompressed siblings, on both the streaming and
  file-based paths.
- **Statement capture** (`query_text`/`query_hash`): MariaDB's `Annotate_rows`
  event carries the originating SQL statement and is captured like MySQL's
  `ROWS_QUERY_EVENT`. `binlog_annotate_row_events` is ON by default since
  10.2.4; streaming requests the events whenever it runs as MariaDB, which it
  does once it detects a MariaDB server. See
  [query-and-recovery.md](query-and-recovery.md#statement-capture-query_text-and-query_hash).
- **Capture-time schema-drift detection**: `binlog_row_metadata=FULL` works on
  MariaDB 10.5+ (`SET GLOBAL` — MariaDB has no `SET PERSIST`; persist it in
  `my.cnf`). The default is `NO_LOG`, so it's opt-in. See
  [indexing.md](indexing.md).
- Other MariaDB-only binlog events (`Gtid_list`, `Binlog_checkpoint`) are
  skipped transparently.
- **MariaDB-only column types**: `UUID` (10.7+), `INET4` (10.10+), `INET6`
  (10.5+) and `VECTOR` (11.7+) are captured and restored by `recover` byte for
  byte, including as a primary key.

---

## Beta limitations

- **A GTID checkpoint is fixed to its flavor.** A saved GTID set cannot be
  read in the other flavor's format, so resuming one against a server of the
  other flavor is refused; use `--reset` to start fresh. A position checkpoint
  written under the other flavor (for example by an older build that captured a
  MariaDB as MySQL) resumes at the same binlog position as the detected flavor,
  with a warning.
- **Multi-server multi-domain topologies are untested.** Per-domain GTID
  resume is validated live on a single server producing several domains (the
  `gtid_domain_id` mechanism itself — see "What works" above). What has NOT
  been validated live: topologies where the domains originate on different
  servers (multi-master rings, Galera), and a primary failover that changes the
  `server_id` *within* a domain mid-stream. Sustained two-domain load on a single
  server was run for several hours in #1349 with no events lost or double
  indexed. Gap detection compares sequences per domain, so the design
  covers these shapes, but treat them as unverified territory.
- **BYOS agent support is the least exercised path.** `bintrail agent` detects
  the flavor the same way for its BYOS streaming (same optional
  `--source-flavor` and `BINTRAIL_SOURCE_FLAVOR` as `stream`), but unlike
  `stream` it has no saved checkpoint. On restart it resumes from `--start-gtid` (parsed with the
  detected flavor) or the server's current binlog position. The web interface
  also captures MariaDB sources and detects them the same way. The Source type
  saved with a server is only a hint: capture follows what the server reports,
  and the server's Overview shows a warning when the two disagree.
- **Index-on-MariaDB is out of scope** — the index database stays MySQL.
- **JSON columns come back as equivalent JSON, not the same text.** In MariaDB
  a `JSON` column is text: the server keeps exactly what your application
  wrote. The index stores JSON objects and arrays in a MySQL `JSON` column,
  which rewrites them, so `recover` restores a document that means the same
  thing but is spelled differently:
  - keys come back in a different order;
  - spaces, tabs and line breaks between values are removed;
  - `\u00e9`-style escapes come back as the character itself, and `<`, `>`
    and `&` come back as `\u003c`, `\u003e` and `\u0026`;
  - `1e2` comes back as `100.0`, and `-0` as `0`.

  In two cases the **value itself** changes:
  - an integer too large for 64 bits loses digits
    (`12345678901234567890123` comes back as `1.2345678901234568e22`);
  - with duplicate keys (`{"a":1,"a":2}`) only the last one is kept.

  A document that is a single string, number, `true`, `false` or `null`, not
  an object or array, comes back exactly. The same applies to JSON text stored
  in a MySQL `TEXT` column. If your application compares JSON by its bytes
  (a hash or a signature), compare the parsed document instead after a
  recovery.
- **Sequences are not rewound.** MariaDB records every change to a `SEQUENCE`
  as an insert into its one-row table, and bintrail captures those like any
  other insert. A reversal that includes the sequence is refused by the server
  (error 1031), so run `recover` for your tables with `--table`. The sequence
  keeps its current value, so ids handed out during the window are not handed
  out again.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `WARN … MariaDB source has no @@server_uuid; synthesized …` | Expected — MariaDB has no `server_uuid`. Benign; bintrail synthesizes a stable `bintrail_id` from the source address instead. See [Server identity on MariaDB](#server-identity-on-mariadb). |
| `source flavor mismatch: declared "mysql", but the server reports "mariadb"` | `--source-flavor` (or `BINTRAIL_SOURCE_FLAVOR`) contradicts the server. Remove it so bintrail detects the flavor, or set it to what the server reports. (A server added in the web interface never refuses for this: it captures as the server reports and shows a warning.) |
| `could not detect the source flavor` | `SELECT VERSION()` failed on the source, usually a broken connection. Fix the connection, or set `--source-flavor` to start anyway (with a warning). |
| `saved checkpoint is a mariadb GTID set, but the source is mysql` | The index was captured from a server of the other flavor. Check `--source-dsn`, or `--reset` to start fresh. |
| `MariaDB GTID gap detected but CANNOT be filled` | The source purged binlogs your checkpoint still needed. bintrail auto-advances past the lost range and records the data loss durably; pass `--no-gap-fill` to refuse to start instead. Raise `binlog_expire_logs_seconds` to give bintrail more time to resume. |
| `auto-discover binlog position` errors on an old MariaDB | Ensure `log_bin = ON` and the source user has `REPLICATION CLIENT`. |

---

## See also

- [Streaming](streaming.md) — the full `bintrail stream` reference (TLS, gap
  detection, RDS gotchas, metrics) — all of it applies to a MariaDB source.
- [DBA guide](guide.md) — day-to-day recovery scenarios.
- [Query & Recovery](query-and-recovery.md) — querying history and generating
  reversal SQL (flavor-agnostic — same for MariaDB and MySQL sources).
