# DDL Tracking and Auto-Snapshot

This page explains how DBTrail detects DDL statements (schema changes) in the binlog stream, automatically takes new schema snapshots, and tracks restore coverage so you know what can be recovered and how far back.

---

## The Problem

DBTrail maps binlog row events to column names using a schema snapshot — a point-in-time copy of `information_schema.COLUMNS` stored in the index database (see [indexing.md](indexing.md) for details). When someone runs `ALTER TABLE` on the source, the snapshot becomes stale: new columns don't have names, removed columns cause mismatches, and the parser starts skipping events. (In stream mode those skips are warn-only; file-based `bintrail index` now fails loud with a `schema gap` error instead — see [indexing.md](indexing.md).)

Before DDL tracking, the only solution was to notice the "column count mismatch" warnings in the logs, manually run `bintrail snapshot`, and hope you didn't miss too many events in between. In stream mode (continuous replication), an unattended schema change could silently break indexing for hours.

DDL tracking solves three problems:

1. **Detection**: The parser identifies DDL statements (`ALTER TABLE`, `CREATE TABLE`, MariaDB's `CREATE OR REPLACE TABLE`, `DROP TABLE`, `RENAME TABLE`, `TRUNCATE TABLE`) and emits them as events instead of just logging warnings.
2. **Auto-snapshot**: When a DDL is detected and a source database connection is available, DBTrail automatically takes a new snapshot and hot-swaps the resolver — no manual intervention needed. This works in both stream mode (always has source connection) and file mode (when `--source-dsn` is provided).
3. **Restore coverage**: The `status` command shows the time range of indexed events and warns about DDLs that weren't followed by a snapshot — whether from file-mode indexing without `--source-dsn` or from a failed auto-snapshot — so you know where recovery gaps might exist.

---

## Stream Mode: Auto-Snapshot

In stream mode (`bintrail stream`), a DDL means the source schema just changed. Since we're connected to the live server, `information_schema` already reflects the new schema — the perfect moment to take a snapshot.

When a DDL is detected, DBTrail automatically takes a new snapshot from the live server, swaps the parser over to the fresh schema, and records the DDL in the `schema_changes` table with the new `snapshot_id`. No manual intervention is needed.

### Error handling

A snapshot (or resolver-reload) failure **aborts the stream**. The DDL is still recorded in `schema_changes` before the abort, but without a `snapshot_id`.

This used to be best-effort: the failure was only logged, and the parser kept decoding with the old resolver. In practice that meant every row event that followed the DDL in the binlog — an unbounded burst of INSERT/UPDATE/DELETE on the altered/created table — was silently skipped as a "column count mismatch" / "table not in snapshot", while the stream checkpoint kept advancing past them. Once healthy schema tracking resumed, those events were already behind the checkpoint and could never be re-streamed: a permanent, unmarked loss.

Aborting instead means the process exits non-zero *before the DDL event itself is even emitted* to the indexer, so the durable checkpoint (which only advances off events it actually receives) stays exactly where it was *before* this DDL. A supervisor restart (`bintrail-console watch`'s monitor, or an external process manager for standalone `bintrail stream`) resumes from that checkpoint, re-reads the same DDL off the binlog, retries the snapshot, and only then decodes the rows that follow — against a fresh resolver. No gap, no silent skip.

The abort surfaces as the process's error output — `bintrail stream` prints it to stderr and exits 1; under `bintrail-console watch` the monitor logs it and applies its crash-loop backoff before retrying — so it's visible in production monitoring either way.

---

## File Mode: Auto-Snapshot or Record Only

In file mode (`bintrail index`), the behavior depends on whether `--source-dsn` is provided:

### With `--source-dsn`: Auto-Snapshot

When `--source-dsn` points to the source MySQL server, file mode behaves identically to stream mode: on DDL detection, it takes a snapshot from `information_schema`, builds a new resolver, and atomically swaps it. This is safe when indexing recent binlogs from the same server, since `information_schema` reflects the current (post-DDL) schema.

### Without `--source-dsn`: Record Only

> **Note:** since the source pre-flight became mandatory (#493), this flow now
> requires an explicit opt-out — run `bintrail index --skip-source-validation ...`.
> Without it, `index` aborts because `--source-dsn` is required; the silent
> no-source skip was removed so a non-`FULL` source can't be indexed by accident.

When no source connection is available (e.g., indexing binlogs from a decommissioned server), DBTrail records the DDL in `schema_changes` and logs a warning:

```
WARN DDL detected but --source-dsn not provided; run `bintrail snapshot` if schema changed.
```

The `snapshot_id` column is NULL for these DDLs, which the status command uses to flag potential restore coverage gaps.

---

## Restoring to Before a Snapshot Was Recorded

A snapshot is taken when capture reaches the DDL, which can be minutes or hours after the DDL ran on the source (capture behind, or catching up after a restart). When a backup is updated, or a Parquet snapshot is written for a moment (`reconstruct --output-format parquet`), DDLs on the table that ran at or after the second the backup's `CREATE TABLE` was read and at or before that moment (by the `detected_at` of their `schema_changes` rows) are checked: if the newest snapshot taken for such a DDL is newer than the one in effect at that moment, the column types are also compared with it, and a difference refuses.

That snapshot read the table after the DDL, and maybe after later DDLs too, so this extra comparison can only add refusals: it may refuse a restore that would have been right, but it never lets through one the existing check would have refused. Clock skew between the source and the DBTrail host moves this window by the skew.

Decoding old rows (ENUM and SET labels, BLOB values, the width of a `BINARY` key) still uses, for each row, the newest snapshot taken at or before the row was written. Dating a snapshot at its DDL instead would be wrong under capture lag: the snapshot reads the table when capture reaches the DDL, so it can already include a later DDL. Rows written between the two DDLs would then be decoded with the later definition, and that later DDL may not be recorded yet.

---

## The schema_changes Table

```sql
CREATE TABLE IF NOT EXISTS schema_changes (
    id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    detected_at     DATETIME NOT NULL,
    binlog_file     VARCHAR(255) NOT NULL,
    binlog_pos      BIGINT UNSIGNED NOT NULL,
    gtid            VARCHAR(255) DEFAULT NULL,
    schema_name     VARCHAR(64) NOT NULL,
    table_name      VARCHAR(64) NOT NULL,
    ddl_type        VARCHAR(50) NOT NULL,
    ddl_query       TEXT NOT NULL,
    snapshot_id     INT UNSIGNED DEFAULT NULL,
    INDEX idx_detected_at (detected_at)
)
```

Key fields:

| Field | Description |
|---|---|
| `ddl_type` | One of `ALTER TABLE`, `CREATE TABLE`, `CREATE OR REPLACE TABLE`, `DROP TABLE`, `RENAME TABLE`, `TRUNCATE TABLE` |
| `ddl_query` | The full DDL statement from the binlog |
| `snapshot_id` | The snapshot taken after this DDL. NULL when none was taken: file mode without `--source-dsn`, a failed auto-snapshot, or `TRUNCATE TABLE` (which changes no table structure, so no snapshot is needed — by design, in every mode) |

This table is created by `bintrail init` and must exist in the index database. Older index databases (created before this feature) won't have it — the status command handles this gracefully by treating a missing table as zero schema changes.

### Querying schema changes from AI (MCP)

The MCP server exposes this table as the `list_schema_changes` tool, so an AI
assistant can answer "what ALTERs hit the orders table this month?" directly.
Filters: `schema`, `table`, `ddl_type` (prefix-matched — `ALTER` matches
`ALTER TABLE`), `since`, `until`, `limit`, and `uncovered_only` — exactly the
rows behind the `status` warning: `snapshot_id` null AND the DDL is not a
`TRUNCATE TABLE`, whose null is by design (see below). Each result carries
the full DDL statement, binlog coordinates, detection timestamp, and the
covering `snapshot_id`; a `null` there means uncovered except on a TRUNCATE
row, which says so itself via `snapshot_note`. An agent can go from the
status warning about uncovered DDLs straight to the exact rows. See
[mcp-server.md](./mcp-server.md).

---

## Restore Coverage

The `status` command includes a "Restore Coverage" section that answers the question: **what can be restored and how far back?**

### Text output

```
=== Restore Coverage ===
  Earliest event:     2026-02-28 14:00:00 UTC
  Latest event:       2026-03-02 09:45:00 UTC
  Total events:       1,284,567
  Schema changes:     3 detected
  Warning: 1 DDL(s) detected without auto-snapshot (file-mode indexing without --source-dsn, or a failed auto-snapshot) — recovery across these DDLs may require manual snapshot
```

The warning appears when `schema_changes` rows have `snapshot_id = NULL` for a DDL type that needs a snapshot — either a DDL detected in file mode without `--source-dsn`, or an auto-snapshot that failed (in any mode). `TRUNCATE TABLE` rows are excluded from the count: they record `snapshot_id = NULL` by design (a truncate changes no table structure, so no snapshot is taken) and are not a coverage gap. Recovery SQL generated for events spanning a genuinely uncovered DDL boundary may use incorrect column names.

### JSON output

```json
{
  "coverage": {
    "earliest_event": "2026-02-28T14:00:00Z",
    "latest_event": "2026-03-02T09:45:00Z",
    "total_events": 1284567,
    "schema_changes": 3,
    "uncovered_ddls": 1
  }
}
```

---

## Comparison: Stream vs File Mode

| Behavior | Stream Mode | File Mode (with `--source-dsn`) | File Mode (no `--source-dsn`) |
|---|---|---|---|
| DDL detection | Yes — from replication events | Yes — from binlog file events | Yes — from binlog file events |
| Auto-snapshot | Yes — immediate | Yes — immediate | No — no source connection |
| schema_changes record | Yes, with `snapshot_id` | Yes, with `snapshot_id` | Yes, with `snapshot_id = NULL` |
| User action needed | None | None | Run `bintrail snapshot` after DDL |
| Restore coverage warning | No (snapshot covers the DDL) | No (snapshot covers the DDL) | Yes (warns about uncovered DDLs) |

Two cases sit outside this happy-path table: a **failed auto-snapshot** (any mode) records the DDL with `snapshot_id = NULL` and counts toward the restore coverage warning; a **`TRUNCATE TABLE`** records `snapshot_id = NULL` by design in every mode and is excluded from the warning.
