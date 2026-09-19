# How Rotation and Status Work

This page explains how `bintrail rotate` manages the partition lifecycle of the `binlog_events` table, and how `bintrail status` reports the state of the index.

---

## The Partition Management Problem

The `binlog_events` table grows continuously. On a busy database, it can accumulate millions of rows per day. You need a way to reclaim space without slow, lock-heavy `DELETE` operations.

MySQL's solution is table partitioning.

---

## Why Partitioning?

`binlog_events` is partitioned by `RANGE (TO_SECONDS(event_timestamp))`. Each partition holds one hour's worth of events. This gives you two powerful properties:

**Instant deletes**: Dropping a partition is a metadata operation — MySQL removes the partition's data files directly, without scanning or logging individual row deletions. Dropping 30 days of events takes milliseconds instead of minutes.

**Partition pruning**: When you query with a time range (`--since`/`--until`), MySQL's optimizer sees that only certain partitions can contain the matching rows and skips the rest entirely. A query for "events in the last hour" touches one or two partitions out of potentially hundreds.

---

## The `p_future` Catch-All

There is always a special partition:

```sql
PARTITION p_future VALUES LESS THAN MAXVALUE
```

`p_future` catches any event whose timestamp is beyond all named partition boundaries. This is MySQL's safety net — without it, inserting an event with a timestamp in the future would fail with an error.

**The invariant**: `p_future` must always exist. You can add or drop any other partition, but never drop `p_future` — `bintrail rotate` always re-appends it at the end of every `REORGANIZE PARTITION` operation.

---

## Dropping Old Partitions

```sh
bintrail rotate --index-dsn "..." --retain 7d
```

The `--retain` flag accepts a duration: `7d` (days) or `24h` (hours). The command:

1. Computes `cutoff = now - retain_duration` (truncated to the current hour UTC).
2. Lists all partitions from `information_schema.PARTITIONS`.
3. Parses the hour from each `p_YYYYMMDDHH` name (`p_future` is skipped automatically because `indexer.PartitionDate` returns `false` for it).
4. Collects all partitions whose date is before the cutoff.
5. Issues a single `ALTER TABLE binlog_events DROP PARTITION p1, p2, p3` statement.

A single `ALTER TABLE DROP PARTITION` statement for multiple partitions is more efficient than separate statements — MySQL does it in one pass.

After dropping, the command automatically adds the same number of future hourly partitions to keep the rolling window size constant (e.g. dropping 168 partitions adds 168 new ones). Use `--no-replace` to suppress this auto-replacement when you genuinely want to reclaim space:

```sh
bintrail rotate --index-dsn "..." --retain 7d --no-replace
```

After dropping, the command warns if `p_future` contains data. If events are landing in `p_future`, it means events are arriving with timestamps beyond all named partition boundaries — you need to add more future partitions.

---

## Adding Future Partitions

```sh
bintrail rotate --index-dsn "..." --add-future 14
```

Adding future partitions converts the `p_future` catch-all into specific hourly partitions, then appends a new `p_future` at the end. This is done with `REORGANIZE PARTITION`:

```sql
ALTER TABLE `binlog_index`.`binlog_events`
REORGANIZE PARTITION p_future INTO (
    PARTITION p_2026021900 VALUES LESS THAN (TO_SECONDS('2026-02-19 01:00:00')),
    PARTITION p_2026021901 VALUES LESS THAN (TO_SECONDS('2026-02-19 02:00:00')),
    ...
    PARTITION p_future VALUES LESS THAN MAXVALUE
)
```

`REORGANIZE PARTITION` moves data from `p_future` into the appropriate new named partitions and creates a fresh `p_future`. Any data that was already in `p_future` goes to the right named partition — nothing is lost.

`nextPartitionStart` determines where to start adding partitions: it finds the latest existing `p_YYYYMMDDHH` partition and starts the hour after. If no named partitions exist yet, it starts from the current hour (UTC).

---

## Partition Naming

Each hourly partition is named `p_YYYYMMDDHH` in UTC (e.g. `p_2026021914`); the only exception is the `p_future` catch-all.

---

## Archiving Partitions to Parquet

Before dropping old partitions, DBTrail can serialize each partition's events to a Parquet file. This gives you a long-term queryable record outside the index database — without requiring the original binlog files.

### Archiving to a local directory

Pass `--archive-dir` to write Parquet files locally before each drop:

```sh
bintrail rotate \
  --index-dsn           "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain              7d \
  --archive-dir         /mnt/archives \
  --archive-compression zstd
```

Each archived partition becomes a single Parquet file under a Hive-partitioned directory: `<archive-dir>/bintrail_id=<uuid>/event_date=<YYYY-MM-DD>/event_hour=<HH>/events.parquet`. If any archive write fails, no partitions are dropped — the command aborts before touching the table.

`--archive-compression` accepts `zstd` (default), `snappy`, `gzip`, or `none`.

### Retrying after a failure

If archiving or S3 upload fails partway through, re-run with `--retry` to skip work that already completed:

```sh
bintrail rotate \
  --index-dsn           "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain              7d \
  --archive-dir         /mnt/archives \
  --archive-s3          s3://my-bintrail-archives/events/ \
  --archive-compression zstd \
  --retry
```

With `--retry`:
- **Local Parquet files**: Partitions whose Parquet file already exists on disk are skipped.
- **S3 uploads**: Partitions whose `s3_uploaded_at` is already recorded in the `archive_state` table are skipped.

This makes the command safe to re-run without re-archiving or re-uploading partitions that already succeeded.

### Archive state tracking

Each archived partition is recorded in the `archive_state` table (created by `bintrail init`). This table tracks:

| Column | Description |
|--------|-------------|
| `partition_name` | The partition that was archived (e.g. `p_2026021300`) |
| `bintrail_id` | Server identity UUID |
| `local_path` | Filesystem path of the Parquet file |
| `file_size_bytes` | Size of the Parquet file |
| `row_count` | Number of rows written |
| `s3_bucket` | S3 bucket (when uploaded) |
| `s3_key` | S3 object key (when uploaded) |
| `s3_uploaded_at` | When the S3 upload completed |
| `min_event_ts` / `max_event_ts` | Content-derived `MIN`/`MAX(event_timestamp)` of the archived rows (NULL on archives written before this column existed, or registered by `upload`/`archive reconcile`) |
| `archived_at` | When the archive was created |

The `--retry` flag on rotate uses this table to determine which S3 uploads can be skipped.

#### Backfilled events and the hour label

A partition's *name* is not a reliable statement of what it holds. When a
stream backfills events whose hourly partitions were already rotated away
(e.g. resuming from an old checkpoint after a multi-day capture stall, with
`--retain` shorter than the stall), MySQL's RANGE partitioning files those
old rows into the **oldest live partition** — and rotation then archives that
partition under its own hour label, so the Parquet file's
`event_date=`/`event_hour=` path disagrees with the timestamps of the rows
inside it.

That is why rotation records `min_event_ts`/`max_event_ts` at archive time:
the query planner counts every hour in that content range as archive-covered,
and time-scoped reads are told to open the mislabeled file even when its hour
label falls outside the queried window (date-scoped S3 listings included).
Row-level time filters still bound what the file contributes.

When a partition's content range escapes its hour label, rotation also logs:

```
WARN archived partition contains events outside its hour label (backfilled rows);
     recording true content time range so time-scoped archive reads still find them
     partition=p_2026072401 min_event_ts=2026-07-22T19:10:00Z max_event_ts=2026-07-24T01:30:00Z
```

Archives created **before** this column existed have NULL ranges and keep the
old label-only pruning; if such an archive is known to contain backfilled
rows, re-populate the columns manually (`UPDATE archive_state SET
min_event_ts=..., max_event_ts=... WHERE partition_name=...`) using the
Parquet file's own footer statistics (see [Parquet
debugging](parquet-debugging.md)).

### Archiving directly to S3

Pass `--archive-s3` alongside `--archive-dir` to upload each Parquet file to S3 after writing it locally:

```sh
bintrail rotate \
  --index-dsn         "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain            7d \
  --archive-dir       /tmp/rotate-staging \
  --archive-s3        s3://my-bintrail-archives/events/ \
  --archive-s3-region us-east-1
```

`--archive-dir` is still required — files are written locally first, then uploaded. You can use a temporary directory if you don't need local copies after upload.

**Hive-partitioned layout**: S3 objects are stored with a Hive-compatible directory structure for compatibility with Athena, Glue, and DuckDB:

```
s3://my-bintrail-archives/events/
  bintrail_id=abc123de-0000-0000-0000-000000000001/
    event_date=2026-02-13/
      event_hour=00/
        events.parquet    ← p_2026021300
      event_hour=01/
        events.parquet    ← p_2026021301
      ...
    event_date=2026-02-14/
      event_hour=00/
        events.parquet
      ...
```

The `bintrail_id` partition key is the stable UUID of the DBTrail server instance that indexed the data (see [Server Identity](server-identity.md)). Multiple DBTrail instances indexing different MySQL sources can share the same S3 prefix without collision.

**AWS credentials**: DBTrail uses the standard credential chain — environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), `~/.aws/credentials`, or EC2/ECS instance metadata. `--archive-s3-region` is optional if `AWS_REGION` is already set.

### Querying archived events

Once partitions are archived to S3, query them alongside live index data with `--archive-s3` on the `query` command. Provide `--bintrail-id` to scope the archive path to a specific server:

```sh
bintrail query \
  --index-dsn   "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schema      mydb \
  --table       orders \
  --since       "2026-01-01 00:00:00" \
  --until       "2026-01-31 23:59:59" \
  --archive-s3  s3://my-bintrail-archives/events/ \
  --bintrail-id abc123de-0000-0000-0000-000000000001
```

Results from the live MySQL index and from Parquet archives are merged, deduplicated by `event_id`, and sorted by timestamp before being returned. Per-source archive query failures (expired credentials, S3 `AccessDenied`, corrupted Parquet, etc.) are non-fatal — the command prints a `Warning: archive query failed for <src>: <err>` line to **stderr** (regardless of `--log-level` or `--log-format`) and continues with whatever other sources succeed. Context cancellation (Ctrl-C or deadline expiry) aborts the whole query with a non-zero exit instead of continuing. See [query-and-recovery.md § Archive Fetch Error Handling](query-and-recovery.md#archive-fetch-error-handling) for the full contract.

---

## Reconciling `archive_state` With Reality

`archive_state` is a **rebuildable cache** over the self-describing archive
layout, not a fragile source of truth. If the registry and the files drift —
the index database was rebuilt (rows lost), or archived Parquet files were
pruned while their rows remained — `bintrail archive reconcile` re-syncs them:

```sh
# cron drift monitor: read-only, exits non-zero when drift exists
bintrail archive reconcile --index-dsn "$IDX" \
  --archive-dir /var/lib/bintrail/archives --archive-s3 s3://bkt/archives/

# rebuild the registry after an index loss
bintrail archive reconcile --index-dsn "$IDX" --archive-s3 s3://bkt/archives/ --repair

# also delete registrations whose files are gone from every referenced backend
bintrail archive reconcile --index-dsn "$IDX" \
  --archive-dir /var/lib/bintrail/archives --archive-s3 s3://bkt/archives/ \
  --repair --prune
```

It scans the given backends for `bintrail_id=<uuid>/event_date=<d>/event_hour=<h>/*.parquet`
files and diffs against `archive_state` in three buckets: **files without
rows** (`--repair` re-registers them — sizes from the listing/stat, row counts
from the Parquet footer for local files), **rows without files** (`--prune`
deletes the registry row; data files are never touched), and **metadata
drift** (`--repair` updates; row-count verification of existing rows costs a
footer read per file and is gated behind `--deep`).

Safety rules worth knowing:

- **Backend-scoped:** a row is only a prune candidate when *every* backend it
  references was scanned by this invocation and held no file. An S3-referenced
  row during an `--archive-dir`-only run is reported as *unverified*, never
  pruned — and repair never touches the columns of a backend it didn't scan.
- **Concurrency margin:** rows younger than `--prune-min-age` (default `1h`)
  are never pruned; a concurrent `rotate` may still be mid-write.
- **Empty scans prove nothing:** a scanned backend whose scan finds *zero*
  layout files anywhere looks identical whether the path was mistyped or the
  backend was legitimately emptied, so its rows are reported *unverified* and
  never pruned. When the wipe is real (e.g. an S3 lifecycle rule expired the
  whole prefix), pass `--trust-empty-scan=local|s3` naming the emptied
  backend — the vouch is per-backend so it can never disarm the gate for the
  other one, and every prune it enabled is marked in the report's reason.
- **No phantom pending uploads:** when reconcile confirms an S3 object, it
  stamps `s3_uploaded_at` — a row with `s3_bucket` set but no stamp reads as
  an upload still in flight, which makes `rotate` refuse to drop that
  partition.

## Status Command

```sh
bintrail status --index-dsn "..."
```

It accepts `--format text` (default) or `--format json`, plus `--baseline-dir`
(to list baseline snapshots) and `--fail-on-gap` (see continuity below).

The status command produces a multi-section report. The sections, in order:

| Section | Shown when | Contents |
|---|---|---|
| **Servers** | `bintrail_servers` has rows | Registered source servers — `bintrail_id`, host/port, server UUID, status |
| **Stream** | `stream_state` has a checkpoint, **or** reading it failed | Replication position/GTID, events indexed, last event/checkpoint, and the **continuity** and **freshness** verdicts (below). On a read failure the block shows only an `unavailable` verdict |
| **Indexed Files** | always | Every row in `index_state`, with the `bintrail_id` that indexed each file |
| **Partitions** | always | Each partition's boundary and estimated row count |
| **Archives** | `archive_state` has data | Archived-file / S3-upload counts |
| **Restore Coverage** | best-effort (skipped on load error) | Earliest/latest event, live + archived totals, **index size**, schema-change count |
| **Summary** | `index_state` has rows | Files grouped by server identity, with aggregated counts (a pure-streaming index has none) |
| **Baselines** | `--baseline-dir` given **and** snapshots found | Baseline snapshots on disk, with their binlog anchors and size |

### Stream continuity ("no data lost")

The **Stream** section ends with an always-present continuity verdict — the cheap
"did I lose any events?" answer the gap detector already computes. It is strictly
about gap-**contiguity** of the captured range; it is **not** a liveness/lag check
(a contiguous stream may still be stopped or behind).

```
=== Stream ===
  Bintrail ID:     abc123de-0000-0000-0000-000000000001
  Mode:            gtid
  Position:        binlog.000042:99012
  Events indexed:  986655
  Last checkpoint: 2026-02-19 10:01:12
  Server ID:       100
  Continuity:      no gaps in the captured range (not a liveness check)
  Freshness:       current — newest indexed event is 3s old
  Capture health:  OK — no events skipped
```

The verdict is one of four states:

| Text | JSON `stream.continuity.status` | Meaning |
|---|---|---|
| `no gaps in the captured range` | `ok` | The captured range is contiguous — nothing was dropped |
| `⚠ GAP LOST at <ts>` | `gap_lost` | An unfillable gap was stamped; the index is valid only up to the gap |
| `not evaluated (legacy index …)` | `unknown` | A legacy index without the gap-detection columns — the state can't be confirmed (never a false "ok") |
| `⚠ unavailable (could not read stream state: <err>)` | `unavailable`, under top-level `stream_error` (not `stream`) | `stream_state` could not be **read** (e.g. transient timeout) — distinct from an empty table, which shows no Stream block. The verdict, and any permanent-loss banner, could not be evaluated; re-run `status` to retry |

When data was permanently lost, a loud banner follows the section:

```
=== ⚠ EVENTS PERMANENTLY LOST ===
  Detected:  2026-02-19 11:30:00
  Detail:    unfillable binlog gap detected; resume requires re-baseline
  The capture stream lost data it could not recover. The index up to the
  gap is still valid for recovery, but to resume capture you must re-baseline.
```

This banner is also how an index-only `status` surfaces a lost or invalidated
PostgreSQL replication slot (#532) after the capture process has exited.

### Stream freshness ("is capture still keeping up?")

Continuity's sibling, and deliberately a different question. Continuity is about
gap-contiguity of what was captured; freshness is the **liveness** half. Both are
needed: a perfectly contiguous index can be three days stale, and a perfectly
fresh one can have a hole in the middle.

| Text | JSON `stream.freshness.status` | Meaning |
|---|---|---|
| `current — newest indexed event is Ns old` | `current` | Checkpointing **and** indexing recent events |
| `idle — checkpointing, newest indexed event is …` | `idle` | Checkpointing, but nothing recent indexed. **Not a fault, and not a clean bill of health** — see below |
| `⚠ STALLED — no checkpoint for …` | `stalled` | The checkpoint itself is stale. The alertable state |
| `not evaluated (a stream row with no checkpoint …)` | `unknown` | Nothing to judge; never a false `current` |
| *(no Stream block)* | `unavailable` / `none` | `stream_state` unreadable, or a file-mode index that ran no capture and makes no claim |

The **checkpoint** is the liveness signal, not the event time: the checkpoint
ticker runs with or **without** traffic, so a stale checkpoint means the daemon
is dead or wedged — never that the workload went quiet.

**What `idle` does and does not tell you.** From the index alone, a source
nobody wrote to and a capture running an hour behind are the *same
observation*: fresh checkpoint, old newest-event. Nothing in `stream_state`
separates them, so `status` reports the ambiguity instead of guessing. The
daemon *can* tell — that is exactly what
`bintrail_stream_index_commit_latency_seconds` measures (see
[observability.md](observability.md)).

**`--fail-on-lag <duration>` (for CI / cron).** The freshness equivalent of
`--fail-on-gap`, and a separate flag because the two failures are independent —
alert on one without the other. It exits non-zero on `stalled`, on any verdict
that is not a claim (**fails closed**), or when the newest indexed event is
older than the duration:

```bash
bintrail status --index-dsn "$IDX" --fail-on-lag 15m || alert "dbtrail capture is behind"
```

The age check is **traffic-sensitive by construction** (see `idle` above): on a
source with genuinely quiet periods it fires with nothing wrong. Pick a
threshold above your quiet windows. Unset, it never changes the exit code.

**`--fail-on-gap` (for CI / cron).** By default a gap **never** changes the exit
code — `status` is a report. Pass `--fail-on-gap` to exit non-zero when continuity
is `gap_lost` **or** `unknown`; it **fails closed**, so an un-migrated legacy index
or an unloadable stream state also trips it. It also exits non-zero when the
capture ledger records **any** dropped events — `statement_format_dml`,
`column_count_mismatch`, and the rest — those changes are permanently absent
from the index, the same loss class as `gap_lost`. A **NULL**
capture ledger does not trip it (the column post-dates the flag, and alerting on
its absence would fail every pre-existing deployment) — but a ledger that is
*present and unreadable* does fail closed: a skip-aware daemon wrote it, and it
may be hiding a loss count. If the daemon restarts while the ledger is
unreadable, it preserves that fact under the `unreadable_previous_ledger`
meta-reason instead of overwriting the evidence with a clean document — which
also fails closed until acknowledged (same runbook as below).

The drop counter is **monotonic for the life of the index** — fixing
`binlog_format` stops new drops but does not clear the count (the dropped
changes stay lost). To retire the alert after remediation, **acknowledge** it:

```sh
bintrail status --index-dsn "$IDX" --ack-capture-skips
```

or press **Mark as read** on the capture-health box in the web console. Either
one records the count you saw and the moment you saw it in
`stream_state.capture_skips_ack`; nothing is erased, `status` keeps reporting
the tally, and `--fail-on-gap` stops failing on it.

Acknowledgement covers a **count**, not the problem: if anything is skipped
afterwards the tally rises above what was acknowledged and both the alert and
the console's alarm come back with no further action. That is what makes it
safe to press — it can retire a record, it cannot mute the next incident.

Do **not** clear `capture_skips` by hand. That was the old advice and it is
worse in both directions: the counts are the only evidence those events were
dropped, and clearing them with the daemon running is ineffective anyway (the
next checkpoint re-persists the in-memory tallies).

```sh
bintrail status --index-dsn "$IDX" --fail-on-gap || alert "dbtrail lost events"
```

Under `--format json` the verdict is **nested in the `stream` object** as
`stream.continuity` (`{"status": "ok|gap_lost|unknown"}`), with a sibling
`stream.gap_lost` object carrying the detail when applicable. It is present
whenever the `stream` object is — i.e. once a checkpoint exists — so a
CI check uses `jq -e '.stream.continuity.status == "ok"'` (a `null` from a
missing `stream` is itself a "can't confirm" signal). The green "no gaps" badge
in the [web console](console.md) keys on `stream.continuity.status == "ok"`.

When `stream_state` could not be **read**, the JSON output instead carries a
top-level `stream_error` object — a **sibling** of `stream`, never a fake
`stream` — shaped `{"continuity": {"status": "unavailable"}, "error": "..."}`.
The jq check above stays fail-closed in this state: `.stream.continuity.status`
is `null` because `stream` is absent.

### Capture health (in-stream discards)

The continuity verdict answers "did the stream **lose** events it never
received?". Its sibling, the **`Capture health`** line right below it, answers
"did the stream **discard** events it *did* receive?" — events the daemon read
off the binlog and chose to skip, most often because the schema snapshot went
stale or corrupt and the column-count guard rejected every row. That failure
used to be invisible: the checkpoint stayed fresh and continuity honestly said
"no gaps" while 100% of rows were being dropped.

The daemon counts every skip by reason (monotonic, persisted with each
checkpoint, surviving restarts) and `status` renders the verdict:

```
  Continuity:      no gaps in the captured range (not a liveness check)
  Capture health:  ⚠ DEGRADED — 41,203 events skipped (column_count_mismatch), last 2026-07-17 12:24:12
  Skipped events were read from the stream but NOT indexed — a restore window
  over them is incomplete.
  shop.plugin_log changed on the source but is missing from the schema snapshot
  capture decodes against, so those row events could not be indexed. […]
  Fix: refresh the schema snapshot for this source — the record of each table's
  columns that capture decodes against (not a baseline, which is a full copy of
  the data). […]
  None of this recovers what was already skipped: those changes are absent from
  the index for good unless the source still has the binlogs covering that
  window […]
  Per-event detail is in the log of the process capturing this source […]
```

The block after the verdict is the same text the [web console](console.md)
shows, built once in `internal/status` and shipped in the JSON as
`capture_health.explanation`, so the two surfaces cannot tell different
stories. It names the affected tables, distinguishes the ordinary cause (a new
table appeared and the stream's own auto-snapshot did not follow it) from the
one a re-snapshot can never fix (a table validation excluded — see below), and
says plainly that **a fresh snapshot fixes capture going forward only**: events
already skipped stay missing unless the source still has the binlogs covering
that window.

| Text | Meaning |
|---|---|
| `OK — no events skipped` | A skip-aware daemon has evaluated the stream and dropped nothing |
| `⚠ DEGRADED — N events skipped (<reasons>), last <ts>` | N events were read and discarded; the reasons and the most recent skip time follow |
| `⚠ ON RECORD — N events skipped (<reasons>), last <ts>` | The same loss, acknowledged by an operator: still reported, no longer alerting. A later skip returns it to `DEGRADED` |
| *(line absent)* | Unknown — a legacy index, or one no skip-aware daemon has written; `OK` is never asserted from absent data |

Skip reasons include `column_count_mismatch` (stale/corrupt snapshot),
`table_not_in_snapshot` (a table the snapshot never saw — fixed by a fresh
snapshot), `table_excluded_from_snapshot` (a table snapshot validation left out
because it has no explicit primary key or is not InnoDB — a fresh snapshot
excludes it again, so the fix is on the source's DDL), `no_resolver`,
`unhandled_row_event`, and `statement_format_dml` (a STATEMENT/MIXED-format DML whose row image is not in
the binlog — requires `binlog_format=ROW`). Routine skips of system schemas
the snapshot deliberately excludes (e.g. RDS's `mysql.rds_heartbeat2`) are
**not** counted.

For `statement_format_dml` and the table-attributed reasons above the daemon
also stamps the most recent drop's
**attribution** — binlog file:pos, and for statement DML the statement keyword
and connection id, enough
to hunt the offending client without ever storing the statement text (it embeds
row values). The DEGRADED block then adds a `Last drop:` line, e.g.
`Last drop:       binlog.000042:99012 (UPDATE, connection id 55)`.

In the daemon log, each skip still emits its per-event `WARN`; after 100
**consecutive** skipped events one `ERROR` with remediation is emitted (once
per degraded episode — it re-arms when an event is captured again).

Under `--format json` the verdict is `stream.capture_health`:
`{"status": "ok"}` or `{"status": "degraded", "total_skipped": N,
"last_skip_at": "...", "skipped": {"<reason>": {"count": N, "last_at": "..."}}}`;
the key is omitted when the verdict is unknown. Attributed reasons additionally
carry `last_file`, `last_pos`, `last_statement_type`, `last_connection_id`
(omitted when absent). Table-attributed reasons also carry `tables` (capped, with
`tables_truncated` when more were skipped than are listed) and `last_detail`;
the verdict carries `explanation`, the rendered prose above. The
[web console](console.md) serves the same payload from `GET /api/status`, with
one difference for a session whose data access is restricted (a data profile,
or per-role access rules): `tables` keeps only the names that session may read,
`tables_withheld` counts the rest, and the explanation is built from that
shorter list ("app.users and 2 tables outside your access"). The counts are
unchanged. The console's Overview shows an orange "Capture incomplete" box in
the same states, with a **Refresh schema snapshot** button for a monitored
server: it re-reads the source's column layout and restarts that server's
capture onto it (a schema snapshot is not a baseline — it records columns, not
data), reporting whether the stream actually reloaded.

An internal error inside that job is contained: the run is reported as failed
with the error, its stack goes to the daemon log, capture keeps running for
every other server, and the button works again rather than answering "already
running" forever. The report says which half broke, rather than hedging over
both. An error while reading the source's columns states that capture was not
touched, which is a fact and not a guess, because the restart is not reached
until after the columns are read. An error during the restart states that the
schema snapshot itself was taken and recorded, and that this server's capture
may now be stopped: open Manage servers and press Start if it is not running.
That second case is contained but not undone. While it lasts, that server can
also be missing from the daemon's list of active sources, so its index is not
archived or pruned on the rotation schedule until capture is started again.

### Sections in detail

**Indexed Files** — shows every row in `index_state`. The `BINTRAIL_ID` column identifies which DBTrail server instance indexed each file:

```
=== Indexed Files ===
FILE              STATUS     EVENTS  STARTED_AT           COMPLETED_AT         ERROR  BINTRAIL_ID
────              ──────     ──────  ──────────           ────────────         ─────  ───────────
binlog.000042     completed  12345   2026-02-19 10:00:00  2026-02-19 10:00:42  -      abc123de-0000-0000-0000-000000000001
binlog.000043     completed  8901    2026-02-19 10:00:43  2026-02-19 10:01:12  -      abc123de-0000-0000-0000-000000000001
binlog.000001     completed  999     2026-02-01 00:00:00  2026-02-01 00:05:00  -      -
```

Rows with `-` in `BINTRAIL_ID` were indexed before the server identity feature was introduced; their server of origin is unknown.

**Partitions** — shows each partition with its boundary and estimated row count:

```
=== Partitions ===
PARTITION     LESS_THAN           ROWS (est.)
─────────     ─────────           ───────────
p_2026021300    2026-02-13 01:00 UTC   142389
p_2026021301    2026-02-13 02:00 UTC   198234
...
p_future      MAXVALUE            0
Total events (est.): 987654
```

**Archives** (shown when `archive_state` contains data) — archive and S3 upload statistics:

```
=== Archives ===
  Total:  168 files (4.2 GB, 987654 rows)
  Local:  168
  S3:     168 (bucket: my-archive-bucket)
```

`S3: 0` means nothing has been uploaded yet. This section is loaded best-effort — if the `archive_state` table does not exist (older index databases created before the archiving feature), it is silently omitted.

**Restore Coverage** follows the Archives section: the earliest/latest indexed event, live + archived event totals, the **index size** on disk (`binlog_events`), and the schema-change count.

**Summary** (printed last, when `index_state` has rows) — groups files by server identity and aggregates counts:

```
=== Summary ===
Server abc123de-0000-0000-0000-000000000001
  Files:  12 completed, 0 in_progress, 0 failed
  Events: 986655 indexed

Server (unknown)
  Files:  1 completed, 0 in_progress, 0 failed
  Events: 999 indexed
```

Files with a NULL `bintrail_id` are grouped under `Server (unknown)`. This is common when a shared index database receives files from multiple DBTrail instances (e.g. one per replica), or when upgrading from a version predating the server identity feature.

The row counts in the partitions section are **estimates** from `information_schema.PARTITIONS.TABLE_ROWS`. InnoDB doesn't maintain exact row counts, so these are good approximations for capacity planning but not for exact totals. For turning these estimates into a disk forecast, see [Capacity Planning](./capacity.md).

---

## Full Lifecycle Diagram

```
bintrail init
    └── creates binlog_events (48 hourly partitions + p_future)
        creates schema_snapshots, index_state, stream_state

bintrail snapshot
    └── reads information_schema on source
        writes to schema_snapshots (snapshot_id N)

bintrail index / bintrail stream
    └── parses events → inserts into binlog_events partitions
        tracks progress in index_state / stream_state (with bintrail_id)

bintrail rotate --retain 7d [--archive-s3 s3://...]
    └── (optional) archives each partition to Parquet → uploads to S3
        records archive metadata in archive_state
        drops old partitions (instant metadata operation)
        auto-adds replacement future partitions (reorganize p_future)

bintrail status
    └── reads bintrail_servers, stream_state, index_state,
        information_schema.PARTITIONS, archive_state, schema_changes
        (and baseline Parquet with --baseline-dir)
        prints multi-section report incl. the stream-continuity verdict
        (--format json for machine-readable; --fail-on-gap to alert on loss,
        --fail-on-lag to alert on capture falling behind or stalling)

bintrail query [--archive-s3 s3://...]
    └── partition pruning: only reads relevant partitions
        pk_hash index: finds rows in microseconds
        merges with Parquet archives when --archive-s3 is given

bintrail recover
    └── generates reversal SQL from row_before / row_after
        → apply manually to source database
```

---

## Built-in Rotation in `bintrail up`

`bintrail up` runs a built-in rotation loop **by default**: every hour it drops index partitions older than 30 days and keeps 3 future hourly partitions ready, so an unattended quickstart can never grow until the disk fills. The settings are announced loudly at boot. Under `bintrail-console watch` (the stream + console daemon — same `--rotate-*` flags and env vars) the loop additionally covers every per-source database the console control plane provisions (`bintrail_idx_<entry>`).

```sh
bintrail up ... --rotate-retain 90d        # keep more history
bintrail up ... --rotate-retain off        # disable entirely
BINTRAIL_ROTATE_RETAIN=7d bintrail up ...  # env form (also _INTERVAL, _ADD_FUTURE)
```

**Safety guards** (two, independent):

1. **Upgrade guard** — if you never set `--rotate-retain` (running on the implicit default) and the index **records no window of its own** (see below) and it already holds history extending beyond *twice* the window (>60d on the 30d default), the loop refuses to drop it: that depth of history means a deployment that predates built-in rotation, and an operator who never chose a retention must not lose months of forensic record to a binary upgrade. It logs an Error each cycle until you choose: `--rotate-retain 30d` to confirm the default, a larger window to keep more, or `off`. Fresh installs never trip this guard.
2. **Archive guard** — whether the built-in loop archives depends on how each target was provisioned. When a target carries an S3 archive destination (the console control plane sets this on the per-source databases it provisions), the loop itself archives each expired partition to S3 — and prunes the local staging copy after upload — before dropping it, the same as an explicit `rotate --archive-s3` run. When a target has **no** archive destination configured (the default boot index, or a BYO index with no console-provisioned archiving), the loop never archives on its own — it only drops-and-tops-up, and it defers to whatever *external* archiving flow it detects: if `archive_state` shows the index has *ever* been archived (e.g. your own `rotate --archive-dir` cron), the loop only drops partitions that are already archived, leaving partitions past retention but not yet archived for your cron (with a warning logged). An index with no archiving history at all — neither built-in nor external — rotates unconditionally (the bounded-volume quickstart behavior).

**Each index remembers the window it was created under.** When `init` (or `up`, `watch`, or the console control plane provisioning a per-source database) creates an index, it records the built-in default in force at that moment in the index's own `rotation_policy` table. While you set no retention yourself, the loop drops on **that** record, not on the running binary's default — so changing the default in a later release moves the indexes created from then on and never shortens the window an existing index has been running on.

An index created before this record existed carries none, and that absence is how the loop recognises it: it keeps 30 days, the default every such index ran under, and stays under the upgrade guard above. Whenever the window an index keeps differs from what a new index would start with, the daemon says so once per index, on that index's next rotation cycle (hourly by default).

The exemption from the upgrade guard is about the history, not about the record: an index created empty and then FILLED with older history — `restore-index` rebuilding it from the archives, or `bintrail index` over months of old binlog files — holds partitions older than its own record, so the guard still refuses to drop them until you choose a retention. Setting `--rotate-retain` (or `BINTRAIL_ROTATE_RETAIN`, or the console's rotation settings) overrides the record everywhere; an unreadable record falls back to the same 30 days and logs why.

If rotation makes no progress it should have — failing, deferring partitions to a stalled archiving flow, or any mix of the two — for 3 consecutive cycles, the loop escalates to an explicit Error in the logs: the index is growing unbounded and needs attention. The explicit `bintrail rotate` command is unaffected by all of the above: it keeps its unguarded, operator-asked-for-it semantics.

## Automating Rotation

In production, run `bintrail rotate` from an hourly cron job or systemd timer. A typical setup:

```sh
# Maintain a 30-day rolling window: drop old partitions and auto-add the same count back
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h
```

`--retain` automatically replaces every dropped partition with a new future hourly partition, keeping the total partition count constant. If you also want extra future partitions beyond the replacements (e.g. 48 hours of extra headroom), use `--add-future`:

```sh
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h \
  --add-future 48   # adds 720 replacements + 48 extras = 768 new partitions total
```

To archive partitions to S3 before dropping:

```sh
bintrail rotate \
  --index-dsn         "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain            720h \
  --archive-dir       /tmp/rotate-staging \
  --archive-s3        s3://my-bintrail-archives/events/ \
  --archive-s3-region us-east-1
```

To drop without adding anything back — useful when disk is critically full — use `--no-replace`:

```sh
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h \
  --no-replace
```

**Daemon mode** — instead of cron, `rotate` can run continuously and repeat on a schedule until `SIGINT`/`SIGTERM`:

```sh
bintrail rotate \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --retain 720h \
  --daemon \
  --interval 1h   # default 1h
```

This is the standalone equivalent of the rotation loop that `bintrail up` / `bintrail-console watch` run built-in — use it when you rotate a BYO index on a process separate from streaming.

Schedule the timer to run once per hour. The drop operation is instant, but `REORGANIZE PARTITION` on a partition containing data does a full table scan of `p_future` to redistribute rows — if your `p_future` is empty (because you add future partitions frequently), the reorganize is also instant.

## Baseline staleness

With `--baseline-dir`, `bintrail status` grades every baseline snapshot
against the oldest available delta coverage — the oldest **live partition**
hour (partition existence is coverage, even if the hour recorded no writes)
extended backwards by archives:
`ok`, `aging` (the snapshot is older than 80% of the coverage span — the
restore window is shrinking), `broken` (the anchor predates coverage —
full-table reconstruct through that window is impossible), or `unknown`
(the floor could not be evaluated — never reported as a false `ok`). A table whose
**newest** snapshot is broken trips a loud banner, and the JSON output
carries per-baseline `staleness` plus a top-level `baseline_staleness`.
The `watch` daemon's webhook channel sends a critical `baseline_stale`
event on the transition into broken (see
[console.md](console.md#webhook-notifications)). The fix is always the
same: take a fresh baseline (`bintrail dump` + `bintrail baseline`).

**Indexes capturing more than one source**: live partitions are shared by
every source, so the live floor needs no attribution — but archived
partitions are per-source, and a baseline snapshot carries no source
identity. On a multi-source index the archives therefore do **not** extend
the floor, and any snapshot older than the oldest live partition grades
`unknown` instead of `ok` or `broken`: claiming one source's archive
coverage for another would hide a genuinely broken restore window, and
declaring it broken would page you over archives that are perfectly fine.
The `watch` daemon reports those servers as *cannot be evaluated* (rate
limited to one warning per day) and — deliberately — never resolves a
standing `baseline_stale` alert on that basis. The way to get a graded
verdict is to keep one source per index database.

Do **not** read reconstruct's gap check as the source-exact fallback: its
planner classifies an hour as covered when *any* source archived it
(`archive_state` is scanned unscoped), so on a multi-source index it can
pass a window the graded source does not actually have. The honest check
there is to restore into a scratch target and compare — see
[drill.md](drill.md).
