# Query and Recovery

How to search indexed events with `bintrail query`, and turn them into SQL that undoes the original operations with `bintrail recover`.

---

## Querying the index

`bintrail query` searches `binlog_events` with optional filters. Results are
ordered by `event_timestamp` (then `event_id` as a tiebreaker) — **oldest first
by default**; pass `--order DESC` for newest-first. With no filters it returns
the first `--limit` events in that order. The same filter set powers
`bintrail recover` and the MCP `query` tool.

Filters: `--schema`, `--table`, `--pk`, `--pk-min` / `--pk-max` (an inclusive
key range, see below), `--event-type`, `--gtid`,
`--since` / `--until`, `--changed-column` (events that touched a given column),
`--column-eq` (events where a column has a given value — see below), and
`--flag` (tables/columns labeled via [RBAC flags](server-identity.md),
authored with `bintrail flag` or from the console's Settings > Access
profiles page). A
`--pk` lookup is fast and collision-safe — it matches a hash of the PK values
plus the exact values, and it requires `--schema` and `--table` alongside it.
That is not a formality: those two columns lead the index, so a lookup without
them would scan the table instead of seeking, and the command refuses rather
than doing that quietly. Binary primary keys need a special spelling — see
[the `0x` hex spelling](#binary-primary-keys-the-0x-hex-spelling) below.

### Binary primary keys: the `0x` hex spelling

A primary-key value whose bytes are **not valid UTF-8** — the typical case is
a `BINARY`/`VARBINARY` PK such as a binary UUID, but a `BLOB` or latin1 `TEXT`
prefix PK can land here too — is stored in `pk_values` as `0x` followed by
**uppercase** hex. To look such a row up, pass that exact form to `--pk` (or
`--pks`); the plain bytes will not match. This applies to `query`, `recover`,
and `recover-cascade`.

The spelling is deliberately identical to MySQL's own binary-literal syntax,
so you can produce it straight from the source table:

```sql
SELECT CONCAT('0x', HEX(pk_col)) FROM app.t_pk_binary WHERE ...;
```

```sh
bintrail recover --index-dsn "..." \
  --schema app --table t_pk_binary \
  --pk 0xB2815CC3C200FF7C0102030405060780 --dry-run
```

Three things to know:

- **The `0x` form is content-dependent, not type-dependent.** A binary column
  whose bytes happen to be valid UTF-8 (e.g. one holding ASCII text) is stored
  **verbatim**, not hexed — so the `HEX()` recipe above is only right when the
  raw bytes were unstorable. When in doubt, run `bintrail query` on the table
  and copy the row's `pk_values` exactly as displayed; the stored spelling is
  always the one that matches.
- **Uppercase matters, especially with `--pks`.** The stored form is uppercase
  hex, and the `--pks` filter (like the Parquet archive path) matches the
  stored string byte-for-byte with no hash-based guard — `0xab...` will not
  find a row stored as `0xAB...`. Type the hex exactly as `HEX()` prints it.
- **`reconstruct` and baseline-anchored `verify` accept these PK types**
  since [#1155](https://github.com/dbtrail/dbtrail/issues/1155) — the same
  `0x` spelling works for `reconstruct --pk`, and `verify` checks such tables
  instead of reporting them `inconclusive`. One wrinkle is handled for you: a
  fixed `BINARY(n)` column is padded with `0x00` on storage but the binlog row
  image drops that padding, so the two stores want the key spelled differently.
  All three index-reading surfaces — `reconstruct --pk`, the console
  `/api/reconstruct`, and the MCP `reconstruct` tool — reconcile both
  directions ([#1157](https://github.com/dbtrail/dbtrail/issues/1157)): the
  baseline reader re-pads the key to its declared width when the exact lookup
  misses, and the event fetch re-spells it back to the stored `pk_values` form
  (stripped, uppercase). So the stripped spelling the events views display, a
  lowercase paste of it, and the full-width
  `SELECT CONCAT('0x', HEX(pk_col))` form all resolve — and all fold the
  post-baseline deltas, never silently answering with baseline-era state. The
  declared width is read from the schema snapshot in effect when the baseline
  was taken, so widening the key with a later `ALTER` does not break lookups
  against older baselines
  ([#1159](https://github.com/dbtrail/dbtrail/issues/1159)). The one surface
  without the reconciler is `--baseline-only`, which never opens the index and
  so has no declared width to pad to — pass the full-width `HEX()` form there.
  `FLOAT`/`DOUBLE`, `TIME`, `BIT`, `JSON` and spatial primary keys remain unsupported.

### `--column-eq` Filter

Filters events by the value of a column inside the row image. Pass `--column-eq column=value`; repeat the flag to AND multiple entries:

```sh
bintrail query --schema mydb --table orders \
  --column-eq status=active --column-eq order_id=7
```

The generated predicate checks both sides of the event so DELETEs match when the value is in `row_before`:

```sql
JSON_UNQUOTE(JSON_EXTRACT(row_after,  '$.status')) = 'active'
OR
JSON_UNQUOTE(JSON_EXTRACT(row_before, '$.status')) = 'active'
```

`--column-eq` requires `--schema` and `--table`, matching the constraint on `--changed-column`.

**Column names** must match `[A-Za-z0-9_]+`. The column name is interpolated into the JSON path literal (MySQL does not accept bind parameters for paths), so the allowlist keeps the clause safe.

**NULL sentinel.** The literal value `NULL` (unquoted, case-sensitive) matches rows where the column is explicitly JSON null:

```sh
bintrail query --schema mydb --table orders --column-eq deleted_at=NULL
```

Internally this translates to `JSON_TYPE(JSON_EXTRACT(..., '$.deleted_at')) = 'NULL'` on both sides. It does **not** match rows where the column is absent from the row image (FULL row images always include every column, so absence only occurs when the source has `binlog_row_image != FULL`, which `bintrail index` rejects).

The literal value `NULL` is reserved as the JSON-null sentinel — there is currently no escape for matching a column whose value is the four-character string `"NULL"`. If you need that, file an issue.

The same filter is applied to DuckDB when archive auto-discovery routes the query through Parquet files (`json_extract_string` / `json_type`), so merged (live + archive) results stay consistent.

### Primary key ranges: `--pk-min` / `--pk-max`

`--pk` and `--pks` match exact keys. To ask for "every event on ids 1000
through 1999" (a burst of inserted ids, everything after a known cutoff row),
pass an inclusive range instead:

```sh
bintrail query --schema mydb --table orders --pk-min 1000 --pk-max 1999 \
  --since "2026-02-19 14:00:00" --until "2026-02-19 15:00:00"
```

Either bound works alone. The same flags exist on `recover`, where they scope
the reversal to the keys in the range, and the MCP `query` and `recover` tools
take them as `pk_min` / `pk_max`.

Three rules keep the answer honest:

- **Single-column integer keys only.** The command reads the table's key from
  the schema snapshot before it runs anything. A composite key, or a
  `DECIMAL`, `FLOAT`, string or binary key, is refused with the table's
  actual key shape (`range filters need a single integer primary key; this
  table's is (a, b)`), never answered by string order. `--schema` and
  `--table` are required for that lookup, and the range cannot be combined
  with `--pk` or `--pks`.
- **Signedness comes from the column.** `pk_values` is stored as text, so a
  bare comparison would say `9 > 10`. Both engines cast it to a 64-bit
  integer whose signedness matches the column (`CAST(... AS SIGNED)` or
  `UNSIGNED` on the live index, `BIGINT` or `UBIGINT` over the Parquet
  archives), so a negative key on a signed column and a key above 2^63 on an
  unsigned one both compare correctly, and live and archived results agree.
  A bound the column cannot hold (a negative bound on an unsigned key, a
  bound above 2^63-1 on a signed one) is refused before the query.
- **It scans.** The cast cannot use the key index, so within the partitions
  `--since`/`--until` keep it reads every event of the table. That is fine
  for an incident window and expensive across a whole retention period, so
  pair a range with a time window.

PostgreSQL snapshots do not record the column type the check needs, so the
range is refused on a PostgreSQL-sourced index.

### Output Formats

Results can be formatted three ways:

**`table`** (default): Uses `text/tabwriter` for aligned columns. Shows `event_id`, `timestamp`, `type`, `schema`, `table`, `pk_values`, `changed_cols`, and `gtid`. Does NOT include `row_before`/`row_after` — the table format is designed to be scannable. Use `--format json` to see full row data.

**`json`**: Each event is a JSON object with all fields including `row_before` and `row_after` as nested objects. Indented for readability. The `event_type` is serialized as a string (`"INSERT"`, `"UPDATE"`, `"DELETE"`), not the raw integer.

**`csv`**: All columns including `row_before`/`row_after` serialized as JSON strings in the CSV cells. Fixed column order matching `csvHeaders`.

### Statement capture: `query_text` and `query_hash`

When the source logs the original SQL statement alongside row events, `bintrail` stores it with every indexed event and shows it in the `json` and `csv` output (the `table` format omits it to stay scannable):

- **`query_text`** — the literal statement that produced the row event (`UPDATE users SET ...`), as the application sent it (subject to the sanitization notes below). One statement covers all of its rows: a 500-row bulk `DELETE` yields 500 events sharing the same text, and each statement in a multi-statement transaction carries its own.
- **`query_hash`** — MySQL's `STATEMENT_DIGEST()` of that text, computed against the **index** server at index time. Statements that differ only in literal values share one hash, so you can group by it to find the query patterns mutating a table ("which statement shape is behind this DELETE volume?").

**Filtering by statement** — pass a digest you already have back as a filter to see everything that statement did:

```bash
# every event the statement produced, across every table it touched
bintrail query --index-dsn "..." --query-hash 3f2a...e91 --format json
```

`--query-hash` is a **read** filter and exists only on `query` — not on `recover`. The digest names a statement *shape*, not one execution: `WHERE id=1` and `WHERE id=999` share it, so the filter returns every execution of that statement inside the window. That is the right answer for "what does this query pattern touch?" and the wrong basis for a reversal, which would undo executions you never named. Reverse a specific blast radius with the PK/time filters `recover` already has.

Three more things it will not do:

- It is refused, not silently ignored, when `--profile` is set. Under a profile the digest is blanked on every returned row, so filtering on it would confirm the statement the redaction hides — one guessed digest at a time.
- Archives written before statement capture existed have no digest column and contribute no rows. That is reported per source (`predate statement capture`, with a file count when only *part* of a source is affected), rather than failing the query — but the report is a log line, not an error, so an answer over a half-upgraded archive is narrower than it looks.
- **Statements over 16 KiB are unreachable by digest.** They are stored truncated and truncated statements are never digested (see the note below), so `query_hash` stays `NULL` on every row a giant bulk `UPDATE`/`DELETE` produced — which is often exactly the statement worth chasing after an incident. You can still see its `query_text`; you cannot filter by it.

When a digest filter returns nothing from a window where **no** event carries a digest at all, `bintrail query` says so on stderr instead of leaving you with a bare empty table: an empty result there means "this source was not logging statements", not "that statement touched nothing".

Capture is **opt-in at the source** — off by default on MySQL, on by default on MariaDB 10.2.4+:

```sql
-- MySQL (dynamic, no restart; costs binlog bytes per statement):
SET PERSIST binlog_rows_query_log_events = ON;

-- MariaDB (default ON since 10.2.4):
SET GLOBAL binlog_annotate_row_events = ON;
```

`bintrail doctor` reports the setting. On MariaDB, **streaming** capture additionally needs `--source-flavor mariadb` — the server only forwards ANNOTATE events to a replica that asks for them (file-based `bintrail index` reads them regardless). Events indexed while capture is off (and all events indexed before upgrading) simply have `NULL` in both columns — nothing else changes.

Notes:

- Statements longer than 16 KiB are stored truncated, ending in `/* bintrail:truncated */`. Truncated statements are not digested (`query_hash` stays `NULL`) — a mid-token fragment would misrepresent the statement's shape.
- Under an active `--profile`, `query_text` and `query_hash` are withheld on **every** row, not just rows of flagged tables — a single statement can touch several tables, so its text can embed values of a column your profile redacts even when the row itself belongs to another table.
- The web console never shows these fields (same boundary as `connection_id`).

### Commit time: `commit_ts_us`

Every event carries an `event_timestamp` with **one-second** resolution — that is all the binlog's common event header holds. A busy server commits many transactions inside one second, so within that second you can see event *order* but not event *time*.

MySQL 8.0.1 and later also write the transaction's commit instant in **microseconds** into the GTID event, and `bintrail` stores it as `commit_ts_us` (microseconds since the Unix epoch, shown in the `json` and `csv` output):

```
"event_timestamp": "2026-02-19 14:00:00",
"commit_ts_us": 1771509600123456
```

It is captured with no configuration: GTIDs do **not** have to be enabled, because MySQL stamps the value on the anonymous GTID event it writes with `gtid_mode=OFF` too. `commit_ts_us` is `NULL` for events indexed before the column existed, on MariaDB (its GTID event carries no commit timestamp), and on MySQL older than 8.0.1 — treat `NULL` as "only the one-second timestamp is known for this event", never as a tie.

Two things it makes possible that the second-resolution timestamp does not: ordering two changes that landed in the same second, and lining an indexed change up with any other microsecond-stamped record — an audit-log entry, an application trace, a slow-query log line.

---

## Parquet Archive Queries

When rotated partitions have been archived (via `bintrail rotate --archive-dir` or `--archive-s3`), events are no longer in the MySQL index. The `query` and `recover` commands can merge results from these archives with the live index.

### Auto-Discovery (default)

When you run `bintrail rotate --archive-dir` or `--archive-s3`, the archive locations are recorded in the `archive_state` table. Both `query` and `recover` automatically discover these sources — no extra flags needed:

```sh
# Archives are discovered from archive_state automatically
bintrail query \
  --index-dsn  "..." \
  --schema     mydb \
  --table      orders \
  --since      "2026-01-01 00:00:00"
```

### Explicit Archive Flags (override)

You can also specify archive sources explicitly with `--archive-dir` and `--archive-s3`. When these are set, auto-discovery is skipped. **`--bintrail-id` is required** with explicit flags — it scopes the DuckDB glob to that server's archives.

```sh
bintrail query \
  --index-dsn  "..." \
  --archive-s3 s3://my-bintrail-archives/events/ \
  --bintrail-id 3e11fa47-71ca-11e1-9e33-c80aa9429562 \
  --schema     mydb \
  --table      orders \
  --since      "2026-01-01 00:00:00"
```

### `--no-archive` Flag

Use `--no-archive` to disable archive auto-discovery entirely and return MySQL-only results. This is useful when you only want live data or when archive queries are slow:

```sh
bintrail query --index-dsn "..." --schema mydb --table orders --no-archive
```

`--no-archive` cannot be combined with `--archive-dir` or `--archive-s3`.

### Coverage Warnings and Query Planner

When a time range is specified (`--since`/`--until`), the query planner inspects live MySQL partition boundaries and the `archive_state` table to detect coverage gaps — hours where data has been rotated out of MySQL but no archive exists. These gaps are reported as warnings:

```
WARN query covers hours with no data (rotated and not archived): 2026-02-10 00:00 – 2026-02-12 23:00
```

The planner also optimizes routing: if the entire queried time range is covered by archives (no live MySQL partitions needed), the MySQL query is skipped entirely.

Archive coverage is **content-derived, not label-derived**: each `archive_state` row records the `MIN`/`MAX(event_timestamp)` of the rows actually archived, and the planner counts every hour in that range as covered. This matters for backfills — events replayed after a capture stall land in the *oldest live partition* and get archived under its hour label, so the file's `event_date=`/`event_hour=` path can be days newer than the rows inside it. The planner reports such mislabeled files to the archive fetcher, which then includes them in date-scoped S3 listings and Hive-path pruning even though their labels fall outside the queried window (row-level time filters still bound the result). Archives written before `min_event_ts`/`max_event_ts` existed fall back to label-only coverage. See [Backfilled events and the hour label](rotation-and-status.md#backfilled-events-and-the-hour-label).

### How merged results are deduplicated

When archives are in play, live MySQL and archive (Parquet) results are combined, deduplicated by `event_id` (the live MySQL row wins on a duplicate), sorted chronologically, and **`--limit` is applied once after the merge** — so no events are dropped before deduplication.

### Archive Fetch Error Handling

When an archive source fails — expired AWS credentials, S3 `AccessDenied`, DuckDB `memory_limit` OOM, a corrupted Parquet file, a `Binder Error` on a schema drift — `bintrail query` prints a visible warning to **stderr** regardless of `--log-level` or `--log-format`:

```
Warning: archive query failed for s3://my-bucket/events/bintrail_id=<uuid>: S3 AccessDenied: assume role failed
```

One failure always occupies exactly one stderr line: embedded newlines in the underlying error message (DuckDB Binder errors, AWS SDK error chains) are collapsed to ` | ` separators so `grep`, `systemd-journald`, and other line-oriented consumers don't split a single warning across multiple log entries.

The same record is also emitted as a structured `slog.Warn` with the **raw** (unsanitized) error for JSON-log consumers and full-fidelity debugging — a `--log-format json` pipeline preserves embedded newlines natively.

**Per-source failures are non-fatal.** One broken archive source does not kill the whole query — the loop continues to the next source, any other source that succeeds contributes its rows to the merged result set, and the command exits 0. This is deliberate: operators running against multi-region S3 archives shouldn't lose a regional query because one bucket is temporarily unreachable. If you need a strict all-or-nothing guarantee, use `bintrail reconstruct` (which sets `AllowGaps=false` in its shared `FetchMerged` pipeline and aborts if *any* archive source fails to load, since each source holds deltas no other source carries).

That strictness deliberately includes stale registrations: an `archive_state` row pointing at a local path whose Parquet files were later deleted (or never written) fails the strict query — the planner counted those hours as covered *from `archive_state`*, so a source that can't be read can't be proven harmless. The remediation is `bintrail archive reconcile --repair` (re-syncs `archive_state` with the files actually present — the flag matters: without `--repair` the command is a dry-run that only reports the drift; `--prune` additionally removes registrations whose files are gone from every referenced backend); `--allow-gaps` degrades to warn-and-continue if you accept a possibly-incomplete result. The same detection now covers S3: a registered source whose date-scoped listing comes back empty is probed once at the base prefix, and a source with no Parquet anywhere fails loudly instead of contributing silent emptiness. The same applies to a transiently unreachable S3 source (expired credentials, throttling): strict mode prefers a clear error over a silently incomplete reconstruction.

**Context cancellation is fatal.** If the user presses Ctrl-C, or the parent context times out, or a fetch error wraps `context.Canceled` / `context.DeadlineExceeded`, the archive loop short-circuits immediately and the command exits non-zero with `query canceled: context canceled`. No per-source warnings are printed during a canceled run — a Ctrl-C'd query should exit cleanly, not dump a warning per remaining source. When cancellation fires mid-loop, any rows already accumulated from earlier sources AND any live-MySQL rows fetched before the archive loop started are discarded: a canceled query is an incomplete query, and showing partial results alongside a "canceled" error would invite the operator to treat them as authoritative.

> **History**: `bintrail` versions before 0.4.8 silently swallowed every archive fetch error into a `slog.Warn` (invisible at the default text log format) and continued. A real production incident in early 2026 caused by pre-v0.4.4 Parquet files missing the `connection_id` column produced six days of zero-result queries with exit 0 and no stderr signal — only caught when a user escalated. 0.4.8 fixed the specific `Binder Error` trigger at the `parquetquery` layer; a follow-up fix (#203) surfaced every remaining archive failure mode on stderr so future unknown failures cannot reproduce the same silent-data-loss pattern.

### Memory Footprint

The merge path loads **all matching rows** from all sources into memory before applying the limit, so the result set is bounded by **row count** (`--limit`), not payload size. Filters (schema, table, time range) keep it manageable in practice — apply at least a `--since`/`--until` range for broad queries against large archives.

Three offline commands hold their working set in memory, so a BLOB/TEXT-heavy or very wide window can pressure RAM at scale. Each has a break-nothing safeguard ([#654](https://github.com/dbtrail/dbtrail/issues/654)):

- **`query --limit 0`** removes the row cap entirely — an unbounded scan into memory. It still works (some pipelines rely on it), but the command now prints a stderr warning; prefer a real `--limit N` or a tight `--since`/`--until` for large tables.
- **`recover`** buffers the whole reversal script in memory before writing (it must, to refuse cleanly on schema drift), roughly doubling peak on top of the already-loaded events. `--max-script-bytes` (default `2GB`; env `BINTRAIL_RECOVER_MAX_BYTES`; `0` = unlimited) makes it **refuse** rather than render a multi-gigabyte script — see [Recovery](#recovery). The bound is on the rendered script, not the initial fetch, which stays `--limit`-bounded.
- **`reconstruct --output-format mydumper`** keeps a per-touched-row change map *per table*, and reconstructs up to `--parallelism` tables (default `runtime.NumCPU()`) concurrently — so the process can hold several tables' maps at once, not just one. It no longer holds the event window itself: since [#1097](https://github.com/dbtrail/dbtrail/issues/1097) each table's window is **fetched a page at a time** and folded into the change map incrementally, so peak memory scales with *rows touched*, not *events fetched*. See [Streaming the event window](#streaming-the-event-window) below for the knob and its trade-off. `--warn-event-threshold` (default `5000000`; env `BINTRAIL_RECONSTRUCT_WARN_EVENTS`; `0` disables) logs a loud warning when a table's event count exceeds an **effective, per-table threshold scaled down by concurrency**: the configured threshold divided by `min(--parallelism, number of --tables)` ([#842](https://github.com/dbtrail/dbtrail/issues/842)) — so 8 tables reconstructing concurrently at 4M events each warn even though each is individually under the raw 5M default, while a single-table run is never penalized just because `--parallelism` defaults high. It only warns — it never refuses. The merge/baseline DuckDB sessions this mode opens also get the same container-safe budget (2 threads/4GB by default, or your `--ultrafast`/`--duckdb-threads`/`--duckdb-memory-limit` choice) as the archive-fetch DuckDB session, instead of defaulting to ~80% of host RAM ([#842](https://github.com/dbtrail/dbtrail/issues/842)). If a run is killed mid-way (OOM-killer included), the output directory is left flagged incomplete — see [Completeness marker](#completeness-marker) below.

The MCP server applies its own agent-facing row ceiling on the `query` tool — see [the MCP tool reference](mcp-server.md#tool-parameters-and-behavior-reference).

#### Streaming the event window

Full-table `reconstruct` rebuilds a table by merging a baseline snapshot with every binlog event since it. Those events used to be fetched in one call and held whole — including each event's *before* image, which the merge never reads.

They are now walked in pages ([#1097](https://github.com/dbtrail/dbtrail/issues/1097)). Each page is folded into the change map ("last event per row") and released, so the raw window never exists in full. `--fetch-batch-size` (default `100000`; env `BINTRAIL_RECONSTRUCT_FETCH_BATCH`; `0` = default) sets the page size:

| | Lower it | Raise it |
|---|---|---|
| **Peak memory** | Less resident per table — each page carries roughly 1–2 KB per event on a narrow table | More |
| **Round trips** | More: each page costs one index query **plus one scan per archive source** | Fewer |

**The archived-window cost is worth doing the arithmetic on.** For local archives DuckDB scans the files in place, but for **S3** archives each file is *downloaded* to disk, scanned, and deleted — per page. Page scoping advances with the cursor, so pages do not re-read the whole window; but it advances at **hour** granularity, and archive partitions are hourly. So the file for the hour a page lands in is re-fetched by every page that lands in that hour:

```
re-fetches of a given hour's file ≈ events in that hour ÷ --fetch-batch-size
```

At the default `100000`, an hour holding 100k events costs ~1 fetch (no amplification); an hour holding 1M events costs ~10. **Budget about double that**: file pruning keeps a file when its hour *ends* at-or-after the bound, so the hour before the cursor is re-fetched alongside the current one and yields nothing. DuckDB's row-group statistics prune the *scan*, not the *download*. If your busiest hours are far above the batch size and the window is mostly archived, raise `--fetch-batch-size` toward your peak hourly event count — memory permitting — before reaching for anything else.

**What this does not bound.** The change map holds one entry per **distinct row touched** in the window, and paging the fetch does not shrink it: the merge scans the baseline once and needs each row's final image at the moment it reaches that row. A window touching tens of millions of distinct rows is still dominated by the map. That half is tracked separately in [#1107](https://github.com/dbtrail/dbtrail/issues/1107). Narrowing the window — a fresher baseline, an earlier `--at`, or fewer `--tables` per run — is what reduces it today.

### TRUNCATE / DROP / RENAME in the reconstruction window

`TRUNCATE TABLE`, `DROP TABLE`, `RENAME TABLE` and MariaDB's `CREATE OR REPLACE TABLE` (on an existing table, a drop and a create in one statement) emit no row-level binlog events — only an audit entry in `schema_changes`. Because `reconstruct --output-format mydumper` and the shim's `_snapshot` merge a baseline snapshot with the row events fetched in that window, one of these statements between the baseline and `--at` leaves nothing for the merge to apply: without a check, the baseline's pre-truncate rows would pass straight through and be reported as the table's state at `--at`, even though the real table had none of them ([#764](https://github.com/dbtrail/dbtrail/issues/764)).

All three baseline-merging entry points — `reconstruct` single-row, `reconstruct --output-format mydumper` (full-table), and the shim's `_snapshot` (both single-row and full-table) — now query `schema_changes` for a `TRUNCATE TABLE`/`DROP TABLE`/`RENAME TABLE`/`CREATE OR REPLACE TABLE` on the target table in `(baseline snapshot time, --at]` and **refuse** the run with an error naming the DDL type and its detected timestamp, rather than silently resurrecting rows. Re-baseline the table after the DDL and reconstruct from the new baseline. A pre-DDL-tracking index (no `schema_changes` table) is not affected — the check treats a missing table as nothing to check, not a hard failure.

This is an offline/`_snapshot` concern only — `_flashback` and `bintrail query`/`recover` read the row-event history directly and never claim a never-touched row still exists.

### PK-changing UPDATE in the reconstruction window

An `UPDATE` that changes a row's **primary key** (e.g. `UPDATE orders SET id = 2 WHERE id = 1`) is stored in the index keyed by its **before-image** PK (`pk_values` = the old key). `reconstruct` folds events into a change map keyed by that before-image PK, so a changed PK cannot be applied safely — the after-image row it would emit carries a *different* key than its map entry:

- **Resurrection** — `UPDATE pk 1→2; DELETE pk=2`: the `DELETE` is keyed by the new PK (`2`) and never collides with the `UPDATE` entry (keyed by `1`), so a naive merge emits the `pk=2` after-image for a row the `DELETE` actually removed.
- **Duplication** — `UPDATE pk 1→2; UPDATE pk=2`: `pk=2` is emitted twice, a `1062 Duplicate entry` that only surfaces when you load the dump.
- **Silent drop** — `UPDATE pk 1→2; INSERT pk=1`: the later `INSERT` reuses the old key, overwriting the `UPDATE` in the change map — a map-only check misses it and the moved row (`pk=2`) is never emitted.

Rather than ship a silently duplicated, resurrected, or row-dropping dump, the full-table reconstruct entry points detect a PK-changing `UPDATE` up front and **refuse** the run with an error naming the table and the `old → new` key transition ([#782](https://github.com/dbtrail/dbtrail/issues/782)). The detection scans the **raw event stream** before it collapses into the change map (so the silent-drop permutation above cannot slip through). `reconstruct --output-format mydumper` and the binlog-only fallback for a never-baselined table fetch the **whole** window, so their check covers every event in `(baseline snapshot time, --at]`. The shim's `_snapshot` and `verify` fetch only the **latest event per PK** (a query-time optimization), so they refuse whenever the PK-changing `UPDATE` is the surviving event for its old key; a PK-changing `UPDATE` later superseded on that key is outside their fetched set — `verify` then reports the resulting drift as a **mismatch** rather than passing silently. **Single-row** `reconstruct` for the *new* key returns a clear "a PK-changing UPDATE in the window likely brought this PK into existence … cannot be resolved" message instead of the misleading `no row found in baseline` (the row *did* exist — under a different stored key). In every case: re-run `bintrail baseline` to capture a snapshot **at or after** the PK change, then reconstruct from the new baseline.

### Writing the result as a baseline instead (`--output-format parquet`)

Full-table `reconstruct` can write its result as a **baseline snapshot** rather than a SQL dump ([#1169](https://github.com/dbtrail/dbtrail/issues/1169)): same layout, same metadata and same `_SUCCESS`/`_MANIFEST` files `bintrail baseline` produces, so the next reconstruct discovers it as the newest baseline with no configuration change. That closes the loop — a fresher snapshot no longer requires re-dumping a source the index has already recorded every change to. `--output-dir` is the baselines **root** in this mode, and the snapshot lands in a `<timestamp>/` subdirectory under it.

Every limit and refusal described above applies unchanged, plus two of its own: a table with no baseline is refused rather than degraded to the binlog-only fallback (a snapshot folded from deltas alone would omit every row the window never touched), and the emitted snapshot carries **no content digest** — the digest certifies fidelity against the live source, which a reconstructed snapshot never read. See [Dump and Baseline](dump-and-baseline.md#step-3-optional-a-newer-baseline-without-a-new-dump) for the full contract, including how the binlog coordinate the next reconstruct resumes from is chosen.

### Completeness marker

`reconstruct --output-format mydumper` writes one mydumper-shaped output per table into `--output-dir`. A run that gets killed uncatchably mid-way — the OOM-killer, `SIGKILL`, a power loss — leaves the tables that finished before the kill (data + schema files) on disk, with nothing distinguishing that directory from a genuinely complete dump missing only the tables that never started: the absent files are the only hint, and an automated loader has no way to tell "the other tables are just empty" from "the other tables were never dumped."

To close this, `reconstruct --output-format mydumper` writes a completeness marker into `--output-dir`, reusing the exact `_SUCCESS` / `_INCOMPLETE` convention `bintrail baseline` established for the same failure mode ([#467](https://github.com/dbtrail/dbtrail/issues/467)) rather than inventing a second one ([#842](https://github.com/dbtrail/dbtrail/issues/842)):

- `_INCOMPLETE` is written before any table is converted, and any stale `_SUCCESS` left by a prior run into the same (reused) directory is removed at that point too — that removal is itself fatal (aborts the run) rather than best-effort, since a surviving stale `_SUCCESS` would defeat the marker for the whole run.
- `_SUCCESS` replaces it only when every requested table converts without error, the run was not cancelled, **and** the shared `metadata` file (binlog coordinates for the baseline anchor) was written successfully — a dump with every table's data but no `metadata` file is a smaller instance of the same incompleteness this marker exists to signal, so it is never marked `_SUCCESS` either.

Under `--output-format parquet` the same discipline applies to the **snapshot** directory (`<output-dir>/<timestamp>/`) rather than to `--output-dir` itself — that is the directory baseline discovery reads, so a run killed mid-write is never discoverable as a baseline. There, a re-run into an existing snapshot directory is refused outright instead of clearing the stale marker: two runs sharing one timestamp would interleave tables folded at different points under a single anchor.

A directory with neither marker (pre-#842 output, or any other mydumper-shaped dump) is treated as complete by convention — only an explicit `_INCOMPLETE` (with no `_SUCCESS`) signals a genuinely partial run. Re-run `reconstruct` into a fresh directory, or the same one, to retry.

### DuckDB resource tuning (`--ultrafast`)

When `query`, `recover`, or `reconstruct` scan Parquet archives, DuckDB runs under a conservative, container-safe budget by default: **2 threads and a 4 GB memory limit**, spilling to the OS temp directory when exceeded. These defaults keep bintrail alive in small shared containers; on a dedicated box with plenty of RAM they leave performance on the table.

- `--ultrafast` lets DuckDB self-tune to the host: it uses **all CPU cores** and **~80% of physical RAM**, still spilling to the temp directory before hitting the limit. This trades memory-safety for speed — use it for offline recovery on a big machine, **not** in a small container. It does **not** remove the memory limit (which would invite the OOM-killer instead of a graceful spill); it lets DuckDB pick its own RAM-aware limit.
- `--duckdb-threads N` and `--duckdb-memory-limit SIZE` (e.g. `16GB`) override either knob independently. An explicit flag wins over `--ultrafast`, which wins over the default — so you can tune to your box without the all-or-nothing switch.

**S3 archives under `--ultrafast`:** the default path downloads each S3 Parquet file to disk and queries it locally (memory-safe). `--ultrafast` instead reads them **directly via DuckDB's `httpfs` extension** in one parallel multi-file scan — faster (no download round-trip, all files scanned concurrently), but `httpfs` holds each scanned file **in memory, outside the `memory_limit` budget**. Peak RAM is roughly **`largest_file_size × thread_count`**, so on a 32-core host scanning 500 MB files that is ~16 GB. The command logs a warning with the estimate when this path activates. **Lower `--duckdb-threads N` to bound the peak to `N` concurrent files** — under `--ultrafast` on S3, `--duckdb-threads` is a memory-safety knob, not just a speed one. (`--ultrafast` is required for this path; the granular flags alone keep the safe download path.)

Equivalent env vars: `BINTRAIL_ULTRAFAST=1`, `BINTRAIL_DUCKDB_THREADS`, `BINTRAIL_DUCKDB_MEMORY_LIMIT`. These flags affect only the offline CLI commands; the long-lived `shim` and `bintrail-console` daemons always use the safe default.

For `reconstruct --output-format mydumper` and `verify`, this budget also covers the merge/baseline DuckDB sessions each command opens directly (streaming the baseline Parquet, downloading an S3 baseline) — not just the archive-fetch session ([#842](https://github.com/dbtrail/dbtrail/issues/842)). Before this, those sessions ignored `--ultrafast`/`--duckdb-*` entirely and always defaulted to DuckDB's native host-greedy budget (~80% of RAM, one thread per core), regardless of what you configured for archive reads — every such session is also paired with a `temp_directory` spill backstop pointed at the OS temp directory, so a query that exceeds an explicit `--duckdb-memory-limit` spills to disk instead of failing outright (DuckDB's own default temp directory is relative to the process's working directory, which is read-only in many containers).

### S3 Prerequisites

- **Archived events** (`--archive-s3`): objects are listed and downloaded with the **AWS SDK** — the full default credential chain applies (env keys, `~/.aws` profiles incl. SSO, EC2/ECS/EKS IAM roles). Every `--archive-s3` query (with or without `--ultrafast`) auto-detects the bucket's region via `s3:GetBucketLocation`, a permission [the documented minimal IAM policy](upload.md#minimum-iam-permissions) deliberately omits — an `AccessDenied` there is expected under that policy, not a misconfiguration: it's logged at debug level (not a warning), and bintrail falls back to the region already resolved for the credential chain (env/config, or EC2/ECS/EKS instance metadata via IMDS). Grant `s3:GetBucketLocation` only if your archive bucket lives in a **different region** than the one otherwise resolved for that principal — the fallback is correct in the common case where they match. DuckDB then scans the downloaded files locally; its `httpfs` extension is not involved on this path — **except under `--ultrafast`**, which instead reads the files directly via `httpfs` (credentials and region resolution are unchanged; only how the files are read differs). See the `--ultrafast` note above for the memory trade-off.
- **Baselines** (`--baseline-s3` / reconstruct / `--include-snapshot`): read through DuckDB `httpfs`, with AWS-SDK-chain credentials from the `aws` extension's `credential_chain` secret, set up automatically (SSO-session profiles have open gaps upstream — [duckdb-aws#125](https://github.com/duckdb/duckdb-aws/issues/125)). When only the `aws` extension is unavailable, reads fall back to static env keys (routing to a custom S3 endpoint is unaffected: it is applied with `SET GLOBAL`, which needs `httpfs` but not the `aws` extension) (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` + `AWS_REGION`); if `httpfs` itself cannot load, the read fails outright. Airgapped: extensions cache under `~/.duckdb` **per DuckDB version and platform** — run any bintrail command that touches `s3://` once on a connected machine of the same OS/arch and bintrail release, then copy the cache (a standalone `duckdb` CLI of a different version writes a cache the bundled engine never reads). `BINTRAIL_DUCKDB_NO_AWS_EXT=1` skips the aws-extension setup — use it behind proxies that blackhole the extension registry, where the install attempt can stall for minutes per session.

---

## Recovery

The `recover` command also supports archive auto-discovery and the `--no-archive` flag, using the same merge logic as `query`. When archives are available, events are fetched from both MySQL and Parquet, merged, and then turned into reversal SQL.

> **`recover` vs `reconstruct` vs `verify` — three different jobs.**
> - **`recover`** (this page) undoes *specific touched rows* from stored before/after images — delta-only. It cannot materialize a whole table.
> - **`reconstruct`** materializes a *full table or single row as of a point in time* by merging a baseline snapshot with binlog deltas — see [Dump & Baseline](dump-and-baseline.md) and [Time-Travel SQL](time-travel-sql.md).
> - **`verify`** doesn't recover anything; it *proves* the reconstruct chain reproduces the source so your recoveries are trustworthy before you need them — see [Verify recoveries](verify.md).

### The Concept

Recovery works because DBTrail stores **full before and after images** for every row event. To undo an operation, you simply reverse it:

| Original operation | Reversal |
|--------------------|----------|
| `DELETE` | `INSERT` the deleted row back (from `row_before`) |
| `UPDATE` | `UPDATE` back to `row_before` values, `WHERE` the current state matches `row_after` |
| `INSERT` | `DELETE` the row (using `row_after` to identify it) |

Recovery never executes SQL — it only generates a script you review and apply yourself.

### Reverse Chronological Ordering

Before generating SQL, the generator reverses the event list so the **most recent event is undone first**. This matters for sequences like:

```
INSERT id=5 (at 14:01)
UPDATE id=5: status=draft→published (at 14:02)
UPDATE id=5: status=published→deleted (at 14:03)
```

Reversed: undo the 14:03 UPDATE first, then the 14:02 UPDATE, then the 14:01 INSERT. This is the correct rollback order for any sequence of operations on the same row.

> **Foreign keys are NOT handled.** Reverse-chronological ordering is the only
> ordering `bintrail recover` applies — there is no FK-graph analysis, no
> topological reordering across tables, and the generated script never emits
> `SET FOREIGN_KEY_CHECKS`. Tables with `ON DELETE/UPDATE CASCADE` produce
> side-effect row changes (InnoDB runs cascades below the binlog, MySQL Bug
> #32506, so neither the cascaded child deletes nor the child FK rewrites an
> `ON UPDATE` cascade performs are ever captured) that plain `recover`
> cannot reliably undo. `bintrail doctor`, and `stream`/`watch`/`index --source-dsn`,
> warn about them and proceed (cascade schemas index normally). To reconstruct
> those rows, use **`bintrail recover-cascade`** (see below).

### `recover-cascade`: reverse FK ON DELETE / ON UPDATE CASCADE / SET NULL

`bintrail recover-cascade` reconstructs the side effects of an InnoDB foreign-key
cascade that were never binlogged. It finds the changed parent rows in the index,
infers which child rows referenced them in their last indexed state, and emits
reversal SQL:

- **ON DELETE CASCADE** → re-inserts **both** the parents and their
  cascade-deleted descendants (recursing through multi-level cascades).
- **ON DELETE SET NULL** → an **idempotent** `UPDATE` restoring each nulled FK,
  guarded by `... AND fk IS NULL` so a re-run, a manual fix, or a later re-point
  of the child is never clobbered (the child row survives — only its FK was
  nulled — so it is not re-inserted).
- **ON UPDATE CASCADE** → for a parent whose **referenced key** was `UPDATE`d,
  InnoDB rewrote every child FK that pointed at the old key. The reversal is an
  idempotent `UPDATE` putting each child FK back, guarded by
  `... AND fk = <new key>` — again so a child re-pointed after the cascade is
  never clobbered. The parent `UPDATE` itself is reversed alongside it.
- **ON UPDATE SET NULL** → same, except the children were nulled rather than
  re-pointed, so the guard is `... AND fk IS NULL`.

Only parent `UPDATE`s that actually **moved a referenced key** are considered: an
`UPDATE` of unrelated columns cannot have cascaded, so it synthesizes nothing and
its own reversal is **not** emitted either.

All wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. Like `recover`, it only generates
SQL — review before applying.

```bash
bintrail recover-cascade --index-dsn "..." \
  --schema shop --table orders --pk '42' --dry-run
```

- `--table` is the **parent** table whose delete or key update cascaded;
  `--pk`/`--pks`, `--since`/`--until` narrow which parents to process.
- `--lookback` (default `30d`) bounds how far back the last child state is
  searched; `--max-depth` (default 5) bounds cascade recursion.
- **Phase-2 baseline fallback** (`--baseline-dir` or `--baseline-s3`): without a
  baseline, a child untouched within `--lookback` (e.g. an insert-once row from
  months ago) cannot be reconstructed — only children with a binlog event in the
  window are visible. Point at a `bintrail baseline` snapshot and those untouched
  children are recovered from it too, and the binlog window is widened to the
  snapshot time. Tables not covered by the baseline are flagged incomplete.
- **Archives are searched.** The parent and child scans read the Parquet
  archives auto-discovered via `archive_state`, merged with the live index the
  same way `recover` does, so a cascade whose evidence rotated out of the live
  index is still reconstructed. `--no-archive` confines the scan to the live
  index; an archive the host cannot read makes the command refuse (a reversal
  missing part of the evidence is a partial undo), naming `--no-archive` as the
  escape hatch. The DuckDB tuning flags (`--ultrafast`, `--duckdb-threads`,
  `--duckdb-memory-limit`) apply to the archive reads.
- **Still best-effort:** baseline augmentation is skipped (and flagged) for a
  table when the scan cannot serve every hour of the `[snapshot, T]` window —
  an hour rotated out with no readable archive (or with archives excluded), an
  hour before the index existed, or a permanent capture loss stamped inside it —
  because a child re-parented or deleted in that gap can't be told apart from an
  untouched one; and when one parent has more cascade victims than the
  per-parent cap. When augmentation is skipped the child scan falls back to the
  plain lookback window (widened to the snapshot when that is older), with the
  baseline still used as a membership filter for children whose last event
  predates it. A table with no baseline keeps the Phase-1 window limit. When the result is
  provably partial the output is flagged `INCOMPLETE RECOVERY` and the command
  exits non-zero unless `--allow-incomplete` is given. If you have already
  re-created a deleted parent, remove its `INSERT` from the output —
  `FOREIGN_KEY_CHECKS=0` does not suppress primary-key violations.

#### `recover-cascade` limitations

- **`delete_rule` and `update_rule` are never conflated.** A parent `DELETE`
  routes through the `ON DELETE` rule only, and a parent key `UPDATE` through
  the `ON UPDATE` rule only. So a `DELETE` on a table whose children are
  `ON UPDATE CASCADE`-only reconstructs nothing (correctly — InnoDB cascaded
  nothing), and vice versa.
- **A repeated parent-key `UPDATE` inside the window under-recovers, and says
  so.** If the same referenced key moved twice (`A → B`, then `B → A`), the
  first cascade rewrote the children below the binlog too, so their last
  *indexed* image still carries `A` and the scan for the second update's old key
  (`B`) matches nothing. That run is flagged `INCOMPLETE RECOVERY` — a
  zero-child result there is not proof there were no children. The chain is
  detected by asking whether the key *arrived* at the parent from somewhere else
  inside the window, so the flag holds whether the referenced column is the
  parent's primary key (`REFERENCES parent(id)`) or a secondary unique key.
- **Composite (multi-column) FKs are skipped, not reconstructed.** A
  single-column victim match would mis-reconstruct a multi-column key, so a
  composite FK is dropped and flagged in the output rather than silently
  matched on one column.
- **Phase-2 (baseline) FK-column type restriction.** The baseline scan binds
  the parent key as a **string** against the child's FK column; DuckDB coerces
  this reliably only for integer and character (`CHAR`/`VARCHAR`/`TEXT`/`ENUM`/
  `SET`) families. A `DATETIME`, `DECIMAL`, `DATE`, or binary-typed FK column
  refuses Phase-2 baseline augmentation (flagged as a coverage gap) rather than
  risk a silent zero-match — so a `BINARY(16)`/UUID-as-binary FK loses Phase-2
  entirely and falls back to Phase-1 (binlog-window only).
- **Cross-schema FK children are included.** A child table in a *different*
  schema with an `ON DELETE CASCADE` / `SET NULL` FK to the `--schema` parent is
  legal in MySQL (common in multi-tenant / reporting layouts) and is
  reconstructed: `recover-cascade` scopes the FK-graph load by the *parent*
  schema (`referenced_schema_name`) and walks it transitively, so multi-level
  cross-schema cascades are covered too. (Earlier releases scoped the load by the
  child's own schema and silently dropped cross-schema children — [#833](https://github.com/dbtrail/dbtrail/issues/833).)
- **The FK graph reflects the latest schema snapshot, not the one in effect at
  the delete being recovered.** If DDL changed the FK topology between the
  delete and now, synthesis uses the newer graph. Acceptable in practice
  because cascade-topology churn mid-recovery-window is rare, and the
  `FOREIGN_KEY_CHECKS=0` wrapper tolerates the resulting over-/under-inclusion
  (which the operator reviews before applying).

### Recovering many rows at once

To reverse a specific set of primary keys, pass `--pks` (comma-separated) instead
of a single `--pk`, and cap how many events are undone per key with
`--limit-per-pk`. To reverse every event on a contiguous run of integer keys,
pass `--pk-min` / `--pk-max` instead (see
[Primary key ranges](#primary-key-ranges---pk-min----pk-max); the same shape
check and scan cost apply). These compose with the shared event filters
(`--schema`/`--table`/`--event-type`/`--since`/`--until`/`--column-eq`/`--gtid`/`--flag`).
For binary primary keys, each entry must use the stored `0x` uppercase-hex
spelling — see [the `0x` hex spelling](#binary-primary-keys-the-0x-hex-spelling).

**PostgreSQL sources:** when the source is PostgreSQL, `recover` emits
PostgreSQL-dialect reversal SQL (double-quoted identifiers and string/boolean literals) rather than
MySQL syntax — see [PostgreSQL](postgres.md#querying-and-recovering).

> **Large recoveries are bounded ([#654](https://github.com/dbtrail/dbtrail/issues/654)).** The whole
> reversal script is buffered in memory before any byte is written, so a bulk recovery over
> BLOB/TEXT-heavy rows can use a lot of RAM. `recover` **refuses** (loudly, writing nothing) when the
> estimated script payload exceeds `--max-script-bytes` — default `2GB`, `0` disables, env
> `BINTRAIL_RECOVER_MAX_BYTES`. If you hit it, narrow `--since`/`--until`/`--pk`/`--limit`, or raise the
> budget. Note `--limit` caps the event **count** (default 1000); this budget guards the rendered
> **script size**. Binary columns render to hex (~2× their stored bytes), so for binary-heavy tables set
> the budget below the RAM you can actually spare. The same default guards `recover-cascade`, the
> console, and the MCP `recover` tool, which inherit it with no configuration.

### WHERE Clause Strategy

For `UPDATE` and `DELETE` reversals, the generator needs a `WHERE` clause to identify the correct row in the current database state.

**With a schema snapshot (preferred)**: Uses only the primary key columns from the resolver. This produces a clean, minimal WHERE clause:

```sql
UPDATE `mydb`.`orders` SET `status` = 'draft' WHERE `id` = 42
```

**Without a snapshot (fallback)**: Uses every column in the row image, capped at one row:

```sql
UPDATE `mydb`.`orders`
SET `status` = 'draft'
WHERE `id` = 42 AND `status` = 'published' AND `created_at` = '2026-02-19 14:01:00' AND `notes` IS NULL LIMIT 1
```

- A `NULL` column renders as `IS NULL` (not `= NULL`, which never matches in SQL).
- An all-columns `WHERE` matches **every byte-identical duplicate row** (PK-less table, no natural key), while the original event touched exactly one row. Fallback `UPDATE`/`DELETE` reversals therefore carry `LIMIT 1` on the MySQL dialect, so reversing one `INSERT` removes one copy instead of silently deleting all duplicates. Which physical copy is affected is undefined — and irrelevant, since the matching rows are identical on every referenced column.
- PostgreSQL has no `UPDATE`/`DELETE ... LIMIT`, so PostgreSQL-dialect fallback statements remain unbounded: with byte-identical duplicate rows the reversal still over-applies there. The generated script flags each affected statement with a leading `-- WARNING: unbounded all-columns WHERE, no per-statement row cap on this dialect ...` comment so the risk is visible in `--dry-run`/`--output` review, not just here. If you rely on this fallback against PostgreSQL, verify the table has a natural uniqueness property before applying the generated script.

The resolver is loaded best-effort in the `recover` command — a failure logs a warning and falls back to the all-columns strategy.

**A subtle detail for `UPDATE` reversals**: The `WHERE` clause uses `row_after` (the current database state), not `row_before`. This is correct because the current database reflects the `row_after` state. It also handles the edge case where the `UPDATE` changed the primary key itself — the `WHERE` still finds the right row.

### Generated Column Handling

Generated columns (`STORED` or `VIRTUAL`) are computed by MySQL and cannot be set explicitly, so the generated script skips them in `INSERT`/`UPDATE` SET clauses — the script won't fail trying to assign a value MySQL owns.

### System-versioned tables (MariaDB)

MariaDB silently extends a system-versioned table's `PRIMARY KEY` with its `ROW END` period column (`PRIMARY KEY (id, row_end)`), and that column is generated — baselines never carry its values, and the binlog records history rows and versioned `DELETE`s under the same surviving key. This applies to **both** spellings: with explicitly declared period columns, and the short implicit form (`CREATE TABLE ... WITH SYSTEM VERSIONING`), whose hidden `row_start`/`row_end` don't appear in `information_schema` — `bintrail snapshot` detects the implicit form and records them anyway, so capture works and both forms behave identically. **Existing indexes need a fresh `bintrail snapshot`** for this to take effect: until then the old snapshot undercounts the table and its events keep being skipped. Note the transition: once re-snapshotted, such a table starts hitting the full-table refusal above (correct parity with the explicit form), so if it sits inside a `baseline refresh` scope, exclude it via `--tables` or the refresh — which is all-or-nothing — will publish nothing. Also new: versioned tables are no longer invisible to snapshot **validation** — a system-versioned table without an explicit primary key (or not using InnoDB) now fails `bintrail snapshot` like any other such table, where it previously slipped through unvalidated; narrow `--schemas` or add a primary key to proceed. Rendering such a table as a whole would risk emitting history rows as live data, so bintrail refuses instead:

- **Full-table `reconstruct`** (baseline merge and binlog-only fallback alike) and the shim's **full-table `_flashback`/`_snapshot`** views refuse with an error naming the generated PK column.
- **`verify`** reports the table as `inconclusive` — a permanent property of the table's shape, not a failure. A run scoped only to such tables still exits non-zero (nothing was proven).
- **`recover-cascade`** skips any FK edge whose **child** table's primary key contains a generated column (typically MariaDB system versioning), reporting it as a permanent incompleteness caveat (its cascade side effects are not synthesized) — a candidate scan over such a child would rebuild rows from history-polluted state. Detection reads the schema snapshot, so keep it current (`bintrail snapshot`); without one, the gate only fires when a configured baseline covers the table.
- **Still available**: `query` and `recover` are not gated, and single-row `reconstruct` works with an explicit column list, e.g. `--pk 1 --pk-columns id`.

### Column type encodings (BLOB/TEXT, GEOMETRY, VECTOR)

Columns that MySQL delivers as raw bytes are stored base64-encoded in the index, so `recover` decodes them back to a loadable literal using the column type from the schema snapshot:

- **`BLOB`/`BINARY`/`VARBINARY`** → an `X'<hex>'` literal; **`TEXT`/`JSON`** → a decoded string literal.
- **`GEOMETRY`** (and `POINT`, `LINESTRING`, `POLYGON`, … the whole spatial family) → `ST_GeomFromWKB(X'<wkb>', <srid>)`. The at-rest MySQL geometry value is `SRID` (4 bytes, little-endian) followed by the WKB; `recover` splits off the SRID and passes the WKB to `ST_GeomFromWKB` with the SRID as its second argument. Before this, a geometry column emitted its raw base64 string, which a geometry column cannot load — failing the entire `BEGIN`/`COMMIT` script over a single geometry value.
- **`VECTOR`** (MySQL 9.0+) is **not yet** transformed. Its base64 value is emitted as-is, which a real `VECTOR` column rejects at apply time (a loud failure, not silent corruption). A `STRING_TO_VECTOR('[…]')` decode of its packed-float at-rest form is a follow-up.

Typing these columns requires a schema snapshot that describes the table. **Without a snapshot covering the table** — none loaded, or the loaded one omits it — `recover` cannot type its columns, so a `BLOB`/`TEXT`/`BINARY`/`VARBINARY`/`GEOMETRY` value is emitted as its **stored base64 text** (e.g. `'aGVsbG8='` instead of `'hello'`), which the target column will not load correctly. Scalar columns (`INT`, `VARCHAR`, `DATETIME`, …) are unaffected and reverse correctly with or without a snapshot. If a table you need to recover has byte-typed columns, take a `bintrail snapshot` covering it **before** recovering so those columns decode to a proper literal.

### Output Format

The recovery output is a self-contained SQL script:

```sql
-- Generated by bintrail recover at 2026-02-19 14:30:00 UTC
-- Events to reverse: 3
-- IMPORTANT: Review carefully before applying to production.

BEGIN;
SET time_zone = '+00:00';
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION';

-- [47] reverse DELETE on mydb.orders pk=42 at 2026-02-19 14:03:00 gtid=3e11fa47-...:99
INSERT INTO `mydb`.`orders` (`id`, `status`, `created_at`) VALUES (42, 'draft', '2026-02-19 14:01:00');

-- [46] reverse UPDATE on mydb.orders pk=42 at 2026-02-19 14:02:00 ...
UPDATE `mydb`.`orders` SET `status` = 'draft' WHERE `id` = 42;

COMMIT;
```

Key properties:
- Wrapped in `BEGIN` / `COMMIT` — all changes apply atomically or not at all.
- Comments before each statement showing the original event ID, type, table, PK, timestamp, and GTID.
- **Per-event generation errors refuse the whole script.** If any event cannot be reversed — e.g. a malformed or truncated stored row image leaves `row_before`/`row_after` `NULL` — `recover` fails loud: it writes nothing and exits non-zero (with `--output`, the target file is left empty), and the error names every un-generatable event. It does **not** emit the rest as a runnable script with the failed events demoted to `-- ERROR ...` comments — a SQL comment has no apply-time effect, so a partial script would commit clean under `BEGIN`/`COMMIT` and silently deliver an *incomplete* reversal. **Schema drift** is the same: if a statement references a column dropped or renamed after the event, `recover` refuses up front rather than emitting SQL that would fail at apply time. Always check the exit code before applying a generated file.
- **Never auto-executed**: DBTrail only generates the file. Applying it is always a manual step.
- `bintrail` (MySQL) pins the apply session before the reversal statements: `SET time_zone = '+00:00'` (TIMESTAMP/DATETIME literals in the script are rendered from the captured UTC value with no explicit zone marker — without the pin, a target session in a non-UTC `time_zone` would reinterpret them and reintroduce a shift) and `SET sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'` — permissive where the script's own encoding needs it (no `NO_BACKSLASH_ESCAPES`, so the backslash-escaped string literals parse as written; no zero-date rules, so captured `0000-00-00` values apply verbatim) while keeping strict truncation/out-of-range checks, so a captured value that no longer fits a since-narrowed column fails loud instead of being silently coerced. `bintrail-pg` (PostgreSQL) pins `standard_conforming_strings = on` for the same reason (its string-escaping assumes it). Beyond these, nothing else about the apply session is pinned *by default* — one more pin is available opt-in on the PostgreSQL path (`--suppress-triggers`, below), and see [Restore limitations](#restore-limitations-mysql) for what is **not** pinned or restored.
- **`--suppress-triggers` (PostgreSQL sources only).** Adds `SET LOCAL session_replication_role = replica;` to the preamble, inside the script's own transaction, so the reversal does **not** re-fire the target's ordinary (`ENABLE`) triggers (see [Triggers re-fire on apply](#restore-limitations-mysql) below for why that matters). Be precise about what `replica` does and does not suppress: triggers configured `ENABLE ALWAYS` **still fire**, and triggers configured `ENABLE REPLICA` fire **only** in this mode — so the flag can *cause* trigger side effects that a normal apply would not. Suppression also only helps when the reversal's scope covers the tables the skipped triggers write: with `--table`/`--pk` filters, the trigger's derived rows (audit rows, counters) were never fetched, so they are neither re-fired nor reverted — they silently keep the state the bad change produced. It is opt-in, not the default, for two reasons: setting that parameter requires superuser (PostgreSQL ≤ 14) or an explicit `GRANT SET ON PARAMETER session_replication_role TO <role>` (15+), so emitting it unconditionally would break every apply performed by an ordinary role; and `replica` **also skips `FOREIGN KEY` constraint triggers, permanently**: a skipped check is never re-run — there is no deferred re-validation at `COMMIT` — so rows written in violation stay violating while the constraint remains marked valid. `SET LOCAL` is transaction-scoped — the applying session's setting is restored at `COMMIT`/`ROLLBACK` — but it **fails soft outside a transaction block** (PostgreSQL warns and does nothing, and triggers then fire while you believe they are suppressed), so apply the file as a whole, in one session (e.g. `psql -f reversal.sql`); a client that autocommits each statement separately defeats the script's own `BEGIN`. The flag has no effect on a MySQL/MariaDB index (`recover` warns if you pass it there, and warns differently when the index does not record its source dialect at all); MySQL has no equivalent toggle.
- **`--restore-auto-increment` (MySQL sources only).** Appends an `AUTO_INCREMENT` restore checklist **after** `COMMIT` — one `SELECT` + `ALTER TABLE ... AUTO_INCREMENT = <N>` pair per table the reversal writes. The statements are emitted **commented out**: the correct `N` is not derivable from the index (the schema snapshot does not record which column is `AUTO_INCREMENT`, and the right value depends on whether you want to reuse the ids the reversal freed), so the block hands over an exact recipe instead of guessing. It sits after `COMMIT` because `ALTER TABLE` is DDL and implicitly commits in MySQL — inside the reversal transaction it would break that transaction's atomicity. No effect on a PostgreSQL index (`recover` warns if you pass it there, and warns differently when the index does not record its source dialect at all).

## Restore limitations (MySQL)

A `bintrail recover` script is applied as ordinary SQL by a normal client connection, so it inherits several effects that are easy to miss when treating "the script ran with no errors" as "the data is back exactly as it was":

- **Triggers re-fire on apply.** Restoring a `DELETE` via `INSERT`, or reverting an `UPDATE`, fires the target table's `AFTER INSERT`/`AFTER UPDATE` triggers just like any other statement — producing **new** side effects (audit rows, counters, denormalized columns). The trigger's original side effects were themselves row-logged, so *when the reversal's scope covers the tables those triggers write* they are reverted as their own separate events in the same script, and a table with side-effecting triggers can then have those effects double-applied. Under a filtered recover (`--table`/`--pk` — the normal invocation) those sibling events are not in the script, so the double-apply concern and the trigger-suppression remedy both shift: suppressing then leaves the derived tables holding the bad state, neither re-fired nor reverted. MySQL has no session-level toggle to suppress triggers during an apply (this is a fundamental limitation for MySQL, unlike PostgreSQL's `session_replication_role` — which `bintrail-pg recover --suppress-triggers` can pin for the reversal transaction, with the caveats documented above; there is no MySQL counterpart and `--suppress-triggers` is inert there).
- **`AUTO_INCREMENT` is not restored.** Reverting an `INSERT` deletes the row but does not decrement the table's `AUTO_INCREMENT` counter; reverting a `DELETE` re-inserts the row with its original (now possibly out-of-sequence) id, which can bump the counter further. The row data ends up identical to before, but the *next* auto-generated id may differ from what it would have been. If this matters, follow up with an explicit `ALTER TABLE ... AUTO_INCREMENT = N` once you know the correct value — `recover --restore-auto-increment` appends that step (commented out, one pair of statements per written table, after `COMMIT`) so you do not have to assemble it by hand; the script still never picks `N` for you.
- **`TRUNCATE TABLE` / `DROP TABLE` / `RENAME TABLE` (and MariaDB's `CREATE OR REPLACE TABLE`) cannot be undone by `recover`.** These emit no row-level binlog events (only an audit entry in `schema_changes`), so there is nothing for `recover` to reverse. The only path back is `bintrail baseline` + `bintrail reconstruct` to a point in time *before* the DDL — see [TRUNCATE / DROP / RENAME in the reconstruction window](#truncate--drop--rename-in-the-reconstruction-window) above, which is enforced (refuse, not silent) on every baseline-merging read path.
- **A crash-tail loss on the source is invisible to `recover` and `reconstruct` alike.** If the source runs with `sync_binlog` other than `1`, an OS crash can drop committed transactions from the binlog tail before DBTrail ever sees them — there is nothing in the index to recover because the data never reached the binlog. `bintrail doctor` warns when the source isn't `sync_binlog=1`; `bintrail verify` is the only way to later notice the gap (as a MISMATCH).
- **`reconstruct --at` and the shim's single-row `_flashback` / `_snapshot` cut at the transaction boundary, using the indexed GTID, not a raw per-statement timestamp** ([#783](https://github.com/dbtrail/dbtrail/issues/783), [#988](https://github.com/dbtrail/dbtrail/issues/988)). Two statements inside the same transaction can carry different `event_timestamp` values (down to the second); a naive per-row cut between them would half-apply the transaction — a state that never existed. Single-row `reconstruct`, `_flashback`, and `_snapshot` instead group row events by GTID (one binlog transaction is always a contiguous run of one GTID) and include or exclude each transaction as a whole: if any of its statements fall after `--at`, the entire transaction is dropped, never partially applied. (Single-row `_flashback` gained this in [#988](https://github.com/dbtrail/dbtrail/issues/988) — it now folds the PK's surviving events with `ApplyAt` instead of taking the latest event verbatim.) Two residual limits remain, both rooted in the index persisting `event_timestamp` as `DATETIME(0)` (one-second resolution) with no true commit-time column: (1) sub-second ordering *within* the surviving events cannot be resolved any better than before; (2) a transaction whose statements **all** execute before `--at` but which **commits** after it is still included whole — indistinguishable, at one-second resolution, from one that legitimately committed earlier. The **full-table** `AS OF` paths do not yet apply this transaction-boundary cut and still cut per row (tracked separately): `reconstruct --output-format mydumper`, and full-table `_flashback` / `_snapshot` over the shim.
- **There is no single command or flag that reports "history starts at `max(baseline, first captured event)`".** The oldest point you can reconstruct to is bounded by whichever is more recent: the oldest baseline snapshot you have, or the oldest event still in the index (live partitions plus any archives) — but nothing surfaces that combined floor directly. Check `bintrail status`'s earliest-indexed-event figure and your baseline listing (`--baseline-dir`) together to work it out for a given table.
