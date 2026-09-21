# Verifying a recovery (`bintrail verify`)

`bintrail verify` checks that reconstructing a table — merging a baseline
snapshot with the indexed binlog deltas on top of it — reproduces the same
row content as a reference read from your database (a baseline taken from a
dump, or the live source) at the specific anchor points being compared: see
[Baseline-anchored](#baseline-anchored-default-drift-free). It answers a narrower
question than "would my recoveries work": *at these anchor points, for the
tables and columns compared, does the full-table reconstruction/`_snapshot`
merge agree with the reference?*

It is read-only and never writes to your source or your index.

## What a MATCH does and does not prove

- **The comparison is a 64-bit, additive, non-cryptographic multiset
  fingerprint** over each table's rows — not a cryptographic hash. An
  accidental collision (two different row sets producing the same digest) has
  roughly a 2⁻⁶⁴ chance, and the digest is trivially forgeable by anyone able
  to write rows to the table being compared. A MATCH against a reference read
  from your database is strong evidence the capture-and-reconstruct pipeline
  works; it is not tamper-evident or
  forensic-grade proof.
- **The default (content) check does not exercise the `recover` path.** With
  `--check content` — the default — `verify` reconstructs full-table state from
  the *latest* event per PK (`row_after`, `LimitPerPK=1`). `recover`'s reversal
  SQL additionally depends on `row_before` images, DELETE row images, and
  intermediate events later superseded by a newer event on the same PK — none
  of which a table-content match touches. A corrupt `row_before`, a corrupt
  DELETE image, or a corrupt superseded intermediate event can all pass a
  content check cleanly while still producing a wrong `recover` reversal from
  that same data.

  **`--check recover` closes exactly that hole** — see
  [Recover-input](#recover-input---check-recover) below. It is a *separate
  run*: a clean `--check content` still proves nothing about `recover`'s
  inputs, and a clean `--check recover` proves nothing about full-table
  content. Run both to cover both.
- **`--check recover` proves internal consistency, not external truth.** It
  asserts the stored images agree *with each other* along each primary key's
  chain. It has no independent ground truth, so a chain that was captured
  self-consistently but wrongly (e.g. every image for a row derived from the
  same bad source read) still passes. It also cannot prove the reversal SQL
  *applies* to today's schema — a column dropped after capture makes `recover`
  refuse at generation time, which is a schema-drift concern, not an image one.
- **It does not exercise point-in-time reconstruction to an arbitrary instant
  between two anchors.** Baseline-anchored mode compares two baseline anchors
  (or a baseline vs. the live source at scan time); there is no independent
  ground truth for an arbitrary `--at`/`AS OF` instant in between, so nothing
  checks one.
- **It does not cover the restored *schema*.** `CREATE TABLE` definitions,
  indexes, `AUTO_INCREMENT` state, triggers, and collations are outside the
  row-content digest.
- **In default (baseline-anchored) mode, a table with no baseline yet is out
  of scope** — the same tables `reconstruct` also cannot materialize.
  `verify` reports these `inconclusive`, never as proof of correctness.
- **Live-source mode requires a quiescent source.** Any commit against the
  table during the scan reads as drift and reports as MISMATCH, even when the
  reconstruction itself is correct — the live table simply kept moving during
  the read.

## The modes

`--check` selects **what** is verified; within the default `--check content`,
passing or omitting `--source-dsn` selects **what it is compared against**.

| | Reads | Answers |
|---|---|---|
| `--check content` (default), no `--source-dsn` | two baselines + index | does the reconstruction match the table's last read of the database? |
| `--check content` + `--source-dsn` | baseline + index + live source | does the reconstruction match the live table? |
| `--check recover` | index only | are the before/after images `recover` consumes internally consistent? |

### Baseline-anchored (default, drift-free)

Omit `--source-dsn`. For each table, `verify` takes the **last baseline that
read it from your database** (a dump: `bintrail baseline`, or a scheduled run
that took a full backup) and the baseline just before that one. It reconstructs
the older one *forward*, applying indexed binlog events up to the read's exact
binlog anchor, and fingerprints the result against what the database held at
that read.

Both sides are at-rest data (Parquet snapshots), so this mode reads **no live
source** and has **no production impact**. Run it any time after a baseline — for
example right after `bintrail baseline`, or on a schedule (cron/CI). Because
neither side is the live table, it can't be fooled by drift that happened on the
source after capture.

**Why the last read and not the two newest baselines.** A baseline that a
refresh built from the recorded changes never read the database, so it is not an
independent reference. The read is. The check replays the recorded changes onto
the older baseline, from that baseline's own starting point (where its chain
started, when a refresh kept the table's changes beside its file), up to the read's
anchor. A match therefore says the capture between the two is complete. It does
not open the change files a refresh keeps beside a table (table deltas), and it
covers the chain up to the last read, not the refreshes after it. A schedule
with a full backup every so often (for example every 7 days) gives this check a
new read to test each time. The JSON report names the read each table was
compared against (`compared_to`).

A table is reported `inconclusive` instead of compared when:

- the baseline that last read it is no longer kept;
- its last read is the oldest baseline that holds it, so there is nothing before
  it to compare with;
- when the database was last read for it is not on record (a baseline that a
  refresh built before DBTrail recorded that).

The next full backup makes such a table checkable. A run where no table was
proven exits non-zero. The window between the two baselines can be days old, so
the events may come from the Parquet archives rather than the live index; with
`--no-archive`, or after rotation dropped them unarchived, the table is
`inconclusive` with the gap as the reason.

A baseline folder that cannot be read at or after the oldest baseline any table
is compared with (or older than a read with no earlier baseline, which that
folder may hold) refuses the whole run and names the folder, whatever
`--tables` selects: fix its permissions first.

```sh
# All tables, baselines on local disk
bintrail verify --index-dsn "$IDX" --baseline-dir /data/baselines

# With a row-level drill-down on any mismatch
bintrail verify --index-dsn "$IDX" --baseline-dir /data/baselines --explain
```

### Live-source

Pass `--source-dsn`. For each table, `verify` reconstructs a consistent
point-in-time snapshot and compares it against the **live source** table. This
reads the whole table off the live server, so **run it off-peak**.

```sh
bintrail verify --source-dsn "$SRC" --index-dsn "$IDX" \
  --baseline-s3 s3://bucket/baselines --tables mydb.orders,mydb.users
```

For a PostgreSQL source the same command works unchanged — pass the
`postgres://` DSN as `--source-dsn`; the index's recorded source flavor routes
the fingerprint. See [Notes per source](#notes-per-source) for how the PG
fingerprint and its coverage check differ under the hood.

### Recover-input (`--check recover`)

Pass `--check recover`. Instead of comparing table content, `verify` walks each
primary key's **event chain** in time order and asserts the images are
internally consistent — the data `bintrail recover` actually reads to build
reversal SQL:

| `recover` reverses | by reading |
|---|---|
| `DELETE` → `INSERT` | `row_before` (the deleted row) |
| `UPDATE` → reverse `UPDATE` | `row_before` (the `SET`) **and** `row_after` (the `WHERE`) |
| `INSERT` → `DELETE` | `row_after` (the `WHERE`) |

So per event it checks that every image `recover` dereferences is present, that
no unchanged-TOAST marker survived capture, and — the core assertion — that
each `UPDATE`/`DELETE` **`row_before` equals the state the previous event on
that same key left behind**. Events superseded by a newer event on the same key
are walked too; that is precisely the class `--check content` skips.

It reads **the index only** — no baseline, no live source — so
`--baseline-dir`/`--baseline-s3` are not required, and `--source-dsn` and
`--explain` (a content-mode drill-down it could never print) are rejected.

```sh
# Walk the last 7 days of chains for every table in the schema snapshot
bintrail verify --index-dsn "$IDX" --check recover --lookback 7d

# One table, machine-readable, for a CI gate
bintrail verify --index-dsn "$IDX" --check recover \
  --tables mydb.orders --format json
```

**A window that begins mid-history is `inconclusive`, never `mismatch`.** The
first event on a key inside `--lookback` usually has no predecessor in the
window (the row already existed), so there is nothing to assert against it —
that is the normal case for any retention window, not corruption. Later events
on the same key *are* asserted, so a table is still proven as long as at least
one comparison was made; a table where *none* was reports `inconclusive`.

Three other conditions also report `inconclusive` rather than risk a false alarm:

- a **coverage gap** in the window (a chain with an interior hole cannot be
  asserted against);
- a **recorded permanent loss** inside the window — `bintrail status` shows it
  as `GAP LOST`, and it is stamped in `stream_state.gap_lost_at`. Those events
  are gone from live MySQL *and* from every archive, so the chains that cross
  the loss have a hole nothing can be asserted across. An index whose
  continuity record cannot be read at all (one predating those columns) reports
  `inconclusive` for the same reason: *unknown* is not *no gap*;
- a window that **exceeded `--max-events`** — only its oldest events were
  walked, so a clean result would be a partial check.

A mismatch found inside a truncated window *is* still conclusive: the events
that were walked are real.

**A chain break tells you the chain broke, not why.** A `mismatch` here means
`row_before` did not match the state the previous event on that key left. Two
explanations produce byte-identical evidence, and the check cannot choose
between them:

1. **The events in between were never captured.** Coverage detection is
   *partition-existence* based, so a hole *inside* an hour whose partition
   exists is invisible to it — an `ALTER` without a re-`snapshot` (the indexer
   logs a column-count warning and *skips that table's events*), a mid-history
   `--tables`/`--schemas` filter change, a `stream --reset`, or an outage
   shorter than the pre-created partition horizon.
2. **The stored before-image is stale or corrupt.**

To tell them apart, check `bintrail status` continuity and the indexer/stream
logs for skipped tables, filter changes, resets or downtime covering the
window, and compare against the source's own binlog history. Do not start from
the assumption that the images are corrupt — a capture hole is at least as
likely.

**Partial coverage annotates a verdict, it does not erase it.** A table is
proven as soon as *one* before-image comparison was conclusive; everything that
stayed unproven is carried as a note on that verdict (visible in `reason`)
rather than collapsing the table:

| Not checked | Effect |
|---|---|
| chains beginning mid-history | noted |
| a value whose representation could not be normalized (an unmapped `ENUM` ordinal, a JSON document the event image cannot render faithfully) | noted |
| **drift rows** — events the index holds with no primary key (`pk_values` NULL) | noted; they belong to no chain and are never walked |
| the entire tail of the window (`--max-events` exceeded) | **collapses** the table to `inconclusive` |

Only a table where *nothing* was conclusive reports `inconclusive`. This
matters for CI: one unresolvable JSON value in a 200,000-event window used to
discard every clean assertion beside it and fail the gate on a healthy index.

> **The `30d` default assumes your window is actually covered.** If partitions
> older than your retention were rotated out of MySQL and never archived to
> Parquet, a 30-day lookback covers hours that no longer exist, and *every*
> table reports `inconclusive` with a coverage-gap reason. That is correct
> behavior, not a bug — but the fix is to **shorten `--lookback`** to your real
> retention (or enable archiving), not to ignore the result. Since an
> all-inconclusive run exits non-zero, a cron gate will tell you immediately.
> An index **younger** than the lookback is called out explicitly: the reason
> says *nothing rotated away; the index did not exist yet*, and the remedy is
> the same: shorten `--lookback` to the index's age or less.

Memory bound: the walk loads at most `--max-events` events per table (default
200,000) plus one row of state per distinct primary key seen. The cap is a
**row count, not a byte budget** — a BLOB/TEXT-heavy table is still large at a
given event count, so lower `--max-events` or narrow `--lookback` for those.

## What it reports

Results are **per table**, one of:

- **match** — the reconstruction reproduces the comparison exactly.
- **mismatch** — they differ. The chain would not reproduce this table; investigate.
- **inconclusive** — verify could not prove the table either way, and this is
  **never reported as a failure**. Causes include: no predecessor baseline yet,
  the index is behind the baseline anchor, an unsupported primary key, a coverage
  gap, a recorded permanent loss (`gap_lost_at`) inside the window, a
  digest-contract skew between a pre-pin baseline and the current scan
  (regenerate the baseline — see below), or a value class this version cannot yet
  compare.

  Under `--check recover`, inconclusive is subdivided by `inconclusive_kind`
  so a summary can be read: `no-activity` (nothing changed in the window),
  `nothing-to-assert` (every chain is a single INSERT — an append-only shape
  where zero assertions is the expected outcome, in every window), and
  `unproven` (there was content the walk could not prove: chains beginning
  mid-history, unresolvable values, or a truncated window). The first two are
  benign and counted in `summary.inconclusive_nothing_to_check`; the remainder
  — including an inconclusive with **no** kind, from a content mode or an
  older producer — is the slice worth a look. The exit contract is unchanged:
  an all-inconclusive run still exits non-zero, whatever the kinds.

## Machine-readable output (`--format json`)

`--format json` (default `text`) emits the whole run as one JSON document, so a
scheduled consumer can tell **which** table diverged — and whether the run was
all-inconclusive — instead of scraping the text columns. The exit code is
identical in both formats.

```sh
bintrail verify --index-dsn "$IDX" --baseline-dir /data/baselines --format json
```

```json
{
  "mode": "baseline-anchored",
  "baseline_source": "/data/baselines",
  "verdict": "mismatch",
  "tables": [
    {
      "schema": "mydb",
      "table": "orders",
      "status": "mismatch",
      "source_rows": 1042,
      "reconstruct_rows": 1041,
      "source_digest": "v2:…",
      "reconstruct_digest": "v2:…",
      "anchor": "binlog.000007:4711",
      "reason": "content digest differs"
    }
  ],
  "summary": { "match": 8, "mismatch": 1, "inconclusive": 2,
               "inconclusive_nothing_to_check": 1, "error": 0, "total": 11 }
}
```

- `mode` — `baseline-anchored`, `live-source`, or `recover-inputs`.
- `verdict` — the run outcome, matching the exit code: `verified` (exit 0),
  `mismatch`, `error`, `unproven` (tables reported, none proven — exit non-zero),
  or `no_predecessor` (only one baseline; reported, exit 0, with a `message`).
- `tables[].status` — `match` / `mismatch` / `inconclusive` / `error`, the same
  bucket counted in `summary`. `anchor` is the point the comparison was anchored
  to (a GTID set in MySQL live-source mode, a `file:pos` binlog coordinate in
  MySQL baseline-anchored mode, an `LSN:` WAL position for a PostgreSQL
  source); `reason` is the detail behind the verdict.
- `tables[].inconclusive_kind` — only on `status: "inconclusive"` rows and
  only under `--check recover` (omitted otherwise, mirroring the counters
  below): `no-activity`, `nothing-to-assert`, or `unproven` — see the
  taxonomy above. `summary.inconclusive_nothing_to_check` is always present
  and counts the benign kinds; it is a subdivision of `summary.inconclusive`,
  not a fifth bucket.
- `tables[].events_checked` / `chains_checked` / `chains_inconclusive` —
  present only under `--check recover` (omitted entirely otherwise, so a
  content-mode document is unchanged): how many events the chain walk visited,
  how many distinct primary keys it walked, and how many of those held at
  least one event with no predecessor state to assert against — the chain
  began mid-window, a PK-changing `UPDATE` moved the row off the key, or the
  chain was restarted after a nil-image or unresolved-TOAST finding (the state
  after such an event is unknowable). The row-count and digest
  fields stay absent in this mode — it compares no table content.
- `explain[]` — present only with `--explain`: per mismatched table, the capped
  list of differing rows (`pk`, `kind`, and per-column `recovery` vs `baseline`
  values), `total_differing_rows`, `overflow_by_kind` for the rows beyond the
  cap, and `deferred_type_note`. A drill-down that could not be produced appears
  as an `unavailable` string instead of failing the run.

Errors follow the CLI-wide convention: with `--format json`, the message is
written to **stderr** as `{"error":"…"}`.

The report carries **no stream-continuity signal** — `verify` never reads
`stream_state`, and reporting a continuity verdict it did not check would be
false assurance. That signal is `continuity.status` in
[`bintrail status --format json`](rotation-and-status.md#stream-continuity-no-data-lost);
a thorough cron gate runs both.

## Exit codes (for cron / CI)

- **Non-zero** on any **mismatch** or error, **or** when comparable tables
  existed but none could be proven (all inconclusive). An `inconclusive` table
  never *by itself* fails the run — but a run where *no* table could be proven does.
- **Zero** when a source has only one baseline (no predecessor to compare yet) —
  this is reported, not failed.
- **Non-zero** when *no* baselines are found at all — that's a misconfiguration
  (wrong `--baseline-dir`/`--baseline-s3`), not a "nothing to do."

This makes `bintrail verify` safe to wire straight into a pipeline: a clean exit
means "nothing disproved the recovery chain."

## `--explain` — row-level drill-down

In baseline-anchored mode, add `--explain` to print, below the per-table report,
exactly which rows diverged on a mismatch: the differing primary keys and, for
changed rows, the differing columns with the reconstructed value vs the new
baseline's. It re-runs the same reconstruction the verdict came from (byte-
identical by construction) — no live source, scratch database, or external
diff tool involved.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--index-dsn` | *(required)* | DSN for the index MySQL database |
| `--source-dsn` | *(empty)* | Live source DSN (MySQL DSN, or `postgres://` for a PostgreSQL source). Pass it for **live-source** mode; omit for **baseline-anchored** mode |
| `--baseline-dir` | *(empty)* | Local directory of baseline Parquet snapshots |
| `--baseline-s3` | *(empty)* | S3 URL prefix of baseline snapshots (e.g. `s3://bucket/baselines/`) |
| `--tables` | *(all)* | Comma-separated `schema.table` list (default: all tables in the latest schema snapshot; in baseline-anchored mode, snapshot tables with no baseline report `inconclusive` — "never baselined") |
| `--no-archive` | `false` | Query live MySQL partitions only; skip Parquet archive discovery |
| `--explain` | `false` | On a baseline-anchored mismatch, print a per-row drill-down. Rejected under `--check recover` |
| `--format` | `text` | Output format: `text` or `json` (see [Machine-readable output](#machine-readable-output---format-json)) |
| `--check` | `content` | What to verify: `content` (reconstructed table content) or `recover` (`recover`'s before/after image inputs, index-only) |
| `--lookback` | `30d` | `--check recover` only: how far back to walk each key's event chain (e.g. `30d`, `24h`). Rejected under `--check content` |
| `--max-events` | `200000` | `--check recover` only: per-table cap on events loaded; exceeding it reports `inconclusive` rather than a partial check. Rejected under `--check content` |

One of `--baseline-dir` or `--baseline-s3` is **required** for `--check content`
(both of its modes read baselines). `--check recover` reads the index only, so
it requires neither — and rejects `--source-dsn` and `--explain`, which it
would not honour. Symmetrically, `--check content` rejects `--lookback` and
`--max-events`, which only the recover-input walk reads: a flag that is
accepted must be a flag that was honoured.

It also accepts the shared DuckDB tuning flags (`--ultrafast`,
`--duckdb-threads`, `--duckdb-memory-limit`) — see the DuckDB resource tuning
section in [Query & Recovery](query-and-recovery.md).

## How it relates to `recover` and `reconstruct`

- **`recover`** undoes *specific touched rows* from stored before/after images
  (delta-only).
- **`reconstruct`** materializes a *full table or row* at a point in time
  (baseline + deltas).
- **`verify`** doesn't recover anything — it *checks* that these are sound, so
  you find out your recoveries are trustworthy **before** you need them.
  `--check content` checks the `reconstruct` chain (baseline + deltas → full
  state); `--check recover` checks the images `recover` itself consumes
  (`row_before`, DELETE pre-images, superseded events). They cover different
  data and neither implies the other.

See also: the stream-continuity "no data lost" signal in
[`bintrail status`](rotation-and-status.md#stream-continuity-no-data-lost), which
verifies the *other* half — that no events were dropped from the capture stream.

## Charset contract: raw stored bytes

This section (including the digest version tag below) describes the **MySQL**
rendering contract. A PostgreSQL source has no charset-transcoding layer in
this path — its rendering contract is the pinned session GUCs described in
[Notes per source](#notes-per-source); the digests still carry the same
version tag, and comparisons are always within one source, never across.

The row-content fingerprint is computed over the bytes MySQL returns for each
column. To make that byte stream independent of the connection charset, the
checksum scan pins `character_set_results = binary`, so the server returns each
string column's **raw stored bytes** with no transcoding — the same contract
mydumper's `SET NAMES binary` writes the baseline Parquet under. A `latin1` `é`
(byte `0xE9`) therefore hashes as `0xE9` on both the live-scan side and the
baseline side, instead of the server transcoding it to utf8mb4 (`0xC3 0xA9`) on
one side only. Without this pin, every non-ASCII row of a legacy-charset
(`latin1`, `sjis`, …) table would differ in bytes even under a byte-correct
restore — a permanent, conclusive false **MISMATCH**. The pin removes the
charset dependency by construction.

### Digest version tag

Each digest carries a leading version tag (currently `v2:`) recording the
contract it was computed under — the Go-side encoding plus the MySQL-side
rendering (text protocol, session time zone UTC, and
`character_set_results = binary`). Pinning the binary charset bumped the tag
from `v1:` to `v2:`. Two digests are only byte-comparable when their tags match;
a **version skew** — e.g. a persisted `v1:` baseline digest written before the
pin, compared against a current `v2:` scan — is reported as **inconclusive**
with a "regenerate the baseline" hint, never as a false MISMATCH. Re-run
`bintrail baseline` to refresh the tag: the raw-byte content is unchanged, only
the contract tag differs.

## Notes per source

`verify` runs against the MySQL index, so every mode works wherever its data
sources exist:

- **MySQL** — all three modes. Live-source mode anchors on `@@gtid_executed`
  and refuses to compare when the index is provably behind the snapshot; on a
  `gtid_mode=OFF` source it proceeds with a `coverage unverified` note.
- **MariaDB** — baseline-anchored and recover-input as MySQL. Live-source
  proceeds with the same `coverage unverified` note (MariaDB has no
  `@@gtid_executed` to prove containment against).
- **PostgreSQL** — baseline-anchored verify works against the PG capture
  (`bintrail-pg stream` / the console's PostgreSQL servers), anchored on WAL
  LSNs instead of binlog coordinates; recover-input works too (index-only —
  it carries no anchor). Live-source mode is supported
  too: the source is fingerprinted inside one `REPEATABLE READ` snapshot,
  read in PostgreSQL's own text rendering under the same pinned session GUCs
  the capture and baseline run under (`TimeZone=UTC`, `DateStyle=ISO`,
  `extra_float_digits=3`, `bytea_output=hex`, `IntervalStyle=postgres`), and
  anchored on `pg_current_wal_lsn()`. Its coverage check differs from MySQL
  by necessity: an index checkpoint at-or-past the anchor **proves** the
  index absorbed everything the snapshot reflects; a checkpoint behind it
  proves nothing (WAL from other databases and non-transactional activity —
  autovacuum, checkpoints — advances the anchor without ever producing
  indexable events), so the run proceeds and the result carries a
  `coverage unverified` note — the same documented trade-off as a GTID-off
  MySQL source, where a genuinely-behind index surfaces as an investigable
  mismatch rather than being masked. A permanent capture loss (`gap_lost`)
  stamped **inside the comparison window** reports `inconclusive`, never a
  false mismatch; a loss older than the baseline snapshot is outside the
  window — the baseline is a fresh dump of the source, so it re-covers
  whatever the gap lost — and does not degrade the verdict. The
  quiescent-source requirement applies unchanged.
