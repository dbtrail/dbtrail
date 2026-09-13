# Iceberg export

`bintrail export iceberg` writes the current state of your tables as Apache
Iceberg tables, and keeps them current by appending only what changed. DuckDB,
Spark, Trino, Athena and Snowflake read Iceberg, so this is the way to put
bintrail's data in front of a reporting engine without a full dump on a timer.

It is an output. bintrail's own storage does not change: the archives,
baselines and index stay what they are (see
[The archives are plain Parquet on purpose](parquet-debugging.md#the-archives-are-plain-parquet-on-purpose)),
and the export reads through them and writes somewhere new. Nothing on the
recovery path links the Iceberg library, and a test in the repository fails if
that ever changes.

## Quick start

You need a baseline snapshot of the tables you want to export (the same one
time-travel uses; see [Dump and Baseline](dump-and-baseline.md)) and the
index.

```bash
# First run: load every table of the newest snapshot, then fold the deltas
bintrail export iceberg \
  --index-dsn "user:pass@tcp(index:3306)/bintrail_index" \
  --baseline-dir /data/baselines \
  --warehouse /data/iceberg

# Read it back (the key column is selected on purpose; see "Reading the tables")
duckdb -c "INSTALL iceberg; LOAD iceberg;
           SELECT status, count(*), min(id) FROM iceberg_scan('/data/iceberg/shop/orders') GROUP BY 1;"
```

Run the same command line again whenever you want the tables brought forward.
From cron, hourly is a good default:

```
17 * * * * bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines --warehouse /data/iceberg
```

Each table lives at `<warehouse>/<schema>/<table>/` with Iceberg's `data/` and
`metadata/` directories; `metadata/version-hint.text` names the current
metadata file. DuckDB and Spark read such a directory directly. Trino, Athena
and Snowflake read Iceberg through a catalog, so with them you register the
table's current metadata file (or the directory, where the catalog supports a
Hadoop-style warehouse) rather than pointing at the path.

## Running it on a timetable

The public guide sends the reader here for this, because the website carries no
`bintrail` commands. Hourly from cron:

```
17 * * * * bintrail export iceberg --index-dsn "..." --baseline-dir /data/backups --warehouse /data/iceberg
```

Keep the password out of the crontab line: put it in a file only the job can read
and have the job read it from there. Where the bundled stack holds the index and
the backups you want exported, the compose one-shot avoids the question, since
the container reads the password from the stack:

```
17 * * * * cd /path/to/stack && docker compose --profile iceberg-export run --rm iceberg-export
```

The export is deliberately not a button and never runs inside the capture
daemon: it writes a whole new copy of your data, and keeping it out of that
process is what stops a long run slowing capture down.

Two runs cannot overlap on one warehouse; the second refuses while the first
holds it. A run that dies half way leaves the previous snapshot readable, and
the next one resumes from the last complete commit.

## What a run does

**The first run** for a table loads its newest baseline snapshot as the
table's first data files, and records the snapshot's binlog position (the
anchor the baseline itself carries) as the table's cursor. Then it continues
as below.

**Every run** fetches the events between the table's cursor and the run's
binlog cut, folds them to the net change per primary key, and commits that as
one Iceberg snapshot:

- an equality-delete file naming every key that was touched, and
- a data file with the current row of every touched key that still exists.

An update is therefore a delete of the old row plus an insert of the new one,
in the same commit. A key changed fifty times in the window costs one delete
row and one data row, not fifty. The table is never rewritten; compaction, if
you want it, is the reader's concern and any engine can do it.

The cursor moves to the run's cut in the same commit as the data. It is stored
in the table's own properties, visible with
`SELECT * FROM iceberg_table_properties('<table dir>')` as
`bintrail.export.binlog_file`, `bintrail.export.binlog_position` and
`bintrail.export.at`. Nothing is written to the index.

Because the commit is atomic, a run that dies after writing files and before
committing leaves the previous snapshot readable and the cursor where it was;
the next run resumes from it and the orphaned files are never referenced.

A window with no events for the table still moves the cursor to the run's
cut, in a commit that carries properties only: the next run then starts from
there instead of re-reading the same empty window. When the whole index holds
nothing past the cursor, the table is reported `unchanged` and nothing is
committed.

## What it refuses, and why

Refusals are per table: a refused table does not advance and is retried on the
next run, the other tables commit, and the exit status is non-zero when any
table did not end current. The vocabulary is the same as `baseline refresh`.

| verdict | when | what to do |
|---|---|---|
| `refused-gap` | the window spans events the index permanently lost, or hours rotated out without an archive | there is no flag for this; the missing events are missing. Take a fresh baseline and remove the table directory so the next run reloads from it |
| `refused-ddl` | the table changed shape since it was exported (a column added, dropped or retyped), or a TRUNCATE / DROP / RENAME sits in the window; on a first load, the baseline is older than the table's current schema | remove the table directory and let the next run reload it from a baseline taken after the change; on a first load, take a fresh baseline |
| `refused` | anything else; the detail line says what | read the detail line |

Four more shapes are refused rather than guessed at, all `refused-gap` or
`refused`:

- **`--at` at or before the table's cursor.** The export only moves forward;
  re-running an older instant is reported instead of quietly answering
  `unchanged`.
- **`--at` below the live window** (older than the oldest live partition).
  The cut is resolved on live events only, so a window under that floor cannot
  be bounded exactly. Export with a later `--at`.
- **The run's cut is before the cursor.** The source's binlogs were reset or
  the index was restored behind the export, so the events in between are not
  in this index. Remove the table directory to reload it from a fresh
  baseline. An index with **no live events at all** is refused too (plain
  `refused`: it cannot tell a rotated-out index from a reset one, so it
  names no cause) once a table has folded deltas; right after a first load
  it is simply reported `loaded` with a note,
  since a fresh install whose stream has not indexed anything yet has
  nothing to fold and nothing lost.
- **Skipped events.** When the capture daemon recorded skipped events (a
  column-count mismatch, for example) at or after the window's start, the
  index may not hold every change. The skip tally keeps one timestamp per
  reason, so a skip recorded after `--at` cannot be told apart from one
  inside the window and refuses too; fix the cause the skip names,
  re-snapshot, and reload the table.

These checks run on every invocation, including one where nothing new was
indexed: a TRUNCATE never lands in the event index, so a quiet window is not
proof of an unchanged table.

Two conditions refuse before anything is written:

- **No primary key**, or a key of type FLOAT, DOUBLE, TIME, BIT, JSON or a
  spatial type. Equality deletes name rows by key, so the export needs one it
  can compare.
- **BIT columns**. The baseline stores them as bytes and the row events store
  them as integers, and the export does not reconcile the two yet.

And one refuses the whole run: an index that holds **more than one source**
(more than one row in `bintrail_servers`). Nothing downstream of the archive
registry attributes an event to a source, so two sources with the same
`schema.table` would interleave in one Iceberg table.

## How the columns map

| MySQL | Iceberg | note |
|---|---|---|
| TINYINT, SMALLINT, MEDIUMINT, INT, YEAR | int | INT UNSIGNED becomes long |
| BIGINT | long | BIGINT UNSIGNED becomes decimal(20,0) |
| FLOAT, DOUBLE | float, double | |
| DECIMAL(p,s) | decimal(p,s) | above 38 digits, string |
| DATETIME, TIMESTAMP | timestamp (no zone) | the value the index stores: TIMESTAMP as UTC, DATETIME as written |
| DATE | date | |
| TIME | string | |
| CHAR, VARCHAR, TEXT family, ENUM, SET | string | ENUM and SET are exported as their labels |
| JSON | string | one rendering whichever run wrote the row: keys sorted, no whitespace, `<` `>` `&` and numbers as written. The first load parses the dump's text (MySQL's own rendering) and re-emits it the same way |
| BINARY, VARBINARY, BLOB family | binary | |
| BIT | refused | |

Zero dates (`0000-00-00`) become NULL, the same choice the baseline writer
makes. A fixed BINARY(n) primary key is exported without its storage padding,
so that the row events, which never carry it, match it.

## Reading the tables

DuckDB:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('/data/iceberg/shop/orders') LIMIT 10;
SELECT * FROM iceberg_snapshots('/data/iceberg/shop/orders');
```

Time travel over the export works with `snapshot_from_id` or
`snapshot_from_timestamp`, since every run is a snapshot.

Two things to know:

- **DuckDB 1.4** applies equality deletes only when the key columns survive
  projection pushdown, so an aggregate straight over `iceberg_scan` can fail
  with `Equality deletes need the relevant columns to be selected`. Select the
  key columns too (as the quick start does), or materialize first:
  `CREATE TABLE orders AS SELECT * FROM iceberg_scan('...')`. DuckDB 1.5 lifts
  this (checked with 1.5.5).
- **The table is not relocatable by default.** Iceberg metadata stores
  absolute paths. If you copy or sync the warehouse elsewhere (for example
  `bintrail upload` to S3), read it with
  `iceberg_scan('...', allow_moved_paths = true)`.

## Limits in this release

- The warehouse is a local directory. Writing directly to S3 is not supported
  yet; sync the directory and read with `allow_moved_paths`.
- No schema evolution yet. A column added or dropped in the source refuses the
  table (`refused-ddl`); remove the table directory to reload it from a fresh
  baseline. Iceberg tracks columns by id, which is what will make an ADD
  COLUMN a metadata change in a later release.
- Tables are unpartitioned.
- Built for MySQL and MariaDB sources; `bintrail-pg` does not have the
  command, and the export does not check that an index was written by the
  MySQL capturer.
- One writer at a time: the run holds a lock file at the warehouse root and a
  second concurrent run refuses.
- Memory: a run holds one entry per primary key touched in its window, like
  `baseline refresh`. Run it often enough that a window stays a fraction of the
  table.
- A TEXT column that holds a JSON document is rendered two ways. The index
  cannot tell a document in a TEXT column from a JSON column (MariaDB declares
  JSON as LONGTEXT), so a document arriving through a delta is re-encoded
  (keys sorted, no spaces) while the first load copies the dump's text as it
  is. Compare such a column as JSON, not as text. A MySQL `JSON` column reads
  one way on both paths.

## Where it runs

This is a one-shot command for your scheduler. It never runs inside
`bintrail-console watch`: that process is the capture plane, and nothing that
competes for its CPU or its S3 bandwidth belongs in it. The DuckDB budget
defaults to the conservative 2 threads and 4 GB; `--ultrafast` is available
here because the process is yours. The budget covers the baseline scan and
the archive reads of the event window.

The audit trail records `cli/export.iceberg` after a commit is durable, by
this rule: every first load is recorded, including one of a baseline with
zero rows (it creates the table and its cursor and has no snapshot to name,
so its event carries `rows: 0` and no `snapshot_id`); a delta is recorded
only when it changed rows. A window with no changes moves the cursor in a
properties-only commit and records nothing; a table with nothing past its
cursor commits nothing and records nothing. Both report `unchanged`.

## Flags

| flag | meaning |
|---|---|
| `--index-dsn` | the index (required) |
| `--baseline-dir` / `--baseline-s3` | where the baseline snapshots are (one required) |
| `--warehouse` | local directory the Iceberg tables live under (required; env `BINTRAIL_ICEBERG_WAREHOUSE`) |
| `--tables` | comma-separated `schema.table` list (default: every table in the newest snapshot) |
| `--at` | export up to this instant (default: now); must be after the table's cursor and not below the live window |
| `--fetch-batch-size` | event page size for the fold (0 = default) |
| `--format` | `text` or `json` |
| `--ultrafast`, `--duckdb-threads`, `--duckdb-memory-limit` | the DuckDB budget for the baseline scan and the archive reads |
