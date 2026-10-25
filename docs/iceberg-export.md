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
and Snowflake read Iceberg through a catalog: export with `--catalog glue` or
`--catalog rest` (see "Writing to AWS Glue or a REST catalog") and they find
the tables by name.

## Running it on a timetable

The public guide sends the reader here for this, because the website carries no
`bintrail` commands. There are three ways to run the export on a timetable:
with `--every`, as a compose service, or from cron. Each run adds only what
changed since the one before, whichever way it is started.

### With `--every`, no cron needed

Add `--every` and the command stays running. It exports, waits, and exports
again, until you stop it:

```
bintrail export iceberg --index-dsn "..." --baseline-dir /data/backups --warehouse /data/iceberg --every 1h
```

- **The wait starts when a run ends.** With `--every 1h`, a run that took ten
  minutes is followed by an hour of waiting, not fifty minutes. Runs never
  overlap, and a slow run is never followed by a burst of runs to catch up.
  The shortest interval is `1m`. The value needs a unit: `15m`, `1h`, `1h30m`.
  It is set on the command line only, never from an environment variable or
  the env file: there it would turn every export cron starts into one that
  never ends.
- **Mistakes in the settings stop it at start.** A bad interval, catalog
  setting or secret ends the command with an error before the first run, so
  you see it right away. So does a backup folder with no snapshot in it, when
  `--tables` does not name the tables.
  `--at` cannot be combined with `--every`: every run would stop at the same
  moment.
- **A failed run does not stop it.** If the index or the catalog does not
  answer, or a table is refused, the run prints one line with the error and the
  time of the next attempt, and the next run tries again from where each table
  last committed. The exit status stays 0.
- **Each run prints its own report.** In text, the usual summary, headed with
  the run's time. With `--format json`, one JSON document per run, on one line,
  so a log reader can take them line by line. Log lines go to stderr, the
  reports to stdout.
- **Stopping it is safe.** Ctrl-C or `SIGTERM` stops the wait at once. If a run
  is in progress it is canceled first, and every table keeps its last complete
  commit. The command then exits with status 0.
- **A new table in a newer snapshot is picked up** on the next run, the same as
  a scheduler starting the command again would, unless you named the tables
  with `--tables`.

### With docker compose

The stack's compose file has the same loop as a service that stays up:

```
docker compose --profile iceberg-export-loop up -d iceberg-export-loop
```

It exports every `ICEBERG_EVERY` (default `1h`; set it in `.env`), restarts
with Docker (`restart: unless-stopped`), and takes the same settings as the
one-shot `iceberg-export` service, catalog included (see
[docker.md](docker.md)). `docker compose logs iceberg-export-loop` shows each
run. `docker compose stop iceberg-export-loop` stops it the safe way described
above.

### From cron

Hourly from cron:

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

### How far behind the tables are

As far behind as the interval you schedule, plus the few seconds a run takes.
A run reads the changes captured since the previous run and nothing else, so
a shorter interval costs shorter runs, not more work. For tables that must be
at most five minutes behind, run it every five minutes:

```
*/5 * * * * cd /path/to/stack && docker compose --profile iceberg-export run --rm iceberg-export
```

Measured on v0.104.1, on one 8-vCPU machine with a 5-million-row TPC-C source
taking 20 transactions per second: twelve runs in a row, five minutes apart,
took 6 to 12 seconds each, none was refused, and when a run ended the tables
were 4 to 11 seconds behind the source. With the load paused, every table read
back through Spark matched the source: the same row count and the same total
of one numeric column. The warehouse grew
from 64 MB to 88 MB and by about 49 files per run across nine tables; each run
adds files and removes none (see "What a run does").

If the reader takes the tables from S3, add the time your sync takes: the
export writes a local directory (see "Limits in this release").

The export is deliberately not a button and never runs inside the capture
daemon: it writes a whole new copy of your data, and keeping it out of that
process is what stops a long run slowing capture down.

Two runs cannot overlap on one local warehouse; the second refuses while the
first holds it. With `--every`, the loop holds the warehouse only while a run
is in progress, so a run you start by hand between two of its runs goes
through. A run that dies half way leaves the previous snapshot readable, and
the next one resumes from the last complete commit.

## Writing to AWS Glue or a REST catalog

By default the tables live in a local directory (`--catalog hadoop`). Two other
catalogs register them where other engines find them by name:

- `--catalog glue` writes the files to S3 and registers each table in the AWS
  Glue Data Catalog. Athena, Spark on EMR or Glue, Trino and Snowflake read
  them from there.
- `--catalog rest` talks to any Iceberg REST catalog: Glue's own Iceberg
  endpoint, S3 Tables, Polaris, Nessie, Unity and others.

### AWS Glue

```bash
AWS_REGION=us-west-2 bintrail export iceberg \
  --index-dsn "user:pass@tcp(index:3306)/bintrail_index" \
  --baseline-dir /data/baselines \
  --catalog glue --warehouse s3://my-bucket/iceberg
```

- `--warehouse` is required and must be `s3://bucket` or `s3://bucket/prefix`.
  Doubled and trailing slashes are dropped. A local path, `gs://` or `s3a://`
  is refused.
- Each source schema becomes a Glue database of the same name, and each table
  is written under `<warehouse>/<schema>/<table>/`.
- Credentials and the region come from the usual AWS chain: environment
  variables, a profile, SSO, or the instance role. With no region the command
  stops and says so. Keep the bucket in the catalog's region.
- The identity needs, on Glue, the calls the export makes: `GetDatabase`,
  `CreateDatabase`, `GetTable`, `CreateTable` and `UpdateTable`; on the
  bucket, reading and writing objects under the prefix. This list comes from
  the code, not from a tested minimal policy.
- Glue keeps database and table names in lowercase letters, digits and
  underscores. bintrail does not rename anything, so a table such as
  `Shop.Orders` or `shop.line-items` is refused with the names that do not fit.
  Export it with `--catalog rest` or to a local directory instead.

### A REST catalog

Glue's Iceberg REST endpoint, signed with your AWS credentials:

```bash
bintrail export iceberg --index-dsn "..." --baseline-dir /data/baselines \
  --catalog rest \
  --catalog-uri https://glue.us-west-2.amazonaws.com/iceberg \
  --catalog-warehouse 123456789012 \
  --catalog-sigv4 \
  --warehouse s3://my-bucket/iceberg
```

- `--catalog-uri` is the catalog's base URL (required).
- `--catalog-warehouse` is the name the server knows the warehouse by: for Glue
  the AWS account id, for S3 Tables the table bucket's ARN, for other servers
  their catalog name.
- `--warehouse` is optional here. When you give it (any object-store location,
  such as `s3://` or `gs://`), each table is created under
  `<warehouse>/<schema>/<table>`. Without it the server picks the location.
- `--catalog-sigv4` signs every request with AWS credentials. The service and
  region come from the URI when it is a Glue or S3 Tables endpoint
  (`glue.<region>.amazonaws.com` signs for `glue`, `s3tables.<region>...` for
  `s3tables`). For any other host the service is `execute-api`, what a catalog
  behind API Gateway expects, and the region is the AWS chain's.
  `--catalog-sigv4-region` and `--catalog-sigv4-service` override both.
- Names are passed through as they are; the server's own rules apply.
- A credential or token is only sent over `https://`, or over `http://` to
  this machine (`localhost`, `127.0.0.1`, `::1`); any other `http://` address
  is refused.

Glue's endpoint and Glue's API register the same tables, so a table written
with one is read with the other. The S3 Tables endpoint is wired the same way
but was not part of this release's tests.

### Secrets

A command line is visible to every user on the machine (`ps`) and lands in
shell history, so no flag takes a secret. A REST catalog that wants an OAuth
client credential or a bearer token gets it one of two ways:

- an environment variable: `BINTRAIL_ICEBERG_CATALOG_CREDENTIAL`
  (`client_id:client_secret`) or `BINTRAIL_ICEBERG_CATALOG_TOKEN`. The env file
  `bintrail` already reads (`.bintrail.env` or `~/.config/bintrail/config.env`)
  can set them;
- a file only the job can read: `--catalog-credential-file` or
  `--catalog-token-file`.

Only one way to sign in is accepted per run: SigV4, a credential, or a token.
Two at once, or the same secret from both the variable and the file, is
refused. A `--catalog-uri` with a user name or password in it
(`https://user:pass@host`), or with a query string, is refused too, and
messages and reports print the URI without either.

### Two runs at once, and tables that are not bintrail's

The local catalog allows one run at a time (a lock file). Glue and REST do not
need the lock: every commit is checked against the table state the run started
from. If another run (or another engine) committed to the table in between,
this run's commit is not applied and the table reports `refused-conflict`. The
next run continues from the cursor the other run committed, so nothing is
folded twice and nothing is skipped. The same happens if an engine rewrites the
table (compaction) during the run. A commit whose outcome the catalog cannot
report (a server error or timeout) is refused with that said; the next run
reads whichever cursor the catalog holds.

A table that already exists in the catalog without the property
`bintrail.export.version` was not created by bintrail. It is refused, never
loaded into, on every catalog. Drop it or export elsewhere.

A catalog is shared by everyone in the account, so two bintrail installs that
read different source servers could both export a `shop.orders`. Each new
table records where it came from: the source server
(`bintrail.export.source_id`, the source's `bintrail_id`) and the exact source
table name (`bintrail.export.source_table`, case kept). With Glue or REST:

- a run whose index holds a different source is refused for that table,
  instead of folding its changes onto the other server's rows;
- a table missing either property is refused (no earlier release wrote to
  these catalogs, so every table there should have both);
- the index must register its source (`bintrail_servers`, filled by
  `bintrail stream`). An index built from binlog files with `bintrail index`
  registers none, and the export to Glue or REST refuses before writing.

On every catalog, the local one included, a table whose recorded source name
differs from the one being exported (`Shop.orders` recorded, `shop.orders`
exported) is refused, naming both: a catalog that ignores case, or a
case-insensitive disk, would otherwise put the two in one table. Local tables
written by earlier releases carry no name and are accepted as before.

If the catalog reports an error for a commit that in fact went through (a
server error or a timeout after it applied the change), the run reads the
table back, finds its own cursor there, and reports the commit as done.

To reload a table from a fresh baseline with Glue or REST, drop it from the
catalog (`DROP TABLE` in Athena, Spark or Trino). The refusals that tell a
local user to remove the table directory say this instead.

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
| `refused-ddl` | the table changed shape since it was exported (a column added, dropped or retyped), or a TRUNCATE / DROP / RENAME (or MariaDB's CREATE OR REPLACE TABLE) sits in the window; on a first load, the baseline is older than the table's current schema | remove the table directory and let the next run reload it from a baseline taken after the change; on a first load, take a fresh baseline |
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
- **DuckDB gets slower on a table that is exported often.** Every run adds
  one equality-delete file per table, and DuckDB's cost of applying them grows
  with each one: with DuckDB 1.5.5 a 300,000-row table exported every five
  minutes went from 3 seconds to read to 200 seconds after twelve runs, about
  16 seconds more per run, while Spark read the same table in under a second
  ([#2372](https://github.com/dbtrail/dbtrail/issues/2372)). Read a frequently-exported table with Spark or another engine, or
  keep DuckDB for a table exported a few times a day.
- **Athena** reads a Glue table by name:
  `SELECT * FROM "shop"."orders"`. Athena's `"orders$files"` lists data files
  only, so the delete files do not show there; `"orders$snapshots"` shows them
  per commit (`added-delete-files`, `added-equality-deletes` in `summary`).
  Checked with Athena engine version 3: every delta reads correctly.
- **Spark, Trino and Snowflake** read Iceberg tables from Glue through their
  own Glue or REST catalog support (see each engine's Iceberg documentation).
  This release was checked with Athena only.
- **The table is not relocatable by default.** Iceberg metadata stores
  absolute paths. If you copy or sync the warehouse elsewhere (for example
  `bintrail upload` to S3), read it with
  `iceberg_scan('...', allow_moved_paths = true)`.

## Limits in this release

- With the local catalog the warehouse is a local directory, and a
  `--warehouse` that is a URL (`s3://`, `gs://`, `file://`, any `scheme://`) is
  refused: that catalog commits through a file that races on object storage.
  To write to S3, use `--catalog glue` or `--catalog rest`. A local warehouse
  copied to S3 cannot be registered in a catalog later, because its metadata
  holds absolute local paths; export to S3 from the start.
- No schema evolution yet. A column added or dropped in the source refuses the
  table (`refused-ddl`); remove the table directory (with Glue or REST, drop
  the table) to reload it from a fresh baseline. Iceberg tracks columns by id, which is what will make an ADD
  COLUMN a metadata change in a later release.
- Tables are unpartitioned.
- Two source schemas or tables whose names differ only in case
  (`Shop.orders` and `shop.orders`) are refused, both of them, on every
  catalog.
- Built for MySQL and MariaDB sources; `bintrail-pg` does not have the
  command, and the export does not check that an index was written by the
  MySQL capturer.
- With the local catalog, one writer at a time: the run holds a lock file at
  the warehouse root and a second concurrent run refuses. With Glue or REST,
  concurrent runs are allowed and the one that commits second is refused for
  that table (see "Two runs at once").
- Scheduling is still yours (cron, a systemd timer, a Kubernetes CronJob).
- Column names are written as the source spells them. Athena compares
  column names without case, so two columns that differ only in case cannot
  both be read there.
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
| `--warehouse` | where the tables live: a local directory for `hadoop` (required), `s3://bucket/prefix` for `glue` (required), an optional location prefix for `rest`; env `BINTRAIL_ICEBERG_WAREHOUSE` |
| `--catalog` | `hadoop` (default, a local directory), `glue` or `rest` |
| `--catalog-uri` | REST catalog base URL (`rest`, required) |
| `--catalog-warehouse` | the warehouse name the REST server expects: account id, S3 Tables ARN, catalog name (`rest`) |
| `--catalog-sigv4` | sign REST requests with AWS credentials (`rest`) |
| `--catalog-sigv4-region`, `--catalog-sigv4-service` | override the signing region and service (default: from a Glue or S3 Tables URI, else the AWS chain and `execute-api`) |
| `--catalog-credential-file`, `--catalog-token-file` | file holding the REST OAuth client credential or bearer token; or env `BINTRAIL_ICEBERG_CATALOG_CREDENTIAL`, `BINTRAIL_ICEBERG_CATALOG_TOKEN` |
| `--tables` | comma-separated `schema.table` list (default: every table in the newest snapshot) |
| `--at` | export up to this instant (default: now); must be after the table's cursor and not below the live window |
| `--fetch-batch-size` | event page size for the fold (0 = default) |
| `--format` | `text` or `json` (with `--every`, one JSON document per run, one per line) |
| `--every` | stay running and export again this long after each run ends (at least `1m`; empty = run once); command line only, no environment variable |
| `--ultrafast`, `--duckdb-threads`, `--duckdb-memory-limit` | the DuckDB budget for the baseline scan and the archive reads |
