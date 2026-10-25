# Analytics with DuckDB

DBTrail keeps an analytical copy of your MySQL tables as plain Parquet, on disk
or in S3, a few minutes behind the source. You query it with DuckDB, or with
any engine that reads Parquet. The primary never sees those queries.

This page is the way in. It links to the reference pages for each step.

```
 your MySQL ──binlog──▶ bintrail ──▶ Parquet in your bucket ──▶ DuckDB · Athena · Spark · Trino
 (nothing installed)    (one daemon)  (snapshots + history)      (your process, your machine)
```

## Why a separate copy

Row stores answer "what is this order now" fast. Reports ask a different kind
of question: revenue by month over three years, churn after a price change.
Asked on the primary, those queries pay in the currency your application needs:

- a full scan pushes the working set out of the buffer pool,
- a long read holds back purge,
- a big `GROUP BY` spills temporary tables to disk.

A read replica keeps them off the primary. But it is one more MySQL to pay for
and patch, it answers at MySQL speed, and it only knows the present.

DBTrail's copy is files. Nothing to patch, columnar and compressed, and it keeps
every version of every row next to the current state.

## How the copy stays current

1. **One full copy.** The first snapshot is a mydumper dump converted to
   Parquet, one file per table. This is the only time the source's tables are
   read in full. It is point-consistent by default, and the lock modes and the
   privileges they need are in
   [Dump & Baseline](dump-and-baseline.md#cross-table-consistency).
2. **Then only the binary log.** DBTrail connects the way a replica does and
   records every row change, before and after, in its index. Nothing is
   installed on the database host, which is why RDS and Aurora work.
3. **Folds, on your schedule.** Each scheduled run takes the newest snapshot,
   applies the changes recorded since, and publishes the result as a new
   snapshot. It never connects to the source. A table that did not change is
   carried forward as it is. A table that changed keeps its file, and the run
   writes only the rows it changed beside it (see [Table deltas](#table-deltas)).

The schedule is set per server on the **Snapshots** page of the web interface.
It can be as often as every 5 minutes or as rare as once a day. A new server
that keeps its snapshots on this machine only gets every 5 minutes once its
first snapshot is taken; one with an S3 location gets none, since its copies
there are never removed automatically. The interval is
the ceiling on how fresh a report can be: at `5m` a dashboard sees this
morning; at `1d` it sees yesterday. The SQL port answers from the same
snapshots by default. Its fresh mode (chosen per server on the Snapshots
page, or `SQL_FRESH=on` for every server) is the exception: it
also reads what capture has indexed since each table's snapshot, so it
answers about as far behind as capture, usually seconds, for some memory and
speed (see [No maximum copy age](time-travel-sql.md#no-maximum-copy-age)). From the
command line, the same fold is
`bintrail baseline refresh`, and `bintrail-console watch
--baseline-refresh-interval` runs it from the daemon. See
[Refreshing on a schedule](dump-and-baseline.md#refreshing-on-a-schedule).

Some changes cannot be folded. A column added, dropped or retyped, or a
`TRUNCATE`, `DROP` or `RENAME` in the window, refuses the table, and so does a
capture gap. On the Snapshots page a failed update falls back to a full backup
at the same slot when the daemon may take one. A run publishes all of its
tables or none, so a snapshot never mixes two points in time.

## Querying with DuckDB

A `views.sql` file turns the snapshots into tables with plain names. There are
two ways to get one:

- **Web interface:** **Snapshots → Download**, open **Use it where it lives**, then **Download a DuckDB schema**.
- **Command line:**

  ```sh
  bintrail views --baseline-dir /data/baselines --out views.sql
  # or, for snapshots in S3:
  bintrail views --baseline-s3 s3://your-bucket/baselines/ --out views.sql
  ```

Then open it with your own DuckDB:

```sh
duckdb -init views.sql lake.db
```

```sql
SELECT status, count(*) FROM shop.orders GROUP BY 1;
```

Each table in the newest snapshot is a view with the table's own name, in a
schema with the source schema's name: `shop.orders`. A name DuckDB cannot read
bare is quoted: `demo."order.items"`.

**The views follow the newest snapshot.** A file generated once keeps up with
the schedule on its own: locally through the `current/` pointer, on S3 through
the `_NEWEST` object each upload writes at the root, which names the newest
snapshot (a root with no `_NEWEST` yet is listed for the newest `_SUCCESS`
marker instead, which is much slower). Nothing needs to be regenerated after
each refresh. Two forms stay pinned to one snapshot on purpose:

- a file generated with `--pin-snapshot`, for reproducible analysis at a fixed
  instant;
- the `views.sql` published next to each snapshot.

What does not follow is which views exist and how each `DECIMAL` column is
read. Both come from the snapshot named in the file's header, so generate the
file again after a table is added or dropped, or a column changes type.

**The file holds no credentials.** S3 is read through your AWS credential
chain, so the file is safe to share or commit. The S3 secret it creates lasts
one DuckDB session, so run the file again (`.read views.sql`) in each session
that reads S3.

**Opening it from outside AWS takes a few seconds on DuckDB 1.5 or newer.**
Creating the views reads a little of every file they name, and over the
internet each read is a round trip. The file reads them all at once: about 9 s
from a laptop for a 17-table snapshot (6 s for a file pinned with
`--pin-snapshot`). Older DuckDB versions read them one after another, close to
a minute. Details and numbers:
[Opening the file from outside AWS](parquet-debugging.md#opening-the-file-from-outside-aws).

**The web interface does not run the file.** Your DuckDB runs it, in your
process, on your machine: a laptop, a notebook, the box your BI tool runs on.
To put a reporting tool such as Metabase in front of the copy, see
[Dashboards](dashboards.md).

### The change history, too

With `--include-events` (or **Include the change log** in the web interface),
the file adds an `events` view over every archived row change. The table
views are the tables as of the newest snapshot. `events` is what happened to
them, row by row, with before and after images.

The change log is opt-in because defining that view opens one Parquet footer
per archived file, a cost that grows with the archive. If the first row takes
long to arrive, see the notes on `bintrail archive reconcile` in the
[DBA guide](guide.md#scenario-o-feed-a-reporting-engine-without-touching-production).

## The files

Everything DBTrail writes for analytics is plain Parquet: no catalog, no table
format, nothing that needs DBTrail running to be read.

**Snapshots**, one folder per moment, because a snapshot is read whole:

```
<baselines>/<timestamp>/<schema>/<table>.parquet
```

**Change history**, one Parquet file per archived hour, Hive-partitioned
(`key=value` folders by server, date and hour), so a filter on those keys skips
whole folders without opening a file:

```sql
SELECT count(*)
FROM read_parquet('s3://your-bucket/archive/**/*.parquet', hive_partitioning = true)
WHERE event_date = '2026-09-18' AND event_hour = 14;
```

The exact layout is in [Rotation & Status](rotation-and-status.md#archiving-partitions-to-parquet)
and the [Parquet reference](parquet-debugging.md).

### Table deltas

By default, a refresh does not rewrite a table that changed. It keeps the
table file and writes one small pair of files beside it, `.posdel` (the rows it
replaced or removed) and `.upserts` (the rows it changed or added):

```
<snapshot>/<schema>/<table>.parquet
<snapshot>/<schema>/<table>.000001.posdel
<snapshot>/<schema>/<table>.000001.upserts
```

This keeps each refresh proportional to what changed, not to the size of the
table. The table views apply the chain for you. **An engine reading the
table file by itself, without the views, sees the table as of the start of
the chain.** It has to apply the `.posdel` and `.upserts` files itself. The
merge rule is in [Dump & Baseline](dump-and-baseline.md#refreshing-on-a-schedule).

## Other engines

Which files to point another engine at depends on the question.

- **The tables as they are now: the Iceberg export.** `bintrail export iceberg`
  writes each source table as an Apache Iceberg table and appends only what
  changed since its last run. With `--catalog glue` the tables are registered
  in the AWS Glue Data Catalog, where Athena, Spark, Trino and Snowflake find
  them by name. The recipes per engine are in
  [Iceberg export: Reading the tables](iceberg-export.md#reading-the-tables).
  The snapshot files are not a substitute: with table deltas on (the default),
  a table's file is the table as of the start of its chain, and an engine that
  reads it alone gets older rows without an error (see
  [Table deltas](#table-deltas) above).
- **What changed, and when: the change history.** Plain Parquet, one file per
  archived hour, read as it is written. The recipes below were checked with
  Athena engine version 3 and Spark 3.5.5. Each row is one change: the event
  type (`1` insert, `2` update, `3` delete), the key, and the row before and
  after as JSON text. The columns are listed in the
  [Parquet reference](parquet-debugging.md#parquet-column-schema).

### Change history in Athena

An external table over the archive prefix, with partition projection so
Athena works out the folders from the query instead of from a crawler or
`MSCK REPAIR TABLE`:

```sql
CREATE EXTERNAL TABLE bintrail_history.events (
  event_id bigint, binlog_file string, start_pos bigint, end_pos bigint,
  event_timestamp timestamp, gtid string, connection_id bigint,
  schema_name string, table_name string, event_type int, pk_values string,
  changed_columns string, row_before string, row_after string,
  schema_version int, query_text string, query_hash string, commit_ts_us bigint)
PARTITIONED BY (bintrail_id string, event_date string, event_hour int)
STORED AS PARQUET
LOCATION 's3://your-bucket/archive/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.bintrail_id.type' = 'injected',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2026-01-01,NOW',
  'projection.event_hour.type' = 'integer',
  'projection.event_hour.range' = '0,23',
  'projection.event_hour.digits' = '2',
  'storage.location.template' = 's3://your-bucket/archive/bintrail_id=${bintrail_id}/event_date=${event_date}/event_hour=${event_hour}/');

SELECT event_timestamp, event_type, pk_values,
       json_extract_scalar(row_after, '$.status') AS status
FROM bintrail_history.events
WHERE bintrail_id = '<server id>'
  AND event_date = '2026-10-25' AND event_hour = 22
  AND schema_name = 'shop' AND table_name = 'orders'
ORDER BY event_id;
```

- **`injected` makes the server id mandatory.** Every query has to say
  `bintrail_id = '...'`, and Athena refuses one that does not. To query every
  server at once, list them instead:
  `'projection.bintrail_id.type' = 'enum'` and
  `'projection.bintrail_id.values' = '<id 1>,<id 2>'`. An id in the list with no
  files yet is fine. A server added later needs the list updated.
- **Start the date range at your oldest archive**, not before it: Athena
  considers every date in the range that the query does not rule out.
- **Filter on `event_date` and `event_hour`** to read only those folders.
  `event_timestamp` alone does not narrow the folders read.
- `start_pos`, `end_pos` and `commit_ts_us` are unsigned in the files; declared
  `bigint`, they read correctly.

### Change history in Spark

```python
events = (spark.read
          .option("basePath", "s3a://your-bucket/archive/")
          .parquet("s3a://your-bucket/archive/"))
events.where("event_date = '2026-10-25' AND event_hour = 22").show()
```

Spark finds the three folder columns on its own: `bintrail_id` as text,
`event_date` as a date, `event_hour` as a number. The unsigned columns
(`start_pos`, `end_pos`, `commit_ts_us`) come back as `decimal(20,0)`.
Reading `s3a://` needs the `hadoop-aws` package that matches Spark's Hadoop.

**Turn off Spark's vectorized Parquet reader for these files:**

```
spark.sql.parquet.enableVectorizedReader=false
```

Without it, Spark reads text columns wrong, without an error, in the archive
files and the snapshot files alike. bintrail writes text with the
`DELTA_LENGTH_BYTE_ARRAY` Parquet encoding, and Spark's vectorized reader loses
its place in such a column after a NULL. The next value comes back
with the start of the one after it glued on, or empty. In the check, a
`row_after` read as `{"id": 5, ...}{"id": 1, "big"` and the next row's as an
empty string. Seen with Spark 3.5.5 and 4.0.0. DuckDB, Athena and Spark with
this setting read the same files correctly. Reads are slower with it, so for
the tables as they are now, read the Iceberg export instead: its files are
written with a different library and Spark reads them correctly with the
default reader.

### Others

ClickHouse's `s3()`, Trino, pandas and anything else that reads Parquet read
these files too; they were not run against them. Check a text column that
holds NULLs before relying on one: the Spark failure above produces no error,
only wrong text. Keep the table-deltas note in mind for the snapshot files.

## What it is not

Said in the same voice as the strengths. A copy you can trust is one whose
edges you know.

- **Not high availability.** Your application never points at the copy, and
  nothing fails over to it.
- **Not your backup.** History starts the day you install it. Keep your
  physical backups.
- **Minutes behind, not seconds, for the files.** The snapshot interval is
  your setting, 5 minutes at the shortest from the web interface. The SQL
  port's fresh mode (per server on the Snapshots page) also reads what capture has indexed
  since, usually seconds behind.
- **The price of entry.** `binlog_format=ROW` and `binlog_row_image=FULL` on
  the source, and a primary key on every table you want refreshed.
  `bintrail doctor` checks the binlog settings and prints the fix.
- **Yours to operate.** One daemon, one MySQL for the index, and a directory or
  bucket for the files. One source per index.
- **Not a lakehouse.** No catalog, no table format, one MySQL per copy. Plain
  Parquet in your bucket, and `bintrail export iceberg` for the day you need
  more.

## Next

- [Install](install.md) and the [start page](https://www.dbtrail.com/docs/quickstart/)
- [Dump & Baseline](dump-and-baseline.md): the first copy and the refreshes, in full
- [Upload to S3](upload.md) and the [S3 IAM policy](s3-iam-policy.md)
- [Web interface](console.md): the Snapshots and MCP Server pages
- [Time-Travel SQL](time-travel-sql.md) and [Query & Recovery](query-and-recovery.md): the same data, used to look back and undo
