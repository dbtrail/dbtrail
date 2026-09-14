# Querying Parquet Archives with DuckDB CLI

When `bintrail rotate --archive-dir` writes Parquet files (or uploads them to S3), you can query those files directly with the DuckDB CLI. This is useful for debugging archive queries, profiling performance, or inspecting archived data outside of `bintrail query --archive-s3`.

---

## Installing DuckDB

Download the CLI from [duckdb.org/docs/installation](https://duckdb.org/docs/installation). On macOS with Homebrew:

```sh
brew install duckdb
```

Verify with `duckdb --version`.

---

## Let bintrail write the views for you (`bintrail views`)

The rest of this page shows how to write the `read_parquet` globs by hand — worth reading once, because it is what the archive layout actually is. For day-to-day use, `bintrail views` generates them from your own layout:

```sh
bintrail views \
  --index-dsn    "user:pass@tcp(index-db:3306)/bintrail_index" \
  --baseline-dir /data/baselines \
  --out          views.sql

duckdb -init views.sql lake.db
```

That gives you one view per table, as of the newest baseline snapshot. The
`events` view over the archived change log is **opt-in**, because defining it
is expensive: DuckDB opens one Parquet footer per archived file before the view
returns a single row, so the cost grows with your archive and every reader of
the file pays it, including one who only wanted their tables. Add it when you
want it:

```sh
bintrail views \
  --index-dsn    "user:pass@tcp(index-db:3306)/bintrail_index" \
  --baseline-dir /data/baselines \
  --include-events \
  --out          views.sql
```

`-init` runs the file as the session opens, so the views and the S3 secret are both there when you get the prompt. In a session that is already open, `.read views.sql` does the same.

### How fresh is what you get (`--include-live`)

By default these views read the **Parquet only**. Parquet exists for a partition once `rotate` has archived it, so everything more recent than that lives solely in the index and is absent from the views. On one measured deployment that was the most recent 12 hours. The gap is silent: a query about this morning returns no rows, which reads exactly like nothing happened.

`--include-live` adds a second leg so the `events` view also covers what the index still holds. It is a leg **of that view**, so it needs `--include-events` with it; asked for alone, the command refuses rather than turning the expensive view on for you:

```sh
bintrail views \
  --index-dsn "user:pass@tcp(index-db:3306)/bintrail_index" \
  --include-events --include-live \
  --out views.sql
```

The view then reads both and is fresh to capture lag, which is seconds when the index keeps up. A partition that has been archived but not yet dropped exists on both sides, so the index leg excludes any `event_id` the archives already returned: **the archives win the overlap.** They have to. An archived row knows its `bintrail_id`, `event_date` and `event_hour` from its storage path, and an index row has to derive or forgo them, so letting the index win would replace a known source with NULL for every event in the overlap window.

Things to know before you use it:

- **You fill in the password.** The generated file carries the index host, port, database and user, because a reader on another machine needs them, and it carries no password, because the file is meant to be shared. Open it, put your password in the empty `PASSWORD ''` slot, then run it. The attachment is read-only.
- **It needs the index reachable from wherever you run DuckDB.** The Parquet-only file works from anywhere the object store does; this one also needs a route to the index. Give `--index-dsn` a TCP address a reader can resolve: a unix socket is refused (it names one machine only), and a DSN with no address at all gets the driver's `127.0.0.1` default, which the file flags as a loopback for exactly that reason.
- **It reads the index capture is writing to.** This is the operational difference between the two legs, and the generated file's two-leg framing hides it: the Parquet leg opens files in an object store and competes with nothing, while this one opens a connection to the index a running bintrail is inserting captured events into. A large scan there contends with those inserts for the same disk and buffer pool. On one measured run an analytical query over a 15 million row `binlog_events` was followed minutes later by capture stopping on that server. An index write that runs past its timeout ends the run in a standalone capture process (`bintrail stream`, `bintrail up`, `bintrail-pg stream`), which then stays down until something restarts it, while `bintrail-console watch` restarts from its last checkpoint instead. Capture is behind either way. Two ways to keep it off capture: query `bintrail_live."binlog_events"` directly with your own `WHERE`, which does reach the index (the next bullet has the detail), or give `--index-dsn` a read replica of the index, at the cost of that replica's own lag on top of capture lag.
- **A filter on `events` does not become a filter on the index.** The index leg derives `event_date` and `event_hour` from `event_timestamp`, so a predicate on them is applied after the rows are read, and the anti-join needs every archived `event_id` regardless of what you asked for. Every query streams the whole live `binlog_events`, `row_before`/`row_after`/`query_text` included. For a narrow read of recent events, query `bintrail_live."binlog_events"` directly with your own `WHERE`: that one does reach the index. The generated file says all this where you will be looking when you measure it.

Live rows come back with `bintrail_id` as NULL unless bintrail could establish which single source the index serves. An index row carries no source of its own, while the archived rows get theirs from the storage path, so the file says which of these it observed rather than assuming: more than one source registered, no source id registered at all (a file-mode index registers none), the registry unreadable, or the registry's id disagreeing with the id the archives are written under. Pass `--bintrail-id` to name the source yourself: it wins over the registry, the way it already does for the archive paths, and it is still cross-checked against the id those paths carry.

You get:

| View | What it is |
|---|---|
| `events` | every archived binlog event across all archive sources, with `event_type` decoded to `INSERT`/`UPDATE`/`DELETE` (the raw code stays as `event_type_code`), `commit_ts_us` also exposed as a real timestamp in `commit_time`, and the Hive path columns `bintrail_id` / `event_date` / `event_hour` projected |
| `state_<schema>_<table>` | one per table in the newest discoverable baseline snapshot — that table's full contents as of the snapshot |

Archive sources come from the index's `archive_state` registry, so a new server or a new bucket shows up without being named. To generate the file without an index at all, name a source directly:

```sh
bintrail views --archive-s3 s3://bucket/archives/ --bintrail-id <uuid> \
  --baseline-s3 s3://bucket/baselines/ --out -
```

Three properties worth knowing:

- **bintrail never runs this SQL.** The command writes text; your DuckDB executes it, in your process, with no result caps and no server involved. There is no new query surface to secure.
- **No credentials are in the file.** S3 access uses DuckDB's credential chain — the same thing bintrail's own reads use — so `views.sql` is safe to commit or paste into a notebook. The generated file shows the explicit-key alternative in a comment if your environment has no chain.
- **An S3-compatible store is named in the file.** When the generating process runs with `BINTRAIL_S3_ENDPOINT`, the file sets `s3_endpoint`, `s3_url_style` and `s3_use_ssl` and repeats them in its secret, so DuckDB on another machine reads the same store instead of AWS, and keeps doing so if the secret fails. A location, not a credential: the file stays shareable.
- **The S3 secret lasts one session.** Views persist in a `.db` file; secrets do not. Reopen `lake.db` tomorrow and a `SELECT` over an S3-backed view fails with "No credentials are provided" until you run the file again (`.read views.sql`). Do not turn the secret into a `PERSISTENT` one: DuckDB resolves your credential chain at that moment and writes the resulting keys to `~/.duckdb/stored_secrets`.
- **Archive sources are named for another machine.** An archive registered both on the generating host and in S3 is listed by its S3 location; a local path appears only when the registry holds no S3 location for it. State views point wherever `--baseline-dir`/`--baseline-s3` points, so a local baseline directory resolves only on the host that holds it. The console's own reads still prefer the local copy.
- **The state views follow the newest baseline; the rest is a snapshot of the layout.** With `--include-events`, that view's globs keep picking up newly rotated partitions on their own. The `state_` views read through `<baseline root>/current`, a pointer that a NEWER completed snapshot updates (a snapshot produced for a past instant, such as a point-in-time restore, completes normally and deliberately does not take it), so a snapshot published after this file was written reaches it with nothing to re-run. That covers the case that used to catch people: a daemon running `bintrail-console watch --baseline-refresh-interval` publishes a new snapshot every interval, and the views move to it when it completes. All of them move together, because the pointer is replaced in one step, so no query can read one table from the new snapshot and another from the old, and a query already running finishes against the files it opened.

  What does *not* follow is the file's idea of the **shape** of the data: which views exist, and the precision each `DECIMAL` column is read at, were both decided from the snapshot named in the header. Regenerate after a table is added or dropped, after a column changes type, and whenever archive sources are added or removed. **Which of those you notice follows one rule**: the generated file names only the paths and the `DECIMAL` columns, so only those two can fail. A table that leaves the newest snapshot is caught by a check the file runs before it creates anything: it names the table, says which snapshot it looked in, and stops, so you never end up with half the views defined. A `DECIMAL` column renamed or dropped fails later, at its own view. Everything else is quiet, because `SELECT *` passes it through: a table *added* to the source simply has no view, a `DECIMAL` whose scale grew is read at the old scale, a column that changes to some other type arrives as whatever the new file holds (an `ORDER BY` can start sorting text), and an archive source added later is not there. The loud half is deliberate: the pointer names the newest complete snapshot whatever tables it holds, and refusing to advance unless every table is still present would freeze it forever on the first dropped table, leaving every generated file quietly serving older and older rows. Only an operator action reaches this — `--tables` on a hand-run baseline, or narrowing a server's schemas — since the periodic refresh folds exactly the tables of the snapshot it read.

  An **S3 baseline root** follows too, by a different route. There is no pointer object to publish there: doing so would mean copying every table to a second prefix, which is not atomic across tables, so a query could read half of one snapshot and half of another. Instead the file resolves the newest `_SUCCESS`-marked snapshot into a session variable when it is read, and every `state_` view reads through that one value. One lookup for the whole file, so it costs what pinning costs, and the views agree with each other the way the pointer makes them agree. A refresh published while you are working is picked up by reading the file again, which an S3 session already does for its credentials.

  Several cases pin to one snapshot instead, and the generated file always says which it did. `--pin-snapshot` is the deliberate one, for reproducible analysis against a fixed instant. A local baseline root written by an older bintrail carries no pointer until its next snapshot completes. And a `current` an operator replaced with a real directory is never touched, so that root pins too.
- **Money columns are cast back to numbers.** MySQL `DECIMAL` and `NUMERIC` are stored as text in the Parquet, so that a value MySQL can hold is never rounded to fit a narrower type. One caveat belongs with a *following* file: the precision and scale are read when the file is generated, and they do not follow. Widen a `DECIMAL` column's scale and the view goes on reading it at the old scale, rounding the extra digits away with no error. Regenerate after a column changes type. The `state_` views cast them back to `DECIMAL(p,s)` using the precision and scale the column was declared with, so `sum()` and the rest work on them directly. See below for the two cases where a column stays text.

`state_` views are the snapshot's rows, not the table's current state. To materialize a *later* point in time, use `bintrail reconstruct` — folding deltas back onto a baseline is what that command does, and it is not expressible as a view.

**Deleted rows are not in the `state_` views, and they are not lost.** A `state_<schema>_<table>` view is the table as it stood at the snapshot, so a row deleted before that instant is not in it, and a `baseline refresh` applies a DELETE by dropping the row, the same way the table did. There is no `_deleted` marker column, which is what a warehouse connector would give you instead. The row lives in the `events` view: every DELETE is kept with its full before-image in `row_before`, for as long as the archives are kept. That view is opt-in: regenerate with `--include-events` if the file you have does not define it. Without `--include-live` it covers the archives only, so a DELETE from the last few hours is not in it until rotation archives its partition; add `--include-events --include-live` to read the index too (the live leg is a leg of that view, so it needs both).

```sql
SELECT event_timestamp, pk_values, row_before
FROM events
WHERE event_type = 'DELETE'
  AND schema_name = 'mydb'
  AND table_name = 'users'
ORDER BY event_timestamp DESC;
```

To get a table back as it stood before a purge, use `bintrail reconstruct --at` with a baseline taken before it. An archival purge (rows moved out of the operational database on purpose) leaves through the binlog as ordinary DELETEs, so the refresh cannot tell it from a business delete and drops those rows from the snapshot too; the event log is where they remain.

### Every flag

| Flag | What it does |
|---|---|
| `--baseline-dir` | Local backup directory. Source of the `state_` views |
| `--baseline-s3` | S3 backup prefix. Mutually exclusive with `--baseline-dir` |
| `--pin-snapshot` | Freeze the `state_` views on the snapshot that exists now, instead of following the newest |
| `--include-events` | Add the `events` view. Off by default; see the cost above |
| `--index-dsn` | The index, where archive locations are discovered. Needed with `--include-events` unless you name a location directly |
| `--archive-dir` | Local archive root, named directly. Needs `--bintrail-id` |
| `--archive-s3` | S3 archive prefix, named directly. Needs `--bintrail-id` |
| `--bintrail-id` | The server's identity UUID. Required with the two flags above |
| `--include-live` | Add the live index leg to `events`. Needs `--index-dsn`, and read its section above first |
| `--no-baselines` | Emit `events` only, no `state_` views. Needs `--include-events` |
| `--region` | AWS region to pin in the generated S3 secret |
| `--out` | Output file, or `-` for stdout. Default `views.sql` |

A baselines-only file needs no index at all: `--baseline-dir` or `--baseline-s3`
on its own is enough, and the result reads on a machine that cannot reach your
database.

### Backups in a folder, on a machine you are not on

A file over a local backup directory names that directory, and those paths
resolve only where it is mounted, at exactly that path. The generated header says
so. Open it elsewhere and DuckDB answers `No files found`, which looks like a
broken backup and is not one.

On the Compose stack the path is a path **inside the container**
(`/var/lib/bintrail/baselines` in the `bintrail-state` volume), so it resolves
neither on a laptop nor in a shell on the host. Copy the file up and run DuckDB
in a container mounting the same volume at the same place:

```sh
scp views.sql you@server:~/
```

```sh
# on the server, from the directory holding docker-compose.yml
docker run --rm -it \
  -v "$(basename "$PWD")_bintrail-state:/var/lib/bintrail:ro" \
  -v "$HOME:/work" \
  duckdb/duckdb -init /work/views.sql /work/lake.db
```

Read-only on purpose, and the mount has to land on `/var/lib/bintrail` so the
paths in the file resolve unchanged. This is the fastest way to query: a 47-view
file opens in well under a second against local Parquet, where the same file over
S3 takes about thirty.

If the server keeps its backups in both places, the console download offers
**Works on another machine**, which emits the S3 paths instead and saves under a
different name.

### Decimal columns read as text without the views

If you point DuckDB at a baseline Parquet yourself instead of using the generated views, every `DECIMAL` and `NUMERIC` column is a `VARCHAR`, and the first aggregate you write against a money column fails:

```
Binder Error: No function matches the given name and argument types 'sum(VARCHAR)'.
```

The data is fine. bintrail stores those columns as text on purpose. MySQL allows up to `DECIMAL(65,30)` while DuckDB stops at 38 digits of precision, so no single numeric type holds every value MySQL can, and picking a narrower one would silently drop digits from a value the operator chose that column to hold. The stored text is also what bintrail's own recovery paths join and compare on, byte for byte, against the value read out of the binlog.

Cast it yourself, with the precision and scale from the source table:

```sql
SELECT sum(CAST(ol_amount AS DECIMAL(6,2))) FROM read_parquet('.../order_line.parquet');
```

Or generate the views and let them do it. Some columns still read as text even in the generated views, and the file names each affected table or column and says why:

- **A column wider than 38 digits** has no DuckDB `DECIMAL` to be cast to. Cast it to `DOUBLE` if an approximate result answers your question.
- **A file that carries no column types** casts nothing at all. The views read the precision out of the `CREATE TABLE` in each Parquet footer, and three kinds of file do not have one: a baseline taken before bintrail started embedding it, which gains the casts the next time it is taken or refreshed; a PostgreSQL-source baseline, which stores every value as text by design and will not gain them; and a footer that could not be read, which is the only one of the three that is a fault and the only one that puts an error in the bintrail log.

---

## Parquet Column Schema

Archive Parquet files contain 17 columns (the `pk_hash` stored generated column is omitted):

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | BIGINT | Auto-increment row ID from `binlog_events` |
| `binlog_file` | VARCHAR | Source binlog filename (e.g. `binlog.000042`) |
| `start_pos` | BIGINT | Byte offset where the event starts in the binlog |
| `end_pos` | BIGINT | Byte offset where the event ends |
| `event_timestamp` | TIMESTAMP | When MySQL executed the event (UTC) |
| `gtid` | VARCHAR | GTID if available (nullable) |
| `connection_id` | INT | MySQL connection ID / pseudo_thread_id (nullable) |
| `schema_name` | VARCHAR | Database name |
| `table_name` | VARCHAR | Table name |
| `event_type` | TINYINT | 1 = INSERT, 2 = UPDATE, 3 = DELETE |
| `pk_values` | VARCHAR | Pipe-delimited primary key values |
| `changed_columns` | VARCHAR | JSON array of changed column names (nullable) |
| `row_before` | VARCHAR | JSON object of the row before the event (nullable) |
| `row_after` | VARCHAR | JSON object of the row after the event (nullable) |
| `schema_version` | INT | Schema snapshot version at index time |
| `query_text` | VARCHAR | Original SQL statement, when the source logs it (nullable; archives written before statement capture existed lack the column entirely — readers substitute NULL) |
| `query_hash` | VARCHAR | `STATEMENT_DIGEST()` of `query_text` (nullable) |

---

## The archives are plain Parquet on purpose

bintrail does not store its archives, baselines or index as an Apache Iceberg
table, and that is a decision rather than a gap.

The archive tier is a change log: the evidence that `recover`, `reconstruct`
and the time-travel shim rebuild rows from. Iceberg describes the state of a
table, which is a different object. Keeping the log as plain Parquet under
`bintrail_id=/event_date=/event_hour=` lets DuckDB prune by path with no
catalog service to run, and keeps every reader on the recovery path free of a
table-format dependency. Moving the archives into a table format would be a
one-way door for that path: every consumer (`query`, `recover`, `reconstruct`,
the shim, `restore-index`, `archive reconcile`) would carry the format's
assumptions from then on.

An output can be added or dropped, so Iceberg is an output bintrail can
write, not the storage it reads from. That output exists:
[`bintrail export iceberg`](iceberg-export.md) writes each table's current
state as an Iceberg table, incrementally, for DuckDB, Spark, Trino and
Athena to read. It reads through the paths above and writes somewhere new;
nothing on the recovery path links the Iceberg library, and a test fails if
that changes. If you need Iceberg as the storage layer itself, open an issue
with the concrete case. The decision is
revisited on evidence: a use the export cannot serve, or a recovery-path
benefit measured against the current layout.

---

## Querying Local Parquet Files

Archives written by `bintrail rotate --archive-dir` follow a Hive-partitioned directory layout:

```
/mnt/archives/
  bintrail_id=abc123de-0000-0000-0000-000000000001/
    event_date=2026-02-13/
      event_hour=00/
        events.parquet
      event_hour=01/
        events.parquet
    event_date=2026-02-14/
      ...
```

Query all files under a directory with a glob pattern:

```sql
SELECT * FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
LIMIT 10;
```

The `hive_partitioning=true` option makes DuckDB recognize `bintrail_id`, `event_date`, and `event_hour` as virtual columns. You can filter on them and DuckDB will skip reading files that don't match:

```sql
-- Only reads Parquet files under event_date=2026-02-13/
SELECT * FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
WHERE event_date = '2026-02-13'
  AND schema_name = 'mydb'
  AND table_name = 'orders'
LIMIT 10;
```

To query a single server's archives without Hive partitioning, scope the glob:

```sql
SELECT * FROM parquet_scan('/mnt/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/**/*.parquet')
WHERE schema_name = 'mydb' AND table_name = 'orders'
LIMIT 10;
```

---

## Querying S3-Hosted Parquet Files

### Loading the httpfs extension

DuckDB needs the `httpfs` extension to read from S3:

```sql
INSTALL httpfs;
LOAD httpfs;
```

On first use DuckDB downloads the extension from its registry. In airgapped environments, pre-install it on a machine with internet access and copy the extension cache.

### Configuring S3 credentials

Give the session the AWS SDK default credential chain (config profiles, IAM roles) via the `aws` extension — without a secret, DuckDB only picks up **static env keys** (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` is required); profiles and roles need the secret, and SSO-session profiles have open gaps upstream ([duckdb-aws#125](https://github.com/duckdb/duckdb-aws/issues/125)):

```sql
INSTALL aws; LOAD aws;
CREATE SECRET (TYPE s3, PROVIDER credential_chain);
```

(bintrail's own DuckDB S3 sessions set this up automatically.) Static credentials still work too:

```sql
SET s3_region = 'us-east-1';
SET s3_access_key_id = 'AKIA...';
SET s3_secret_access_key = '...';
```

### Running queries

```sql
-- List all archived events for a specific table
SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE schema_name = 'mydb' AND table_name = 'users'
LIMIT 10;

-- Scope to a single bintrail server
SELECT * FROM parquet_scan('s3://my-bucket/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/**/*.parquet')
WHERE schema_name = 'mydb' AND table_name = 'users'
LIMIT 10;
```

---

## Inspecting Parquet Files

DuckDB provides metadata functions for examining Parquet file structure without reading the data.

### File schema

```sql
SELECT * FROM parquet_schema('/mnt/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/event_date=2026-02-13/event_hour=00/events.parquet');
```

Shows column names, types, and encoding. Useful for verifying the archive format matches expectations.

### File metadata

```sql
SELECT * FROM parquet_metadata('/mnt/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/event_date=2026-02-13/event_hour=00/events.parquet');
```

Shows row group count, row counts per group, compression codec, and sizes. This helps diagnose performance issues — many small row groups or uncompressed files can slow queries.

These functions also work with S3 paths (after loading `httpfs`):

```sql
SELECT * FROM parquet_metadata('s3://my-bucket/archives/bintrail_id=abc123de-0000-0000-0000-000000000001/event_date=2026-02-13/event_hour=00/events.parquet');
```

---

## Performance Profiling

### EXPLAIN ANALYZE

Measure query execution time and see how DuckDB processes the query:

```sql
EXPLAIN ANALYZE
SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE pk_values = '42';
```

The output shows the operator tree with row counts and timing at each stage.

### Filter pushdown

DuckDB pushes certain filters into the Parquet reader so it can skip entire row groups using Parquet min/max statistics. Filters that benefit from pushdown:

| Filter | Pushdown? | Notes |
|--------|-----------|-------|
| `event_timestamp >= ?` / `<= ?` | Yes | Effective when data is sorted by timestamp (DBTrail writes in timestamp order) |
| `schema_name = ?` | Yes | Pushed as a predicate on the string column |
| `table_name = ?` | Yes | Same as above |
| `pk_values = ?` | Partial | Pushed down, but row groups contain many distinct PKs so few groups are skipped |
| `event_type = ?` | Yes | Small cardinality (0/1/2) — effective at skipping groups with only one type |
| `json_contains(changed_columns, ?)` | No | Function call — evaluated post-scan |
| Hive partition keys (`event_date`, `event_hour`) | Yes | DuckDB skips entire files that don't match |

To see whether pushdown is happening, check the `EXPLAIN ANALYZE` output for `PARQUET_SCAN` filters vs. `FILTER` operators above it. Filters listed in the `PARQUET_SCAN` node are pushed down; filters in a separate `FILTER` node are applied post-scan.

### Tips for diagnosing slow queries

1. **Add time range filters**: Always include `event_timestamp` (or Hive partition key) bounds. Without them, DuckDB reads every Parquet file.

2. **Check row group size**: Archives written with very small row groups (< 10,000 rows) produce excessive per-group overhead. The default `--row-group-size` in DBTrail is large enough to avoid this. Check with `parquet_metadata()`.

3. **Reduce file count**: Scanning thousands of small files is slower than fewer large ones. If you have many hourly partitions archived, consider the Hive partition filters (`event_date`, `event_hour`) to narrow the scan.

4. **Profile PK lookups**: PK lookups (`pk_values = '...'`) can't use an index (Parquet has no indexes), so they scan all row groups. Combine with a time range to limit the scan:

   ```sql
   EXPLAIN ANALYZE
   SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
   WHERE pk_values = '42'
     AND event_date = '2026-02-13';
   ```

5. **Compare with bintrail query**: Run the same filters through `bintrail query --archive-s3` and through DuckDB CLI to compare results and timing. Differences may reveal filter translation issues.

---

## Example Queries

### Count events per table

```sql
SELECT schema_name, table_name, event_type, COUNT(*) AS cnt
FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
GROUP BY schema_name, table_name, event_type
ORDER BY cnt DESC;
```

### Find all changes to a specific primary key

```sql
SELECT event_id, event_timestamp, event_type, changed_columns, row_before, row_after
FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE pk_values = '42'
  AND schema_name = 'mydb'
  AND table_name = 'orders'
ORDER BY event_timestamp;
```

### Show event volume by hour

```sql
SELECT date_trunc('hour', event_timestamp) AS hour, COUNT(*) AS events
FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
WHERE event_date = '2026-02-13'
GROUP BY hour
ORDER BY hour;
```

### Inspect row data for a DELETE

```sql
SELECT event_id, event_timestamp, pk_values, row_before
FROM parquet_scan('/mnt/archives/**/*.parquet', hive_partitioning=true)
WHERE event_type = 3  -- DELETE
  AND schema_name = 'mydb'
  AND table_name = 'users'
ORDER BY event_timestamp DESC
LIMIT 5;
```

### Profile a PK lookup with EXPLAIN ANALYZE

```sql
EXPLAIN ANALYZE
SELECT * FROM parquet_scan('s3://my-bucket/archives/**/*.parquet', hive_partitioning=true)
WHERE pk_values = '42';
```

---

## Troubleshooting Archive Fetch Errors from `bintrail query`

When `bintrail query` reads from a Parquet archive and the read fails, it prints a warning like this to stderr at the default log level:

```
Warning: archive query failed for s3://my-bucket/events/bintrail_id=<uuid>: <error text>
```

The warning is **visible regardless of `--log-level` / `--log-format`** — if you see it once, the archive in question was skipped and the query proceeded with whatever other sources (live MySQL + other archives) succeeded. One bad archive never kills the whole query; only context cancellation (Ctrl-C or deadline expiry) short-circuits the command with a non-zero exit. See [query-and-recovery.md § Archive Fetch Error Handling](query-and-recovery.md#archive-fetch-error-handling) for the full behavior contract.

The subsections below catalogue the common failure modes and how to diagnose each one with the DuckDB CLI.

### Binder Error: column `connection_id` not found

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: Binder Error: Referenced column "connection_id" not found in FROM clause
```

**Cause**: archive Parquet files written by `bintrail` versions before v0.4.4 lack the `connection_id` column. v0.4.8 fixed the per-file query to tolerate the missing column, but old Parquet files written by even older DBTrail versions might still trigger this on environments that haven't upgraded.

**Diagnose**:

```sql
-- Check which columns the offending file actually has
SELECT * FROM parquet_schema('s3://my-bucket/events/bintrail_id=<uuid>/event_date=2026-01-15/event_hour=14/events.parquet');
```

If `connection_id` is missing, the file was written by a pre-v0.4.4 indexer. Either re-archive from the live index with a current DBTrail version, or accept that the column will be NULL for those events in merged results (which is what current DBTrail already does).

### S3 AccessDenied / credential errors

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: IO Error: S3 AccessDenied: ...
```

**Cause**: expired AWS credentials, a mis-scoped IAM role, or a bucket policy change. DBTrail uses DuckDB's standard AWS credential chain (env vars → `~/.aws/credentials` → IAM role).

**Diagnose**: reproduce the failure in the DuckDB CLI with the same credentials:

```sh
# First, confirm the AWS CLI can see the bucket:
aws s3 ls s3://my-bucket/events/bintrail_id=<uuid>/

# Then, reproduce the bintrail archive read in DuckDB:
duckdb -c "INSTALL httpfs; LOAD httpfs; SELECT COUNT(*) FROM parquet_scan('s3://my-bucket/events/bintrail_id=<uuid>/**/*.parquet');"
```

If DuckDB reports the same error, the issue is the credential chain, not DBTrail. If `aws s3 ls` succeeds but DuckDB fails, DuckDB may be using a different credential profile than the AWS CLI — explicitly set `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` in the shell running `bintrail query`.

### DuckDB memory_limit exceeded

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: Out of Memory Error: could not allocate block of size ... (memory_limit is ...)
```

**Cause**: the scan loaded more row groups than fit in DuckDB's `memory_limit` setting. This typically only fires on broad queries (no `--since`/`--until`) against large archives.

**Diagnose + fix**: narrow the time range. Every `bintrail query` invocation against an archive should include at least a `--since`/`--until` window to bound the Parquet scan:

```sh
bintrail query \
  --index-dsn "..." \
  --schema    mydb \
  --table     orders \
  --since     "2026-02-01 00:00:00" \
  --until     "2026-02-08 23:59:59" \
  --archive-s3 s3://my-bucket/events/ \
  --bintrail-id <uuid>
```

By default `bintrail query`/`recover`/`reconstruct` run their internal DuckDB under a conservative, container-safe budget — **2 threads and a 4 GB `memory_limit`**, spilling to the OS temp directory when exceeded — so narrowing the query is the usual fix. On a host with plenty of RAM you can instead **lift the cap**: `--ultrafast` lets DuckDB self-tune to the host (all CPU cores, ~80% of system RAM), and `--duckdb-memory-limit 16GB` / `--duckdb-threads N` tune either knob explicitly. See "DuckDB resource tuning" in [query-and-recovery.md](query-and-recovery.md#duckdb-resource-tuning---ultrafast). Note `--ultrafast` also switches S3 reads to in-memory `httpfs` (held outside `memory_limit`) — that section covers the trade-off.

### Corrupted Parquet file

```
Warning: archive query failed for s3://.../bintrail_id=<uuid>: IO Error: Failed to open Parquet file ... Invalid Input Error: ...
```

**Cause**: a Parquet file was truncated during upload, corrupted in transit, or never fully written (e.g. `bintrail rotate` was killed mid-archive). `bintrail rotate` writes to a temp file and renames atomically, so a partial file on disk usually indicates external interference.

**Diagnose**: identify the offending file from the archive path, then inspect it with DuckDB:

```sql
SELECT * FROM parquet_metadata('s3://my-bucket/.../events.parquet');
```

If `parquet_metadata` itself errors, the file is unreadable. Either restore it from a backup or re-archive the corresponding hour from the live index (if it's still within the retention window) via `bintrail rotate --archive-dir` against the original source.

### Context canceled / deadline exceeded

```
Error: query canceled: context canceled
```

(Note: this is an **error**, not a warning — it's printed via cobra's error path, not the archive-failure stderr channel. The command exits non-zero.)

**Cause**: the user pressed Ctrl-C, or the parent context (e.g. an orchestrator-imposed timeout) fired. Unlike plain archive failures, cancellation halts the whole query immediately without printing per-source warnings.

**Diagnose**: if you didn't press Ctrl-C, check for a parent-process timeout. Common culprits: a `timeout` wrapper in a shell script, a Kubernetes liveness probe killing the pod, a CI runner with a job-level time budget.

If the cancellation is fired by a context deadline (not Ctrl-C), the wrapped error is `context.DeadlineExceeded` instead of `context.Canceled`. Both short-circuit the archive loop via the same path.

### "Works in DuckDB CLI, fails in bintrail query"

If you can run the same glob directly in the DuckDB CLI but `bintrail query` fails with an archive warning, compare the exact query DBTrail issued. Run with `--log-level debug` to see the generated DuckDB SQL:

```sh
bintrail query --index-dsn "..." --archive-s3 s3://... --bintrail-id <uuid> \
  --since "2026-02-01 00:00:00" --log-level debug 2>&1 | grep -i parquet
```

Copy the generated `SELECT ... FROM parquet_scan(...)` into the DuckDB CLI with the same filters and compare. The two should produce identical results; if they don't, the discrepancy is either (a) a filter-translation bug in DBTrail (worth filing), or (b) a DuckDB version mismatch between DBTrail's embedded DuckDB and your CLI install.
