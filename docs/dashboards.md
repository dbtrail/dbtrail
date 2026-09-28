# Dashboards: a reporting tool on the Parquet copy

This page puts a reporting tool in front of DBTrail's Parquet copy, so your
charts read the copy instead of the production database. The worked example
is Metabase with its community DuckDB driver. The steps are the same for any
tool that runs DuckDB inside its own process.

When you are done, the tool shows your tables, and a change made on the source
reaches its charts at the next scheduled snapshot, with nothing to rebuild.

**Needs a DBTrail release newer than v0.90.0.** Before that, a views file
stopped answering at the first refresh that changed a table, and had to be
generated again.

## How it works

There is no server to connect to: no host, port, user and password to type
into the tool's form. Instead, the tool runs DuckDB inside its own process, and
DuckDB reads the Parquet files directly. What connects the two is the **views
file**, a short SQL file that turns each snapshot file into a table with a
plain name (`state_<schema>_<table>`).

Three things have to be true, and each step below takes care of one:

1. The tool can read the snapshot folder, **at the same path** the views file
   names.
2. DuckDB runs the views file every time the tool opens a connection.
3. The views file is one that **follows** the newest snapshot.

## Before you start

- The server's snapshots must be kept in a **local directory**. On the
  **Snapshots** page, under **Where and how often**, give the server a local
  backup directory. A folder there carries a `current` pointer that every
  scheduled snapshot moves forward, and that pointer is what lets the views
  follow. A server whose snapshots go only to S3 has no such pointer; see
  [Snapshots only in S3](#snapshots-only-in-s3) below.
- A snapshot schedule, set on the same page. The interval is how fresh the
  charts can be.

## 1. Get the views file

On the **MCP Server** page, **Download a DuckDB schema**. Leave **Pin to the
backup that exists now** unticked: a pinned file keeps showing that one
snapshot forever.

Or, from the command line, pointing at the same directory:

```sh
bintrail views --baseline-dir /var/lib/bintrail/baselines --output views.sql
```

Do not use the `views.sql` file stored inside each snapshot folder. It is
pinned to that snapshot on purpose, so it never moves.

The views file names absolute paths. Open it and look at one
`read_parquet('...')` line: that path is where the tool has to find the
files.

## 2. Let the tool see the files

Mount the snapshot directory into the tool's container, **read-only, at the
same path** it has in DBTrail's container. With the standard
`docker-compose.yml`, the daemon keeps its state in the `bintrail-state`
volume, mounted at `/var/lib/bintrail`, so mount that volume at the same
place:

```yaml
    volumes:
      - bintrail-state:/var/lib/bintrail:ro
```

Read-only is enough: DuckDB only reads the files. The snapshot files are
written readable by every user, so the tool's own user can read them.

## 3. Run Metabase with a DuckDB driver that loads

The official `metabase/metabase` image cannot load DuckDB. It is built on
Alpine Linux, and the DuckDB library needs the GNU C library; connecting fails
with `Error loading shared library libstdc++.so.6`. Installing packages does
not fix it.

The driver's authors publish a Dockerfile on a Debian-based image instead. Use
it:

```sh
curl -LO https://raw.githubusercontent.com/motherduckdb/metabase_duckdb_driver/main/Dockerfile
docker build -t metabase-duckdb .
```

Then run it with the volume from step 2:

```sh
docker run -d --name metabase -p 3000:3000 \
  -v bintrail-state:/var/lib/bintrail:ro \
  metabase-duckdb
```

Docker Compose puts the project name, usually the name of the folder holding
the compose file, in front of the volume's name (`<folder>_bintrail-state`).
Run `docker volume ls` to see yours. Or add Metabase as a service in a
`docker-compose.override.yml` next to DBTrail's compose file, where the short
name `bintrail-state` works.

## 4. Add the database

In Metabase, **Admin settings > Databases > Add a database**, and pick
**DuckDB**:

| Field | Value |
|---|---|
| Database file | `:memory:` |
| Init SQL | the whole content of the views file |

Save. Metabase lists one table per source table, named
`state_<schema>_<table>`, and both typed SQL and the visual query builder work
on them.

Why `:memory:` and Init SQL rather than a DuckDB database file: the views are
only definitions, and the driver runs Init SQL on each new connection, so every
connection gets them. Nothing is written to disk, no file has to be writable,
and no DuckDB version has to match the driver's.

## What stays current, and what does not

- **Rows follow the schedule.** Each scheduled snapshot moves the `current`
  pointer, and the next query reads the new one. Nothing to regenerate or
  reload. This holds whether a refresh writes a table in full or only its
  changes beside the file.
- **The list of tables does not.** Which views exist, and how each `DECIMAL`
  column is read, come from the snapshot the file was generated against. After
  a table is added or dropped, or a column changes type, get the views file
  again and paste it into Init SQL.
- **A few tables warn.** If the views file has a comment above a view saying it
  "reads the table file alone", that table's view stops with an error at the
  first refresh that writes changes beside its file. The comment says why
  (DBTrail could not read the table's schema, or another table's name starts
  with this one's name and a dot). Get the views file again when that happens.

## Snapshots only in S3

The views file for an S3-only server has no `current` pointer to read through.
Instead, it looks up the newest completed snapshot when it is run, and it
creates a session-only credential lookup rather than holding keys. In
Metabase, Init SQL runs that lookup again for each query, which also re-reads
each table's layout from S3. With many tables that is slow, and the tool's
container also needs AWS credentials in its environment.

The simpler route is to give the server a local backup directory as well (on
the Snapshots page), and follow the steps above.

Do not point the tool at a copy made with `aws s3 sync`. A synced folder has no
`current` pointer, and the sync can copy a snapshot's completion marker before
its files, so a reader could pick up a half-copied snapshot without an error.

## What this does not do

- **No server endpoint.** DuckDB runs inside the tool's process, so heavy
  queries use the tool's memory. The DuckDB driver has a field to cap it.
- **Not the source.** Every query reads the Parquet files, never the
  production database, so a heavy dashboard costs the source nothing.

See [Analytics with DuckDB](analytics.md) for what the copy is and how it
stays current, and [Dump & Baseline](dump-and-baseline.md#refreshing-on-a-schedule)
for the schedule.
