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

You run the views file once, into a small DuckDB database file, and point the
tool at that file. Three things have to be true, and the steps below take care
of them:

1. The views file is one that **follows** the newest snapshot.
2. The tool can read the snapshot folder, **at the same path** the views file
   names.
3. The database file is built with the same DuckDB version the tool's driver
   uses.

## Before you start

- The server's snapshots must be kept in a **local directory**. On the
  **Snapshots** page, under **Where and how often**, give the server a local
  backup directory. A folder there carries a `current` pointer that every
  scheduled snapshot moves forward, and that pointer is what lets the views
  follow. A server whose snapshots go only to S3 has no such pointer; see
  [Snapshots only in S3](#snapshots-only-in-s3) below.
- A snapshot schedule, set on the same page. The interval is how fresh the
  charts can be.
- The steps below assume that local directory is inside `/var/lib/bintrail`,
  the daemon's state volume in the standard `docker-compose.yml`. If it is
  somewhere else, mount that folder instead, at its own path.

## 1. Get the views file

On the **MCP Server** page, **Download a DuckDB schema**. If the panel shows a
**Works on another machine** box, leave it unticked: ticked, the file names the
S3 copy of the snapshots instead of the local folder.

Or, from the command line, pointing at the same directory:

```sh
bintrail views --baseline-dir /var/lib/bintrail/baselines --output views.sql
```

Do not use the `views.sql` file stored inside each snapshot folder. It is
pinned to that snapshot on purpose, so it never moves.

The views file names absolute paths. Open it and look at one
`read_parquet('...')` line: that path is where the tool has to find the
files.

## 2. Build the database file

Run the views file into a DuckDB database file, somewhere the snapshot paths
resolve. With the standard `docker-compose.yml`, the daemon keeps its state in
the `bintrail-state` volume, mounted at `/var/lib/bintrail`, so run DuckDB's
own image with that volume at the same place. From the folder holding
`views.sql`:

```sh
docker run --rm \
  -v <folder>_bintrail-state:/var/lib/bintrail:ro \
  -v "$PWD:/work" -w /work \
  duckdb/duckdb:1.5.5 /duckdb lake.duckdb -c ".read views.sql"
```

Docker Compose puts the project name, usually the name of the folder holding
the compose file, in front of the volume's name. Run `docker volume ls` to see
yours.

The result, `lake.duckdb`, holds only the view definitions, not data: a few
hundred kilobytes. If a table the file names is no longer in the newest
snapshot, this step stops and names it; get a new views file.

**Match the DuckDB version to the driver's.** The driver's version starts with
it: driver `1.5.5.0` is DuckDB `1.5.5`. An older DuckDB cannot open a file
written by a newer one.

## 3. Run Metabase with a DuckDB driver that loads

The official `metabase/metabase` image cannot load DuckDB. It is built on
Alpine Linux, and the DuckDB library needs the GNU C library; connecting fails
with `Error loading shared library libstdc++.so.6`. Installing packages does
not fix it.

The driver's authors publish a Dockerfile on a Debian-based image instead.
Build it with the driver version that matches step 2:

```sh
curl -LO https://raw.githubusercontent.com/motherduckdb/metabase_duckdb_driver/1.5.5.0/Dockerfile
docker build -t metabase-duckdb --build-arg METABASE_DUCKDB_DRIVER_VERSION=1.5.5.0 .
```

Then run it with two read-only mounts: the snapshot volume **at the same path**
it has in DBTrail's container, and the folder holding `lake.duckdb`:

```sh
docker run -d --name metabase -p 3000:3000 \
  -v <folder>_bintrail-state:/var/lib/bintrail:ro \
  -v "$PWD:/bi:ro" \
  metabase-duckdb
```

Read-only is enough for both: DuckDB only reads them. The snapshot files are
normally written readable by every user, so Metabase's own user can read them.

## 4. Add the database

In Metabase, **Admin settings > Databases > Add a database**, and pick
**DuckDB**:

| Field | Value |
|---|---|
| Database file | `/bi/lake.duckdb` |
| Establish a read-only connection | on |

Save. Metabase lists one table per source table, named
`state_<schema>_<table>`, and both typed SQL and the visual query builder work
on them.

Why not `:memory:` with the views file pasted into **Init SQL**, which skips
step 2: the driver runs Init SQL before every query, not once per connection.
With 100 tables that added about 1.5 seconds to each query, against 27
milliseconds with the database file. And when one table leaves the snapshot,
every query fails instead of only that table's.

## What stays current, and what does not

- **Rows follow the schedule.** Each scheduled snapshot moves the `current`
  pointer, and the next query reads the new one. Nothing to regenerate or
  reload. This holds whether a refresh writes a table in full or only its
  changes beside the file.
- **The list of tables does not.** Which views exist, and how each `DECIMAL`
  column is read, come from the snapshot the file was generated against. After
  a table is added or dropped, or a column changes type, get the views file
  again and build `lake.duckdb` again (step 2). Until then, a dropped table's
  chart fails and the others keep answering.
- **A few tables warn.** If the views file has a comment above a view saying it
  "reads the table file alone", that table's view stops with an error if a
  refresh writes changes beside its file. The comment says why (DBTrail could
  not read the table's schema, or another table's name starts with this one's
  name and a dot). Get the views file again when that happens.

## Snapshots only in S3

The views file for an S3-only server has no `current` pointer to read through.
Instead, it looks up the newest completed snapshot in a session variable when
it is run, and it reads S3 through a credential lookup that lasts one session.
Neither is kept in a database file, so each connection would have to run those
statements again, through **Init SQL**, which the driver runs before every
query. The tool's container would also need AWS credentials. This page does not
cover that route.

Give the server a local backup directory as well (on the Snapshots page), and
follow the steps above.

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
