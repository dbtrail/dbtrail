<div align="center">

<img src="docs/img/dbtrailheader.png" alt="DBTrail: the open-source analytical replica for MySQL. Reads your binlog, writes Parquet to your bucket, query it with DuckDB." width="100%">

**DBTrail keeps your MySQL tables as Parquet in your bucket, minutes behind the source, and every version of every row. Query them with DuckDB. Production never sees the query.**

[![Release](https://img.shields.io/github/v/release/dbtrail/dbtrail)](https://github.com/dbtrail/dbtrail/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/dbtrail/dbtrail/actions/workflows/ci.yml/badge.svg)](https://github.com/dbtrail/dbtrail/actions)

</div>

```sh
$ duckdb -init views.sql
D SELECT status, count(*) FROM state_shop_orders GROUP BY 1;
```

<p align="center"><em>That query reads Parquet in your bucket. The MySQL primary never saw it.</em></p>

---

## What you get

Reports, dashboards and ad-hoc analysis compete with your application when
they run on the primary: full scans push the working set out of the buffer
pool, and a long read holds back purge. A reporting replica is one more MySQL
to pay for and patch, answering at MySQL speed. DBTrail gives those queries a
copy of their own, one that also remembers the past.

### An analytical copy

- **Zero ETL.** One full copy of each table, then only the
  binary log. DBTrail folds the changes into a new Parquet snapshot on your
  schedule, from every 5 minutes to once a day, without reading the source
  again. No pipeline, no warehouse, no Kafka. See [Analytics with DuckDB](docs/analytics.md).
- **DuckDB, ready to open.** One `views.sql` gives you one view per table,
  `state_<schema>_<table>`, that follows the newest snapshot on its own. Your
  DuckDB runs it, on your machine: a laptop, a notebook, a BI box.
- **Open files, your bucket.** Plain Parquet on disk or in S3, Hive-partitioned
  change history, nothing proprietary. Hand a table to Spark, Trino or Athena
  with `bintrail export iceberg`. See [Iceberg export](docs/iceberg-export.md).
- **Nothing inside the server.** No plugin, no fork, no agent on the database
  host. DBTrail connects the way a replica does, so it works where you cannot
  load an engine: Amazon RDS and Aurora.

### Time travel and recovery, from the same stream

DBTrail keeps every change on your MySQL server, before and after, and writes the SQL that undoes the ones you didn't want.

- **Every version of every row.** Query any row or table as it was at a
  past moment, from the web interface or the `reconstruct` CLI; the live SQL
  `AS OF` interface also needs ProxySQL. See [Time-Travel SQL](docs/time-travel-sql.md).
- **Undo precisely.** Generate the SQL that reverses just the damaged rows,
  including the child rows an `ON DELETE CASCADE` removed below the binary log.
  See [Query & Recovery](docs/query-and-recovery.md).
- **Prove it holds.** `bintrail verify` checks offline, without touching the
  source, that a recovery would reproduce it. `bintrail status` flags any gap
  in the captured stream. See [Verify](docs/verify.md).
- **Web interface and MCP.** Add servers, schedule snapshots and browse
  changes in the browser. Connect Claude Desktop or any MCP client to ask in
  plain English ([5-minute guide](docs/connect-ai.md)).

Works with **MySQL**, **Percona Server for MySQL**, **Amazon RDS for MySQL**,
and **Amazon Aurora MySQL** (verified). **Google Cloud SQL for MySQL** should
work too; please report issues. **MariaDB** is supported as an alpha source
([MariaDB source](docs/mariadb.md)). DBTrail connects over the replication
protocol and never needs the binlog files on disk, which is why managed cloud
databases work. It requires MySQL 8.0+ with `binlog_format=ROW` and
`binlog_row_image=FULL`; `bintrail doctor` checks both and prints the exact fix.

**What it is not.** Not high availability: your application never points at
the copy. Not a replacement for your physical backups: history starts the day
you install it. Minutes behind the source, not seconds: the snapshot interval
is your setting. See [Analytics with DuckDB](docs/analytics.md#what-it-is-not).

## Try it in 30 seconds

One container, zero setup, time-travel SQL:

```sh
docker run --rm -p 6033:6033 ghcr.io/dbtrail/bintrail-demo
```

See [the demo image](docs/demo.md).

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | sh
```

This downloads the Docker Compose stack, starts it, waits until DBTrail answers,
and prints the next steps. The first four are the same as the
[start page](https://www.dbtrail.com/docs/quickstart/):

1. **Sign in.** Open **http://127.0.0.1:8090** and create a username and password.
2. **Connect.** Click **+ Add server**, give the server a name, and fill in the host and port of your MySQL server. The form suggests a user and password for DBTrail and shows the SQL that creates that user; run it on your MySQL with a login that can create users and grant them privileges, then press **Save**.
3. **First change.** Change a row on your MySQL. It shows on the Overview within a minute, with an **Undo** that writes the SQL to reverse it.
4. **First copy.** The first snapshot is the analytical copy. Today it takes a folder created first, then on the **Snapshots** page: type it in **Backup dir**, **Save**, and press **Create backup**. The start page has the command that creates the folder. Set a schedule on the same page (every 5 minutes, for example) and each run after the first updates the copy from the recorded changes instead of reading the source.
5. **Query it.** On the **MCP Server** page, **Download a DuckDB schema**, then open it with `duckdb -init views.sql`. See [Analytics with DuckDB](docs/analytics.md).

Prefer the command line? See the [command-line quickstart](docs/quickstart.md).

## Documentation

| Start here | Reference | Operations |
|---|---|---|
| [Install](docs/install.md) | [Analytics with DuckDB](docs/analytics.md) | [Deployment](docs/deployment.md) · [Capacity](docs/capacity.md) |
| [Start page](https://www.dbtrail.com/docs/quickstart/) · [Command-line quickstart](docs/quickstart.md) | [Dump & Baseline](docs/dump-and-baseline.md) · [Iceberg export](docs/iceberg-export.md) | [Rotation & Status](docs/rotation-and-status.md) |
| [DBA guide](docs/guide.md) | [Query & Recovery](docs/query-and-recovery.md) · [Time-Travel SQL](docs/time-travel-sql.md) | [Docker](docs/docker.md) |
| [30-second demo](docs/demo.md) | [Verify recoveries](docs/verify.md) · [Web interface](docs/console.md) | [Upload to S3](docs/upload.md) · [S3 IAM policy](docs/s3-iam-policy.md) · [Upgrading](docs/upgrade.md) |
| | [Streaming](docs/streaming.md) · [Indexing](docs/indexing.md) · [DDL tracking](docs/ddl-tracking.md) | [Server identity](docs/server-identity.md) |
| | [Connect an AI assistant](docs/connect-ai.md) · [MCP server](docs/mcp-server.md) | [Parquet debugging](docs/parquet-debugging.md) |
| | [MariaDB source (alpha)](docs/mariadb.md) | |

## Privacy

bintrail runs entirely in your infrastructure. **Your database data never
leaves it**: not your rows, schemas, table names, queries, hostnames, DSNs, or
file paths.

Official release builds do report **metadata-only usage statistics**: which
command ran, whether it succeeded, a coarse error class, and your version and
platform. It is on by default and turns off in one line:

```sh
bintrail telemetry off      # or: DO_NOT_TRACK=1, BINTRAIL_TELEMETRY=off
bintrail telemetry show     # prints what would be sent; sends nothing
```

No identifier of any kind is stored or transmitted, so nothing ties those
statistics to you or your machine. A binary you build yourself has no reporting
address compiled in and cannot send anything. [TELEMETRY.md](docs/TELEMETRY.md)
documents every field, every control, and the CI tests that enforce both.

The [privacy policy](PRIVACY.md) covers the Claude Desktop extension (`.mcpb`),
which reports nothing at all, and how data moves when an AI client queries
your deployment.

## License

[Apache-2.0](LICENSE): free for any use, including commercial and production.
Contributions are welcome; see [CONTRIBUTING.md](.github/CONTRIBUTING.md) (a CLA
is required, prompted automatically on your first PR).

Want the index server **operated** for you (sized, backed up, upgraded, kept
alive on-call) instead of running it yourself? That is the managed service at
[dbtrail.com](https://dbtrail.com). [SUPPORT.md](docs/SUPPORT.md) draws the
ship-vs-operate line.
