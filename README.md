<div align="center">

<img src="docs/img/dbtrail _ header.png" alt="DBTrail: the open-source time-travel flashback for MySQL. Every change leaves a trail. Follow it back." width="100%">

**DBTrail keeps every change on your MySQL server, before and after, and writes the SQL that undoes the ones you didn't want.**

[![Release](https://img.shields.io/github/v/release/dbtrail/dbtrail)](https://github.com/dbtrail/dbtrail/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![CI](https://github.com/dbtrail/dbtrail/actions/workflows/ci.yml/badge.svg)](https://github.com/dbtrail/dbtrail/actions)

```sql
SELECT * FROM orders WHERE id = 123 AS OF '2026-05-20 14:00:00'
```

*That query runs against production MySQL or Postgres. DBTrail makes it work.*

<img src="docs/img/console-overview.png" alt="DBTrail console: recent changes across every table, deletes surfaced first" width="850">

</div>

---

## What you get

Point-in-time recovery restores the whole database and replays every log to
reach one change. DBTrail keeps the changes themselves — it tails the MySQL
binary log or the Postgres WAL and stores every row change, with full before and
after images, in a searchable index:

- **See every change.** What changed and when, for every row, with before/after diffs.
- **Undo precisely.** Generate exact reversal SQL for just the damaged rows.
- **Undo foreign-key cascades.** Rebuild the child rows an `ON DELETE CASCADE` wiped out. Restore the foreign keys an `ON DELETE SET NULL` cleared, or an `ON UPDATE CASCADE`/`SET NULL` re-pointed when a parent's key changed. InnoDB applies all of these below the binlog, where most tools cannot see them. See [Query & Recovery](docs/query-and-recovery.md).
- **Time-travel.** Query any row or table as it was at any moment, from the web console or the `reconstruct` CLI. The live SQL `AS OF` interface also needs ProxySQL. See [Time-Travel SQL](docs/time-travel-sql.md).
- **Who changed this?** Session attribution (the database user, host, and client program behind a change) ships in the commercial distribution, DBTrail EE.
- **Prove the safety net holds.** `bintrail verify` checks offline, without touching the source, that a recovery would reproduce it. `bintrail status` flags any gap in the captured stream. You find out before you need it. See [Verify](docs/verify.md).
- **Web console.** Browse, recover, and add servers to monitor, all in the UI.
- **Ask it in plain English.** Connect Claude Desktop to your console in one click with an `.mcpb` bundle or a copy-paste URL ([5-minute guide](docs/connect-ai.md)). Any MCP client works; see the [MCP server](docs/mcp-server.md) reference.

Works with **MySQL**, **Percona Server for MySQL**, **Amazon RDS for MySQL**,
and **Amazon Aurora MySQL** (verified). **Google Cloud SQL for MySQL** should
work too; please report issues. DBTrail connects over the replication protocol
and never needs the binlog files on disk, which is why managed cloud databases
work. It requires MySQL 8.0+ with `binlog_format=ROW` and
`binlog_row_image=FULL`; `bintrail doctor` checks both and prints the exact fix.

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
and prints the next steps. They are the same four steps as the
[start page](https://www.dbtrail.com/docs/quickstart/):

1. **Sign in.** Open **http://127.0.0.1:8090** and create a username and password.
2. **Connect.** Click **+ Add server** and fill in the host and port of your MySQL server. The form suggests a user and password for DBTrail and shows the SQL that creates that user; run it on your MySQL with a login that can create users and grant them privileges, then press **Save**.
3. **First change.** Change a row on your MySQL. It shows on the Overview within a minute, with an **Undo** that writes the SQL to reverse it.
4. **First snapshot.** With one, DBTrail can rebuild a whole table as it was at a past moment. Today it takes a folder created first, then on the **Snapshots** page: type it in **Backup dir**, **Save**, and press **Create backup**. The start page has the command that creates the folder.

Prefer the command line? See the [command-line quickstart](docs/quickstart.md).

### PostgreSQL & MariaDB (alpha) sources

The same one-line install captures **PostgreSQL** and **MariaDB** sources too,
not just MySQL. In **+ Add server**, choose the source type; PostgreSQL reveals
fields for the database, replication slot, and publication. PostgreSQL needs a
one-time setup on the source first (`wal_level = logical`,
`REPLICA IDENTITY FULL`, and a publication). DBTrail validates these and never
runs DDL on your source. The full walkthrough, including managed RDS, Aurora,
and Cloud SQL: **[PostgreSQL source](docs/postgres.md)** ·
**[MariaDB source (alpha)](docs/mariadb.md)**.

## Documentation

| Start here | Reference | Operations |
|---|---|---|
| [Install](docs/install.md) | [Query & Recovery](docs/query-and-recovery.md) | [Deployment](docs/deployment.md) · [Capacity](docs/capacity.md) |
| [Start page](https://www.dbtrail.com/docs/quickstart/) · [Command-line quickstart](docs/quickstart.md) | [Web console](docs/console.md) | [Rotation & Status](docs/rotation-and-status.md) |
| [DBA guide](docs/guide.md) | [Time-Travel SQL](docs/time-travel-sql.md) · [Verify recoveries](docs/verify.md) | [Docker](docs/docker.md) |
| [30-second demo](docs/demo.md) | [Streaming](docs/streaming.md) · [Indexing](docs/indexing.md) | [Upload to S3](docs/upload.md) · [S3 IAM policy](docs/s3-iam-policy.md) · [Upgrading](docs/upgrade.md) |
| | [MariaDB source (alpha)](docs/mariadb.md) · [PostgreSQL source](docs/postgres.md) | [Server identity](docs/server-identity.md) |
| | [Connect an AI assistant](docs/connect-ai.md) · [MCP server](docs/mcp-server.md) | [Parquet debugging](docs/parquet-debugging.md) |
| | [Dump & Baseline](docs/dump-and-baseline.md) · [DDL tracking](docs/ddl-tracking.md) | [Iceberg export](docs/iceberg-export.md) |

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
