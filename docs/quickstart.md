# Command-line quickstart

DBTrail keeps every change on your MySQL server, before and after, and writes
the SQL that undoes the ones you didn't want.

This page runs it from the command line: the `bintrail` binary, for scripts,
cron and systemd. To start from the web interface, which is what the one-line
installer sets up, follow the
[start page](https://www.dbtrail.com/docs/quickstart/) instead.

The command line calls a snapshot a baseline. Commands and flags such as
`bintrail baseline` and `--baseline-dir` use that word, where the web
interface and the start page say snapshot. Both name the same thing: a full
copy of your tables at one moment, which `bintrail reconstruct` starts from to
rebuild a row or a table as it was.

---

## Prerequisites

- A MySQL **source** with `binlog_format = ROW` and `binlog_row_image = FULL`.
  (DBTrail's preflight checks this and shows the exact fix if it's missing.)
- A user on the source for DBTrail to read from. Create it on the source:

  ```sql
  CREATE USER 'dbtrail'@'%' IDENTIFIED BY <choose a password>;
  GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';
  -- Only if you want baselines (Time-travel / reconstruct): they are
  -- point-consistent by default, and that guarantee needs a lock.
  -- SHOW VIEW lets the dump copy views.
  GRANT RELOAD, BACKUP_ADMIN, SHOW VIEW ON *.* TO 'dbtrail'@'%';
  -- MariaDB and MySQL 5.7 have no BACKUP_ADMIN; run this instead:
  -- GRANT RELOAD, SHOW VIEW ON *.* TO 'dbtrail'@'%';
  -- Managed MySQL (RDS, Aurora, Cloud SQL) and RDS for MariaDB cannot use the
  -- default lock mode; run this instead, with BASELINE_LOCK_MODE=lock-all:
  -- GRANT LOCK TABLES, SHOW VIEW ON *.* TO 'dbtrail'@'%';
  ```

  Put a password of your own in quotes where it says `<choose a password>`.
  As written, MySQL refuses the line on purpose, so no user is ever created
  with a password copied from this page. The web interface's + Add server form
  fills in a generated one for you.
  On MariaDB or MySQL 5.7, run the `CREATE USER` line on its own first and
  check it worked: there, a `GRANT` to a user that does not exist can create it
  with no password.

  `RELOAD`/`BACKUP_ADMIN` let the baseline dump take a point-in-time snapshot.
  **On managed MySQL (RDS, Aurora, Cloud SQL), `BACKUP_ADMIN` cannot be granted**, so grant `LOCK TABLES, SHOW VIEW` and set `BASELINE_LOCK_MODE=lock-all`: equally point-consistent, and the mode mydumper itself names for RDS. If you would rather grant nothing extra on a self-hosted source, `BASELINE_LOCK_MODE=safe-no-lock` never writes a torn snapshot, but it refuses on a write-active source.
  `REPLICATION SLAVE`/`REPLICATION CLIENT` drive the binlog stream; `SELECT` lets
  DBTrail snapshot the schema. DBTrail never writes to or locks the source.
  (Least-privilege variant: [streaming.md](streaming.md#the-source-mysql-user).)

Works with self-managed MySQL **and** managed services (RDS, Aurora, Cloud SQL):
DBTrail streams over the replication protocol and never needs the binlog files on
disk.

---

## Capture changes

Install the `bintrail` binary (see [Install](install.md)) and set shorthands
for your two DSNs:

```sh
export SRC="dbtrail:<your password>@tcp(127.0.0.1:3306)/"   # source MySQL
export IDX="root:secret@tcp(127.0.0.1:3306)/binlog_index"   # the index
```

> The example points both DSNs at one host for brevity. In production, run the
> index on a **separate** MySQL instance. Co-locating it on the source means a
> source-disk failure takes the index down with it. See
> [Deployment](./deployment.md#separate-server-recommended).

**Start capturing changes.** One command runs the preflight, creates the index,
snapshots the schema, and streams in real time (and rotates old partitions
hourly):

```sh
bintrail up --source-dsn "$SRC" --index-dsn "$IDX"
```

It keeps running (leave it in its own terminal or run it under systemd) and
resumes from its checkpoint on restart. Want to check prerequisites on their own
first? Run `bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX"`.

**Query what changed** (in another terminal):

```sh
bintrail query --index-dsn "$IDX" --schema mydb --table orders \
  --since "2026-02-19 14:00:00"
```

Useful filters: `--event-type DELETE`, `--pk 12345`, `--changed-column status`,
`--until "..."`. Add `--format json` to see full before/after values.

**Undo it.** Generate reversal SQL, review, then apply it yourself:

```sh
bintrail recover --index-dsn "$IDX" --schema mydb --table orders \
  --event-type DELETE --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
  --output recovery.sql

cat recovery.sql                 # always review before applying
mysql -u root -p mydb < recovery.sql
```

The script is wrapped in `BEGIN`/`COMMIT` and reverses events most-recent-first.
DBTrail never applies it for you. Check progress any time with
`bintrail status --index-dsn "$IDX"`.

> Same query + recover screens, read-only, without the full stack:
> `bintrail-console serve --index-dsn "$IDX"`.
>
> **Backfilling history** from binlog files already on disk (self-managed MySQL
> only): `bintrail index --index-dsn "$IDX" --source-dsn "$SRC" --binlog-dir
> /var/lib/mysql --all`. See [Indexing](./indexing.md).

---

## Next Steps

| Want to... | Read... |
|---|---|
| Browse changes and generate undo SQL from a browser | [Web console](./console.md) |
| Time-travel: reconstruct full rows as of a point in time | [Dump and Baseline](./dump-and-baseline.md), or the compose [`baseline` profile](./docker.md#baselines-and-time-travel-the-baseline-profile) |
| Use RDS, Aurora, or Cloud SQL | [Streaming](./streaming.md) |
| Understand the query and recovery options in depth | [Query and Recovery](./query-and-recovery.md) |
| Prove a recovery would actually reproduce the source | [Verify recoveries](./verify.md) |
| Archive old events to S3 before dropping | [Rotation and Status](./rotation-and-status.md#archiving-partitions-to-parquet) |
| Plan disk space for the index MySQL | [Capacity Planning](./capacity.md) |
| Use AI (Claude) to investigate changes | [MCP Server](./mcp-server.md) |
| Set up cron, systemd, Docker | [Guide](./guide.md) |
| Understand server identity and access flags | [Server Identity](./server-identity.md) |
