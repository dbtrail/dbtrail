# DBTrail Quickstart

DBTrail records every INSERT, UPDATE, and DELETE from MySQL into a searchable
index — so when something goes wrong you can find exactly what changed and
generate SQL to undo it.

There are two ways to start: the **web console** (no commands — recommended) or
the **command line**. Both need a source MySQL user first.

---

## Prerequisites

- A MySQL **source** with `binlog_format = ROW` and `binlog_row_image = FULL`.
  (DBTrail's preflight checks this and shows the exact fix if it's missing.)
- A user on the source for DBTrail to read from — create it on the source:

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

  `RELOAD`/`BACKUP_ADMIN` let the baseline dump take a point-in-time snapshot.
  **On managed MySQL (RDS, Aurora, Cloud SQL), `BACKUP_ADMIN` cannot be granted**, so grant `LOCK TABLES, SHOW VIEW` and set `BASELINE_LOCK_MODE=lock-all` — equally point-consistent, and the mode mydumper itself names for RDS. If you would rather grant nothing extra on a self-hosted source, `BASELINE_LOCK_MODE=safe-no-lock` never writes a torn snapshot, but it refuses on a write-active source.
  `REPLICATION SLAVE`/`REPLICATION CLIENT` drive the binlog stream; `SELECT` lets
  DBTrail snapshot the schema. DBTrail never writes to or locks the source.
  (Least-privilege variant: [streaming.md](streaming.md#the-source-mysql-user).)

Works with self-managed MySQL **and** managed services (RDS, Aurora, Cloud SQL) —
DBTrail streams over the replication protocol and never needs the binlog files on
disk.

---

## Option A — Web console (recommended, no CLI)

**1. Bring up the stack:**

```sh
curl -fsSL https://raw.githubusercontent.com/dbtrail/dbtrail/main/install.sh | sh
```

This downloads the Docker Compose stack — the console plus a bundled index store —
and starts it. (Equivalent manual steps are in the [README](../README.md).)

**2. Open the console:** go to **http://127.0.0.1:8090**. On first run, create a
username and password — that's your login from now on.

**3. Add the server to watch:** click **+ Add server** and paste the source MySQL
host, user, and password. DBTrail runs the preflight (any failure comes back as a
fix-this card), provisions an index for it, and starts streaming — you'll see
changes within the minute.

**4. Use it** — entirely in the browser:

- **Overview** — what changed recently and where, with a Recent-changes list and
  an inline **Undo**.
- **Events** — search by free text or `type:` / `pk:` / `col:` / `schema.table`
  tokens; each row expands to a before→after diff.
- **Recover** — filter to the damage, preview the affected rows, **Generate undo
  SQL**, then copy/download and apply it yourself. The console **never executes
  SQL**.
- **Status** — index health: partitions, coverage, stream lag, archives.

That's the whole loop — see a change, undo it — without touching a terminal.

> Reconstruct a full row *as of* a point in time? A **Time-travel** view appears
> once a baseline is configured — the easiest way is the compose
> [`baseline` profile](./docker.md#baselines-and-time-travel-the-baseline-profile).

See [Web console](./console.md) for login/TLS, the server switcher, and the API.

---

## Option B — Command line

Prefer the CLI (or scripting and automation)? Install the `bintrail` binary (see
[Install](install.md)) and set shorthands for your two DSNs:

```sh
export SRC="dbtrail:<your password>@tcp(127.0.0.1:3306)/"   # source MySQL
export IDX="root:secret@tcp(127.0.0.1:3306)/binlog_index"   # the index
```

> The example points both DSNs at one host for brevity. In production, run the
> index on a **separate** MySQL instance — co-locating it on the source means a
> source-disk failure takes the index down with it. See
> [Deployment](./deployment.md#separate-server-recommended).

**Start capturing changes** — one command runs the preflight, creates the index,
snapshots the schema, and streams in real time (and rotates old partitions
hourly):

```sh
bintrail up --source-dsn "$SRC" --index-dsn "$IDX"
```

It keeps running — leave it in its own terminal or run it under systemd — and
resumes from its checkpoint on restart. Want to check prerequisites on their own
first? Run `bintrail doctor --source-dsn "$SRC" --index-dsn "$IDX"`.

**Query what changed** (in another terminal):

```sh
bintrail query --index-dsn "$IDX" --schema mydb --table orders \
  --since "2026-02-19 14:00:00"
```

Useful filters: `--event-type DELETE`, `--pk 12345`, `--changed-column status`,
`--until "..."`. Add `--format json` to see full before/after values.

**Undo it** — generate reversal SQL, review, then apply it yourself:

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
| Time-travel: reconstruct full rows as of a point in time | [Dump and Baseline](./dump-and-baseline.md) — or the compose [`baseline` profile](./docker.md#baselines-and-time-travel-the-baseline-profile) |
| Use RDS, Aurora, or Cloud SQL | [Streaming](./streaming.md) |
| Understand the query and recovery options in depth | [Query and Recovery](./query-and-recovery.md) |
| Prove a recovery would actually reproduce the source | [Verify recoveries](./verify.md) |
| Archive old events to S3 before dropping | [Rotation and Status](./rotation-and-status.md#archiving-partitions-to-parquet) |
| Plan disk space for the index MySQL | [Capacity Planning](./capacity.md) |
| Use AI (Claude) to investigate changes | [MCP Server](./mcp-server.md) |
| Set up cron, systemd, Docker | [Guide](./guide.md) |
| Understand server identity and access flags | [Server Identity](./server-identity.md) |
