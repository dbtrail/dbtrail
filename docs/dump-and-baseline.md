# Dump and Baseline — Using mydumper with DBTrail

DBTrail uses [mydumper](https://github.com/mydumper/mydumper) to create logical dumps of MySQL databases. The dump output is then converted to Parquet files by `bintrail baseline`, producing a point-in-time snapshot of every table that can be stored alongside archived binlog event partitions for long-term audit reconstruction.

This document covers running dumps, converting to Parquet baselines, and scheduling.

---

## Why mydumper?

DBTrail's binlog index captures every change (INSERT, UPDATE, DELETE) but not the initial state of rows that existed before indexing began. A baseline snapshot fills that gap — it records every row as it existed at a known point in time.

mydumper is used instead of `mysqldump` because it:

- Dumps tables in parallel (configurable thread count)
- Produces a **point-consistent** snapshot by default (`--sync-thread-lock-mode FTWRL --trx-tables`), which needs `RELOAD`/`FLUSH_TABLES` (plus `BACKUP_ADMIN` on MySQL/Percona 8.0+) — see [Cross-table consistency](#cross-table-consistency) for the lower-privilege alternatives and what each one trades away
- Outputs per-table files that `bintrail baseline` can process independently
- Supports both SQL INSERT and TSV (`*.dat`) output formats

---

## Getting mydumper

**No installation required** — if Docker is available on your system, `bintrail dump` will automatically use the [official mydumper Docker image](https://hub.docker.com/r/mydumper/mydumper) (`mydumper/mydumper`). This is the recommended zero-setup approach.

The resolution order is:

1. If `--mydumper-path` is explicitly set — use that binary (no validation)
2. If a **compiled** `mydumper` binary is found on `$PATH` — use it (shell script wrappers are skipped automatically)
3. If Docker is available — invoke mydumper via `docker run`
4. If none of the above — fail with a clear error message

> **Note:** Do not place a shell script wrapper named `mydumper` on your PATH. DBTrail detects scripts (files starting with `#!`) and skips them to avoid argument-handling issues with volume mounts. If you need to force a specific binary, use `--mydumper-path`.

To pin a specific mydumper Docker image version:

```sh
bintrail dump \
  --mydumper-image mydumper/mydumper:v1.0.3-1 \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output /tmp/mydumper-output
```

<details>
<summary>Manual mydumper installation (advanced)</summary>

If you prefer to install mydumper as a local binary instead of using Docker:

### Ubuntu / Debian

```sh
# Download from the releases page — check for the latest version:
# https://github.com/mydumper/mydumper/releases/latest
wget https://github.com/mydumper/mydumper/releases/download/v1.0.3-1/mydumper_1.0.3-1.jammy_amd64.deb
sudo dpkg -i mydumper_*.deb

# Or from the system repository. Ubuntu 24.04 packages 0.10.1, older than
# 0.18.1, the first build that accepts --sync-thread-lock-mode. With such a
# build the default lock mode runs with mydumper's own FTWRL; any explicit
# `bintrail dump --lock-mode` (even ftwrl) and a lock mode other than ftwrl set
# in the web interface are refused, naming the installed version. It is also
# older than 0.16.3, so against MySQL 8.4 and newer it cannot record the binlog
# position and its dumps are refused whatever the lock mode.
sudo apt-get install mydumper
```

### RHEL / CentOS / Amazon Linux

```sh
# Check for the latest version: https://github.com/mydumper/mydumper/releases/latest
wget https://github.com/mydumper/mydumper/releases/download/v1.0.3-1/mydumper-1.0.3-1.el9.x86_64.rpm
sudo rpm -i mydumper-*.rpm
```

### macOS

```sh
brew install mydumper
```

### Custom path

If mydumper is installed in a non-standard location, pass its path explicitly:

```sh
bintrail dump --mydumper-path /opt/mydumper/bin/mydumper ...
```

### Verify

```sh
mydumper --version
```

</details>

---

## The dump → baseline pipeline

The pipeline has two steps:

```
Step 1: bintrail dump    →  mydumper output directory (SQL/TSV files per table)
Step 2: bintrail baseline  →  Parquet files (one per table)
```

**Step 1** requires a live connection to the source MySQL server.
**Step 2** operates purely on files — no database connection needed. It can run on a different machine from where the dump was taken.

**Disk.** Step 1 writes the whole dump uncompressed, and step 2 writes the Parquet while the dump is still there, so the disk has to hold both at once. On synthetic data the dump stayed below the tables' `DATA_LENGTH + INDEX_LENGTH`, and dump plus Parquet reached 1.8× of it for random binary data. A full read started from the web interface or a schedule checks this before mydumper starts. It refuses only below half the estimate, since the sum is an upper bound for the dump, and warns below the estimate and when the peak may not fit. It writes the dump to the working folder (`BINTRAIL_CONSOLE_BASELINE_STAGING`, or the **Working folder** row in the backup settings). With nothing set that is `baseline-staging` beside DBTrail's own data, so it shares that disk (the capacity page lists the cases where it stays under the system temp folder); move it to a disk with room. Measurements, the rule of thumb and how the check decides: [Capacity planning](./capacity.md#disk-for-a-full-read-the-dump-is-on-disk-before-it-is-parquet).

---

## Step 1: Running a dump (`bintrail dump`)

`bintrail dump` is a thin wrapper around mydumper. It validates inputs, acquires a lockfile to prevent concurrent dumps, and invokes mydumper with the correct flags.

### Basic usage

```sh
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output /tmp/mydumper-output
```

This dumps **every accessible schema** into `/tmp/mydumper-output` — bare `bintrail dump` applies no schema filter, so mydumper also tries `mysql`, `sys`, and `performance_schema`. Pass `--schemas mydb,otherdb` to scope it to your data: a least-privilege capture user (no `SHOW VIEW` on the `sys` views) **needs** that filter or the dump fails loudly. (The bundled Compose `baseline` profile excludes system schemas automatically; the bare CLI does not.)

### All flags

| Flag | Default | Description |
|---|---|---|
| `--source-dsn` | *(required)* | DSN for the source MySQL server |
| `--output` | *(required)* | Directory for mydumper output. Never blindly deleted: a non-empty directory that is not a recognizable prior dump is refused; a prior dump is moved aside and restored if the new dump fails (see [Output directory behavior](#output-directory-behavior)). The older name `--output-dir` keeps working |
| `--schemas` | *(all)* | Comma-separated schema filter (e.g. `mydb,otherdb`) |
| `--tables` | *(all)* | Comma-separated table filter (e.g. `mydb.orders,mydb.items`) |
| `--mydumper-path` | `mydumper` | Path to the mydumper binary. When set, skips Docker fallback. |
| `--mydumper-image` | `mydumper/mydumper:latest` | Docker image for mydumper. Used only when no local binary is found. |
| `--threads` | `4` | Number of parallel dump threads |
| `--lock-mode` | `ftwrl` | How mydumper syncs its threads onto one instant: `ftwrl` (point-consistent), `lock-all` (point-consistent, needs only LOCK TABLES; use it on managed MySQL), `safe-no-lock` (no extra privilege, aborts rather than emit a torn snapshot), `no-lock` (accepts a torn snapshot) |
| `--encrypt` | `false` | Encrypt dump files at rest using AES-256-CBC, with an HMAC-SHA256 integrity sidecar per file (requires `openssl` on `$PATH`) |
| `--encrypt-key` | `~/.config/bintrail/dump.key` | Path to the encryption key file (generate with `bintrail generate-key`) |
| `--format` | `text` | Output format: `text` or `json` |

### Encryption and integrity (`--encrypt`)

With `--encrypt`, every dump file is piped through `openssl enc -aes-256-cbc -pbkdf2` as mydumper writes it, producing `<file>.enc`. CBC encrypts but does **not** authenticate, so after the dump completes bintrail also writes an HMAC-SHA256 digest of each `.enc` file to a `<file>.enc.hmac` sidecar, keyed with your `--encrypt-key` file. Keep the sidecars next to the `.enc` files (copy/upload them together).

`bintrail baseline --encrypt` verifies each sidecar **before** decrypting:

- **Mismatch** — the `.enc` file was modified (tampering, bit rot, truncated copy) or a different key is in use. This is a **hard error**; the file is never decrypted, so corrupted SQL can't silently flow into your baseline.
- **Sidecar missing** — a dump made by an older bintrail. Decryption proceeds with a warning (`legacy unauthenticated dump, integrity cannot be verified`). Re-run `bintrail dump --encrypt` to get authenticated dumps.

### Schema and table filtering

```sh
# Dump only the 'mydb' schema
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output /tmp/mydumper-output \
  --schemas mydb

# Dump specific tables
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output /tmp/mydumper-output \
  --tables mydb.orders,mydb.customers
```

When a single schema is given, DBTrail passes `--database <schema>` to mydumper. When multiple schemas are given, it constructs a regex filter (`--regex ^(s1|s2)\.`). Table filtering uses mydumper's `--tables-list` flag.

### What mydumper flags does DBTrail pass?

DBTrail always passes these flags to mydumper:

| mydumper flag | Purpose |
|---|---|
| `--host`, `--port`, `--user` | Connection details (parsed from `--source-dsn`). The password is **not** passed as a flag — see below. |
| `--outputdir` | Output directory |
| `--threads` | Parallelism |
| `--compress-protocol` | Compress the MySQL protocol traffic |
| `--complete-insert` | Generate `INSERT INTO table (col1, col2, ...) VALUES (...)` with column names — required for `bintrail baseline` to parse the output correctly |

The source password never appears on mydumper's command line, so it is not visible in `ps aux` or (Docker mode) `docker inspect`. With a local mydumper binary it is delivered via `MYSQL_PWD` in the child process environment; in Docker mode it is written to a temporary `0600` MySQL option file bind-mounted read-only into the container, which mydumper reads via `--defaults-file`.

### Cross-table consistency

Under `no-lock` (`--sync-thread-lock-mode NO_LOCK --trx-tables`), every mydumper worker thread opens its own transactional snapshot **independently, with no synchronization barrier between threads**. Each table's snapshot is therefore anchored at *that thread's* instant, not at one shared instant for the whole dump. On a quiet source the skew between threads is negligible; on a write-heavy source it can be milliseconds to a few seconds. Two consequences follow:

- **The dump's recorded binlog coordinates don't correspond exactly to any single thread's actual snapshot instant** — they're an approximation across however many threads ran.
- **A reconstruct that spans multiple tables (e.g. a parent/child pair joined by a foreign key) can read mutually inconsistent state** — the parent table's snapshot may be a few rows ahead of or behind the child table's.

In practice this skew is almost always absorbed by DBTrail's idempotent delta replay and the baseline's second-granularity anchor, but it is a real gap, not a rounding error. It is also worse on non-transactional tables (MyISAM), which get no consistency guarantee at all under `NO_LOCK` — every row can be mid-write when read. mydumper still dumps a MyISAM table under `NO_LOCK` (it does not refuse), it just gives it no consistency guarantee — see the note on the `FTWRL` mode below, which behaves differently for the *same* table.

**The default is `ftwrl` (point-consistent).** A baseline is not an archive: it is the seed state
`reconstruct` merges binlog deltas onto, so a snapshot stitched from several instants yields a table
that never existed at any point in time, and every downstream answer — `reconstruct`, `verify`,
`drill` — inherits it with nothing saying so. That is not something an operator should get by not
choosing, which is why `--lock-mode` (CLI) defaults to `ftwrl` and the weaker modes must be asked for by name.
The console's snapshots choose between the two point-consistent modes on their own when
`BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` (`BASELINE_LOCK_MODE` in the compose `.env`) is unset:
`lock-all` for an Amazon RDS or Aurora host name, `ftwrl` elsewhere, and one retry with `lock-all`
when `ftwrl` is refused. They never fall to a weaker mode; setting the variable overrides the choice.
`bintrail dump` does not choose: its `--lock-mode` is what runs.

| `--lock-mode` | mydumper mode | Point-consistent? | Works on a write-active source? | Privileges |
|---|---|---|---|---|
| `ftwrl` (default) | `FTWRL` | yes | yes | `RELOAD`/`FLUSH_TABLES`, plus `BACKUP_ADMIN` on MySQL/Percona 8.0+ |
| `lock-all` | `LOCK_ALL` | yes | yes | `LOCK TABLES` |
| `safe-no-lock` | `SAFE_NO_LOCK` | yes — or it aborts | **usually not** | `SELECT` + `REPLICATION CLIENT` |
| `no-lock` | `NO_LOCK` | **no** | yes | `SELECT` + `REPLICATION CLIENT` |

What each mode costs the application while it waits for its lock, and what it waits for, is
measured in [How long a snapshot blocks writes](snapshot-lock-measurements.md).

Every mode also needs `SHOW VIEW` when the dumped schemas hold views: mydumper stops the whole dump
at the first view it cannot read (`SHOW VIEW command denied`), including under `no-lock`.

**On managed MySQL, use `lock-all`.** RDS (and equivalents) will not grant
`BACKUP_ADMIN` at all — `GRANT BACKUP_ADMIN ON *.* TO CURRENT_USER()` is refused
with *"ERROR 1227 ... you need the RDSADMIN USER privilege"* — and mydumper's
`FTWRL` path issues `LOCK INSTANCE FOR BACKUP` first, so `ftwrl` cannot work
there no matter what else you grant. `lock-all` synchronizes the workers by
locking the exported tables instead of the instance, needs only `LOCK TABLES`
(which the RDS master user already has, and which also works granted on just the
dumped schema), and is equally point-consistent. mydumper says so itself — its
help carries *"We support LOCK_ALL and SAFE_NO_LOCK modes for RDS/Aurora"* — and
both directions were verified against the pinned build.

`safe-no-lock` does not *prevent* thread skew — it *detects* it. mydumper compares the binlog
position before and after syncing threads and, on any difference, stops with *"we cannot guarantee
the backup to be consistent. Stopping backup due to the use of SAFE_NO_LOCK."* So it never writes a
torn snapshot, but on a source taking concurrent writes it will mostly refuse. Verified empirically
against mydumper `v1.0.3-1` on MySQL 8.0 and 8.4, RDS for MySQL 8.4 and MariaDB 10.11: with a steady load
of 300 transactions per second it aborted in 5 of 5 runs on each, and it is the only low-privilege mode
that will not lie to you.

`no-lock` accepts a torn snapshot. mydumper's own help describes it as the mode to use *"if you
don't need a consistent backup"* and pointedly leaves it out of its list of sync modes (*"There are 4
modes that can be use to sync: SAFE_NO_LOCK, FTWRL, LOCK_ALL and GTID"*). Choose it only when you
knowingly accept that the snapshot may not represent any single instant.

Neither surface downgrades silently: if the privileges for `ftwrl` are missing, the dump refuses
with an actionable error naming the alternatives, rather than quietly producing a weaker snapshot.

#### The snapshot records how it was locked

Every table file of a snapshot records the lock mode of the read it comes from, in its footer
(`bintrail.lock_mode`). It is read three ways, and only three:

| Record | Read as | What it means |
|---|---|---|
| `ftwrl`, `lock-all`, `safe-no-lock`, or `pg-repeatable-read` (PostgreSQL) | `consistent` | every row is from one moment |
| `no-lock` | `torn` | rows were copied at different moments and may not agree with each other |
| none, or a value this version does not know | `unknown` | nothing says how it was locked |

`unknown` is never read as `consistent`. Every snapshot taken before this record existed has
none, and `no-lock` was the default before point-consistent dumps became the default.

Where the record comes from:

- `bintrail dump` writes the mode it gave mydumper into the dump, in a file named
  `bintrail_dump_lock_mode`, and `bintrail baseline` copies it into the snapshot. The web
  interface's full snapshot and the compose `baseline` profile do the same.
- A dump made by running mydumper by hand has no such file, so the snapshot made from it is
  `unknown`. The same goes for a mydumper too old to take `--sync-thread-lock-mode`: it
  chooses its own mode, and what it chose is not on record.
- A PostgreSQL snapshot reads every table inside one `REPEATABLE READ` transaction, so it is
  `consistent` by construction.

**The record is inherited.** A snapshot updated from the recorded changes (`baseline refresh`,
a scheduled update, a restore to a moment, `reconstruct --output-format parquet`) reads no
database. Each of its tables says what the snapshot it was built from said: `torn` stays
`torn` and `unknown` stays `unknown`. The rows an update does not touch are the ones a torn
read left as they were, so an update cannot make a snapshot consistent. A table that did not
change is reused as the older snapshot's own file, and keeps that file's record. No published
snapshot is modified for any of this.

A snapshot is only as good as its worst table: one `torn` table makes the snapshot `torn`.
Only a new point-in-time database read clears it (a locked read, a `safe-no-lock` read that completed, or any PostgreSQL read).

`bintrail status --baseline-dir` prints it in the `POINT_IN_TIME` column (`snapshot_lock` in JSON), the
web interface marks the snapshot's row, and `bintrail verify` uses it to read a difference
(see [verify.md](verify.md)).

**Every point-consistent mode covers transactional tables only**, `lock-all` exactly as much as `ftwrl`, verified separately for each. bintrail passes `--trx-tables` on all modes, and under a mode that is attempting a consistent backup mydumper detects a non-transactional (MyISAM) table and **refuses to dump at all** ("Non transactional table found ... Restart backup using --trx-tables=0"), which the daemon passes on as the run's error, instead of silently proceeding the way it does under `NO_LOCK`. Verified empirically: the identical MyISAM table dumped successfully (with only a warning) under `NO_LOCK`, and was hard-refused under both `FTWRL` and `LOCK_ALL`. The refusal is gated to an actual "consistent backup attempt" (mydumper's own wording), which `NO_LOCK` explicitly is not attempting and the point-consistent modes are. **Switching `ftwrl` → `lock-all` does not get you past it**; a source with MyISAM tables needs those tables converted, excluded via `--tables`, or a low-privilege mode.

`FTWRL` mode needs `RELOAD` or the `FLUSH_TABLES` dynamic privilege (for `FLUSH TABLES WITH READ LOCK`) on **every** source flavor, verified against the pinned mydumper build (`v1.0.3-1`) against a real MySQL 8.0 source, plus `BACKUP_ADMIN` (for `LOCK INSTANCE FOR BACKUP`) **only on MySQL/Percona 8.0+**. `BACKUP_ADMIN` is a MySQL 8.0+ dynamic privilege: it does not exist on MariaDB (any version) or MySQL 5.7, and neither of those issues `LOCK INSTANCE FOR BACKUP`, so `FTWRL` there needs only `RELOAD`/`FLUSH_TABLES`. The daemon detects this from the source's own `SELECT VERSION()` and checks for exactly the privileges that source actually needs **before** invoking mydumper, refusing with a clear, actionable error if any are missing. This is not just belt-and-suspenders: on a MySQL/Percona 8.0+ source, granting `BACKUP_ADMIN` **without** `RELOAD`/`FLUSH_TABLES` does not make mydumper fail cleanly, it makes the pinned build **crash** (a segfault, reproduced on both amd64 and arm64), so the daemon's own preflight check exists specifically to turn that crash into a clean error instead of ever letting mydumper attempt it half-privileged. Point-consistent mode never silently falls back to `NO_LOCK`. Both surfaces expose the same four modes: `bintrail dump --lock-mode`, and `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` for the daemon's in-process baseline pipeline.

### Concurrency protection

Only one `bintrail dump` can run at a time. A lockfile at `$TMPDIR/bintrail-dump.lock` prevents concurrent runs. If a previous dump crashed without cleaning up, DBTrail detects the stale lock (by checking if the PID is still alive) and removes it automatically.

### Output directory behavior

`bintrail dump` never blindly deletes the `--output`:

- **Absent or empty** — used as-is (mydumper creates it if needed).
- **Non-empty and recognizable as a prior dump** (contains a `metadata` or `metadata.partial` file, or the `bintrail_dump_started_at_utc` marker) — moved aside to a unique sibling `<dir>.old-<pid>-<nanos>` before the new dump starts. The backup is deleted only after the new dump **succeeds**; if the dump fails, the previous dump is restored in place.
- **Non-empty and anything else** — the dump is **refused**:

  ```
  --output "<dir>" is not empty and does not look like a prior bintrail/mydumper dump (no "metadata" marker); refusing to delete it. Remove it yourself or point --output elsewhere
  ```

  This protects against a typo'd `--output` (or a stray `BINTRAIL_OUTPUT_DIR` picked up from an env file) wiping an arbitrary directory, including baselines that `reconstruct`/`verify` depend on.

Source connectivity is also validated **before** the output directory is touched (`cannot connect to source; refusing to touch --output ...`), so a dump that fails to connect never disturbs the previous dump.

---

## Step 2: Converting to Parquet (`bintrail baseline`)

Once mydumper finishes, convert the output to Parquet:

```sh
bintrail baseline \
  --input  /tmp/mydumper-output \
  --output /data/baselines
```

### All flags

| Flag | Default | Description |
|---|---|---|
| `--input` | *(required)* | mydumper output directory (from step 1) |
| `--output` | *(required)* | Parquet output base directory. `--output-dir` is accepted too |
| `--timestamp` | *(from mydumper metadata)* | Override the snapshot timestamp (ISO 8601) |
| `--tables` | *(all)* | Comma-separated `db.table` filter |
| `--compression` | `zstd` | Parquet compression: `zstd`, `snappy`, `gzip`, `none` |
| `--row-group-size` | `500000` | Rows per Parquet row group |
| `--encrypt` | `false` | Decrypt encrypted dump files (`.enc`, from `dump --encrypt`) before processing, verifying each file's `.enc.hmac` integrity sidecar first — a mismatch is a hard error, a missing sidecar (legacy dump) warns and proceeds (requires `openssl` on `$PATH`) |
| `--encrypt-key` | `~/.config/bintrail/dump.key` | Path to the decryption key file |
| `--upload` | *(disabled)* | S3 URL to upload Parquet files after generation |
| `--upload-region` | *(from AWS env)* | AWS region for `--upload` |
| `--baseline-retain` | *(disabled)* | Prune local snapshots older than this (`Nd`/`Nh`) once a durable S3 copy exists (requires `--upload`) |
| `--retry` | `false` | Skip tables whose Parquet file already exists and S3 objects already uploaded |
| `--bintrail-id` | *(unsigned)* | Server identity UUID to sign the snapshot with (env `BINTRAIL_ID`); see [One writer per location](#one-writer-per-location) |
| `--format` | `text` | Output format: `text` or `json` |

### Output structure

Files are organized as:

```
<output>/<timestamp>/<database>/<table>.parquet
```

For example:

```
/data/baselines/2026-03-02T14-30-00Z/mydb/orders.parquet
/data/baselines/2026-03-02T14-30-00Z/mydb/customers.parquet
```

The timestamp defaults to the dump's start time, resolved in this order:

1. **The `bintrail dump` marker, if present.** `bintrail dump` records its own UTC wall-clock time immediately before invoking mydumper into a `bintrail_dump_started_at_utc` sidecar in the output directory. This is unambiguous — `bintrail baseline` prefers it whenever present.
2. **mydumper's `Started dump at:` metadata line, otherwise.** This line is written in the **dump host's local time**, but `bintrail baseline` parses it as if it were already UTC. If you produced the mydumper dump yourself (outside `bintrail dump` — e.g. running mydumper directly, or from another tool), the dump host's clock **must** be set to UTC (`TZ=UTC`), or every reconstruct/verify/shim consumer that anchors replay on this snapshot will be off by the host's UTC offset — on a UTC+2 host, for example, deltas in the resulting 2-hour window are silently excluded from replay.

The web interface's **Create baseline** button runs mydumper and the Parquet conversion in the same process, so it passes its own captured UTC time straight through and is unaffected by the dump host's timezone either way.

Override the resolved timestamp with `--timestamp` if needed.

Each snapshot also records its **binlog anchor** (the file/position/GTID where the deltas on top of it begin) and, per table, a **content digest + row count** in the Parquet metadata (used by [`bintrail verify`](verify.md)). Baseline-anchored consumers (full-table and single-row `reconstruct`, the shim's `_snapshot`, `verify`, and cascade recovery's baseline fallback) fetch deltas using this recorded binlog **position** as the window's exact lower bound, not the snapshot's wall-clock timestamp: a row-event's timestamp reflects when its statement executed, not when its transaction committed, so a transaction that executed just before the snapshot instant but committed (and so was logged) just after it would otherwise be silently missed by a timestamp-only lower bound. Snapshots taken before this position was recorded (or that never recorded one) fall back to the timestamp alone. A `_SUCCESS` marker is written when the conversion completes; a partially-converted snapshot carries an `_INCOMPLETE` marker instead and is excluded from discovery (see [Pruning old local snapshots](#pruning-old-local-snapshots---baseline-retain)). Since [#1583](https://github.com/dbtrail/dbtrail/issues/1583) a completing snapshot is also published with its own `views.sql` next to `_SUCCESS`: the same DuckDB schema `bintrail views` writes, pinned to the snapshot's own paths (respelled as `s3://` URLs when the snapshot is uploaded), so the file that says how to open the data can never disagree with the data it sits beside. The `.tar.gz` download in the web interface carries a relative copy instead, which works from inside the unpacked folder wherever it lands.

### One writer per location

**One DBTrail installation writes into one snapshot folder or S3 prefix.** Give each installation its own.

Archives are kept apart by a `bintrail_id=<uuid>/` folder per server. Snapshots have no such folder: each one is a folder named after its time, directly under the location. When two installations write into the same location their snapshots interleave, and whatever takes the newest one gets one installation's tables this time and the other's the next. If both have a schema and a table of the same name (production and staging of one application, two replicas), a read returns rows from the wrong database, with no error.

Nothing refuses this today. It is reported instead:

- Each snapshot is signed by its writer: an empty file named `_WRITER.<bintrail_id>` next to `_SUCCESS`. The web interface, `baseline refresh` and `reconstruct --output-format parquet` sign with the `bintrail_id` recorded in the index they work from. `bintrail baseline` and `bintrail-pg baseline` have no index, so they sign with `--bintrail-id` and leave the snapshot unsigned without it. A snapshot built on top of an older one is signed by the installation that builds it.
- When a listing finds snapshots signed by more than one writer in a location, it logs a warning that names them, and the web interface shows it on the Snapshots page, in the server's settings, naming the other writer.
- Snapshots written before signatures existed are unsigned. An unsigned snapshot names no writer and never raises the warning, so a location with older snapshots and one signing installation reports nothing.

The warning stays while the location still holds the other writer's snapshots. An index that was created again gets a new `bintrail_id`, so its new snapshots are signed differently from the ones it wrote before: point it at a new folder.

A later release will refuse to write into a location that holds another writer's snapshots.

### Views are skipped

A view holds no rows of its own, so it is not copied. The snapshot holds the tables only, and the run says what it left out:

```
Baseline complete.
  tables    : 2
  views     : 1 skipped (shop.big_orders)
  rows      : 6
  files     : 2
```

The log names every skipped view, and `--format json` lists them under `views_skipped`. A source with no views prints no `views` line. The dump user still needs `SHOW VIEW`: without it mydumper stops at the first view.

The snapshot keeps the list too, in a file named `_VIEWS_SKIPPED` next to `_SUCCESS`: the count, the names (up to 1000) and the time the source was read. The Snapshots page of the web interface shows the count on the snapshot's row ("2 tables, 1 view skipped") and the names when the row is opened. A source with no views writes no file, and a snapshot written before this file existed has none, so neither says anything about views.

A snapshot updated from the recorded changes reads no dump. It carries the list of the snapshot it was built from and says so: the page dates it ("as of the full read of ..."). That list is what the source held at that read. A view created since is not in it, and a view dropped since still is, until the next full read.

mydumper writes two files for a view, `<db>.<view>-schema.sql` (a placeholder table) and `<db>.<view>-schema-view.sql` (the view). `bintrail baseline` takes an object for a view only when the second file holds a `CREATE VIEW`, the object has no data file, and the first file is the placeholder mydumper writes. An object with rows is always converted, and so is a real table that is empty. If that file is empty or cannot be read, the run stops and names it.

Naming a view in `--tables` is refused. A dump that holds views and no table is refused too, because there is nothing to copy.

### A folder written by `reconstruct` is not always a dump to convert

`bintrail baseline` converts nothing and exits non-zero for two kinds of folder that `bintrail reconstruct --output-format mydumper` can leave:

- A folder holding a table written with `--allow-changes-only`. That table has only the rows its recorded changes touched. As a snapshot it would be the starting point of every later reconstruct, which would return it as a complete table with no flag and no warning. The error names the table. The run that wrote it listed it in the folder's `_CHANGES_ONLY` file, and its schema file opens with `-- bintrail: CHANGES ONLY.`; either one is enough. To convert the other tables of the folder, name them with `--tables`. For the table itself, take a snapshot from the source (`bintrail dump`, then `bintrail baseline`).
- A folder marked `_INCOMPLETE`. The reconstruct that wrote it failed or was stopped, so tables may be missing or cut short. Run it again to the end into an empty folder.

A dump made by mydumper carries neither mark. See [A table with no snapshot fails the run](query-and-recovery.md#a-table-with-no-snapshot-fails-the-run).

### At-rest integrity (the `_MANIFEST` sidecar)

Alongside each snapshot, `bintrail baseline` writes a `_MANIFEST` sidecar holding a **CRC-32C** over every Parquet file's bytes. Every local read path that consumes a baseline — full-table and single-row `reconstruct`, cascade recovery, the time-travel shim's `_snapshot`, and `query --include-snapshot` — **re-validates the CRC before it reads** and **fails loud** on a mismatch (bit-rot, a truncated/partial write), rather than silently reconstructing from corrupt data. Within one process every reader of a base shares one memo of verified files ([#2517](https://github.com/dbtrail/dbtrail/issues/2517)), except the compaction job (deliberate) and `query --include-snapshot`, which read and hash the file every time. A file is read whole and hashed when it is new to the process, when its snapshot's `_MANIFEST` lists a different digest than the one the memo matched, and when its last check is older than a week; every other check of it reads nothing. The memo names a file by its device, inode, size and modification time: a table carried forward by hard link from one snapshot to the next keeps all four and is the bytes already verified, a table rewritten under its name is a new inode with a new digest in its manifest and is verified again, a table copied across filesystems is new bytes, hashed by the manifest writer that certifies it. A file modified less than 2 seconds before its check is not remembered at all and is read again at its next check, because file timestamps are coarse and a rewrite in the same tick would keep the same modification time. What this does not see, said plainly: corruption that appears on the disk after a check (a sector that rots) changes none of the four, so it is not found until the entry's week is up, the file is rewritten, or the process restarts. A file that nothing changed is read again about once a week, so corruption that appears on disk after a check is found within a week; the week is a constant in this release (0 would disable the re-read for the life of the process). The log says which happened: a read is an Info line, `integrity check read the whole file and verified it`, with the path, the size in MB and the milliseconds it took; a skip is a Debug line, `integrity check skipped: these bytes were verified earlier by this process`, with the path; and every refresh cycle logs one Info line, `baseline refresh: integrity checks this cycle`, with the files read, their MB and the files skipped. After a restart every file is read once more on its first check.

### Upload to S3

Generate and upload in one step:

```sh
bintrail baseline \
  --input         /tmp/mydumper-output \
  --output        /tmp/baselines \
  --upload        s3://my-bucket/baselines/ \
  --upload-region us-east-1
```

See [Scenario I in the Practical Guide](guide.md#scenario-i-uploading-baseline-parquet-files-to-s3) for full S3 setup instructions.

### Retrying after a failure

If a baseline run fails partway through (e.g. network error during S3 upload, disk full), re-run with `--retry` to skip work that already completed:

```sh
bintrail baseline \
  --input         /tmp/mydumper-output \
  --output        /tmp/baselines \
  --upload        s3://my-bucket/baselines/ \
  --upload-region us-east-1 \
  --retry
```

With `--retry`:
- **Local Parquet files**: Tables whose output `.parquet` file already exists are skipped.
- **S3 uploads**: Files that already exist in S3 (checked via `HeadObject`) are skipped.

This makes the command safe to re-run without duplicating work.

### Pruning old local snapshots (`--baseline-retain`)

On a daemon (`bintrail-console watch`), each server's own count bounds its local folder, with or without an S3 destination: see [the count](#every-servers-folder-keeps-its-newest-n) below. This section starts with `--baseline-retain`, the flag of `bintrail baseline` run from cron, which prunes by age. On the daemon the same flag still works and is the old way.

Periodic baselines written by `bintrail baseline` accumulate under `--output` forever: nothing in `bintrail rotate` touches them. On a long-lived host this fills the disk, even though every snapshot is already in S3 and therefore redundant on local disk.

`--baseline-retain` reclaims that space after a successful upload:

```sh
bintrail baseline \
  --input           /tmp/mydumper-weekly \
  --output          /data/baselines \
  --upload          s3://my-bucket/baselines/ \
  --upload-region   us-east-1 \
  --baseline-retain 14d
```

It prunes a local snapshot **only** when all of these hold — pruning never risks data:

- **A durable S3 copy exists.** The snapshot's `_SUCCESS` marker must be confirmed present in S3 at the same timestamp prefix. Without `--upload` (or on a local-only setup) the prune is a deliberate, logged no-op — a local snapshot with no S3 copy is the only copy, and the only copy is never deleted. (This mirrors `bintrail rotate`'s `PruneLocalAfterUpload && --archive-s3` rule.)
- **It is not the newest snapshot for any table.** Time-travel resolves a table to the newest snapshot that contains it (`reconstruct`), so the newest snapshot per table is always kept — even when older than the retention window. Pruning only ever narrows how far *back* local Time-travel can reach, never the present.
- **It is past the retention window** (and at least an hour old; the hour applies to this age rule only, not to a server's count, below).
- **It is complete.** A snapshot mid-write or resumable via `--retry` (an `_INCOMPLETE` marker) is never touched.

Snapshots old enough to go whose copy the S3 destination answered it does not have stay on the local disk. Every completed prune against S3, by age or by count, writes their count to `.not-in-destination.json` beside the snapshots (removed when none is missing; while neither an age nor a count prunes the folder nothing refreshes it, so it is not shown, and a shown count carries the time it was checked), and the web interface shows it on the server's retention card ("Not in S3 yet, so kept here"). Snapshots that could not be checked, because S3 did not answer, are a failed attempt instead and go to `.last-prune-failure.json`.

#### Every server's folder keeps its newest N

A server added in the web interface has a keep-newest count (**Keep by count** on the Snapshots page, `local_keep_newest` in the server list; a server added from the web interface gets 3, with or without an S3 destination). `watch` prunes its folder to that count on every cycle, whether or not `--baseline-retain` is set: for every table, the newest N complete snapshots holding it (dated at or before now) always stay, so a run over a few tables never pushes out the only other copies of the rest, and so do `_INCOMPLETE` snapshots, the `current` pointer's target, the result of the last restore to a moment, and, when `--baseline-retain` is set, anything younger than it. The count is applied after each full read, update and restore that `watch` runs, and at every hourly cleanup. A snapshot folder that holds no file at all, in it or below, for an hour (by the time the folder and its subfolders last changed, not by its name) is removed by any cleanup, in every mode (#2691): every run that writes a snapshot puts a marker in its folder right after creating it, so an empty one is left by a run that stopped in between. Only empty folders are removed, one at a time, so a file written into it meanwhile keeps it; a folder with any file in it, the `current` target and a snapshot being read are left alone.

**With an S3 destination (#2658)** the count names the same snapshots, and each one past it is removed only once its `_SUCCESS` marker is confirmed in the bucket at the same timestamp prefix:

- A snapshot the bucket answers it does not have (not uploaded yet, or an upload that failed) stays, is counted in `.not-in-destination.json`, and the daemon sends it again, up to three per folder per cleanup. One round of re-sends runs per folder at a time, and after a round in which a send failed the next one waits an hour (the page says when it is tried again), with one warning in the log when the sending starts to fail and one line when it works again. The wait is per folder: one snapshot that keeps failing makes the folder's others wait with it.
- When the bucket cannot be asked (no network, expired credentials, throttling), that cleanup removes no snapshot that was sent to S3, not even the ones the bucket had already confirmed, and records the failed attempt in `.last-prune-failure.json`: "The S3 destination could not be asked, so no snapshot that was sent to S3 was removed from this folder in this cleanup."
- A snapshot the schedule kept here on purpose (`.upload-held.<snapshot>`, see [Send to S3 every](#refreshing-on-a-schedule)) was never meant to be in the bucket, so removing it loses nothing that was meant to be durable. Past the count it is removed by the count alone, as on a server with no S3 destination: the bucket is not asked about it, so it goes even in a cleanup where the bucket cannot be asked, and it does not wait for a newer snapshot to reach the bucket. The bucket keeps the last snapshot that was sent, and the disk keeps the newest N. The guarantees above hold for it too: it stays while it is one of the newest N holding a table or the `current` target.
- The mark is what separates the two. A run whose upload slot came and whose upload failed leaves a snapshot with no mark: it was meant for the bucket and stays until the bucket has it. The newest snapshot is always inside the count, so the one whose upload the interval still owes (sent at the next cycle with nothing new to fold) is never removed; if that upload fails it keeps its mark and is tried again at each such cycle, and once a newer snapshot exists, that one is sent in its place.

**The folder holds N (#2678).** No snapshot is kept for being recent: with a 5-minute schedule and a count of 3 the folder holds 3 snapshots, where it held about 13 while an hour was added to the count. Without an S3 destination that is also how far back time travel and a restore reach from this folder: up to N times the schedule's interval (15 minutes at the defaults, 10 right after a new snapshot). Raise the count to reach further, or add an S3 destination: a restore then reads the bucket, back to its oldest snapshot, and a moment between two snapshots there replays the changes recorded since the earlier one.

**A job that reads older snapshots stops the cleanup of its folder while it runs.** A restore to a moment, a `.sql` build, a check (`verify`), a full read's upload and the merge of a snapshot's changes each read snapshots that can be past the count, for minutes or hours, and pick them as they go. While one runs in `watch`, nothing is removed from that server's folder, by the count or by the age; the cleanup after it ends does the work. None of these jobs has a time limit of its own, so the wait has one: a job still running after 24 hours (the longest interval a schedule can have) no longer stops the cleanup. Once the job has run for more than 10 minutes (two of the shortest schedule intervals), the Snapshots page says which job the cleanup waits for and when it started; a cleanup that falls inside a routine update waits for seconds and is not mentioned. A check and a `.sql` build apply the count when they end, like a job that publishes. The record of a wait is believed for twice the cleanup interval (`--rotate-interval`) after it was written.

**A reader of one snapshot keeps that snapshot only.** A snapshot download from the Snapshots page, a SQL statement run by `watch` itself (the SQL page, the MCP `sql` tool) and the Explain of a check each read one known snapshot (Explain, two). The cleanup keeps that snapshot while they run and goes on removing the others past the count, so the folder holds at most the snapshots being read on top of its count. It looks at the readers again right before each removal, so one that started while the bucket was being asked is kept too. Each is kept for a limited time, whatever the reader does: a statement's snapshot for 2 minutes 14 seconds (twice the 60-second statement limit and the 7 seconds the sandbox takes to stop it); a download's for the time its bytes take at 1 MiB/s plus one minute (under 3 minutes for 100 MiB, under 3 hours for 10 GiB); Explain's for 24 hours. A download whose client stops reading is cut after one minute without a write going through, so a connection left open does not keep a snapshot; a slow client that keeps reading is not cut, and past the time above its snapshot is no longer kept, so the download fails if the cleanup then removes it. What the downloads of a server hold back is limited, on the state at the moment each one asks: downloads pin at most twice the server's count of different snapshots in all (twice 3 for a folder pruned by age alone), because a snapshot the cleanup keeps now is past the count a few minutes later; and of snapshots the cleanup would already have removed, at most the count. So downloads never keep more than twice the count on top of it, however many are started, each for no longer than its own time limit. One more is refused with "Try again shortly" (HTTP 429, `Retry-After: 60`). A second download of a snapshot already being downloaded is always let in. A console that prunes nothing (`serve`) and a server that keeps every snapshot have no limit. A statement whose snapshot is removed while it runs is run once more on the newest snapshot. A row lookup at a past moment through `/api/reconstruct` or the MCP `reconstruct` tool whose snapshot is removed while it runs fails with an error and can be run again. Explain on a check reads the snapshots the last check compared; if the cleanup has removed one since, it fails and the check has to run again.

**`_snapshot` for a moment that no snapshot covers is refused.** With a snapshot location configured and no snapshot of the table at or before the moment asked for, in the folder or, on a server with an S3 destination, in the bucket, a single-row query and a full-table query both fail: error 1526 on the MySQL port and on a standalone `bintrail shim`, SQLSTATE 22023 on the PostgreSQL front end. The message says which case it is: the table has no snapshot yet (a server just added, or a table created after the newest snapshot), or the moment is older than the oldest snapshot. Before #2678 the single-row query answered from the recorded changes alone, and a row that no recorded change touched came back as not existing. Ask for a later moment, raise the count or add an S3 destination where there is none, or query `_flashback` for the changes-only view. With no location configured `_snapshot` stays changes-only, as documented in [time-travel-sql.md](./time-travel-sql.md).

**Other processes are not seen.** Each process has its own table of readers, in memory; the lock file (`.prune.lock`) only stops two cleanups of one folder at once. So the cleanup that `watch` runs does not wait for, or keep a snapshot for:

- `bintrail-console router`, the process that serves the MySQL port. A statement through the port keeps its snapshot in the router's memory, which `watch` does not see: its protection is the single retry on the newest snapshot. That matters only with a count of 1; with 3, the snapshot a 60-second statement started on takes 15 minutes to leave the count.
- `bintrail-console serve`, a second `watch` during a rolling deployment, `bintrail reconstruct`, `bintrail export iceberg` and a standalone shim reading the same folder.

A table whose snapshot is removed while one of them reads it fails; run it again, read from the S3 destination, or raise the count. That includes `bintrail reconstruct --output-format mydumper`: a table whose snapshot is gone by the time the table starts, with no older snapshot left, fails the run and is named in the error, instead of being written from the recorded changes alone (that output needs `--allow-changes-only`).

**Who reads S3 for a past moment.** The web interface and its MCP tools read the local folder first and the S3 destination when the folder has no snapshot at or before the moment. A scheduled update on a server with an S3 destination reads the bucket only. A restore to a moment reads the bucket first, and the folder when the bucket holds no snapshot at or before the moment or cannot be reached (#2691): the restore then starts from the folder's newest snapshot at or before the moment, never from a later one, and its result on the Snapshots page says it started from the local folder and why. When the bucket could not be read, the result is not sent in that run: the bucket may already hold a complete snapshot of that second, which the restore could not see, and sending would overwrite it; the cleanup sends it later, skipping anything S3 already has, so a complete copy there is never replaced. A bucket that answers with an older snapshot is still the one read. When neither has a snapshot at or before the moment, the restore fails and its error names both. The MySQL port of the web interface reads the same way as the web interface: the folder first, the bucket for a moment older than the folder's snapshots, read by the process that serves the port (`bintrail-console router`), which needs S3 access of its own. A standalone `bintrail shim` reads the bucket when one is configured and the folder otherwise. The command-line tools read only the location they are given (`--baseline-dir` or `--baseline-s3`).

**A restore's result stays for 24 hours.** A restore to a moment writes a snapshot named for that moment, which is in the past, so by its name it is never among the newest N. It carries a mark beside its folder (`.restored.<snapshot>`), and the cleanup keeps the marked snapshot of the last restore, in every mode, outside the count: the newest N are worked out without it, so a folder with a count of 3 holds 4 after a restore, whatever moment was restored. The mark is written when the restore starts, before its snapshot exists, so a daemon that stops between writing the snapshot and the end of the restore does not leave it unmarked (the mark is written whole, through a temporary file renamed over it, so a cleanup run by another process never reads it half-written; a mark it cannot read keeps the snapshot like a running restore's): while the restore runs, the mark keeps nothing and does not replace the previous restore's, and a restore that ends without writing its snapshot removes its own (one stopped before it wrote anything leaves it, keeping nothing, until its 24 hours pass). It stays for 24 hours from when the restore finished (the time is written in the mark; for a restore that stopped after writing its snapshot, from when it started), or until the next restore, and its row on the Snapshots page says until when ("last restore, kept until ..."). After that the mark is removed and it is an ordinary snapshot: dated in the past, it is past the count and goes at the next cleanup, under the rules of any other (a job or a download reading it keeps it meanwhile; with an S3 destination it goes once the bucket has it). With an S3 destination the kept result is asked about at each cleanup, so one whose upload failed is listed as not in S3 and sent again; a failed question about it never stops the rest of the cleanup. One older than the age S3 keeps is not asked about and not sent: its row says it is on this machine only, and after its 24 hours it goes by the count. The next restore's result replaces it, and the earlier one then goes by the count like any other snapshot. Before this the cleanup removed a restore's result as an old snapshot (after an hour at most).

**Two servers on one S3 prefix are never pruned to a count.** The cleanup asks the bucket for `<prefix>/<snapshot name>/_SUCCESS`, and a snapshot's name is the second its run started, with no server in it. Two servers of one DBTrail on the same schedule are started in the same tick, so they often write the same name; on one prefix the bucket would answer "I have it" for the other server's upload, and a local snapshot whose own upload failed would be removed with no copy anywhere. So a server whose S3 destination is also another server's, or the daemon's own `--baseline-s3`, keeps every local snapshot whatever its count says, and its settings card says why. Give each server its own prefix. (A nested prefix, `s3://b/p/sub/` under `s3://b/p/`, is its own.)

**Servers saved before #2658 keep what they have.** A server with no count keeps every local snapshot. A count saved on a server with an S3 destination did nothing before #2658 (every such server added since #1681 carries a 3 it never used), so the first start of this version sets it to 0 and logs which servers it touched; the registry records that it ran (`local_keep_with_s3`), and a count set afterwards is applied. Set **Keep by count** on such a server to bound its folder.

A folder two servers share, or the daemon's own `--baseline-dir` (compared after resolving symlinks), is never pruned to a count: a snapshot does not record which server wrote it. A save that would point a server with a count at a folder that already holds snapshots is refused, with or without an S3 destination, and a new server added over such a folder keeps everything. `bintrail baseline` run by hand into a folder `watch` prunes is another process: its snapshot is counted at the next hourly cleanup, not when it is published. Each prune that removes anything writes `.last-prune.json` (when, and how many) beside the snapshots; the Snapshots page reads it to say why copies are gone. Two prunes of one folder never overlap: the second steps aside.

#### `--baseline-retain` on the daemon: the old way

`bintrail-console watch` still takes `--baseline-retain` (**Delete by age** on the Snapshots page; env `BINTRAIL_CONSOLE_BASELINE_RETAIN`), runs it on its rotation cadence, and says once at startup that each server's count is the control now. **How the two combine on one server: a snapshot is removed only when every rule set for that server says so, and S3 has it.** So:

- A server with no count (one saved before #2658, or a shared folder): by the age alone, once the bucket confirms the snapshot, as before. This covers the global `--baseline-dir` (against `--baseline-s3`) and every server's own folder (against that server's S3 prefix).
- A server with a count and an S3 destination: past the count **and** older than the age **and** confirmed in the bucket. The age only ever keeps more; it never takes a folder under its count. A snapshot the schedule kept here on purpose goes when it is past the count and older than the age, with no confirmation, since there is none to give.
- A server with a count and no S3 destination: past the count **and** older than the age. The age alone removes nothing there.

The local prune never removes anything from the bucket. Env for the CLI: `BINTRAIL_BASELINE_RETAIN`.

## Step 3 (optional): a newer baseline without a new dump

`bintrail baseline refresh` folds the changes the index already holds onto the newest snapshot and publishes the result as a new one — no mydumper run, no connection to the source:

```sh
bintrail baseline refresh \
  --index-dsn    "user:pass@tcp(index-db:3306)/bintrail_index" \
  --baseline-dir /data/baselines
```

Tables default to every table in the newest snapshot; `--tables` narrows that, and `--at` targets a point in the past. The snapshot lands in `/data/baselines/<timestamp>/<db>/<table>.parquet` with the same `_SUCCESS` / `_MANIFEST` files a converted dump gets, and the next `reconstruct`, `verify` or shim query discovers it as the newest baseline with no configuration change.

When the source snapshots live on S3, read from there and write locally, then publish:

```sh
bintrail baseline refresh --index-dsn "..." \
  --baseline-s3 s3://bucket/baselines/ --output /data/baselines
bintrail upload --source /data/baselines --destination s3://bucket/baselines/
```

The same thing is available a level down as `bintrail reconstruct --output-format parquet --output <baselines root>`, which is what `refresh` runs. Use it when you want to name every table and instant yourself.

**Publication is all-or-nothing.** If any table refuses, nothing is published and the exit status is non-zero, with a per-table verdict:

```
baseline refresh to 2026-05-01T12:00:00Z

  mydb.orders  refreshed
  mydb.users   refused-gap
               └─ reconstruct: stamped capture gap at 2026-04-20T06:00:00Z falls inside …

NOTHING was published: a snapshot mixing refreshed and stale tables under one anchor
would be worse than no refresh. Fix or exclude the tables above (--tables) and retry.
```

A snapshot holding some tables from one point in time and others from another, under a single anchor, is worse than no refresh at all, so a partial run publishes nothing, and `--tables` is the tool for isolating a problem table. A table with no changes is the one exception, and only because it is not really an exception: its rows are identical at both instants, so publishing its previous file leaves it at exactly its own moment, not somebody else's. That is the opt-in below.

Why this matters beyond convenience: reconstructing from a **fresh** snapshot reads a short delta window instead of replaying months of events, so time-travel gets faster and the archive hours the replay depends on stop growing without bound.

**What it refuses, and why refusing beats warning.** A baseline is picked up automatically by every later reconstruct, so a wrong one is not a bad output — it is a wrong answer to every future question.

| Refusal | What happened | What fixes it |
|---|---|---|
| `refused-gap` | The window spans events the index permanently lost, or the index is too old to rule that out | `--allow-gaps` to accept the loss knowingly, or a fresh dump. Acknowledging the loss (stopping and starting the server in the web interface) silences the alarm and does not lift this refusal; one full read after the loss does |
| `refused-ddl` | The table's columns, their declared types, their character sets, or whether they allow NULL changed since the baseline, or a `TRUNCATE`/`DROP`/`RENAME` (or MariaDB's `CREATE OR REPLACE TABLE`) landed in the window, or an `ALTER TABLE` that drops, truncates, exchanges or converts a partition, or discards or imports a tablespace | `bintrail dump` + `bintrail baseline` (in the web interface, a full backup). No flag helps |
| `refused-renumbered` | The source's binary log started again from another numbering since the snapshot (a `RESET MASTER`, a failover, a new `log_bin` name), so the changes since cannot be found by binlog position | A new full snapshot (a scheduled server takes it by itself). No flag helps |
| `refused` | Anything else (no baseline for the table, no primary key, a PK-changing `UPDATE` in the window of a PostgreSQL table; on MySQL and MariaDB such an `UPDATE` is folded as a delete of the old key and an insert of the new one, [#2501](https://github.com/dbtrail/dbtrail/issues/2501)) | Named in the message |

**When the source's binary log starts again from another numbering.** A refresh finds the changes after a snapshot by binlog position: everything at or after the file and offset the snapshot records. A `RESET MASTER` (`RESET BINARY LOGS AND GTIDS`), a failover to another server, or a new `log_bin` name starts the source's binary log again, often at `binlog.000001`, and every change after that sorts before the snapshot's position. Without a check such a refresh found nothing to apply, published the old rows again under a new date, and did the same on every later run. Now it refuses, as `refused-renumbered`, with a message that starts "the changes since the snapshot cannot be found by binlog position" and ends "A new full snapshot is needed." On a schedule the daemon takes that full snapshot by itself, as after any refused update, and the updates after it work again. `--allow-gaps` does not let such a refresh through: nothing was lost that the flag could accept, the positions on the two sides just cannot be compared.

How it is seen: every snapshot written from now on records the newest change the index held when it was taken (its "event mark"). A change that reached the index after that one but sorts before it in the binary log can only come from a binary log that started again. Capture also records when a different server answers at the source's address (a new `server_uuid`); an update from a snapshot an earlier update wrote before that record is refused the same way, even when the new server's files are numbered higher. A full snapshot taken right after the reset or the failover is not refused: its position is in the new numbering, which is where its updates start. Three cases are not seen: a snapshot written before this version that records no mark, until the next one replaces it; and, on MariaDB, which has no `server_uuid`, a failover behind the same address to a server whose files are numbered above the old one's; and a failover to such a server after a full snapshot, before any update from it.

**In the web interface.** The Snapshots page shows the same verdicts, one row per refused table: its name, what happened, the fix and the reason. The first line says how many tables stopped the update and that nothing was published, because one refused table stops the copy of every table. A run keeps up to 20 refused tables by name and counts the rest. A run recorded before this list existed shows its count only. A session with a data profile sees the count and no names.

A table with no baseline is refused rather than degraded to the binlog-only fallback: a snapshot folded from deltas alone would silently omit every row the window never touched.

### Refreshing on a schedule

Every case in which a scheduled run reads the whole database instead of updating, what causes each and what to do about it, is on one page: [When DBTrail reads your whole database](full-reads.md).

`bintrail-console watch --baseline-refresh-interval 12h` (env `BINTRAIL_BASELINE_REFRESH_INTERVAL`) runs the same refresh from the daemon, per monitored server, with no cron. It is **off by default**. The interval accepts minutes, hours or days (`15m`, `6h`, `1d`).

Minutes matter because this interval is, in practice, the ceiling on the freshness of reporting. A table's view (`shop.orders`) sees the world as of the snapshot it was generated against, so at `12h` the tier answers yesterday's questions and at `15m` this morning's. **A views file generated once keeps up with this schedule on its own.** `bintrail views` writes state views that FOLLOW the newest snapshot (locally through the `current/` pointer, on S3 through the newest `_SUCCESS` marker), so each refresh becomes visible to a dashboard reading them, with nothing to regenerate. Two forms are pinned to one snapshot instead, on purpose: a file generated with `--pin-snapshot`, for reproducible analysis against a fixed instant, and the `views.sql` published beside each snapshot. Those stop moving when the next refresh lands, which is what they are for; regenerate them on this schedule if you want them current. Every generated file names in its header which of the two it is. A malformed interval refuses at startup; a daemon with nothing refreshable yet starts and says so, because every tick re-checks: servers added from the web interface later are picked up automatically, and refusing would mean a compose file carrying the interval could not boot a fresh install.

It is **independent of the Read database now button**: the refresh needs neither mydumper nor `BINTRAIL_CONSOLE_BASELINE_TRIGGER=1`, which is the whole point of it. Enabling one does not enable the other in either direction.

**From the web interface, per server.** On a daemon with a baseline feature on (`BINTRAIL_CONSOLE_BASELINE_TRIGGER=1` or `--baseline-refresh-interval`), the Snapshots page can set a schedule for one server (#1442): one of six intervals from 5 minutes to 24 hours (the daily one lined up on a UTC hour). The operator picks when; the daemon picks how, per run, and the page says which it will be: a server with no previous backup yet gets a full backup; otherwise the newest backup is updated from the recorded changes, this same refresh, with no load on the source. When the backups go to S3 that update reads its previous snapshot from the bucket and uploads its result back there, so an S3 destination does not force a full backup every slot. A server with no local folder is not updated, and not read in full either (#2431): every server keeps its snapshots in a local folder, with S3 beside it, and for a server without one the page and the schedule say why no snapshot is written. Updates are no longer built in the working folder, and unchanged tables are no longer copied inside S3. An update that fails (a capture gap, a schema change, too many changed rows, an internal error) falls back to a full backup at the same slot when the daemon may take one (the creation opt-in); otherwise that slot is recorded as skipped with both reasons. **A schedule can also ask for full backups of its own** (#1564): **full backup every** (for example `7d`, at least as long as **every**, empty for none) is a second timetable on the same fixed grid and the same UTC time. At each of its slots the run is a full backup from the source whatever the rule above would pick, so the backups stop resting only on the recorded changes, on the cadence you state rather than whenever an update happens to fail. When it is a whole multiple of **every** (`6h` with `7d`) each full backup lands on a scheduled run and takes that run's place; otherwise some fall between runs and are runs of their own. It needs the creation opt-in: a save asking for full backups on a daemon that cannot take one is refused with the reason, and when the opt-in is turned off later the page says so in red before the next one is due, each missed full backup is recorded as skipped with the reason, and the updates keep running. The schedule can still be edited without removing a full backup that was refused this way; setting a new one is refused (and a save that does not mention **full backup every** at all, from a script or a page loaded before an upgrade, keeps the one saved). A full backup slot that finds another job holding the server is not lost: the next scheduled run takes the full backup instead of waiting a whole **full backup every**, and the card says so while that is the plan. The debt lives in the daemon's memory, so a restart or a save of the schedule in between drops it; the miss then stays on the card until a full backup succeeds, and the next one is the timetable's own slot. Moving the schedule's UTC time moves every full-backup slot, so the timetable starts afresh from that edit. A slot that passed while the daemon was stopped is not made up (no read of production at boot), but it is recorded as missed when the daemon starts. A full backup that did not start, or started and failed, stays on the card in red until a full backup of the server succeeds after it, whatever started that one (the timetable, the **Read database now** button, or the fallback for a failed update): a missed run is made up by the next run, a missed weekly full backup by nothing for a week. The timetable's grid starts at the Unix epoch like the schedule's, which was a Thursday, so `7d` always lands on a Thursday at the schedule's UTC time; the page shows the date of the next one. The page shows the next full backup, and the 30-day count adds the full backups that fall between runs and says how many read the whole database. The page sets it with the **Read the whole database again every** row under the interval (Off, 24 hours, 7 days or 30 days; one saved through the API outside that list shows as a choice of its own). It is stored as `full_every` in the schedule. Every run publishes a full-table snapshot, and local-only backups are never removed automatically: the page shows the 30-day count next to the form and the daemon logs it at save and at boot. It lives on the server's registry entry as `backup_schedule` and the watch daemon reads it every minute, so it applies without a restart. Three rules keep it predictable: the timetable is a fixed grid (`every 1d at 03:00` is 03:00 UTC daily, `every 6h at 03:00` is 03/09/15/21; an interval that does not divide a day evenly drifts, and the page shows where it lands next), a time that passed while the daemon was stopped is not made up (and saving or editing a schedule never starts one on the spot), and a scheduled run that finds another backup job on the server skips that time rather than queuing. Every finished scheduled run and every skip lands in the baseline run history (a run that crashed writes no record; the daemon keeps its outcome for the page until its next run) (a streak of identical skips is one record whose time moves to the latest missed slot), so the page shows the last run, its result and the last skip, restart or not. A per-server schedule and the daemon-wide interval can run side by side; they share the one-job-per-server lock like everything else that produces a backup.

**A schedule can send to S3 less often than it updates** (#2596). **Send to S3 every** (`upload_every`, at least as long as **every**, only for a server with an S3 destination; empty sends every update; a new schedule on such a server starts at `1d`, see below) keeps the updates in between on this machine: a run uploads only when the bucket has no snapshot from the current slot of a fixed grid on the schedule's UTC time, so `every 15m` with `upload_every 6h` sends at the first update of each 6-hour slot. Each update still folds from the newest copy, which is the local one. A snapshot kept here on purpose gets a mark beside its folder (`.upload-held.<snapshot>`), so it is told apart from a failed upload: the full read's sweep and the local prune do not re-send it, and turning the interval off later does not send hours of them at once. On a server with a count, the prune removes such a snapshot as soon as it is past the count, without asking the bucket: those in-between points are given up, and the disk stays at the count. On a server pruned by age alone (no count), it removes such a snapshot past the age once a newer one is confirmed in S3, as before. A cycle with nothing new to fold sends the newest snapshot if it is held and its slot has come, so a quiet source does not keep its latest state off the bucket. If the schedule is deleted or stops while the newest snapshot is held, nothing sends it; the Snapshots page shows how old the newest S3 copy is and says so. The bucket rule check counts and sizes from this interval. **A new schedule sends once a day (#2658).** On a server with an S3 destination, a schedule that did not exist starts with `upload_every: 1d`: the form's row starts at 24 hours, the default 5-minute schedule a new server starts by itself carries it, and `PUT /api/servers/{id}/backup-schedule` without the field takes it (`dbtrail snapshot schedule` sends no field and prints the interval the daemon chose). A schedule that already runs on a server with no S3 destination has no interval, since there was nothing to send; when that server gets its first S3 destination (from the page, `dbtrail snapshot location --s3`, or the API), the same save sets the interval to `1d`, and the page, the terminal and the log say so. A server that already had a destination keeps its schedule as it is, "each update" included, and so does one whose destination moves to another bucket. What a request says wins: `"upload_every": ""` is each update, on a new schedule too. A schedule that exists keeps what it has when a save does not mention the field, so one saved before #2658 goes on sending each update. When **every** is longer than a day the default is not applied: each run is already rarer than that.

**Removing old snapshots from S3** (#2596). The local "Delete by age" never touches S3. A server can also set **Delete from S3 after** (`baseline_s3_retain` on `PUT /api/backup-settings/servers/{id}`, at least `1d`, for a server with an S3 destination; empty, the default, removes nothing). Setting or changing it needs the `settings:write` permission, the tier of the daemon-wide "Delete by age", while turning it off needs only the row's own `servers:write`; a save that moves the server's S3 destination turns it off, so an age set for one prefix never starts removing snapshots from another. The daemon's cleanup loop then removes, at each cleanup, that destination's snapshots older than the age, with the local prune's rules: the newest complete snapshot holding each table stays however old, counted per writer (the `_WRITER.` signature), so one installation's newer copy never makes another's redundant (snapshots with no signature, written before signatures existed or when the index identity could not be read, all count as one writer, as before; a server's unsigned snapshots next to its signed ones are each kept as the newest of their own kind); a snapshot dated after now covers nothing and is not removed; an incomplete snapshot (an upload in progress, or one that failed) is never touched; only folders named as snapshot times are read or touched (archives and other servers' snapshots under the same prefix are not); and a listing that fails, one snapshot folder that cannot be read, or one that reads empty removes nothing that time. One cleanup removes at most 100 snapshots and about 5,000 objects per destination, oldest first, and stops before the next snapshot if a restore or a SQL file build for one of the destination's servers starts; the rest go in the next cleanups. A snapshot is removed so that no moment of the removal reads as complete: a `_PRUNING` and an `_INCOMPLETE` marker are written first, `_SUCCESS` is the first object removed, then the data, then the two markers; a cleanup that stops midway (a refused permission, a network error) leaves a snapshot every reader skips as incomplete, and the next cleanup finishes it; one that still lists `_SUCCESS` covers no table in that cleanup (a store whose listings lag can still show a folder already going); its mark is taken off when the removal had not begun (no `_INCOMPLETE` listed), and the next cleanup weighs it as any other. Servers sharing a destination use the longest age among them, and the destination is not cleaned if any of them has none (moving or removing the server with the longest age lowers it, which `servers:write` can do; shared destinations are a legacy setup the console no longer creates); the destination set at the daemon's startup (`--baseline-s3`) is never cleaned. The age is never shorter than the local "Delete by age", and the S3 cleanup runs after the local one. Nothing past the S3 age is sent to S3 again by the full read's sweep or by the cleanup's resend, except a table's newest local copy, and no send writes into a folder the cleanup is removing. The local prune still never removes a snapshot whose copy S3 does not confirm, so a local snapshot whose upload failed for longer than the S3 age stays on this machine and is listed as not at the destination, to remove by hand. Removing needs `s3:DeleteObject` and `s3:PutObject` on the prefix. On a bucket with versioning, Object Lock included, a removal only adds a delete marker: the snapshot leaves every listing, and the old versions stay, and are billed, until a bucket rule expires noncurrent versions; DBTrail never removes a version for good.

These properties are deliberate:

- **Conservative resources.** The refresh runs with DuckDB's container-safe budget (2 threads / 4 GB), never `--ultrafast`. That flag is for offline commands that own the machine; a background job that self-tuned to ~80% of host RAM would starve the capture path it depends on. It also folds at most **2 tables at a time** rather than one per CPU, because peak memory is the sum of the tables in flight, and it emits the large-window warning at the same raw threshold the CLI ships. Neither is operator-tunable: they are fixed so the daemon's memory does not track the size of whatever host it lands on.
- **A limit on changed rows held in memory.** An update keeps one row image per changed row until it merges. The daemon (the refresh, a scheduled update, a restore to a moment, a `.sql` backup) keeps at most 2,000,000 of them in memory, divided by the tables folding at once. Past that, it writes that table's changes to the system temp directory in 64 groups and merges a few groups at a time, reading the backup it starts from once per set of groups. The snapshot is the same; it takes longer and uses temp disk for the changes, and a disk that fills while they are written is recorded as a disk refusal. It refuses, before publishing anything, only when one group alone passes the limit, which takes about 64 times as many changed rows (about 64,000,000 of one table with two tables folding at once). A build of a table that has no backup at all, rebuilt from the recorded changes alone, still refuses at the limit itself. Then a full backup, which reads the table instead of holding its changes, is the way out, and a restore needs a moment closer to an existing backup. The limit is per run, and each server runs its own, so several servers updating at once each get it. CLI runs have no limit and keep every change in memory.

  **Why the merge is not handed to DuckDB.** DuckDB can merge two tables and spills its own joins to disk, so it looks like the natural tool. But the recorded changes and the backup spell a row's key differently: the index stores the key as text written by the capture (a `DATETIME` as the binlog parser prints it, a `BINARY(n)` without its trailing zero bytes), while the backup stores the typed values `mydumper` dumped. The Go code that turns one spelling into the other, per key type, is what earlier bugs with date keys, binary keys and a row stored under two spellings were fixed in. The changes' values need Go too (`ENUM` numbers and base64 `BLOB`/`TEXT` are decoded against the schema in effect when each change happened). Grouping the changes on disk in Go keeps both translations as they are and changes only where the changes wait. The full reasoning is at the top of `internal/reconstruct/changespill.go`.
- **The events are read in index order.** A refresh reads each table's window off the index's `(schema, table, timestamp)` entry in the order it folds them, oldest first, page by page, and picks up each row as it goes. It does not do what the other readers do, which is to sort the page's keys first and then fetch each row by primary key: under load, with the index's pages pushed out of memory by the capture, that fetch was one disk read per row (#1720, about 1.7 ms each and 100,000 per page). On an index written by `bintrail stream` or the daemon, each file a refresh writes records the highest event id it applied, and the next refresh reads no row below that id. The scan still starts the hour before its anchor, the safety margin for a transaction that executed before the anchor and committed after it; the id only lets the rows of that hour the previous refresh already applied be skipped in the index instead of read. An index built with `bintrail index --files` gets neither the stamp nor the floor: there, ids follow the order the files were given, which need not be the binlog's. Every per-table log line (`table reconstructed`, `table carried forward unchanged`, `table published as a delta` and the rest) carries `fetch_ms` and `fold_ms`, so a slow table says whether it waited on the index or on the fold, including a table that waited and changed nothing. The delta line also carries `lookup_ms` and `lookup_rows`: the time spent locating, in the table's file, the rows the window changed or deleted, and how many rows of that file the lookup examined (the matches when the keys were handed to DuckDB, every row per pass when they were read through Go), summed over the passes of a window that went to disk.
- **The run's own line says whether the machine slowed it down.** Every published refresh logs `baseline refresh: published` with `took` (the whole run, upload included), `lane_wait` (the part spent waiting for the snapshot lane, behind another server's fold or a check), `process_waiting_for_cpu` and `process_waiting_for_disk`: the share of the run during which the process had a task ready to run but no free core, or a task waiting on storage, read from its cgroup's stall counters (Linux pressure stall information, `cpu.pressure` and `io.pressure`). They count everything in the process's cgroup over the whole of `took`, not only this refresh: with `watch`, capture and the console; another server's refresh running at the same time; and, during `lane_wait`, the fold this run was waiting behind. They do not see an index MySQL in another container: a fold waiting on a starved index waits on a socket, which neither counts (a MySQL in the same container, as in the demo image, is counted). So a slow run with both shares low points away from this process, to the index or to where the baseline is read from; the per-table `fetch_ms` then says which. Only the process's own cgroup is read: where the counters cannot be read (cgroup v1, a kernel without pressure stall information or with it switched off for the cgroup, a cgroup that does not list the process, a host that is not Linux) the value says `unknown:` and why, never `0%`, and never the machine-wide counter in its place.
- **A key index is written beside every table file a conversion or a rewrite produces** (#2496). `<table>.keyidx` sits next to `<table>.parquet`, in the same folder: every row's integer key with the row's number in the file, sorted by key, in a flat fixed-width file that can be searched without being read whole. Its header names the key columns, in the order the file itself holds them (the converter stores a table's columns in alphabetical order, so that is neither the table's column order nor the `PRIMARY KEY` line's): a conversion, which reads the key off the dump's `PRIMARY KEY` line, and a refresh, which reads it off the schema snapshot, write the same bytes. A record is the key columns followed by the 8-byte row number, so its width is `(sum of the key columns' widths, 4 bytes per INTEGER and 8 per BIGINT or BIGINT UNSIGNED) + 8`: 12 bytes per row for one `int` key, 16 for two, 20 for an `int` plus a `bigint`; the whole file is that times the rows plus one entry of the key's width plus 4 bytes per block of 4096 records, so a 60 M-row table on four `int` columns carries a 1.4 GB index and a 20 M-row table on two a 320 MB one. It is tied to its file by the file's size, its footer's checksum, its row count, its inode and its modification time (not by `_MANIFEST`, which lists the data files only; the footer alone would still match a file rewritten in place with the same rows in another order, which is the one case an index must never answer), is written to a `.tmp`, flushed to disk and renamed, and every block of 4096 records carries its own checksum. **What gets no index**, each said in the log: a table whose dump declares no `PRIMARY KEY` (or whose snapshot has no key), a key that is not integer-typed in the file (a text, date or decimal key), a key column holding a NULL, a file on S3, and a folder with less free space than the index plus a gigabyte (the sort's own scratch space, about the index's size again, is counted too, in the DuckDB spill directory or added to the folder's when the two are one filesystem). The table is published all the same: an index is an accelerator, never something a refresh depends on; and a build that writes none removes any older index beside the file, so a stale one never sits beside a rewritten table. Indexes are built one at a time per process, however many tables the conversion converts at once, so a backup never opens one DuckDB sort per CPU. `bintrail baseline --retry` builds the index for a table it skips when the index is missing or belongs to another version of the file. `bintrail upload` leaves the index (and a build's `.tmp`) local: nothing reads it from a bucket. The line `key index written` carries the table, its records, bytes and `build_ms`; building the index for a 10 M-row `int` key takes about a second. The refresh that reads it is the next bullet; a table published by carrying its previous file forward (a hard link) carries the index with it, so every refresh of a chain uses it, while a file that had to be copied (another filesystem) gets none from the carry and none from a refresh either (a copy is a new file every time, so a build beside it would be paid at every refresh): it is read through DuckDB until the snapshot folders share a filesystem. `bintrail reconstruct --output-format parquet` writes a discoverable snapshot through the same writer and gets the index too.
- **A refresh resolves its changed keys against that index instead of scanning the table's file** (#2496; on by default since slice 4). The position lookup of every refresh with table deltas opens `<table>.keyidx` beside the table's file, checks that it is that file's (size, footer checksum, row count, inode, modification time) and that its key columns and their types are the table's, encodes the window's keys the way the index stores them and reads only the blocks of 4096 records those keys fall in, each once. Measured on a 4-core host under a write load: the lookup of a 61 M-row table went from 2.0 s to 0.22 s per refresh, 10 to 30 times faster on 6 M-row tables, every lookup through the index. The index's answer is used as it is: no read of the file and no canonical check follows it. What protects that answer is the index's own checksums (every block, and the header with the block table), its binding to that very file, the name and type checks, and a guard on the key's snapshot metadata (a key column the snapshot says is a string type while the file stores an integer, the column's type changed after the file was written, is left to the join, which refuses it as before); the tests pin the index's rows to the scan's on every shape (negatives, the extremes of every integer width, composite keys whose file order is not the key's, duplicates, keys absent or out of range, block boundaries, a window read back from disk in several passes) and end to end. **A table whose file has no usable index gets one built at its next refresh, once.** No index beside the file (a snapshot from before the index existed, a file that was copied rather than linked when carried forward, a build that was skipped at the time) is one Debug line and the lookup builds the index there and then, with the same writer the conversion uses, and resolves through it in the same refresh; an index that does not read (a checksum, or records not in order), belongs to another version of the file, or names other key columns or types than the table's is a WARN line saying which and that it is being rebuilt now, the damaged file is removed and a new one built. The build costs what it costs the conversion: about 15 s and 1.5 GB for a 61 M-row table, 26 s and 2.2 GB for nine tables of a TPC-C schema at once (one build at a time per process, the others wait), paid once per version of the file, so the refresh that hits it is that much longer and says so: its per-table line carries `lookup_index_built_ms=` beside `lookup_ms`, and only then. A build that produces nothing (a key that is not integer-typed in the file, a key column holding a NULL, too little free disk, a stale index that cannot be removed, an index that is unusable again right after being rebuilt) is said in the log with its reason and with the retry condition ("not tried again until the table's file is rewritten or the daemon restarts"), the refresh falls back to reading the file through DuckDB as before, and the build is not tried again while the file keeps its identity (size, inode, modification time: a file carried forward by hard link keeps all three); the next rewrite of the table is a new file and gets one new attempt. A build cut short by a shutdown is tried again at the next refresh. An index that cannot be checked because the table's file itself could not be read (a permission, an I/O error) is left as it is: one warning, nothing removed, nothing remembered. A file carried forward by copy (snapshot folders on two filesystems) is a new file at every refresh, so no index is built beside it: it is read through DuckDB until the folders share a filesystem. A key column the schema snapshot says is not an integer type is left to today's lookup, which refuses it; a key in the window that is not spelled as a plain integer is the same fallback the join takes today. The per-table line says which path resolved the lookup, `lookup_path=index` (or `join`, `scan`, and `mixed` when a window read back from disk in several passes took different ones), and `lookup_blocks=N`, the index blocks read, beside `lookup_ms` and `lookup_rows` (on the index path, the rows that matched). **Turning it off**: `BINTRAIL_KEY_INDEX=0` on the daemon (`false`, `no` and `off` work too, in any case) keeps the DuckDB lookup and builds no index at a refresh (the conversion and the rewrite still write one beside every file they produce); unset, `1`, `true`, `yes` or `on` is the default, and any other value is the default with a warning naming the accepted spellings. In the compose stack it is `KEY_INDEX=0` in `.env`. The switch is read from the process environment once, when the daemon builds its snapshot supervisor at boot, and says what it read in one line: `key index lookup enabled by default: ... set BINTRAIL_KEY_INDEX=0 to scan the file instead`, or `key index lookup disabled by BINTRAIL_KEY_INDEX; every refresh scans the table's file for its changed keys`; a daemon started with `--baseline-refresh-interval` also carries `key_index=true|false` on its `baseline refresh loop enabled` line. The process environment is the daemon's: a value in the compose file's `environment:` block reaches it, and so does one in the env file (`.bintrail.env` in the working directory, or `~/.config/bintrail/config.env`), which `bintrail-console watch` loads into the process environment before it reads anything.
- **Where the scan starts is read from the index, not from a clock.** A refresh continues from the exact binlog position its previous snapshot stopped at. To avoid walking the whole index it also bounds the scan in time, and the natural bound, the time the previous snapshot was written, is the wrong clock: the index files each event under the time its statement ran on the source. When the index receives events later than they ran (capture catching up after an outage, a source that is itself a delayed replica, a transaction left open for hours), those events come after the position and hours before that time. So before reading, a refresh looks at the newest event of every hourly partition older than its time bound (a few statements for all of them, once per run). A partition whose newest event comes before the snapshot's position holds nothing new and stays out; one whose newest event comes at or after it may, and the scan starts at the table's oldest event there. The bound only ever moves earlier. The log line `the fetch starts earlier than the time of the snapshot it continues` names the table and both times when it does. The look is per hour, not per table: one late event of any table makes every table with events in that hour start there, and the exact position discards the rest. Three limits. (1) While `bintrail index` adds binlog files to an index a stream also writes, the newest event of a partition says nothing about the others, so for a snapshot older than that run every older partition counts as one that may hold something new and the scan can start at the table's oldest event in the index (slower, never less). A run recorded as unfinished counts as still going; if it crashed, the daemon warns every ten minutes with the file and the statement that marks it as ended (`UPDATE index_state SET status = 'failed', completed_at = UTC_TIMESTAMP() ...`), which keeps what it had indexed. Do not run `bintrail index` on that file again: it starts from the file's beginning and indexes the same events twice. (2) Archives count too. When rotation archives an hour, it records in `archive_state` the newest binlog position the file holds (`max_binlog_file`, `max_start_pos`, plus `max_event_id`). An archive whose newest position is at or after the cut of the refresh that published the snapshot moves the start back to that archive's hour (or to its oldest event, when the file holds events older than its hour), so a late event whose hour was archived and dropped before the refresh ran is still read, filtered by position like the index. The cut, and not the table's own position, because a table with no changes keeps its old position from refresh to refresh: every event indexed after a refresh ran is after its cut, and that refresh already looked at the rest, so a quiet table reads no archive in the steady state. Each snapshot directory records how far each of its tables has been checked, in an `_ARCHIVE_CHECKED` file beside `_SUCCESS`: the run's cut when the run checked the archives itself (an index only a stream writes, archive sources found, no `--allow-gaps`, an index that holds events), otherwise the value each table was read with, unchanged, so a run that could not check never lifts a table to another table's position. Without a value (an index `bintrail index` also wrote, which indexes old positions late; a snapshot written by an older build, by a dump, or kept in S3; a value ahead of the update's own cut or on another binary log base name), the refresh instead reads the archives after the table's own position that were written after the snapshot's time, less one hour, and on the backfilled index it says so once in the log. A partition archived again after a failed S3 upload counts as written at the second attempt. `archive reconcile --repair` and `restore-index` register a file without a newest position, so every refresh of a snapshot taken before that, or less than an hour after, reads it. An outage longer than the index retention no longer needs a new full snapshot, as long as the hours it covers were archived. An archive with no record (written by an older build, or registered without reading the file) is read when it was written after the snapshot, less one hour for clock differences, and ignored when written earlier, since it cannot hold a change the snapshot has not seen. An archive is not narrowed to the table being refreshed: when one qualifies, every table's refresh starts at its hour. A moved start is paid in reads: the index and every archive from there, once per refresh that finds a late event, and on every refresh while an index run is unfinished. (3) Hours older than the index's first hour cannot be checked: a late event that old is kept in the first partition and read there, but the refresh cannot tell an hour that never had a partition from one rotated out without an archive, so it logs one warning (how many hours, from when to when) instead of refusing, and an event rotated out without an archive stays lost. If any of these lookups fails, the refresh stops for that table and publishes nothing.
- **Never `--allow-gaps`.** An unattended job must not publish a knowingly-incomplete baseline: accepting a permanent capture loss has consequences for every future reconstruct, and nobody is watching this one to make that call. A gapped window is refused and retried next interval.
- **Isolated from capture.** A failure is logged and retried; it can never take down the stream or the supervisor. That includes an internal error inside the refresh itself, not only one it reports. An error while rebuilding a table is recorded against that table, the other tables carry on, and the run fails without publishing; an error anywhere else in the job marks the run failed. Either way the stack goes to the daemon log, and the other backup jobs that share this server's lock (a manual backup, a restore, a custom `.sql` build) stay available. The one gap left: a manual backup runs `mydumper` and converts its output table by table, and an internal error inside one of those conversions still stops the daemon. A baseline that stopped refreshing is a degradation, a daemon that stopped capturing is an outage, and the first must never cause the second.
- **The daemon-wide interval uploads nothing itself.** `--baseline-refresh-interval` names no destination, so its snapshots are written locally only. On a server with no count and no S3 destination nothing removes them, and they accumulate. On a server with a count and no S3 destination they are pruned to the count like any other. On a server with an S3 destination, a count of its own and a per-server schedule, the snapshots this loop writes are marked as kept on this machine on purpose, like the updates the schedule's upload interval keeps back ([#2675](https://github.com/dbtrail/dbtrail/issues/2675)): the count removes them by itself, the cleanup does not send them, and the schedule's next due upload sends the newest local snapshot, this loop's included. On a server with no schedule of its own nothing would ever send a held snapshot, so they are not marked, and the bucket keeps receiving them the way it did: the cleanup sends them, up to three per folder per cleanup, which is a cost of this flag on such a server (every server added with a destination since the single count has a count). Prefer a per-server schedule there. On a server with an S3 destination and no count (or a count that does not apply, because the folder or the prefix is shared), nothing marks them either: the prune by age removes a snapshot only once the bucket confirms it, finds these missing there, and the daemon's cleanup then sends them, up to three per folder per cleanup, like any snapshot whose upload did not happen. Prefer the per-server schedule on such a server, which uploads on its own interval and marks the updates it keeps back. A snapshot whose upload failed is complete on disk and absent from the bucket: the cleanup keeps it, sends it again at each pass, and removes it once the bucket has it; a full read's sweep sends it too. Where neither a count nor an age prunes the folder, nothing sends or removes these snapshots: upload and prune them on your own schedule (`bintrail upload --source <baseline-dir> --destination s3://…`), or size the disk for the rate the interval implies. **Read that rate before choosing a short interval**: it is one full-table snapshot per monitored server per tick, so `1h` is 720 a month per server and `5m` is 8,640. The startup warning prints that projection for the interval you set, over 30 days because that is the horizon a disk fills on, rather than leaving the arithmetic to be discovered from a full disk. A run that publishes **nothing** does not add to that rate: a refusal folds every table it can before it reports, so it leaves a near-complete snapshot directory behind, and the loop removes the one its own failed run wrote. The daemon log names that directory either way. It removes only what it can prove that run created, and keeps the directory (saying which case it hit, and where it is) when the directory already held files before the refresh started, when the run failed with every table folded and only the final marker or integrity manifest missing, or when the directory no longer carries the `_INCOMPLETE` marker. A run that fails before its first table rebuilds (an unreachable index, a missing schema snapshot) leaves the directory holding nothing but that marker, and those are reclaimed too. The removal moves the directory aside under a `.<timestamp>.discarding` name before deleting it, so an interrupted delete can never leave table files with no marker on them; the next refresh cycle for that backup directory sweeps any staging directory it finds, so a daemon killed mid-delete does not leak one for as long as that directory is still being refreshed. One left behind in a directory the refresh no longer runs for (the server was removed, or the interval was turned off) stays until you remove it. A refresh you started yourself with `bintrail baseline refresh`, a point-in-time restore, and a custom `.sql` build all keep their fragments: you are there to look at them.
- **A DELETE removes the row from the snapshot, and the row is kept in the event log.** The refresh applies a DELETE the way the table did: the row is gone from the new snapshot, and from the table's view built on it. There is no deleted-marker column, which is what a warehouse connector would give you instead. The DELETE itself stays in the index and in the archived Parquet with the row's full before-image, so what was removed, and when, is a query over the `events` view (see [Parquet debugging](parquet-debugging.md)), and a table as it stood before a purge is `bintrail reconstruct --at` with a baseline from before it. A snapshot that kept every row ever deleted would also cost a full rewrite of a growing table on every refresh, which is the cost this loop already pays for tables that change.
- **A table with no changes is left as it is (on by default since #1681).** When the delta window held no events for a table, its previous Parquet file is published into the new snapshot as-is (a hard link where the filesystem allows one, a copy otherwise) instead of being folded and re-emitted. It does not apply when the previous snapshot is read from S3 into a local folder: linking a file needs both ends on a filesystem, so those runs take the ordinary path and the daemon log says so. With table deltas on (the default), an unchanged table and the pair beside it are kept whether this setting is on or off: it governs only tables without table deltas. A per-server schedule on an S3-backed server reads the bucket, with one exception (#1626): when the server's local directory already holds the bucket's newest snapshot, table for table, which is the normal state after the first run (the fold writes locally and uploads, and retention never removes the newest snapshot), the run reads that local copy and unchanged tables are reused; an older or partial local copy keeps the bucket. Each table's file carries its OWN binlog anchor, so an untouched table's anchor still points exactly where its deltas resume. The rows are identical either way; the on-disk representation is not: two snapshots end up sharing one file, so the prune pass reports space it will not reclaim while the newer snapshot still references it (a `du` run per snapshot directory double-counts it too; one `du` over the baseline root reports the truth). That is a reporting cost, not a correctness one, which is why it is on by default: a destructive DDL or a stale schema snapshot refuses the backup before reuse is reached; a known capture gap (including under `--allow-gaps`, which reports and proceeds) makes that table ineligible, so it is folded and written the ordinary way; and a failed `_MANIFEST` check on the file being reused fails the run rather than copying a corrupt file forward. The run summary calls a reused table `unchanged` rather than `refreshed`, and the web interface counts them separately, because which tables actually cost a full rewrite is the number worth seeing. A `TRUNCATE`, `DROP` or `RENAME` emits no row events either, which is why the destructive-DDL refusal runs first and this can never republish rows a truncate deleted.

  | Where | How |
  |---|---|
  | `bintrail baseline refresh` | `--carry-forward-unchanged` (opt-in on this one-shot command) |
  | `bintrail reconstruct --output-format parquet` | `--carry-forward-unchanged` (opt-in) |
  | `bintrail-console watch` | on by default; `--baseline-carry-forward-unchanged=false`, or `BINTRAIL_BASELINE_CARRY_FORWARD_UNCHANGED=false` (a true/false value: `1`, `true`, `0`, `false`; anything else keeps the default, which is now ON), turns off THIS path — with table deltas on (the default, below) the previous file is still linked forward, so writing every table again needs both off |
  | Web interface | nothing to set (#1681); a setting saved by an older version is ignored. The saving only applies to a server that keeps a copy on this machine, which is the per-server question under **Where and how often** on the Snapshots page |

- **A table that changed is left as it is too, with its changes written beside it (on by default since #1729; #1638, #1718).** Turned off, a refresh rewrites every table that changed in full, however little of it changed. On, the table's previous Parquet file is published into the new snapshot untouched (a hard link, as above) and each refresh writes ONE new pair of small files next to it, numbered in sequence:

  ```
  <snapshot>/<schema>/<table>.parquet          the table as it was when the chain of deltas started
  <snapshot>/<schema>/<table>.000000.posdel    the chain's start: an empty pair, written by the full backup
  <snapshot>/<schema>/<table>.000000.upserts   or the full rewrite the table file came from
  <snapshot>/<schema>/<table>.000001.posdel    refresh 1: row numbers of rows in the table file that this refresh replaced or removed
  <snapshot>/<schema>/<table>.000001.upserts   refresh 1: the rows it changed or added, and a tombstone for each row it deleted
  <snapshot>/<schema>/<table>.000002.posdel    refresh 2, and so on
  <snapshot>/<schema>/<table>.000002.upserts
  ```

  All the small files are Parquet; they carry another suffix so that nothing that lists `.parquet` files can take one for a table. The table's state at the snapshot is the table file minus every row number named in any `.posdel`, plus, for each key that appears in the `.upserts` files, its newest version, unless that version is a tombstone. A `.upserts` row carries two technical columns besides the table's own: `bintrail_pk`, the row's key as one string, and `bintrail_op`, `u` for a current row or `d` for a tombstone. A refresh links every earlier pair into the new snapshot and writes only its own: what it writes grows with the rows changed since the previous refresh, never with the chain (the layout in v0.83.0, one pair per table rewritten on every refresh, cost 28 µs per accumulated row per refresh under load, and the cost grew until the chain ended; see #1718). A refresh whose window touched nothing writes no pair.

  The table is written again in full, and the chain starts over with an empty `000000` pair, when the chain's files together pass half of the table's file (tables whose chain is under 1 MiB are left alone), when the chain's changes (its upserts files together) pass 24 MB (at the default 2 GB of SQL memory, in proportion to it; the `watch` daemon only, read at each refresh, so a memory saved in the web interface moves it on the next one), so a query over the table reads a short chain from the newest copy, when the chain is a day old, when the chain's start comes close to the oldest events the index keeps (the `watch` daemon only, see [rotation-and-status.md](rotation-and-status.md)), when the run went over a known capture gap, when the previous snapshot is read from S3 and the table changed, when the table has a column named `bintrail_pk`, `bintrail_op`, `filename` or `file_row_number`, or when the previous snapshot was written by v0.83.0 (its one pair is folded in, once). A schema change refuses the refresh exactly as it does without this option, and the full backup that follows starts a new chain. With the option on, every table in the snapshot has at least the `000000` pair. A window that changed more rows of one table than the refresh holds in memory (1,000,000 in the `watch` daemon) does not end the chain: the changes go to disk and the pair is written from there in passes. Each pass reads the key columns of the table file, so such a window costs more than one that fit in memory and far less than writing the table again.

  **A long chain is merged by the daemon (#1723).** Once a chain lists 16 entries (16 plain pairs the first time; after that, a range plus 15 pairs), `bintrail-console watch` runs a compaction job right after the refresh has released the server's slot, never inside the refresh's own time: a DuckDB-only merge of all but the last pair into ONE range pair, `<table>.000000-000014.posdel` / `.upserts` (the union of the dead positions; the newest version of every key, tombstones included), whose footer is the last merged pair's with the two ends added. The result waits under `<backup dir>/.compact/<schema>/<table>/<chain start>/` and the next refresh links it forward in place of the pairs it merged, so from that snapshot on the chain reads `000000-000014, 000015, 000016, ...` while older snapshots keep their plain pairs; the state read through the range is the state read through the pairs, and every reader (the views, the web interface, `verify`, restore) takes a range like any pair. The table file is never touched by this job. Every input pair is checked against the snapshot's manifest first; a result the refresh cannot adopt (another chain or base, footers that do not name its range) is removed with a warning, an unfinished one (a daemon that died mid-merge) is swept at the next refresh cycle, and a refresh that finds none carries the chain forward as before. The job shares the server's single-flight with every other backup job. It is recorded in the daemon log and in the on-disk run history file (`kind: compact`; a failure that repeats is one record whose end moves); it does not appear on the Snapshots page, which lists the runs that produced a snapshot, and a daemon shutdown mid-merge is logged, not recorded. A snapshot holding a range pair must not be read by a bintrail older than the one that wrote it.

  **The newest snapshot's chains are also merged for statements (#2231).** A statement over a table with a chain has to find the newest version of each changed row, an aggregate and a join it pays every time. So right after each refresh, outside the refresh's own time and outside the server's job slot (a server's first refresh after `watch` starts is the exception: it writes the pairs the newest snapshot lacks before it does anything else, #2261), `bintrail-console watch` merges every chain of two pairs or more in the newest snapshot, the last pair included, into one range pair kept under `<backup dir>/.resolved/<snapshot>/<schema>/`. SQL on the copy and the MySQL port read the table file and that pair when it is there, and the chain as before when it is not. Measured on a 20M-row table with 1.03M changed rows (DuckDB 1.4.5, 2 threads, the same rows read both ways): a count and a sum went from 423 ms to 323 ms and a lookup by key from 80 ms to 31 ms; merging that pair took 0.7 s and under 0.5 GB. It is a cache beside the snapshots and never part of one: no snapshot, manifest or retention rule sees it, an upload leaves it out, and a snapshot still holds links to its earlier pairs and only its own window. The pairs of the two newest snapshots are kept, since a statement that started on the previous one is still reading its pair; those of an older snapshot are marked when it leaves the two, which stops them being handed to new statements, and are removed 15 minutes later, not at once, because a statement names a pair before it opens it. A table with no change in a refresh keeps its pair as a link, not a second merge. Before the merge reads a file of the chain, the file is checked against the snapshot's manifest when the manifest lists it. A table that could not be merged (no room, a file that fails that check) is a line in the log, is read through its chain, and is tried again an hour later; the wait ends early when the table is written again in full. A table read through its pair does not count toward the limit on unmerged changes a statement may read (#2239). The job and a compaction of the same server stand aside for each other (they merge nearly the same files, each in its own DuckDB session): a compaction writes the pairs when it ends, and a compaction that found the pairs being written is started by the refresh after. **A table read through its pair is written in full less often (#2261).** A refresh ends a table's chain, and writes the table again, once the changes beside it pass a fixed size (24 MB at the default `--sql-memory` of 2 GB, scaled with it), so that a statement never merges a long chain. That size does not look at the table, and an 80M-row table was written in full as often as a small one, each time for minutes. A table whose chain had its pair at the previous refresh is not held to that size: its chain ends when its files pass half the size of the table's file, or when its changes pass 192 MB (at 2 GB, scaled the same way), whichever comes first, or when it is a day old. The 192 MB is half the size past which a statement is answered from an earlier copy, and covers a statement over one such table that arrives before the pair of the newest snapshot is written; a statement that joins several of them in that moment can add up to more, and is then answered from the copy before, which has its pairs. A table whose pair could not be written is back on the fixed size at the next refresh, and a chain of a single pair, which has no merged pair, is always on it (the first window after a table gets its first chain; a table written in full starts its next chain with an empty pair, so its first window already makes two). Views files for DuckDB clients read such a table through a longer chain than before, as do `reconstruct`, a restore and the next full write of the table, and the pairs under `.resolved` grow with the chains they copy. Views files for DuckDB clients never read these pairs, and a server whose snapshots this daemon only reads from S3 (no local folder) has none.

  **Who reads the chain.** The generated DuckDB views do: a table's view over such a table reads the table file and every pair of the chain, so it shows the snapshot's rows. A query through it costs more than a query over one file, because the big file is read with its row numbers; on a 40M-row table a full-table sum went from 0.03 s to 0.12 s in the measurement on #1638. Everything else (`reconstruct`, `recover-cascade`, the shim's `_snapshot`, `verify`, `drill`, a restore, the Iceberg export) keeps reading the table file and the index, and stays correct because the index still holds every change since that file was written. Those readers fetch a longer window for such a table, up to the age of its chain, which is why the chain ends after a day. `verify` compares table files: for a table whose newest backup is its previous file plus a pair there is nothing for it to compare, so it reports that table as `inconclusive` and says why, and verifies it again the next time the table is written in full. The backup-age verdict in `status` and in the web interface is graded on the snapshot's time, not on the start of the chain, so with a retention shorter than a day it can read `ok` for a table those readers can no longer rebuild; they refuse with a coverage error in that case, they do not return wrong rows.

  **Two things to know.** A snapshot written this way must not be read by a bintrail older than the one that wrote it: an older build takes the snapshot's directory time as the start of its event window, and for a table with a pair the window has to start where the chain did, so it would skip changes without an error. And the chain has to travel with its table file. The daemon's own upload and `bintrail upload` both carry it, and the snapshot's `_MANIFEST` covers it; a copy made some other way that takes only `*.parquet` leaves a table file that is older than its directory says, and a reader of that copy skips changes without an error. Uploads still send the table's file and every pair again under each new snapshot prefix, so this saves local writes, not upload. If a pair is lost or damaged, at any sequence, the refresh sets the whole chain aside and rebuilds from the table file, and the other readers keep working from the time that file was written. Views generated by v0.83.0 that follow the newest snapshot guard against a `<table>.upserts` appearing beside a table, not against the numbered files, so generate them again after upgrading.

  A full backup the daemon takes while this is on writes an empty pair beside every table, so the layout does not change from one snapshot to the next.

  **Turning it off needs nothing else.** The next refresh reads each table file and the index, writes every table in full and writes no pair, and the snapshot after it has the plain layout again. **The DuckDB views need nothing either.** A view that follows the newest snapshot reads each table file with whatever chain is beside it, none included, so it keeps working when a pair appears, when a table is written in full with no pair, and after this is turned on or off. Two kinds of table are the exception, and the generated file names each one above its view: a table whose schema could not be read from its file, and a table whose name, followed by a dot, starts another table's name in the same schema (`orders` next to `orders.old`). Those read the table file alone and refuse once a pair appears, until the views are generated again. Views generated by an earlier release do not: one generated while a table had no pair refuses once a pair appears, and one generated while it had a pair fails with "No files found" once the pair is gone. Generate those again once. The `views.sql` published inside each snapshot, the download in the web interface and its SQL panel are generated per snapshot and are always right.

  | Where | How |
  |---|---|
  | `bintrail baseline refresh` | on by default; `--table-deltas=false` turns it off |
  | `bintrail-console watch` | on by default; `--baseline-table-deltas=false`, or `BINTRAIL_BASELINE_TABLE_DELTAS=false`, turns it off (a value that is neither true nor false keeps the default) |

  **Upgrading from a build where it was off** needs nothing: the next refresh starts a chain beside each table that changed (the table file is kept, an empty pair 000000 and this refresh's pair are written) and the daemon log says so once per table; a snapshot from v0.83.0's single-pair layout is compacted once with its changes, as above. Generate the DuckDB views again, for the reason below.

- **An interval shorter than a refresh is a request, not a schedule.** A refresh rewrites every table that changed in full, however little of it changed, so it has a cost the interval cannot go below. Asking for less does not queue refreshes up: a server whose previous refresh is still folding is skipped for that tick, and the tick says so.

  Each refresh also logs its own duration, and one that outran the configured interval says so explicitly, naming the server. Read that line before reaching for the interval, because it distinguishes two cases that look alike:

  - **It finished inside the window of changes it folded.** The refresh is slower than you asked for but not slower than the source. Raising the interval to at least the duration on the line stops the skipped ticks, and nothing else has to change.
  - **It took longer than that window.** Each run then hands the next one a larger window. If that keeps up across two published runs, with the run growing by more than its window did, the line says the backup is falling further behind and a longer interval will not fix it, because the next refresh cannot start until this one ends and it inherits everything that arrived meanwhile. The way out is a full backup, which reads the source instead of folding and re-anchors the refreshes that follow to the moment it began, plus reducing what a run costs. The line names the slowest table, how long the upload took, and which of the two grew.

It shares its single-flight with the web interface's **Create baseline** button, so a refresh and a dump never run against the same server at once. The last outcome per server shows up on the web interface's **Snapshots** page and in `GET /api/baselines`.


This interval loop skips a server that has an S3 destination and no local folder, with a warning: a refresh writes into the local folder. Every server keeps one since #2431, and `watch` gives a server saved without one its default folder when it starts, so this is a server it could not give one; give it a Local folder on the Snapshots page. A server that has both is refreshed normally, into its local folder, and a per-server schedule there reads its previous snapshot from the bucket.

**A knowingly-gapped snapshot stays marked forever.** `--allow-gaps` exists because sometimes the incomplete result is genuinely what you want — but the snapshot then carries a `bintrail.capture_gap` line in its Parquet metadata naming what was lost, and **every snapshot later refreshed from it inherits that line**. The missing events stay missing down the chain, so the record does too; one refresh can never launder a knowingly-incomplete baseline into a clean-looking one.

**When the source was last read, inherited too** ([#1570](https://github.com/dbtrail/dbtrail/issues/1570)). A refresh chain can go a long time without reading the source, and nothing in one snapshot used to say how long. Every table file now carries `bintrail.last_dump_at` (the instant of the newest real dump its rows descend from; a dump stamps its own) and `bintrail.fold_generation` (how many refreshes built the rows since: 0 for a dump, and for a refresh one more than the file it started from). A refresh copies both from what it folded, so a descendant never looks fresher than its ancestor. The date never moves forward without a dump; the count can go down, because a refresh that starts again from an older file (table deltas turned off, a chain set aside as damaged, a previous backup read from S3) counts from that file. With table deltas the count is on the newest pair of the chain beside the table, since the table file under a chain is carried forward unchanged. A file carried forward is a hard link to the older snapshot's file and cannot be stamped; its values are that snapshot's, read where they are. A dump written before this reads as its own instant with zero refreshes; a refresh written before it cannot be dated, and neither can anything refreshed from it, until the next dump. The Snapshots page shows the oldest of these per snapshot, and the most refreshes since only when every table records its count (a chain written before this version keeps an unknown count until the next full backup, and the page says how many tables that is). It matters for `verify`: its default rebuilds one snapshot forward to the next, which proves much less when both ends were themselves built from recorded changes.

**What it deliberately omits.** A dumped snapshot carries a content digest fingerprinting the rows against the live source, which `bintrail verify` compares. A reconstructed snapshot never read the source, so it carries **no** digest — that table is not verifiable against a source through this snapshot until the next real dump. It also carries no GTID set (the index stores GTIDs per event, not as an accumulated executed-set). Both absences are visible in the file's metadata, alongside a `bintrail.snapshot_producer = reconstruct` marker so a reconstructed snapshot stays distinguishable from a dumped one forever.

**Where the deltas resume.** The snapshot records the exact binlog coordinate the next reconstruct starts from — chosen as the position of the first transaction committed after `--at`, not derived from `--at` itself. Binlog row events carry the time a statement *executed*, not the time it *committed*, so a cut made on the timestamp alone can drop a transaction from both the snapshot and the following delta window. Cutting on position on both sides of the seam cannot: what one side ends at is exactly what the other starts from.

### When DBTrail is killed in the middle of a snapshot job

A snapshot update or a full read that fails while DBTrail keeps running cleans up after itself. One that dies with the process (killed, or stopped by the kernel's out-of-memory killer) cannot, so since [#2180](https://github.com/dbtrail/dbtrail/issues/2180) the next start does it:

- **Each job writes down what it creates, before it writes into it.** Every folder (the update's snapshot folder, the full read's `dump-…` folder in the working folder, `BINTRAIL_CONSOLE_BASELINE_STAGING`) is recorded in the run history file (`console-baseline-history.json`, beside the server list), and the job holds a lock on its own file in `snapshot-jobs/` beside it for as long as it runs.
- **The lock is the proof the job is gone.** The operating system releases it when the process exits, however it exits. A job whose lock is still held, by this process or by another DBTrail on the same state folder, is left alone. A process id is not used: a restarted container usually gets the same one.
- **Only the host that ran a job cleans up after it.** A lock is only visible within one kernel when the state folder is on NFS mounted with local locks, after an NFSv4 lease loss, or shared between a host and Docker Desktop's VM, so each job records its host (the kernel boot id, else the host name), and a job recorded by another host, or by none, is never touched. On a network or FUSE file system DBTrail says so once in its log. A state folder shared between hosts needs a lock every host sees (Amazon EFS does). After a reboot the boot id changes, so what the reboot interrupted is kept: delete it by hand.
- **mydumper dies with DBTrail** (Linux): it is started with a death signal, so an orphaned dump cannot go on writing into a folder the next start removes.
- **A writer never publishes a folder whose marker it lost.** Every producer marks its folder `_INCOMPLETE` first and, at the end, writes `_SUCCESS` only if that marker is still there; otherwise the run fails and the marker is put back, so a folder removed and partly recreated under a running writer is never published as complete.
- **At startup, DBTrail cleans up after every job whose lock is free.** It removes the folders that job wrote down, records the run in the run history as interrupted (its start, and the last time it touched its files), and logs one warning per job naming what it removed and what it kept.
- **What it never removes:** a folder the job did not write down (one from an older version, from `bintrail baseline`, or yours), a finished snapshot (`_SUCCESS`; the run is recorded with that snapshot, since it may not have reached the snapshot destination), a snapshot folder with no marker that holds files (every reader takes it as complete), a symbolic link in place of a folder, and anything under a parent folder that now resolves somewhere else than when the job started.

**Not covered:** leftovers from a version before this one (the listing warns about each `_INCOMPLETE` folder once per process; delete it by hand), the snapshot folder of a PostgreSQL full read into a local backup directory (the database chooses its name, so it cannot be written down before it exists), and a partial upload in S3 (excluded from every listing by its `_INCOMPLETE` marker; DBTrail never deletes snapshot data from a bucket). **Retention does not remove these either:** `--baseline-retain` and the keep-newest count never touch an `_INCOMPLETE` snapshot, at any age, and never look at the working folder.

### No database connection required

`bintrail baseline` reads only files — it never connects to MySQL. This means you can:

- Run the conversion on a different machine from where the dump was taken
- Re-run the conversion with different options (compression, row group size) without re-dumping
- Archive the mydumper output and convert it later

---

## When to run a dump

### Initial setup

Run a dump once when you first set up DBTrail, before starting to index binlog events. This captures the starting state of your data:

```sh
# 1. Dump
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output /tmp/mydumper-output

# 2. Convert to Parquet
bintrail baseline \
  --input  /tmp/mydumper-output \
  --output /data/baselines

# 3. Then start indexing binlog events
bintrail init --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
bintrail snapshot --source-dsn "..." --index-dsn "..."
bintrail stream --index-dsn "..." --source-dsn "..." --server-id 99999
```

### After major schema changes

If you run large DDL migrations (adding/dropping columns, restructuring tables), take a fresh baseline so the Parquet snapshot reflects the new schema.

### Periodic refresh

For audit or compliance purposes, you may want periodic full baselines. A weekly or monthly schedule is typical. Set it on the server's schedule: the **Read the whole database again every** row of the schedule form on the Snapshots page offers 24 hours, 7 days and 30 days, and the schedule API takes any interval as `full_every` (see above; its weekday is fixed by the grid, a Thursday for `7d`); to choose the day, use cron:

```cron
# Weekly baseline dump at 2am Sunday
0 2 * * 0 root bintrail dump \
  --source-dsn "$SOURCE_DSN" \
  --output /tmp/mydumper-weekly \
  && bintrail baseline \
  --input  /tmp/mydumper-weekly \
  --output /data/baselines \
  --upload s3://my-bucket/baselines/ \
  --baseline-retain 30d \
  >> /var/log/bintrail-baseline.log 2>&1
```

`--baseline-retain 30d` keeps the last month of snapshots on local disk and prunes older ones once they are safely in S3 — see [Pruning old local snapshots](#pruning-old-local-snapshots---baseline-retain) above. Without it, a weekly job grows `/data/baselines` without bound.

### On-demand

Trigger a dump at any time:

```sh
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output /tmp/mydumper-adhoc \
  --schemas mydb

bintrail baseline \
  --input  /tmp/mydumper-adhoc \
  --output /data/baselines
```

---

## How often?

| Use case | Recommended frequency |
|---|---|
| Initial setup | Once, before first binlog indexing |
| Audit/compliance baselines | Weekly or monthly |
| After major schema changes | On-demand |
| Small, rarely-changing databases | Monthly or quarterly |
| Large, high-write databases | Weekly (with `--schemas` to limit scope) |

The dump frequency depends on your recovery and audit requirements. DBTrail's binlog index captures every change between baselines, so even infrequent baselines provide full coverage when combined with the change log.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `mydumper not found on $PATH and Docker is not available` | Neither mydumper nor Docker is installed | Install Docker (recommended) or install mydumper manually (see above) |
| `mydumper not found at "/custom/path"` | Explicit `--mydumper-path` points to a missing binary | Verify the path is correct and the binary is executable |
| `found mydumper on $PATH but it appears to be a shell script wrapper` | A shell script named `mydumper` is on your PATH (e.g. a Docker wrapper) | Remove the wrapper script — DBTrail handles Docker invocation automatically. Or use `--mydumper-path` to point to the real binary. |
| `another dump is already running` | A previous dump is still running or crashed | Wait for it to finish, or check if the PID in `$TMPDIR/bintrail-dump.lock` is still alive. Stale locks from crashed processes are cleaned up automatically on the next run. |
| `--output ... does not look like a prior bintrail/mydumper dump ... refusing to delete it` | The output directory is non-empty and carries no `metadata`/`metadata.partial`/`bintrail_dump_started_at_utc` marker, and DBTrail refuses to delete unrecognized content | Point `--output` at an empty or dedicated dump directory, or remove the existing contents yourself if you are sure they are disposable. |
| `mydumper failed: exit status 2` | mydumper itself encountered an error (wrong credentials, unreachable host, etc.) | Check mydumper's stderr output for details. Verify the `--source-dsn` is correct. |
| Docker: `permission denied` on `/var/run/docker.sock` | Current user is not in the `docker` group | Run `sudo usermod -aG docker $USER` and log out/in, or use `sudo bintrail dump ...` |
| Docker: `Cannot connect to the Docker daemon` | Docker daemon is not running | Start Docker: `sudo systemctl start docker` (Linux) or open Docker Desktop (macOS) |
| Docker: mydumper cannot reach MySQL on localhost | On macOS/Windows, `--network host` does not work as on Linux | Use `host.docker.internal` instead of `localhost` in `--source-dsn` (e.g. `user:pass@tcp(host.docker.internal:3306)/`) |
| Docker: volume mount permission errors | Docker cannot write to the `--output` path | Ensure the output directory's parent exists and is writable. On SELinux systems, add `:z` to the volume mount or use `--security-opt label=disable`. |
| Docker: dump files owned by root | Older DBTrail versions ran the container as root | Upgrade — DBTrail now passes `--user <uid>:<gid>` to `docker run` so dump files are owned by the invoking user. |
| Baseline produces no files | mydumper output directory is empty or has no table data files | Verify the dump ran successfully and the `--schemas`/`--tables` filters match existing tables. |
| `--timestamp: expected ISO 8601 format` | Invalid timestamp override format | Use `2026-03-02T14:30:00Z` or `2026-03-02 14:30:00` format. |
