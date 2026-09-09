# Dump and Baseline — Using mydumper with dbtrail

dbtrail uses [mydumper](https://github.com/mydumper/mydumper) to create logical dumps of MySQL databases. The dump output is then converted to Parquet files by `bintrail baseline`, producing a point-in-time snapshot of every table that can be stored alongside archived binlog event partitions for long-term audit reconstruction.

This document covers running dumps, converting to Parquet baselines, and scheduling.

---

## Why mydumper?

dbtrail's binlog index captures every change (INSERT, UPDATE, DELETE) but not the initial state of rows that existed before indexing began. A baseline snapshot fills that gap — it records every row as it existed at a known point in time.

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

> **Note:** Do not place a shell script wrapper named `mydumper` on your PATH. dbtrail detects scripts (files starting with `#!`) and skips them to avoid argument-handling issues with volume mounts. If you need to force a specific binary, use `--mydumper-path`.

To pin a specific mydumper Docker image version:

```sh
bintrail dump \
  --mydumper-image mydumper/mydumper:v1.0.3-1 \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output
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

# Or from the system repository (may be older)
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

---

## Step 1: Running a dump (`bintrail dump`)

`bintrail dump` is a thin wrapper around mydumper. It validates inputs, acquires a lockfile to prevent concurrent dumps, and invokes mydumper with the correct flags.

### Basic usage

```sh
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output
```

This dumps **every accessible schema** into `/tmp/mydumper-output` — bare `bintrail dump` applies no schema filter, so mydumper also tries `mysql`, `sys`, and `performance_schema`. Pass `--schemas mydb,otherdb` to scope it to your data: a least-privilege capture user (no `SHOW VIEW` on the `sys` views) **needs** that filter or the dump fails loudly. (The bundled Compose `baseline` profile excludes system schemas automatically; the bare CLI does not.)

### All flags

| Flag | Default | Description |
|---|---|---|
| `--source-dsn` | *(required)* | DSN for the source MySQL server |
| `--output-dir` | *(required)* | Directory for mydumper output. Never blindly deleted: a non-empty directory that is not a recognizable prior dump is refused; a prior dump is moved aside and restored if the new dump fails (see [Output directory behavior](#output-directory-behavior)) |
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
  --output-dir /tmp/mydumper-output \
  --schemas mydb

# Dump specific tables
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output \
  --tables mydb.orders,mydb.customers
```

When a single schema is given, dbtrail passes `--database <schema>` to mydumper. When multiple schemas are given, it constructs a regex filter (`--regex ^(s1|s2)\.`). Table filtering uses mydumper's `--tables-list` flag.

### What mydumper flags does dbtrail pass?

dbtrail always passes these flags to mydumper:

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

In practice this skew is almost always absorbed by dbtrail's idempotent delta replay and the baseline's second-granularity anchor, but it is a real gap, not a rounding error. It is also worse on non-transactional tables (MyISAM), which get no consistency guarantee at all under `NO_LOCK` — every row can be mid-write when read. mydumper still dumps a MyISAM table under `NO_LOCK` (it does not refuse), it just gives it no consistency guarantee — see the note on the `FTWRL` mode below, which behaves differently for the *same* table.

**The default is `ftwrl` (point-consistent).** A baseline is not an archive: it is the seed state
`reconstruct` merges binlog deltas onto, so a snapshot stitched from several instants yields a table
that never existed at any point in time, and every downstream answer — `reconstruct`, `verify`,
`drill` — inherits it with nothing saying so. That is not something an operator should get by not
choosing, which is why `--lock-mode` (CLI) and `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` (console)
default to `ftwrl` and the weaker modes must be asked for by name.

| `--lock-mode` | mydumper mode | Point-consistent? | Works on a write-active source? | Privileges |
|---|---|---|---|---|
| `ftwrl` (default) | `FTWRL` | yes | yes | `RELOAD`/`FLUSH_TABLES`, plus `BACKUP_ADMIN` on MySQL/Percona 8.0+ |
| `lock-all` | `LOCK_ALL` | yes | yes | `LOCK TABLES` |
| `safe-no-lock` | `SAFE_NO_LOCK` | yes — or it aborts | **usually not** | `SELECT` + `REPLICATION CLIENT` |
| `no-lock` | `NO_LOCK` | **no** | yes | `SELECT` + `REPLICATION CLIENT` |

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
against mydumper `v1.0.3-1` and MySQL 8.0: it aborts under sustained writes, and it is the only
low-privilege mode that will not lie to you.

`no-lock` accepts a torn snapshot. mydumper's own help describes it as the mode to use *"if you
don't need a consistent backup"* and pointedly leaves it out of its list of sync modes (*"There are 4
modes that can be use to sync: SAFE_NO_LOCK, FTWRL, LOCK_ALL and GTID"*). Choose it only when you
knowingly accept that the snapshot may not represent any single instant.

Neither surface downgrades silently: if the privileges for `ftwrl` are missing, the dump refuses
with an actionable error naming the alternatives, rather than quietly producing a weaker snapshot.

**Every point-consistent mode covers transactional tables only** — `lock-all` exactly as much as `ftwrl`, verified separately for each. bintrail passes `--trx-tables` on all modes, and under a mode that is attempting a consistent backup mydumper detects a non-transactional (MyISAM) table and **refuses to dump at all** ("Non transactional table found ... Restart backup using --trx-tables=0"), which the console propagates as the run's error, instead of silently proceeding the way it does under `NO_LOCK`. Verified empirically: the identical MyISAM table dumped successfully (with only a warning) under `NO_LOCK`, and was hard-refused under both `FTWRL` and `LOCK_ALL` — the refusal is gated to an actual "consistent backup attempt" (mydumper's own wording), which `NO_LOCK` explicitly is not attempting and the point-consistent modes are. **Switching `ftwrl` → `lock-all` does not get you past it**; a source with MyISAM tables needs those tables converted, excluded via `--tables`, or a low-privilege mode.

`FTWRL` mode needs `RELOAD` or the `FLUSH_TABLES` dynamic privilege (for `FLUSH TABLES WITH READ LOCK`) on **every** source flavor, verified against the pinned mydumper build (`v1.0.3-1`) against a real MySQL 8.0 source — plus `BACKUP_ADMIN` (for `LOCK INSTANCE FOR BACKUP`) **only on MySQL/Percona 8.0+**. `BACKUP_ADMIN` is a MySQL 8.0+ dynamic privilege: it does not exist on MariaDB (any version) or MySQL 5.7, and neither of those issues `LOCK INSTANCE FOR BACKUP`, so `FTWRL` there needs only `RELOAD`/`FLUSH_TABLES`. The console detects this from the source's own `SELECT VERSION()` and checks for exactly the privileges that source actually needs **before** invoking mydumper, refusing with a clear, actionable error if any are missing — this is not just belt-and-suspenders: on a MySQL/Percona 8.0+ source, granting `BACKUP_ADMIN` **without** `RELOAD`/`FLUSH_TABLES` does not make mydumper fail cleanly, it makes the pinned build **crash** (a segfault, reproduced on both amd64 and arm64), so the console's own preflight check exists specifically to turn that crash into a clean error instead of ever letting mydumper attempt it half-privileged. Point-consistent mode never silently falls back to `NO_LOCK`. Both surfaces expose the same four modes: `bintrail dump --lock-mode`, and `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` for the console's in-process baseline pipeline.

### Concurrency protection

Only one `bintrail dump` can run at a time. A lockfile at `$TMPDIR/bintrail-dump.lock` prevents concurrent runs. If a previous dump crashed without cleaning up, dbtrail detects the stale lock (by checking if the PID is still alive) and removes it automatically.

### Output directory behavior

`bintrail dump` never blindly deletes the `--output-dir`:

- **Absent or empty** — used as-is (mydumper creates it if needed).
- **Non-empty and recognizable as a prior dump** (contains a `metadata` or `metadata.partial` file, or the `bintrail_dump_started_at_utc` marker) — moved aside to a unique sibling `<dir>.old-<pid>-<nanos>` before the new dump starts. The backup is deleted only after the new dump **succeeds**; if the dump fails, the previous dump is restored in place.
- **Non-empty and anything else** — the dump is **refused**:

  ```
  --output-dir "<dir>" is not empty and does not look like a prior bintrail/mydumper dump (no "metadata" marker); refusing to delete it. Remove it yourself or point --output-dir elsewhere
  ```

  This protects against a typo'd `--output-dir` (or a stray `BINTRAIL_OUTPUT_DIR` picked up from an env file) wiping an arbitrary directory — including baselines that `reconstruct`/`verify` depend on.

Source connectivity is also validated **before** the output directory is touched (`cannot connect to source; refusing to touch --output-dir ...`), so a dump that fails to connect never disturbs the previous dump.

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
| `--output` | *(required)* | Parquet output base directory |
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

The console's own **Create baseline** trigger runs mydumper and the Parquet conversion in the same process, so it passes its own captured UTC time straight through and is unaffected by the dump host's timezone either way.

Override the resolved timestamp with `--timestamp` if needed.

Each snapshot also records its **binlog anchor** (the file/position/GTID where the deltas on top of it begin) and, per table, a **content digest + row count** in the Parquet metadata (used by [`bintrail verify`](verify.md)). Baseline-anchored consumers (full-table and single-row `reconstruct`, the shim's `_snapshot`, `verify`, and cascade recovery's baseline fallback) fetch deltas using this recorded binlog **position** as the window's exact lower bound, not the snapshot's wall-clock timestamp: a row-event's timestamp reflects when its statement executed, not when its transaction committed, so a transaction that executed just before the snapshot instant but committed (and so was logged) just after it would otherwise be silently missed by a timestamp-only lower bound. Snapshots taken before this position was recorded (or that never recorded one) fall back to the timestamp alone. A `_SUCCESS` marker is written when the conversion completes; a partially-converted snapshot carries an `_INCOMPLETE` marker instead and is excluded from discovery (see [Pruning old local snapshots](#pruning-old-local-snapshots---baseline-retain)). Since [#1583](https://github.com/dbtrail/dbtrail/issues/1583) a completing snapshot is also published with its own `views.sql` next to `_SUCCESS` — the same DuckDB schema `bintrail views` writes, pinned to the snapshot's own paths (respelled as `s3://` URLs when the snapshot is uploaded), so the file that says how to open the data can never disagree with the data it sits beside. The console's `.tar.gz` download carries a relative copy instead, which works from inside the unpacked folder wherever it lands.

### At-rest integrity (the `_MANIFEST` sidecar)

Alongside each snapshot, `bintrail baseline` writes a `_MANIFEST` sidecar holding a **CRC-32C** over every Parquet file's bytes. Every local read path that consumes a baseline — full-table and single-row `reconstruct`, cascade recovery, the time-travel shim's `_snapshot`, and `query --include-snapshot` — **re-validates the CRC on every read** and **fails loud** on a mismatch (bit-rot, a truncated/partial write), rather than silently reconstructing from corrupt data. Snapshots created before this feature (no `_MANIFEST`) are read without validation, so it degrades gracefully. S3 reads validate too ([#698](https://github.com/dbtrail/dbtrail/issues/698)): before an S3 baseline is read, the original object is streamed once through CRC-32C via the AWS SDK (default credential chain) and compared against the snapshot's `_MANIFEST`, failing loud on a mismatch exactly like the local paths — at the cost of one extra full read of each object, memoized per process. When the validating client itself cannot reach the manifest or the object (for example a region or IAM mismatch with the credentials DuckDB's reader uses), the read proceeds with a logged warning instead of blocking recovery: only a completed hash that disagrees with the manifest is treated as corruption.

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

Periodic baselines accumulate under `--output` forever — nothing in `bintrail rotate` or the daemon's rotation loop touches them. On a long-lived host this silently fills the disk, even though every snapshot is already in S3 and therefore redundant on local disk.

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
- **It is past the retention window** (and at least an hour old).
- **It is complete.** A snapshot mid-write or resumable via `--retry` (an `_INCOMPLETE` marker) is never touched.

The S3 copy is **not** pruned — only the redundant local copy. To prune on a long-lived daemon instead of from cron, `bintrail-console watch` takes the same `--baseline-retain` and runs the prune on its rotation cadence. It reclaims both the global `--baseline-dir` (against `--baseline-s3`) and every monitored server's own baseline directory (against that server's S3 prefix) — including the per-server dirs the console **Create baseline** button writes into. Env: `BINTRAIL_BASELINE_RETAIN` (CLI) / `BINTRAIL_CONSOLE_BASELINE_RETAIN` (`watch`).

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

The same thing is available a level down as `bintrail reconstruct --output-format parquet --output-dir <baselines root>`, which is what `refresh` runs — use it when you want to name every table and instant yourself.

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
| `refused-gap` | The window spans events the index permanently lost, or the index is too old to rule that out | `--allow-gaps` to accept the loss knowingly, or a fresh dump |
| `refused-ddl` | The table's columns moved since the baseline, or a `TRUNCATE`/`DROP`/`RENAME` landed in the window | `bintrail dump` + `bintrail baseline` — no flag helps |
| `refused` | Anything else (no baseline for the table, no primary key, a PK-changing `UPDATE` in the window) | Named in the message |

A table with no baseline is refused rather than degraded to the binlog-only fallback: a snapshot folded from deltas alone would silently omit every row the window never touched.

### Refreshing on a schedule

`bintrail-console watch --baseline-refresh-interval 12h` (env `BINTRAIL_BASELINE_REFRESH_INTERVAL`) runs the same refresh from the daemon, per monitored server, with no cron. It is **off by default**. The interval accepts minutes, hours or days (`15m`, `6h`, `1d`).

Minutes matter because this interval is, in practice, the ceiling on the freshness of reporting. A `state_<schema>_<table>` view sees the world as of the snapshot it was generated against, so at `12h` the tier answers yesterday's questions and at `15m` this morning's. **Regenerate `views.sql` on this same schedule.** `bintrail views` resolves the newest snapshot when it runs and writes that one path into each state view, so a file generated once keeps reading the snapshot that was newest then. Every refresh after that is invisible to it: no error, no warning, the numbers simply stop changing. Pinning is the right default for reproducible analysis, and it is why the file names the snapshot it is bound to in its header. It is not what you want behind a dashboard. A malformed interval refuses at startup; a daemon with nothing refreshable yet starts and says so, because every tick re-checks: servers added from the console later are picked up automatically, and refusing would mean a compose file carrying the interval could not boot a fresh install.

It is **independent of the Create-baseline button**: the refresh needs neither mydumper nor `BINTRAIL_CONSOLE_BASELINE_TRIGGER=1`, which is the whole point of it. Enabling one does not enable the other in either direction.

**From the console, per server.** On a daemon with a baseline feature on (`BINTRAIL_CONSOLE_BASELINE_TRIGGER=1` or `--baseline-refresh-interval`), the Backups page can set a schedule for one server (#1442): every N minutes/hours/days (at least `5m`), lined up on a UTC time of day. The operator picks when; the daemon picks how, per run, and the page says which it will be: a server with no local backup directory gets a full backup, and so does one with no previous backup yet; otherwise the newest backup is updated from the recorded changes, this same refresh, with no load on the source. When the backups go to S3 that update reads its previous snapshot from the bucket and uploads its result back there, so an S3 destination does not force a full backup every slot. An update that fails (a capture gap, a schema change, an internal error) falls back to a full backup at the same slot when the daemon may take one (the creation opt-in); otherwise that slot is recorded as skipped with both reasons. Every run publishes a full-table snapshot, and local-only backups are never removed automatically: the page shows the 30-day count next to the form and the daemon logs it at save and at boot. It lives on the server's registry entry as `backup_schedule` and the watch daemon reads it every minute, so it applies without a restart. Three rules keep it predictable: the timetable is a fixed grid (`every 1d at 03:00` is 03:00 UTC daily, `every 6h at 03:00` is 03/09/15/21; an interval that does not divide a day evenly drifts, and the page shows where it lands next), a time that passed while the daemon was stopped is not made up (and saving or editing a schedule never starts one on the spot), and a scheduled run that finds another backup job on the server skips that time rather than queuing. Every finished scheduled run and every skip lands in the baseline run history (a run that crashed writes no record; the daemon keeps its outcome for the page until its next run) (a streak of identical skips is one record whose time moves to the latest missed slot), so the page shows the last run, its result and the last skip, restart or not. A per-server schedule and the daemon-wide interval can run side by side; they share the one-job-per-server lock like everything else that produces a backup.

These properties are deliberate:

- **Conservative resources.** The refresh runs with DuckDB's container-safe budget (2 threads / 4 GB), never `--ultrafast`. That flag is for offline commands that own the machine; a background job that self-tuned to ~80% of host RAM would starve the capture path it depends on. It also folds at most **2 tables at a time** rather than one per CPU, because peak memory is the sum of the tables in flight, and it emits the large-window warning at the same raw threshold the CLI ships. Neither is operator-tunable: they are fixed so the daemon's memory does not track the size of whatever host it lands on.
- **Never `--allow-gaps`.** An unattended job must not publish a knowingly-incomplete baseline: accepting a permanent capture loss has consequences for every future reconstruct, and nobody is watching this one to make that call. A gapped window is refused and retried next interval.
- **Isolated from capture.** A failure is logged and retried; it can never take down the stream or the supervisor. That includes an internal error inside the refresh itself, not only one it reports. An error while rebuilding a table is recorded against that table, the other tables carry on, and the run fails without publishing; an error anywhere else in the job marks the run failed. Either way the stack goes to the daemon log, and the other backup jobs that share this server's lock (a manual backup, a restore, a custom `.sql` build) stay available. The one gap left: a manual backup runs `mydumper` and converts its output table by table, and an internal error inside one of those conversions still stops the daemon. A baseline that stopped refreshing is a degradation, a daemon that stopped capturing is an outage, and the first must never cause the second.
- **The daemon-wide interval uploads nothing, so nothing it publishes is pruned.** `--baseline-refresh-interval` names no destination, so its snapshots are written locally only, and the prune pass behind `bintrail baseline --baseline-retain` reclaims a snapshot only once it has confirmed a durable S3 copy of it, so the snapshots that loop publishes accumulate, whether or not an S3 destination is configured. A per-server schedule is different: on a server with an S3 destination it uploads what it folds, so retention can reclaim those. One exception, and the daemon says so at each prune: a snapshot whose upload failed is complete on disk and absent from the bucket, so retention keeps it until a full backup sweeps the directory or you remove it by hand. Upload and prune them on your own schedule (`bintrail upload --source <baseline-dir> --destination s3://…`), or size the disk for the rate the interval implies. **Read that rate before choosing a short interval**: it is one full-table snapshot per monitored server per tick, so `1h` is 720 a month per server and `5m` is 8,640, none of them reclaimable by that loop. The startup warning prints that projection for the interval you set, over 30 days because that is the horizon a disk fills on, rather than leaving the arithmetic to be discovered from a full disk. A run that publishes **nothing** does not add to that rate: a refusal folds every table it can before it reports, so it leaves a near-complete snapshot directory behind, and the loop removes the one its own failed run wrote. The daemon log names that directory either way. It removes only what it can prove that run created, and keeps the directory (saying which case it hit, and where it is) when the directory already held files before the refresh started, when the run failed with every table folded and only the final marker or integrity manifest missing, or when the directory no longer carries the `_INCOMPLETE` marker. A run that fails before its first table rebuilds (an unreachable index, a missing schema snapshot) leaves the directory holding nothing but that marker, and those are reclaimed too. The removal moves the directory aside under a `.<timestamp>.discarding` name before deleting it, so an interrupted delete can never leave table files with no marker on them; the next refresh cycle for that backup directory sweeps any staging directory it finds, so a daemon killed mid-delete does not leak one for as long as that directory is still being refreshed. One left behind in a directory the refresh no longer runs for (the server was removed, or the interval was turned off) stays until you remove it. A refresh you started yourself with `bintrail baseline refresh`, a point-in-time restore, and a custom `.sql` build all keep their fragments: you are there to look at them.
- **A DELETE removes the row from the snapshot, and the row is kept in the event log.** The refresh applies a DELETE the way the table did: the row is gone from the new snapshot, and from the `state_<schema>_<table>` view built on it. There is no deleted-marker column, which is what a warehouse connector would give you instead. The DELETE itself stays in the index and in the archived Parquet with the row's full before-image, so what was removed, and when, is a query over the `events` view (see [Parquet debugging](parquet-debugging.md)), and a table as it stood before a purge is `bintrail reconstruct --at` with a baseline from before it. A snapshot that kept every row ever deleted would also cost a full rewrite of a growing table on every refresh, which is the cost this loop already pays for tables that change.
- **A table with no changes can be left as it is (opt-in, off by default).** When the delta window held no events for a table, its previous Parquet file is published into the new snapshot as-is (a hard link where the filesystem allows one, a copy otherwise) instead of being folded and re-emitted. It does not apply when the previous snapshot is read from S3: linking a file needs both ends on a filesystem, so those runs take the ordinary path and the daemon log says so. A per-server schedule on an S3-backed server reads the bucket, with one exception (#1626): when the server's local directory already holds the bucket's newest snapshot, table for table, which is the normal state after the first run (the fold writes locally and uploads, and retention never removes the newest snapshot), the run reads that local copy and unchanged tables are reused; an older or partial local copy keeps the bucket. Each table's file carries its OWN binlog anchor, so an untouched table's anchor still points exactly where its deltas resume. It is opt-in because the rows are identical either way but the on-disk representation is not: two snapshots end up sharing one file, so the prune pass reports space it will not reclaim while the newer snapshot still references it (a `du` run per snapshot directory double-counts it too; one `du` over the baseline root reports the truth). The run summary calls a reused table `unchanged` rather than `refreshed`, and the console counts them separately, because which tables actually cost a full rewrite is the number worth seeing. A `TRUNCATE`, `DROP` or `RENAME` emits no row events either, which is why the destructive-DDL refusal runs first and this can never republish rows a truncate deleted. Turn it on with:

  | Where | How |
  |---|---|
  | `bintrail baseline refresh` | `--carry-forward-unchanged` |
  | `bintrail reconstruct --output-format parquet` | `--carry-forward-unchanged` |
  | `bintrail-console watch` | `--baseline-carry-forward-unchanged`, or `BINTRAIL_BASELINE_CARRY_FORWARD_UNCHANGED` (a true/false value: `1`, `true`, `0`, `false`) |
  | Console | Backup settings (left nav), the **Backups & disk space** card |

  The console setting overrides the daemon flag and applies on the next cycle without a restart. Once you have saved one there, the card grows a **Use the default** button that clears it again.

- **An interval shorter than a refresh is a request, not a schedule.** A refresh rewrites every table that changed in full, however little of it changed, so it has a cost the interval cannot go below. Asking for less does not queue refreshes up: a server whose previous refresh is still folding is skipped for that tick, and the tick says so. Each refresh also logs its own duration, and one that outran the configured interval says so explicitly, naming the server. That duration is the honest measure of what a refresh costs on your data, which is the number to size a shorter interval against.

It shares its single-flight with the console's **Create baseline** button, so a refresh and a dump never run against the same server at once. The last outcome per server shows up on the console's **Protect -> Backups** page and in `GET /api/baselines`.


An S3-only baseline destination is skipped with a warning: a refresh writes Parquet to a filesystem, so it needs the server's own local backup directory to fold into. A server that has both is refreshed normally, and a per-server schedule there reads its previous snapshot from the bucket.

**A knowingly-gapped snapshot stays marked forever.** `--allow-gaps` exists because sometimes the incomplete result is genuinely what you want — but the snapshot then carries a `bintrail.capture_gap` line in its Parquet metadata naming what was lost, and **every snapshot later refreshed from it inherits that line**. The missing events stay missing down the chain, so the record does too; one refresh can never launder a knowingly-incomplete baseline into a clean-looking one.

**What it deliberately omits.** A dumped snapshot carries a content digest fingerprinting the rows against the live source, which `bintrail verify` compares. A reconstructed snapshot never read the source, so it carries **no** digest — that table is not verifiable against a source through this snapshot until the next real dump. It also carries no GTID set (the index stores GTIDs per event, not as an accumulated executed-set). Both absences are visible in the file's metadata, alongside a `bintrail.snapshot_producer = reconstruct` marker so a reconstructed snapshot stays distinguishable from a dumped one forever.

**Where the deltas resume.** The snapshot records the exact binlog coordinate the next reconstruct starts from — chosen as the position of the first transaction committed after `--at`, not derived from `--at` itself. Binlog row events carry the time a statement *executed*, not the time it *committed*, so a cut made on the timestamp alone can drop a transaction from both the snapshot and the following delta window. Cutting on position on both sides of the seam cannot: what one side ends at is exactly what the other starts from.

### No database connection required

`bintrail baseline` reads only files — it never connects to MySQL. This means you can:

- Run the conversion on a different machine from where the dump was taken
- Re-run the conversion with different options (compression, row group size) without re-dumping
- Archive the mydumper output and convert it later

---

## When to run a dump

### Initial setup

Run a dump once when you first set up dbtrail, before starting to index binlog events. This captures the starting state of your data:

```sh
# 1. Dump
bintrail dump \
  --source-dsn "user:pass@tcp(source-db:3306)/" \
  --output-dir /tmp/mydumper-output

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

For audit or compliance purposes, you may want periodic full baselines. A weekly or monthly schedule is typical:

```cron
# Weekly baseline dump at 2am Sunday
0 2 * * 0 root bintrail dump \
  --source-dsn "$SOURCE_DSN" \
  --output-dir /tmp/mydumper-weekly \
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
  --output-dir /tmp/mydumper-adhoc \
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

The dump frequency depends on your recovery and audit requirements. dbtrail's binlog index captures every change between baselines, so even infrequent baselines provide full coverage when combined with the change log.

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `mydumper not found on $PATH and Docker is not available` | Neither mydumper nor Docker is installed | Install Docker (recommended) or install mydumper manually (see above) |
| `mydumper not found at "/custom/path"` | Explicit `--mydumper-path` points to a missing binary | Verify the path is correct and the binary is executable |
| `found mydumper on $PATH but it appears to be a shell script wrapper` | A shell script named `mydumper` is on your PATH (e.g. a Docker wrapper) | Remove the wrapper script — dbtrail handles Docker invocation automatically. Or use `--mydumper-path` to point to the real binary. |
| `another dump is already running` | A previous dump is still running or crashed | Wait for it to finish, or check if the PID in `$TMPDIR/bintrail-dump.lock` is still alive. Stale locks from crashed processes are cleaned up automatically on the next run. |
| `--output-dir ... does not look like a prior bintrail/mydumper dump ... refusing to delete it` | The output directory is non-empty and carries no `metadata`/`metadata.partial`/`bintrail_dump_started_at_utc` marker — dbtrail refuses to delete unrecognized content | Point `--output-dir` at an empty or dedicated dump directory, or remove the existing contents yourself if you are sure they are disposable. |
| `mydumper failed: exit status 2` | mydumper itself encountered an error (wrong credentials, unreachable host, etc.) | Check mydumper's stderr output for details. Verify the `--source-dsn` is correct. |
| Docker: `permission denied` on `/var/run/docker.sock` | Current user is not in the `docker` group | Run `sudo usermod -aG docker $USER` and log out/in, or use `sudo bintrail dump ...` |
| Docker: `Cannot connect to the Docker daemon` | Docker daemon is not running | Start Docker: `sudo systemctl start docker` (Linux) or open Docker Desktop (macOS) |
| Docker: mydumper cannot reach MySQL on localhost | On macOS/Windows, `--network host` does not work as on Linux | Use `host.docker.internal` instead of `localhost` in `--source-dsn` (e.g. `user:pass@tcp(host.docker.internal:3306)/`) |
| Docker: volume mount permission errors | Docker cannot write to the `--output-dir` path | Ensure the output directory's parent exists and is writable. On SELinux systems, add `:z` to the volume mount or use `--security-opt label=disable`. |
| Docker: dump files owned by root | Older dbtrail versions ran the container as root | Upgrade — dbtrail now passes `--user <uid>:<gid>` to `docker run` so dump files are owned by the invoking user. |
| Baseline produces no files | mydumper output directory is empty or has no table data files | Verify the dump ran successfully and the `--schemas`/`--tables` filters match existing tables. |
| `--timestamp: expected ISO 8601 format` | Invalid timestamp override format | Use `2026-03-02T14:30:00Z` or `2026-03-02 14:30:00` format. |
