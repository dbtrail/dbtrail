# Capacity Planning

How much disk the index MySQL needs, how to estimate it before you deploy, and how to monitor it afterwards.

This page covers the math; the *operation* of the index MySQL — provisioning the disk, watching it, backing it up — is the operator's responsibility, as spelled out in [SUPPORT.md](./SUPPORT.md). It applies to whatever MySQL 8.0+ your `--index-dsn` points at. (Want the index sized, monitored, and operated for you? That is what [DBTrail](https://dbtrail.com) is.)

## What one event costs

Every row change becomes one row in `binlog_events`. Its size has two parts:

1. **A fixed floor** (~0.8 KB measured) — the event metadata (binlog coordinates, GTID, schema/table names, `pk_values`) plus the secondary indexes (~0.5 KB of the floor is index entries alone). The largest single contributor is `pk_hash`, a 64-character SHA-256 hex string stored twice: once in the row, once in the PK-lookup index. This floor is the same whether the source row is 3 columns or 40.
2. **The row image(s)** — JSON copies of the row, stored in `row_before` / `row_after`. Because DBTrail requires `binlog_row_image=FULL`, every image contains **all** columns, with **column names as JSON keys**. Numeric values are nearly free (they inline into the JSON binary format); strings cost their byte length; long column names tax every single event.

The event type decides how many images are stored:

| Event type | `row_before` | `row_after` | Cost |
|---|---|---|---|
| INSERT | — | full image | floor + 1 image |
| DELETE | full image | — | floor + 1 image |
| UPDATE | full image | full image | floor + **2 images** + `changed_columns` |

**An UPDATE costs roughly twice the image payload of an INSERT or DELETE on the same table** — even if it changed a single column. A flat per-event figure hides up to a 2× error on update-heavy workloads, which is why the table below splits by type.

### Measured per-event sizes (live index, InnoDB)

Measured against the real `binlog_events` schema on MySQL 8.0 (InnoDB defaults, 10,000 rows per combination, `data_length + index_length` from `information_schema`). Figures **include** the clustered row, all three secondary indexes, and page overhead — there is no separate "InnoDB overhead" to add on top.

| Source row shape | INSERT / DELETE | UPDATE |
|---|---|---|
| Narrow — ~4 columns, short values (~60 B image) | ~0.85 KB | ~1.0 KB |
| Typical OLTP — ~15 columns (~380 B image) | ~1.2 KB | ~1.6 KB |
| Wide — ~40 columns incl. a ~1.5 KB text value | ~3.8 KB | ~8.7 KB |

Two practical notes:

- **Statement capture** (`binlog_rows_query_log_events=ON` / MariaDB
  `binlog_annotate_row_events`, see
  [query-and-recovery.md](query-and-recovery.md)) stores the originating SQL
  statement on **every row event it produced** — a 500-row bulk `DELETE`
  stores the same text 500 times (capped at 16 KB per copy, plus a 64-byte
  digest). For hand-written/ORM statements this adds roughly the statement's
  length per event; for bulk-statement-heavy workloads it can dominate, which
  is one reason capture is opt-in at the source.
- **Rows with very large TEXT/JSON values** (image > ~8 KB) move off-page in InnoDB: each image is stored in whole 16 KB chunks, and an UPDATE stores two of them. A table with a 20 KB JSON blob costs ~40 KB *per UPDATE event*. If you have such a table and it's hot, it will dominate your index — consider excluding it with `--schemas`/`--tables` filters.
- **Sparse partitions have a fixed floor.** Each hourly partition is its own `.ibd` file with its own B-trees (a few hundred KB even when nearly empty). For low-traffic deployments the per-partition floor, not the per-event cost, can dominate — don't multiply a per-row number across near-empty hours.

## The sizing formula

```
live_index_GB = events_per_day × retain_days × avg_event_bytes / 1e9
```

- `retain_days` is your **rotation window** (`bintrail rotate --retain`), not your total history. History older than the window lives in Parquet archives (see below), which are dramatically cheaper. See [Rotation and Status](./rotation-and-status.md).
- `avg_event_bytes` is the mix-weighted figure from the table above:

```
avg_event_bytes = f_insert × I + f_update × U + f_delete × D
```

where `f_*` are your workload fractions and `I`/`U`/`D` come from the row-shape table.

On top of `live_index_GB`, provision **~30% free-space headroom** on the volume — for redo/undo logs, temporary space during partition operations, and growth spikes. A volume sized exactly to the formula is a volume that fills on the first traffic surge.

### If you watch multiple servers, multiply

The control plane provisions a **separate index database per source** (each "+ Add server" in the console). Total capacity is the **sum over all watched sources**:

```
total_GB = Σ over sources ( events_per_day × retain_days × avg_event_bytes ) / 1e9
```

Adding a server from the console is a disk decision, not just a connection — budget for it.

**Baselines are separate storage.** A `bintrail baseline` snapshot (used for full-table time-travel and [`verify`](verify.md)) is Parquet, written to disk or S3 *outside* the index MySQL — so it is **not** part of `total_GB` above. Size it like an archive (≈30–60× smaller than the live index per the table above), one snapshot per dump; local snapshots can be pruned automatically once a durable S3 copy exists (`--baseline-retain`, see [Dump & Baseline](./dump-and-baseline.md)). `bintrail status` reports the live index size and per-snapshot baseline sizes directly.

### Estimating `events_per_day` before you have history

If DBTrail isn't streaming yet, ask the source itself. On the production MySQL:

```sql
SHOW GLOBAL STATUS LIKE 'Innodb_rows_%';
-- note Innodb_rows_inserted / _updated / _deleted, wait exactly 1 hour, repeat
```

The deltas × 24 approximate daily row events (and give you the insert/update/delete mix for `avg_event_bytes` too). After 24 hours of streaming you can replace the estimate with reality: `bintrail status` shows real per-partition row counts.

## Worked examples

**Typical OLTP source** — 1M events/day (60% INSERT / 30% UPDATE / 10% DELETE), 30-day retention:

```
avg_event_bytes = 0.7 × 1200 (INSERT+DELETE) + 0.3 × 1600 (UPDATE) ≈ 1320 B
live_index      = 1,000,000 × 30 × 1320 / 1e9 ≈ 40 GB
volume size     ≈ 40 × 1.3 ≈ 52 GB
```

**Update-heavy wide table** — 10M events/day (70% UPDATE / 30% INSERT+DELETE) on a ~40-column table, 7-day retention:

```
avg_event_bytes = 0.7 × 8700 (UPDATE) + 0.3 × 3800 (INSERT+DELETE) ≈ 7230 B
live_index      = 10,000,000 × 7 × 7230 / 1e9 ≈ 506 GB
volume size     ≈ 506 × 1.3 ≈ 660 GB
```

Same product, ~13× the per-event cost: **row width and update ratio dominate everything else**. If the second example's budget hurts, the levers are (in order): shorten `--retain` and lean on archives, filter out the offending table, or reduce its row width at the source.

## Archives are ~30–60× smaller

When `rotate` archives a partition to Parquet before dropping it, the same events shrink dramatically — the secondary indexes disappear, the layout becomes columnar, and zstd compresses the repetitive JSON keys:

| Source row shape | Live InnoDB (per event) | Parquet+zstd (per event) |
|---|---|---|
| Narrow | ~850–1000 B | ~15–19 B |
| Typical OLTP | ~1.2–1.6 KB | ~27–43 B |
| Wide | ~3.8–8.7 KB | ~107–203 B |

(Measured on the same datasets as above. The ratio is the combined effect of dropping indexes + columnar layout + zstd — not "zstd compression" alone — and varies with data entropy; treat it as a range.)

This is the economic core of the retention design: **keep days hot in MySQL, keep years cold in Parquet.** A year of the typical-OLTP example above is ~365M events ≈ **11 GB of Parquet** on S3 or local disk — versus ~480 GB if you tried to keep it live. Queries read both tiers transparently.

> **If `bintrail init --s3-bucket` created your archive bucket, it silently caps this at one year.** `init --s3-bucket` provisions the bucket with a lifecycle rule (`bintrail-1yr-expiry`, `Expiration: {Days: 365}` over the whole bucket) that deletes every object — archived partitions **and baselines** — once it turns 365 days old, regardless of how long you intended to retain history. `archive_state`/your baseline listing keep pointing at the now-deleted objects: a `reconstruct`/`_snapshot` read fails loud (`SourceEmptyError`, with an `archive reconcile` hint), but `recover` under `--allow-gaps` degrades to warn-and-continue, silently producing a partial reversal script. If you want to "keep years cold," either bring your own bucket with your own lifecycle policy (`--s3-arn`), or remove/widen the `bintrail-1yr-expiry` rule after `init` creates the bucket (`aws s3api put-bucket-lifecycle-configuration` / delete the rule via the console). There is currently no flag to change the expiry at `init` time.

```
required = live window (MySQL, expensive)  +  archive history (Parquet, cheap)
```

## The failure mode: the index volume is a forensic gap, whether it is full or slow

This is not a performance footnote. If the index volume fills:

1. Index writes fail and **the stream stalls**.
2. The source keeps purging its binlogs on its own retention schedule.
3. If the stall outlives the source's binlog retention, the missed events are **gone — a permanent gap in the forensic record**, which is the product's entire value.

**A volume that is merely too slow arrives at the same place by a quieter route.** Writes still succeed, so nothing errors and nothing crosses a free-space threshold, but they complete slower than events arrive. Capture falls further behind every second, and steps 2 and 3 then follow unchanged: once the lag exceeds the source's binlog retention, the events the stream has not reached yet are purged before it gets to them.

The two cases need different alarms, because free disk catches the first and is blind to the second. The signal for the second is `bintrail_stream_replication_lag_seconds`, which `stream` already exports on `--metrics-addr`; ready-made alert rules are in [deployment.md §7](./deployment.md) as `BintrailReplicationLagHigh` and `BintrailReplicationLagCritical`. Pair them with `BintrailNothingBecomingRecoverable` from the same section, because that lag gauge only moves when an event is processed: a stream that has stalled outright stops updating it, and a frozen value reads on a dashboard as the healthiest number there is. The stalled case is caught by `time() - bintrail_stream_last_flush_timestamp_seconds` instead. Set the threshold against the source's **actual** binlog retention (`SHOW VARIABLES LIKE 'binlog_expire_logs_seconds'`, or `CALL mysql.rds_show_configuration()` and read `binlog retention hours` on RDS), because that retention is the deadline the lag is racing. An empty result means no retention is configured, not unlimited: RDS ships `binlog retention hours` unset, and on MariaDB or older MySQL the variable is `expire_logs_days` instead, so a blank answer is the loudest finding rather than a reason to skip the alarm. [Streaming](./streaming.md) covers setting it on each flavour. A lag alert at 5 minutes protects nothing if the source keeps 30 minutes of binlog and the stream needs an hour to recover.

Sustained write throughput on the index is a configuration question before it is a hardware one. The InnoDB settings in [deployment.md §3](./deployment.md) are not polish. `innodb_redo_log_capacity` is the one to check first: it defaults to 100 MB, and an index left there forces a checkpoint flush every 100 MB of redo, which on a busy source is minutes, not hours. A BYO index that skips that section can fall behind a source that the same hardware would otherwise keep up with, and the symptom is growing lag rather than any error.

Prevention is rotation: without a scheduled `rotate`, `binlog_events` grows **unbounded** — the design is explicit lifecycle, not implicit GC. Run `rotate --retain <window>` on a schedule (cron, systemd timer, or `--daemon`) from day one, not after the first scare. If you are already near full, the emergency recipe is in [deployment.md §12](./deployment.md) — `DROP PARTITION` is a metadata operation and reclaims space immediately.

## Monitoring

The index is a regular MySQL table — every tool you already have works. The primitives DBTrail exposes:

- **`bintrail doctor --retain <window>`** — runs this page's math for you: it measures events/day and bytes/event from the last 24 hours of partition statistics and projects the steady-state size over the retention window. When it can also measure the index's free disk space — FAIL when the projection exceeds it, WARN above 70%. Two ways it measures that: when the index MySQL is on the same host (loopback/socket DSN) and its `@@hostname` matches this host's; or, on the bundled `docker-compose.yml` stack, via a dedicated read-only mount of the index's datadir volume into the daemon container (the index runs in its own container there, reachable only over TCP, so the loopback check alone could never fire — see [docker.md](./docker.md)). BYO index deployments that hit neither path get a SKIP, with the projection and the ≥30% headroom guidance instead of a hard number — it never goes silent-green without saying so, and it names which path was missing (no read-only datadir mount, a mount it cannot read, an index at another address, or an index on a local address whose server it cannot confirm is this host) instead of assuming where your index runs. The same check runs in `bintrail up`'s preflight with your actual `--rotate-retain`.
- **The console's Status page** shows the same check for the selected server, as an **Index disk** card (index size, write rate, the size it settles at for the configured retention, free space and how long it lasts at the current rate) plus a warning box on the doctor's warn and fail grades. Under `bintrail-console watch` the retention is what the selected server's index actually rotates on: a console override when one is saved, otherwise the window that index records (each index keeps the retention it was created under, so an index older than the 48-hour default keeps its own — the card names which of the two it used). Rotation off grades as unbounded growth, since that daemon is the one that would rotate. The standalone `bintrail-console serve` runs no rotation, so it reports the window as not known and projects no steady-state size; only the free-space floor (under 3 days of writes) still warns there. Free space follows the same two measurement paths as `doctor`; when neither applies the card says "not measurable from here" rather than showing a number, and names which path was missing: no read-only mount of the index data directory (the fix is `BINTRAIL_INDEX_DATADIR_RO` plus the mount, as the bundled `docker-compose.yml` wires them), a mount configured at a path the console cannot read, an index reached at another address, where it suggests no local mount at all because that would measure the wrong volume, or an index on a local address whose server it cannot confirm is this machine (what a port-forward or a tunnel looks like), where the mount is suggested with the warning to point it at the index's own data directory and nothing else. See [console.md](./console.md).
- **`bintrail status --index-dsn …`** — per-partition row counts (InnoDB *estimates*, fine for capacity planning), per-file indexing progress, and archive totals (`Total size: X GB`) from `archive_state`.
- **Per-partition size over time**, straight from SQL — this is your growth trend:

```sql
SELECT PARTITION_NAME,
       ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024) AS size_mb,
       TABLE_ROWS AS rows_estimate
FROM information_schema.PARTITIONS
WHERE TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL
ORDER BY PARTITION_NAME;
```

- **`archive_state`** — `file_size_bytes` and `row_count` per archived partition, queryable with plain SQL for the cold-tier trend.

Alert on three things:

1. **Free disk on the index volume** below your headroom margin (the standard node alert — but pointed at this volume specifically, because here disk-full means data loss, not just downtime).
2. **Rows landing in `p_future`** (visible in the query above) — it means partition pre-creation/rotation isn't running, the first symptom of unbounded growth.
3. **Capture keeping up**, which the first two cannot see. A volume that is slow rather than full reaches the same permanent gap with plenty of free space and no `p_future` rows, as described above. Alert on `bintrail_stream_replication_lag_seconds` for a stream falling behind and on `time() - bintrail_stream_last_flush_timestamp_seconds` for one that has stopped, since the lag gauge freezes when nothing is being processed. Both rules ship in [deployment.md §7](./deployment.md). Without a metrics stack, `bintrail status --fail-on-lag <duration>` is the equivalent exit-code check for cron.

For InnoDB server tuning (buffer pool, redo log, flush settings — RAM and throughput concerns, not disk footprint), see [deployment.md §3](./deployment.md); for the rotation/archive mechanics and `--retain` semantics, see [Rotation and Status](./rotation-and-status.md).
