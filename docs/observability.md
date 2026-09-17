# Observability

`bintrail stream` (and the `bintrail-console watch` control plane) expose
Prometheus metrics describing both the **live replication pipeline** and the
**state of the index** a DBA cares about for recovery readiness.

Enable the endpoint on a standalone stream with `--metrics-addr`:

```bash
bintrail stream --metrics-addr :9090 --index-dsn ... --source-dsn ... --server-id 100
# scrape http://localhost:9090/metrics
```

Under `bintrail-console watch` the daemon serves one `/metrics` endpoint for
every monitored source; the per-source `source` label keeps them distinct.

Every stream and index metric carries a `source` label (the supervisor's entry
ID when monitored, else the resolved `bintrail_id`, or `default`). The
exceptions are the top-level capture-loss counters
[`bintrail_statement_dml_dropped_total` and
`bintrail_unhandled_rows_dropped_total`](#capture-loss-bintrail_statement_dml_dropped_total),
which have no `source` label.

## Stream metrics (`bintrail_stream_*`)

The live pipeline — emitted on the hot path as events flow.

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_stream_events_received_total` | counter | Row events received from the replication stream |
| `bintrail_stream_events_indexed_total` | counter | Row events written to `binlog_events` |
| `bintrail_stream_batch_flushes_total` | counter | Batch INSERT operations executed |
| `bintrail_stream_checkpoint_saves_total` | counter | Successful checkpoint saves |
| `bintrail_stream_last_event_timestamp_seconds` | gauge | Timestamp of the last event processed |
| `bintrail_stream_replication_lag_seconds` | gauge | Now minus the last processed event timestamp. **Receive-time** — see the warning below |
| `bintrail_stream_index_commit_latency_seconds` | histogram | Seconds from replication read to the event being **queryable** in the index |
| `bintrail_stream_availability_lag_seconds` | gauge | Approximate seconds from source commit to queryable — the effective RPO |
| `bintrail_stream_last_flush_timestamp_seconds` | gauge | Unix timestamp of the last batch successfully committed to the index |
| `bintrail_stream_errors_total` | counter | Errors, by `type` |
| `bintrail_stream_batch_size` | histogram | Events per INSERT batch |

### Received is not recoverable

`replication_lag_seconds` is set when an event comes **off the replication
stream**, before batching. An event can therefore sit in an unflushed batch —
up to a full batch or a checkpoint interval — while this gauge reads
"caught up", even though the event is **not yet queryable and not yet
recoverable**. It is unchanged and still useful for "is the source→daemon
connection keeping up", but it is not a recovery-readiness signal.

The three commit-side metrics close that window. They are published only after
a batch is durably in the index:

| Metric | Clock | Read it as |
|---|---|---|
| `index_commit_latency_seconds` | one process's monotonic clock — **skew-free, sub-second** | How long a change existed but was not yet recoverable. **This is the one to alert on.** Observed per event, so the histogram's tail is the real worst case, not a per-flush average. |
| `availability_lag_seconds` | source clock **minus** local clock | The effective RPO. **Approximate**: it carries any clock skew between the source and this host, and the binlog header timestamp it starts from has one-**second** resolution. Negative values (a source clock running ahead) are clamped to 0. Reported as the batch **maximum** — the oldest change in a batch is what defines the recovery point. |
| `last_flush_timestamp_seconds` | local | When this process last made anything queryable. Alert on `time() - <metric>`, never on its raw value. |

A zero read time — a file-mode `bintrail index` backfill, or any event a
non-replication producer built — is **skipped**, never recorded as a
0-second latency. Re-indexing month-old binlogs must not publish
"everything is perfectly fresh".

## Capture loss (`bintrail_statement_dml_dropped_total`)

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_statement_dml_dropped_total` | counter | DML statements (INSERT/UPDATE/DELETE/REPLACE/LOAD DATA) seen in the binlog as SQL text instead of row events — the signature of `binlog_format=STATEMENT`/`MIXED` or a session-level override away from ROW. Each one is a change bintrail could **not** capture. |
| `bintrail_unhandled_rows_dropped_total` | counter | Rows carried by a binlog row event of a type bintrail does not decode — skipped at capture, never indexed. Incremented **per dropped row** (one event can carry many rows). |

**Nonzero means changes are being missed.** bintrail validates
`binlog_format=ROW` at startup against the global value, but the variable is
session-settable and MIXED falls back to STATEMENT for non-deterministic DML.
The stream is not aborted — each dropped statement emits a loud warning
(`statement-format DML in binlog — event NOT captured …`) plus one increment
of this counter.

`bintrail_unhandled_rows_dropped_total` is the row-event sibling: rows carried
by a binlog row event of a type bintrail does not decode are skipped with a
loud warning (`unhandled row event type — rows skipped`) plus one counter
increment per dropped row. Nonzero means row changes reached the parser but
were never indexed.

Both counters are deliberately **top-level**: they are not part of
`bintrail_stream_*` and carry **no `source` label**, so under
`bintrail-console watch` concurrent streams conflate into one counter each. The
paired warning carries the binlog file/position (plus the statement type for
the former — never the SQL text, which embeds row values — and the
schema.table and raw event type for the latter); use the log line to tell
which source produced the drop.

## Index metrics (`bintrail_index_*`)

The **state of the index** — refreshed periodically (default 60s,
`--metrics-scrape-interval`) from a status snapshot, not per event.

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_index_oldest_event_timestamp_seconds` | gauge | Oldest event still in the index — the recovery floor |
| `bintrail_index_newest_event_timestamp_seconds` | gauge | Newest event in the index |
| `bintrail_index_retention_horizon_seconds` | gauge | How far back recovery reaches (now − oldest) |
| `bintrail_index_events_total` | gauge | Approximate event count (information_schema estimate) |
| `bintrail_index_partitions_total{kind}` | gauge | Partition count, `kind` = `active` or `future` |
| `bintrail_index_gap_hours` | gauge | Hours rotated out of MySQL but **not** archived to Parquet — holes in recovery coverage |
| `bintrail_index_storage_bytes{location}` | gauge | Index size, `location` = `mysql` (the `binlog_events` table) or `parquet` (archived partitions) |

> The oldest/newest timestamp gauges are **not published** for an empty index,
> so they never report a misleading 1970 epoch.
>
> `events_total` is deliberately a single aggregate per source — it is **not**
> labelled per schema/table, which would grow unbounded as tables come and go.
> Per-table baseline size is surfaced via `bintrail status --format json`
> instead (the `size_bytes` field on each baseline).
>
> `gap_hours` counts holes **within the retained span** — from the oldest data
> still known (live or archived) up to the newest explicit hourly partition. It
> cannot detect holes entirely *before* the oldest surviving data (nothing
> records that those hours ever existed), and the not-yet-rotated current hours
> (`p_future`) are excluded, so a stream with no rotation loop never false-counts
> them.

## PromQL recipes

**Recovery floor is slipping** — alert if the index no longer reaches back the
required retention (e.g. 7 days):

```promql
bintrail_index_retention_horizon_seconds < 7 * 24 * 3600
```

**Coverage gaps** — any hour rotated out without an archive is unrecoverable:

```promql
bintrail_index_gap_hours > 0
```

**Changes slipping past capture** — statement-format DML, or rows from
undecodable row events, that the index will never contain (see
[Capture loss](#capture-loss-bintrail_statement_dml_dropped_total)):

```promql
rate(bintrail_statement_dml_dropped_total[5m]) > 0
```

```promql
rate(bintrail_unhandled_rows_dropped_total[5m]) > 0
```

**Index disk growth** — MySQL index size trend, to project a disk-full date
(two separate queries — current size, then the 7-day projection):

```promql
bintrail_index_storage_bytes{location="mysql"}
```

```promql
predict_linear(bintrail_index_storage_bytes{location="mysql"}[6h], 7 * 24 * 3600)
```

**Capture is not making data recoverable** — the alert that actually protects
recovery. Every lag *gauge* is moved only BY TRAFFIC, so under an idle source
they all freeze at their last value and a dashboard reads "caught up" forever,
including when the stream is dead. Dividing on the flush timestamp is immune to
that, because the flush timestamp stops advancing whether the cause is a dead
daemon or a quiet source:

```promql
time() - bintrail_stream_last_flush_timestamp_seconds > 300
```

Expect this to fire during the resume-time cleanup a restart runs before
capture starts. The stream's series are now registered before that step rather
than after it (#1690), so the flush timestamp exists and reads zero for as long
as the cleanup takes — minutes, on a large index. That reading is correct:
nothing is becoming recoverable yet. What changed is the timing, not the
verdict — before, the series did not exist during the cleanup, so the same
restart stayed silent until it finished. The daemon logs `dedup-on-resume:
deleting ...` while it happens and the console shows `CLEANING UP`, which is
how you tell this apart from a stream that is actually stuck.

**Alerting on a lag gauge alone is the mistake this metric exists to prevent.**
`bintrail_stream_availability_lag_seconds > 300` looks equivalent and is not: a
stream that dies leaves the gauge at whatever it last was, so a healthy-looking
value can be hours stale.

**Commit latency is degrading** — changes are taking longer to become
recoverable, before it turns into an outage. Same-process clock, so this is
exact:

```promql
histogram_quantile(0.99, rate(bintrail_stream_index_commit_latency_seconds_bucket[5m])) > 30
```

**Stream stalled** — the source→daemon connection is behind, **or** no events
indexed (either condition is a separate alert). Note the first is receive-time:
it says the daemon is behind the source, not that data is unrecoverable:

```promql
bintrail_stream_replication_lag_seconds > 300
```

```promql
rate(bintrail_stream_events_indexed_total[5m]) == 0
```

**Newest event is stale** — the index has stopped advancing even though the
stream is up:

```promql
time() - bintrail_index_newest_event_timestamp_seconds > 600
```

## Permanent data loss is not a metric — alert on `status`

`bintrail_index_gap_hours` counts coverage holes **within the retained span**
(hours rotated out of MySQL but never archived). It does **not** signal a
permanently lost capture stream — an unfillable binlog gap or a lost PostgreSQL
replication slot. That verdict lives in `bintrail status` (the `Continuity:`
line / `stream.continuity.status` JSON field), and the alertable hook is its exit code:

```bash
# in CI/cron — exits non-zero on gap_lost OR an unconfirmable state (fails closed)
bintrail status --index-dsn "$IDX" --fail-on-gap
```

The liveness half has the same shape. `status` reports a freshness verdict
(`current` / `idle` / `stalled`, plus the never-a-false-ok states) in the Stream
section and as `stream.freshness.status` in JSON, and `--fail-on-lag` is its
alertable exit:

```bash
# non-zero on a stalled checkpoint, an unevaluable verdict (fails closed),
# or a newest indexed event older than the threshold
bintrail status --index-dsn "$IDX" --fail-on-lag 15m
```

Two things to know before you put that in cron. The **checkpoint** is the
liveness signal, not the event time — the checkpoint ticker runs with or
without traffic, so a stale checkpoint means the daemon, never the workload.
And the age check is **traffic-sensitive**: offline, a source nobody wrote to
and a capture running an hour behind are the *same observation* (fresh
checkpoint, old newest-event), which is exactly why `idle` is its own verdict
rather than being called healthy or unhealthy. Pick a threshold above your
quiet windows, and use `index_commit_latency_seconds` on the daemon when you
need to tell the two apart.

See [the continuity signal](rotation-and-status.md#stream-continuity-no-data-lost).
A Prometheus gauge for this state is not exposed in this release.

## See also

- [rotation-and-status.md](rotation-and-status.md) — `bintrail status` reports
  the same coverage, index size, and per-table baseline size in text/JSON, plus
  the stream-continuity verdict.
- [capacity.md](capacity.md) — sizing math and the `doctor` disk-capacity check.

## Watch health metrics (`bintrail_continuity_*`, `bintrail_verify_*`, `bintrail_rotation_*`)

The `bintrail-console watch` daemon exports its three safety-net conditions —
the same ones the webhook channel notifies on
([console.md](console.md#webhook-notifications)) — as gauges. Each publishes
only while its feature is actually producing verdicts; an **absent series
means "no verdict" — never evaluated, or (for the verify pair) never a
conclusive run — never "healthy"** (the continuity gauge is even unpublished
for an index the watcher cannot reach, so unknown can never read as no-gap).

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_continuity_gap_lost{server}` | gauge | 1 = the stream stamped a permanent capture gap (events in it are unrecoverable). Runs under `--notify-webhook` and/or `--metrics-addr` |
| `bintrail_verify_last_run_timestamp_seconds{server}` | gauge | Unix time of the newest verify run that **succeeded and conclusively verified at least one table** — failed and all-inconclusive runs do not refresh it, so staleness means verification is broken. Re-seeded from the persisted run history at startup |
| `bintrail_verify_tables{server,status}` | gauge | Per-status table counts (`match` / `mismatch` / `inconclusive` / `error`) of that run |
| `bintrail_rotation_healthy` | gauge | 1 = the last built-in rotation cycle neither failed nor deferred unarchived partitions |
| `bintrail_rotation_deferred_partitions` | gauge | Unarchived partitions the last cycle declined to drop |

## Example Prometheus alert rules

The push-based sibling of these metrics is the watch daemon's
`--notify-webhook` (see [console.md](console.md#webhook-notifications)).
For pull-based alerting, a starting rule set over the metrics above:

```yaml
groups:
  - name: bintrail
    rules:
      - alert: BintrailCoverageGap
        expr: bintrail_index_gap_hours > 0
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "bintrail: hours rotated out of MySQL but not archived — holes in recovery coverage"
      - alert: BintrailStreamDown
        expr: up{job="bintrail"} == 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "bintrail stream/metrics endpoint is down — capture may have stopped"
      - alert: BintrailNothingBecomingRecoverable
        expr: time() - bintrail_stream_last_flush_timestamp_seconds > 300
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "bintrail has not committed a batch to the index in 5 minutes — changes since then are NOT recoverable"
      - alert: BintrailCommitLatencyHigh
        expr: histogram_quantile(0.99, rate(bintrail_stream_index_commit_latency_seconds_bucket[5m])) > 30
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "bintrail p99 read→queryable latency is over 30s — the recovery window is widening"
      - alert: BintrailReplicationLagHigh
        expr: bintrail_stream_replication_lag_seconds > 300
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "bintrail is more than 5 minutes behind the source (receive-time; see BintrailNothingBecomingRecoverable for recoverability)"
      - alert: BintrailStreamErrors
        expr: rate(bintrail_stream_errors_total[15m]) > 0
        for: 15m
        labels: {severity: warning}
        annotations:
          summary: "bintrail stream is logging errors — check the daemon log"
      - alert: BintrailContinuityGapLost
        expr: bintrail_continuity_gap_lost == 1
        labels: {severity: critical}
        annotations:
          summary: "bintrail: permanent capture gap — events in it are unrecoverable; re-baseline the stream"
      - alert: BintrailVerifyProblems
        expr: bintrail_verify_tables{status=~"mismatch|error"} > 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "bintrail: the last verify run found mismatched or erroring tables"
      - alert: BintrailVerifyStale
        expr: time() - bintrail_verify_last_run_timestamp_seconds > 172800
        labels: {severity: warning}
        annotations:
          summary: "bintrail: no verify run succeeded in 2 days — scheduled verification is broken or persistently failing"
      - alert: BintrailRotationUnhealthy
        expr: bintrail_rotation_healthy == 0
        for: 2h
        labels: {severity: warning}
        annotations:
          summary: "bintrail: built-in rotation keeps failing or deferring — the index is growing"
```

Tune thresholds to your write rate; on a genuinely idle source the lag gauge
grows between writes, so widen its threshold there.
