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
| `bintrail_stream_batch_flushes_total` | counter | Batch INSERT statements executed, including ones inside an index transaction a stop later rolled back; `events_indexed_total` counts only committed rows |
| `bintrail_stream_checkpoint_saves_total` | counter | Successful checkpoint saves |
| `bintrail_stream_transaction_splits_total` | counter | Times a source transaction too large or too slow for one index transaction was made visible in parts; readers can see part of it until its end is captured |
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
| `bintrail_index_oldest_event_timestamp_seconds` | gauge | Oldest event still in the index — the recovery floor, to the hour: the lower bound of the oldest partition whose row estimate is not zero (an estimate of 0 on a partition that has rows moves it one partition newer until statistics catch up; `bintrail status` prints the exact event) |
| `bintrail_index_newest_event_timestamp_seconds` | gauge | Newest row in the index, by primary key: it stops where the index stopped, even while capture keeps reading commits of tables the index does not take |
| `bintrail_index_retention_horizon_seconds` | gauge | How far back recovery reaches (now − oldest, to the hour) |
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
deleting ...` while it happens and the web interface shows `CLEANING UP`, which is
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

The `bintrail-console watch` daemon exports its three safety-net conditions,
the same ones the webhook channel notifies on
([Alerts](https://www.dbtrail.com/docs/monitoring/alerts/#webhook)), as gauges. Each publishes
only while its feature is actually producing verdicts; an **absent series
means "no verdict" — never evaluated, or (for the verify pair) never a
conclusive run — never "healthy"** (the continuity gauge is even unpublished
for an index the watcher cannot reach, so unknown can never read as no-gap).

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_continuity_gap_lost{server}` | gauge | 1 = the stream stamped a permanent capture gap (events in it are unrecoverable). Runs under `--notify-webhook` and/or `--metrics-addr` |
| `bintrail_verify_last_run_timestamp_seconds{server}` | gauge | Unix time of the newest verify run that **succeeded and conclusively verified at least one table**, or found a table that differs from a snapshot read with no locks — failed and other all-inconclusive runs do not refresh it, so staleness means verification is broken. Re-seeded from the persisted run history at startup |
| `bintrail_verify_tables{server,status}` | gauge | Per-status table counts (`match` / `mismatch` / `inconclusive` / `error` / `differs`) of that run. `differs` counts the tables that differ from a snapshot read with no locks: they are reported inconclusive, and they are ALSO counted in `mismatch`, so a rule on `mismatch` keeps firing while one stands. A run whose only findings are such tables is published too |
| `bintrail_rotation_healthy` | gauge | 1 = the last built-in rotation cycle neither failed nor declined to drop a partition past retention |
| `bintrail_rotation_deferred_partitions` | gauge | Partitions past retention the last cycle declined to drop: unarchived, with an unconfirmed S3 upload, or changed since they were archived |

## The refresh's arithmetic (`bintrail_refresh_arithmetic_mismatch_total`)

A refresh with table deltas (the daemon's scheduled update of the copy) checks
its own counting on every table it publishes: rows after = rows before +
inserts − deletes, where inserts and deletes are the window's INSERT and
DELETE events, rows before is the state the window was folded over and rows
after the state it published. The numbers come from what the refresh already
has (the fold's event counts, the rows of the delta files it just wrote, the
row count the previous delta carries in its footer), so the check costs
nothing: no second pass over the table. A table published as a delta checks
that the keys it found in its base agree with the keys the events say were
alive before the window (exactly, at the start of a chain; as a bound later in
it); a table rewritten in full checks that what the merge read, kept and wrote
closes against the events and against the chain's running count.

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_refresh_arithmetic_mismatch_total{table}` | counter | Tables (`schema.table`) whose refresh did not close its arithmetic. The table is **still published**: a refresh that refuses on its own check would turn a counting bug into a copy that silently ages, which is worse than a loud wrong number. The daemon's log carries an error line, `refresh arithmetic does not close`, with the four numbers and what did not close; every table logs the same numbers at debug level, so a mismatch can be read against its neighbours |

Alert on any increase: `increase(bintrail_refresh_arithmetic_mismatch_total[1h]) > 0`.
A mismatch says the copy may hold a wrong row count for that table; check it
against the source (`bintrail verify`) and report it with the error line.
One miscount can move the counter twice: a delta whose check failed still
carries its running count forward, so the rewrite that later compacts the
chain compares a state read in full against that count and reports the
same miscount again ("the chain's count said N"). Two increases on one
table, a delta's and then a rewrite's, are one bug, not two; the error
lines say which was which (`published_as`).

## SQL on the copy (`bintrail_sql_statement_phase_seconds`)

SQL on the copy runs in two places: the SQL card of the web interface, in
the capture process (`watch`), and the MySQL port, in the router
(`bintrail-console router`). Each process has its own workers and exports
these metrics for its own statements, on its own `--metrics-addr`. Each
statement that runs to a result is timed per phase:

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_sql_statement_phase_seconds{phase}` | histogram | Seconds per phase: `slot_wait` (the wait for a free slot: up to 30 s in the capture process, and as long as it takes in the router unless it was started with `--max-wait`), `view_build` (the daemon's discovery of the copy, which stops before the worker starts), `views_for` (the daemon's callback inside the worker's run, once the worker has parsed the statement: the tables' recent changes, the checks on what it names, and the views script; it contains `tails`, the step that reads the tables' recent changes, or brings them up to date when nothing is published for the snapshot the statement reads; `tails` is present only when that step ran, which is a statement on the MySQL port with recent changes on, so a statement from the SQL card or MCP has no `tails` key rather than a zero), `spawn` (the worker's whole lifetime, which contains `open`, `lockdown`, `views` and `query`; `views` contains `views_for`), `decode`, `total` (the worker's run as the daemon drove it) and `wall` (the whole statement as the daemon saw it, from accepting it, before asking for a slot, to handing the result to the port or the HTTP response). `wall − total − slot_wait − view_build` is the time no phase accounts for. A statement answered from an earlier copy ran twice (the newest copy refused it for unmerged changes); its phases are the statement's: `wall` covers both runs and the search for that copy, and `slot_wait`, `view_build`, `views_for` and `tails` are the two runs' added together, while the worker's phases and `total` are the run that answered. Exported by `bintrail-console watch --metrics-addr` for the SQL card and by `bintrail-console router --metrics-addr` for the MySQL port (`serve` has no metrics endpoint). A statement refused as busy, or one that fails, is not observed, and a statement that hits the 60-second cap or the result-size cap counts as failed |

The count of `phase="total"` is the number of statements that ran to a
result. Refusals are not in it. The capture process runs two statements at
once by default (`--sql-max-in-flight`), one per person on the SQL card. The
router works out its own number from its cores and memory. In both, the
rest wait, so a flat count under load can mean the
slots are full, not that nobody asked. In the capture process a statement
waits up to 30 seconds and is then refused: a `slot_wait` p90 near 30 s
says statements are queueing long enough to be refused. In the router a
statement waits for as long as its client stays connected (`--max-wait`
sets a limit), so there a growing `slot_wait` is the copy answering more
slowly, not refusing. Timeouts are not in it either, so the quantiles below describe the
statements that finished, not the slowest ones.

```promql
# where a statement's time goes, p90 per phase
histogram_quantile(0.9, sum by (le, phase) (rate(bintrail_sql_statement_phase_seconds_bucket[5m])))
```

### The wait for a slot (`bintrail_sql_slot_wait_seconds`, `bintrail_sql_slot_waiting`)

A statement that finds every slot taken waits its turn. The phase
histogram above shows that wait only for the statements that then ran to a
result. These two show it for every statement that asked for a slot,
whatever happened next:

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_sql_slot_wait_seconds{outcome}` | histogram | Seconds a statement waited for a slot, by how the wait ended: `slot` (it got one; a statement that found a slot free is counted here with a wait under a millisecond, so the count is every statement served and the lowest bucket is the share that did not wait), `queue_full` (the line was full when it arrived, so it did not wait at all: the line holds 16 statements in the capture process and 4,096 in the router), `timeout` (it waited the whole wait and no slot came free: 30 seconds in the capture process; in the router only when it was started with `--max-wait`, since a statement there waits with no time limit by default), `cancelled` (its client disconnected, or the connection's own time limit ran out, while it waited). Buckets from 1 ms to about 131 s. No server label: the slots belong to the process that exports the metric |
| `bintrail_sql_slot_waiting` | gauge | Statements asking for a slot right now and not yet given one or turned away, in the process that exports it. A statement is counted from the moment it asks, so a scrape can catch one or two more than the line holds, about to be turned away |

What happens to a statement that does not get a slot depends on where it
came from. On the SQL card it is refused (HTTP 429). On the port a statement
waits for as long as its client stays connected, so only a full line or a
time limit the router was given (`--max-wait`) turns one away: without
Engine Routing the client gets MySQL error 1203, and with Engine
Routing the client gets MySQL error 1040, which drivers and pools retry,
and the decision is counted as `copy_queue_full` or
`copy_wait_timeout` under `route="refused"` (next section). The statement is
not handed to MySQL.

```promql
# how long statements wait for the copy, p90, among those that got a slot
histogram_quantile(0.9, sum by (le) (rate(bintrail_sql_slot_wait_seconds_bucket{outcome="slot"}[5m])))

# statements per second that did not get a slot, by why
sum by (outcome) (rate(bintrail_sql_slot_wait_seconds_count{outcome!="slot"}[5m]))
```

A p90 of seconds with `bintrail_sql_slot_waiting` rarely at zero says the
copy has more heavy reads than slots. On the router, more cores and memory
for the service give it more workers. In the capture process,
`--sql-max-in-flight` adds slots for the SQL card.

<a id="read-routing-bintrail_read_routing_decisions_total"></a>
## Engine Routing (`bintrail_read_routing_decisions_total`)

With Engine Routing on (a saved setting of the MySQL port, off by default),
every routed statement is one decision, counted once by the router:

| Metric | Type | Meaning |
|---|---|---|
| `bintrail_read_routing_decisions_total{server,route,reason}` | counter | Routing decisions per server (`server` is the registry id), side (`route` is `copy`, `mysql`, or `refused` for a statement nobody ran: a write on a read-only port, or a heavy read the busy copy had no room for) and reason, a closed set: `expensive_plan` (the copy answered), `cheap_plan`, `bounded_limit` (a small `LIMIT` MySQL answers without reading past it), `result_over_row_cap` (an expensive plan whose result is estimated above the copy's row cap: MySQL answered, and the copy was not tried), `not_a_select`, `write`, `session_setting`, `session_differs` (the source's session on that connection holds a setting the copy does not reproduce: a time zone, a SQL mode, a locale; the log names it once per connection), `connection_pinned` (a `CREATE TEMPORARY TABLE`, `LOCK TABLES` or `PREPARE` ran earlier on that connection), `in_transaction`, `veto`, `explain_failed`, `copy_refused`, `copy_queue_full` (refused with error 1040: the copy was busy and the line for it was full when the statement arrived, so it did not wait), `copy_wait_timeout` (refused with error 1040: the copy was busy and the statement waited the whole `--max-wait` for its turn; never counted when the router has no time limit; see [SQL on the copy](#sql-on-the-copy-bintrail_sql_statement_phase_seconds) for the wait itself), `cancelled` (under `route="refused"`: the client left, or the router was stopping, while the statement waited its turn or ran on the copy; it is not handed to MySQL), `copy_results_held` (the port was still sending as many results as it keeps in memory at once, 256 MB over all its connections; MySQL answered: what clears it is clients reading their answers, not more slots), `copy_columns_differ` (the copy works and declined a `SELECT *` or a `NATURAL JOIN` over a table whose columns there are not MySQL's, a statement that names a column the copy does not hold, one that does arithmetic on a date column, one that names a `TIME` or `YEAR` column, or one that writes a column's name right before a string: a decision, not a fault), `show_warnings`, `upstream_lost` (nobody answered: the connection to the source is lost, the client got 2006), `read_only` (refused: not a read, and the port is read-only, which is how the router starts unless told otherwise; the client got 1290). Four more names are in the set and are no longer counted by the port: `copy_age_unknown`, `copy_too_old` and `tables_unchanged` belonged to the maximum copy age, which no longer exists, and `routing_off` is listed for completeness. Decisions, not successes: a statement MySQL then fails was still MySQL's. Exported by `router --metrics-addr`; the capture process serves no MySQL port and creates no series |
| `bintrail_read_routing_copy_snapshot_timestamp_seconds{server}` | gauge | Unix time of the newest snapshot of a server's copy, as `bintrail-console router` last read it (every 30 seconds). The router sends an expensive read to the copy whatever its age, so `time()` minus this is the number to alert on, the way replica lag is. A server whose copy cannot be read has no series. Exported by `router --metrics-addr` only |
| `bintrail_read_routing_answers_total{server,route}` | counter | Statements a side (`copy` or `mysql`) finished, failed ones included. Fewer than the decisions: a refused statement, and one that found the source unreachable, finished on neither side, and a few statements are forwarded without being timed (`SHOW WARNINGS` after a forwarded statement, `USE` sent as text). Exported by `router --metrics-addr` |
| `bintrail_read_routing_answer_seconds_total{server,route}` | counter | Seconds those statements took, added up. Divided by the count above, it is the average time per statement on that side. The two sides are not the same measure: MySQL's time includes sending the rows to the client, and the copy's ends when its answer is ready to send |
| `bintrail_read_routing_answer_errors_total{server,route,kind}` | counter | Statements a side finished with an error to the client: `kind` is `timeout` when the statement's time ran out, `error` otherwise (MySQL's own refusal of the statement). Only MySQL has any: a statement the copy does not answer goes to MySQL |

The copy's share of the work is `route="copy"` over the total. A copy that
never answers is visible as `copy_refused` or `session_differs` climbing
while `expensive_plan` stays flat (for `session_differs`, the log line "the copy does not answer on this
connection" says which setting and value to change); `explain_failed`
climbing means the forwarding account cannot `EXPLAIN` what the clients run.
`upstream_lost` climbing means the source is unreachable or the forwarding
account's credentials are wrong. `copy_queue_full` or `copy_wait_timeout` climbing means the copy
is asked for more heavy reads than it serves: it works, and the reads that
did not fit were refused with error 1040. `SHOW WARNINGS` after a copy-served statement and the schema
seeded at connect are not decisions and are not counted. The same counts, per server since the router started,
are in `SHOW ROUTER STATUS` on the port and, when the capture process knows the router's address, on the web interface under Settings → MySQL clients → The router.

```promql
# statements per second the copy takes off each source, by server
sum by (server) (rate(bintrail_read_routing_decisions_total{route="copy"}[5m]))
# why statements go to MySQL instead, by reason
sum by (reason) (rate(bintrail_read_routing_decisions_total{route="mysql"}[5m]))
# average seconds per statement, by server and side
sum by (server, route) (rate(bintrail_read_routing_answer_seconds_total[5m]))
  / sum by (server, route) (rate(bintrail_read_routing_answers_total[5m]))
```

### Which statements took the time

The counters above are per side, never per statement: a statement's text
would be a label without end, and it names tables and columns. The router
keeps a table per statement shape only when started with
`--statement-table` (`BINTRAIL_ROUTER_STATEMENT_TABLE=1`, or
`ROUTER_STATEMENT_TABLE=1` in the Docker stack's `.env`). Each row is one
shape on one side, with its count, errors, timeouts, total and slowest time,
and `reasons`: why the router sent it to that side, counted per reason in
the same vocabulary as the `reason` label above (`cheap_plan`,
`expensive_plan`, `copy_refused`, ...), adding up to the row's count. A row
of the copy also carries `fresh`: how far behind capture the data of its
answers was, the last answer's (`last_age_seconds`, `last_as_of`) and the
least and most over its answers (`min_age_seconds`, `max_age_seconds`), each
one answer's own lag, rounded up; `known` and `unknown` count the answers
whose lag is and is not known (`unknown_reason` says why, in a fixed
sentence), `pending` says the last answer's waits for the router to place
its snapshot in the index, `tailed` counts the answers that read every table
up to date with what capture indexed since its snapshot, and `snapshot` those
that read some table as of its snapshot. A row of MySQL has none: the source
answers with its own current data.
Every value in a shape is blanked (`WHERE id = 12345` and `WHERE id = ?` are
one row, and so are `IN` lists of any length); table and column names are
kept. A server keeps up to 200 rows; a shape first seen past that is counted
in `not_counted`, so the table never reads as complete when it is not.

It is published in the router's `/status` document, under each server's
`statements`, on the `--metrics-addr` address, which asks for no password.
Whoever reaches that address reads the names of the tables and columns your
clients query. The web interface passes it on only to sessions with no data
profile, and tells the others it was withheld.

The web interface draws it on the MySQL clients page, under the router's
two sides, as **Which statements take the time**: one bar per shape, as
long as its total time next to the shape that took the most, with how many
times it ran, its time each, its slowest, its errors and, on the copy, the
lag of its last answer. Five are shown to a page, most time first, and
buttons above the list show all of them, only your MySQL's or only the
copy's. A shape opens its detail: its whole text, and for each side that
answered it how often, why the router sent it there, and how fresh the data
was. On the copy that is the lag of its last answer, its least and its most,
and whether the answers read every table up to date (fresh) or read some
table as of its snapshot (as old as that snapshot). A router without the
table says so in one folded line.

## Example Prometheus alert rules

The push-based sibling of these metrics is the watch daemon's
`--notify-webhook` (see [Alerts](https://www.dbtrail.com/docs/monitoring/alerts/#webhook)).
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
