# bintrail-console

`bintrail-console serve` serves an embedded, **read-only, single-operator** web
UI over an existing index. It is the MCP server with a web face: the same query,
recovery, and status engines, reached from a browser. Browse indexed row events
with full before/after diffs, and generate recovery (undo) SQL — all without
leaving the terminal that started it.

The console ships as its own binary (and Docker image,
`ghcr.io/dbtrail/bintrail-console`), separate from the core `bintrail` CLI —
install it only where an operator wants the UI.

The console **never executes SQL**. Recover produces a transaction-wrapped
script you copy or download and apply yourself after review, exactly like
`bintrail recover --dry-run`.

> **Scope:** event browsing, recovery-SQL generation, index status, and — when a
> baseline is configured — **single-row point-in-time reconstruct** (a row's full
> state "as of T" plus its history). Full-*table* reconstruction (mydumper output)
> stays in the offline `bintrail reconstruct` CLI.

## Usage

```sh
bintrail-console serve --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

On start it prints the URL to open. On a fresh console the first visit is a
**"create your password"** screen (see [Password login](#password-login));
after that, you sign in:

```
Bintrail console (read-only) is running. Open:

    http://127.0.0.1:8090/

First run: open the URL and create your console username and password.
```

Or serve it **alongside a live stream** in one process with
`bintrail-console watch` (the daemon formerly known as `bintrail up
--console` — `up`'s preflight + init + stream plus the console and the
multi-server control plane):

```sh
bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX"
```

`--console-listen` / `--console-token` (or `BINTRAIL_CONSOLE_LISTEN` /
`BINTRAIL_CONSOLE_TOKEN`) customize the bind and token; a single Ctrl-C drains
both the stream and the console. Passing `--baseline-dir` or `--baseline-s3`
(or `BINTRAIL_CONSOLE_BASELINE_DIR` / `BINTRAIL_CONSOLE_BASELINE_S3`) enables
the baseline-gated Time-travel surface here too, so one process serves the live
stream **and** point-in-time reconstruct:

```sh
bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX" --baseline-dir /var/bintrail/baselines
```

With an S3 baseline (`--baseline-s3`), the `watch` process reads S3 at request
time using the ambient AWS credential chain — same as the standalone console,
but note the plain stream daemon didn't need AWS credentials before.

For a TLS-requiring source (RDS, Aurora, Cloud SQL), `watch`'s own stream
accepts `--ssl-mode` / `--ssl-ca` / `--ssl-cert` / `--ssl-key` (env
`BINTRAIL_SSL_MODE` / `BINTRAIL_SSL_CA` / `BINTRAIL_SSL_CERT` /
`BINTRAIL_SSL_KEY`), same semantics as `bintrail stream` — see
[streaming.md → TLS/SSL for managed MySQL](streaming.md#tlsssl-for-managed-mysql-rds-aurora-cloud-sql).
The default stays `--ssl-mode preferred` (opportunistic, no certificate
verification).

> **Scope of `--ssl-mode` under `watch`.** The flag encrypts `watch`'s embedded
> **stream** connections — the source replication and the index *write*. It does
> **not** currently cover the console's own index *reads* (the connections the
> multi-server manager opens to serve the web UI) or the index reads behind the
> embedded flashback port (`--flashback-listen`). Encrypt those index
> connections by adding a `tls=` parameter to their DSN (`...?tls=true`, or
> `?tls=skip-verify` for self-signed dev certs) — the same knob the offline read
> commands use. (The flashback port's *inbound* MySQL-protocol listener — the
> client→port leg — has no TLS option in this release; `tls=` only covers its
> backend index reads.) If your index MySQL is reached over loopback or a
> private network this is moot; over an untrusted network, set `tls=` on the
> index DSN in addition to `--ssl-mode`.

Open that URL in a browser. A left **sidebar** groups the views (Time-travel
appears only when a baseline is configured), with a **server switcher** at the
top (see [Managing servers](#managing-servers)) and a **⌘K command palette**
(also reachable from the "Search & commands" button) for jumping between views
and searching events:

1. **Overview** (landing) — what changed recently and where: a **Restore
   coverage** card answering "to when can I restore, right now?" — any point
   between the delta-coverage floor and the last *indexed* event (never the
   wall clock; the capture-lag chip says how close to now that edge is), the
   continuity verdict, and — with a baseline source configured — the
   full-table restore window plus any table whose newest baseline predates
   coverage. It degrades loudly on `gap_lost`/`unavailable`/`unknown`/an
   empty index, and full-table coverage that could not be evaluated says so
   instead of rendering as "nothing broken".
   Below it: headline counts (changes indexed, deletes, tables touched, most
   recent change), a **Recent changes** list (each row opens Events, with an
   inline **Undo**), and an **Activity by table** breakdown. The
   window-scoped counts cover the **live retention** — exactly what the live
   index still holds, derived from its oldest partition — and come from a
   server-side materialization refreshed about every 30 minutes; each tile
   states its window and an "as of …" refresh time, so a cached number is
   never presented as live. The page paints progressively: the frame and
   per-card placeholders appear immediately, and each card fills as its own
   fetch lands, so one slow aggregate cannot hold the whole page. The
   starting point for "what happened?". For history older than the live
   retention, use Events with time filters or `bintrail query`.
2. **Events** — a smart search box (free text plus `type:`, `pk:`, `col:`, and
   `schema.table` tokens) with an expandable **Filters** panel. Each row expands
   in place to a before→after diff; `j`/`k` move the cursor, `↵` expands, `u`
   jumps to Recover. Results are **paged**: `‹ Newer` / `Older ›` walk the
   stream a page at a time, and the header states where you are
   (`showing 1–100 of more`, or `showing 201–247 of 247 (end)` once you reach
   the bottom) rather than restating the page size. Paging is keyset-based —
   the cost of page 40 is the cost of page 1 — and each page is still bound by
   the same result cap; editing any filter returns you to page 1.
   **Export JSON / Export CSV** export *every match of the current search*, up
   to the endpoint's 1000-event cap, not just the page on screen; if the search
   has more than that, the console says so instead of handing you a silent
   prefix. Rows and exports include `connection_id` (the transaction's
   originating thread number) but never `query_text`/`query_hash`.
3. **Schema changes** lists every CREATE, ALTER, DROP, RENAME and TRUNCATE
   the stream recorded for this server, newest first: time, table, type, the
   statement and its binlog position. Filter by schema, table, type and time
   range; the same result cap as Events applies, and the page says when more
   exist. Changes detected in the same second list in binlog order. Under an
   access policy, rows are scoped by the table the index attributed each
   statement to (a statement naming several tables is attributed to the
   first), and the statement text itself is withheld, because it can carry
   values and name other tables; time, table, type and binlog position stay,
   and the page says what it left out.
4. **Time-travel** — single-row point-in-time reconstruct, drawn as a timeline
   (baseline snapshot → each change, with a **Restore to this state** jump to
   Recover). Appears **only when a baseline is configured**
   (`--baseline-dir`/`--baseline-s3`); otherwise it is hidden, never shown
   empty. See [Time-travel](#time-travel-reconstruct).
5. **Recover** — filter schema / table / PK / time, preview the affected rows
   with before→after diffs, then **Generate undo SQL** and copy/download the
   script. Arriving via an **Undo** action scopes it to that row and shows a
   context banner; it fills in **Until** from the event you clicked and leaves
   **Since** open. Timestamps are second-granular and the bound is inclusive,
   so the window closes at the **end of that event's second**: every change to
   the row up to there is reversed — including events that happened *after*
   the one you clicked, inside the same second — not only the event you
   clicked. **Since** narrows the window but cannot split events within a
   single second; use `--limit-per-pk` for that.
   When you undo a `DELETE` on a foreign-key **parent** whose
   children InnoDB cascade-deleted *below* the binlog (MySQL ≤ 8.x / MariaDB) —
   or an `UPDATE` of a referenced key whose `ON UPDATE` cascade rewrote them just
   as invisibly —
   Recover **auto-detects** it and folds the invisible children into the same
   script — no separate tab, no extra step. **Nothing is ever executed.** See
   [Recover and cascade](#cascade-recovery).
6. **Status** — index health: partitions, coverage, stream lag, archives, and a
   first-class **stream-continuity** signal — a green "✓ No gaps in captured
   stream" badge when the captured range is contiguous, or a red "⚠ Events
   permanently lost" record when an unfillable gap (or a lost PostgreSQL slot)
   was detected. Both fire for any source family. See
   [the continuity signal](rotation-and-status.md#stream-continuity-no-data-lost).
   An **Index disk** card carries the projection `bintrail doctor` computes
   for the selected server: index size, write rate, the size it settles at
   for the configured retention, free space on the index volume, and how
   long that free space lasts at the current rate. The grade is the
   doctor's; a warn or fail grade also renders a box above the cards saying
   what fills the disk and what fixes it. Two things it says rather than
   guesses: free space that this process cannot measure reads "not
   measurable from here", never a number, and the card names why it could
   not measure it plus the fix when there is one (mount the index data
   directory into the console read-only and set `BINTRAIL_INDEX_DATADIR_RO`
   to the mount point, the way the bundled `docker-compose.yml` does; an
   index reached at another address gets no such suggestion, because a
   mount that is not the index's would report the wrong volume's free
   space, and an index on a local address whose server the console cannot
   confirm is this machine, which is what a port-forward or a tunnel looks
   like, gets the suggestion with that warning attached); and the
   standalone read-only console, which runs no rotation of
   its own, reports the retention as "not known here" instead of grading an
   index another process rotates as unbounded. See
   [capacity planning](capacity.md#monitoring).
7. **Protect** (under `watch` only) — **Backups** (the selected server's
   snapshot listing; each row expands to its tables, sizes and how long the
   backup took, with a **Download (.tar.gz)** of the whole snapshot — the
   archive includes a `views.sql` with relative paths, so unpacking it and
   running `duckdb -init views.sql` from inside the folder opens every table
   ([#1583](https://github.com/dbtrail/dbtrail/issues/1583)); plus
   **Create backup** and — for a server with its own local backup directory —
   **Restore to a moment**, which folds a chosen past instant into a NEW
   discoverable snapshot in the same store, and **Build a .sql backup for
   any moment**, which folds a chosen instant into a mydumper-format dump
   downloaded as one `.tar.gz` — load it with `myloader`, nothing from
   bintrail needed on the restore side; the build is a full plaintext copy
   of every row, staged on the daemon's disk under the system temp
   directory unless `BINTRAIL_CONSOLE_BASELINE_STAGING` says otherwise) and
   **Verification** (run
   `bintrail verify` and read past runs). These produce and validate the
   artifacts a restore depends on, so they are operations rather than
   settings; they lived on the old Storage page until they outgrew it.
   A staged `.sql` build does not stay on disk: it is removed as soon as a
   download completes, 4 hours after the build finished if nobody
   downloaded it (the Ready line shows the deadline and the size), when a
   new build for the same server starts, or when the daemon restarts (every
   build a previous process left behind, finished or interrupted, is removed
   at startup). If a removal fails, the build keeps its state, the card says
   why, and the daemon retries every minute. If a new build starts and the
   previous one cannot be removed, the new build still runs and stays
   downloadable; its status names the previous build's directory until that
   removal succeeds. The This daemon page shows what is staged while it
   exists, previous builds that could not be removed included.
8. **Settings** — **Backup settings** (every parameter that shapes a
   backup or a snapshot, with its provenance — see
   [The Backup settings page](#the-backup-settings-page);
   on `serve` only the editable per-server half renders, since this page is
   the one editor of a server's backup location), and under `watch` only:
   **Retention** (rotation policy and
   per-source S3 archiving), **This daemon** (AWS credential signals, staged
   downloads and a usage-telemetry opt-out — see
   [The Retention and This daemon pages](#the-retention-and-this-daemon-pages))
   and **Rotation** (opens the rotation dialog).

Every view whose subject has a page on www.dbtrail.com/docs (Events, Restore,
Backups, Verification, Storage, Connect AI) shows a small **Docs** link beside
its title. It opens that page in a new tab and is a plain link: the console
makes no request for it, so it costs nothing on an air-gapped host. Views with
no page of their own show no link.

## Managing servers

The header has a server switcher and a **Servers** button: add, edit, and
remove named connections to dbtrail index databases, and switch every view
between them. The registry is a **local YAML file on the console host**
(`~/.config/bintrail/console-servers.yaml` by default, override with
`--servers-file` / `BINTRAIL_CONSOLE_SERVERS`) — adding a server registers a
connection for browsing; it does **not** start monitoring. Monitoring still
starts with `bintrail up` / `stream` against that server (or from the UI
under `watch` — see the control plane below).

How it behaves:

- **The command-line entry.** `--index-dsn` (or `watch`'s stream index)
  appears as an ephemeral entry labeled by its database name, e.g.
  `bintrail_index (cli)`: it is never written to the registry file and cannot
  be edited or deleted from the UI. With at least one saved server,
  `--index-dsn` becomes optional — the console can start registry-only.
  **It is a connection to the daemon's own index database, not a monitored
  source** — under source-less `watch` nothing ever streams into it (each
  added source gets its own per-source database). For that reason a
  source-less `watch` daemon **hides it entirely**: a fresh install lists no
  servers (the switcher shows "no servers yet" and the Servers dialog is
  empty), and the views render against the internal index underneath until
  the first server is added (a note under the switcher says so — the
  internal index is not guaranteed empty, e.g. after restarting without a
  previous `SOURCE_DSN`); the default selection is then the first server
  with a source configured (or the first saved one). The cli entry remains
  visible where it actually carries data — `serve`, and `watch` with
  `--source-dsn` (the main stream writes into it) — labeled by its database
  name and sorted last in the switcher.
- **Lazy connections.** Saved servers connect on first selection (with an
  eager ping, so a dead server fails the moment you switch to it, not on your
  first query). Editing a server's connection details closes and reopens its
  connection; editing only its baseline/archive settings keeps it.
- **Per-server selection is per-tab.** The selection rides an
  `X-Bintrail-Server` header on each request — there is no server-side "active
  server" — so two browser tabs can watch two different servers.
- **Per-server Time-travel.** The reconstruct gate (baseline configured, no
  RBAC profile, archives enabled) is evaluated per server; the Time-travel view
  appears and disappears as you switch. A server whose registry entry has no
  baseline of its own inherits the process-wide `--baseline-dir`/`--baseline-s3`
  (when set), so servers added from the UI get Time-travel and verify under a
  single-baseline-dir deployment without extra configuration.
- **Test connection.** Each server (saved or being typed) has a write-free
  probe: ping, MySQL version, latency, whether the database looks like a
  dbtrail index, and whether its schema is current.

Security notes specific to the registry:

- The registry file stores full DSNs **including passwords** (`0600`, directory
  `0700`) — the same secret-at-rest class as `shim.yaml` and `dump.key`.
- Passwords never travel to the browser. List/get responses carry parsed
  non-secret fields plus `has_password`; leaving the password blank on an edit
  keeps the stored one.
- **The console never migrates servers added in the UI.** The one schema
  migration (`EnsureSchema`, an idempotent ALTER) runs at startup on the DSN
  you typed on the command line — never on a DSN typed into a browser form. A
  registry index that predates the `connection_id` column returns an
  actionable `422` (run a writer command against it once) instead of being
  silently ALTERed.
- The registry file, the [auth file](#password-login) (written by
  set-password), and the [managed MCP token file](#mcp-endpoint)
  (`~/.config/bintrail/console-mcp-token.yaml`, SHA-256 only) are the only
  things the console ever writes. Each has a path override
  (`--servers-file`, `--auth-file`, `--mcp-token-file`, or the matching
  `BINTRAIL_CONSOLE_*` variable); in a container, point all three at a mounted
  volume or they are lost when the container is recreated. Their write
  endpoints sit behind the same bearer token and Host-header guard as
  everything else.

The registry file is versioned and forward-compatible: fields written by a
newer dbtrail survive load→edit→save round-trips on an older binary, and a
file written by a newer *schema* version loads read-only rather than being
rewritten lossily.

### Monitoring a source from the UI (the control plane)

Under **`bintrail-console watch`** — and only there — the console is also a
control plane: "+ Add server" with a **source MySQL** (host/user/password,
optional schema filter) runs the `bintrail doctor` preflight inline
(failures come back as remediation cards), provisions a dedicated index
database for that source (`bintrail_idx_<id>` on the daemon's index server:
`CREATE DATABASE` + tables + schema migration, done by the daemon — the
console's request handlers still never migrate anything), and starts a
supervised binlog stream. Auto-start: a green preflight starts streaming
immediately; warnings (e.g. short binlog retention) show but don't block.

The **source user** you paste into the form needs `REPLICATION SLAVE,
REPLICATION CLIENT, SELECT` on the source MySQL — the form spells out the
exact `CREATE USER` / `GRANT` to copy. dbtrail never writes to the source, and
capture never locks it. The **Create backup** button needs one privilege
more, `LOCK TABLES`, because a baseline is point-consistent by default; without
it capture keeps running and only the baseline is refused, naming the exact
`GRANT`. On RDS/Aurora set `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all` —
`ftwrl` needs `BACKUP_ADMIN`, which managed MySQL will not grant. Full
per-privilege breakdown and the least-privilege (schema-scoped `SELECT`)
variant: [streaming.md](streaming.md#the-source-mysql-user).

- Each monitored source gets **its own index database** — per-source state
  (checkpoints, snapshots) stays structurally isolated, and the server
  switcher lists it like any other connection.
- The supervisor reconciles **desired state** (`monitor_desired` in the
  registry) at boot: restart the daemon and monitoring resumes from each
  stream's saved checkpoint.
- A per-entry **advisory lock** (`GET_LOCK`) on the index server makes a
  second daemon refuse to double-stream the same entry.
- **Stream states**: `PENDING` covers launch through the stream's first
  checkpoint (connecting, snapshotting, finding the start position) — the
  badge only says `RUNNING` once the stream has proven it is attached and
  writing. Two unhealthy-but-alive variants surface what used to be silent:
  `STALLED` (connected but no checkpoint/batch progress for 5+ minutes) and
  `LOST POSITION` (binlogs were purged past the saved position while the
  stream was behind or stopped — it auto-advanced and events in the gap are
  permanently lost; the record is durable, survives daemon restarts, and is
  cleared only by an explicit Stop on the entry).
- A failing stream is retried with exponential backoff (15s → 5m) and shows
  as `FAILED` with the scrubbed error; `Stop` requires an explicit click, and
  deleting or re-pointing a *running* entry is refused (409) until stopped.
  A stream that crash-loops continuously for 6 hours stops retrying
  (permanent `FAILED`, the message says it gave up) — press Start to re-arm
  it after fixing the cause.
- The add-server preflight warns (amber, never blocks) when the new source
  **looks like a replica or duplicate of an already-monitored one** — GTID
  lineage comparison; monitoring both would double-index the same changes.
  Detection needs `gtid_mode=ON`; in position mode the check is skipped.
- With `--metrics-addr`, the daemon serves one Prometheus `/metrics` endpoint
  for all supervised streams; every stream series carries a `source` label set
  to the entry ID (see [streaming.md](streaming.md)). Exception: the capture-loss
  counter `bintrail_statement_dml_dropped_total` has **no** `source` label, so
  concurrent streams conflate into one counter — see
  [observability.md](observability.md).
- **Archive to S3** (the `Archive to S3` field on a monitored source): set an
  `s3://bucket/prefix/` destination and the daemon's built-in rotation
  **uploads that source's rotated partitions as Parquet before dropping them**,
  so the forensic record survives the retention window and stays queryable —
  the console auto-discovers the archive on the next query, no extra config.
  Partitions are staged locally (`--archive-staging-dir` /
  `BINTRAIL_CONSOLE_ARCHIVE_STAGING`, default a temp dir), uploaded, then
  pruned. The S3 upload uses the **ambient AWS credential chain** (`AWS_*`
  env, `~/.aws`, or an instance/role) — the same credentials the console needs
  to read the archive back; there is no per-source credential. Archiving for a
  source begins once its identity (`bintrail_id`) is resolved (right after its
  first stream connect); until then it rotates drop-only, and the
  protect-unarchived guard never drops un-uploaded data. A persistently failing
  upload (bad bucket/credentials) keeps partitions undropped and escalates to a
  loud Error after a few cycles — the index does not silently lose data, but it
  does stop shrinking, so fix the bucket/credentials. The archived Parquet is
  unencrypted; rely on bucket-level SSE/policy. Archive to S3 ≠ Baseline S3
  (the latter is read-side Time-travel input).
- Registry fields: `source_dsn` (replication credentials — a secret with the
  same masking/keep-password discipline as the index DSN; `source_dsn: ""`
  clears it), `source_server_id` (0 = derived), `schemas`, `monitor_desired`,
  `archive_s3` (the bucket above — non-secret, round-trips in the masked DTO),
  and per-source TLS: `ssl_mode` / `ssl_ca` / `ssl_cert` / `ssl_key` (same
  semantics as `bintrail stream`'s `--ssl-*` flags — see
  [streaming.md → TLS/SSL for managed MySQL](streaming.md#tlsssl-for-managed-mysql-rds-aurora-cloud-sql)).
  Setting these in the registry is the only way to get `verify-ca` / mutual
  TLS on a "+ Add server" source; an empty `ssl_mode` means the default
  `preferred` (opportunistic, no certificate verification).
  `ssl_ca`/`ssl_cert`/`ssl_key` are certificate/key file paths **on the
  daemon host**, not secrets.

The standalone read-only `bintrail-console serve` never offers any of this:
the `monitor` capability is false and the verbs return 403 there.

### Configuring rotation from the UI

`bintrail-console watch` runs the built-in rotation loop that keeps the index
from growing without bound: it drops binlog partitions older than a **retention
window** every **interval**, keeping a few **future partitions** ready. Under
`watch` you can tune that policy from the console — the sidebar's
**Settings → Rotation** entry, or **⌘K → "Configure rotation…"** — without
editing flags or restarting:

- **Live:** changes apply on the loop's next cycle. Retention and future-partition
  count take effect immediately; a changed interval re-tunes the schedule.
- **Global, one schedule:** the loop is a single shared ticker, so the policy
  applies to **every** index the daemon rotates (the boot index and every
  monitored source). Per-source retention is not offered — the schedule is one.
- **Override vs default:** the saved policy lives in the local console registry
  (`console-servers.yaml` — the only file the console writes). When nothing is saved the panel shows the
  daemon's `--rotate-retain` / `--rotate-interval` / `--rotate-add-future`
  (`BINTRAIL_ROTATE_*`) values as the **effective default**.
- **Disabling** rotation entirely stays a daemon-level decision
  (`--rotate-retain off`); the panel tunes a running loop rather than turning it
  off. A retain like `off` is rejected at save.
- The standalone `bintrail-console serve` hides the panel and refuses the write
  (HTTP 403) — only the daemon running the loop consumes the policy.

### The Protect pages

Under `watch` the sidebar carries a **Protect** group with two pages. They were
part of the old Storage page until they outgrew it: both are operations that
produce and validate the artifacts a restore depends on, rather than settings,
and the snapshot listing is unbounded in practice — it pushed verification, the
panel that answers whether a restore would work, far below the fold.

**Protect → Baselines**

- A read-only listing of the **selected server's** baseline source
  (`baseline_dir` / `baseline_s3`): each snapshot's timestamp, age, table
  count, and (local sources) the binlog coordinates its deltas start from. The
  empty states explain how to produce a first baseline (`bintrail dump` →
  `bintrail baseline`). When the **Create baseline** button is enabled it sits
  in this panel's header.
- **Keep it current with Iceberg** (#1466) — a display-only panel at the
  bottom of the page that prints the exact `bintrail export iceberg` command
  for the selected server, with its index connection and its resolved backup
  destination filled in, a Copy button, and an hourly cron line. Nothing runs
  from here: the export writes a new copy of your data and is kept out of the
  process that captures changes. The password is shown as `***` for you to
  replace, and the command carries the index host, port, database and user
  and nothing else about the connection, so an index that needs TLS or a
  timeout needs those added by hand. The panel also names the compose route
  (`docker compose --profile iceberg-export run --rm iceberg-export`), which
  runs against the **bundled stack's own** index and backups: for any other
  server, point it with `INDEX_DSN` and `BASELINE_DIR` / `BASELINE_S3` in the
  stack's `.env`, or run the printed command where bintrail is installed. See
  [Iceberg export](iceberg-export.md) and [docker.md](docker.md).
- **Scheduled backups** (#1442) — a per-server timetable, set from this page:
  every N minutes, hours or days (at least 5m), lined up on a UTC time of
  day. The operator picks WHEN; HOW each run is made is the daemon's decision
  per slot (`console.ChooseBackupMethod`), and the page says which one comes
  next and why: a server with no local backup directory gets a **full backup**
  (the Create backup job, reads the source, needs
  `BINTRAIL_CONSOLE_BASELINE_TRIGGER=1`), and so does one with no previous
  backup yet; otherwise the newest backup is **updated from the recorded
  changes** (the baseline-refresh fold: reads nothing from the source, needs
  the server's own local backup directory to write into). When the backups go
  to S3 that update reads its previous snapshot straight from the bucket and
  uploads its result back to the same place (#1539), so an S3 destination no
  longer forces a nightly full read of the source; when the local directory
  already holds that same newest snapshot, table for table, the update reads
  the local copy instead so unchanged tables can be reused by hard link
  (#1626), and any other local state keeps the bucket as the source. An update that
  fails (a capture gap, a schema change, an internal error) falls back to a
  full backup at the same slot when the daemon may take one (the creation
  opt-in); otherwise that slot is recorded as skipped with both reasons.
  Every run is a full-table snapshot, and on a server without an S3
  destination nothing removes them automatically: the card shows the 30-day
  count under the form, and the daemon logs it at save and at boot. Stored on the server's registry
  entry (`backup_schedule`), read by the watch daemon every minute, so it
  applies without a restart. The grid is fixed (`every 1d at 03:00` is 03:00
  UTC daily; `every 6h at 03:00` is 03/09/15/21; an interval that does not
  divide a day evenly, `5h` or `36h`, drifts through the day; the page shows
  where it lands next). It fires only on a slot boundary the daemon is up to
  see: a slot missed while stopped is not made up, and saving a schedule, new
  or edited, never starts a backup on the spot (the API tells the loop the
  save instant, so a boundary inside the next minute is not lost either). A
  scheduled run that collides with a manual backup, restore or export skips
  that slot rather than queuing, and the skip, like every scheduled run, is
  written to the baseline run history (a streak of identical skips is one
  record whose time moves to the latest missed slot) so the page shows the
  last run, its result and the last skip after a restart too; the daemon's
  own view of the job it last started fills in when the history file is
  unavailable or a job died without writing one (the daemon watches every
  job it starts, so that view is complete). When an update from the
  recorded changes fails and a full backup is started in its place, the
  page says so in red until a later scheduled update goes through. A
  schedule the daemon cannot serve at all (no producer possible: creation
  opt-in not set AND no local directory, a lock-mode misconfiguration on a
  server that has no local directory either, no destination) is refused on save with
  the reason, and one already saved is reported as not runnable on the
  page, never silently skipped.
  `PUT`/`DELETE /api/servers/{id}/backup-schedule`; state on `GET /api/baselines`
  (`schedule`). The daemon-wide `--baseline-refresh-interval` below is
  independent and can run alongside.
- **Automatic baseline refresh** — when `--baseline-refresh-interval` is set,
  the panel reports the daemon's last automatic refresh for the selected
  server: how many tables it published, or that it published nothing and why.
  A refusal there is the fail-closed contract working (a capture gap, a schema
  change), not a broken daemon — nothing was overwritten and the next run
  retries. The partial files that run wrote are removed and the daemon log
  names the directory either way, so a table that refuses every interval does
  not fill the disk with unusable snapshot directories
  ([details](dump-and-baseline.md#refreshing-on-a-schedule)). The refresh is
  opt-in on its own: it does not require, and does not enable, the **Create
  baseline** button.

**Protect → Verification** carries the verification runner and the history of
past runs for the selected server.

The Backups summary card that used to point here from Storage is gone (#1543):
Backups has its own entry in the same sidebar, and the pointer only existed
because the page it pointed away from was a drawer.

### The Backup settings page

One page owns every parameter that shapes a backup or a snapshot (#1582),
because no two of them were configured the same way: some are daemon flags,
some are environment variables, some live per server in the registry, and the
precedence between them — per server, then daemon flag, then nothing — was
real and invisible. A server backed by the daemon's `--baseline-dir` showed
an empty Backup dir field, indistinguishable from a server with no backup
location at all.

The page shows the three kinds of setting instead of describing them (#1603).
Two section labels split it: **Change here** and **Set when dbtrail starts**.

- **Backups & disk space** (change here) — the carry-forward toggle, moved
  here from the Backups page; it applies live. What it does is drawn: two
  backups of five tables, the unchanged ones carried across as dashed tiles
  and the changed ones written again, with one sentence under it. The
  local-only rule, the S3 skip count and whose choice the value was sit in a
  compact **More about disk space** block, with links into the docs guide.
- **Per server** (change here) — each registry server's Backup dir, Backup
  S3 and archive toggle, editable in place, with which location is in force
  drawn rather than said: a three-row legend (own location, daemon default,
  no location) with a tick or a cross per lane, and the server's own case
  highlighted under its fields. The daemon default backs time-travel,
  verification and `.sql` exports but backups, restores and the schedule
  refuse, which is the cross on the middle row. The per-server fields left
  the server edit form for this page (the form still round-trips them, so an
  unrelated edit cannot wipe them). A stored schedule that cannot run as
  things stand shows the refusal above the compact block; the schedule
  itself and the full-backup note sit inside it. Save wakes up when a field
  differs from what was loaded.
- **Set at startup** — the nine daemon-wide values (`--baseline-dir`,
  `--baseline-s3`, `--baseline-retain`, `--baseline-refresh-interval`,
  `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE`, `BINTRAIL_CONSOLE_BASELINE_TRIGGER`,
  `BINTRAIL_CONSOLE_BASELINE_STAGING`, `--verify-interval`,
  `--verify-tables`), each shown verbatim with the exact flag or variable
  name, on a plain card with one **Restart to change** chip. An empty value
  reads as the word for what applies (`none`, `off`, `all tables`, `temp
  folder`), never as a fault. Read-only on purpose: the console never edits
  the process's command line or environment. A value the daemon refused
  (today: an invalid lock mode) stays loud under its row.

The per-server half is the whole page on the standalone `serve` console: the
daemon cards describe loops only `watch` runs, but the backup location is
registry state, and this page is its only editor.

### The Retention and This daemon pages

Under `watch` the sidebar grows two settings pages. They were one page,
Storage, which had become a drawer: seven cards from five unrelated concerns
(#1543). They are split by the question each answers.

**Retention** — what happens to your data as it ages:

- **Rotation** — the effective policy (override vs daemon defaults) with an
  edit shortcut to the rotation dialog.
- **S3 archiving per source** — every monitored server with its
  `Archive to S3` destination (or `drop-only` when none), with a shortcut into
  that server's edit form. The boot (cli) index always rotates drop-only.

**This daemon** — what this process can reach, is holding, and sends:

- **AWS credentials** — which credential signals this process sees. Presence
  and the non-secret profile and region names only; no value is ever shown or
  stored.
- **Staged downloads**, and **Usage telemetry**, both described below.

Two cards left the page entirely. **Backups & disk space** moved to the
Backups page beside **Scheduled backups** (#1543), and from there to the
**Backup settings** page (#1582), which owns settings the way
the Backups page owns the work — schedules, runs and downloads stay beside
the data they report on.
**Download a DuckDB schema** moved to the SQL page, from there to
**Connect** (#1549) — `GET /api/views.sql` requires `settings:read`, while the
SQL page is gated on `query:execute` and on the `sql` capability, so the
download was unreachable for a role the endpoint would have authorized, and
gone entirely on a daemon started with `BINTRAIL_CONSOLE_SQL_PANEL=0` — and
finally to **Backups** (#1581), below the snapshot listing: the file describes
the baseline snapshots, and the `.tar.gz` of the very same files downloads
from that page, so the two halves of one task now share it. `GET
/api/baselines` requires the same `settings:read`, so the move loses nobody
the download. On a read-only console (`serve`, where the Backups page does
not exist) the card still renders on Connect. The old `/storage` link still
works and lands on Retention.

- **Backups & disk space** (#1528/#1543, formerly *File reuse for unchanged
  tables*, and before that *Automatic backup refresh*; on the **Backups &
  snapshots** settings page since #1582) — the one behaviour behind
  `--baseline-carry-forward-unchanged`: whether a table with no changes in the
  window keeps its previous Parquet file instead of being written again. It has
  no timetable in it, which the first name promised and which **Scheduled
  backups** on the Backups page actually is. The saving is real and it is
  not free, which is what the name says: where the filesystem allows a hard
  link, two backups then share the same bytes on disk, so deleting the older
  one frees nothing while the newer one still points at it, and a `du` per
  snapshot directory double-counts it (one `du` over the baseline root reports
  the truth). It does not apply when the previous snapshot is read from S3,
  which is what a per-server schedule on an S3-backed server does: linking a
  file needs both ends on a filesystem, so those runs take the ordinary path
  and the daemon log says so. The setting is
  **process-global** (`GET`/`PUT /api/baseline-refresh`) even though it sits
  beside a per-server schedule; the card says so. It is consumed by the daemon-wide refresh interval, by the
  per-server backup schedules, and by point-in-time restores; the card says
  which of those are live. Saving here overrides the daemon flag without a
  restart, and a **Use the default** button then clears the override.
  See [dump-and-baseline.md](dump-and-baseline.md#refreshing-on-a-schedule).
- **Staged downloads**: the `.sql` backups built from the Backups page that
  are waiting on the daemon's disk for their download: each build's server,
  size and download deadline, the total, and where they live. A build is
  removed once downloaded or 4 hours after it finished, so this card is
  usually empty; it exists so that space is never invisible. A build that
  could not be removed stays listed with the reason (a previous build a
  newer one could not clear included) and is retried every minute. Shown
  only on a daemon that can build `.sql` backups.
- **Download a DuckDB schema** (#1528, formerly *Query in DuckDB*; on Backups since #1581, below the snapshot listing) — a one-click download of `views.sql`: a ready-made
  DuckDB schema over the selected server's own Parquet — one
  `state_<schema>_<table>` view per table in the newest baseline snapshot, plus
  an `events` view across every archive source registered in `archive_state`
  when you tick **Include the change log**. It
  is the same file `bintrail views` writes. **The console does not run it.**
  You get a text file; your own DuckDB executes it, in your process, on your
  machine — which is why unrestricted SQL over your lake needs no sandbox, no
  timeout and no result cap here. No credentials appear in the file (S3 uses
  your AWS credential chain), so it is safe to share or commit. Archive
  sources are named for another machine: an archive registered with both a
  local path and an S3 location is listed by its S3 location (state views
  point wherever the server's backup destination points). The S3 secret it
  creates lasts one DuckDB session, so run the file again (`.read views.sql`)
  in each session that reads S3. The state views follow the newest backup: they
  read through the backup folder's `current` pointer, which a NEWER backup
  updates (a point-in-time restore publishes a backup dated in the past, and
  deliberately does not take the pointer), so a scheduled backup reaches an already-downloaded file with
  nothing to download again, and all the views move to it together. What does
  not follow is which views exist and how each `DECIMAL` column is read, both
  decided from the backup named in the file's header, so download again after a
  table is added or dropped or a column changes type. Tick **Pin to the backup
  that exists now** for a fixed point in time instead; a backup destination in
  S3 has no pointer to follow and is always pinned, and the generated file says
  which of the two it did. The download gives you one view per table by
  default; tick **Include the change log** for a view over every archived
  change, which takes longer to open the further back your archive goes because
  it reads a piece of every archived file first. Tick **…and the live index**
  under it before
  downloading to add a second leg over the index itself, covering the recent
  changes rotation has not archived yet (the same thing `bintrail views
  --include-live` adds). It is off by default for two reasons the card states:
  that leg reads the live capture index and cannot narrow the read, so every
  query over it scans the whole table and competes with capture on that
  server; and the file then carries the index host, port, database and user.
  Never its password: the slot is written empty for you to fill in your own
  session. A server whose index this console reaches over a unix socket
  refuses the box, because the file locates the index by host and port so it
  can run on another machine. When the index's registered sources cannot be
  read, the file says so and the daemon logs the reason, so a revoked `SELECT`
  on `bintrail_servers` is diagnosable rather than one sentence in a file.
  The card is hidden for a server with archives disabled, for one with
  nothing archived and no baseline yet, and while an access-control profile
  is active — the file
  maps straight onto the unredacted Parquet a profile exists to withhold.
  For an engine that wants a table rather than files (Spark, Trino, Athena, or
  DuckDB without the merge step), `bintrail export iceberg` writes an Apache
  Iceberg copy of each table from the same baseline plus the change history
  (archives and live index), kept current run after run; it is a scheduler command, not a console feature, and it
  never runs inside `watch`. See [Iceberg export](iceberg-export.md).
- **Usage telemetry** — the current state of dbtrail's metadata-only usage
  telemetry and a one-click opt-out. Turning it off stops this `watch` daemon's
  beacons immediately (no restart) and records the machine-wide choice, exactly
  like `bintrail telemetry off`. When an environment variable (`DO_NOT_TRACK`,
  `BINTRAIL_TELEMETRY`) or the `--telemetry` flag already controls it, the card
  says so and defers to that. Open **Show a sample event** to see the exact
  JSON one event would carry, the same bytes `bintrail telemetry show` prints;
  opening it sends nothing. See [TELEMETRY.md](./TELEMETRY.md).

### The Access profiles page

**Settings > Access profiles** authors the flags, profiles and rules that
`--profile` enforces, from the browser. It is the console path beside the
CLI verbs (`bintrail flag`, `bintrail profile`, `bintrail access`; see
[server-identity.md](server-identity.md#rbac-flags)): the same code runs the
validation and the writes, so a profile authored here is the rows the CLI
would write, refused for the same reasons with the same words. The page is
available on `serve` and on `watch` alike (it is not a daemon feature) and
edits the **selected server's index**, the command-line entry included.

Three panels, top to bottom:

- **Flags** label a table (`schema.table`, column left empty) or one of its
  columns with a name such as `pii` or `billing`. A flag on a whole table hides
  the table from a profile that denies the flag; a flag on one column blanks
  that column and leaves the rest of the row. Adding a flag that exists with
  the same spelling changes nothing; a spelling that differs only by case or
  accents (in the flag, the schema, the table or the column) is refused,
  naming the stored row.
- **Profiles** are named groups of people (`marketing`, `support`). Adding a
  name that exists updates its description; a name that differs from an
  existing profile only by case or accents is refused (the index compares
  names without regard to either). Removing a profile removes its rules with
  it (the page says how many before asking).
- **Rules** say whether a profile may see what a flag covers. Only `deny`
  changes what a query returns; `allow` records intent. Adding a rule for a
  profile and flag that already have one replaces its permission.

This is the one console write that lands in an index database (everything
else the console writes is the local registry file and the daemon's live
settings), so it runs under rules the tests pin:

- reading the page needs `settings:read`; every change needs
  `settings:write`, the permission that already governs console
  administration, so a read-only auditor role sees the configuration and
  none of the buttons;
- while an access-control profile is active the whole page is refused,
  reading included: a console started under `--profile` (a profile with no
  rules yet included) does not edit the rows that profile is built from, and a session that itself carries a data
  profile could lift its own redaction (the flagged tables and columns are
  exactly what its profile withholds), whatever its permissions;
- names are trimmed, and a value longer than its column (64 characters for
  a schema, table or column name, 255 for a flag or profile name) is refused
  with the limit in the message, on the page and on the command line alike;
- every change is recorded on the audit seam as `console/flag.add`,
  `flag.remove`, `profile.add`, `profile.remove`, `access.add` or
  `access.remove`, with the flag, profile or rule it named and the server it
  targeted;
- a change takes effect on a profiled session's next request: the console
  drops that server's cached profile rules when it writes.

An index created before the RBAC tables existed answers `422` here: the
console cannot create tables on an index.

### The SQL panel (removed)

The console used to serve a **SQL** page: a read-only `SELECT` box answered by
DuckDB inside the daemon, over the selected server's Parquet. It was removed in
0.75.0, together with `POST /api/sql`.

Two reasons. It executed SQL in the same process that captures, which is why it
needed a sandbox, a statement gate, two timeout budgets and a single-query
latch. And it was not usable on a real archive: defining the `events` view
opens one Parquet footer per archived file before returning a row, measured at
114.2s over 1886 files, against a 30s setup budget. `SHOW TABLES` — the only
way to learn the derived `state_*` names — built the whole catalog and hit that
budget, and the page's own example named `events`.

Query the same Parquet in your own DuckDB instead. **Download a DuckDB schema**
on the **Backups** page writes a `views.sql` over the same files, with no row
cap, no time limit and nothing running in the daemon. See
[Query in DuckDB](https://www.dbtrail.com/docs/guides/query-in-duckdb/).

`BINTRAIL_CONSOLE_SQL_PANEL` is still read for one release and warns that it no
longer does anything. Remove it.

### Environment variables

- `BINTRAIL_INDEX_DSN` — same as `--index-dsn` (shared with other commands).
- `BINTRAIL_CONSOLE_LISTEN` — same as `--listen`.
- `BINTRAIL_CONSOLE_TOKEN` — same as `--token`.
- `BINTRAIL_CONSOLE_BASELINE_DIR` — same as `--baseline-dir`.
- `BINTRAIL_CONSOLE_BASELINE_S3` — same as `--baseline-s3`.
- `BINTRAIL_CONSOLE_SERVERS` — same as `--servers-file`.
- `BINTRAIL_CONSOLE_AUTH` — same as `--auth-file`.
- `BINTRAIL_CONSOLE_MCP_TOKEN_FILE`: same as `--mcp-token-file`.
- `BINTRAIL_CONSOLE_TLS_CERT` / `BINTRAIL_CONSOLE_TLS_KEY` — same as `--tls-cert` / `--tls-key`.
- `BINTRAIL_CONSOLE_ALLOWED_HOSTS` — comma-separated, same as `--allowed-hosts`.
- `BINTRAIL_CONSOLE_ALLOW_SETUP` — `1`/`true`, same as `--allow-setup`.
- `BINTRAIL_CONSOLE_SQL_PANEL` — retired. The SQL page and `POST /api/sql` were
  removed in 0.75.0 (see [The SQL panel (removed)](#the-sql-panel-removed)). The
  variable is still read for one release and warns that it does nothing; a later
  release stops reading it.
- `BINTRAIL_CONSOLE_ARCHIVE_STAGING` (`watch` only) — local staging dir for the
  Archive-to-S3 feature, same as `--archive-staging-dir`. AWS credentials for
  the upload come from the ambient chain (`AWS_*` / `~/.aws` / role).
- `BINTRAIL_CONSOLE_BASELINE_TRIGGER` (`watch` only) — `1`/`true` enables the
  **Create baseline** button (runs `mydumper` → convert → upload in-process;
  see [Protect → Baselines](#the-protect-pages)). Off by default for a bare
  `watch` invocation; the bundled compose stack sets this on by default (see
  [docker.md](docker.md) — `BASELINE_TRIGGER=0` in `.env` opts out there).
- `BINTRAIL_CONSOLE_BASELINE_STAGING` (`watch` only) — local staging dir for
  S3-destined baselines created by that button (default a temp subdir).
- `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` (`watch` only) — `ftwrl` (default),
  `lock-all`, `safe-no-lock` or `no-lock`. Selects how mydumper synchronizes its worker
  threads onto one instant for the **Create baseline** button. The default is
  point-consistent and needs `RELOAD`/`FLUSH_TABLES` (plus `BACKUP_ADMIN` on
  MySQL/Percona 8.0+). `lock-all` is also point-consistent and needs only
  `LOCK TABLES` — **the mode to use on RDS/Aurora**, where `BACKUP_ADMIN`
  cannot be granted and `ftwrl` therefore cannot run. `safe-no-lock` needs no
  elevated privilege but aborts rather than write a torn snapshot; `no-lock`
  accepts one. A baseline is the seed state
  `reconstruct` merges deltas onto, which is why the weaker modes must be named
  explicitly. An unrecognized value disables baseline DUMPS only — capture and
  the periodic refresh keep running.
- `BINTRAIL_CONSOLE_NOTIFY_WEBHOOK` (`watch` only) — same as
  `--notify-webhook`: URL for JSON notifications on lost continuity, verify
  problems, and unhealthy rotation (see
  [Webhook notifications](#webhook-notifications)).
- `BINTRAIL_CONSOLE_VERIFY_INTERVAL` (`watch` only) — same as
  `--verify-interval`: enables scheduled verification on that cadence
  (e.g. `24h`, `7d`; see
  [Running verification from the console](#running-verification-from-the-console)).
- `BINTRAIL_CONSOLE_VERIFY_TABLES` (`watch` only) — same as `--verify-tables`.
- `BINTRAIL_CONSOLE_VERIFY_TRIGGER` (`watch` only) — `1`/`true` enables the
  **Protect → Verification** page (runs `bintrail verify` in-process;
  see [Running verification from the console](#running-verification-from-the-console)).
  Off by default for a bare `watch` invocation; the bundled compose stack sets
  this on by default (see [docker.md](docker.md) — `VERIFY_TRIGGER=0` in
  `.env` opts out there).
- `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` (`watch` only) — same as `--flashback-listen`
  (e.g. `127.0.0.1:3308`): serve an embedded MySQL-protocol time-travel port for
  every monitored server, routed by the connection username. Off by default;
  requires a console token. See [Time-travel over the MySQL protocol](#time-travel-over-the-mysql-protocol-flashback-port).

There is deliberately **no** environment variable for the password itself —
env vars leak through `docker inspect`, `ps e`, and `/proc`; the password is
set interactively or via `--password-stdin`, never inlined.

Precedence is the usual CLI flag > environment variable > default. The
`BINTRAIL_CONSOLE_*` variables apply equally to `bintrail-console watch` (where
the matching flags are `--console-listen`, `--console-token`, `--baseline-dir`,
`--baseline-s3`, `--console-servers-file`, `--console-auth-file`,
`--console-mcp-token-file`, `--console-tls-cert`, `--console-tls-key`,
`--console-allowed-hosts`, `--console-allow-setup`).

## Password login

**Username + password is the primary way in.** On a fresh loopback console
with no credential, the first browser visit shows a **"create your password"**
screen; you set it once and you're signed in. Every later visit is a normal
sign-in. (Prefer the terminal, or setting it up before first launch? Run
`bintrail-console user set-password`.)

```console
$ bintrail-console user set-password
New console password: ********
Retype to confirm: ********
Console password set for user "admin" (~/.config/bintrail/console-auth.yaml).
A running server accepts it on the next login; no restart needed.
```

- **First-run setup is loopback-only.** The unauthenticated `POST
  /api/auth/setup` endpoint (which the "create your password" screen calls)
  is enabled only on a loopback bind — where reaching it already implies local
  access — and **self-disables the instant a password exists**. It is also
  enabled by `--allow-setup` (`BINTRAIL_CONSOLE_ALLOW_SETUP`), the assertion
  the Docker stack makes because it binds `0.0.0.0` inside the container but
  publishes the port on the host's loopback only. A non-loopback bind with no
  credential and no `--allow-setup` is refused — set the password from the
  shell first.
- The credential lives in a 0600 YAML file (`version`, `username`, a
  bcrypt-cost-12 `password_bcrypt`, `updated_at`) — same envelope and atomic
  write as the server registry. One user; multi-user/RBAC/SSO is dbtrail.
- A successful login (or first-run setup) mints an **in-memory session token**
  (24 h absolute, 8 h idle, max 16 concurrent) the SPA uses as its Bearer
  credential. Sessions die on logout, on password change (which revokes all
  of them), and on process restart — nothing session-shaped touches disk.
- The login response also sets the session token as an **HttpOnly
  `bintrail_session` cookie** (`Secure; SameSite=Lax; Path=/`, `Max-Age`
  matching the session's absolute expiry), so opening a console link in a new
  tab or window is already signed in — same store, same expiry, same
  revocation as the Bearer path; the cookie holds nothing new. Logout revokes
  the session server-side *and* expires the cookie, killing every tab at once.
  The `Secure` flag is safe on the local first-run flow (browsers treat
  loopback as a secure context); **operators terminating TLS at a reverse
  proxy** should serve the console over `https://` end-to-end from the
  browser's point of view, or the browser will drop the cookie and each tab
  falls back to its own login (the Bearer flow keeps working either way).
- **An opt-in static token** for automation: set `--token` /
  `BINTRAIL_CONSOLE_TOKEN` explicitly and scripts/curl/CI can call the API with
  it. It is never generated for you, and it is not the human path — when a
  token *is* set, the browser never sees the setup screen (the token is the
  credential).
- Login, setup, and password-change are throttled (per-IP 5 failures/min and
  20/15 min, 30/min globally, `Retry-After` on 429) and bcrypt-verified in
  constant time with no username enumeration. Loopback peers are exempt from
  the **global** window only — the deliberate no-self-DoS guarantee for the
  on-host operator; the per-IP windows still apply. There is no lockout — locking
  the single user out would hand an attacker a denial-of-service against the
  operator.
- **Rotate or reset:** from the UI (⌘K → "Change console password", revokes
  every other session immediately) or re-run `user set-password` (overwrites;
  applies on the next login, live sessions ride out their TTL). **Forgot the
  password?** Shell access is the recovery path — re-run `user set-password`.
  `user remove` deletes the file (a loopback console then returns to first-run
  setup; a non-loopback one refuses its next restart until a credential is set
  again). `user status` shows what is configured without printing secrets.
- Off-loopback password logins over plain HTTP are warned about at startup:
  use `--tls-cert`/`--tls-key` or terminate TLS at a reverse proxy (with
  `--allowed-hosts`).

### External login providers

Embedding distributions — builds that construct their console binary from the
importable `consoleapp` package (`cmd/bintrail-console` is a thin `main()`
over `consoleapp.Main`) — may install an external login flow (e.g. OIDC
single sign-on) through the `ext.ConsoleAuth` seam: call `ext.SetConsoleAuth`
once from `main()` before `consoleapp.Main`, like `ext.SetAuditSink`. When a
provider is installed, the sign-in screen adds a **"Continue with \<name\>"**
button, and a successful external login mints the same in-memory session a
password login does (same lifetime, logout, and revocation). An installed
provider also counts as a valid sole credential for a non-loopback bind —
first-run browser setup stays loopback-gated regardless. The standalone
`bintrail-console` binary has no provider installed: the button never
appears, and the provider routes (`/api/auth/ext/*`) simply require a normal
credential like any other `/api` path.

### Extension views

Embedding distributions — builds that construct their console binary from the
importable `consoleapp` package — may add one additional view to the console
through the `ext.ConsoleView` seam: call `ext.SetConsoleView` once from
`main()` before `consoleapp.Main`, like `ext.SetConsoleAuth`. An installed view
contributes a nav item, a frontend module, and its own authenticated data API;
the console reveals the nav item, routes to it, and loads the module in the same
page (same origin, not an iframe).

The view's static assets are served **unauthenticated** at `/ext/<id>/` (the
code always ships, like the console's own `app.js`), while its data routes at
`/api/ext/<id>/` require the same bearer credential as every other `/api` path
and are **refused while an access-control profile is active** (the console can't
guarantee a third-party handler honors table-deny / column-redaction rules, so
it withholds the whole surface under a profile). Each data route reads the index
of the server currently selected in the switcher, with the operator's profile
applied.

The module must export `render(mount, ctx)`. `ctx` is built in one place
(`extContract` in `app.js`) and is the same for both extension surfaces:

| key | what it is |
|---|---|
| `apiBase` | the extension's own data-route prefix (`/api/ext/<id>/`) |
| `api` | the console's authenticated fetch — bearer token and `X-Bintrail-Server` already applied. An HTTP error throws an `Error` carrying `.status`; a malformed body throws one **without** it, an aborted request rejects with `AbortError`, and a 204 resolves to `null`. Branch on `.status` defensively. |
| `ui.dateField(label, name, size, placeholder, required)` | the console's own date field: a text input plus the calendar/clock popover, returning the same `.field` wrapper the console's forms use |

`ui` exists so an extension does not reimplement a widget the console already
has — two copies drift, and the operator ends up looking at two different date
pickers in one console. It is also the boundary of what may be relied on:
`app.js` is a classic script, so an extension running same-origin *could* reach
any of its functions as a window global, but only what arrives through `ctx` is
a promise. Everything else may be renamed without notice, and because the two
sides are built in different repos and never compile together, such a rename
would surface as a widget that quietly stopped appearing rather than as an
error. An extension that wants to run against older console builds should
feature-detect (`typeof ctx.ui?.dateField === "function"`) and degrade. Two
things that detection does not give you. It establishes **presence, not
shape** — a build whose builder took different arguments would still answer
`"function"` — so treat the signature above as the contract and expect it to
be versioned with the console, not sniffed. And `render()` is called but **not
awaited**: a synchronous throw is caught and rendered as an error in the
mount, while an `async render()` that rejects escapes as an unhandled
rejection and leaves the mount blank. An extension doing async work in
`render` should catch its own failures and render them, rather than relying on
the console to.

The standalone `bintrail-console` binary ships **no extension views**: no nav
item appears, `/api/capabilities` advertises none, and `/ext/*` and
`/api/ext/*` are absent from the router entirely.

## Serving the console on a hostname

The console binds `127.0.0.1:8090` by default, which is the right default and
the wrong one for a team. Putting it on `console.example.com` is four things:
a DNS record, a certificate, a listener, and one header rule that is the only
part people get stuck on.

**1. DNS.** An `A` (or `AAAA`) record for the hostname pointing at the host's
public address. Nothing about the console is involved.

**2. The firewall.** Open 443 to the clients that need it. Leave 8090 closed:
nothing outside the host should reach the console's own port.

**3. TLS.** Two shapes, and neither is more supported than the other.

*The console terminates TLS itself:*

```
bintrail-console serve --index-dsn '<dsn>' \
  --listen 0.0.0.0:8090 \
  --tls-cert /etc/ssl/console.crt --tls-key /etc/ssl/console.key \
  --allowed-hosts console.example.com
```

*Or a reverse proxy terminates TLS* and the console stays on loopback — the
usual choice when the host already runs a web server, and the one that gets you
automatic certificate renewal for free:

```nginx
server {
    listen 443 ssl;
    server_name console.example.com;

    ssl_certificate     /etc/letsencrypt/live/console.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/console.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8090;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Backup and SQL-export downloads are written straight to the
        # response as they are built. With buffering on, nginx spools the
        # whole archive to disk before sending a byte of it. The /mcp
        # endpoint needs this line too: an AI client connecting through a
        # buffering proxy sees its stream held back and stalls or drops
        # during the handshake.
        proxy_buffering off;
    }
}
```

If a large export returns `504 Gateway Time-out`, that is nginx's
`proxy_read_timeout` (60 seconds by default) elapsing while the console works
before its first byte, not the console failing — raise it on this `location`.

**4. The header rule, which is where the time goes.** With the vhost above the
console answers:

```
HTTP 403  {"error":"forbidden: host not allowed"}
```

That is the DNS-rebinding defence doing its job, not a misconfiguration. The
console accepts a `Host` header only if it is `localhost`, an IP literal, or a
name you listed — so `console.example.com` has to be listed:

```
--allowed-hosts console.example.com
# or: BINTRAIL_CONSOLE_ALLOWED_HOSTS=console.example.com
# on `watch`, the flag is --console-allowed-hosts
```

Why the defence exists: a browser on someone's laptop can be pointed at a
hostname that resolves to `127.0.0.1`, and without a Host check that page could
drive a console the attacker cannot reach directly. The allowlist costs one flag
and closes it.

There is a way to skip the flag, and it is worth knowing about mostly so you
recognise it. nginx's *default* — with no `proxy_set_header Host` line at all —
sends the upstream address as the `Host`, i.e. `127.0.0.1:8090`, which is an IP
literal and therefore always allowed. A vhost written that way works
immediately and never mentions `--allowed-hosts`. It also means every request
reaches the console claiming to be for `127.0.0.1`, so the real hostname is
absent from the console's own logs, and any future behaviour that depends on
knowing its public name has nothing to work with. Prefer passing the real
`Host` and listing it.

Two consequences worth stating:

- **`/mcp` rides the same allowlist.** Once the hostname is allowed, an
  MCP client can reach the console's endpoint at
  `https://console.example.com/mcp` — which removes the need for a tunnel or a
  port-forward to use it from elsewhere. It is behind the same credential as
  everything else; see [MCP endpoint](#mcp-endpoint).
- **A non-loopback console refuses to start without a credential.** That is
  deliberate and it is checked before any of the above matters: set a password
  (or pass `--token`) first, or the process exits. See
  [Password login](#password-login).

## Security model

The binary has no Supabase/RBAC backend to lean on, so the console defends
itself:

- **Loopback by default + a credential required.** On a loopback bind the
  console prompts you to create a password on first run — no credential is ever
  auto-generated for you. Binding to a non-loopback address (`0.0.0.0`, a LAN
  IP, …) **requires** an explicit `--token` or a configured console password, or
  the command refuses to start.
- **Constant-time credential checks** (`crypto/subtle` for the token;
  sessions are looked up by SHA-256 of the presented value, so raw session
  tokens never live server-side; unknown-username logins still burn a full
  bcrypt compare so timing cannot enumerate the username).
- **Bearer header first; the session cookie is the tab-bridging sibling.**
  `/api/*` accepts the credential (static token or login session) in the
  `Authorization: Bearer <token>` header, or — for browser navigation — the
  HttpOnly session cookie a login sets. A request carrying a Bearer header is
  judged on that header alone (no cookie fallback), so scripted access is
  unchanged. Cookie-authenticated **state-changing** requests (anything but
  GET/HEAD/OPTIONS) must additionally send `Content-Type: application/json`,
  which an HTML form cannot produce — combined with `SameSite=Lax`, a
  cross-site form POST cannot carry the ambient cookie to `/api/recover`.
  Login itself requires `Content-Type: application/json` too — login-CSRF
  dies the same way. The page shell loads without a token (it reads the token
  from the URL, or probes the session cookie, to bootstrap its requests).
- **No CORS headers.** Requests are same-origin only.
- **Host-header allowlist.** Requests whose `Host` is a domain name are
  rejected (only IP literals and `localhost` pass), which defeats DNS-rebinding
  attacks against the local bind.
- **Static security headers** on every response: `Referrer-Policy:
  no-referrer` (keeps the `?token=` bootstrap URL out of Referer headers),
  `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`.
- **Brute-force throttling** on the two bcrypt-verifying endpoints (login and
  change-password), counting failures per client IP plus a global window.
  Loopback socket peers are exempt from the **global** window only (per-IP
  windows still apply), so a remote attacker can never rate-limit the on-host
  operator out. The exemption keys on the real socket peer —
  `X-Forwarded-For` / `X-Real-IP` are never trusted — so it cannot be spoofed
  remotely. Consequence: behind a same-host reverse proxy the socket peer *is*
  loopback, so the global 30/min backstop does not apply and only the per-IP
  buckets throttle — and all proxied clients share that one bucket; rate-limit
  at the proxy if that matters to you.
- **Result caps.** Every query is bounded — events default 100 (max 1000),
  recover default 1000 (max 10000). Never unlimited.
- **Unauthenticated endpoints**: `/api/healthz` (liveness), `GET /api/auth`
  (does password login exist — what the login form's presence would reveal
  anyway), `POST /api/auth/login`, and — only during first-run setup —
  `POST /api/auth/setup`. Everything else needs a Bearer
  credential or the login session cookie.

## PostgreSQL sources

The console reads only the **index**, never the source database, so it works
identically for a PostgreSQL source captured by `bintrail-pg`: the index schema
is the same. It does adapt its **presentation** to the source family (reported
per server as `source` in [`/api/capabilities`](#api), derived from
`stream_state.flavor`):

- **Stream vocabulary.** A PostgreSQL stream shows its cursor as an **LSN** (and
  labels the source "PostgreSQL · logical replication") instead of MySQL binlog
  file / position / GTID. Slot and publication *names* are capture-side
  configuration and are not stored in the index, so the console does not show
  them.
- **Permanent-loss badge.** The Status page surfaces the durable loss record
  (`stream_state.gap_lost_at`) — for PostgreSQL, an invalidated/lost replication
  slot; for MySQL, an unfillable binlog gap. The index is valid only up to that
  point and capture must be re-baselined to resume.
- **Connection-id note.** PostgreSQL logical replication (`pgoutput`) carries
  no backend connection id, so `connection_id` is empty for PostgreSQL sources
  — no console or capture setting can add it. The Events page says so for
  PostgreSQL sources rather than leaving it an unexplained gap.
- **Replication-health panel.** The Status page shows the replication slot's
  WAL-retention state (`wal_status`, retained WAL, the safe margin before
  invalidation) and whether every published table is at `REPLICA IDENTITY FULL`.
  The console is still index-only: it never queries the source. Instead the
  streaming daemon (`bintrail-pg stream` / `watch`) polls the source every ~30s
  and persists a snapshot to the index (`stream_state.source_health`), which the
  console renders. Because a snapshot can outlive a stopped daemon, the panel
  shows **how recently it was checked** and **degrades a stale snapshot** (older
  than ~90s) to muted with a warning — a frozen "reserved" must never read as
  live-healthy. If the daemon cannot read the source at all (for example a
  standby, where the slot-retention metrics are unavailable), the panel shows
  **probe failing** with the reason rather than disappearing. For an on-demand,
  always-live check, use `bintrail-pg doctor`.

## MCP endpoint

The console serves the same six read-only MCP tools as
[`bintrail-mcp`](mcp-server.md) — `query`, `recover`, `recover_cascade`,
`reconstruct`, `status`, `list_schema_changes` — over **Streamable HTTP**, on
both `bintrail-console serve` and `bintrail-console watch`:

| URL | Target |
|---|---|
| `/mcp` | The console's **default server** (same selection rules as the browser UI). |
| `/mcp/{id-or-name}` | A named server from the registry (`default` = the command-line entry). Unknown → `404`. |

MCP clients cannot reliably send custom headers, so the server choice lives in
the URL path (mirroring how the [time-travel port](time-travel-sql.md) routes
by username) instead of the `X-Bintrail-Server` header.

Point any Streamable-HTTP-capable MCP client at it with the console token as a
Bearer credential:

```json
{
  "mcpServers": {
    "bintrail-console": {
      "type": "http",
      "url": "http://127.0.0.1:8090/mcp",
      "headers": { "Authorization": "Bearer <console token>" }
    }
  }
}
```

Rules that differ from the standalone `bintrail-mcp` server:

- **A token is required.** Password login is a browser credential and cannot
  authenticate a headless MCP client, so `/mcp` needs either the static
  `--token` / `BINTRAIL_CONSOLE_TOKEN` **or a managed MCP token generated
  from Settings → Connect AI** (#1052) — one click from an authenticated
  browser session, no flags, no restart. The managed token is persisted as a
  SHA-256 hash only (`~/.config/bintrail/console-mcp-token.yaml`, or the
  path given to `--mcp-token-file` / `BINTRAIL_CONSOLE_MCP_TOKEN_FILE`; `0600`,
  atomic write, versioned envelope with the registry's read-only-if-newer
  contract), its plaintext is shown exactly once at generation, and it is
  **scoped to `/mcp` alone** — it cannot drive the browser API (registry
  CRUD, monitor verbs, or its own rotation). New token / Delete token from
  the same card take effect on the next request, including for sibling console
  processes sharing the file: every `/mcp` request re-validates the
  credential, so a rotated-away or revoked token stops authenticating
  immediately. An MCP *session* is additionally bound to the credential that
  created it — no other token can continue it, so a session opened by a
  since-rotated token is orphaned outright (its already-open stream can idle
  out but never be driven again) and is discarded after the idle timeout:
  sessions with no request for **30 minutes** are closed (clients keep a
  session alive with pings, or transparently re-initialize).
- **A managed token carries the grants of the session that minted it.** Each
  tool call requires the same permission as its `/api` counterpart — `query`
  and `list_schema_changes` need `query:execute`, `recover` needs
  `recover:execute`, `reconstruct` needs `reconstruct:execute`, `status`
  needs `status:read` — checked against the permission set recorded into the
  token file at mint time, so a session cannot mint its way past what the
  API would refuse it directly. A call the token's grants do not cover fails
  with a tool error naming the missing permission. Tokens minted by a
  full-access session (the static token, a password login — every session in
  a plain OSS install) record no cap and keep the full read surface, and
  **tokens minted before grants were recorded behave the same** (they were
  all minted by full-access sessions); rotate the token to stamp the current
  session's grants. The static `--token` / `BINTRAIL_CONSOLE_TOKEN` is
  environment-owned and always full-access.
- **`index_dsn`, `profile`, `baseline_dir` and `baseline_s3` tool parameters are
  rejected.** Connections, the baseline location and the RBAC posture are all
  managed by the console process — an authenticated MCP client cannot point the
  console at an arbitrary DSN or storage prefix, nor change redaction rules.
- **`reconstruct` is gated per server**, on the same signal as the Time-travel
  tab and `/api/reconstruct`: that server needs a baseline location configured,
  with archives enabled and no access-control profile active (baseline reads
  aren't redacted). Otherwise the tool refuses with that explanation.
- **The console's read boundary applies.** Result caps match the API (events
  100 default / 1000 max, recover 1000 / 10000), each server's archive and
  baseline posture is honored, and `query_text` / `query_hash` are withheld
  from query results exactly as on the events API.

The host-header allowlist (`--allowed-hosts`) covers `/mcp` like every other
route.

The UI's **Settings → Connect AI** page assembles all of this for you: the
ready-to-copy `/mcp` URL for the selected server (the per-server form when
more than one server is registered), the `.mcpb` bundle download for the
running version, and the raw-config fallback above. Its **Access token** card
generates, replaces (**New token**), and deletes the managed MCP token; the plaintext is
displayed exactly once, at generation, and never stored. For the
start-to-finish walkthrough (bundle install included), see
[Connect an AI assistant](connect-ai.md). Below the three steps, the
**Connect a SQL client** panel does the same for the embedded time-travel SQL
port: see [Time-travel over the MySQL protocol](#time-travel-over-the-mysql-protocol-flashback-port).

## API

All endpoints return JSON except `GET /api/views.sql`, which serves a SQL file. `/api/*` (except `healthz`) require
`Authorization: Bearer <token>`.

| Method & path | Purpose |
|---|---|
| `GET /api/healthz` | Liveness probe (no token). |
| `GET /api/auth` | Auth mode (no token): `{"password_login": bool, "setup": bool}` — `setup` true means first-run create-password is open. |
| `POST /api/auth/login` | Exchange `{username, password}` for a session: `{token, expires_at}`. Rate-limited; requires `Content-Type: application/json`. |
| `POST /api/auth/setup` | First-run only (loopback / `--allow-setup`, self-disables once a password exists): create the password, returns a session. |
| `POST /api/auth/logout` | Revoke the presented session (static token → 204 no-op). |
| `POST /api/auth/password` | Set (first time; requires static-token auth) or rotate (`current_password` verified) the console password. Revokes all sessions and returns a fresh one. |
| `GET /api/status` | Index status (same payload as `bintrail status --format json`). For a session with restricted data access, the capture-health detail names only the tables that session may read; `tables_withheld` counts the rest and the counts stay whole. |
| `GET /api/capacity` | The doctor's index disk-capacity check for the selected server: `{status, reason, retention: {known, retain, source, enabled}, measured, sample_hours, current_bytes, events_per_day, bytes_per_event, growth_bytes_per_day, projected_bytes, remaining_bytes, free_known, free_bytes, days_until_full}`. `status` is `pass`/`warn`/`fail`/`skip` as `bintrail doctor` grades it; `reason` names the branch (`ok`, `headroom_low`, `free_under_floor`, `growth_exceeds_free`, `no_retention`, `free_unknown`, `retention_unknown`, `not_enough_history`, `not_initialized`). Rate and projection fields are absent while `measured` is false; `free_bytes` is meaningful only when `free_known`; `retention.known` is false on the standalone console. `502` when the partition statistics cannot be read. |
| `GET /api/coverage` | Live RPO summary: restorable delta window `[delta_from, delta_to]`, `lag_seconds`, `continuity`; with a baseline source, `full_table_status` (`ok`/`unknown`), `full_table_from`, `broken_tables`, `unreachable_tables`, `restore_reads`, `unevaluable_tables` and `restore_needs_local` (profile-restricted sessions get the delta half only). The full-table half is graded across **every** configured backup location, but `full_table_from` names only anchors the console's own Restore can fold from, and Restore folds from ONE location: the server's S3 backups when it has an S3 destination, else its local backup directory, the same rule the scheduled update follows ([#1541](https://github.com/dbtrail/dbtrail/issues/1541)); `restore_reads` (`s3`/`dir`) says which the card graded against; it is empty when the server has no local directory of its own, since Restore refuses such a server outright, and `inherited` when the server names no location of its own and the card graded the daemon-wide ones (a backup is there and Time-travel reads it, but Restore refuses that server as well, #1602). A restore at the exact second of a backup the bucket already holds is refused rather than overwriting it. A table whose only usable backup is in the other location is listed in `unreachable_tables` instead: not counted toward the window, and not in `broken_tables`, since the backup exists and Time-travel still reads it (Time-travel reads local first and falls back to the bucket). On a `dir` server that is a table backed up only in a daemon-wide bucket; on an `s3` server it is a table backed up only on this host, which the daemon-wide refresh interval and a failed upload both produce. On a `dir` server a table that DOES have a local copy stays in `broken_tables` even when a fresher one sits in the bucket: the S3 fallback fires only when the local location holds nothing at or before the instant, so a stale local copy shadows the fresh offsite one and no console surface reaches it; on an `s3` server the same shape is restorable, because Restore folds the fresh copy from the bucket, and the mirror shape is what stays `broken` there: a stale bucket copy with a fresh copy only on this host, since the fold anchors on the bucket copy and the fresh one was never sent up. On a server that backs up only to S3 there is no local directory to fold into, so `restore_needs_local` is true and `unreachable_tables` is left empty rather than naming every table. `unevaluable_tables` names the tables that forced `full_table_status: "unknown"`: their newest backup predates a floor whose archives cannot be attributed to one source, so it may or may not still be covered (see the ambiguity demotion in `bintrail status`). Too uncertain to call broken, and too specific to leave unnamed. `restore_needs_local` reports that no configured backup location is a local directory; note a server inheriting the daemon-wide `--baseline-dir` is not detected by it, and Restore refuses that server for a different reason (#1602). |
| `GET /api/activity` | Window aggregate behind the Overview tiles: counts by event type, distinct tables touched, and a per-table breakdown. The window **is the live retention** — derived from the oldest live `binlog_events` partition, so the counts cover exactly what the live index still holds and read the live tier only (no archive scan, and nothing archived can fall inside the window by construction). Returns `{label, since, until, refreshed_at, total, inserts, updates, deletes, other, tables, top_tables, complete, notes}`. The aggregate is a **server-side materialization** refreshed when older than ~30 minutes (a stale copy is served immediately while one recompute runs in the background); `refreshed_at` is when it was computed, and the UI renders it on the tiles ("as of …") so a cached number is never presented as live. `complete: false` means the counts are knowably a floor (an index with a pathological table count trips the grouping cap) and `notes` says so; the UI marks the affected tiles "partial". RBAC deny rules are applied, so a denied table contributes to neither the counts nor `top_tables`, and each deny profile gets its own materialization. |
| `GET /api/schemas` | Schemas known to the index: those observed in `binlog_events` **plus** those in the latest schema snapshot, so a schema whose partitions have all been rotated out to Parquet/S3 is still listed (the archives still answer `/api/events` and `/api/recover`). `schemas` is that full union; `snapshot_only` (when present) is the subset with no live events observed — the UI labels these "snapshot only" since queries against them may return nothing; `snapshot_unavailable: true` means the snapshot half was skipped because the schema resolver failed to load (check the server log), so archive-only schemas may be missing from the list. The snapshot half is skipped under `--no-archive` or an active `--profile`, where archived data is unreachable anyway. Note this answers *which schemas this index knows of*, not *which have data in a given window* — for that, see `bintrail status`'s continuity verdict. `?schema=<name>` → that schema's tables. |
| `GET /api/events` | Event browser. Query params: `schema, table, pk, event_type, gtid, since, until, changed_column, order, limit, limit_per_pk` plus the `after`/`before` keyset cursors. `limit_per_pk` keeps only the latest N events per row, requires `pk`, and is **refused alongside a cursor** — it is a whole-result-set cap, so paging would re-anchor it to each page's remainder. `scope=live` serves the **live index only** and answers immediately (the UI's phase 1: rows in `binlog_events` are milliseconds away, an archive scan can take tens of seconds); the response then carries `scope: "live"` and `archives_pending` (never omitted — `false` is a meaningful answer) — `true` means registered archives were **not** read and a follow-up full read is required before the list is complete (the warning says so, loudly); `false` means no follow-up read would add anything: either nothing is registered, or the archives are excluded for this console/session (a session profile always announces itself; a --no-archive console announces only when the window has gaps to point at). Anything else in `scope` is a 400, never a silent full read. |
| `GET /api/schema-changes` | DDL history from the index's `schema_changes` table. Query params: `schema, table, ddl_type, since, until, limit`. `ddl_type` is one of `CREATE`, `ALTER`, `DROP`, `RENAME`, `TRUNCATE`, matched as a prefix of the stored type (`ALTER` matches `ALTER TABLE`), like the MCP `list_schema_changes` tool. Default limit 100, max 1000; `has_more` says whether the cap cut the list. Ordered by `detected_at, binlog_file, binlog_pos, id`, all descending, so DDLs detected in the same second keep their binlog order. Returns `{changes: [{id, detected_at, schema_name, table_name, ddl_type, statement, binlog_file, binlog_pos}], count, limit, has_more}`. The session's table deny and allow rules scope the rows by the table the index attributed each statement to (a statement naming several tables is attributed to the first). Under an active access profile (a named profile, session restrictions, or the startup `--profile`) `statement` is empty on every row and `statement_withheld: true` says so, with `warnings` naming the scoping and the withholding. `422` when the index has no `schema_changes` table (an index provisioned before DDL tracking; `bintrail init` adds it). |
| `POST /api/recover` | Undo-SQL generation. JSON body with the same filter fields (requires at least `schema`; an `order` field is accepted but ignored — recover always processes oldest-first). `limit_per_pk` reverses only the latest N events for the matched row and **requires `pk`** — it is the only filter that can separate events sharing a timestamp, since `since`/`until` are second-granular (`/api/events` accepts it too, so Restore's preview can mirror the same window). Returns `{sql, statement_count, row_count, warnings, notes, generated_in_ms}`. `generated_in_ms` is the wall time from request-body decode to the finished script: filter parsing, session-profile resolution, the event fetch including any archive/Parquet leg, cascade victim synthesis when auto-detected, and SQL rendering. It **excludes** selecting and opening the target server's connection, which is a one-off cost of switching servers and can dominate a first request. Always present: `0` means the script was generated in under a millisecond, not that timing is unavailable. When the target is a foreign-key **parent** whose `DELETE` cascaded below the binlog (MySQL/MariaDB index only), cascade victims are **auto-detected** and folded into the same script; the response then also carries `{cascade_detected, victim_count, set_null_count}` (see [Recover and cascade](#cascade-recovery)). |
| `POST /api/recover-cascade` | Cascade-recovery SQL generation (reverse FK `ON DELETE CASCADE` / `SET NULL` side effects). JSON body: `schema, table` (the **parent**), `pk, pks, since, until, lookback, max_depth, allow_incomplete`. Returns `{sql, statement_count, victim_count, set_null_count, complete, incomplete, generated_in_ms}` — text only, never executed. Returns `403` under an active RBAC redaction profile (see [Cascade recovery](#cascade-recovery)). |
| `GET /api/capabilities` | Reports enabled optional surfaces for the **selected server**, e.g. `{"reconstruct": true, "recover_cascade": true, "recover_cascade_baseline": false, "source": "mysql", "auth": {"password_set": true, "auth_kind": "session"}}`. The frontend uses it to show/hide gated tabs on every switch (`reconstruct` → Time-travel) and to gate the logout affordance (`auth_kind` says how this request authenticated). `recover_cascade` reports whether cascade synthesis is available (false under an RBAC redaction profile) — it gates the standalone `POST /api/recover-cascade` endpoint; the **Recover** tab's auto-detection follows the same server-side rule. `source` (`"mysql"` or `"postgresql"`, read from the index's `stream_state.flavor`) drives **source-aware presentation** only — never a gate; see [PostgreSQL sources](#postgresql-sources). |
| `GET /api/reconstruct` | Single-row point-in-time reconstruct (baseline-gated **per server**; 404 when not configured). Query params: `schema, table, pk, at, history, allow_gaps`. Returns `{found, deleted, state, history, baseline_time, event_count, warnings}`. |
| `GET /api/servers` | List servers (masked: parsed host/port/user/dbname + `has_password`, never a DSN or password) plus `default_id`. |
| `POST /api/servers` | Add a server to the registry (validates, does not connect; never runs DDL). |
| `GET /api/servers/{id}` | One masked entry (prefills the edit form). |
| `PUT /api/servers/{id}` | Edit. Omitted password = keep stored; `""` = clear; value = replace. `409` for the command-line entry. |
| `DELETE /api/servers/{id}` | Remove from the registry and close its cached connection. `409` for the command-line entry. |
| `POST /api/servers/{id}/test`, `POST /api/servers/test` | Write-free reachability probe (short timeout): `{ok, server_version, dbname, latency_ms, has_index, schema_current}`. Accepts an unsaved candidate body; with `{id}`, a blank password merges the stored one. |
| `POST /api/servers/{id}/monitor/start` | Supervisor only (403 on the standalone console): doctor preflight → on green, record intent + provision + stream. Returns `{doctor, started, monitor}`. |
| `POST /api/servers/{id}/monitor/stop` | Supervisor only: clear intent, drain the stream (final checkpoint), release the advisory lock. |
| `GET /api/servers/{id}/monitor` | Supervisor only: `{monitor: {state, last_error, since}}` — `stopped\|pending\|running\|stalled\|lost_position\|failed`. |
| `GET /api/rotation` | Effective global rotation policy: `{retain, interval, add_future, source, enabled}` — `source` is `"override"` (console-saved) or `"default"` (daemon `--rotate-*`). |
| `PUT /api/rotation` | Supervisor only (403 on the standalone console): save a global rotation override `{retain, interval, add_future}` (validated; `off` rejected). Applies live on the next cycle. |
| `GET /api/baselines` | Read-only listing of the **selected server's** baseline snapshots, grouped per snapshot: `{configured, source, kind, reconstruct, snapshots: [{time, age_hours, tables, binlog_file, binlog_pos, gtid_set}]}` (coordinates local-only, capped at 50 snapshots). `502` when the configured source is unreadable. |
| `GET /api/views.sql` | **Not JSON** — a `text/plain` DuckDB schema over the selected server's Parquet (the same output as `bintrail views`), served as a `views.sql` attachment. Nothing is executed here; the file runs in your own DuckDB. `?include_events=1` adds the `events` view over the archived change log, which is left out by default because defining it opens one Parquet footer per archived file (`bintrail views --include-events`). `?include_live=1` adds the leg over the live index (`bintrail views --include-live`), with the index host, port, database and user in the file and never its password; it requires `include_events=1`, since the leg hangs on that view, and 400s without it. 404 when archives are disabled or nothing is archived yet, 403 while an access-control profile is active, 422 when this server cannot carry the live leg (an index reached over a unix socket, or one with no `binlog_events` table), 502 when the index could not be asked, and 400 for an `include_live` or `include_events` value other than `1`/`true`/`0`/`false` (so a request that meant to ask never comes back as an archives-only file). |
| `GET /api/storage` | Process-global storage context: `{aws: {access_key_env, profile, region_env, shared_config, container_creds, web_identity, web_identity_token_readable, web_identity_role_arn}}` — presence booleans and non-secret names only, never credential values. |
| `GET /api/flashback` | Process-global: the embedded time-travel SQL port (`watch --flashback-listen`): `{enabled, listen, host, port}`. `enabled: false` alone on the standalone console and on a daemon that did not open the port; `host` is empty on a wildcard bind (the UI then uses the name it was opened with). Never the console token that authenticates the port. Backs the **Connect a SQL client** panel on Settings → Connect AI. |
| `GET /api/profiles` | RBAC data-profile **names** defined on the selected server's index: `{"profiles": ["..."]}`, sorted; empty on a legacy index without the table. Vocabulary for administration panels (e.g. a settings-surface profile picker) — never the rules or flagged tables/columns behind a name. |
| `GET /api/access-profiles` | The selected server's access-profile configuration in one document: `{flags: [{schema, table, column, flag, created_at}], profiles: [{name, description, created_at}], rules: [{profile, flag, permission, created_at}]}` (`column` empty = a table-level flag). `settings:read`. `403` while an access-control profile is active (a startup `--profile`, even one with no rules yet, or the session's own data profile: the flagged tables and columns are what that profile withholds). `422` on an index without the RBAC tables. |
| `POST /api/access-profiles/flags`, `.../flags/remove` | Add / remove a flag: `{flag, schema, table, column}` (`column` optional). `settings:write`; `403` while an access-control profile is active, as for the GET. Names are trimmed. Answers with the full document. `400` with the CLI's own message on missing fields or a value past its column width, `404` when the flag to remove is not there, `409` when the flag exists under a spelling that differs only by case or accents (the stored row is named). When the write landed but the readback failed, a `500` whose message begins `The change was saved but the page could not be re-read:`. |
| `POST /api/access-profiles/profiles`, `.../profiles/remove` | Add (or re-describe) / remove a profile: `{name, description}`. `409` when the name differs from an existing profile only by case or accents (the index compares names without regard to either; the existing row is named). Removing cascades to the profile's rules. Same permission, refusals and response as the flag verbs. |
| `POST /api/access-profiles/rules`, `.../rules/remove` | Add (or replace the permission of) / remove a rule: `{profile, flag, permission}` with `permission` `allow` or `deny` (`400` otherwise, `404` for an unknown profile). Same permission, refusals and response as the flag verbs. |

Every data endpoint (`status`, `capacity`, `schemas`, `events`, `schema-changes`, `recover`,
`recover-cascade`, `capabilities`, `reconstruct`, `baselines`,
`access-profiles`) targets the
server named by the `X-Bintrail-Server` request header; without the header they
target the default entry (`storage` and `flashback` are the process-global
exceptions).
Selection is stateless — concurrent clients can each target a different server.

### Cascade recovery

On MySQL 8.x and earlier (and all MariaDB), InnoDB enforces a foreign-key `ON
DELETE CASCADE` / `ON DELETE SET NULL` *below* the binary log — only the parent
`DELETE` is logged, so the cascaded child deletes and SET-NULL updates are never
recorded as events. A plain undo of the parent `DELETE` would re-create the
parent but leave those children gone. The `ON UPDATE` cascades have the same
blind spot: only the parent `UPDATE` is logged, so undoing it alone would leave
every child foreign key pointing at the key the cascade wrote.

**Recover handles this automatically — there is no separate tab.** When you
generate undo SQL for a `DELETE` — or for an `UPDATE` of a referenced key — on a
table that is a foreign-key **parent**, the console detects it (one index lookup
of the recorded FK graph, matched to the referential action the reversed events
can actually trigger) and folds the invisible children into the **same** script:
`INSERT`s for the cascade-deleted children, idempotent guarded `UPDATE`s
(`… AND fk IS NULL`) for SET-NULL'd foreign keys, and guarded `UPDATE`s
(`… AND fk = <new key>`) for the foreign keys an `ON UPDATE CASCADE` rewrote, all
wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. A **CASCADE detected**
banner above the result reports how many children, SET-NULL restores and FK
restores were included. Copy or download it; **nothing is ever executed.**
Detection only fires
for a MySQL/MariaDB index (PostgreSQL logical replication captures cascade
deletes as real events — no blind spot to reconstruct) and only when the matched
rows actually contain an event on the target table that its FK rules say could
have cascaded. The same engine is exposed
for scripting as `bintrail recover-cascade` and `POST /api/recover-cascade` (with
explicit `lookback` / `max-depth` knobs) — see
[query-and-recovery.md](query-and-recovery.md) for the full mechanism, the
baseline Phase-2 fallback, and the coverage limits.

**Coverage is surfaced, never hidden.** Phase-1 (live binlog window) recovery is
partial by construction — a child not touched within `lookback` (default `30d`)
and not in a baseline cannot be reconstructed. When the result is provably
partial (a coverage gap, a per-parent overflow, or archived-out partitions the
live scan can't see) the warnings carry a prominent **provably partial** notice
listing every caveat — and the same caveats are embedded in the generated SQL's
preamble, so a partial recovery can never read as a full restore even after you
copy or download it.

**Under an RBAC redaction profile**, cascade synthesis is **disabled** (it does
not yet pass through the query engine's column redaction, so synthesizing child
rows could leak redacted/denied data): the parent-only undo is still generated,
but with an explicit warning that cascade children are **not** included — it is
never silently presented as a full restore. The standalone `POST
/api/recover-cascade` endpoint returns `403` in that mode. When a baseline is
configured, the `recover_cascade_baseline` capability signals that Phase-2
(recovering children untouched within the window) is active for the selected
server.

### Time-travel (reconstruct)

When `--baseline-dir` or `--baseline-s3` is set, the console can reconstruct a
single row's **full state at a point in time** — the baseline snapshot merged
with the binlog deltas after it — and show the row's history. PK column names are
read from the schema snapshot, so you only pass the value(s) (pipe-delimited for
composite keys). The response cleanly distinguishes three outcomes: the row's
state, the row *deleted as of T* (`deleted: true`), and *no baseline row* for
that PK (`found: false`).

Three gates protect it, all enforced at the endpoint (not just by hiding the tab):

- **Baseline required** — without `--baseline-dir`/`--baseline-s3`, `GET
  /api/reconstruct` returns 404 and the tab is hidden.
- **No active RBAC profile** — the baseline row is read directly (it does not
  pass through the query engine's column redaction), so a `--profile` disables
  reconstruct, mirroring the way a profile forces `--no-archive`.
- **Archives enabled** — `--no-archive` disables reconstruct. The gap check that
  makes reconstruct fail loud can only verify coverage of rotated-out hours if
  their archives are actually fetched; with archives off, an archived-but-rotated
  hour would be skipped yet counted as covered, producing a silently-wrong state.

Unlike events/recover browsing, reconstruct treats a coverage gap between the
baseline and the target time as a **hard error (422)**, not a warning: a missing
hour means a silently-wrong reconstructed state, not just a few deltas missing
from a script. Pass `allow_gaps=true` to override (best-effort), mirroring the
CLI's `--allow-gaps`. A single row touched by more than 10,000 events in the
`[baseline, at]` window is also refused (422) rather than reconstructed from a
truncated prefix.

> **Archive-source failures fail loudly here (#377):** when several archive
> sources are configured and *any* of them fails to load, `query.FetchMerged`
> aborts the strict-mode (`allow_gaps=false`) fetch — the request returns 500
> naming the failed source — instead of folding an incomplete delta set into a
> 200. Pass `allow_gaps=true` to fall back to warn-and-continue — with the
> skipped source reported in the response `warnings` (#1281).

### Time-travel over the MySQL protocol (flashback port)

The reconstruct tab above is HTTP/browser-oriented. For a `mysql` client or an
application that speaks `AS OF` SQL, `bintrail-console watch --flashback-listen
<addr>` opens an embedded MySQL-protocol port that serves the `_flashback` /
`_snapshot` / `_diff` virtual schemas for **every monitored server** — routed by
the connection username (the server's registry id or display name), authenticated
with the console token. It replaces running a separate `bintrail shim` per
per-source index: the daemon already resolves each server's `bintrail_idx_<id>`
and baseline, so one port covers them all. A token is required (`--console-token`
/ `BINTRAIL_CONSOLE_TOKEN`) because MySQL-protocol auth cannot use the console's
password store. Full setup, routing, and the `_snapshot` baseline-parity edge:
[docs/time-travel-sql.md → the embedded port](time-travel-sql.md#the-embedded-port-multi-source).

The console shows the port on **Settings → Connect AI**, in the **Connect a
SQL client** panel (#1446): when `watch` opened it, the listen address, the
user rule (the server picked in the sidebar: its registry name or id, `default`
for the command-line entry), the password rule (the console token, never
displayed) and a ready-to-copy `mysql -h <host> -P <port> -u <server> -p` line
for that server. When the port is off, the panel says so and names
`--flashback-listen` / `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` as daemon
configuration; on the standalone `serve` console it says the port belongs to
`watch`. Display only: the console never opens or closes the port.

### Coverage gaps and incomplete data

Both `/api/events` and `/api/recover` report coverage gaps (hours rotated out of
MySQL with no archive) in a `warnings` array rather than failing the request —
matching the CLI `recover`, which warns and continues so a human can review the
script. The recover screen renders those warnings prominently, so an
incomplete-coverage undo is flagged to the operator rather than silently
presented as complete.

On an archive-heavy server the default Events browse (newest first, no time
filter) is answered from the live index alone whenever the live index fills
the whole page: rotation only archives hours **older** than the oldest live
partition, so archived events cannot appear in a newest-first page the live
index already filled. When that shortcut is taken the response says so with an
entry in `notes` ("This page was answered from the live index; the registered
archives were not read…"; `/api/recover` carries the same fact in its own
words when a filled reversal window elides the archives). On `/api/events`
and `/api/recover` — and only there — `notes` is the informational sibling of
`warnings`: it carries benign audit facts — this one records a
completeness-preserving optimization, nothing is missing from the page — and
the console renders it as a muted line, not as an alert. Cautionary facts
(coverage gaps, a session's archive exclusion, divergence findings) stay in
`warnings`. Do not generalize the field name across endpoints:
`/api/activity`'s pre-existing `notes` is CAUTIONARY — it explains a
`complete: false` aggregate. Older pages, time filters that reach archived
hours, and pages the live index cannot fill read the archives exactly as
before.

One residual limitation: a few failure modes are logged server-side but not
surfaced to the browser (this matches the CLI `recover`, which warns to stderr
and continues; both apply only to these permissive `AllowGaps=true` endpoints —
the reconstruct endpoint fails loudly by default, and when you opt into
`allow_gaps` it reports these same conditions as response warnings instead):

- some of several configured archive sources fail to load, and
- the query planner itself fails to run (gap detection is skipped entirely).

In both cases you get results without a coverage caveat in the response. Watch
the server log when running with archives configured. (One narrower reconstruct
case remains log-only: the straddling-transaction probe re-fetches with gaps
allowed, so a source failing only during that probe is logged server-side.)
