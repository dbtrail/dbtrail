# bintrail-console

`bintrail-console serve` serves an embedded, **read-only, single-operator** web
UI over an existing index. It is the MCP server with a web face: the same query,
recovery, and status engines, reached from a browser. Browse indexed row events
with full before/after diffs, and generate recovery (undo) SQL — all without
leaving the terminal that started it.

The web interface ships as its own binary (and Docker image,
`ghcr.io/dbtrail/bintrail-console`), separate from the core `bintrail` CLI —
install it only where an operator wants the UI.

The web interface **never executes SQL**. Recover produces a transaction-wrapped
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

On start it prints the URL to open. On a fresh install the first visit is a
**"create your password"** screen (see [Password login](#password-login));
after that, you sign in:

```
The DBTrail web interface (read-only) is running. Open:

    http://127.0.0.1:8090/

First run: open the URL and create your username and password.
```

Or serve it **alongside a live stream** in one process with
`bintrail-console watch` (the daemon formerly known as `bintrail up
--console`: `up`'s preflight + init + stream plus the web interface and the
multi-server control plane):

```sh
bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX"
```

`--console-listen` / `--console-token` (or `BINTRAIL_CONSOLE_LISTEN` /
`BINTRAIL_CONSOLE_TOKEN`) customize the bind and token; a single Ctrl-C drains
both the stream and the web interface. Passing `--baseline-dir` or `--baseline-s3`
(or `BINTRAIL_CONSOLE_BASELINE_DIR` / `BINTRAIL_CONSOLE_BASELINE_S3`) enables
the baseline-gated Time-travel surface here too, so one process serves the live
stream **and** point-in-time reconstruct:

```sh
bintrail-console watch --source-dsn "$SRC" --index-dsn "$IDX" --baseline-dir /var/bintrail/baselines
```

With an S3 baseline (`--baseline-s3`), the `watch` process reads S3 at request
time using the ambient AWS credential chain, same as standalone `serve`,
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
> **not** currently cover the web interface's own index *reads* (the connections the
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

1. **Overview** (landing) — what changed recently and where. It keeps
   itself current: every five seconds it asks whether the index gained a
   change (`GET /api/events/head`, which carries no row data), and re-reads
   the recent changes and the window counts only when it did. The two
   expensive reads, the all-time count and the coverage card, are limited to
   once a minute while changes keep arriving and run every five minutes when
   none are. Each request carries a deadline of its own, so a locked table or
   a host that went away ends as a refusal the page reports rather than a
   page that sits there looking healthy. A hidden tab asks nothing and
   catches up the moment it is shown again; a refresh that keeps failing says
   on the page since when it has not updated, one the server refuses says it
   stopped, and figures whose own read failed say they are the ones from
   before. Nothing is repainted: the cards fill in place, an unchanged list
   or activity panel is left alone, the address does not change, and a
   keyboard focus on an Undo button survives a newer change landing above
   it.

   While the selected server (on `bintrail-console watch`) has no backup yet,
   a **Getting started** list at the top shows each step from adding
   it: create the index database, connect to the source, read the table
   structure (not for PostgreSQL, whose stream saves it when changes arrive),
   start capturing changes, capture the first change, and take the first
   backup. When DBTrail cannot create that backup, the step still shows,
   waiting, with the reason: creating backups is turned off for the daemon
   (see `BINTRAIL_CONSOLE_BASELINE_TRIGGER` below), or the server has no
   backup location of its own. It is left out only for a PostgreSQL server
   with no replication slot or publication (the server form refuses to save
   one), which cannot capture either, so its capture steps are the ones to fix
   first. Each step is waiting, running, done or failed;
   a failure shows the error and what to do, and capture with no change on the
   source yet is shown as running, not stuck. The list goes away once a backup
   exists. What keeps it up is a backup step that FAILED, which is not a done
   one, so a backup that failed last night on a server backed up last week
   says so instead of vanishing. A capture step that failed does not: this is
   a list of setup steps, not a health indicator, and the Overview already
   says on its own when it has stopped updating, which is where a dead stream
   belongs. Capture failing before any backup exists still keeps the list, by
   the same plain rule. A backup counts whoever made it,
   since the server reads that server's own backup locations, so one taken
   before a restart or from the command line ends the step; a location that
   cannot be read is said on the step rather than taken for "no backup". Each
   server's locations are read at most once a minute for a sequence of asks,
   because reading an S3 one is a listing over the network. It is a reuse
   window and not a lock, so two tabs asking at the same instant can both
   miss it and both read.
   Then a **Restore
   coverage** card answering "to when can I restore, right now?" — any point
   between the delta-coverage floor and the last *indexed* event (never the
   wall clock; the **last change** chip says how long ago that edge is, in
   plain hours and minutes, red while capture is stalled, green while it
   keeps up and neutral on a server nobody is writing to), the
   continuity verdict, and — with a baseline source configured — the
   full-table restore window plus any table whose newest baseline predates
   coverage. It degrades loudly on `gap_lost`/`unavailable`/`unknown`/an
   empty index, and full-table coverage that could not be evaluated says so
   instead of rendering as "nothing broken".
   The drawing of the path from your database to its copy gives the copy's
   age as the age of its **data**, from the newest change the copy holds,
   with "data as of HH:MM" under it, and "written HH:MM" beside that when the
   copy's files are a minute or more younger than its data ([#2201](https://github.com/dbtrail/dbtrail/issues/2201)).
   An update made while capture is an hour behind writes a copy whose data is
   an hour old, however recently it was written. Each update records the
   newest change it folded (the later of what the snapshot it started from
   held and the newest change the index had when it started); a full read
   holds the database as of its own time. A snapshot with no such record (made
   by the command line, or before this record existed) is shown as at least
   as old as capture's lag, since a copy cannot hold a change capture has not
   read. The age is never negative; an update's
   record is on the source's clock, so a source clock running ahead of this
   machine makes the age read short by that much. When the gap between the source and
   the index keeps widening for several minutes (see `lag_growth` below)
   while capture still indexes new changes, only slower than the source
   writes them, or while the source says capture is behind, the binlog arrow
   says **falling behind** and a card names the
   first things to check: the `doctor` check "Index buffer pool" (an index
   MySQL still on the 128 MB default cannot keep up with a busy source) and
   the source's write rate. On a database nobody writes to the same gap
   widens by itself, and so does a capture that indexes nothing at all; only
   the source can tell those two apart, so without one of those two signals
   nothing is said.
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
   has more than that, the web interface says so instead of handing you a silent
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
   directory into the `bintrail-console` container read-only and set `BINTRAIL_INDEX_DATADIR_RO`
   to the mount point, the way the bundled `docker-compose.yml` does; an
   index reached at another address gets no such suggestion, because a
   mount that is not the index's would report the wrong volume's free
   space, and an index on a local address whose server the daemon cannot
   confirm is this machine, which is what a port-forward or a tunnel looks
   like, gets the suggestion with that warning attached); and the
   standalone read-only `serve`, which runs no rotation of
   its own, reports the retention as "not known here" instead of grading an
   index another process rotates as unbounded. See
   [capacity planning](capacity.md#monitoring).
7. **Protect** — **Snapshots**, one page for the copies, the checks and the
   settings (#1573; on a standalone `serve` the parts only the watch daemon
   can run are left out). Its top half is the selected server's
   snapshot listing; each row expands to its tables, sizes and how long the
   backup took; for a backup in a local directory, how each table was made
   (read from the database, built from the recorded changes, or reused
   unchanged; with table deltas this is read from the newest file of the
   chain beside the table, since the table file itself is carried forward
   on every update) and one line saying when the database was last really
   read under this backup, how long before it, and how many updates were
   built on that read since, because a backup that looks recent can rest on
   a read days older ([#1570](https://github.com/dbtrail/dbtrail/issues/1570));
   and a **Download (.tar.gz)** of the whole snapshot — the
   archive includes a `views.sql` with relative paths, so unpacking it and
   running `duckdb -init views.sql` from inside the folder opens every table
   ([#1583](https://github.com/dbtrail/dbtrail/issues/1583)); plus
   **Read database now** and — for a server with its own local backup directory —
   **Restore to a moment**, which folds a chosen past instant into a NEW
   discoverable snapshot in the same store, and **Build a .sql backup for
   any moment**, which folds a chosen instant into a mydumper-format dump
   downloaded as one `.tar.gz` — load it with `myloader`, nothing from
   bintrail needed on the restore side; the build is a full plaintext copy
   of every row, staged on the daemon's disk under the system temp
   directory unless `BINTRAIL_CONSOLE_BASELINE_STAGING` says otherwise). Its
   **Checks** section (`watch` only) runs
   `bintrail verify` and shows past runs, and **Where and how often** holds
   every setting that shapes a backup. The first two produce and validate the
   artifacts a restore depends on, so they are operations rather than
   settings; they lived on the old Storage page until they outgrew it, then
   on two pages of their own until #1573 put the whole job on one.
   A staged `.sql` build does not stay on disk: it is removed as soon as a
   download completes, 4 hours after the build finished if nobody
   downloaded it (the Ready line shows the deadline and the size), when a
   new build for the same server starts, or when the daemon restarts (every
   build a previous process left behind, finished or interrupted, is removed
   at startup). If a removal fails, the build keeps its state, the card says
   why, and the daemon retries every minute. If a new build starts and the
   previous one cannot be removed, the new build still runs and stays
   downloadable; its status names the previous build's directory until that
   removal succeeds. The .sql lane on Snapshots shows what is staged on
   the machine while it exists, previous builds that could not be removed
   included (#1867; it was on the This daemon page).
8. **Settings** — the backup settings live in the **Snapshots** page's
   "Where and how often" section (every parameter that shapes a
   backup or a snapshot, with its provenance — see
   [Where and how often](#where-and-how-often-snapshotssetup);
   on `serve` only the editable per-server half renders, since it is
   the one editor of a server's backup location), and under `watch` only:
   **Retention** (rotation policy and
   per-source S3 archiving — see
   [The Retention page](#the-retention-page))
   and **Rotation** (opens the rotation dialog). The **This daemon** page
   was dissolved in #1867: its usage-telemetry card is on **Status**, its
   AWS credential signals sit under the S3 field of a server's snapshot
   setup, and its staged .sql downloads under the .sql lane on Snapshots;
   `/daemon` still works and lands on Status.

Every view whose subject has a page on www.dbtrail.com/docs (Events, Restore,
Snapshots, Storage, MCP Server) shows a small **Docs** link beside
its title. It opens that page in a new tab and is a plain link: the web interface
makes no request for it, so it costs nothing on an air-gapped host. Views with
no page of their own show no link.

## Managing servers

The header has a server switcher and a **Servers** button: add, edit, and
remove named connections to DBTrail index databases, and switch every view
between them. The registry is a **local YAML file on the host that runs `bintrail-console`**
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
  `--index-dsn` becomes optional: the daemon can start registry-only.
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
  appears and disappears as you switch. A server's snapshot location is its
  own, or it has none: the process-wide `--baseline-dir`/`--baseline-s3` is
  the command-line server's location only. A server added from the UI gets a
  folder of its own. On the first start of a version with this rule, every
  saved server that had no location and read the process-wide one is given
  that value as its own, once, so it keeps reading what it read before. If
  the server file cannot be saved (written by a newer version, or not
  writable), the servers keep it for that run and the Snapshots page says
  the file was not updated. Several servers given the same location share
  it, and the Snapshots page warns once their snapshots mix there.
- **Test connection.** Each server (saved or being typed) has a write-free
  test. On a new server you are typing, it runs the source half of the startup
  checks Save runs (connection, binlog settings, grants, tables without a
  primary key or not on InnoDB) on the database as typed, and saves and starts nothing, so Test
  cannot pass a database that Save would refuse. The index checks are left to
  Save, because one of them creates a probe database. On a saved server it
  probes the index: ping, MySQL version, latency, whether the database looks
  like a DBTrail index, and whether its schema is current. Testing a saved server
  reuses its stored password only for the stored host, port and user; testing
  a different host, port or user requires the password to be re-entered, so
  the saved credential is never sent to a destination the operator did not
  configure. When the server has an
  [S3 store](upload.md#a-store-per-server-from-the-web-interface), it also sends a
  `HeadBucket` for each of its buckets through that store.

Security notes specific to the registry:

- The registry file stores full DSNs **including passwords** (`0600`, directory
  `0700`) — the same secret-at-rest class as `shim.yaml` and `dump.key`.
- Passwords never travel to the browser. List/get responses carry parsed
  non-secret fields plus `has_password`; leaving the password blank on an edit
  keeps the stored one.
- The registry file also stores each server's S3 secret key, when one is set.
  Responses carry the access key and `has_s3_secret_access_key` only, and a
  blank secret on an edit keeps the stored one. A user who can only read
  servers can run `Test connection`, so it signs with a stored secret, or
  with the daemon's own credentials, only for the saved server's buckets at
  the saved server's endpoint.
- **The web interface never migrates the servers added in it.** The one schema
  migration (`EnsureSchema`, an idempotent ALTER) runs at startup on the DSN
  you typed on the command line — never on a DSN typed into a browser form. A
  registry index that predates the `connection_id` column returns an
  actionable `422` (run a writer command against it once) instead of being
  silently ALTERed.
- The registry file, the [auth file](#password-login) (written by
  set-password), and the [managed MCP token file](#mcp-endpoint)
  (`~/.config/bintrail/console-mcp-token.yaml`, SHA-256 only) are the only
  things `bintrail-console` ever writes. Each has a path override
  (`--servers-file`, `--auth-file`, `--mcp-token-file`, or the matching
  `BINTRAIL_CONSOLE_*` variable); in a container, point all three at a mounted
  volume or they are lost when the container is recreated. Their write
  endpoints sit behind the same bearer token and Host-header guard as
  everything else.

The registry file is versioned and forward-compatible: fields written by a
newer DBTrail survive load→edit→save round-trips on an older binary, and a
file written by a newer *schema* version loads read-only rather than being
rewritten lossily.

### Monitoring a source from the UI (the control plane)

Under **`bintrail-console watch`**, and only there, the web interface is also a
control plane: "+ Add server" with a **source MySQL** (host/user/password,
optional schema filter) runs the `bintrail doctor` preflight inline
(failures come back as remediation cards), provisions a dedicated index
database for that source (`bintrail_idx_<id>` on the daemon's index server:
`CREATE DATABASE` + tables + schema migration, done by the daemon; the
web interface's request handlers still never migrate anything), and starts a
supervised binlog stream. Auto-start: a green preflight starts streaming
immediately; warnings (e.g. short binlog retention) show but don't block.

The **source user** you paste into the form needs `REPLICATION SLAVE,
REPLICATION CLIENT, SELECT` on the source MySQL — the form spells out the
exact `CREATE USER` / `GRANT` to copy. DBTrail never writes to the source, and
capture never locks it. The **Read database now** button needs more,
`RELOAD` plus `BACKUP_ADMIN` on MySQL/Percona 8.0+ and `SHOW VIEW` for views,
because a baseline is point-consistent by default. Without the lock privileges
capture keeps running and only the baseline is refused, naming the exact
`GRANT`; without `SHOW VIEW` the dump itself stops at the first view with
mydumper's `SHOW VIEW command denied`. On
managed MySQL (RDS, Aurora, Cloud SQL) the default lock mode is not available
(`ftwrl` needs `BACKUP_ADMIN`, which managed MySQL will not grant): grant
`LOCK TABLES` and `SHOW VIEW` instead. There is nothing to set: snapshots use
`lock-all` on their own for an Amazon RDS or Aurora host name, and retry once
with `lock-all` when the default is refused (an IP, a CNAME, Cloud SQL). Full
per-privilege breakdown and the least-privilege (schema-scoped `SELECT`)
variant: [streaming.md](streaming.md#the-source-mysql-user).

- Each monitored source gets **its own index database** — per-source state
  (checkpoints, snapshots) stays structurally isolated, and the server
  switcher lists it like any other connection.
- The supervisor reconciles **desired state** (`monitor_desired` in the
  registry) at boot: restart the daemon and monitoring resumes from each
  stream's saved checkpoint.
- A per-entry **capture lock** (`GET_LOCK`) on the index server makes a
  second daemon wait instead of double-streaming the same entry: it shows
  **WAITING FOR OTHER DBTRAIL** and captures once the first one stops. The
  holder checks the lock every 5 seconds and stops writing without it
  ([deployment.md](deployment.md#when-the-host-dies)).
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
  A MariaDB source has no `server_uuid`, and its GTIDs name servers by
  `server_id`, which is `1` on every server nobody configured, so GTIDs are
  not compared there. Instead the check reports:
  - the same server, when two entries use the same `host:port`, or when both
    connections land on one running server;
  - a replica, when a replication channel of one server (`SHOW ALL SLAVES
    STATUS`) connects to the other by host, port and `server_id`, and the
    other server confirms it: its `SHOW SLAVE HOSTS` lists the replica.
    The channel's host is resolved in the replica's network, so without
    that confirmation (for example two cloned stacks that both have a
    `mariadb-primary`) the pair is reported as "could not be verified". A
    `127.0.0.1` or `localhost` master is never matched, and neither is a
    `server_id` of `1`, which unrelated servers share.

  Reading replication status needs the `SLAVE MONITOR` privilege, and
  reading the replica list needs `REPLICATION MASTER ADMIN`. Without the
  first the check is skipped and says so; without the second a replica is
  reported as "could not be verified".
- The Source type saved with a MySQL or MariaDB server is a hint. Capture asks
  the server what it is, and DBTrail saves that as the server's Source type,
  so the server list and every page show what capture runs as. A warning
  shows on the server's Overview only when the saved type cannot be changed
  (for example, a server list written by a newer version). The daemon's own
  source (`watch --source-dsn`) is labeled with the flavor its capture
  detected.
- With `--metrics-addr`, the daemon serves one Prometheus `/metrics` endpoint
  for all supervised streams; every stream series carries a `source` label set
  to the entry ID (see [streaming.md](streaming.md)). Exception: the capture-loss
  counter `bintrail_statement_dml_dropped_total` has **no** `source` label, so
  concurrent streams conflate into one counter — see
  [observability.md](observability.md).
- **Archive to S3** (the `Archive to S3` field on a monitored source): set an
  `s3://bucket/prefix/` destination and the daemon's built-in rotation
  **uploads that source's rotated partitions as Parquet before dropping them**,
  so the forensic record survives the retention window and stays queryable:
  the daemon auto-discovers the archive on the next query, no extra config.
  Partitions are staged locally (`--archive-staging-dir` /
  `BINTRAIL_CONSOLE_ARCHIVE_STAGING`, default a temp dir), uploaded, then
  pruned. The S3 upload uses the **ambient AWS credential chain** (`AWS_*`
  env, `~/.aws`, or an instance/role), the same credentials the daemon needs
  to read the archive back; there is no per-source credential. Archiving for a
  source begins once its identity (`bintrail_id`) is resolved (right after its
  first stream connect); until then it rotates drop-only, and the
  protect-unarchived guard never drops un-uploaded data. A persistently failing
  upload (bad bucket/credentials) keeps partitions undropped and escalates to a
  loud Error after a few cycles — the index does not silently lose data, but it
  does stop shrinking, so fix the bucket/credentials. The archived Parquet is
  unencrypted; rely on bucket-level SSE/policy. Archive to S3 ≠ Baseline S3
  (the latter is read-side Time-travel input).
- **S3 store** (the `S3 endpoint`, `S3 addressing` and `S3 region` fields
  under Archive to S3): where this server's buckets live when that is not
  AWS or the process-wide `BINTRAIL_S3_ENDPOINT`, e.g. `http://minio:9000`
  for MinIO or `https://s3.eu-central-1.wasabisys.com` + region
  `eu-central-1` for Wasabi. Locations only, no keys: the daemon's ambient
  credential chain signs for every store. Applied **per bucket** to the
  Archive and Backups buckets, for uploads and DuckDB reads alike; two
  servers naming one bucket with different stores, one of them possibly
  none, is refused (422), and a store needs this server's own Archive or
  Backups location, never the daemon's `--baseline-s3` bucket. Details
  in [upload.md → A store per server](upload.md#a-store-per-server-from-the-web-interface).
- Registry fields: `source_dsn` (replication credentials — a secret with the
  same masking/keep-password discipline as the index DSN; `source_dsn: ""`
  clears it), `route_dsn` (the optional forwarding account read routing
  connects to the source with instead of `source_dsn`: a secret with the
  same discipline, set through `route_user` / `route_password` or a raw
  `route_dsn`, shown as `has_route` / `route_user` / `has_route_password`,
  plus `route_host` / `route_port` only when it connects to another address
  than the source; `route_is_capture` marks a saved value that is the
  capture account itself and `route_unreadable` one that cannot be parsed,
  neither counts as `has_route`; fields left out of a `PUT` keep it, an
  empty `route_user` removes it, and clearing the source removes it too; a
  change closes that server's open connections on the MySQL-protocol port;
  `POST /api/servers[/{id}]/test` answers with a `route` object, the login
  tried with it: `ok`, `user`, `error`, `needs_password`, or `skipped` with
  the reason in a process that has no port (`serve`); and
  `GET /api/flashback` carries `account_refused` per server when the
  source turned the port's login away, and `session_untracked` when the
  source does not tell the port about session settings changed inside a
  stored function (with the reason; the web interface does not show it
  yet, DBTrail's log does, once per server); see
  [time-travel-sql.md](time-travel-sql.md#read-routing-mysql-answers-the-copy-takes-the-heavy-reads-experimental)),
  `source_server_id` (0 = derived), `schemas`, `monitor_desired`,
  `archive_s3` (the bucket above — non-secret, round-trips in the masked DTO),
  `s3_endpoint` / `s3_path_style` (`path`, `vhost` or empty = path with an
  endpoint) / `s3_region` (the S3 store above — non-secret, round-trip too),
  and per-source TLS: `ssl_mode` / `ssl_ca` / `ssl_cert` / `ssl_key` (same
  semantics as `bintrail stream`'s `--ssl-*` flags — see
  [streaming.md → TLS/SSL for managed MySQL](streaming.md#tlsssl-for-managed-mysql-rds-aurora-cloud-sql)).
  Setting these in the registry is the only way to get `verify-ca` / mutual
  TLS on a "+ Add server" source; an empty `ssl_mode` means the default
  `preferred` (opportunistic, no certificate verification).
  The startup checks ("Connect a database", Test, Start) and the
  replica-overlap check connect to the source with these same settings, so
  a server that only accepts encrypted connections
  (`require_secure_transport=ON`, the default on Amazon RDS for MariaDB
  11.8) passes them under the default `preferred`.
  A full read (snapshot) uses them too: its checks before the dump, and
  mydumper itself. `preferred` connects once the way capture does and dumps
  over TLS when the source offers it, in cleartext when it does not (the
  run then says "Read without encryption" on its card and in its history);
  `required` encrypts without checking the certificate; `verify-ca` and
  `verify-identity` check it against `ssl_ca`, or, when `ssl_ca` is empty,
  against the host's system CA bundle (the first of
  `/etc/ssl/certs/ca-certificates.crt`, `/etc/pki/tls/certs/ca-bundle.crt`,
  `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`,
  `/etc/ssl/ca-bundle.pem`, `/etc/ssl/cert.pem`, after `SSL_CERT_FILE` when set; the console image has the
  first). With no bundle on the host the read is refused; set `ssl_ca` to
  the CA, or to a CA bundle file. A `tls=` in the source DSN wins over
  `ssl_mode`, as for every other connection (`tls=true` checks the
  certificate and host name against the system bundle).
  The arm64 build of mydumper (linked against MariaDB Connector/C) does not
  enforce `--ssl-mode REQUIRED`: against a server without TLS it dumps in
  cleartext. So before that build runs an encrypted read, DBTrail connects
  with TLS required (refusing the read if the server does not encrypt) and
  pins mydumper to the certificate that connection saw, by its SHA-1
  fingerprint; a server that changes certificate in between fails the read.
  A mydumper whose client library cannot be read from `--version` gets the
  same check but no pin (its run says "Encrypted, not pinned"), so an attacker on the network between the check and
  the dump is not stopped there.
  `ssl_ca`/`ssl_cert`/`ssl_key` are certificate/key file paths **on the
  daemon host**, not secrets.

The standalone read-only `bintrail-console serve` never offers any of this:
the `monitor` capability is false and the verbs return 403 there.

### Configuring rotation from the UI

`bintrail-console watch` runs the built-in rotation loop that keeps the index
from growing without bound: it drops binlog partitions older than a **retention
window** every **interval**, keeping a few **future partitions** ready. Under
`watch` you can tune that policy from the web interface (the sidebar's
**Settings → Rotation** entry, or **⌘K → "Configure rotation…"**) without
editing flags or restarting:

- **Live:** changes apply on the loop's next cycle. Retention and future-partition
  count take effect immediately; a changed interval re-tunes the schedule.
- **Global, one schedule:** the loop is a single shared ticker, so the policy
  applies to **every** index the daemon rotates (the boot index and every
  monitored source). Per-source retention is not offered — the schedule is one.
- **Override vs default:** the saved policy lives in the local registry file
  (`console-servers.yaml`, the only file `bintrail-console` writes). When nothing is saved the panel shows the
  daemon's `--rotate-retain` / `--rotate-interval` / `--rotate-add-future`
  (`BINTRAIL_ROTATE_*`) values as the **effective default**, which describes a
  server added from now on: with no override saved, each index keeps the
  retention it was created under, so the panel also shows what the selected
  server keeps whenever that differs (an index created before the default
  became 48 hours keeps the 30 days it was created under until someone sets a
  retention). Saving an override applies to every index, and overrides that.
- **Disabling** rotation entirely stays a daemon-level decision
  (`--rotate-retain off`); the panel tunes a running loop rather than turning it
  off. A retain like `off` is rejected at save.
- The standalone `bintrail-console serve` hides the panel and refuses the write
  (HTTP 403) — only the daemon running the loop consumes the policy.

### The Snapshots page

The sidebar carries a **Protect** group with one page, **Snapshots**, which
answers the whole question in one place (#1573): what copies of this server
exist, whether they would restore, and where and how often they are made.
Those were three pages — Backups, Verification and Backup settings — and each
one alone read as the complete answer, so nobody could tell whether their data
was safe without visiting all three.

The page is read top to bottom in that order, and the two lower parts are
sections with addresses of their own:

| Part | Anchor | What it answers |
|---|---|---|
| (top) | `/snapshots` | what copies exist, and what you can do with one |
| **Checks** | `/snapshots#checks` | whether a copy would restore |
| **Where and how often** | `/snapshots#setup` | where copies are kept, and the timetable |

The three old addresses (`/baselines`, `/verification`, `/backup-settings`)
still work: each rewrites to its part of the Snapshots page, and a one-line note says
where the page they asked for went. Closing that note is remembered in that
browser, per old address.

**Snapshots opens on a standalone `serve` too**, where two of the three pages
it replaces did not exist. It leaves out what only the watch daemon can do —
taking a backup, running a check — and keeps what `serve` can answer: the
listing, the backup location (which it also edits), and a timetable somebody
saved, shown with the reason `serve` is not running it.

**What copies exist**

- A read-only listing of the **selected server's** baseline source
  (`baseline_dir` / `baseline_s3`): each snapshot's timestamp, age, table
  count, and (local sources) the binlog coordinates its deltas start from. The
  empty states explain how to produce a first baseline (`bintrail dump` →
  `bintrail baseline`). When the **Read database now** button is enabled it sits
  on the page's top strip, for a server with a backup location of its own (the
  daemon-wide default lists backups, but a backup refuses to write to it).
  When the page lists backups for a server with a source (from its own
  location or the daemon-wide default) and the button cannot be used, that
  spot says why instead, under **CREATE BACKUP**: *turned off at startup*
  when creating backups is turned off for the daemon, *needs this server's
  own backup location* when it has none, or both, followed by "(Backup
  settings page)". A server with no location at all shows the setup empty
  state instead.
- **Whether a snapshot is point-in-time** (#1380): a row carries a mark when
  the snapshot was read with no locks (**different points-in-time**: its rows
  were copied at different moments and may not agree with each other) or does
  not record how it was read (**point-in-time unknown**, drawn with a dashed
  outline, which is every snapshot taken before this was recorded). A
  point-in-time snapshot carries no mark. The page says "point-in-time", not
  "locks", because a safe-no-lock read and a PostgreSQL read are point-in-time
  without taking a lock. The answer
  is the worst of the snapshot's tables, and a snapshot updated from the
  recorded changes inherits it. Opening the row says how many tables are
  behind it and marks each one. A snapshot kept only in S3 shows **not
  checked**: the list does not read its files there. `GET /api/baselines`
  carries it as `lock` on each snapshot (`consistent`, `unknown`, `torn`, or
  absent when not checked), and `GET /api/baselines/files` as `lock`,
  `lock_torn` and `lock_unknown`, plus `lock` on each table.
- **Update the copy** (#1442): a per-server timetable, set from the Snapshots page:
  one of six intervals, 5 minutes to 24 hours (the daily one lined up on a
  UTC hour). The operator picks WHEN; HOW each run is made is the daemon's decision
  per slot (`console.ChooseBackupMethod`), and the page says which one comes
  next and why: a server with no previous backup yet gets a **full backup**
  (the Read-database-now job, reads the source, needs
  `BINTRAIL_CONSOLE_BASELINE_TRIGGER=1`); otherwise the newest backup is
  **updated from the recorded changes** (the baseline-refresh fold: reads
  nothing from the source, writes into the server's local backup directory,
  or, for a server whose backups go only to S3, into a folder of its own
  under the staging folder that is uploaded and then deleted, #2212; if the
  staging folder cannot be used, that server gets a full backup and the page
  says why). When the backups go
  to S3 that update reads its previous snapshot straight from the bucket and
  uploads its result back to the same place (#1539), so an S3 destination no
  longer forces a nightly full read of the source; when the local directory
  already holds that same newest snapshot, table for table, the update reads
  the local copy instead, so with the disk-space setting on, unchanged tables
  can keep their previous file (a hard link where the filesystem allows one, a
  copy otherwise) (#1626); any other local state keeps the bucket as the source. An update that
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
  opt-in not set AND no update possible, such as S3 only with a staging
  folder that cannot be used, a lock-mode misconfiguration on such a server,
  no destination) is refused on save with
  the reason, and one already saved is reported as not runnable on the
  page, never silently skipped.
  `PUT`/`DELETE /api/servers/{id}/backup-schedule`; state on `GET /api/baselines`
  (`schedule`). Every scheduled run on record says how it was made
  (`last_run.method`: `refresh` is an update from the recorded changes,
  `backup` a full read of the source) and, for a full backup, why an update
  was not possible when it ran (`last_run.why`, with a stable `why_code`:
  `no_index`, `no_local_dir`, `first_backup`, `previous_unreadable`,
  `fold_refused`, `fold_crashed`, `window_measured`, `window_age`). The
  last two are the cut-over after a long stop (#1721): before each slot the
  daemon measures what an update would have to fold (how far the index's
  high-water mark moved since the previous snapshot, against a cost model
  fitted on its last five measured updates, a fixed cost plus a per-event
  rate, and the duration of the last full backup on record) and takes a
  full backup instead when the update is estimated to cost CLEARLY more
  (more than twice as long AND over 30 seconds longer, #2181) and no
  recorded update that large was done in less time. The same margin
  applies when the cheapest recent update alone already costs more than
  the last full backup. Below the margin the update runs: a full read
  loads the source and an update does not, so when the two cost about the
  same the update is preferred (on a small source, 15 s against 6 s is an
  update; two minutes against 6 s is a full read). When one of the three
  is unknown (no full backup on record, no rate yet because the recent
  updates differ too little to read a per-event cost from, an index that
  did not answer the probe) it cuts over on age alone, once the previous
  snapshot is older than two hours or six schedule intervals, whichever is
  longer (counted from when the full backup that made it finished, when a
  full backup did: its own duration is not the schedule falling behind),
  and the reason names what was missing. One exception (#1791): when
  nothing at all was indexed since the previous snapshot, the daemon asks
  the source whether it wrote anything the capture has not recorded, and
  updates when it did not, since there is nothing to fold. The answer is
  yes only when the capture's checkpointed GTID set is EQUAL to the
  source's `@@GLOBAL.gtid_executed` (a capture holding more than its source
  means the source was reset, restored or replaced), with no capture gap
  recorded since the previous snapshot, and no dropped rows (the capture's
  skip ledger) recorded since the newest full backup started. Dropped rows
  are dated against a full backup, not against the previous snapshot,
  because an update is folded from the index, which never received them:
  only a read of the source brings them back, and with no full backup in
  the run history any dropped row keeps the full backup. Everything else
  keeps the full backup and the reason says why: the source is ahead (the
  capture may have stopped), or it cannot be asked (a capture in
  binlog-position mode, which is what a source without GTIDs runs, since
  its checkpoint stops short of each commit; PostgreSQL; MariaDB GTIDs; an
  index on the source server itself, whose own checkpoint writes keep the
  source ahead; a source that does not answer; an index with no live
  capture). An index that records nothing while the source keeps writing
  is a capture that stopped, and a full backup is the one producer that
  does not depend on it. One limit: stopping a source from the web interface
  clears its capture-gap record (the Stop is the acknowledgement of the
  loss), so a gap acknowledged that way before the cut-over no longer
  keeps the full backup, and the rows it lost stay out of the backups
  until a full backup reads them: the full-backup timetable, or one taken
  by hand after acknowledging the loss. After a full backup the
  model may not choose another one until an update after it has been
  measured (it may still choose an update): each full backup records the
  index's high-water mark before it starts, so the update that follows it
  is measured, and a rate that stopped being true gets corrected instead
  of choosing a full backup every other slot (#1737). Both say so in the daemon log with the numbers
  and on the page as the run's reason (the page's next-slot method uses the
  same measurement, cached for a minute); neither applies when a full
  backup cannot start (the creation opt-in off), where the update runs
  however long it takes, being the producer that can. Every update's run
  records `events`, `update_seconds` and `index_mark`, and every MySQL
  full backup that published a snapshot `index_mark` (a PostgreSQL one
  cannot: its snapshot is stamped by the database), which is what the model
  and the count after a restart or a full backup are read from. The reason is persisted with the run,
  never recomputed later, so a cleared bucket error cannot show the cheap
  producer for a run that read production in full (#1604); the snapshot
  detail carries the same on `run.why`. The page turns the two permanent
  reasons into the setting to change. The daemon-wide
  `--baseline-refresh-interval` below is independent and can run alongside.
- **Automatic baseline refresh** — when `--baseline-refresh-interval` is set,
  the panel reports the daemon's last automatic refresh for the selected
  server: how many tables it published, or that it published nothing and why.
  A refusal there is the fail-closed contract working (a capture gap, a schema
  change), not a broken daemon — nothing was overwritten and the next run
  retries. The exception is a run refused for too many changed rows (see
  [the limit](dump-and-baseline.md#refreshing-on-a-schedule)): the next run
  starts from the same backup over a longer window, so the panel says it is
  refused again until a newer full backup exists. The partial files that run wrote are removed and the daemon log
  names the directory either way, so a table that refuses every interval does
  not fill the disk with unusable snapshot directories
  ([details](dump-and-baseline.md#refreshing-on-a-schedule)). The refresh is
  opt-in on its own: it does not require, and does not enable, the **Create
  baseline** button.

**Checks** (`/snapshots#checks`, under `watch` only) carries the verification
runner and the history of past runs for the selected server.

The per-table results use a few words of their own:

- **Row history**: every recorded change to one row, oldest to newest. The
  check walks each row's history in order.
- **Before-image**: each update or delete stores what the row looked like just
  before it. The check compares that against what the previous change left.
  Undo scripts are built from these images.
- **No known earlier state**: the check saw a change but held nothing older to
  compare it against. A longer window may reach the history it needs
  (`bintrail verify --check recover --lookback`).
- **Nothing to check**: the table did not change, or only gained new rows. Zero
  comparisons is the expected result there, not a finding.

The Backups summary card that used to point here from Storage is gone (#1543):
this is a part of the page in the same sidebar entry, and the pointer only
existed because the page it pointed away from was a drawer.

### Where and how often (`/snapshots#setup`)

This part of the page owns every parameter that shapes a backup or a snapshot
(#1582; a page of its own until #1573),
because no two of them were configured the same way: some are daemon flags,
some are environment variables, some live per server in the registry, and the
precedence between them — per server, then daemon flag, then nothing — was
real and invisible. A server backed by the daemon's `--baseline-dir` showed
an empty Local folder field, indistinguishable from a server with no backup
location at all.

One DBTrail installation writes into one folder or S3 prefix. When the
selected server's location holds snapshots signed by another installation,
its settings say so in red and name the other writer by its `bintrail_id`;
nothing is refused. See
[One writer per location](dump-and-baseline.md#one-writer-per-location).

The page offers three settings and nothing else: how often the copy is
updated, a manual read of the database (Read database now) and retention: the
daemon's retention row and the per-server rows. What is set in the launch command (the lock mode, the `.sql` build
folder, the verify table filter, the default locations) is documented under
[settings that need a restart](https://www.dbtrail.com/docs/settings/backups#set-at-startup) and no
longer drawn on the page.

- **Local folder** (#1681) — every server keeps a copy of its snapshots in a
  folder on the machine DBTrail runs on; the yes/no question left the page
  and the answer is yes. A server added from the web interface gets one of its own,
  `<state dir>/snapshots/<server id>` (the state directory is the one holding
  the server list, `/var/lib/bintrail` in the compose stack), created `0700`
  and named by the server's id, so renaming the server moves nothing. With no
  S3 destination, the row also asks how many to keep: a new server keeps the
  newest 3 and older ones are removed; empty keeps every snapshot. Older
  snapshots go at the next hourly cleanup, never the only copy of a table.
  Changing only the count on a server's own folder is the choice to remove
  the older ones there, so it is not refused. Moving a server that has a
  count to another folder that already holds snapshots is refused (from
  this row or the server form), and a server added on a folder that already
  holds snapshots starts with no count, so it keeps them all.
  Beside the count the row says how far back that lets you go: the count
  times how often the server gets a snapshot on its own: its schedule
  (including full backups that fall between its runs) and the
  `--baseline-refresh-interval` loop together, since both write into the
  folder (for example 3 daily snapshots reach back up to about 3 days, but
  only about 3 hours if an hourly refresh also runs), never less than an
  hour, because
  the cleanup leaves snapshots younger than an hour alone, and never less
  than the `--baseline-retain` age, which keeps younger snapshots past the
  count. When nothing takes snapshots on a timer it names no number, since
  the reach then depends on when snapshots are taken. The count it uses is the one the Snapshots
  listing reports (`local_retention.keep_newest`); a number typed and not
  yet saved or applied reads "Once this number applies". Past the oldest
  snapshot kept, a restore, a `.sql` export and full-table time travel have
  no snapshot to start from, so they cannot reach that far back; the
  recorded row changes themselves (row history, `recover`) do not depend on
  snapshots and keep their own retention. A folder that two servers shared
  is never counted, and neither is one that stopped being shared while it
  still holds the other server's snapshots (the other server was deleted,
  answered no, or moved away): a snapshot does not record which server
  wrote it. That folder keeps everything, also after answering no and then
  yes on it again, until its server moves to a new empty folder. A server
  whose creation failed half way leaves no such mark. A table
  that did not change keeps its last file (a hard link where the filesystem
  allows it), so a new snapshot only costs the tables that changed. A server
  saved as S3-only before the page stopped asking keeps working that way
  until its next save from the Snapshots page, which turns the local copy on.
  A folder is checked when it is saved: it must be a full path, a missing one
  is created, and DBTrail must be able to write into it. That is on the
  `watch` daemon, which takes the snapshots; the read-only `serve` creates
  no folder and writes nothing but its server list, so there a folder must
  already exist and be writable by its user, and a server it adds gets no
  default folder. Saving needs `servers:write`; a session without it sees
  the answers and no controls. The server edit form no longer carries these
  fields, and an edit that leaves them out keeps what is stored. Servers that existed
  before #1681 are unchanged: none gets a folder or a count it did not have.
- **Per server** (changed on the Snapshots page): each registry server's local-copy answer,
  Local folder, S3 location and keep count, editable in place, with which location is in force
  drawn rather than said: the server's own case (own location, daemon
  default, or no location) under its fields, with a tick or a cross per lane.
  The daemon default backs time-travel, verification and `.sql` exports but
  backups, restores and the schedule refuse, which is the cross on that
  case. The per-server fields left
  the server edit form for the Snapshots page; an edit there that leaves them out
  keeps what is stored, so it cannot wipe them. A stored schedule that cannot run as
  things stand shows the refusal above the compact block; the schedule
  itself and the full-backup note sit inside it. Save wakes up when a field
  differs from what was loaded.
  A server with its own S3 destination also gets the growth line and the
  rule (#1622), schedule or not, since the Read database now button, a restore
  and the daemon-wide refresh upload too: about how many full backups
  reach the bucket every 30 days at the schedule's rate (or that every one
  stays, without a schedule), and that DBTrail never removes one. Under
  **Bucket rule to expire old backups** the page generates a lifecycle rule
  scoped to the backup prefix only, with the two commands to read the
  bucket's current rules and to apply the merged set, and says what such a
  rule cannot do: it expires by age alone, so it cannot spare the only
  complete copy or a backup a restore is reading, and a stopped schedule
  under an age rule reaches zero backups. Three refusals, in red: a
  retention shorter than the schedule (the newest complete backup would
  expire before the next one exists; no rule is shown), backups at the
  bucket root (an empty prefix would expire everything in the bucket), and
  archived changes stored under the same prefix as the backups (the rule
  would expire the evidence recovery is built from). Another server's
  backups nested under the prefix are named, since they would expire under
  this rule. The prefix is spelled exactly as the daemon builds object
  keys, spaces and slashes included, so the rule matches what was really
  written. The rule also expires noncurrent versions after the same number
  of days, since on a versioned bucket an expiration alone only writes a
  delete marker; under an Object Lock retention nothing expires before it
  ends. DBTrail itself never deletes from S3 and never sets a bucket rule;
  the rule is the operator's to apply.
  Above the rule, one line says whether the bucket already expires these
  snapshots (#1680). The web interface reads the bucket's rules after the row is
  drawn and answers with one of three states. A rule covers this prefix: the
  line names it and the age it expires at, and turns red when that age is no
  longer than the schedule interval. No rule covers it: the line is red and
  says the bucket grows without limit, with the schedule's rate when there
  is one. The rules could not be read: the line says so, and that a missing
  `s3:GetBucketLifecycleConfiguration` permission is the likely reason; it
  never reads as "no rule". A disabled rule, a rule on another prefix, and a
  rule that only moves objects to another storage class, expires old
  versions or aborts unfinished uploads do not count. A rule limited to
  objects with a tag or a size is not counted either, and the line says how
  many of those it found. The whole-bucket one-year rule that
  `bintrail init --s3-bucket` sets does count. With several rules, the
  shortest age is the one shown, since it deletes first. Whether the bucket
  keeps versions is not read (it needs another permission). On a bucket that
  does, a rule that expires current objects only leaves the old versions in
  place and the bucket still grows, which is why the generated rule expires
  those too; when the rule found has no expiry for old versions, the line
  says so. The read is one
  request with a 5 second limit, its answer is reused for 30 seconds, and it
  is only a read: `bintrail doctor --baseline-s3` reports the same answer
  from the command line ([s3-iam-policy.md](s3-iam-policy.md)).
- **Set at startup** — the nine daemon-wide values (`--baseline-dir`,
  `--baseline-s3`, `--baseline-retain`, `--baseline-refresh-interval`,
  `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE`, `BINTRAIL_CONSOLE_BASELINE_TRIGGER`,
  `BINTRAIL_CONSOLE_BASELINE_STAGING`, `--verify-interval`,
  `--verify-tables`), each shown verbatim with the exact flag or variable
  name, on a plain card with one **Restart to change** chip. An empty value
  reads as the word for what applies (`none`, `off`, `all tables`, `temp
  folder`), never as a fault. Read-only on purpose: the web interface never edits
  the process's command line or environment. A value the daemon refused
  (today: an invalid lock mode) stays loud under its row.

The per-server half is the whole page on standalone `serve`: the
daemon cards describe loops only `watch` runs, but the backup location is
registry state, and the Snapshots page is its only editor.

### The Retention page

Under `watch` the sidebar grows a Retention page. It was half of one page,
Storage, which had become a drawer: seven cards from five unrelated concerns
(#1543), split by the question each answers into Retention and This daemon.
This daemon was then dissolved (#1867): each of its three cards went where
its question is asked, and the section after this one says where.

**Retention** — what happens to your data as it ages:

- **Rotation** — the effective policy (override vs DBTrail's defaults) with an
  edit shortcut to the rotation dialog.
- **S3 archiving per source** — every monitored server with its
  `Archive to S3` destination (or `drop-only` when none), with a shortcut into
  that server's edit form. The boot (cli) index always rotates drop-only.

**Where the This daemon cards went (#1867):**

- **AWS credentials** — which credential signals this process sees, as one
  sentence plus a "Raw signals" fold under the **S3 location** field of a
  server's snapshot setup, shown while an S3 location is typed or saved.
  Presence and the non-secret profile and region names only; no value is
  ever shown or stored. A server with its own access key (Manage servers)
  reads that it signs with its own key instead.
- **Staged downloads** — under the .sql lane on Snapshots: what is staged on
  the machine, how many builds are for other servers, and a "Staged builds"
  fold with each build's size, state and deadline.
- **Usage telemetry** — the last card on **Status**, for a session that may
  read settings; described below.

Two cards left the page entirely. **Backups & disk space** moved to the
backups page beside **Scheduled backups** (#1543), from there to the
**Backup settings** page (#1582), with that page into the "Where and how
often" section of **Snapshots** (#1573), and was removed in #1681: reusing an
unchanged table is unconditional, and the saving it described is said beside
each server's local-copy question, where it is true or not.
**Download a DuckDB schema** moved to the SQL page, from there to
**Connect** (#1549) — `GET /api/views.sql` requires `settings:read`, while the
SQL page is gated on `query:execute` and on the `sql` capability, so the
download was unreachable for a role the endpoint would have authorized, and
gone entirely on a daemon started with `BINTRAIL_CONSOLE_SQL_PANEL=0` — then
to **Backups** (#1581), below the snapshot listing, and back to **MCP Server**
(#1573), with or without the daemon, beside the other ways to take the data
somewhere else. The Backups take-away lane keeps a **Download views.sql**
button that saves the default file (no change log) and shows the command to
open it; the options stay on the card. The old `/storage` link still works
and lands on Retention.

- **Table deltas are not on the Retention page.** Table deltas (#1638), which make a refresh keep a changed table's file and write its changes beside it, are on by default (#1729) and turned off with a daemon flag only (`--baseline-table-deltas=false`, or `BINTRAIL_BASELINE_TABLE_DELTAS=false`); there is no card for them in the web interface. It changes the files every refresh publishes; [Dump and baseline](dump-and-baseline.md) describes the layout, who reads it, and what to do with DuckDB views when turning it on or off.

- **Reusing an unchanged table** (`--baseline-carry-forward-unchanged`, on
  by default) has no card since #1681. Where the filesystem allows a hard
  link, two snapshots then share the same bytes on disk, so deleting the
  older one frees nothing while the newer one still points at it, and a `du`
  per snapshot directory double-counts it (one `du` over the root reports the
  truth). It does not apply when the previous snapshot is read from S3
  into a local folder, because linking a file needs both ends on a
  filesystem; on a server whose snapshots go only to S3 the unchanged table
  is copied inside the bucket instead (#2212). With table deltas on (the
  default) an unchanged table and its pair are kept, locally or by a copy in
  S3, whether this is on or off; it governs only tables without deltas. A
  `baseline_refresh:` block saved by an older version is ignored and kept in
  the registry file untouched.
  See [dump-and-baseline.md](dump-and-baseline.md#refreshing-on-a-schedule).
- **Staged downloads**: the `.sql` backups built from the Snapshots page that
  are waiting on the daemon's disk for their download: each build's server,
  size and download deadline, the total, and where they live. A build is
  removed once downloaded or 4 hours after it finished, so this card is
  usually empty; it exists so that space is never invisible. A build that
  could not be removed stays listed with the reason (a previous build a
  newer one could not clear included) and is retried every minute. Shown
  only on a daemon that can build `.sql` backups.
- **Download a DuckDB schema** (#1528, formerly *Query in DuckDB*; on MCP Server since #1573, after the SQL client panel) — a one-click download of `views.sql`: a ready-made
  DuckDB schema over the selected server's own Parquet — one
  view per table in the newest baseline snapshot, named like the source table
  (`shop.orders`), plus
  an `events` view across every archive source registered in `archive_state`
  when you tick **Include the change log**. It
  is the same file `bintrail views` writes. **The web interface does not run it.**
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
  table is added or dropped or a column changes type. For a fixed point in
  time, generate the file with `bintrail views --pin-snapshot` instead (the
  download has no box for it); a backup destination in
  S3 has no `current` pointer to follow, so it follows the newest completed
  backup instead, resolved when the file is read, and the generated file says
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
  session. A server whose index the daemon reaches over a unix socket
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
  (archives and live index), kept current run after run; it is a scheduler command, not a feature of the web interface, and it
  never runs inside `watch`. See [Iceberg export](iceberg-export.md).
- **Usage telemetry** — the current state of DBTrail's metadata-only usage
  telemetry and a one-click opt-out. Turning it off stops this `watch` daemon's
  beacons immediately (no restart) and records the machine-wide choice, exactly
  like `bintrail telemetry off`. When an environment variable (`DO_NOT_TRACK`,
  `BINTRAIL_TELEMETRY`) or the `--telemetry` flag already controls it, the card
  says so and defers to that. Open **Show a sample event** to see the exact
  JSON one event would carry, the same bytes `bintrail telemetry show` prints;
  opening it sends nothing. See [TELEMETRY.md](./TELEMETRY.md).

### The Access profiles page

**Settings > Access profiles** authors the flags, profiles and rules that
`--profile` enforces, from the browser. It is the web interface's counterpart to the
CLI verbs (`bintrail flag`, `bintrail profile`, `bintrail access`; see
[server-identity.md](server-identity.md#rbac-flags)): the same code runs the
validation and the writes, so a profile authored on the Access profiles page is the rows the CLI
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

This is the one write from the web interface that lands in an index database (everything
else it writes is the local registry file and the daemon's live
settings), so it runs under rules the tests pin:

- reading the page needs `settings:read`; every change needs
  `settings:write`, the permission that already governs web interface
  administration, so a read-only auditor role sees the configuration and
  none of the buttons;
- while an access-control profile is active the whole page is refused,
  reading included: a daemon started under `--profile` (a profile with no
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
- a change takes effect on a profiled session's next request: the daemon
  drops that server's cached profile rules when it writes.

An index created before the RBAC tables existed answers `422` here: the
web interface cannot create tables on an index.

### SQL in the browser

The Overview's **Ask it here** card opens an editor on the page. You write a
`SELECT`, press **Run** (or Ctrl+Enter, Cmd+Enter on a Mac), and the result
comes back as a table. It runs on DBTrail's copy of your data, the Parquet
files on the index host. It never runs on MySQL.

What you can query is what the copy defines: one table per source table, named
like the source table, `shop.orders` (the table as of the newest snapshot;
a name DuckDB cannot read bare is quoted, `demo."order.items"`), and `events`,
the change log, when archived changes exist on local disk. Under `watch` with
an S3 location set from the start they do not, once the first hour has been uploaded: each archived
hour is uploaded and the local file removed once the upload is confirmed, so the
local folder no longer holds the whole change log, `events` is not listed, a
note under the list says why, and a query
that reads it is refused with the two things that do answer: the **Events**
page for one row's history, and your own DuckDB over the bucket for the whole
of it (**Download a DuckDB schema** with **Include the change log**, and
**Works on another machine** to run it anywhere but the index host). The list on the left
shows them; type in the filter to narrow it, click a name to put it in the
query. The line under the editor says how old the copy is.

The limits, so a query can never hurt capture:

- Each query runs in its own process, separate from the one that captures,
  with 2 threads and 2 GB of memory by default (`--sql-memory`, at least
  512MB). A query that needs more fails with an out-of-memory message;
  capture does not notice. The line under the editor shows the memory in
  force. It can be changed in **Settings, MCP Server, Memory for SQL on the
  copy**: Save applies it to the next query, with no restart, and **Use
  default** goes back to 2 GB. A value given where DBTrail starts
  (`--sql-memory` or `BINTRAIL_CONSOLE_SQL_MEMORY`) wins; the panel then
  shows it read-only and says so, and removing it (and restarting) hands the
  setting to the page. The saved value lives in `console-sql-settings.json`
  beside the servers file; DBTrail reads that file when it starts, so change
  it from the page rather than by hand.
- A query whose tables have more than 384 MB of changes not merged into them
  yet (all the tables it names, together; the line grows with the memory,
  768 MB at 4GB) is answered from the newest earlier copy in which those
  tables fit, and the result says which copy and why. The answer is
  consistent with that copy's moment, like every answer here: SQL on the
  copy reads a point in time, not the source as it is now. With no such
  copy kept on disk the query is refused before it runs, with the
  name of the table that has the most. Between two rewrites of a table,
  DBTrail keeps its changes in small files beside it, and a query has to
  merge them in memory; past that size they do not fit. DBTrail merges the
  changes into the table on its own, within a day while updates run
  ([Snapshots and updates](dump-and-baseline.md)). Until then, and for any
  heavy read, use your own DuckDB (below): this page is for quick looks on
  the host that captures. The table list on the left shows how many MB are
  waiting beside each table that has any: in amber from three quarters of
  the line, in red past it (a query naming that table reads an earlier
  copy, or is refused when there is none), with a line under the list naming
  the red ones. A query over several tables adds
  their changes together, so two tables under the line can still be refused
  together. The list is read again after each query.
- 60 seconds. A longer query is stopped.
- 1,000 rows come back. The page says when there were more. **Download CSV**
  saves the same rows as a file.
- One query at a time per person, two at a time for the whole daemon by
  default (`watch --sql-max-in-flight`, see below; shared
  with the MySQL-protocol port, where the unit is one per server). One more
  statement waits for a free slot, up to 30 seconds, and runs as soon as one
  is free, so a burst turns slow instead of failing. One identity still runs
  one statement at a time: a dashboard's panels come one after another, and
  a panel that would wait more than 30 seconds is still refused. Past 30 seconds, or with 16 statements
  already waiting, it is refused as busy (HTTP 429). The line of 16 is one
  for the whole daemon: statements from the MySQL-protocol port wait in it
  too, and `bintrail_sql_slot_wait_seconds` and `bintrail_sql_slot_waiting`
  ([Observability](observability.md)) show the wait. It never runs beside the
  others: the process that serves it is the one capturing changes. Waiting
  costs nothing, because the slot is taken before anything is read, and a
  page closed while waiting leaves the line. For a team
  or a dashboard tool, each reader's own DuckDB on the bucket is how reads
  scale (see [Dashboards](dashboards.md)).
- Only the tables a query names are prepared before it runs, so a copy with
  hundreds of tables costs a short query no more than a copy with a few. A
  query that lists tables (`SHOW ALL TABLES`, `information_schema`,
  `duckdb_tables()`), or that names a table the copy does not have, gets
  every table, so the list is complete and a misspelled name is still
  answered with the closest real one.
- Text compares close to MySQL's default collation: `'Paid' = 'paid'` and
  `'café' = 'cafe'` are true, `GROUP BY` and `SELECT DISTINCT` fold them,
  and NULLs sort first on an ascending `ORDER BY`, last on a descending one;
  `'ß' = 'ss'` and full-width letters are equal too, as on MySQL.
  Not folded: `LIKE`, `REGEXP`, `count(DISTINCT ...)`, `instr`/`position`,
  and the duplicate removal of `UNION`.
  A column MySQL declares under a `_bin` collation is compared byte by byte,
  as on MySQL.
  A DuckDB of your own over the same files keeps DuckDB's defaults, so the
  same query can group `live` and `LIVE` apart there.
- Read-only. One `SELECT` per run (`DESCRIBE`, `SHOW` and `SUMMARIZE` work
  too). It can read the copy and nothing else on the host: no other file, no
  network, no extension.

Every query is written to the audit trail when one is installed: who ran it,
on which server, the statement, and how many rows came back. The response's
`phases_ms` field, the daemon's log at debug level, and the
`bintrail_sql_statement_phase_seconds` histogram under `watch --metrics-addr`
([Observability](observability.md)) say where the time went: `slot_wait`
(the wait for a free slot), `view_build` (the console's own discovery), `spawn` (the worker's whole lifetime, which
contains `open`, `lockdown`, `views` and `query`; what is left of it is
process start, result encoding and exit), `decode` and `total`. `views` is
installing the views the query names, including the console writing them
while the worker waits.

Who sees it: a session that holds the `sql:execute` permission. With no access
policy (the built-in password login and the static token) every session holds
it. It is **not available**:

- to a session with a data profile or table and column restrictions. The
  profile filters what the console shows, and SQL reads the raw files, which it
  cannot filter;
- for a server whose copy is only on S3. SQL in the browser needs the copy on
  local disk (a local snapshot directory, or a local archive directory);
- when archive access is disabled for the server.

In those cases the card is not shown. For no row cap and no time limit, query
the same files in your own DuckDB: **Download a DuckDB schema** on the **MCP
Server** page writes a `views.sql` over them. See
[Query in DuckDB](https://www.dbtrail.com/docs/guides/query-in-duckdb/).

The API behind it is `POST /api/sql` with `{"sql": "..."}` (JSON result, or CSV
with `?format=csv`), and `GET /api/sql` for the list of tables, the copy's age
and the limits. Both need `sql:execute`.

The same queries run from a `mysql` client, with the same caps, over the
embedded time-travel port (`watch --flashback-listen`): see
[Ordinary SQL on the copy](time-travel-sql.md#ordinary-sql-on-the-copy-embedded-port-only).

An earlier SQL page was removed in 0.75.0. It ran queries inside the process
that captures, and it defined the `events` view on every page load, which took
minutes on a large archive. This one runs each query in a separate process and
reads the change log only for a query that names `events`.
`BINTRAIL_CONSOLE_SQL_PANEL`, which switched the old page, is still read for
one release and warns that it no longer does anything. Remove it.

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
- `BINTRAIL_CONSOLE_URL`: the address people open the web interface at, when it is not
  the listen address (a container whose port is published as another, a reverse
  proxy). Only the startup banner uses it; nothing listens or redirects
  differently. The compose file sets it, and the installer moves it with
  `DBTRAIL_PORT`. A value that is not an `http` or `https` URL with a host is
  ignored with a warning.
- `BINTRAIL_CONSOLE_SQL_PANEL`: retired. It switched the first SQL page, which
  was removed in 0.75.0. It does not control [SQL in the browser](#sql-in-the-browser),
  which is gated by the `sql:execute` permission. The variable is still read for
  one release and warns that it does nothing; a later release stops reading it.
- `BINTRAIL_CONSOLE_ARCHIVE_STAGING` (`watch` only) — local staging dir for the
  Archive-to-S3 feature, same as `--archive-staging-dir`. AWS credentials for
  the upload come from the ambient chain (`AWS_*` / `~/.aws` / role).
- `BINTRAIL_CONSOLE_BASELINE_TRIGGER` (`watch` only) — `1`/`true` enables the
  **Read database now** button (runs `mydumper` → convert → upload in-process;
  see [The Snapshots page](#the-snapshots-page)). Off by default for a bare
  `watch` invocation; the bundled compose stack sets this on by default (see
  [docker.md](docker.md) — `BASELINE_TRIGGER=0` in `.env` opts out there).
  The bare default stays off on purpose (#1677): the `bintrail-console`
  deb/rpm package does not install `mydumper`, so a default-on button would
  fail on first use, and a full backup reads every table in scope on the
  source, which is load an operator should choose. Turning it on also lets the
  backup schedule take a full backup on its own when an update cannot serve
  the server (no previous backup, or S3 only with a staging folder that
  cannot be used) or fails (a capture
  gap, a schema change), and it is what a schedule's full-backup timetable
  (`full_every`, set through the schedule API; the page no longer edits it)
  needs: without it, saving one is refused with the reason, and one saved
  before the opt-in was turned off is shown in red and skipped at its slots
  while the updates keep running
  ([#1564](https://github.com/dbtrail/dbtrail/issues/1564)).
  When it is off, the Overview's Getting started list says so until a backup
  for that server exists anyway, taken from somewhere this daemon does not
  run, and the Snapshots page says so for a server with a source and a
  location it can list. It is turned on where the daemon is started, with this
  variable, and not in the web interface; see
  [settings that need a restart](https://www.dbtrail.com/docs/settings/backups#set-at-startup).
- `BINTRAIL_CONSOLE_BASELINE_STAGING` (`watch` only) — local staging dir for
  S3-destined baselines created by that button (default a temp subdir).
- `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` (`watch` only) — empty (default:
  automatic), `ftwrl`, `lock-all`, `safe-no-lock` or `no-lock`. Selects how
  mydumper synchronizes its worker threads onto one instant for console
  snapshots. **Automatic** picks per snapshot: `lock-all` for an Amazon RDS or
  Aurora host name (`*.rds.amazonaws.com`, RDS Proxy included), `ftwrl`
  elsewhere, and one retry with `lock-all` when `ftwrl` is refused (the global
  lock denied, or `RELOAD`/`BACKUP_ADMIN` missing); the snapshot records the
  mode it used. Setting the variable overrides that for every server, with no
  retry. `ftwrl` is point-consistent and needs `RELOAD`/`FLUSH_TABLES` (plus
  `BACKUP_ADMIN` on MySQL/Percona 8.0+). `lock-all` is also point-consistent
  and needs only `LOCK TABLES`; it is what works on RDS/Aurora, where
  `BACKUP_ADMIN` cannot be granted and `ftwrl` therefore cannot run. `safe-no-lock` needs no
  elevated privilege but aborts rather than write a torn snapshot; `no-lock`
  accepts one. A baseline is the seed state
  `reconstruct` merges deltas onto, which is why the weaker modes must be named
  explicitly. An unrecognized value disables baseline DUMPS only — capture and
  the periodic refresh keep running.
- `BINTRAIL_CONSOLE_NOTIFY_WEBHOOK` (`watch` only) — same as
  `--notify-webhook`: URL for JSON notifications on lost continuity, verify
  problems, and unhealthy rotation (see
  [Alerts](https://www.dbtrail.com/docs/monitoring/alerts/#webhook)).
- `BINTRAIL_CONSOLE_VERIFY_INTERVAL` (`watch` only) — same as
  `--verify-interval`: enables scheduled verification on that cadence
  (e.g. `24h`, `7d`; see
  [Checks, on the Snapshots page](#the-snapshots-page)).
- `BINTRAIL_CONSOLE_VERIFY_TABLES` (`watch` only) — same as `--verify-tables`.
- `BINTRAIL_CONSOLE_VERIFY_TRIGGER` (`watch` only) — `1`/`true` enables the
  **Checks** section of Snapshots (runs `bintrail verify` in-process;
  see [Checks, on the Snapshots page](#the-snapshots-page)).
  Off by default for a bare `watch` invocation; the bundled compose stack sets
  this on by default (see [docker.md](docker.md) — `VERIFY_TRIGGER=0` in
  `.env` opts out there).
- `BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS` (`watch` only) — same as
  `--sql-port-max-rows`: the most rows one statement returns on the
  time-travel port (default `100000`; below `1` refuses to start). A result
  with more is refused with error 1104, not cut. This page's SQL card shows
  1,000 rows whatever it is. A result is also refused past 64 MB, and is
  held in the daemon while it is sent, one per connection receiving one
  whatever `--sql-max-in-flight` says: lower it on a host with little
  memory. On the compose stack the value goes in `.env` as
  `SQL_PORT_MAX_ROWS`.
- `BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT` (`watch` only) — same as
  `--sql-max-in-flight`: how many SQL-on-the-copy statements run at once, the
  SQL card and the time-travel port together (default `2`; below `1` refuses
  to start). Each one is its own process with 2 threads and up to
  `--sql-memory` (2 GB by default), on
  the host that captures, and every result is held in the daemon while it is
  sent, so the daemon's own memory grows with it; on the compose stack the
  workers share the `watch` container, which sets no limit, and the value goes
  in `.env` as `SQL_MAX_IN_FLIGHT`. The default is
  right for a 4-core host: measured there, two heavy statements kept capture
  current (#2026). The daemon warns at startup when the statements could take
  more threads than the host has cores.
- `BINTRAIL_CONSOLE_SQL_MEMORY` (`watch` and `serve`) — same as
  `--sql-memory`: the memory each SQL-on-the-copy statement may use, e.g.
  `4GB` (default 2GB; under 512MB, or not a size, refuses to start). The
  changes a statement's tables may hold unmerged grow with it: 384 MB at 2GB,
  768 MB at 4GB. In `.env` as `SQL_MEMORY` (leave it empty to set the memory
  in the web interface). `watch` warns at startup when this
  times `--sql-max-in-flight` is more than the host's memory. Set this way it
  wins over the value saved in Settings, MCP Server (which then shows it
  read-only); unset, the saved value applies, else 2GB. The same setting is
  `GET`/`PUT /api/sql-settings` (`{"memory":"4GB"}`, `""` for the default;
  `settings:read` / `settings:write`).
- `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` (`watch` only) — same as `--flashback-listen`
  (e.g. `127.0.0.1:3308`): serve an embedded MySQL-protocol time-travel port for
  every monitored server, routed by the connection username. Off by default;
  requires an access token. See [Time-travel over the MySQL protocol](#time-travel-over-the-mysql-protocol-flashback-port).
- `BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE` (`watch` only) — same as
  `--route-max-copy-age` (e.g. `15m`): experimental read routing on the
  flashback port. Statements are forwarded to each server's source MySQL;
  SELECTs whose plan is expensive run on the copy while its snapshot is at
  most this old (past that age, the ones whose tables have not changed since
  their snapshot still do), and fall back to MySQL when the copy rejects
  them. Off by
  default (`0`). `BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD` and
  `BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS` set the plan thresholds
  (`--route-cost-threshold`, default 10000; `--route-scan-rows`, default
  100000). `BINTRAIL_CONSOLE_ROUTE_READ_ONLY` (`--route-read-only`, off by
  default, needs routing on) makes the routed port read-only: a statement
  that is not a read is refused with an error that names the flag and is
  never sent to the source. The **Connect a SQL client** panel (Settings → MCP Server) says
  whether routing is on, whether the port is read-only or read-write and, per server, how many statements MySQL and the
  copy each answered since start; `bintrail_read_routing_decisions_total`
  under `--metrics-addr` carries the same counts by reason. See
  [time-travel-sql.md](time-travel-sql.md#read-routing-mysql-answers-the-copy-takes-the-heavy-reads-experimental).

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

**Username + password is the primary way in.** On a fresh loopback install
with no credential, the first browser visit shows a **"create your password"**
screen; you set it once and you're signed in. Every later visit is a normal
sign-in. (Prefer the terminal, or setting it up before first launch? Run
`bintrail-console user set-password`.)

```console
$ bintrail-console user set-password
New password: ********
Retype to confirm: ********
Web interface password set for user "admin" (~/.config/bintrail/console-auth.yaml).
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
  write as the server registry. One user; multi-user/RBAC/SSO is DBTrail.
- A successful login (or first-run setup) mints an **in-memory session token**
  (24 h absolute, 8 h idle, max 16 concurrent) the SPA uses as its Bearer
  credential. Sessions die on logout, on password change (which revokes all
  of them), and on process restart — nothing session-shaped touches disk.
- The login response also sets the session token as an **HttpOnly
  `bintrail_session` cookie** (`Secure; SameSite=Lax; Path=/`, `Max-Age`
  matching the session's absolute expiry), so opening a link to the web interface in a new
  tab or window is already signed in — same store, same expiry, same
  revocation as the Bearer path; the cookie holds nothing new. Logout revokes
  the session server-side *and* expires the cookie, killing every tab at once.
  The `Secure` flag is safe on the local first-run flow (browsers treat
  loopback as a secure context); **operators terminating TLS at a reverse
  proxy** should serve the web interface over `https://` end-to-end from the
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
- **Rotate or reset:** from the UI (⌘K → "Change password…", revokes
  every other session immediately) or re-run `user set-password` (overwrites;
  applies on the next login, live sessions ride out their TTL). **Forgot the
  password?** Shell access is the recovery path — re-run `user set-password`.
  `user remove` deletes the file (a loopback install then returns to first-run
  setup; a non-loopback one refuses its next restart until a credential is set
  again). `user status` shows what is configured without printing secrets.
- Off-loopback password logins over plain HTTP are warned about at startup:
  use `--tls-cert`/`--tls-key` or terminate TLS at a reverse proxy (with
  `--allowed-hosts`).

### External login providers

Embedding distributions, builds that construct their own binary from the
importable `consoleapp` package (`cmd/bintrail-console` is a thin `main()`
over `consoleapp.Main`), may install an external login flow (e.g. OIDC
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

Embedding distributions (builds that construct their own binary from the
importable `consoleapp` package) may add one additional view to the web interface
through the `ext.ConsoleView` seam: call `ext.SetConsoleView` once from
`main()` before `consoleapp.Main`, like `ext.SetConsoleAuth`. An installed view
contributes a nav item, a frontend module, and its own authenticated data API;
the web interface reveals the nav item, routes to it, and loads the module in the same
page (same origin, not an iframe).

The view's static assets are served **unauthenticated** at `/ext/<id>/` (the
code always ships, like the web interface's own `app.js`), while its data routes at
`/api/ext/<id>/` require the same bearer credential as every other `/api` path
and are **refused while an access-control profile is active** (the daemon can't
guarantee a third-party handler honors table-deny / column-redaction rules, so
it withholds the whole surface under a profile). Each data route reads the index
of the server currently selected in the switcher, with the operator's profile
applied.

The module must export `render(mount, ctx)`. `ctx` is built in one place
(`extContract` in `app.js`) and is the same for both extension surfaces:

| key | what it is |
|---|---|
| `apiBase` | the extension's own data-route prefix (`/api/ext/<id>/`) |
| `api` | the web interface's authenticated fetch: bearer token and `X-Bintrail-Server` already applied. An HTTP error throws an `Error` carrying `.status`; a malformed body throws one **without** it, an aborted request rejects with `AbortError`, and a 204 resolves to `null`. Branch on `.status` defensively. |
| `ui.dateField(label, name, size, placeholder, required)` | the web interface's own date field: a text input plus the calendar/clock popover, returning the same `.field` wrapper the web interface's forms use |

`ui` exists so an extension does not reimplement a widget the web interface already
has — two copies drift, and the operator ends up looking at two different date
pickers on one screen. It is also the boundary of what may be relied on:
`app.js` is a classic script, so an extension running same-origin *could* reach
any of its functions as a window global, but only what arrives through `ctx` is
a promise. Everything else may be renamed without notice, and because the two
sides are built in different repos and never compile together, such a rename
would surface as a widget that quietly stopped appearing rather than as an
error. An extension that wants to run against older builds should
feature-detect (`typeof ctx.ui?.dateField === "function"`) and degrade. Two
things that detection does not give you. It establishes **presence, not
shape** — a build whose builder took different arguments would still answer
`"function"` — so treat the signature above as the contract and expect it to
be versioned with the binary, not sniffed. And `render()` is called but **not
awaited**: a synchronous throw is caught and rendered as an error in the
mount, while an `async render()` that rejects escapes as an unhandled
rejection and leaves the mount blank. An extension doing async work in
`render` should catch its own failures and render them, rather than relying on
the web interface to.

The standalone `bintrail-console` binary ships **no extension views**: no nav
item appears, `/api/capabilities` advertises none, and `/ext/*` and
`/api/ext/*` are absent from the router entirely.

## Serving the web interface on a hostname

The web interface binds `127.0.0.1:8090` by default, which is the right default and
the wrong one for a team. Putting it on `console.example.com` is four things:
a DNS record, a certificate, a listener, and one header rule that is the only
part people get stuck on.

**1. DNS.** An `A` (or `AAAA`) record for the hostname pointing at the host's
public address. Nothing about DBTrail is involved.

**2. The firewall.** Open 443 to the clients that need it. Leave 8090 closed:
nothing outside the host should reach the web interface's own port.

**3. TLS.** Two shapes, and neither is more supported than the other.

*The daemon terminates TLS itself:*

```
bintrail-console serve --index-dsn '<dsn>' \
  --listen 0.0.0.0:8090 \
  --tls-cert /etc/ssl/console.crt --tls-key /etc/ssl/console.key \
  --allowed-hosts console.example.com
```

*Or a reverse proxy terminates TLS* and the web interface stays on loopback, the
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
`proxy_read_timeout` (60 seconds by default) elapsing while the daemon works
before its first byte, not the daemon failing. Raise it on this `location`.
The SQL card hits the same limit sooner than it looks: a query can wait up to
30 seconds for a free slot and then run for up to 60, so set
`proxy_read_timeout 120s;` if the card is used through the proxy.

**4. The header rule, which is where the time goes.** With the vhost above the
web interface answers:

```
HTTP 403  {"error":"forbidden: host not allowed"}
```

That is the DNS-rebinding defence doing its job, not a misconfiguration. The
daemon accepts a `Host` header only if it is `localhost`, an IP literal, or a
name you listed — so `console.example.com` has to be listed:

```
--allowed-hosts console.example.com
# or: BINTRAIL_CONSOLE_ALLOWED_HOSTS=console.example.com
# on `watch`, the flag is --console-allowed-hosts
```

Why the defence exists: a browser on someone's laptop can be pointed at a
hostname that resolves to `127.0.0.1`, and without a Host check that page could
drive a web interface the attacker cannot reach directly. The allowlist costs one flag
and closes it.

There is a way to skip the flag, and it is worth knowing about mostly so you
recognise it. nginx's *default* — with no `proxy_set_header Host` line at all —
sends the upstream address as the `Host`, i.e. `127.0.0.1:8090`, which is an IP
literal and therefore always allowed. A vhost written that way works
immediately and never mentions `--allowed-hosts`. It also means every request
reaches the daemon claiming to be for `127.0.0.1`, so the real hostname is
absent from the daemon's own logs, and any future behaviour that depends on
knowing its public name has nothing to work with. Prefer passing the real
`Host` and listing it.

Two consequences worth stating:

- **`/mcp` rides the same allowlist.** Once the hostname is allowed, an
  MCP client can reach the endpoint at
  `https://console.example.com/mcp` — which removes the need for a tunnel or a
  port-forward to use it from elsewhere. It is behind the same credential as
  everything else; see [MCP endpoint](#mcp-endpoint).
- **A daemon on a non-loopback address refuses to start without a credential.** That is
  deliberate and it is checked before any of the above matters: set a password
  (or pass `--token`) first, or the process exits. See
  [Password login](#password-login).

## Security model

The binary has no Supabase/RBAC backend to lean on, so the web interface defends
itself:

- **Loopback by default + a credential required.** On a loopback bind the
  web interface prompts you to create a password on first run. No credential is ever
  auto-generated for you. Binding to a non-loopback address (`0.0.0.0`, a LAN
  IP, …) **requires** an explicit `--token` or a configured password, or
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

The web interface reads only the **index**, never the source database, so it works
identically for a PostgreSQL source captured by `bintrail-pg`: the index schema
is the same. It does adapt its **presentation** to the source family (reported
per server as `source` in [`/api/capabilities`](#api), derived from
`stream_state.flavor`):

- **Stream vocabulary.** A PostgreSQL stream shows its cursor as an **LSN** (and
  labels the source "PostgreSQL · logical replication") instead of MySQL binlog
  file / position / GTID. Slot and publication *names* are capture-side
  configuration and are not stored in the index, so the web interface does not show
  them.
- **Permanent-loss badge.** The Status page surfaces the durable loss record
  (`stream_state.gap_lost_at`) — for PostgreSQL, an invalidated/lost replication
  slot; for MySQL, an unfillable binlog gap. The index is valid only up to that
  point and capture must be re-baselined to resume.
- **Connection-id note.** PostgreSQL logical replication (`pgoutput`) carries
  no backend connection id, so `connection_id` is empty for PostgreSQL sources
  and no setting, in the web interface or in capture, can add it. The Events page says so for
  PostgreSQL sources rather than leaving it an unexplained gap.
- **Replication-health panel.** The Status page shows the replication slot's
  WAL-retention state (`wal_status`, retained WAL, the safe margin before
  invalidation) and whether every published table is at `REPLICA IDENTITY FULL`.
  The web interface is still index-only: it never queries the source. Instead the
  streaming daemon (`bintrail-pg stream` / `watch`) polls the source every ~30s
  and persists a snapshot to the index (`stream_state.source_health`), which the
  web interface renders. Because a snapshot can outlive a stopped daemon, the panel
  shows **how recently it was checked** and **degrades a stale snapshot** (older
  than ~90s) to muted with a warning — a frozen "reserved" must never read as
  live-healthy. If the daemon cannot read the source at all (for example a
  standby, where the slot-retention metrics are unavailable), the panel shows
  **probe failing** with the reason rather than disappearing. For an on-demand,
  always-live check, use `bintrail-pg doctor`.

## MCP endpoint

The web interface serves the same six read-only MCP tools as
[`bintrail-mcp`](mcp-server.md) — `query`, `recover`, `recover_cascade`,
`reconstruct`, `status`, `list_schema_changes` — over **Streamable HTTP**, on
both `bintrail-console serve` and `bintrail-console watch`:

| URL | Target |
|---|---|
| `/mcp` | The web interface's **default server** (same selection rules as the browser UI). |
| `/mcp/{id-or-name}` | A named server from the registry (`default` = the command-line entry). Unknown → `404`. |

MCP clients cannot reliably send custom headers, so the server choice lives in
the URL path (mirroring how the [time-travel port](time-travel-sql.md) routes
by username) instead of the `X-Bintrail-Server` header.

The URL picks the connection's own server. One connection can still reach
every registered server: each tool takes an optional `server` argument, a
server name or id (or `default` for the command-line entry), that sends that
one call to another server. Leave it out and the connection's server answers,
as before. The tool descriptions list the names, and a call that names a
server gets one extra line in its answer, `Answered by server: <name>`. An
unknown name is a tool error that lists the valid ones. The access token's
permissions apply to every server alike, and the audit record names the
server the call went to. One call reaches one server; nothing fans out.

A token minted by a session without the `servers:read` permission is not
shown the server list, the same list `/api/servers` refuses it: the tool
descriptions carry no names, the unknown-name error lists no valid ones, and
an answer or a connection error echoes the name or id the call sent instead
of the server's stored name. It can still route to a server it already
knows.

Point any Streamable-HTTP-capable MCP client at it with the access token as a
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
  from Settings → MCP Server** (#1052) — one click from an authenticated
  browser session, no flags, no restart. The managed token is persisted as a
  SHA-256 hash only (`~/.config/bintrail/console-mcp-token.yaml`, or the
  path given to `--mcp-token-file` / `BINTRAIL_CONSOLE_MCP_TOKEN_FILE`; `0600`,
  atomic write, versioned envelope with the registry's read-only-if-newer
  contract), its plaintext is shown exactly once at generation, and it is
  **scoped to `/mcp` alone** — it cannot drive the browser API (registry
  CRUD, monitor verbs, or its own rotation). New token / Delete token from
  the same card take effect on the next request, including for sibling `bintrail-console`
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
  managed by the daemon: an authenticated MCP client cannot point the
  daemon at an arbitrary DSN or storage prefix, nor change redaction rules.
- **`reconstruct` is gated per server**, on the same signal as the Time-travel
  tab and `/api/reconstruct`: that server needs a baseline location configured,
  with archives enabled and no access-control profile active (baseline reads
  aren't redacted). Otherwise the tool refuses with that explanation.
- **The web interface's read boundary applies.** Result caps match the API (events
  100 default / 1000 max, recover 1000 / 10000), each server's archive and
  baseline posture is honored, and `query_text` / `query_hash` are withheld
  from query results exactly as on the events API.

The host-header allowlist (`--allowed-hosts`) covers `/mcp` like every other
route.

The UI's **Settings → MCP Server** page assembles all of this for you: the
ready-to-copy `/mcp` URL for the selected server (the per-server form when
more than one server is registered), the `.mcpb` bundle download for the
running version, and the raw-config fallback above. Its **Access token** card
generates, replaces (**New token**), and deletes the managed MCP token; the plaintext is
displayed exactly once, at generation, and never stored. For the
start-to-finish walkthrough (bundle install included), see
[Connect an AI assistant](connect-ai.md). Below the three steps, the
**Connect a SQL client** panel does the same for the embedded time-travel SQL
port: see [Time-travel over the MySQL protocol](#time-travel-over-the-mysql-protocol-flashback-port).
After it comes the **Download a DuckDB schema** card, when the server's
Parquet can be described (the `views` capability), with or without the
daemon (#1573).

Last on the page, the **Keep it current with Iceberg** panel (#1466; on
MCP Server since #1573, it used to sit at the bottom of Backups) prints the
exact `bintrail export iceberg` command for the selected server, with its
index connection and its resolved backup destination filled in, a Copy
button, and an hourly cron line. It shows when the selected server has a
backup location (its own or the daemon-wide default) and an index reached
over TCP, and the session can read settings; while a data profile is active
(a named `--profile`, even one with no rules yet, or the session's) the page
does not offer it. The page learns the location from
`GET /api/baselines?location_only=1`, which reads neither the storage nor the
server's index; when that lookup or the server list fails for any reason
other than a refusal, a one-line note takes the panel's place.
Nothing runs from the web interface: the export writes a new copy of your data and is kept
out of the process that captures changes. The password is shown as `***` for
you to replace, and the command carries the index host, port, database and
user and nothing else about the connection, so an index that needs TLS or a
timeout needs those added by hand. The panel also names the compose route
(`docker compose --profile iceberg-export run --rm iceberg-export`), which
runs against the **bundled stack's own** index and backups: for any other
server, point it with `INDEX_DSN` and `BASELINE_DIR` / `BASELINE_S3` in the
stack's `.env`, or run the printed command where bintrail is installed. See
[Iceberg export](iceberg-export.md) and [docker.md](docker.md).

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
| `POST /api/auth/password` | Set (first time; requires static-token auth) or rotate (`current_password` verified) the password. Revokes all sessions and returns a fresh one. |
| `GET /api/status` | Index status (same payload as `bintrail status --format json`). For a session with restricted data access, the capture-health detail names only the tables that session may read; `tables_withheld` counts the rest and the counts stay whole. |
| `GET /api/capacity` | The doctor's index disk-capacity check for the selected server: `{status, reason, retention: {known, retain, source, enabled}, measured, sample_hours, current_bytes, events_per_day, bytes_per_event, growth_bytes_per_day, projected_bytes, remaining_bytes, free_known, free_bytes, days_until_full}`. `status` is `pass`/`warn`/`fail`/`skip` as `bintrail doctor` grades it; `reason` names the branch (`ok`, `headroom_low`, `free_under_floor`, `growth_exceeds_free`, `no_retention`, `free_unknown`, `retention_unknown`, `not_enough_history`, `not_initialized`). Rate and projection fields are absent while `measured` is false; `free_bytes` is meaningful only when `free_known`; `retention.known` is false on standalone `serve`. `502` when the partition statistics cannot be read. |
| `GET /api/coverage` | Live RPO summary from the index alone: restorable delta window `[delta_from, delta_to]`, `lag_seconds`, `continuity`, `freshness` and `checkpoint_age_seconds`. `lag_growth: {grown_seconds, over_seconds, indexing}` is present when the gap between the source and the index widened by at least two minutes, and by at least a fifth of the time that passed, over coverage reads at least four minutes apart (kept per server for 15 minutes, fed by every read of this endpoint), and was still widening over the last minute or more ([#2201](https://github.com/dbtrail/dbtrail/issues/2201)). The growth is the time that passed on this machine minus how far the newest indexed change moved, so a constant offset between the two clocks cancels out. It is a trend over reads, not a verdict: a source nobody writes to widens the same gap, so the Overview shows it only when `GET /api/capture-status` calls capture `behind`, or when `indexing` is true (the newest indexed change kept moving forward, and the gap measured at the reads where it had just moved still grew by the same two-minute and one-fifth rule: capture is indexing, only slower than the source writes; a capture that keeps up with a source writing now and then has next to no gap at those reads) and `freshness` is `current` or `idle`. It reads no backup location ([#1850](https://github.com/dbtrail/dbtrail/issues/1850)): per-snapshot staleness is on the Snapshots page (`GET /api/baselines`, bounded to its window) and in `bintrail status`. |
| `GET /api/capture-status` | Whether capture has read everything the selected server's source wrote ([#1794](https://github.com/dbtrail/dbtrail/issues/1794)): `{server_id, state, detail, retry_in_seconds}`. `state` is `up_to_date`, `behind` or `unknown`. From the index alone a server nobody writes to and a capture that fell behind look the same, so under `watch` the daemon asks the source: it compares the GTID set capture has saved with the source's `@@GLOBAL.gtid_executed` (`@@GLOBAL.gtid_binlog_pos` on a MariaDB source). `up_to_date` means the two sets are equal. `behind` is said only on a second read, when the source holds more than one transaction capture has not recorded and a position saved since the first read still does not hold what the source had then. Everything else is `unknown`, with the reason in `detail`: the read-only `serve`, a server with no source, a capture in binlog-position mode, PostgreSQL sources, a MariaDB source whose position is not exactly the capture's (a MariaDB source is never called `behind`), an index on the source server, tagged GTIDs, a source ahead only by transactions in its `@@GLOBAL.gtid_purged` (for example after loading a `mysqldump` taken on another server, which sets it; no binlog carries those, so capture can never read them), a source one transaction ahead (a `GRANT` or another statement with no rows is recorded when the next transaction arrives), and a read that failed or took over three seconds. `unknown` is never shown as up to date or as behind. The Overview asks after the coverage card is drawn, and only while capture is idle. The daemon keeps each answer for 30 seconds and runs one read at a time per server, so the source is read at most twice a minute per server however many tabs are open (three times while a first read that found the source ahead is being confirmed), and in practice once per coverage read, which is every five minutes on an idle server. Each read opens one connection to the source and runs two `SELECT`s of global variables (one on a MariaDB source). Dropped rows and lost events do not change this answer: the continuity chip and the capture health list say those. |
| `GET /api/activity` | Window aggregate behind the Overview tiles: counts by event type, distinct tables touched, and a per-table breakdown. The window **is the live retention** — derived from the oldest live `binlog_events` partition, so the counts cover exactly what the live index still holds and read the live tier only (no archive scan, and nothing archived can fall inside the window by construction). Returns `{label, since, until, refreshed_at, total, inserts, updates, deletes, other, tables, top_tables, complete, notes}`. The aggregate is a **server-side materialization** refreshed when older than ~30 minutes (a stale copy is served immediately while one recompute runs in the background); `refreshed_at` is when it was computed, and the UI renders it on the tiles ("as of …") so a cached number is never presented as live. `complete: false` means the counts are knowably a floor (an index with a pathological table count trips the grouping cap) and `notes` says so; the UI marks the affected tiles "partial". RBAC deny rules are applied, so a denied table contributes to neither the counts nor `top_tables`, and each deny profile gets its own materialization. |
| `GET /api/schemas` | Schemas known to the index: those observed in `binlog_events` **plus** those in the latest schema snapshot, so a schema whose partitions have all been rotated out to Parquet/S3 is still listed (the archives still answer `/api/events` and `/api/recover`). `schemas` is that full union; `snapshot_only` (when present) is the subset with no live events observed — the UI labels these "snapshot only" since queries against them may return nothing; `snapshot_unavailable: true` means the snapshot half was skipped because the schema resolver failed to load (check the server log), so archive-only schemas may be missing from the list. The snapshot half is skipped under `--no-archive` or an active `--profile`, where archived data is unreachable anyway. Note this answers *which schemas this index knows of*, not *which have data in a given window* — for that, see `bintrail status`'s continuity verdict. `?schema=<name>` → that schema's tables. |
| `GET /api/events` | Event browser. Query params: `schema, table, pk, event_type, gtid, since, until, changed_column, order, limit, limit_per_pk` plus the `after`/`before` keyset cursors. `limit_per_pk` keeps only the latest N events per row, requires `pk`, and is **refused alongside a cursor**: it is a whole-result-set cap, so paging would re-anchor it to each page's remainder. `scope=live` serves the **live index only** and answers immediately (the UI's phase 1: rows in `binlog_events` are milliseconds away, an archive scan can take tens of seconds); the response then carries `scope: "live"` and `archives_pending` (never omitted; `false` is a meaningful answer). `true` means registered archives were **not** read and a follow-up full read is required before the list is complete (the warning says so, loudly); `false` means no follow-up read would add anything: either nothing is registered, or the archives are excluded for this daemon/session (a session profile always announces itself; a --no-archive daemon announces only when the window has gaps to point at). Anything else in `scope` is a 400, never a silent full read. |
| `GET /api/events/head` | `{newest_event_id}`: the highest `event_id` in the live index, `0` when it holds none (#1801). The query carries a five-second deadline, because the daemon sets no write timeout and a metadata lock or a host that went away would otherwise hold the request open for minutes. The Overview asks for it every five seconds to learn whether anything changed, and re-reads the events list only when the number moved, up or down (a stream reset deletes rows). It is tiered with `GET /api/events` and refuses a session whose data profile does not exist on the server, exactly as the list does. It carries no row data, so it is **not** audited: a list read every five seconds per open tab would have written a `query.run` to the audit trail for every one of them, and the list is still audited each time it is actually read. The number is not narrowed by a data profile: it says the index gained a row, not in which table, which the coverage card's newest-event time already gives every session. |
| `GET /api/schema-changes` | DDL history from the index's `schema_changes` table. Query params: `schema, table, ddl_type, since, until, limit`. `ddl_type` is one of `CREATE`, `ALTER`, `DROP`, `RENAME`, `TRUNCATE`, matched as a prefix of the stored type (`ALTER` matches `ALTER TABLE`), like the MCP `list_schema_changes` tool. Default limit 100, max 1000; `has_more` says whether the cap cut the list. Ordered by `detected_at, binlog_file, binlog_pos, id`, all descending, so DDLs detected in the same second keep their binlog order. Returns `{changes: [{id, detected_at, schema_name, table_name, ddl_type, statement, binlog_file, binlog_pos}], count, limit, has_more}`. The session's table deny and allow rules scope the rows by table: a `DROP` or `RENAME` that names several tables has a row for each, each scoped by its own rules; the statement is on the first table's row, and the others say which row carries it. One recorded by a version before this behavior has a single row, under its first table. Under an active access profile (a named profile, session restrictions, or the startup `--profile`) `statement` is empty on every row and `statement_withheld: true` says so, with `warnings` naming the scoping and the withholding. `422` when the index has no `schema_changes` table (an index provisioned before DDL tracking; `bintrail init` adds it). |
| `POST /api/recover` | Undo-SQL generation. JSON body with the same filter fields (requires at least `schema`; an `order` field is accepted but ignored — recover always processes oldest-first). `limit_per_pk` reverses only the latest N events for the matched row and **requires `pk`** — it is the only filter that can separate events sharing a timestamp, since `since`/`until` are second-granular (`/api/events` accepts it too, so Restore's preview can mirror the same window). Returns `{sql, statement_count, row_count, warnings, notes, generated_in_ms}`. `generated_in_ms` is the wall time from request-body decode to the finished script: filter parsing, session-profile resolution, the event fetch including any archive/Parquet leg, cascade victim synthesis when auto-detected, and SQL rendering. It **excludes** selecting and opening the target server's connection, which is a one-off cost of switching servers and can dominate a first request. Always present: `0` means the script was generated in under a millisecond, not that timing is unavailable. When the target is a foreign-key **parent** whose `DELETE` cascaded below the binlog (MySQL/MariaDB index only), cascade victims are **auto-detected** and folded into the same script; the response then also carries `{cascade_detected, victim_count, set_null_count}` (see [Recover and cascade](#cascade-recovery)). Auto-detection needs a single `table` in scope; a schema-wide undo whose window holds a DELETE or UPDATE on a table with cascading children instead gets a `warnings` entry naming those children and saying the script reverses only what was recorded (undo the parent table on its own to have them repaired); a check that fails is reported as such, though an index that never recorded foreign keys answers "no children" (#1616). |
| `POST /api/recover-cascade` | Cascade-recovery SQL generation (reverse FK `ON DELETE CASCADE` / `SET NULL` side effects). JSON body: `schema, table` (the **parent**), `pk, pks, since, until, lookback, max_depth, allow_incomplete`. Returns `{sql, statement_count, victim_count, set_null_count, complete, incomplete, generated_in_ms}` — text only, never executed. Returns `403` under an active RBAC redaction profile (see [Cascade recovery](#cascade-recovery)). |
| `GET /api/capabilities` | Reports enabled optional surfaces for the **selected server**, e.g. `{"reconstruct": true, "recover_cascade": true, "recover_cascade_baseline": false, "source": "mysql", "auth": {"password_set": true, "auth_kind": "session"}}`. The frontend uses it to show/hide gated tabs on every switch (`reconstruct` → Time-travel) and to gate the logout affordance (`auth_kind` says how this request authenticated). `recover_cascade` reports whether cascade synthesis is available (false under an RBAC redaction profile) — it gates the standalone `POST /api/recover-cascade` endpoint; the **Recover** tab's auto-detection follows the same server-side rule. `data_profile` is true while a data profile governs the request (a named startup `--profile`, even one with no rules yet, or the session's); the page then does not offer surfaces that hand out unredacted data, such as the Iceberg export command. `source` (`"mysql"` or `"postgresql"`, read from the index's `stream_state.flavor`) drives **source-aware presentation** only — never a gate; see [PostgreSQL sources](#postgresql-sources). |
| `GET /api/reconstruct` | Single-row point-in-time reconstruct (baseline-gated **per server**; 404 when not configured). Query params: `schema, table, pk, at, history, allow_gaps`. Returns `{found, deleted, state, history, baseline_time, event_count, warnings}`. Refuses with `422` when a `TRUNCATE`, `DROP`, `RENAME` or `CREATE OR REPLACE` on the table falls between the snapshot and `at`, placed by time and by the snapshot's binlog position, as the CLI and the MCP tool do: that statement wrote no row changes, so the row would show as present when it was removed. `allow_gaps` does not override it; a newer snapshot does. Also refuses when the index recorded a permanent capture loss in that window, or is too old to say; `allow_gaps` overrides that one and the finding comes back as a `capture_gap:` entry in `warnings`. A snapshot whose file footer cannot be read is a `500`. |
| `GET /api/servers` | List servers (masked: parsed host/port/user/dbname + `has_password`, never a DSN or password) plus `default_id`. |
| `POST /api/servers` | Add a server to the registry (validates, does not connect; never runs DDL). |
| `GET /api/servers/{id}` | One masked entry (prefills the edit form). |
| `PUT /api/servers/{id}` | Edit. Omitted password = keep stored; `""` = clear; value = replace. `409` for the command-line entry. |
| `DELETE /api/servers/{id}` | Remove from the registry and close its cached connection. `409` for the command-line entry. |
| `POST /api/servers/{id}/test`, `POST /api/servers/test` | Write-free reachability probe (short timeout): `{ok, server_version, dbname, latency_ms, has_index, schema_current}`. Accepts an unsaved candidate body; with `{id}`, a blank password merges the stored one. |
| `POST /api/servers/{id}/monitor/start` | Supervisor only (403 on standalone `serve`): doctor preflight → on green, record intent + provision + stream. Returns `{doctor, started, monitor}`. |
| `POST /api/servers/{id}/monitor/stop` | Supervisor only: clear intent, drain the stream (final checkpoint), release the advisory lock. |
| `GET /api/servers/{id}/monitor` | Supervisor only: `{monitor: {state, last_error, since, source_connected, retrying, phase}}` — `stopped\|pending\|running\|stalled\|lost_position\|failed`. `phase` names a long startup step a `pending` stream is inside, currently only `resume_cleanup` (the pre-capture delete of changes a replayed window would save twice); absent when none is running. |
| `GET /api/servers/{id}/first-run` | Supervisor only, servers with a source: `{complete, steps: [{name, state, detail, fix}], check_error}`, the Overview's Getting started list. `state` is `waiting\|running\|done\|failed`. Each capture step is done from evidence: the server's own index database exists, the supervisor reports `source_connected` for the latest run (reset when a run starts), a schema snapshot (MySQL only), a saved stream position, and a change in the index; a later step's evidence marks the earlier ones done, and the first step not done takes the supervisor's state. `complete` is true once a backup exists for the server. A first-backup step follows the capture steps: with its job's state when backups from the web interface are enabled and the server has its own baseline location, and as `waiting` with a `detail` and `fix` when backups are turned off for the daemon or the server has no baseline location of its own (#1677). It is left out only for a PostgreSQL server with no slot or publication (the server form refuses to save one), which cannot capture either. `complete` is the backup step being done (#1801). A backup ends the list whatever the capture steps are still doing, since seeing the first change is not something anyone can make happen; a backup step that FAILED is not a done one, so it keeps the list up. A capture step that failed deliberately does not, so a finished list never comes back days later: a dead stream is reported by the Overview's own "stopped updating" note, and capture failing before any backup exists keeps the list by the same rule. The server's own backup locations are read for a complete snapshot, at most once a minute per server since an S3 location is a listing over the network, so a backup made before a restart or from the command line ends the step, and one that cannot be read is reported on the step (`detail`) instead of counting as "no backup". The job's own state comes first: a backup that failed or is running outranks an older snapshot. `check_error` means the index database could not be read, and nothing is marked done from it. |
| `GET /api/rotation` | Effective global rotation policy: `{retain, interval, add_future, source, enabled}`; `source` is `"override"` (saved in the web interface) or `"default"` (daemon `--rotate-*`). |
| `PUT /api/rotation` | Supervisor only (403 on standalone `serve`): save a global rotation override `{retain, interval, add_future}` (validated; `off` rejected). Applies live on the next cycle. |
| `GET /api/baselines` | Read-only listing of the **selected server's** baseline snapshots, grouped per snapshot: `{configured, source, kind, reconstruct, snapshots: [{time, age_hours, tables, binlog_file, binlog_pos, gtid_set, data_as_of}]}` (coordinates local-only, capped at 50 snapshots). Every configured location is listed and merged; `sources` reports each one (`source`, `kind`, `count`, `error`, and `skipped`, the number of snapshot or schema directories under it that could not be read, #1601) and `incomplete` is true when any location did not answer or answered only in part. `502` only when no location could be read at all. With `?location_only=1` it answers only `{configured, source, kind}` (the location the listing reads first: the server's own, else the daemon-wide default, a directory over a bucket) from configuration, without the schedule, the storage or the server's index; same permission as the listing, refused while a data profile is active (a named startup `--profile` even with no rules yet, or the session's, wider than the listing because the export it feeds is not redacted); any value other than `1` is a 400. MCP Server uses it for the Iceberg export command. Since #1681 it also carries, at the top level, `local_retention: {keep_newest}` when this daemon removes snapshots past a count from the selected server's local folder, and `last_prune: {at, removed}` (RFC 3339 UTC) once a prune has removed any, read from the `.last-prune.json` the prune leaves beside the snapshots; `last_prune_failure: {at, reason}` while the most recent prune attempt on that folder failed (`reason` holds one cause per line, separated by `\n` only) (from `.last-prune-failure.json`, removed by the next attempt that succeeds); and `last_prune_error` when either record exists but cannot be read. All are omitted, never null, when there is nothing to say. Each snapshot also carries `staleness` (`ok`, `aging`, `broken` or `unknown`), the worst verdict over its tables, and the response carries the headline `staleness` over each table's newest snapshot. A table with table deltas is graded on where its chain of deltas started, the instant a restore reads events from, and `reads_from` gives the earliest such instant of the snapshot when it is not the snapshot's own time ([#1707](https://github.com/dbtrail/dbtrail/issues/1707)). Reading that instant costs one file footer per table, so it is read for the newest snapshot and for each table's newest one. An older snapshot whose tables have deltas is `unknown` unless the snapshot is already past coverage (`broken`): its start was not read, and it is never reported as `ok`. A start that cannot be read is `unknown` too. `data_as_of` is the newest change the snapshot holds, from the run that made it as this daemon recorded it ([#2201](https://github.com/dbtrail/dbtrail/issues/2201)): a full read's own time, or an update's recorded value (on the source's clock). It is omitted when no run on record says, which the Overview reads as "at least as old as capture's lag", never as current. |
| `GET /api/views.sql` | **Not JSON** — a `text/plain` DuckDB schema over the selected server's Parquet (the same output as `bintrail views`), served as a `views.sql` attachment. Nothing is executed here; the file runs in your own DuckDB. `?include_events=1` adds the `events` view over the archived change log, which is left out by default because defining it opens one Parquet footer per archived file (`bintrail views --include-events`). `?include_live=1` adds the leg over the live index (`bintrail views --include-live`), with the index host, port, database and user in the file and never its password; it requires `include_events=1`, since the leg hangs on that view, and 400s without it. 404 when archives are disabled or nothing is archived yet, 403 while an access-control profile is active, 422 when this server cannot carry the live leg (an index reached over a unix socket, or one with no `binlog_events` table), 502 when the index could not be asked, and 400 for an `include_live` or `include_events` value other than `1`/`true`/`0`/`false` (so a request that meant to ask never comes back as an archives-only file). |
| `GET /api/storage` | Process-global storage context: `{aws: {access_key_env, profile, region_env, shared_config, container_creds, web_identity, web_identity_token_readable, web_identity_role_arn}}` — presence booleans and non-secret names only, never credential values. |
| `GET /api/flashback` | Process-global: the embedded time-travel SQL port (`watch --flashback-listen`): `{enabled, listen, host, port, routing}`. `enabled: false` alone on standalone `serve` and on a daemon that did not open the port; `host` is empty on a wildcard bind (the UI then uses the name it was opened with). `routing` (present when the port is on) is the read router's state: `{enabled: false}` when `--route-max-copy-age` is unset, else `{enabled, max_copy_age, cost_threshold, scan_rows, read_only, since, servers}`: a threshold is omitted when that rule is off (0), `read_only` is omitted on a read-write port, `servers` is omitted until a server has a decision, and each entry (keyed by registry id) is `{copy, mysql, refused, reasons, unavailable}` counting decisions since the daemon started (`refused`, omitted while zero, counts statements a read-only port did not run), `unavailable` (omitted when routing works) saying why connections to that server cannot route and read the copy alone. Never the access token that authenticates the port. Backs the **Connect a SQL client** panel on Settings → MCP Server. |
| `PUT /api/flashback` | `settings:write`. `{enabled, listen}`: turn the embedded port on at `listen` (omitted = the address saved last, else one with the web interface's own reach on port 3309) or off. Answers the same document as the GET plus, in the response that creates it, `password`: the port's own password, never returned again. `409` when the address was set at startup, on `serve`, when the address cannot be bound (nothing is changed), or when the saved settings file cannot be read or was written by a newer version; `403` from a session with a data access policy. The GET also carries `source` (`startup`/`saved`), `can_manage`, `suggested_listen`, `has_password`, `password_created_at` and `error` (why a port saved as on is not listening). |
| `POST /api/flashback/password` | `settings:write`. Replaces the port's password and returns the new one once. Open connections stay; the next login needs the new password. Same refusals as the PUT. |
| `GET /api/sql-settings` | `settings:read`. Process-global: the memory each SQL-on-the-copy statement uses (#2210): `{memory, memory_bytes, value, source, saved, default, floor_mb, max_unmerged_mb, max_in_flight, host_memory_bytes, warning, can_manage, locked, error}`. `source` is `startup` (`--sql-memory` / `BINTRAIL_CONSOLE_SQL_MEMORY`, which wins), `saved` or `default`; `saved` names a saved value a startup one overrides; `max_unmerged_mb` is the changes a statement's tables may hold unmerged at that memory; `warning` is set when `max_in_flight` statements could take more than the host's memory (`host_memory_bytes`, omitted when unknown); `locked` says why `can_manage` is false; `error` is why the saved file did not load. Backs the **Memory for SQL on the copy** panel on Settings → MCP Server. |
| `PUT /api/sql-settings` | `settings:write`. `{"memory":"4GB"}` saves the memory (KB/MB/GB or KiB/MiB/GiB, at least 512MB) and applies it to the next statement; `{"memory":""}` goes back to the default. Answers the GET document. `400` for a value that is not such a size (or no `memory` field); `409` when the memory was set at startup, when this console keeps no settings file, or when the saved file cannot be read. |
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
table that is a foreign-key **parent**, the web interface detects it (one index lookup
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
partial (a coverage gap, a per-parent overflow, or hours of the baseline window
the scan can't serve — rotated out with no readable archive, or a recorded
capture loss) the warnings carry a prominent **provably partial** notice
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

When `--baseline-dir` or `--baseline-s3` is set, the web interface can reconstruct a
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
with the access token. It replaces running a separate `bintrail shim` per
per-source index: the daemon already resolves each server's `bintrail_idx_<id>`
and baseline, so one port covers them all. A token is required (`--console-token`
/ `BINTRAIL_CONSOLE_TOKEN`) because MySQL-protocol auth cannot use the web interface's
password store. The port does not filter by schema: anyone with the token can
read the full history of every schema on every monitored server. For per-schema
filtering, use the standalone `bintrail shim` (or `bintrail-pg flashback`) with
`allowed_schemas` on each tenant. Full setup, routing, and the `_snapshot` baseline-parity edge:
[docs/time-travel-sql.md → the embedded port](time-travel-sql.md#the-embedded-port-multi-source).

The web interface shows the port on **Settings → MCP Server**, in the **Connect a
SQL client** panel (#1446): when `watch` opened it, the listen address, the
user rule (the server picked in the sidebar: its registry name or id, `default`
for the command-line entry), the password rule (the access token, never
displayed) and a ready-to-copy `mysql -h <host> -P <port> -u <server> -p` line
for that server. When the port is off, the panel turns it on: an address and
a **Turn on** button, which also creates the port's own password and shows it
once (**New password** replaces it, **Turn off** closes the port: the call
answers once a new connection is refused, and closes the connections already
open). A session
without `settings:write`, or one with a data access policy, gets no controls.
When the address was given with `--flashback-listen` /
`BINTRAIL_CONSOLE_FLASHBACK_LISTEN`, that decides and the panel shows it as
fixed; on standalone `serve` it says the port belongs to `watch`.

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
the web interface renders it as a muted line, not as an alert. Cautionary facts
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
