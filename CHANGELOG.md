# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.80.0] - 2026-09-09

### Fixed
- **A table with a `GENERATED ALWAYS AS ... STORED` column can be folded**
  (#1624). `bintrail baseline` leaves generated columns out of the dump, so
  the baseline Parquet has no such column, while every ROW image carries it.
  The guard that refuses a column added after the baseline (#602) read that
  as a schema change and refused the table with `ErrSchemaChanged`; on a
  schedule with the full-backup opt-in on, the refusal fell back to a full
  backup at every slot. Columns the baseline's `CREATE TABLE` declares as
  generated (MySQL's `GENERATED ALWAYS AS`, MariaDB's short `AS (expr)
  PERSISTENT`, and explicit `ROW START`/`ROW END` period columns, decided by
  the same parser that built the baseline) are now left out of the emitted
  rows, which the server recomputes on load. A generated column that was
  turned into a plain column after the baseline still refuses and names
  itself, since the baseline holds no value for it.

## [0.79.0] - 2026-09-09

### Changed
- **Backup schedules can run every 5 minutes** (#1620). The Backups page
  refused any `every` under `15m`. That floor predates #1539: a server whose
  backups go to S3 now folds from the bucket and uploads the result, so a
  five-minute schedule costs it neither a full read of the source nor
  unbounded local disk, and a reporting copy of a busy table wants exactly
  that cadence. The floor is now `5m`. The two costs the old number guarded
  against are still said out loud at save and at boot: a server with an S3
  destination and no local backup directory takes a full backup every slot,
  and a local-only server keeps every snapshot it publishes.

### Added
- `docker-compose.yml` passes `BASELINE_RETAIN` through to
  `BINTRAIL_CONSOLE_BASELINE_RETAIN`, so a short schedule can prune its
  local snapshots once S3 holds a copy without editing the compose file.
  It reclaims only servers that have an S3 destination.

## [0.78.0] - 2026-09-07

### Fixed
- **Point-in-time restore folds from the server's S3 backups** (#1541). The
  console's Restore listed the backup to fold from in the server's local
  directory only, which on an S3-backed server holds what this daemon folded
  since it started, so every backup that was uploaded and pruned, or made by
  another host, was invisible and the restore refused with "no backup exists
  at or before" while the bucket held dozens. It now reads where the scheduled
  update reads (`BaselineFoldSource`: the bucket when the server has one, the
  local directory otherwise), still writes into the local directory, and
  uploads the result so retention can reclaim it; a failed upload keeps the
  local snapshot and says so, and a restore at the exact second of a backup
  the bucket already holds is refused rather than overwriting it. The
  Overview coverage card, which was graded on the old behaviour, now grades
  against the location the button actually reads: `GET /api/coverage` gains
  `restore_reads` (`s3`/`dir`, empty when the server has no local directory
  and Restore refuses it, `inherited` when it grades the daemon-wide locations), and `offsite_tables` is **renamed**
  `unreachable_tables`, whose meaning mirrors per server — on a `dir` server,
  tables backed up only in a daemon-wide bucket; on an `s3` server, tables
  backed up only on this host, which the daemon-wide refresh interval and a
  failed upload both produce. The Backups page's restore lane offers the
  snapshots the fold reads, and says which location the skipped ones are in.

### Changed
- **Console: the Backups & snapshots settings page is now Backup settings, and
  it shows the three kinds of setting instead of describing them** (#1603).
  The nav item and the page head both read Backup settings, so the pair with
  the Backups page reads as work vs configuration (the item already sits
  inside the sidebar's Settings group, which is why it is not the bare word).
  The three kinds are told apart by layout: the disk-space switch and the
  per-server rows sit under "Change here" and apply at once; the daemon's own
  values sit under "Set when dbtrail starts" on a plain card, retitled Set at
  startup, with ONE "Restart to change" chip at card level instead of nine
  identical chips. An empty daemon value renders as the word for what applies
  (`none`, `off`, `all tables`, `temp folder`), never as `not set`, which read
  as nine faults on a healthy install.
  Two rules that used to be paragraphs are drawn: the disk-space switch as two
  backups of five tables, the unchanged ones carried across as dashed tiles
  and the changed ones written again; and which backup location is in force,
  as a three-row legend (own location, daemon default, no location) with the
  server's own case highlighted and a tick or a cross per lane, so the daemon
  default's "time-travel reads, backups and restores refuse" is one glyph
  instead of a sentence. Everything a reader does not need in order to act is
  compact by default under "More about ..." blocks, not cut, and each block
  links into the docs guide; the page head carries the Docs link the page
  never had. Visible text on the daemon-side cards drops from 1719 to about
  900 characters, and the guards that keep it there: an e2e budget with the
  fine print compact, a Go test pinning the drawn location cases to the API's
  own `Source` verdicts in both directions and by count (a fourth verdict on
  either side fails), and the e2e holding each server's drawn case against
  the API response. Nothing about what the settings do changes.

### Added
- **`doctor` warns about tables with no primary key** (#1608). Those tables are
  not captured at all: the first snapshot REFUSES outright and the stream does
  not start, and a later snapshot after a schema change EXCLUDES them and skips
  their row events. Until now nothing said so before the refusal. The check
  names the tables, up to ten, keeps the count exact past that, and its
  remediation states what actually happens rather than describing degraded
  recovery over data that does not exist.
  It is advisory on every path, including its own errors: nothing downstream
  consumes the answer, since the snapshot re-derives it and refuses on its own,
  so a transient `information_schema` error must not stop capture on a source
  that is healthy. Nothing visible in scope reports `skip`, never `pass`.
  The finding comes from `metadata.TablesWithoutPrimaryKey`, which is the
  snapshot's OWN classifier, rather than a second query asking a similar
  question. Both directions of drift were measured on live servers before this
  landed: a `TABLE_CONSTRAINTS` question warns about a `UNIQUE NOT NULL` table
  that MySQL marks `COLUMN_KEY = 'PRI'` and the product keys perfectly well,
  and a `TABLE_TYPE = 'BASE TABLE'` filter misses MariaDB's `SYSTEM VERSIONED`
  tables, which is the shape where the data loss is total (#1272).

## [0.77.0] - 2026-09-03

### Fixed
- **Restore Coverage is graded across every backup location** (#1571). The
  panel that answers how far back a server can be restored derived that from
  `bundle.baselineSrc` alone, so on a server with a local directory and an S3
  destination a table whose only surviving anchor lives in the bucket was not
  graded, not named broken, and not counted — a clean verdict over an inventory
  missing a table. It now merges both locations through the same
  `listBaselinesMerged` the Backups listing uses (#1542), and **a location that
  fails to list** makes the verdict `unknown` rather than grading the half that
  did: a partial listing can only understate coverage, and an understated
  window names healthy tables broken. (A location that lists but skips an
  unreadable snapshot directory returns no error, so it is not covered by that
  rule.) Merging can also flip a verdict from `ok` to `unknown`, when the
  second location contributes a table whose anchor is unattributable.
  A table whose only usable backup is in S3 is reported in a new
  `offsite_tables` bucket instead of widening the restorable window: the
  listing reads every location, but the Restore button folds from the local
  backup directory alone (#1541), so counting that anchor would print a start
  the button then refuses. It is not `broken_tables` either — that drives an
  alarm and advises a fresh backup, and the backup exists. Time travel still
  reads it, through the S3 fallback `bundle.findBaseline` already has (#766) —
  which is also why a table whose STALE LOCAL copy shadows a fresh offsite one
  stays in `broken_tables`: that fallback fires only on `ErrNoBaseline`, so a
  local hit means no console surface ever reaches the bucket. On a server that
  backs up only to S3 there is no local anchor at all, so the card states that
  once (`restore_needs_local`) instead of naming every table. Tables that
  could not be graded at all are named in `unevaluable_tables` rather than
  collapsing into a bare "could not be checked": on an index whose archives
  cannot be attributed to one source, the ambiguity demotion (#1219) turns
  every shadowed table's verdict into `unknown`, which is the right call for
  the verdict and would have erased the inventory it applies to.
- **The downloaded `views.sql` says when a newer backup lives somewhere it does
  not read** (#1571). The file names ONE root and every state view resolves a
  path under it, so merging the two locations would produce views that half of
  its readers cannot open — the paths would not resolve. What silence cost was
  quieter: with the newest snapshot aged out of local retention but still in
  the bucket, the file pinned the older local one and read as current. The
  header now names the newer snapshot, where it is, and the control that reads
  it instead ("Works on another machine"). The route is named only when the
  control moves the reader *toward* the newer snapshot: with the box already
  ticked the newer snapshot is the local one, and unticking would hand a file
  of local paths to someone who asked for one that travels, so the fact is
  stated and the route withheld. A second location that will not answer costs
  the download nothing and is disclosed in the header, since silence there is
  indistinguishable from "it holds nothing newer" and the two lead to opposite
  actions. Every operator-supplied path printed into the header is now escaped
  for newlines: a registry backup path containing one ended the comment and
  left the rest of the value on a line DuckDB would execute.

### Changed
- **The `events` view binds one Parquet footer per SCHEMA, not per archived
  file** (#1535). `archive_state` gains `column_set`, the archived file's own
  column set, and `bintrail views` and the console's DuckDB-schema download now
  emit one `read_parquet` per distinct set — each with an explicit file list and
  `union_by_name = false`, padding the columns a group lacks with `NULL` and
  joining the groups with `UNION ALL BY NAME`. A view re-binds on every
  statement, so what used to cost one footer read per archived file, forever
  growing, now costs one per group; the S3 LIST the glob needed goes away with
  it. Explicit paths still carry `hive_partitioning`, so `bintrail_id`,
  `event_date` and `event_hour` are still synthesized and a filter on them
  still prunes files.
  Rotation records the set for archives it writes, and `restore-index` records
  it for archives it re-registers. For archives already on disk, `bintrail
  archive reconcile --repair` records it from the footer it already reads for
  `row_count` — no extra file opens. **On an S3 archive that means `--deep
  --repair`**: without `--deep` no remote footer is read, so the repair records
  nothing. The first reconcile after upgrading reports drift on every partition
  that predates the column; that is the backfill asking to be run, not a new
  fault.
  Until every registered partition has a recorded set AND its file is where the
  registry says it is, the generated SQL keeps the globbed form and says so,
  naming the command. The file list comes from the registry rather than a glob,
  so grouping a partial one would leave those partitions out of the view rather
  than merely slow to bind — and a listed file that is not there makes DuckDB
  refuse the whole script, `events` and every state view with it.
  **A grouped file does not follow the layout**: it names its archives
  explicitly, so partitions archived after it was generated are not in it. The
  header says which of the two forms the file uses; regenerate on the schedule
  rotation archives on. `archive reconcile` now migrates `archive_state` before
  reading it (that table only — its dry run is a cron drift monitor, and the
  full migration also touches `binlog_events`).

## [0.76.0] - 2026-09-01

### Added
- **The Backups page answers what to download, in two lanes.** A reader who
  opens it wants a copy of their tables, and until now the page listed backups
  without saying what a copy is made of. There are two lanes now, drawn rather
  than explained. To open in DuckDB: the Parquet files plus the `views.sql`
  that tells DuckDB how to read them, two downloads, and the lane says so and
  shows the two tiles. To load into MySQL: one download, plain SQL in mydumper
  format, built for whatever moment you pick, with the build starting from the
  backup before that moment and replaying the changes already recorded. The
  source database is never touched by either.

- **The backups list pages five at a time**, and a row opens with the chevron
  instead of scrolling a hundred snapshots to reach the oldest.

### Changed
- **A row of settings cards fills its row, whatever the count.** The grid was
  three fixed columns, which is right only for the pages that carry three
  cards. Retention and Backups carry one, so a third of the row was card and
  two thirds were nothing. A card left alone on a row now spans it, at every
  width, rather than leaving a hole beside itself.

- **The "Backups & disk space" card was rewritten, and it no longer promises a
  saving that cannot happen.** It used to open with two label and value rows
  reading `reuse: off` and `set by: daemon default`, which name neither what is
  reused nor who set it, and it printed command line flags at a reader who is
  clicking buttons. The state is a word at the top now, and the rest is
  sentences: what the setting does to your disk, what it does not change about
  your backups, and whether anything is using it yet.

  It also stated the disk saving unconditionally. Reusing a file means linking
  it on disk, so it only happens when the previous backup is read from the
  machine dbtrail runs on. A scheduled backup on a server that has a Backup S3
  reuses nothing and writes every table. The card says which, and the daemon
  log has been saying so on every published run.


## [0.75.0] - 2026-09-01
### Fixed
- **A DuckDB schema file now refuses a dropped table up front, by name, before
  it creates any view.** When a table is dropped at the source it leaves the
  snapshot, and the read used to break at that table's own `CREATE VIEW`. DuckDB
  does not roll back a multi-statement script, so against a persistent database
  what remained was a partial schema whose contents depended on emission order,
  reported as a missing Parquet path — which reads as a corrupt snapshot rather
  than as a table somebody dropped on purpose. The generated file now checks
  every table it names against one listing of the snapshot, and raises naming the
  tables, where it looked, and what to do. A pinned file is not checked: it names
  a snapshot that held them all when it was written.

- **The Backups page lists every backup location, not just the local folder.** A
  server with a local directory and an S3 destination kept the bucket as a
  per-table fallback that time-travel consulted and the listing did not, so once
  local retention pruned, the page showed nothing while the bucket held dozens.
  The listing merges both, says where each backup was found, and reports a
  location it could not read instead of quietly serving a shorter list. A backup
  held only in the bucket can now be opened and downloaded, and the restore and
  `.sql` build offer only the backups they can actually read.

### Added
- **Each table in a backup says how it got there**: read from the source, built
  from the recorded changes, or reused unchanged from the previous backup. Per
  table rather than per backup, because a refresh that reuses cold tables makes
  one backup a mix. It answers a question no surface could answer before — how
  much of this was independent evidence from your database, and how much was
  computed — and it is shown in the expanded backup detail.

### Changed
- **The Iceberg export panel draws what the export produces instead of
  describing it**, and says which engines read the folder directly versus through
  a catalog.
- **The Download a DuckDB schema card draws what the file will hold, instead of
  describing it.** It carried three checkboxes, each trailed by a multi-line
  caveat, on a page whose job is to be legible to someone who has read nothing.
  The one decision a reader actually makes is whether to include the change log,
  and its whole cost is a shape: your tables are one Parquet file each, the
  change log is one file per archived hour and grows forever. Ticking the box
  lights the strip, so the difference is seen rather than read.

  `--pin-snapshot` and `--include-live` are no longer offered here. Both remain
  CLI flags and route parameters; neither is a decision to put in front of a
  first-time reader, and the live leg in particular can compete with capture on
  the source server. A file downloaded from the console can no longer carry the
  live leg, so its archives-only note names `bintrail views --include-live`
  rather than a checkbox that no longer exists.

  It is a full-width panel below the three numbered steps, beside "Other AI
  tools" and "Connect a SQL client", not a fourth card inside the steps grid.
  A schema file for your own DuckDB is not a step in connecting Claude, and the
  grid tints every child by position, so in there it took a colour and a
  treatment that read as step four on a page that promises three.

  Under 300 rendered characters, pinned by an e2e budget of its own. That guard
  checks the drawing is on screen as well as the count, because folding prose
  out of sight passes a character budget while leaving the same wall one click
  away.

### Removed
- **The console's SQL page and `POST /api/sql`** (#1549). The page answered
  free-form `SELECT`s with DuckDB running inside the daemon, which is why it
  needed a sandbox, a statement gate, a table-function allowlist, a
  forensics-column filter, two timeout budgets and a single-query latch. The
  download it sat next to needs none of that: it hands you a schema over files
  you already own and executes nothing.

  It was also not usable on a real archive. Defining the `events` view opens one
  Parquet footer per archived file before returning a row (`union_by_name` is
  load-bearing for correctness, so the read cannot be skipped) — 114.2s over
  1886 files, against a 30s setup budget. `SHOW TABLES`, the only way to learn
  the derived `state_*` names, built the whole catalog and hit that budget; and
  the page's one on-screen example named `events`, so it failed the same way.
  The page could not answer its own example, and the way out of that was the
  query that timed out.

  Query the same Parquet in your own DuckDB instead: **Download a DuckDB
  schema** on **Connect** writes a `views.sql` over the same files, with no row
  cap and no time limit. `BINTRAIL_CONSOLE_SQL_PANEL` is still read for one
  release and warns that it no longer does anything; a later release stops
  reading it. `GET /api/views.sql` is unaffected.

### Changed
- **The Download a DuckDB schema card moved from the SQL page to Connect**
  (#1549). `GET /api/views.sql` requires `settings:read`, but its only route in
  the UI was a nav item gated `data-perm="query:execute"` and on the `sql`
  capability. So a `settings:read` role could not reach a download the server
  would have authorized, a `query:execute` role without `settings:read` got a
  button that only 403s, and `BINTRAIL_CONSOLE_SQL_PANEL=0` removed the card
  altogether while `views` stayed on — taking it from the operator who declined
  server-side query execution and whose file executes nothing. Connect carries
  no capability or permission gate of its own and already calls `settings:read`
  endpoints. The SQL page keeps a line pointing at it.

### Fixed
- **`bintrail views` no longer demands `--index-dsn` for a baselines-only
  file** (#1552). Only the `events` view reads an archive source, and since
  0.74.0 that view is opt-in, so the default file defines `state_*` views over
  a baseline location and names no archive path at all. The check was not
  revisited when the default flipped, so it went on asking for the index in
  order to discover sources the file would never use — and refused the exact
  shape a snapshot downloaded from the console arrives in, on a machine that
  may not reach the index at all. The documented way around it was to invent
  an `--archive-dir`, which produced the file that had just been refused.

  `--baseline-dir`/`--baseline-s3` on its own is now enough. `--include-events`
  without `--index-dsn` and without `--archive-dir`/`--archive-s3` is still
  refused, and names the view that needs the source rather than the flag that
  supplies it. A file that would define nothing at all is still refused, by the
  check keyed on what the file would contain, so the reason names the file
  rather than a flag combination.

## [0.74.0] - 2026-08-31

### Changed
- **The `state_*` views in a generated `views.sql` now follow the newest
  baseline snapshot** (#1484). A baselines root gains a `current` symlink beside
  its timestamped snapshot directories, updated when a NEWER snapshot completes,
  and the state views read through it. (A snapshot produced for a past instant,
  such as a point-in-time restore, completes normally and does not take it.) A snapshot published after the file was
  written therefore reaches it with nothing to re-run — which is the case the
  file used to warn about and could do nothing else about: a daemon running
  `bintrail-console watch --baseline-refresh-interval` publishes on a timer, and
  nobody performs an action "regenerate this file" could attach to. All the
  views move together, because replacing the pointer is a single `rename(2)`, so
  no path ever resolves into a half-published snapshot and a running query
  finishes against the files it opened. (Two views resolved either side of that
  one step can still land on different snapshots; the window is the swap itself,
  not the hours a stale file spans.)

  What does not follow is the file's idea of the SHAPE of the data: which views
  exist, and the precision each `DECIMAL` column is read at, are still decided
  from the snapshot named in the header, so regenerate after a table is added or
  dropped or a column changes type. The generated file says which of the two it
  did rather than leaving the reader to work it out.

  Following is the default and pinning is the opt-out: `bintrail views
  --pin-snapshot`, or the **Pin to the backup that exists now** checkbox on the
  console download (`pin_snapshot=1`). Several shapes pin regardless, and the file always says
  which it did: an S3 baseline root, which has no pointer to follow (copying
  every table object to a second prefix is not atomic across tables, so a query
  could read half of one snapshot and half of another); a root whose `current`
  is a real directory, which is never replaced; a root written by an older
  bintrail, until its next snapshot completes; and a pointer naming a snapshot
  other than the one the views were resolved from. The pointer is invisible to recovery: discovery and
  prune both enumerate with `ReadDir`+`IsDir`, which reports false for a
  symlink, and additionally require a timestamp name, so it can never be
  selected as a baseline. Retention also keeps whatever it names, so it cannot be
  left dangling by a publish that failed and left the pointer behind, and a
  snapshot dated in the FUTURE never takes it (`--timestamp` has no upper bound,
  and one future-dated snapshot would otherwise outrank every real one after it
  and freeze the pointer permanently).

  Publishing is serialized by an flock on the baselines root. Deciding whether a
  snapshot outranks the newest complete one and then renaming the link are two
  syscalls, and a peer completing a newer snapshot in between made the older one
  win: measured at 118 in 400 concurrent pairs comparing against the pointer,
  and still 12 in 400 after moving the comparison to the directory listing.
  flock rather than a sentinel file, as elsewhere in this project, because the
  kernel releases it when the process dies.

  The SQL panel pins rather than follows. It executes the views it builds, so
  following buys it nothing and costs it per-statement consistency: a join over
  two state views resolves two paths, and a pointer swap between them would
  return a join of two snapshots that never coexisted. The downloadable file
  discloses that window in its header; the panel has no header to disclose it
  in.

- **`bintrail views` no longer emits the `events` view by default; add
  `--include-events`** (#1535). Defining that view makes DuckDB open one Parquet
  footer per archived file before it returns a single row — `union_by_name` is
  load-bearing for correctness there, so the read cannot be avoided — and the
  cost is O(archived files) and grows forever. Measured on a real source of 1886
  files it was 114s to define, against 3.2s without. Everyone reading the file
  paid it, including an operator who only wanted their tables: the `state_*`
  views read one file each. The console download grew a matching **Include the
  change log** checkbox (`include_events=1`), with **…and the live index**
  nested under it, since that leg is part of the `events` view.
  `--include-live` without `--include-events` is refused rather than silently
  turning the expensive view on, and `--no-baselines` alone is now refused too
  because it would define nothing. The generated file says which views it left
  out and how to ask for them, and no longer claims a self-following behaviour
  it only has when the `events` view is present.

  A render that would define **no view at all** is now refused outright, on both
  surfaces: `--no-baselines` was refused by name, but the identical empty file
  was reached by simply not naming a baseline location — a shape that used to
  produce the `events` view, so the flip turned a useful command into one that
  exited 0 and wrote a file with nothing in it. The `bintrail views` summary
  line names the events view either way, so an unchanged scripted invocation
  says what it left out instead of looking exactly as it did before.

  **This does not make the `state_*` views follow a periodic baseline refresh**
  (#1484): they still name one snapshot path, resolved when the file was
  generated. Regenerate on the same schedule the refresh runs on. The change
  makes the default file cheap, not self-updating.
- **The Storage page is split in two, and two of its cards moved to the pages
  they belong to** (#1543, part of #1528). Storage had become a drawer: seven
  cards from five unrelated concerns, on a page you had to read rather than
  use. It is now **Retention** (the rotation policy and per-source S3
  archiving: what happens to your data as it ages) and **This daemon** (AWS
  credential signals, staged downloads and the telemetry opt-out: what this
  process can reach, is holding, and sends). *File reuse for unchanged tables*
  is now **Backups & disk space** and lives on the Backups page directly under
  **Scheduled backups** — the two names both promised "backups,
  automatically" from different pages for unrelated settings, and side by side
  under names that say what each does, the difference needs no paragraph. The
  *Download a DuckDB schema* card moved to the SQL page, which queries the
  same data. The Backups summary card is gone: Backups has its own sidebar
  entry, and the pointer existed only because the page it pointed away from
  was a drawer. Existing `/storage` links still work and land on Retention.
- **A scheduled backup on a server whose backups go to S3 no longer forces a
  full read of your database every slot** (#1539). The daemon has two ways to
  produce a backup: a full one it dumps from the source, and an update of the
  newest backup folded from the recorded changes, which touches the source not
  at all. An S3 destination used to force the full one at every slot, on the
  reasoning that only a full backup uploads, so the cheapest producer was
  unreachable on exactly the servers most likely to run nightly: a schedule
  that was supposed to be free became a nightly full read of production.
  The scheduled update now reads its previous snapshot straight from the
  bucket, folds into the server's local backup directory, and uploads the
  result to the same destination a full backup would have written to, with the
  same crash-safe ordering (`_INCOMPLETE` first, `_SUCCESS` last), so a run
  interrupted mid-upload leaves the remote copy excluded from discovery rather
  than half-visible and readable. An upload that finds no completed snapshot to
  bracket is now refused outright rather than sending the data unmarked, which
  is the shape that would have read as a complete backup. A local backup directory is still required,
  because the fold writes Parquet to a filesystem, and a server that has only
  an S3 destination still gets a full backup: the reason the page now shows
  names that setting, which is the one that unlocks the cheap path, rather than
  the destination, which is not the thing to change. Publishing is not counted as finished until the
  upload succeeds, and a failed upload keeps the finished local snapshot and
  says the fold itself worked. The daemon-wide `--baseline-refresh-interval`
  is unchanged: it names no destination, so its snapshots stay local.
  Two failures had to stop meaning what they used to. A failed upload does NOT
  trigger the stand-in full backup: that fallback exists because a failed
  update produced nothing, and here it produced a snapshot, so falling back
  would answer one S3 permission error with a full lock-and-read of production
  that publishes nothing new. And a previous backup that cannot be READ (a
  throttled bucket, an expired credential) no longer costs the slot: it takes a
  full backup, naming the real cause, because before this change those servers
  were guaranteed one without touching the network. That degrade is scoped to a
  REMOTE source: an unreadable local directory still refuses the slot, because
  it is persistent and the full backup that would stand in writes into that
  same directory. Both the console and the run history now say when a scheduled
  update wrote its backup but could not send it, rather than reporting that
  nothing was published, a successful upload records how many files it sent,
  and the prune pass reports the snapshots it could not reclaim instead of only
  the ones it did.
  Two consequences worth knowing. Retention improves: `PruneLocal` reclaims a
  local snapshot once it can confirm its `_SUCCESS` in S3, so the snapshots a
  scheduled update publishes on an S3-backed server are now prunable, where
  before nothing this fold produced ever was. And carry-forward of unchanged
  tables does not apply when the previous snapshot is read from a bucket — it
  publishes a table by hard-linking the previous file, which needs both on a
  filesystem — so those runs take the ordinary merge path: correct, and the
  same rows, just not free.

### Fixed
- **Baseline upload to S3 no longer fails on the baselines root's `current`
  pointer, and no longer skips a symlinked table file.** `Upload` walks with `filepath.WalkDir`, which does not follow
  symlinks, so a link to a directory arrived with `IsDir()` false and was handed
  to the file uploader, which opens the path, follows it to a directory, and
  fails with "is a directory" — taking the whole upload down. Found while adding
  the `current` pointer above, which is exactly such a link. The walk now skips
  the pointer and its staging links by name, and RESOLVES every other
  non-regular entry rather than skipping it: an operator who symlinks one large
  table's Parquet onto another volume had it uploaded before and must keep
  having it, since skipping quietly would publish `_SUCCESS` over a snapshot
  missing a table and the loss would only surface mid-recovery. Something that
  is neither the pointer nor a file is now a loud refusal instead. The pointer's
  lock file is skipped by name: it is a regular file, so the symlink test cannot
  see it and it would otherwise be published as snapshot data.

- **views.sql: an index this machine cannot reach no longer costs you the whole
  file** (#1536). `duckdb -init` aborts the session at the first error, and the
  `ATTACH` was emitted ahead of every view, so an unreachable index host left
  the reader with nothing: no `events` view and no `state_*` views, even though
  all of them read Parquet and needed nothing from the index. The `state_*`
  views are now defined first, then the `ATTACH`, then the `events` view that
  reads through it. DuckDB keeps what ran before the aborting statement, so the
  table snapshots survive an index this machine cannot reach — in a session
  that outlives the error: `.read views.sql` from an open DuckDB, or
  `duckdb -init views.sql your.db` and then reopen `your.db`. A bare
  `duckdb -init views.sql` exits and its in-memory database goes with it, so
  nothing is kept; the generated file says which is which, and says plainly
  that nothing survives when the file defines no `state_*` view at all.
  The deliberate trade: under `--include-live` a failed `ATTACH` costs the
  `events` view entirely, not just its hot leg. Defining it archives-only
  beforehand and replacing it afterwards would bind the archive file list twice,
  and that bind is the expensive statement in the file — `union_by_name` opens
  one Parquet footer per archived file at `CREATE VIEW` time (#1535), measured
  at ~7s over 120 files with the second definition costing ~3.6s more, whether
  it repeats the literal or just references the first view.
- **views.sql warns when the index host is a bare name** (#1536). A console
  running under Docker Compose emits its compose service name (`index-mysql`),
  which resolves for containers on that network and nowhere else, so a
  downloaded file failed with `Unknown server host` the first time it was run.
  The existing warning covered only loopback addresses. A single-label host now
  gets its own note, kept separate because the two fail differently: loopback
  resolves everywhere and quietly answers from a different index, while a bare
  name usually does not resolve at all.

## [0.73.0] - 2026-08-30

### Added
- **The daemon reports a compose stack the images have outgrown** (#1529).
  `docker compose pull && up -d` upgrades the images and leaves
  `docker-compose.yml` alone, because that file belongs to the operator and was
  downloaded once. Volumes, mounts, ports and profiles can only come from it,
  so a stack keeps whatever it was wired with the day it was downloaded, and
  the worst case is silent: with no durable home, console state lands in the
  container and every recreate deletes it, and a console that starts with no
  state looks exactly like a fresh install. `bintrail-console watch` now says
  it once, at startup, on stderr: one line per problem, each naming the fix.
  Two checks ship, both evidence-based and both silent when they cannot
  establish a fact rather than guessing at one. The first fires when the
  index DSN in effect is the bundled index and the read-only mount that
  measures its free disk space is not there, using the same condition the
  capacity check itself applies so the two can never contradict each other.
  The second fires when console state files this daemon will open are on the
  container's writable layer while others are on a volume, or when the console
  settings folder is on the writable layer and holds console state. Everything
  is gated on the process observing that its own root filesystem is a
  container image layer, so a bare-metal daemon and a machine with no mount
  table to read are both silent. A bring-your-own index silences the
  disk-space finding specifically, by the same rule the capacity check
  follows; the console-state finding does not depend on which index is in
  use. No card in the UI, no timer, no repeat.
- **`docker-compose.yml` carries a version** (#1529). A new
  `x-bintrail-compose-version` key at the top of the file, passed to the
  console service, so a newer daemon can say a file is behind and name what is
  not in effect instead of leaving the operator to diff two files. Optional by
  design: a stack that passes no version gets exactly today's behaviour and
  never a false alarm.

### Changed
- **The console's SQL page is on by default in the daemon** (#1529). The
  default used to live in the bundled `docker-compose.yml`
  (`BINTRAIL_CONSOLE_SQL_PANEL: ${SQL_PANEL:-1}`) while the binary defaulted
  off, so the decision reached new installs only: pulling a newer image
  delivered the page to nobody. The default now lives in the daemon and the
  compose file does not mention the variable at all. `serve` and `watch` both
  show the page unless `BINTRAIL_CONSOLE_SQL_PANEL` turns it off. That
  variable is an opt-out now, so it fails closed: `0` and `false` hide the
  page, and so does any value the daemon cannot read either way (`off`, `no`,
  a typo), which is logged with the values that work rather than serving the
  page to someone who believes they closed it. Everything
  that makes the page safe is unchanged: it is behind console sign-in, it
  carries `query:execute`, it is refused outright while an access-control
  profile is active, and it answers one `SELECT` at a time in a sandboxed
  DuckDB session that can read nothing but the selected server's archive and
  backup folders. **If you hide the page with `SQL_PANEL=0` in `.env`**, note
  that the shortcut only worked through the line the compose file no longer
  carries: after re-downloading the file, add
  `BINTRAIL_CONSOLE_SQL_PANEL: "0"` to the `bintrail` service instead.
- **The upgrade documentation says to re-download the compose file** (#1529).
  [docs/install.md](docs/install.md) and
  [docs/docker.md](docs/docker.md) described the upgrade as `pull` plus
  `up -d`, which is true of the binaries and not of the stack around them.
  Both now spell out the third command, and docker.md gains an "Upgrading the
  stack" section with a table of what a stale compose file costs, starting
  with the console user store, because that one is silent loss rather than a
  missing feature. The download lands on `docker-compose.yml.new`, beside the
  operator's file rather than over it, so the edits the next step asks them to
  carry across still exist when they get there. `install.sh` no longer just
  says it kept an existing compose file: it says an existing file is never
  upgraded, and what to do about it.
- **Console: the reuse setting is named for what it does, and two cards stop
  explaining themselves** (#1528). The Storage card titled **Automatic backup
  refresh** controlled exactly one thing, whether a table with no changes keeps
  its previous Parquet file instead of being written again, and it had neither
  automation nor a timetable in it. One route away, **Scheduled backups** on the
  Backups page IS the timetable, so two unrelated settings both promised
  "backups, automatically". The card is now **File reuse for unchanged tables**,
  its two paragraphs are one line per state, and its buttons read **Turn on** /
  **Turn off**. It stays on Storage rather than moving beside the schedule it was
  confused with, because `/api/baseline-refresh` is process-global while the
  schedule is per server, and a global toggle inside a per-server fold would
  claim something false. The **Scheduled backups** fold loses the paragraph that
  explained the producer choice in general directly above the line that names it
  for the next run. The consequence of a reused file, two backups sharing the
  same bytes on disk, moved to [docs/console.md](docs/console.md), which also
  gains the card's own entry. The Storage cards are reordered so what the daemon
  does with your data comes first and usage telemetry, which is about neither
  storage nor this page, comes last. Smaller cuts to the same rule on the AWS
  credentials, Staged downloads and telemetry sample cards. The **Query in
  DuckDB** card is now **Download a DuckDB schema**: no query runs there, the
  file it hands you runs in your own DuckDB, and the page that does run DuckDB
  server-side is the SQL panel. The two are not merged, because they are gated
  by different capabilities (`views` vs `sql`) and merging would make the
  download unreachable on a daemon with one on and the other off. The generated
  `views.sql` names the card by title, so both sides moved together.
  No setting, route, API, permission or capability gate changes; 124 words leave
  the screen. Storage is still a drawer, and the split is proposed on the issue.

### Fixed
- **The console's SQL panel reports the whole wait, and builds only the views
  your statement names** (#1526). The number on screen used to be the
  statement's own time, measured after the session was already built, so
  `SELECT 1` reported 0 ms after about 16 seconds on a console whose backups
  live in S3, and sent the reader looking at their query for a cost that was
  never in it. The result line now says both: the whole wait, and how much of it
  the statement took. The wait itself is smaller, because a session no longer
  defines every view in the layout before running anything. Defining a view over
  Parquet binds its columns, which opens the file, and on an S3 layout that is a
  network round trip per file, so a query that named one table was paying for
  every table in the newest backup. The panel now parses the statement first, on
  a throwaway DuckDB session that is sealed from its first statement and reads
  nothing, and then builds only the views that statement names, plus none at all
  for one that names none. Measured against a stand-in for S3 latency, with 20
  tables in the backup: `SELECT 1` went from 1.85 s and 40 network requests to
  32 ms and none, and a query over one `state_` view from 40 requests to 2. A
  statement naming something the layout does not define still builds everything,
  so DuckDB's own "did you mean" answer stays as useful as it was, and so does a
  catalog listing, which has to list what is really there. Those two are the
  longest waits the panel has, so a failed statement now reports its wait as
  well, instead of leaving the line blank. Warnings describe the session that
  answered: a query that opens no baseline file is no longer told what those
  files are missing. Nothing about the panel's posture moves: still one
  `SELECT` at a time, still the same
  allowlist, sandbox, budget and audit trail, and still refused while an
  access-control profile is active.
- **Console: two arms of the AWS credentials card report the signals they
  probed instead of naming a credential source they never observed** (#1528).
  Two of the card's five arms asserted use from presence. The shared-config arm is selected by `hasSharedAWSConfig()`
  (a file stat) **or** `AWS_PROFILE`, and it sits below the env-key, ECS and
  IRSA arms, so it caught both an EC2 host with an instance role and a
  region-only `~/.aws/config` (what `aws configure set region` writes) and a
  container with `AWS_PROFILE` exported and no `~/.aws` mounted; it said "Using
  credentials from a shared ~/.aws config file", which named only one of the two
  probes and, in the second shape, contradicted the card's own
  `~/.aws config: absent` row two lines below it. The access-key arm said "Using
  access keys set in an environment variable" from `AWS_ACCESS_KEY_ID` alone,
  which is false whenever the ID is exported without its secret. Both arms now
  name the signal they saw, say what was not checked, and say an IAM role on the
  machine can still be what signs the requests. The card's question is whether
  this daemon can reach S3 at all, so a confident wrong answer was worse than a
  hedged right one; these two arms are the reason the card's word count went up
  rather than down. The Raw signals row under them is relabelled `access key ID
  (env)`, since `access keys (env): set` contradicted the summary two lines
  above it. **The ECS and EKS arms are deliberately not part of this**: both
  still read "Using an IAM role" from an environment variable being non-empty,
  with no stat and no `AWS_ROLE_ARN` check, which is weaker evidence than the
  file stat behind the shared-config arm. Hedging their copy would paper over a
  signal the daemon could check properly instead, so they are tracked in #1534
  and named as unhedged in the code. The **Scheduled backups** fold
  regains, on its unconditional intro line, the clause saying a run is built
  from the recorded changes and does not read your database: with no schedule
  saved yet every per-run line is gated behind a saved schedule, leaving "each a
  full copy of every table" (true of what a run writes) as the only description
  of what a run costs. The **Staged downloads** empty state says again that a
  build is removed, rather than implying it.
- **The Index disk card says why free space is not measurable, and what
  would make it measurable, instead of guessing where the index runs**
  (#1527). "Free on disk: not measurable from here" came with the sentence
  "The index runs on another host or container", which the check never
  learned: it is what the code assumed after a measurement did not land, and
  it reads as plainly false on a single machine that runs everything. The
  capacity check now reports the branch it landed on next to the verdict
  (`free_reason` on `GET /api/capacity`, and the same wording in `bintrail
  doctor`), and the card reads it. An index this process can reach locally,
  including the bundled stack's `index-mysql`, is told the fix: mount the
  index data directory into the console read-only and set
  `BINTRAIL_INDEX_DATADIR_RO` to the mount point, the way the bundled
  `docker-compose.yml` wires both. A mount that is set but unreadable says
  so, rather than blaming the topology. An index answering at another
  address is told that free space cannot be seen from here and is offered no
  local mount at all, because a volume that is not the index's would report
  the wrong free space, which is worse than reporting none. An index on a
  local address whose server cannot be confirmed to be this machine, which is
  what a port-forward or an ssh tunnel looks like, is its own state: the mount
  is still offered, with the warning to point it at the index's own data
  directory and nothing else, because a local database's data directory may
  well be sitting right there and pointing at it would show a measured number,
  with the thresholds live, for a volume that is not the index's. Nothing new is
  measured and no path is ever guessed: the only directory this check stats
  is still one the operator named, or the server's own data directory behind
  the existing locality gate, so the shipped compose file's guarantee that
  the mount and the index DSN cannot drift apart is untouched. The check
  stays advisory, and the reason never moves a grade.

## [0.72.0] - 2026-08-30

### Added
- **The console's DuckDB schema download can include the live index**
  (#1480). The **Query in DuckDB** card on the Storage page grows an
  **Include the live index** checkbox, and `GET /api/views.sql` an
  `include_live=1` parameter, so the downloaded `views.sql` can carry the
  same second leg `bintrail views --include-live` adds: the recent changes
  rotation has not archived yet. Off by default, and the card says why in
  plain words before you tick it, because that leg reads the live capture
  index and cannot narrow the read, so every query over it scans the whole
  table and competes with capture on that server. The file carries the index
  host, port, database and user, resolved from the connection the console
  already holds, and never its password: the slot is written empty for the
  reader to fill in their own session. A server whose index this console
  reaches over a unix socket, or whose database has no `binlog_events`
  table, or an address with a port and no host, refuses with that reason
  (422) instead of quietly downloading the half that works, and an index that
  could not be asked answers 502. The
  tier and the audit posture do not move: `settings:read`, unaudited (view
  definitions are not row data), and refused outright while an
  access-control profile is active, the parameter included. An `include_live`
  value the route does not understand is refused with a 400 naming the ones
  that work, rather than read as "off" and answered with an archives-only
  file whose own note tells the reader to tick the box they believe they
  ticked. When the leg is not included, that note names the checkbox rather
  than a command-line flag the reader is not at a command line to pass. If
  the index's registered sources cannot be read, the file says so, and the
  daemon now logs the reason: the file states one outcome where a revoked
  `SELECT`, a dropped connection and a timeout are three different things to
  fix.
- **A "Keep it current with Iceberg" panel on the console's Backups page**
  (#1466). For the selected server it prints the exact `bintrail export
  iceberg` command, with the index DSN and backup destination filled in from
  what the page already knows (the resolved destination decides between
  `--baseline-dir` and `--baseline-s3`), a Copy button, the hourly cron line,
  and a collapsed "How to run it" block. The index password is elided as
  `***`, the way the SQL-client panel elides the console token. The panel
  says the address and folder in the command are the ones this console uses,
  which in the bundled stack are inside Docker, and offers the compose
  profile for that case, naming what that profile actually exports: the
  stack's own index and backups, which for any other server is a different
  dataset exported successfully and with nothing to see, so it names the
  variables that point it (`INDEX_DSN`, `BASELINE_DIR`, `BASELINE_S3`). It is a
  command and not a button on purpose, and the panel says so: the export
  writes a new copy of your data and is deliberately kept out of the process
  that captures changes, so a long export can never slow capture down. No new
  API route, nothing executed, and nothing in the console links the Iceberg
  writer.
- **An `iceberg-export` profile in the bundled compose stack** (#1466). A
  one-shot service that runs `bintrail export iceberg` against the bundled
  index and the baselines in the state volume, writing Iceberg tables into a
  new `bintrail-iceberg` volume:
  `docker compose --profile iceberg-export run --rm iceberg-export`, with the
  cron line in the file and in [docs/docker.md](docs/docker.md). It uses the
  CORE image, like the `baseline` profile, because the console image ships
  without the export commands, and it is behind its own profile so it never
  starts with the stack. `WAREHOUSE_DIR`, `BASELINE_S3` and `ICEBERG_TABLES`
  tune it; with no snapshots to export from it exits with a message naming
  the directory it looked in and the command that makes one.

### Changed
- **The console's SQL panel reads and renders in UTC** (#1177). DuckDB's
  default timezone is the host's, so on a daemon whose machine is not set to
  UTC an archived `event_timestamp` printed shifted and
  `date_trunc('day', event_timestamp)` bucketed events by a local midnight,
  both silently. Every panel session now sets UTC, which is what bintrail
  stores. The operator could not have fixed it themselves: the panel takes
  one `SELECT` and refuses every other statement, `SET` included, which is
  deliberate and unchanged.
- **The bundled compose stack enables the console's SQL page** (#1177). The
  page is still off for a bare `bintrail-console serve` or `watch`, where the
  operator chose the bind; in the stack, whose console is published on the
  host loopback only, it is on so the page is there to find. Set
  `SQL_PANEL=0` in `.env` to hide it.

### Fixed
- **The console's Copy buttons say when they cannot copy.** The clipboard API
  does not exist outside a secure context (plain HTTP on anything but
  localhost), where every Copy button threw and looked as if it had worked.
  They now tell you to copy the text by hand, and say why.

## [0.71.0] - 2026-08-29

### Added
- **Access profiles from the console** (#1445). A Settings > Access profiles
  page authors the flags on tables and columns, the named profiles and the
  allow/deny rules that `--profile` enforces, against the server selected in
  the sidebar (the command-line entry included), on `serve` and `watch`
  alike. The validation and the writes are one shared package,
  `internal/accessprofiles`, which the `bintrail flag`, `profile` and
  `access` verbs now call too, so a profile authored in either place is the
  same rows and is refused with the same words. The page and its routes
  (`GET /api/access-profiles` plus six `POST` verbs under it) need
  `settings:read` to view and `settings:write` to change; while an
  access-control profile is active (a startup `--profile`, even one with no
  rules yet, or the session's own data profile) the whole page is refused,
  reading included, whatever the permissions; each change is audited as `console/flag.add`,
  `flag.remove`, `profile.add`, `profile.remove`, `access.add` or
  `access.remove`, naming what changed and the server it landed on; and a
  change reaches a profiled session on its next request (the console drops
  that server's cached rules when it writes). On both surfaces names are
  trimmed, a value longer than its column is refused with the limit in the
  message rather than as a raw database error, and a profile or flag whose
  spelling differs from an existing row only by case or accents (the index
  compares names without regard to either) is refused, naming the stored
  row, instead of silently re-describing or keeping it.
- **The console's Status page shows the index disk-capacity projection**
  (#1444). An **Index disk** card carries what `bintrail doctor` already
  computes, for the selected server: index size, write rate, the size the
  index settles at for the configured retention, free space on the index
  volume and how long it lasts at the current rate. The grade is the
  doctor's (`GET /api/capacity` runs the doctor's own probe and verdict over
  the console's connection, with no thresholds of its own); a warn or fail
  grade also renders a box above the cards with the plain reading and what
  fixes it, so a filling index volume is seen where operators look, before
  capture stalls. Honest where a piece is missing: free space the process
  cannot measure (the index on another host or container) reads "not
  measurable from here", never a number, and the standalone read-only
  console, which runs no rotation, reports the retention as not known
  instead of grading an index another process rotates as unbounded; only
  the free-space floor still warns there. Under `watch` the window is the
  effective rotation policy, override or daemon default, and rotation off
  grades as unbounded growth.
- **Console pages link to their docs page** (#1450). Every view whose subject
  has a page on www.dbtrail.com/docs now shows a small Docs link beside its
  title, opening the page in a new tab: Events and Restore (the recovery
  guide), Backups (backup strategy), Verification, Storage (capacity
  planning) and Connect AI (Claude setup). It is a plain link, so an
  air-gapped console makes no request for it. The route to page table lives
  in one place in the console's app.js; a Go test pins it to that exact set
  of pages, and `BINTRAIL_CHECK_DOCS_LINKS=1 go test ./internal/console/
  -run Docs` fetches each page and checks it is the real page, not the
  site's catch-all shell. Views with no page of their own show no link.
- **The console's telemetry card shows the exact sample event** (#1447). The
  Usage telemetry card (Settings > Storage) gains a folded "Show a sample
  event" section with the JSON one event would carry, byte for byte as
  `bintrail telemetry show` prints it. Both surfaces render it through one
  function (`telemetry.SampleEventJSON`), pinned by a test that compares the
  console payload with the command's output, so the card can never drift into
  a hand-maintained copy. Read-only; opening it sends nothing.
- **`bintrail export iceberg`** (#1466). Writes each table's current state as
  an Apache Iceberg table under `--warehouse`, incrementally: the first run
  loads the newest baseline snapshot, every run after that folds the events
  since the table's own cursor into ONE Iceberg snapshot (equality deletes for
  every touched key plus the rows that still exist). The table is never
  rewritten. The cursor is the run's binlog cut, stored in the table's
  properties in the same commit as the data, so nothing is written to the index
  and a run that dies before committing resumes from the previous snapshot.
  Refuses per table, with `baseline refresh`'s vocabulary, on a capture gap,
  skipped events, a cut behind the cursor, a schema or type change or a
  destructive DDL in the window; refuses tables without a primary key or with
  a BIT column, and an index that holds more than one source. Every commit is
  audited as `cli/export.iceberg`. DuckDB and Spark read the directory
  directly; Trino, Athena and Snowflake read it once registered in their
  catalog. See docs/iceberg-export.md.
- Guard `cliapp/icebergfree_test.go`: the Iceberg and Arrow libraries are
  imported only by `internal/icebergexport`, and only that package, the
  `cliapp` root and `cmd/bintrail` may link them. The guarded set is derived
  from `go list` over the module minus that three-entry allowlist, so a new
  package is guarded the day it appears; the console, MCP and pg binaries are
  pinned free of both. Records the #1467 decision mechanically: Iceberg is an
  output, not the storage layer.
- **Schema changes view in the console** (#1443). A new read-only page under
  Investigate lists every CREATE, ALTER, DROP, RENAME and TRUNCATE the stream
  recorded for the selected server: time, table, type, the statement and its
  binlog position, newest first, with the same schema, table, type and time
  filters as Events and the same caps (100 by default, 1000 at most, and a
  note when more exist). Backed by `GET /api/schema-changes`, which reads the
  index's `schema_changes` table, takes the filters the MCP
  `list_schema_changes` tool takes (`ddl_type` is a prefix, so `ALTER` matches
  `ALTER TABLE`), and orders by `detected_at, binlog_file, binlog_pos, id`,
  all descending, so DDLs detected in the same second list in their true
  binlog order. Under an access policy the rows are scoped by the table the
  index attributed each statement to (a statement naming several tables is
  attributed to the first), and the statement text is withheld, as the Events
  view withholds `query_text`: DDL text can carry values and name other
  tables. A statement that did not name its schema (`USE app; ALTER TABLE
  users ...`) is recorded with an empty schema, so a deny on a table also
  withholds unqualified DDL on a table of that name, and the page shows such
  rows by table alone. The response says what it left out.
- **Primary key ranges on `query` and `recover`** (#1440). `--pk-min` /
  `--pk-max` (MCP: `pk_min` / `pk_max`), inclusive, either bound alone or
  both, answer "every event on ids 1000 through 1999" without enumerating the
  keys into `--pks`. Single-column integer keys only: the table's key is read
  from the schema snapshot before anything runs, and a composite, DECIMAL,
  FLOAT, string or binary key is refused with the table's actual key shape.
  The stored key is text, so both engines cast it to a 64-bit integer whose
  signedness matches the column (SIGNED/UNSIGNED on the live index, BIGINT/
  UBIGINT over the Parquet archives), and a bound the column cannot hold is
  refused up front. Both engines accept the same spellings, the canonical
  decimal rendering of an integer, through a round trip of the cast (a
  `DECIMAL` key stored as `2.00`, a composite `1|2`, `abc`, `007`, `+5` or
  an empty key is out on both sides), and the live and archived result sets
  are pinned equal for keys that expose string order (9, 10, 100), negatives,
  64-bit width and those left-behind spellings. The range cannot use the key
  index, so it scans the partitions the time filters keep: the help text says
  so and points at `--since`/`--until`. See docs/query-and-recovery.md.
- **The Connect page shows the time-travel SQL port** (#1446). Settings →
  Connect AI gains a "Connect a SQL client" panel for the embedded
  MySQL-protocol port (`bintrail-console watch --flashback-listen`). With the
  port open it shows the listen address, the user rule (the server picked in
  the sidebar, by its registry name or id), the password rule (the console
  token, never displayed) and a ready-to-copy `mysql` line for that server.
  With the port off it says so and names the flag and environment variable
  that open it, as daemon configuration; the standalone `serve` console says
  the port belongs to `watch`. Backed by a new read-only `GET /api/flashback`
  (`{enabled, listen, host, port}`, `settings:read`; `host` is empty on a
  wildcard bind, so the page fills in the name it was opened with). Display
  only: the console never opens or closes the port.

### Changed
- **Staged `.sql` backups expire** (#1448). A backup built from the Backups
  page used to stay in the daemon's staging directory until the next build
  or a restart, parking a full plaintext copy of every table on the index
  host with nothing on any page to show it. Now the build is removed the
  moment a download completes, 4 hours after it finished if nobody
  downloaded it, and at once when the build fails; a new build for the same
  server still replaces the previous one. The Ready line shows the size and
  the download deadline, and the status reports `expires_at`, then
  `downloaded` or `expired` once the file is gone. Startup sweeps every
  build a previous process left behind, one build at a time, and deleting a
  staged run by hand now reads as `expired` on the next poll instead of a
  download button that answers 409. Every removal is scoped to the
  `sql-export` staging directory and refuses any path outside it or across
  a symbolic link. The Storage page gains a **Staged downloads** card
  (`staging` in `GET /api/storage`) with each build's server, size and
  deadline, so the space is visible while it exists. A previous build that
  a new build could not remove stays on that card (state `replaced`, with
  the reason), is named in the new build's `staging_error` until its removal
  succeeds, and is retried every minute; the new build stays downloadable
  (`removal_owed` in the status says when a build's OWN removal is what is
  pending). A download whose response writer cannot flush aborts and logs
  the writer type instead of consuming the build.
- **Usage telemetry classifies first-run failures** (#1503). A fresh install
  that could not start reported every attempt as `error_class: unknown`, so
  the one thing telemetry exists to show — why a first run fails — was
  invisible. Errors now declare their own class through a small interface
  (`telemetry.Classed`), with no message text involved: a refused preflight
  (`doctor`, `bintrail-pg doctor`, or `up` / `bintrail-console watch`
  refusing to boot) reports the class of what failed —
  `db_connection` when the source or index could not be reached,
  `db_permission` for grants, index write access or schema access,
  `config_invalid` for a server setting (as do the `binlog_format` and
  `binlog_row_image` refusals that `stream`, `index --source-dsn` and `agent`
  run without the doctor) — the server's
  "binlog purged" error
  (1236, which only the replication client ever sees and which the classifier
  used to miss because it arrives as go-mysql's error type, not the driver's)
  and the `--no-gap-fill` refusal are `binlog_not_found`, the stale-snapshot
  guard is `schema_mismatch`, a missing snapshot is `not_found`, a server
  identity conflict is `config_invalid`, and "host not allowed" (1130) is
  `db_permission`. Three classes no code path could ever emit
  (`binlog_parse`, `flag_invalid`, `network`) are removed from the taxonomy
  and from `docs/TELEMETRY.md`. The wire format and `schema_version` are
  unchanged.

### Fixed
- **`export iceberg` no longer records `snapshot_id: 0` for a zero-row first
  load** (#1509). A baseline with zero rows commits the table and its cursor
  and no data file, so the table has no Iceberg snapshot; the audit event
  named snapshot `0`, an id no snapshot has. The event still fires (a table
  and a cursor were written) with `rows: 0` and no `snapshot_id`. The
  `--format json` outcome already left the field out, but only because 0 is
  the zero value; it now leaves it out on absence. `Outcome.SnapshotID` and
  `Commit.SnapshotID` are `*int64`, nil when absent.
- **`export iceberg` renders a JSON column one way on both paths** (#1508).
  The first load copied the baseline's text, which is MySQL's own rendering
  of the document (keys in MySQL's order, a space after every comma), while
  the deltas re-encoded the decoded row image (keys sorted, no spaces, `<`
  escaped), so one value read as two texts depending on which run wrote the
  row. Both paths now emit one rendering: keys sorted, no whitespace, `<`,
  `>`, `&` and numbers as written. The load parses and re-emits the
  baseline's text; a JSON column whose text does not parse refuses the load
  instead of copying it. The first load records which columns it rendered
  as JSON in the table's properties, in the same commit as the data, so
  every later run renders the same columns whatever the schema snapshot
  says, and a column changed between JSON and TEXT since the load is
  refused like any other type change. A table loaded by a build without
  this property keeps MySQL's text in its loaded rows until the table
  directory is removed and reloaded.
- **`list_schema_changes` keeps same-second DDLs in the order they ran**
  (#1441). `detected_at` has one-second resolution and a migration lands
  dozens of statements inside one second, but the tool sorted by that column
  alone, so inside a same-second group the rows came back in storage order:
  oldest first, the opposite of the newest-first promise. A CREATE listed
  before the DROP that followed it read as "dropped, then created". The order
  now breaks ties by binlog coordinate (`binlog_file`, then `binlog_pos`) and
  finally by `id`, which also makes a `limit` that cuts inside the group keep
  the same rows on every call. The tool description and docs carry the one
  caveat the stored data forces: `schema_changes` records no source identity,
  so in an index fed by more than one source, or by a Postgres source (whose
  LSN file names are not zero-padded), same-second changes have a repeatable
  order, not their true one. The unfiltered listing used to walk the
  `detected_at` index backward and stop at the limit; it now costs a sort over
  the filtered rows, so narrow with `since`/`until` on a large table.
- **The console's capture-health detail no longer names tables a restricted
  session cannot read** (#1452). `GET /api/status` served
  `capture_health.skipped[*].tables` and the explanation prose built from
  them to every session that may see the health verdict, and those name the
  tables whose capture stopped. Every other listing already withholds a table
  name from a session with a data profile or with per-role access rules, so
  this was the one inventory left: a session allowed only `app.users` could
  read the name of every table with a capture skip.
  The names now pass through the same rule the table pickers use, per name:
  a denied table, or one outside the session's allow list, is dropped from
  the list and from the explanation, which is rebuilt from the filtered
  ledger rather than left carrying the names. The counts do not change (a
  count is not a name), and the entry says how many names it is not showing:
  `tables_withheld` on the wire, and "app.users and 2 tables outside your
  access" in the prose. A reason whose every table is withheld still gets its
  sentence, in the singular when it is one table. An unrestricted session, and
  `bintrail status`, render the ledger exactly as before.
  Two other fields of the same payload can still carry a table name and are
  not filtered by this change: `stream.source_health.replica_identity_not_full`
  (PostgreSQL sources) and `files[].error_message` (file-index mode).
  One change beyond the names: to resolve the scope, `/api/status` now looks
  the session's data profile up on the selected server, as the events and
  schema listings do. A session whose profile is not defined on that server
  gets a 403 there too (and a 500 if the lookup itself fails), where it used
  to be served unscoped. On a console with several servers, a profile
  defined on one server and not on another now refuses the status page for
  the second one.

## [0.70.0] - 2026-08-28

### Added
- **`bintrail views --include-live`** (#1480). The generated DuckDB views read
  the Parquet tier, which only holds partitions `rotate` has already archived,
  so the most recent window lived nowhere in the file and a query about it
  returned no rows exactly as if nothing had happened. The new flag adds a
  second leg over the index itself, making a DuckDB query fresh to capture lag
  instead of to the archive retention window. Overlap between the two is
  deduplicated by `event_id` with the **archives** winning: an archived row
  knows its `bintrail_id`, `event_date` and `event_hour` from its storage path
  and an index row has to derive or forgo them, so the leg that knows the source
  is the one that survives the overlap. The index leg reads `event_timestamp` as
  UTC explicitly, because the archives' Parquet column is timezone-aware and the
  index's `DATETIME` is not, and a union of the two otherwise reads the index's
  values in whatever timezone the reader's session happens to have. It names
  only columns the index actually has, so an index migrated to an older schema
  still yields a file that binds rather than a binder error and no view at all.
  The file carries the index host, port, database and user and never its
  password: fill the empty slot in your own session. It also says what a
  loopback host means for a reader elsewhere, and what a query against the view
  costs, where you will be looking when you measure it. Attribution of the live
  rows states what was OBSERVED (one source, several, none registered, the
  registry unreadable, or an id that disagrees with the archives') rather than
  one sentence covering all five. With nothing archived yet the file is the
  index leg alone rather than no `events` view at all (#1485). Without the flag
  the file is unchanged except that it now states, in the `events` view's own
  comment, which events it does not cover and how to add them.

- **S3-compatible object stores (MinIO, Wasabi, LocalStack) for every S3
  path** (#1453, #1454). `BINTRAIL_S3_ENDPOINT=scheme://host[:port]` points
  the SDK clients (rotation archiving, `upload`, baseline upload and prune,
  `restore-index`, `archive reconcile`, `doctor`, `init`) AND every DuckDB
  httpfs read (`query`/`recover` over archives, `reconstruct --baseline-s3`,
  `verify`, the shim, the console, `--ultrafast`) at the same store; before,
  the SDK half could follow `AWS_ENDPOINT_URL_S3` by accident while the DuckDB
  half always went to `s3.amazonaws.com`, so a baseline that verifiably
  existed read as missing. Bucket-in-path addressing is on by default with
  `BINTRAIL_S3_ENDPOINT` (`BINTRAIL_S3_PATH_STYLE=false` for virtual-hosted-only
  stores). `AWS_ENDPOINT_URL_S3`/`AWS_ENDPOINT_URL` are honored as fallbacks
  and otherwise left to the SDK, so an environment already configured for the
  AWS CLI keeps its behavior; an endpoint set only in `~/.aws/config` routes
  the SDK half alone and now warns, since DuckDB reads no AWS configuration.
  An invalid `BINTRAIL_S3_ENDPOINT` fails any command that reads or writes S3,
  instead of falling back to AWS, on the baseline read paths too; a command
  whose data is entirely local is not refused over a setting it never reads. Routing is applied with `SET GLOBAL`
  rather than only inside the credentials secret, so it survives an air-gapped
  host where the `aws` extension cannot be installed, and it reaches every
  connection of a pool rather than the one that ran it. The
  `views.sql` download and `bintrail views` name the endpoint in their routing
  statements AND in their secret, so the file reads the same store from
  another machine even when its secret fails (an interactive DuckDB continues
  past a failed statement). Every S3 client in
  the tree is now built by `storage.NewS3ClientFromConfig`, and CI runs the
  round trip against a real MinIO.

### Fixed
- **A crash while refreshing a schema snapshot no longer stops capture**
  (#1497). Under `bintrail-console watch` one process is both the console and
  the capture plane, and neither of the two goroutines behind the Status page's
  **Refresh schema snapshot** button had a panic guard, so an internal error
  while re-reading a source's column layout ended replication capture for every
  monitored server. A schema snapshot that did not refresh is a degradation, a
  daemon that stopped capturing is an outage, and the first must never cause
  the second. This is the defect #1494 fixed for the four backup jobs, in the
  file that fix did not cover.
  Both goroutines are guarded now, and the guard is deliberately not a quiet
  swallow, which would trade a loud outage for a silent one: the stack is
  logged at error level, which is the only place it is recorded now that the
  process does not die printing it, and the run is reported as **failed**
  carrying the error. That second half matters as much as the first, because a
  new snapshot is refused for a server whose previous one still reads as
  running, so a guard that only logged would leave that server's button
  answering "already running" until the daemon was restarted.
  The report names the half that broke rather than hedging over both. An error
  while reading the source's columns says capture was not touched, which the
  ordering makes a fact rather than a guess. An error during the stream restart
  says the schema snapshot itself was taken and recorded, keeps its table
  counts, and says that this server's capture may now be stopped, naming the
  page and the button that fix it (Manage servers, Start). Warning about
  capture on every panic instead would have been a false alarm on a surface an
  operator has to trust during an incident.
  Two related repairs. Starting a monitored source reserves its slot before it
  provisions, and a crash in that window used to end the process; now that one
  can be recovered, the reserved slot is marked failed first, because nothing
  else ever clears it and a later Start on it would have reported success while
  doing nothing. And a snapshot that outlives its ten minute bound and then
  crashes no longer leaves the earlier "the source did not answer" text
  standing, which blamed the source for something this daemon did.
  One gap is contained but not undone, and it is worth knowing about because
  the cost is not only the stopped stream: while the restart is interrupted
  that server can be missing from the daemon's list of active sources, so its
  index is not archived or pruned on the rotation schedule until capture is
  started again.
- **The managed MCP token survives a container restart** (#1493). The token
  minted from **Settings → Connect AI** is stored as a SHA-256 hash in a file
  that had no path override, so it always resolved under `$HOME` inside the
  image while the compose stack redirected every other piece of console state
  onto the persistent volume. It therefore lived in the container's writable
  layer and was destroyed by any `docker compose up` that recreated the
  container. Nothing reported it: a missing token file is also what a console
  that never had one looks like, so the daemon came up quiet and the AI client
  you had configured got a 401 on every call with no server-side explanation.
  The file now takes a path the same way the registry and the auth file
  already did, `--mcp-token-file` / `BINTRAIL_CONSOLE_MCP_TOKEN_FILE` on
  `bintrail-console serve` and `--console-mcp-token-file` on `watch`, and the
  shipped `docker-compose.yml` points it at
  `/var/lib/bintrail/console-mcp-token.yaml` in the `bintrail-state` volume.
  What this means for an upgrade: **the default path did not move**, so a
  console that uses it keeps reading the same file and no existing token is
  affected. **On the compose stack, generate a new token once** after taking
  the updated compose file: the console reads the volume path from then on, so
  a token minted before the change does not carry over, and the replacement is
  the first one that is still there after the next restart. Paste the new value
  into your AI client; the old one authenticates nothing.
- **`sum()` works on a money column in the generated DuckDB views** (#1486).
  MySQL `DECIMAL` and `NUMERIC` columns are stored as text in the baseline
  Parquet, so the `state_<schema>_<table>` views handed DuckDB a `VARCHAR` and
  the first aggregate anyone wrote against a money column died with
  `No function matches the given name and argument types 'sum(VARCHAR)'`,
  which reads like the data is wrong rather than like a storage choice. The
  views now cast those columns back to `DECIMAL(p,s)`, using the precision and
  scale read from the `CREATE TABLE` in each file's Parquet footer. This works
  on baselines you already have: nothing about how a value is stored changed,
  and nothing needs re-taking. Text remains the storage form on purpose, and
  the reason is worth knowing before reaching for a wider type: MySQL allows
  `DECIMAL(65,30)` while DuckDB stops at 38 digits, and the stored text is also
  what the recovery paths join and compare on, byte for byte, against the value
  read out of the binlog. Some columns still read as text, and the generated
  file names each one and says why: a column wider than 38 digits has no DuckDB
  `DECIMAL` to be cast to; a baseline taken before bintrail embedded the
  `CREATE TABLE` in the footer carries no column types to read; and a
  PostgreSQL-source baseline stores every value as text and never carries that
  key, by design, so there is nothing to read. The console's SQL panel
  executes these views and discards their
  text, so when a file carries no column types it now says so next to the rows
  instead of leaving a `sum(VARCHAR)` error unexplained. `docs/parquet-debugging.md`
  covers casting by hand for anyone pointing DuckDB at the files without the
  views.
- **One primary-key limitation, one sentence, and `recover-cascade` no longer
  calls it a transient failure** (#1460, #1461). A primary key whose type the
  baseline canonicalizer cannot handle (`FLOAT`/`DOUBLE`, `TIME`, `BIT`,
  `JSON`, the spatial family) is refused on several surfaces, and each one used to word it
  differently: full-table `reconstruct` said the type "is not in the supported
  PK type set", the shim's full-table `_snapshot` said "the baseline merge
  cannot canonicalize" it, and `verify` and single-row `reconstruct` said it
  is "unsupported by the baseline canonicalizer". An operator who met two of
  those had no way to tell it was the same refusal. All of them now render the
  same sentence, the last one, inside whatever frame the surface needs, so
  full-table `reconstruct` and the `_snapshot` wire error read differently
  only where they name the table or point at `_flashback`. Full-table
  `reconstruct` no longer ends with "file a follow-up issue if you need this
  type"; the per-row message underneath it still carries that advice.
  `recover-cascade` gets the bigger change. Its Phase-2 baseline lookup can
  hit the same limit on a child table, and it used to arrive as an ordinary
  error, which the engine filed under "baseline lookup failed ... (recovery
  may be partial)". That reads as bad luck worth retrying, when nothing about
  a retry can change the type of a column, and the engine re-ran the whole
  lookup for every parent key on the edge to be refused identically each time.
  The refusal is now recognized, reported once as a permanent property of the
  table, and the lookup is attempted once per child table instead of once per
  parent key. The binlog half of the recovery is untouched: only the baseline
  join is blocked, so the children with an event inside the lookback window
  are still reconstructed, and the ones untouched within that window are not.
  The new caveat says exactly that, and it now says it for both kinds of root
  event rather than assuming a deleted parent.
- **A backup job that hits an internal error no longer stops capture**
  (#1472). Under `bintrail-console watch` one process is both the console and
  the capture plane. The four background baseline jobs (the scheduled refresh,
  the Create-baseline button, the point-in-time restore, the custom `.sql`
  build) each ran in a goroutine with no crash guard, so an internal error in
  any of them killed the daemon and stopped replication capture. The docs
  promise the opposite: a backup that stopped refreshing is a degradation, a
  daemon that stopped capturing is an outage. Two layers now contain it. An
  internal error while rebuilding one table is recorded against that table, the
  other tables carry on, and the run fails without publishing, exactly as it
  would for any other unusable table. An internal error anywhere else in the
  job marks the run failed on the Backups page. Either way the daemon keeps
  capturing and the stack goes to the daemon log at error level, so the cause
  is still there to read. It is contained, not hidden. A job that already
  published its snapshot before the error is left reported as succeeded,
  because the snapshot is on disk. The four jobs share one lock per server, so
  the failed state also frees that lock, where a crashed goroutine used to
  leave the server stuck as busy and refuse every later backup job for it.
  Two things to know: a run whose error lands outside the table rebuild leaves
  no row in the Backups page's run history, so read its status card and the
  daemon log for it; and the manual Create-baseline button converts mydumper's
  output table by table, where an internal error still stops the daemon.
  Everything the CLI did before is unchanged apart from where the stack is
  printed: `bintrail reconstruct` and `baseline refresh` still exit non-zero,
  and now log the stack instead of dying on it.
- **A slow index no longer ends capture** (#1482). `bintrail-console watch`
  supervised the sources added from its console, restarting them with backoff,
  but ran its own `--source-dsn` source as a single call: a batch INSERT that
  ran past `--write-timeout` returned an error the daemon exited on, and
  nothing restarted it. Read pressure on the index was enough to trigger it,
  and capture then stayed down until an operator noticed, with the exposure
  bounded only by how long the source keeps its binlogs. That exit also ran the
  supervisor's shutdown, so the main source's failure stopped every supervised
  source alongside it, and those recorded `stopped` rather than `failed`,
  leaving nothing to show they had died of another source's error. The write
  deadline is now its own failure class and the main source restarts on it,
  under the policy the supervised sources already used: 15s doubling to a 5m
  cap, reset by a run that streamed for more than 10 minutes, and a permanent
  stop after 6h of continuous crash-looping that returns the original error
  with its original advice. Every other error still ends the run on the first
  failure, unchanged. This restarts the stream and deliberately does NOT retry
  the INSERT in place, a distinction that must survive any later
  simplification: the deadline is enforced by the client, which cancels by
  closing the socket without a `KILL QUERY`, so a batch that was merely slow
  usually goes on to commit on the server after the client stopped waiting, and
  re-running that statement would write every row a second time, permanently,
  into a table with no natural key to deduplicate on. A restart instead replays
  from the last checkpoint through the dedup-on-resume step, which exists to
  drop exactly those rows. Know three limits. The breaker bounds a fast crash
  loop, not a slow flap: a source that fails only after runs longer than the
  healthy-reset window restarts indefinitely, which is long-standing behaviour
  and is what you want, since capture is up for all but the backoff of such a
  cycle. The new failure class covers the batch INSERT only, so a deadline on
  the checkpoint or the gap record still ends the daemon as before. And the
  daemon no longer exiting means its Prometheus target no longer disappears, so
  an `up == 0` alert will not fire and the stream gauges freeze at their last
  healthy value rather than rising: alert on staleness of
  `bintrail_stream_last_flush_timestamp_seconds` instead of on a lag gauge, and
  note that `watch` exposes no metrics at all unless `--metrics-addr` is set.
- **A connection attempt can no longer block forever** (#1482). `Connect` and
  `ConnectWithTLS` verified a new connection with an unbounded ping, and the
  `timeout=` DSN parameter is the dial timeout only, with no read or write
  timeout set anywhere. A server that accepted the TCP connection and then
  never completed the MySQL handshake, which is what a frozen VM or an
  idle-dropped load balancer looks like, left the caller blocked with no way
  out. Both paths now spend the DSN's own connect budget on the handshake as
  well, so such a peer surfaces as a ping error. The bound covers the connect
  phase only and never becomes a read deadline on an established connection, so
  long-running statements are unaffected; raise `timeout=` in the DSN if a
  server legitimately needs longer to complete a handshake.
- **A supervised source no longer gives up after one failure that follows a
  long healthy run** (#1482). The crash-loop circuit breaker measured the
  failed run's own uptime instead of how long the source had been failing, so a
  source that had streamed for longer than the 6h give-up threshold, which is
  the normal state of a healthy daemon, was marked permanently `failed` on its
  first failure without a single retry, reporting a crash loop that had never
  happened. It now measures from the first failure. The retry delay also
  overflowed after 30 consecutive failures and became no delay at all, hammering
  a failing source roughly 2h13m into what should have been a 6h budget; it now
  stops growing once it reaches the 5m cap.
- **The console daemon keeps its baseline run history, and says when it cannot
  find a home directory** (#1487). A daemon started without `HOME`, which is
  how a service manager starts one, resolved its server registry, auth file,
  MCP token, verify history and baseline run history through a fallback
  written as `filepath.Join(".", ".config", ...)`. That reads as though it
  anchors to the current directory and does not: `Join` runs `Clean`, which
  deletes the leading `"."`, so the result was a bare relative path resolved
  against the process working directory at each IO. The path now names the
  working directory outright. Nothing moves as a result, since no code path
  changes directory between resolving a path and writing it; what changes is
  that a failure names a directory an operator can go and look at, instead of
  one that depended on when the syscall happened. Separately, and this is what
  actually lost the history: `BaselineRunHistory.save` was the only one of five
  sibling atomic savers that did not create its directory first, so every
  refresh failed with `ENOENT` whenever `~/.config/bintrail` did not exist yet.
  That is the normal state of a fresh install with a perfectly good `HOME`,
  since nothing creates the tree until something saves into it. Because that
  repeated failure was in practice the only signal anywhere that `HOME` was
  unset, fixing it would have removed the signal and left the cause, so a
  daemon with no home that falls back to a default path now warns once, naming
  the directory it anchored to and the settings that override it. It is a warning and not a refusal:
  losing run history must never stop a refresh, and the console is a recovery
  path that does not decline to boot over where its own state file landed. The
  same relative fallback was shared verbatim by `generate-key`'s default key
  path and is fixed there too.
- **The console daemon's background folds are bounded, and their volume warning
  works** (#1477). `bintrail-console watch` rebuilds snapshots in the same
  process that captures binlogs, for the scheduled baseline refresh, the
  point-in-time restore, and the SQL export build. All three left
  `FullTableConfig.WarnEventThreshold` at zero, and zero DISABLES that warning
  (`threshold > 0 && n > threshold`), so the one signal that a fold was about
  to exhaust RAM could never fire. Two of the three also left `Parallelism` at
  zero, which means `runtime.NumCPU()`: peak memory is the sum of the tables
  folding at once, so the daemon's memory tracked the core count of whatever
  host it landed on. Both are now fixed at shared constants (2 tables, the
  5,000,000 the CLI ships), and the warning's advice no longer names `--at`,
  `--parallelism` or `--warn-event-threshold`, which this binary does not
  register. Refreshes on hosts with more than 2 cores will take longer; the
  daemon already reports and skips a cycle that overruns its interval.
- **A console-generated `views.sql` pins the S3 region when it is known**
  (#1462). The console filled no region at all, while archive reads pin one
  detected from the bucket, so a downloaded file described a different read
  than the one this process performs: a store that checks the signing region
  rejected what the recipient sent. The SQL panel's own DuckDB session gets the
  same treatment, and is resolved separately because the download names
  archives portably while the panel is local-first. Only a region actually
  **detected** is pinned: `s3:GetBucketLocation` is deliberately outside
  bintrail's documented minimal IAM policy, so falling back to the daemon's
  ambient region is the common case, and writing that guess into a file would
  override a correct configuration on a machine bintrail cannot see, where
  pinning nothing lets the reader's own credential chain resolve it. Detection
  is memoized per bucket, so it stays off the SQL panel's per-query path;
  failures expire, since a blip is not a property of a bucket. When two buckets
  are each detected in a different region, nothing is pinned (one secret cannot
  name two) and the file says so.
- **The generated DuckDB views say whose server the live leg reads, and that a
  baseline refresh never reaches a file already generated** (#1483, #1484). Two
  things the file left the reader to find out by measuring. First, `bintrail
  views --include-live` adds a leg that opens a connection to the index and
  scans `binlog_events`, while the other leg reads Parquet off disk or S3. The
  file presented those as two halves of one view and neither the flag help nor
  the generated header said the hot half is the index capture is writing to, so
  a large analytical query contends with capture for the same disk and buffer
  pool. In one measured run a query over a 15 million row live table was
  followed minutes later by capture stopping on that server. The flag help and
  a block next to the generated `ATTACH` now say it, along with what it costs
  today: an index write that runs past its timeout ends the run in a standalone
  capture process (`bintrail stream`, `bintrail up`, `bintrail-pg stream`),
  which then stays down until something restarts it, while `bintrail-console
  watch` restarts from its last checkpoint instead. Capture is behind either
  way. The way out is in the same block, and it is deliberately not "filter the
  view", which that same block has just said does not reach the index: query the
  attached `binlog_events` directly with your own `WHERE`, or point `HOST` at a
  read replica, at the cost of that replica's own lag on top of capture lag.
  Second, each
  `state_<schema>_<table>` view is written with one snapshot path, resolved
  when `bintrail views` ran. The header already said to regenerate after taking
  or refreshing a baseline, which covers someone who takes them by hand and
  does not cover a daemon running `bintrail-console watch
  --baseline-refresh-interval`, where the snapshot is published on a timer and
  nobody regenerates anything: seven snapshots
  in forty minutes were ignored that way, with no error and no warning and
  numbers that simply stopped changing. The header and `docs/dump-and-baseline.md`
  now name that case and say to regenerate on the same schedule the refresh
  runs on. The pinning itself is unchanged and deliberate, since a fixed
  snapshot is what reproducible analysis wants, and the file already names the
  snapshot it is bound to in its header and in every view's path, so which one
  you have is readable without running anything. Making the views resolve the
  newest snapshot themselves was considered and rejected: the decimal casts are
  built from each file's Parquet footer read at generation time, so a view that
  silently moved to a newer file would cast a changed precision wrong or fail
  to bind on a dropped column; snapshot completeness is a `_SUCCESS` marker
  rather than something a path expresses, and snapshots taken before that
  marker existed are complete without one; and per-view resolution would let a
  join between two state views land on two different snapshots when a refresh
  publishes mid-query, which is a state that never existed. Text only: no
  generated view's behaviour changed, and files you already have keep working.
- **A scheduled backup refresh that refuses no longer leaves a near-complete
  snapshot on disk** (#1473). A refresh folds every table it can before it
  reports a refusal, and each table that folds writes its Parquet into the new
  snapshot directory as it finishes; only the marker written at the end says the
  result is unusable. On a server where one table carries a permanent capture
  gap, every cycle therefore left behind a fresh directory holding all the other
  tables and a marker saying it cannot be used. Backup discovery correctly
  ignores such a directory, and retention cannot reclaim it either, because a
  prune needs a confirmed S3 copy and this loop never uploads. At the old one
  hour interval floor that was 24 directories a day; the new one minute floor
  makes it 1440. The scheduled refresh now removes the directory its own failed
  run created, and the failure line names that directory either way. Three
  guards decide, and all three have to agree before anything is removed: the
  directory has to have held nothing but a previous failed run's marker when
  this refresh started, so everything in it now is this run's; at least one
  table has to have refused, so a run that failed only at the last step with
  every table already folded is kept rather than deleted; and the directory has
  to still carry the incomplete marker, so a published backup is never a
  candidate. When any of them says no, the directory is kept and the line says
  which one it is and why. The second guard stands down when the directory holds
  nothing but the marker, which is what a run that fails before its first table
  rebuilds leaves behind: an unreachable index, a missing schema snapshot, a
  refused archive lookup. There is no data in those to protect, and they used to
  accumulate one empty directory per interval, each one printing a skipped
  incomplete snapshot warning on every later listing. The removal moves the
  directory aside first and deletes it second, so a delete interrupted part way
  can never leave table files behind with no marker on them, which is the one
  shape a reader would take for a real backup; a daemon killed during that
  delete leaves the moved-aside directory behind, and the next refresh cycle for
  that same backup directory clears it, the same way the retention prune clears
  its own staging directories. One left in a directory the refresh no longer
  runs for (the server was removed, or the interval was turned off) stays until
  you remove it. Nothing changes for `bintrail baseline refresh` on the command
  line, or for a restore or a custom `.sql` build you started yourself: those
  keep their fragments, because someone who typed a command is there to look at
  them. A shutdown partway through a refresh usually reclaims: a table already
  rebuilding reports the cancellation as an ordinary table failure, so only a
  shutdown observed while no table is in flight keeps its fragment.

- **`views.sql` names archives for another machine, and says how to keep S3 working**
  (#1456). The console download and `bintrail views` named an archive by its
  local path whenever a local copy with data existed on the generating host,
  which is the normal shape after a rotation that archives locally and uploads
  to S3; on the operator's own machine that path does not exist and every
  `events` query failed. Both now name such an archive by its S3 location
  (`query.PortableArchiveSources`); a local path appears only when the
  registry holds no S3 location the file can use. The console's own reads,
  including the SQL panel, keep the local-first routing. In the console
  download, a registry that cannot be read is now named as such in the file's
  header (or answers 502 when there is no backup half to serve) instead of
  reading as "nothing archived yet"; `bintrail views` fails the command as
  before. The SQL panel still serves the `state_*` views when `archive_state`
  cannot be read, and says so: a `warnings` entry on success, and the same
  note after the engine's error on a failed statement. The file's S3 preamble
  now states that the credential-chain secret lives only in the session that
  runs the file (views persist in a database file, secrets do not) and why it
  must not be made
  `PERSISTENT`: DuckDB would resolve the chain at creation and write the
  resulting keys to disk. The CLI example, the docs and the console toast that
  piped the file into `duckdb lake.db` now show `duckdb -init views.sql lake.db`
  and `.read views.sql`.

## [0.69.0] - 2026-08-23

### Added
- **Session policies can carry direct table and column restrictions**
  (#1449). An installed auth provider can now attach data scoping to the
  console session it mints: tables withheld, column values blanked, or an
  allow-list where only the named tables (and, within them, the named
  columns) are visible. Deny wins when both name the same data. The console
  enforces these in the same pass as session data profiles: events and
  recover results are filtered and redacted, the schema listing shrinks to
  what the session may see, and the surfaces that cannot redact per column
  (time-travel, baseline listings, recover-cascade, verify, extension
  views) answer forbidden, exactly as they do for profiled sessions.
  Sessions with restrictions query the live index only, since archive reads
  bypass the redaction pass; the response says so. The OSS build's own
  logins are unchanged: nothing in the core constructs these policies.

### Security
- **A session carrying a data access policy can no longer mint a managed
  MCP token** (#1449). The token would have outlived the session's
  restrictions and authenticated unrestricted. Tokens minted by such
  sessions BEFORE this release still work unrestricted: revoke them and
  mint replacements from an unrestricted session.
- **The changed-column filter is refused for sessions under column-level
  rules** (#1449). Filtering by a hidden column's changes would reveal that
  the column changed, which is the fact the redaction hides.

## [0.68.0] - 2026-08-22

### Fixed
- **The Claude Desktop bridge crashed when connecting through a
  response-buffering reverse proxy** (#1433). The console announces its tool
  list moments after a session is born, and that notification could arrive
  while the bridge was still finishing its own connection, dereferencing a
  session that did not exist yet. A buffering proxy (nginx without
  `proxy_buffering off`) shifted timing into the crash window every time.
  The bridge now treats the early notification as a no-op and syncs the tool
  list right after connecting, so nothing is lost.
- **A trailing slash on the console's MCP address answered the web page
  instead of the MCP endpoint** (#1433). `/mcp/` and `/mcp/<server>/` now
  route to the MCP handler exactly like their slash-less forms. The bridge
  also prints a plain-words next step when an address lands on a web page or
  a credential is refused, and the reverse-proxy recipe in docs/console.md
  names the symptom.

## [0.67.0] - 2026-08-22

### Changed
- **Connect AI is now three short steps with a drawn install dialog** (#1432).
  The page said everything and showed nothing; live feedback was that it read
  as a wall of text. Step numbers are badges, each card is one action plus one
  short line, a miniature of Claude Desktop's install dialog shows the two
  fields with chips pointing back at steps 1 and 2, and every contingency
  (Intel Mac, Windows, claude.ai in the browser, fixed startup tokens,
  several servers) folds into fine print. Visible text drops from ~2300 to
  under 1000 characters, and the e2e suite now enforces the budget, reads the
  bundle manifest's field names at run time so the drawing can never drift
  from the real dialog, and pins that no arm ever renders a download link
  that can only answer Not Found.

### Fixed
- The console e2e's Events-skeleton failure reporter crashed while reporting
  a broken fixture, taking the rest of the suite with it; and the severity
  split scenario could sample the transient scope=live warning that
  progressive Events paint before the final read lands (#1432).

## [0.66.0] - 2026-08-22

### Added
- **Build a .sql backup for any moment, from the Backups page** (#1428). Pick
  a past instant; the watch daemon folds the newest backup at-or-before it
  forward through the recorded changes and hands out one tar.gz of plain SQL
  files (mydumper format) that `myloader` restores with nothing from
  bintrail on the receiving side. Every build writes into its own staging
  directory (a rebuild can never interleave two instants into one archive),
  the download is gated on the build's completeness marker read together
  with its status in one locked snapshot, and the handler re-checks the
  marker after the last byte so a build replaced mid-stream aborts loudly
  instead of ending as an apparently whole archive. Fail-closed like every
  fold: a stamped capture gap or a table whose backup vanished mid-build
  fails the run; nothing incomplete is ever downloadable. Downloads sit on
  the `query:execute` tier and are audited (aborted streams included);
  profile-scoped sessions are refused on trigger and download. Staging is
  swept at daemon startup, so no plaintext dump outlives the process that
  can serve it.

### Changed
- **The console wears the dbtrail.com sunset** (#1429). The sidebar goes to a
  light tropical-morning ground (tinted, never white) with the site's sunset
  as its accents: the active page is a pink-to-orange pill, section labels
  are deep gold, and the wordmark keeps its headline gradient. The content
  canvas warms up: a blush ground, a sunset haze at the top of each page
  (anchored to the page, so scrolled list headers never ride it), page
  titles in the headline gradient, and config cards rotating the home's four
  tints with their own borders and ribbons. Every measured contrast floor
  holds and is recorded next to the values; data surfaces (events, diff,
  the INSERT/UPDATE/DELETE code) keep their exact colors.
- **Connect AI reads as literal steps for non-technical users** (#1430).
  Three cards become Step 1 (Access token), Step 2 (Console address) and
  Step 3 (Add it to Claude, a real ordered list quoting Claude Desktop's own
  field names). Rotate/revoke became New token / Delete token everywhere,
  the Bearer-header detail moved into the technical accordion, and the
  bundle card stopped promising files that do not exist: there is no
  Windows installer, so Windows browsers are routed to the claude.ai custom
  connector, and Intel Macs are pointed at the darwin-amd64 bundle instead
  of the arm64 one. docs/connect-ai.md now numbers its steps the same way
  the page does.

## [0.65.0] - 2026-08-22

### Added
- **The console's Baselines page is now Backups, and acts like it** (#1427).
  Each snapshot row expands to its tables with sizes, the total weight, and
  how long the backup took: exact for runs this daemon performed (a new
  console-local run history records dumps, refreshes and restores), bounded
  by file timestamps otherwise. Every backup downloads as one tar.gz stream
  (local and S3 stores; markers and the integrity manifest included, so what
  lands on disk is a complete discoverable snapshot; `_INCOMPLETE` snapshots
  are refused; a mid-stream failure cuts the connection so a truncated
  archive can never look like a success; audited as
  `console/baseline.download`, aborted streams included). A point-in-time
  restore folds the backup at-or-before a chosen instant forward through the
  index and publishes the result as a NEW discoverable snapshot named by
  that instant — the periodic refresh's engine with an operator-chosen
  anchor, same single-flight and fail-closed refusals; it needs the server's
  own local backup directory and is gated by the new `baseline_restore`
  capability. A running dump, restore or refresh paints a live in-progress
  region and the page settles itself when the run finishes. The rename is
  UI-only: routes, APIs, CLI commands, flags and env names keep "baseline".
  S3 listings now carry sizes and timestamps (`storage.InfoLister`), and the
  four new routes are classified in the console's route-permission table:
  the download requires `query:execute`, because it is a full unredacted
  copy of every baseline row.

### Changed
- **Every user-visible sentence says it in plain words** (#1426). The verify
  verdicts stopped talking about "event budgets" and "chains": a truncated
  recover-inputs run says what was read and what each remedy buys, CLI flags
  never reach console text without an explicit "(CLI: ...)" marker, and
  cause-neutral wording replaced claims the walk cannot know. Roughly 200
  em-dash strings across the console, CLI errors and help, MCP tool output
  and generated-file headers were rewritten to plain punctuation, and docs
  that transcribe CLI output were re-synced. The verification page's counts
  column sizes to its content instead of painting over the reason column.

## [0.64.0] - 2026-08-22

### Added
- **The Events browser renders the live index first** (#1414). The page used
  to block on a single all-or-nothing fetch: rows already sitting in
  `binlog_events` — milliseconds away — waited behind an S3 archive scan that
  can take tens of seconds. `GET /api/events?scope=live` now serves the live
  index immediately and says exactly what it left unread (`scope: "live"`,
  `archives_pending`, and a PARTIAL warning louder than the elision note); the
  UI paints that phase at once, completes with the full merged read in the
  background, and replaces the list wholesale keyed on each event's `anchor`,
  so backfill layouts cannot reorder rows under the operator. A failed
  background read leaves the marker up and names the failure — and both browse
  endpoints now put skipped archive sources and discovery failures in the
  response warnings instead of only the daemon log, so the follow-up read can
  keep the phase-1 promise.
- **A fourth archive-elision proof, `windowSatisfiedLive`** (#1414): a `since`
  bound at or above contiguous live coverage makes every label-accurate
  archived row unable to survive the row-level time filter, whatever the page
  looks like — the sparse table reached from a live-retention widget could
  never fill a page, so no earlier short-circuit could help it. Misfiled
  archives (#1037) and `SincePos` veto the proof. The Overview's *Activity by
  table* click now carries its own window as a visible `since:` token, so the
  count an operator clicked and the search it opens ask the same question.
  The two reconstruct surfaces (console and MCP), where the proof made elision
  reachable on ASC fetches, grew the notes lists their guards had reserved.
- **Verification's inconclusive bucket is subdivided** (#1416). One bucket
  carried three meanings and rendered them identically, so a healthy run over
  a server full of log tables read as a page of warnings. `--check recover`
  now reports `inconclusive_kind` per table — `no-activity`, `nothing-to-assert`
  (append-only shapes where zero assertions is the expected outcome), or
  `unproven` — and every surface splits the summary: CLI text and JSON
  (`summary.inconclusive_nothing_to_check`), the console's cards (benign kinds
  render neutral, not amber), toast, history, and a verdict sentence in words.
  The attention side is the REMAINDER, so an unclassified inconclusive is
  never rounded toward benign, and the exit contract is unchanged.

### Changed
- **The console adopts the home page's surface language** (#1421): tinted
  bento cards (violet/sun structure tints with AA-measured text partners — the
  site's own deep values fail 4.5:1 and were darkened, not copied), pill
  eyebrows, and a dark code-block recipe. Body text, row dividers and the
  warning register were all measured on the tint grounds after review caught
  the first cut failing all three.
- **`/baselines` redesigned** (#1415): a full-width context strip — source
  path as one-line code, snapshot count, latest with its relative age beside
  the absolute time, tables-per-snapshot when uniform, and Create baseline as
  the page action — over a full-width list where the newest snapshot (the one
  a restore actually uses) wears the treatment and rows carry only what
  varies.
- **`/verification` redesigned** (#1417, #1418, #1419, #1420): three separated
  regions (run / current / history); per-mode help stating what each check
  proves, needs and costs; result rows as scannable columns sorted
  worst-first; a glossary for the walk's vocabulary; a running verification
  that finally looks like one (animated live chip, progress strip, counts
  framed "so far" so a partial tally cannot read as a verdict, a perceptible
  completion); and history rows that expand to their per-table detail — data
  that was already persisted and on the wire, dropped on the floor by the old
  renderer. `LAST VERIFIED` no longer shares its treatment with `RUNNING`,
  and the recover-inputs counters now actually reach the console's wire shape.

## [0.63.0] - 2026-08-21

### Fixed
- **Undo reverses the event you clicked, not the last one in its second**
  (#1411). The Undo bridge carried the clicked event to Restore as a **time**,
  not an identity: `until` is second-granular, and paired with #1404's
  `limit_per_pk = 1` that resolves to "the newest event at or before the end of
  that second". Inside one second that is not necessarily the event pointed at,
  and on one shape the outcome **inverted** — a row INSERTed and DELETEd in the
  same second yields two events sharing a timestamp, the cap keeps the DELETE,
  and reversing a DELETE re-INSERTs, so clicking Undo on the INSERT put the row
  **back** while the badge still read INSERT. #1404's banner disclosed this in
  words; this names the event instead. Events now carry an `anchor` — the
  server's own `<RFC3339Nano>|<event_id>` token, the same spelling the
  `?before=`/`?after=` cursors use — and `query.Options.EventAnchor` filters on
  it in both the live index and the Parquet archives. The per-row cap is no
  longer prefilled: two mechanisms narrowing one scope is how they drift apart.
  Emitted by the server rather than rebuilt client-side because the displayed
  timestamp carries no offset, so reconstructing an instant from it means
  guessing a location — and guessing wrong does not fail, it names a different
  row.
- An anchored request that matches nothing now **says so**. An empty reversal
  used to mean one thing (nothing happened in the window); an anchor adds
  several causes that render identically as a 200 with an empty script — a
  selection left over from an earlier target, a time range narrowed past the
  anchored instant, a table withheld by an access profile, an archive source
  that failed, or an anchor from a different server's index, since `event_id` is
  a per-index AUTO_INCREMENT. Both `/api/recover` and `/api/events` now name the
  event id and refuse the finding.
- The Restore banner's **Clear** button retires the single-event selection and
  keeps the target and the upper bound, which is what the banner beside it has
  always said it does. It previously re-rendered the route into a fresh empty
  form.

### Added
- An **archive short-circuit for anchored reads**, joining the two added in
  #1403 and cheaper than either: it needs no query plan, no archives-below-live
  premise and no boundary check. An anchor admits at most one event and
  `event_id` is the merge layer's own dedup key, so an archive can hold a copy
  of the event already in hand or nothing. An anchor whose event is **not** live
  falls through to the archives as before — the predicate accelerates the common
  case, it never decides membership.

## [0.62.0] - 2026-08-21

### Added
- **Undo reverses one change, not the row's whole history** (#1404). Arriving at
  Restore through an event's **Undo** prefilled `until` from the clicked event
  and never `since`, so the window ran from the beginning of time: undoing the
  third of five changes to a row put it back to before the **first**. The bridge
  now also prefills `limit_per_pk = 1`, so the script reverses the latest change
  at or before the ceiling. Prefilled rather than hardcoded — it lands in a
  visible, editable field and the banner states it, so widening the scope is one
  cleared field away and narrowing it was never a silent act. The ceiling is
  unchanged and still second-granular, and the banner now names the one outcome
  that inverts because of it: on a row INSERTed and DELETEd inside one second the
  cap keeps the DELETE, so undoing from either event **re-creates** the row.
  Identifying the clicked change by event rather than by second is #1411.
- **A row's state and history open in a dialog** (#1405). `Show state` /
  `Show history` rendered into a strip between the filter form and the reversal
  panel. The output is unbounded — a churny row's history is a long table — and
  it is consulted on the way to the script rather than being the script, so
  checking one pushed the artifact the page exists to produce off screen. Both
  now open a dialog, with the reconstruct warnings carried inside it (a
  `stale_baseline` caveat left on the page behind a scrim is worse than the old
  layout) and **Restore to this state** dismissing it before retargeting the
  form underneath.

### Fixed
- **A PK-scoped reversal no longer reads every archive** (#1403). `recover` for a
  single row with `limit_per_pk` set fetched from the live index **and then
  downloaded and scanned every registered Parquet source**, even when the live
  partitions already held that row's latest N events and nothing archived could
  survive the per-PK trim. Measured on a real index: 25s spent scanning archives
  back to a month before the event being reversed. `fetchPage` now skips the
  archive leg when every named PK already has its `limit_per_pk` rows live, and
  reports the elision so the operator is told the archives went unread rather
  than left to assume they were consulted.

  The skip is refused unless the planner can prove **archives are strictly
  older than live** — a new `QueryPlan.ArchivesBelowLive`, computed from
  partition hours the same way `PlanBrowse` has always computed it. Without that
  premise, an index whose archives sit at or above the live floor (a restored or
  hand-surgered index, or a rotate that archived without dropping) would let a
  PK with its newest live rows skip an archive holding **newer** ones — a short
  reversal script reported as complete. The same requirement is now applied to
  the pre-existing top-N short-circuit, which was blind to the same layout.
  Note the CLI's `recover --pk` sets a second PK spelling and is deliberately
  excluded from the skip; the console and MCP paths are not (MCP builds its own
  merge loop and does not reach this at all — #1410).

## [0.61.0] - 2026-08-21

### Added
- **Extension views get the console's own date picker** (#1406). The extension
  seam handed a view its mount, API base and fetch helper, but no widgets — so
  a panel wanting a date field either hand-rolled one or shipped a plain input.
  The contract now carries a `ui` namespace whose first member is
  `dateField`, the same builder the console's own Since/Until use, which means
  a calendar in an extension panel is one implementation rather than a second
  copy of the popover, keyboard rules and outside-click dismissal to keep in
  step on one page. Additive and defensive on both sides: a view built before
  `ui` existed ignores it, and a view that uses it is expected to fall back
  when an older host does not pass one.

## [0.60.0] - 2026-08-21

### Added
- **Protect is its own section of the console** (#1384). Settings > Storage
  carried three unrelated concerns: storage policy, backup lifecycle and
  recovery assurance. Two of those produce and validate recovery artifacts
  rather than configure anything, and the page had already been patched around
  the mismatch — with a snapshot list unbounded in practice, Verification, the
  panel that answers whether a restore would actually work, sat about two
  screens below the fold. Baselines and Verification are now routes under a
  **Protect** nav group beside Investigate / Resolve / Monitor; Storage keeps
  storage policy and a compact baseline summary card that links onward. The
  whole group is gated on watch-daemon capabilities, so a standalone `serve`
  collapses it rather than showing a heading with nothing under it.
- **Restore can isolate the latest N events per row** (#1387). The query engine
  has implemented `LimitPerPK` for a long time and the CLI exposes it; the
  console could not reach it from the form or the API. It is the only filter
  that separates events sharing a timestamp: `since`/`until` are
  second-granular, so a row INSERTed and DELETEd inside one second cannot be
  split by time, and reversing that window nets to no row when the operator
  wanted to undo the DELETE alone. Wired into `/api/events` as well as
  `/api/recover`, deliberately — the preview documents that it mirrors
  recover's effective window, and adding the filter to one side only would
  break that promise silently. Requires a PK, enforced in the request layer
  rather than the form, since the API is reachable without the UI.
- **Recover reports how long the reversal script took to generate** (#1386).
  `/api/recover` and `/api/recover-cascade` return `generated_in_ms`, rendered
  as *"2 statement(s) from 2 event(s) · generated in 0.4s"*. The clock covers
  the event fetch — including any archive/Parquet leg — plus SQL rendering,
  because that fetch is where a recover reaching S3 differs from one served
  from live partitions by orders of magnitude. Zero and absent are different
  answers and the field is not `omitempty`: a sub-millisecond recover is a real
  result, and a client must be able to tell it from an older server that does
  not report timing at all.
- **The console has a first motion pass** (#1385). Overview tiles rise as their
  fetch lands and lift on hover; nav icons nudge toward their destination. The
  sidebar wordmark and the large Overview counts now carry the site's headline
  gradient, and the Overview loading bars are warmed toward the hero pink.
  Every animation sits inside `prefers-reduced-motion: no-preference` with a
  usable resting state outside it. The gradient is opt-in per element rather
  than a rule over the count class: its sweep bottoms out at 3.53:1 on the tile
  it paints on, which clears WCAG's large-text bar and never the body bar, so
  it lands only on 19px/700 and 32px text and never on the semantic
  `--delete` tile.

### Changed
- **Reduced motion has one shape, and a guard that reads the whole file**
  (#1392). The stylesheet had accumulated three different ways of honouring
  `prefers-reduced-motion`, which is why an earlier audit scoped to one shape
  manufactured alarms against correct code. They are now one shape, and the
  guard covers the file rather than a section — it also caught five animations
  that no guard had been watching.

### Fixed
- **The Events loading skeleton was nearly invisible** (#1397). `.ev-skel-bar`
  filled with `--surface-3`, which renders **1.09:1** against the page —
  fainter than the 1.17:1 hairline separating the rows it stands in for. A
  loading list therefore rendered as an empty list with dividers, which is
  precisely the "blank list" the loading state exists to avoid, on the page an
  operator opens mid-incident. The pulse made it worse rather than better, and
  it is gated behind `prefers-reduced-motion`, so with motion reduced there was
  nothing moving to suggest the bar was there at all. It now renders at 1.64:1.
- **The Undo banner stated a target state the script does not produce**
  (#1388). It read *"Reverting this row to before this point"*, but the
  prefill fills only `until`, so the generated script reverses every event on
  that row in an unbounded window ending at the end of the clicked event's
  SECOND. Those coincide when the clicked event is the only one in range, which
  is why the wording survived — and they diverge exactly where it hurts: a row
  created and deleted inside one second nets to no row at all. The banner now
  names the scope it actually reverses and the control that narrows it.
- **The documented source user could not take a baseline.** Every place that
  spells out the source grants — `streaming.md`, `quickstart.md`, `install.md`,
  `console.md`, `docker.md`, `guide.md`, `mariadb.md`, `deployment.md`,
  `.env.example`, and the console's own "+ Add server" form — listed
  `REPLICATION SLAVE, REPLICATION CLIENT, SELECT` and nothing else. Since
  baselines became point-consistent by default in 0.58.0, that user is refused
  by **every** consistent lock mode, so an operator who followed the docs
  exactly hit a wall at the Create-baseline button. `streaming.md` additionally
  asserted dbtrail "does not need `RELOAD`, `LOCK TABLES`…", which stopped being
  true for the baseline path.

  The grant lists now separate the two: capture needs the original three and
  still never locks the source; a baseline needs `LOCK TABLES` on top, and says
  so where the list is read. Omitting it is a DELAYED failure — capture starts
  clean and only the snapshot is refused — which is why the console form now
  carries the line too. `streaming.md` gained a section covering both
  point-consistent modes and why RDS/Aurora must use `lock-all`.

## [0.59.0] - 2026-08-17

### Fixed
- **Point-consistent baselines were impossible on managed MySQL** (#1381).
  v0.58.0 made them the default; on RDS the button then failed for every
  operator, for two independent reasons.
  - The privilege preflight read `information_schema.USER_PRIVILEGES`, which
    exposes only the rows the connecting user can SEE in `mysql.user`. A
    managed master user cannot see its own: on RDS MySQL 8.4 that view returned
    a single `USAGE` row for an account whose `SHOW GRANTS` listed `RELOAD` and
    `FLUSH_TABLES` as direct grants. The check therefore failed **closed** on
    every managed source and reported "the current user has neither" about
    privileges the user held. It now parses `SHOW GRANTS FOR CURRENT_USER()`,
    which needs no privilege to run and reflects the session's active roles.
  - `ftwrl` genuinely cannot work on RDS: `BACKUP_ADMIN` is refused outright
    (*"ERROR 1227 ... you need the RDSADMIN USER privilege"*) and mydumper's
    FTWRL path issues `LOCK INSTANCE FOR BACKUP` first. So **`lock-all` is
    new**: bintrail's fourth lock mode, mapping to mydumper's `LOCK_ALL`, which
    is equally point-consistent and synchronizes workers by locking the exported
    tables instead of the instance. mydumper names it for this itself — *"We
    support LOCK_ALL and SAFE_NO_LOCK modes for RDS/Aurora"* — and it needs only
    `LOCK TABLES`, at any scope covering the dumped tables, which the RDS master
    user already has. Verified to succeed against the exact privilege set on
    which `ftwrl` fails. Available as `--lock-mode lock-all`,
    `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` and `BASELINE_LOCK_MODE`.
    Note this changes which privileges are required, not which tables can be
    dumped: like `ftwrl`, it refuses a source with non-transactional tables.
  - Privilege requirements are now evaluated PER MODE rather than assuming
    every point-consistent mode needs the same grants, and a refusal names
    `lock-all` **before** the weaker modes: on a source that cannot do `ftwrl`
    it is the only alternative that is still point-consistent. `lock-all`
    accepts `LOCK TABLES` granted on the dumped schema rather than demanding it
    globally — `LOCK_ALL` locks the exported tables, and a dump runs to
    completion with only `GRANT SELECT, LOCK TABLES, SHOW VIEW ON <schema>.*`.
  - **Partial revokes are no longer read as a grant.** MySQL 8.0.16+ renders
    `REVOKE LOCK TABLES ON \`db\`.*` as its own `SHOW GRANTS` line; reading only
    the `GRANT` lines reported a privilege the user does not have, and mydumper
    would then launch and die partway leaving a partial dump. A revoke that
    provably covers a dumped schema is now a refusal naming it, one that
    provably does not is ignored, and an undecidable grant pattern is a
    warning — never a refusal, which would be the same over-refusal as above.

### Changed
- **Console failures no longer disappear on their own.** Failure notices were
  toasts that auto-hid after 2.2 seconds. The baseline privilege refusal is
  ~550 characters — which privilege to grant, the exact `GRANT`, and the
  alternative modes — so at that duration it was not merely easy to miss, it
  was unreadable, and there was no way to recover the reason afterwards.
  Failures now stay until dismissed with the close button or ESC, and scroll
  if long; success and progress notices still fade. Failures render in their own
  element, so neither channel can suppress the other: a success notice cannot
  replace an unread failure, and an undismissed failure cannot swallow a warning
  that is reported nowhere else (an interrupted MCP-token display, a schema
  snapshot that capture did not restart onto, an export hitting its row cap).
  Concurrent failures stack, and a repeat of a message already on screen is
  counted rather than dropped — several of them carry no server name, so a
  second failure would otherwise change nothing on screen.

## [0.58.0] - 2026-08-16

### Changed
- **BREAKING (operators without `RELOAD`, or with any non-transactional table)
  — baseline dumps are point-consistent by default** (#1377). `bintrail dump` and the console's **Create baseline**
  both passed mydumper `--sync-thread-lock-mode NO_LOCK`, which is the mode
  mydumper's own documentation describes as the choice for when *"you don't
  need a consistent backup"* — and which it **deprecated in 0.18.1**. Under it
  each worker thread opens its own snapshot with no barrier between them, so
  the dumped data could postdate the binlog coordinate recorded beside it. A
  baseline is the seed state `reconstruct` merges deltas onto, so that skew
  propagates silently into every reconstruct, verify and drill answer. It also
  produced real, correct-but-baffling `verify` mismatches: rows present in the
  newer baseline that replaying the change log to its anchor could not
  reproduce, because they were written during the dump.
  - The new `--lock-mode` (CLI) and `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE`
    (console) select `ftwrl` (the new default, point-consistent),
    `safe-no-lock` (no elevated privilege; **aborts** rather than emit a torn
    snapshot, so expect it to refuse on a write-active source) or `no-lock`
    (accepts a torn snapshot). The weaker modes must now be asked for by name.
  - `ftwrl` needs `RELOAD`/`FLUSH_TABLES`, plus `BACKUP_ADMIN` on MySQL/Percona
    8.0+. A source that lacks them now **refuses with an actionable error**
    naming the alternatives; neither surface downgrades silently. Deployments
    running as a least-privilege replication user must either grant those
    privileges or pass `--lock-mode safe-no-lock` / set the console variable.
  - **A second break, independent of privileges**: `FTWRL` with `--trx-tables`
    makes mydumper refuse a dump outright when it finds a non-transactional
    (MyISAM) table — *"Non transactional table found ... Restart backup using
    --trx-tables=0"* — where `NO_LOCK` only warned and proceeded. A source with
    one legacy MyISAM table now fails even with every privilege granted. The
    refusal is mydumper's, gated to an actual "consistent backup attempt".
  - The stack's own documented source user (`REPLICATION SLAVE, REPLICATION
    CLIENT, SELECT` in `.env.example` and the quickstart) does **not** satisfy
    the new default, and `BASELINE_TRIGGER` defaults on — so the documented
    happy path needs either the extra grants or an explicit weaker mode. Both
    documents now say so.
  - The compose `baseline` profile follows the same default and gains a working
    passthrough (#1378): the pre-existing opt-in was unreachable from the
    shipped stack, and the first attempt at this fix declared a variable the
    service's `environment:` block never passed into the container — so it was
    read as unset and the operator's choice was silently ignored. Both compose
    surfaces now take the same `BASELINE_LOCK_MODE` in the same vocabulary, and
    an unrecognized value fails the run loudly instead of falling through.
  - An invalid `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE` disables **baselines**, not
    the daemon: under `watch` this process is also the capture plane, so
    refusing to boot over a baseline setting would turn a typo into permanently
    lost events. The error surfaces from every baseline trigger instead.

## [0.57.1] - 2026-08-16

### Fixed
- **The console's verify "Explain" button no longer looks dead** (#1375). The
  row-level drill-down RE-RECONSTRUCTS the table in order to diff it — minutes
  of DuckDB work on a large one — and it was answered synchronously with no
  progress indication of any kind. The console itself tolerates a long
  response (it sets no `WriteTimeout`, and clears the read deadline for the
  handlers that legitimately run long), but a fronting reverse proxy at its
  stock read timeout — 60s on nginx — does not. Behind one, the only possible
  outcome was a request that died in the proxy and a toast that faded: a
  button that appeared to do nothing at all. The work now runs on the daemon
  like a verify run does. The request returns immediately with
  `202 {"state":"running"}`, the console polls behind a busy dialog that says
  what it is doing and can be cancelled, and a failure is rendered *in* that
  dialog rather than as a toast.
  - Closing the dialog only stops the waiting — the drill-down keeps
    computing, and reopening **Explain** picks up the finished result if it is
    still held. It is not held indefinitely: the first read consumes it, and a
    new verify run discards *and cancels* the previous run's drill-downs,
    since each explains the snapshot pair its own run compared. Only a
    baseline-anchored run can produce a replacement, so after a live-source or
    check-recovery-inputs run Explain stays unavailable until the next
    baseline-anchored one reports the table as a mismatch again.
  - A drill-down failure is now logged by the daemon whether or not anyone is
    still polling. Previously the HTTP response was the only delivery path for
    that error, so a reconstruction that failed after the operator closed the
    dialog left no trace anywhere.

## [0.57.0] - 2026-08-15

### Added
- **A console sign-in now carries to new tabs** (#1370). The session lived only
  in `sessionStorage`, which browsers scope per tab, so opening any console
  link in a new tab — middle-click, "open in new tab", pasting a URL — landed
  on the login panel while the original tab stayed signed in. It read as
  "the console logged me out"; it never had. Logging in now also sets an
  `HttpOnly; Secure; SameSite=Lax` session cookie whose value IS the session
  token, so a fresh tab authenticates against the same store, same expiry and
  same policy. Signing out revokes the session server-side and clears the
  cookie, so it ends every tab at once, not just the one that clicked.
  - **The `Authorization: Bearer` path is untouched**: `--token`, the
    `?token=` bootstrap, `/mcp`, and every scripted client behave exactly as
    before. When a Bearer header is present it is judged alone and never
    falls back to the cookie.
  - Cookies bring CSRF exposure that a header-only scheme does not have, so
    a request authenticated *by cookie* must carry `Content-Type:
    application/json` to change state — a marker no cross-site HTML form can
    send. Bearer-authenticated requests are exempt. The belt runs after the
    credential check, so an invalid cookie is still a plain 401 and the
    actionable 403 only ever reaches someone genuinely signed in.

### Changed
- **BREAKING (embedders only) — `ext.ConsoleSessionIssuer` takes the
  `http.ResponseWriter`** (#1370): `func(w http.ResponseWriter, identity
  string, policy *AccessPolicy) (token string, expiresAt time.Time, err
  error)`. The issuer sets the session cookie at the moment it mints the
  session, which requires the in-flight response. There is no in-repo
  implementor, so the break is absorbed in one release by the external
  authentication provider that plugs into the console login surface; a
  compile-time pin in `ext/consoleauth_test.go` keeps the shape from drifting
  silently. Passing `w` through only to satisfy the type — without letting
  the issuer set the cookie — compiles and leaves SSO logins tab-scoped,
  which is the behavior this change exists to fix.
- **The console wears the site's sunset in its brand moments** (#1371).
  0.55.0 adopted dbtrail.com's ink ramp, typefaces and product accents but
  kept brand color to two micro-moments, so the console read as a different
  product from the site that introduces it. The sign-in gate is now a
  full-screen hero canvas with the panel floating on it, empty states carry a
  faint sunrise wash and a sunset hairline, the Restore progress dots animate
  on the pink→orange arm, and the active nav rail extends the accent gradient
  into the sunset. **No data surface changed**: event tables, badges, diffs
  and the INSERT/UPDATE/DELETE semantic colors are untouched, and brand color
  never encodes data. Every new text-over-brand pairing was measured against
  WCAG AA (21 of 21 pass), which is why the canvas stops sit slightly darker
  than the site's — at site lightness the white panel would land at 2.05:1.

### Fixed
- **The Restore filter row no longer stair-steps** (#1369). The Table
  combobox added in 0.56.0 carries a hint line below the input, and because
  the filter row aligns fields by their bottom edge, the space that hint
  reserved — even while empty — pushed the Table label and input above every
  other field. The hint is now positioned out of flow, so the field's box
  matches its siblings whether the hint is empty, loading or showing a note,
  and a hint appearing mid-load no longer shifts the row.

## [0.56.0] - 2026-08-15

### Added
- **Every long-running daemon now emits the daily `daemon_beacon`** (#1362).
  The beacon machinery existed and was documented, but `bintrail-console
  serve`, `bintrail shim`, and `bintrail-pg flashback` never emitted it — a
  fleet of read-only consoles and time-travel shims read as zero installed
  base. All eight daemon entry points now run the beacon loop, each pinned by
  a wiring test that delivers a real beacon end-to-end and fails if the loop
  is ever unwired. The payload, consent gates, and TELEMETRY.md claims are
  unchanged — the documented event finally fires everywhere.
  - Fixed alongside, because wiring serve exposed it: serve's telemetry
    opt-out endpoint could report `reporting: false` while the new beacon
    loop kept delivering. The console's opt-out now flips the live client in
    `serve` exactly as it does in `watch`.
- **The Restore form got both hands** (#1363, #1364). Generate undo SQL and
  Preview rows open a progress dialog the instant they're clicked — it names
  what's being generated, blocks double-submits, and Cancel/Escape genuinely
  abort the in-flight request. The Table field offers the selected schema's
  tables as suggestions (a typed name still always submits — recovering a
  dropped table stays possible), and a failed Generate no longer leaves the
  previous run's script on screen with a live Download button.

### Changed
- **The archive-elision record is an info note, not a warning** (#1365).
  Skipping archives that could not change a page (#1353) was rendered through
  the same amber ⚠ banner as real coverage gaps, so a benign audit fact read
  as an incident. Responses now split advisories: `warnings` keeps cautionary
  facts (gaps, session exclusions, divergence findings), a new additive
  `notes` list carries benign audit facts, and the console renders notes in a
  muted register with plain-words copy. The fact itself still travels on
  every response — auditability was the requirement; the alarm was the bug.

## [0.55.1] - 2026-08-15

### Fixed
- **The vendored console fonts now ship with their SIL Open Font License
  obligations met** (#1360). v0.55.0 embedded three OFL-1.1 typeface families
  but carried neither the license text nor the copyright notices: the
  THIRD-PARTY-NOTICES generator only sees Go modules, and the subsetting
  pipeline had stripped the fonts' own license name records. The notices file
  that travels in every artifact now includes the full OFL-1.1 text with the
  three families' copyright lines, the embedded woff2 files carry their
  license metadata again (name IDs 13/14), and two CI guards pin both — a
  notices regeneration or a font swap that loses either fails the build.

## [0.55.0] - 2026-08-15

### Changed
- **The console Overview paints in seconds on archive-heavy sources** (#1352).
  The activity aggregate is now materialized per server — computed once,
  served from a cache with a ~30-minute refresh, and every tile disclosing
  its own `as of …` freshness stamp instead of re-scanning the index on every
  page load. The Overview now reads exclusively the live index, and its
  window IS the live retention (derived from the oldest live partition; the
  fixed 1h/6h/24h picker is gone). Window == live coverage by construction,
  so archived-only hours can no longer fall inside it and the per-request
  archive completeness pass is deleted with the ambiguity it guarded.
- **The default Events browse answers from the live index when archives
  cannot change it** (#1353). Archives only ever hold partitions older than
  the rotation boundary, so a newest-first page that the live index fills
  completely cannot gain anything from opening them — but the default browse
  (no time range) never had that proof and opened every archive source per
  request. On an archive-heavy fixture this took the default browse from
  93ms (local archives) / 259ms (S3) to ~8ms with zero S3 reads. The skip is
  auditable: the response `warnings` say when archives were elided and why,
  and any query whose window reaches archived hours still reads them. The
  Events view shows skeleton rows while loading — never a blank list.
- **Every time the console renders now states its timezone** (#1354). The
  "as of" clock was the browser's local time, unlabeled, directly above data
  timestamps that are all UTC — the same instant read hours apart on one
  page. Everything rendered is now declared UTC (section-level chips,
  labeled headers, labeled freshness stamps), with the viewer's local
  equivalent as a hover tooltip. Displayed data timestamps stay the exact
  wire text, so they remain copy-pasteable into `--since`/`--until`/`AS OF`
  unchanged.
- **The console adopts the dbtrail.com brand palette and typography**
  (#1355). Colors are mapped by role in oklch keeping the console's
  lightness relationships — every text/background pair in use passes WCAG AA
  (the old ramp failed seven of them). The semantic INSERT/UPDATE/DELETE/diff
  colors keep their meanings. The declared brand fonts now actually ship:
  subsetted woff2 files (~123 KB, OFL) embedded in the binary and served
  same-origin, keeping the console's zero-external-requests posture.

## [0.54.0] - 2026-08-14

### Added
- **A capture-skip record can be acknowledged** (#1314). `stream_state.capture_skips`
  is monotonic, so a single skip episode kept the console's capture-health box
  on screen and `status --fail-on-gap` exiting non-zero forever. The only escape
  the product documented — stop the daemon and clear the column — is impossible
  from the console and *destroys the loss record*, which is the one thing that
  should survive. An alert nobody can clear is an alert everybody removes, and
  the next real loss then lands in silence.
  - `bintrail status --ack-capture-skips` and the console's **Mark as read**
    button (`POST /api/capture-skips/ack`, `servers:write`) record the count and
    the moment in a new `stream_state.capture_skips_ack` column. Nothing is
    erased: the tally stays, `status` keeps printing it (`⚠ ON RECORD` instead
    of `⚠ DEGRADED`), and the console renders it muted rather than as an alarm.
  - An acknowledgement covers a **count**, not a fact, so a later skip lifts the
    tally above it and the alarm — and the `--fail-on-gap` failure — return with
    no operator action. It can retire a record; it cannot mute the next
    incident.
  - The console endpoint takes the total the page rendered and refuses with 409
    if the live tally is higher, so a tab left open cannot acknowledge skips
    nobody looked at.
  - `status --format json` gains `capture_health.acknowledged` and
    `capture_health.acknowledged_at`. `capture_health.status` deliberately stays
    `"degraded"`: the events really are still missing, and a consumer keying on
    the verdict must not read a human's "seen it" as the loss being undone.

- **The compose stack ships observable by default** (#975). The `bintrail`
  service now serves Prometheus metrics (`BINTRAIL_METRICS_ADDR`, published on
  host loopback `127.0.0.1:9090`, override with `METRICS_ADDR`) and carries a
  real healthcheck, so a wedged-but-alive `watch` daemon is visible —
  previously deployment.md's observability story pointed at a port the default
  stack never opened, and `restart: unless-stopped` fires on process exit only.

- **Two copies of one event that disagree are now a surfaced finding** (#841,
  #1325). While a partition is archived but not yet dropped, the same
  `event_id` legitimately arrives from both the live index and the Parquet
  archive. The copies agreeing is an invariant (the archive is written from
  the index), so a divergence means something real — an index row mutated
  after archiving, or a damaged archive. The merge now resolves
  deterministically in favor of the live index instead of by argument order,
  counts the divergences, and the finding travels: a `slog.Warn` at the merge
  layer plus a warning in the console and MCP responses — including on the
  recover path, where the chosen copy's row images become the `SET` clauses of
  the generated reversal SQL.

- **SBOM coverage extends to every shipped artifact** (#976). deb/rpm packages
  get syft SPDX `*.sbom.json` sidecars on the release page like the archives
  always did, and the GHCR images (`bintrail` / `bintrail-console` /
  `bintrail-pg`) get per-architecture SBOMs attached as cosign attestations on
  the pushed manifest lists (GoReleaser's sboms pipe cannot catalog container
  images, so a publish-phase script does).

- **`bintrail agent` gains `--source-flavor`** (mysql | mariadb; #623). The
  BYOS agent was the last capture surface with a hardwired `"mysql"` flavor;
  MariaDB sources now stream through the agent with the same flag and
  `BINTRAIL_SOURCE_FLAVOR` env binding as `stream`. Existing invocations are
  unchanged (default `mysql`).

- **PG extension-type capture is evidence now, not "should work"** (#1210).
  Composite types join the shared PG type matrix on every plain
  `postgres:14–17` CI cell — through both the recover round-trip and the
  baseline+delta reconstruct fold — and PostGIS geometry + pgvector vectors
  get their own image-gated integration coverage.

### Changed
- **Release signatures are Sigstore bundles now** (#1350). `checksums.txt` is
  signed as `checksums.txt.sigstore.json` — certificate, signature and
  transparency-log entry in one file, the cosign v3+ default — INSTEAD of the
  former `checksums.txt.sig` + `checksums.txt.pem` pair. Verifying needs
  cosign v3.0 or newer; the recipe is in docs/install.md ("Verify a
  download"). A new CI `signing` job replays the exact goreleaser signs
  cmd/args against a throwaway file on every push and same-repo PR — signing
  otherwise only ever executed on tag push, so a breaking cosign or config
  change could merge green and surface mid-release — and asserts the
  ci/release installer-pin lockstep executably.

- **The MCP `recover`/`query` tool filters are aligned with the CLI, and the
  parity is pinned** (#962). `changed_column` is removed from the `recover`
  tool: `bintrail recover` refuses `--changed-column` by design (a
  changed-column filter names row *versions*, and reversing a filtered subset
  of a row's history can produce a state that never existed). A client still
  sending it gets a loud schema-validation error naming the property —
  `additionalProperties: false` — instead of a silently ignored filter.

- **`bintrail-console` no longer carries its own copy of the env-file loader**
  (#963). `consoleapp/env.go` held `loadEnvFile`/`parseAndSetEnv` byte-for-byte
  identical to `internal/cli/env.go`, plus its own `sync.Once`, so any change
  to env-file semantics would land in one binary and silently not the other.
  Both now call the exported `cli.LoadEnvFile`. Sharing the `sync.Once` is also
  more correct: `consoleapp` imports `internal/cli`, so two of them in one
  process meant the file could be read twice. A guard test fails if the
  duplicate comes back — the previous marker was a code comment naming itself
  a "consolidation candidate", and a comment cannot fail.

- **MariaDB multi-domain GTID resume is ratified as per-domain** (#621, with
  the #517 alpha residuals). Live empirical validation on a real multi-domain
  topology plus the docs stance; the validation immediately exposed a real
  data race in go-mysql's MariaDB GTID handling, fixed by pinning go-mysql
  v1.16.0. From the residuals: the `--no-gap-fill` MariaDB-GTID refusal has a
  regression test driving the same cmd-layer entry point the CLI uses, a dead
  wrapper is gone, and unhandled-row drops are counted instead of silent.

- CI: the two batches of major GitHub-Actions bumps dependabot could not land
  are taken deliberately (#1316, #1333), and the MariaDB-source integration
  job is a matrix across 10.6 LTS, 10.11 LTS and 11.4 (#1339).

### Fixed
- **The query planner no longer plans an unreadable `archive_state` as "no
  archives"** (#1324). `Plan` swallowed every `archive_state` failure at
  `Debug` and planned as if the table were empty, so two different facts were
  indistinguishable: an index with no archive tier, and an archive tier that
  exists but could not be read. Only `ER_NO_SUCH_TABLE` now counts as "no
  archive tier"; every other failure — a permission denial, a corrupt table, a
  legacy shape the fallback cannot read — is logged at `Warn` and recorded as
  `QueryPlan.ArchiveCoverageUnavailable`. The same conflation #816 retired in
  `status.LoadCoverage`, one layer over.
  - The console's activity tiles keyed "complete" on that plan: with coverage
    unread, every archived hour classified as a gap, the archived-hours caveat
    counted zero, and an `archive_state` read failure rendered as "these
    counts are complete." The response now says the archive records could not
    be read and drops the completeness claim.
  - Gap classification itself is unchanged and stays fail-closed: an hour
    whose coverage could not be verified is still a gap, so a strict
    (`AllowGaps=false`) `reconstruct` still refuses — but the real cause now
    reaches the operator instead of sitting at `Debug` under a message that
    blames rotation.

- **The query planner no longer counts archives the read will never open**
  (#1232). Rotation is per-process, so an index capturing more than one source
  has more than one archive destination and nothing makes them archive the same
  partitions. `loadArchiveCoverage` read `archive_state` unscoped, so an hour
  archived by one rotation was classified as covered for a read that opens a
  different destination — the read fetched nothing for that hour and `GapHours`
  came back empty. Coverage is now scoped to the archive sources the read will
  actually fetch from, closing the gap between what the planner counts and what
  the fetch opens. Concretely this fixes: `bintrail query` scoped to a subset
  with `--archive-dir`/`--archive-s3` + `--bintrail-id`; `bintrail query
  --profile`, which opens no archives at all yet was credited with every
  registered one; and, on every merged read, `archive_state` rows with a NULL
  `bintrail_id` or with paths that resolve to nothing on this host, which
  previously counted as coverage for files nobody could open.
  - Single-source indexes are byte-identical to before, as are the index-wide
    callers that legitimately read every archive (the
    `bintrail_index_gap_hours` gauge and the console's activity caveat), which
    pass an explicit unscoped nil.
  - Note this is **not** scoping by which source produced the events:
    `binlog_events` has no source discriminator and `ArchivePartition` archives
    the whole shared partition, so one source's archive of an hour holds every
    source's rows for it. `archive_state.bintrail_id` records who archived a
    partition, not whose rows are inside — scoping by data ownership would
    report gaps over data that is present.

- **A console session with an RBAC data profile now says it reads the live
  index only** (#1311). Such a session is served with `NoArchive` because the
  Parquet path cannot apply per-column redaction — the right call, and it fails
  in the safe direction — but the response never said so. Hours rotated into
  archive storage still exist; the session simply does not open them, and a
  short or empty result reads as "nothing happened in that window", which is
  the one conclusion the data does not support. `/api/events` and
  `/api/recover` now carry a warning that states the scope and denies that
  inference in words.
  - The warning does **not** depend on the query planner. The planner only runs
    with a time range, so the default browse — newest N events — produced no
    plan and therefore no warning at all, which was the worst case.
  - Gap hours under an archive-excluded read are no longer explained as
    "rotated and not archived". The planner classifies archived-only hours as
    gaps for such a read by design; naming rotation as the cause sends the
    operator to audit a rotation that is working. The hours are still named,
    with the cause left open.
  - A console started with `--no-archive` announces itself only once the plan
    actually finds hours it could not read, so a whole-deployment setting does
    not become a permanent banner on every response — which is read by nobody,
    including on the day it matters. A session profile is announced regardless,
    because it is the one the reader cannot otherwise discover.
  - The events view now renders the response's `warnings`, and Preview no
    longer clears the notice Generate-undo rendered. The server had been
    computing the right sentence for `/api/events` and the browser was dropping
    it, so the flagship case — a profiled session's default browse — stayed
    silent on screen.

- **`status` no longer reports an unreadable archive tier as no archive tier**
  (#816). `LoadCoverage` degraded every `archive_state` failure to live-only
  coverage behind a `slog.Warn`, so "this index has no archives" and "I could
  not read the archives" printed identically — a restore window SHORTER than
  reality, which an operator reads as "that incident is beyond recovery" while
  the Parquet covering it is still in the bucket. Only a genuinely missing
  table now counts as no archive tier; any other outcome — including an
  `archive_state` row whose `partition_name` will not parse, which left the
  floor silently live-only — sets
  `CoverageInfo.ArchiveUnavailable`, and both renderings say the window is a
  lower bound (`⚠ NOT READ` in the text report, `coverage.archives_unavailable`
  plus `archives_error` in JSON). The "(includes archives)" label is withheld
  in that state rather than claiming coverage that was never read.

- **A `status` coverage or archives read failure no longer deletes its
  section** (#1323). One frame above #816's fix, `CollectStatus` swallowed a
  `LoadCoverage` error with a `slog.Warn` — and the `binlog_events` full scan
  is the statement in that package most likely to hit `max_execution_time` or
  a lost connection — so the report printed no restore window, no error, and
  exited 0; a monitor keyed on the new `coverage.archives_unavailable` saw
  nothing rather than `true`. `LoadArchiveStats` swallowed the identical
  failure on the identical table. Both failures now surface in the section
  they would have filled.

- **Binlog positions past 2^63 archive correctly** (#1218). Pre-#1180 builds
  stored the MariaDB underflow shape (`StartPos = 2^64 − EventSize`) in real
  indexes, and `ArchivePartition` scanned `start_pos`/`end_pos` through
  `sql.NullInt64` — any partition holding such a row failed to archive
  forever, so rotation could never drop it. Positions are now unsigned end to
  end (the Parquet columns included, so nothing is written silently wrapped),
  and one scan reads both schema generations.

- **A schema-scoped publication's UNLOGGED tables are warned about** (#1211).
  The UNLOGGED-table coverage guard evaluated capture scope only for
  `FOR ALL TABLES` publications; an UNLOGGED table pulled into scope by a
  `FOR TABLES IN SCHEMA` publication (PostgreSQL 15+) was silently uncaptured
  with no warning — the exact silent-loss shape the guard exists to close.
  The shared probe now enumerates published schemas too, on both the stream
  startup preflight and doctor.

- **A profiled console session's schema dropdown no longer offers schemas its
  every query answers zero rows for** (#1326). The session is served
  archive-excluded, but `/api/schemas` listed snapshot-only (archive-only)
  schemas anyway — promising a target the session cannot reach and
  contradicting the #1311 warning on the very results it produced.

## [0.53.0] - 2026-08-11

### Fixed
- **The capture-skip banner can now show that a fix worked** (#1312): the tally
  in `stream_state.capture_skips` is monotonic — it counts skips that
  *happened*, not skips still happening — so pressing the console's own
  "Refresh schema snapshot" button left the orange alarm byte-identical. The
  banner documented that in a paragraph, which is documentation standing in for
  a missing feature, and the escape hatch it named (stop the daemon, hand-edit a
  JSON column) is not something a console user can do from the console. The
  schema snapshot's own timestamp is now the acknowledgement: `MAX(schema_snapshots.snapshot_time)`
  against each reason's `last_at`, shipped as `capture_health.snapshot_at` and
  `capture_health.skips_predate_snapshot`. Skips that all predate the current
  snapshot render quietly instead of as a live alarm — quietly, **not** gone:
  those events are permanently missing from the index and the box keeps saying
  so.

  Neither wording claims "resolved". `stream_state` does not record which
  snapshot capture is running on, so a newer one proves it exists, not that the
  stream reloaded onto it; and on a source with no writes, "nothing skipped
  since" is true for the trivial reason. Both facts are in the text.
  `capture_health.status` stays `"degraded"` in both states — `--fail-on-gap`
  keys on it, and turning a permanent-loss record into `"ok"` would be a change
  to exit semantics, not to rendering. A backend that sends no anchor (an older
  daemon) stays loud and unchanged.

### Changed
- **The capture-skip explanation is collapsed by default in the console**
  (#1312): ~250 words of cause, remedy and scope rendered flat inside an alarm
  box, which is the reliable way to have none of it read. It moves behind a
  disclosure — one click away, nothing deleted. `bintrail status` still prints
  the full text, unchanged.

## [0.52.0] - 2026-08-11

One isolation bug and one route. **Operators running `bintrail-pg flashback`
with `allowed_schemas` in `shim.yaml` should read the Security item before
upgrading**: the allowlist starts being enforced on that front-end, so a query
that worked yesterday can now be refused — which is what the configuration
asked for.

### Security
- **`allowed_schemas` is enforced on the PostgreSQL wire front-end** (#1261):
  the allowlist landed in the MySQL front-end's two chokepoints (`UseDB` and
  `HandleQuery`), and the PostgreSQL session reaches neither — it parses and
  dispatches on its own. A tenant connecting over `bintrail-pg flashback` could
  therefore read **every schema in the index** regardless of what `shim.yaml`
  said. Silent, and in the direction that matters: the operator who configured
  the isolation believed it was in force. The verdict now lives in one
  protocol-neutral place (`shim.SchemaAuthzCheck`) that both front-ends call, so
  the two cannot drift — a parity test asserts they never disagree. Denial is
  SQLSTATE `42501` on the query, not a dropped connection, and a connect
  database outside the allowlist is not a startup refusal either: the session
  seeds and the first real query answers `42501`, which the client can read.

### Added
- **`GET /api/profiles`** (#1299 follow-up): the RBAC data-profile names for the
  current index, sorted, names only. A settings surface that wants to offer the
  profile vocabulary as a picker — instead of a free-text field the operator has
  to already know — has no index access of its own, so the vocabulary needs a
  route. Tiered `settings:read`, and pinned there by test: the route-table
  guards check that every route *is* classified, never what it is classified
  *as*, so a move to `query:execute` would keep every caller working while
  handing access-control vocabulary to any analyst. An index predating the
  profiles table lists empty rather than erroring (the `archive_state` 1146
  precedent), and that swallow is deliberately narrow to 1146 — on a dead
  connection, "no profiles exist" would render an empty picker, strictly worse
  than the free-text fallback a real error produces.

### Changed
- **Point-in-time-recovery jargon dropped from the operator-facing
  `reconstruct` strings and the README.** The term describes a database's own
  log-replay restore; what `reconstruct` does is merge a baseline with indexed
  deltas, and borrowing the phrase set an expectation about the mechanism that
  the command does not meet.

## [0.51.0] - 2026-08-09

A second console release driven by using the product rather than reading it.
Every item below started as an operator's complaint on a live deployment, and
three of them turned out to be the console reporting something that was not
true.

### Added
- **The Events view pages** (#1297, #1306): it rendered one window of the newest
  100 events and event 101 was unreachable without inventing a filter. Paging is
  **keyset**, not offset — an offset re-scans and re-sorts every skipped row, and
  on a merged live+archive read it would re-download the archives for each page,
  so page 40 costs what page 1 costs. The cursor carries
  `(event_timestamp, event_id)` because `event_timestamp` has one-second
  resolution and collides heavily under load: a timestamp-only cursor would
  either re-serve or skip every event sharing the boundary second. The header
  states position rather than a count that described the limit, and export
  covers every match of the current search instead of the page on screen — an
  operator four pages into an incident was getting a quarter of the evidence.
- **`GET /api/activity`, and a scope on every Overview tile** (#1300, #1307): the
  landing page derived four figures from a 200-event fetch (~700 kB of row
  images) and labelled them as if they described the whole index. Counts now
  come from a grouped query over a stated window, with a period picker, and each
  tile says what it covers (`all time · estimate`, `last 24 h`, `point in
  time`). The window is capped at 24 h on purpose: no index leads with
  `event_timestamp`, so hourly partition pruning is the only lever, and a
  week-long aggregate is a report, not a tile that loads on every visit. Hours
  that have rotated to Parquet are reported as `partial` rather than silently
  undercounted.
- **`ext.ConsoleSettingsProvider`, and a registry instead of one view slot**
  (#1299, #1305): an embedding distribution could contribute exactly one console
  view, and its routes are refused wholesale while a data profile is active —
  which locks out precisely the administrator an administration surface exists
  for. Settings panels are a sibling surface: mounted at `/ext-settings/<id>/`,
  authorized per HTTP method (`settings:read` to read, `settings:write` to
  mutate, with unrecognized verbs failing closed to write), and handed no
  database, so the redaction reasoning that withholds the view surface does not
  reach them. `SetConsoleView` still works unchanged.
- **A schema-snapshot action on each monitored server** (#1296, #1308):
  `POST /api/servers/{id}/schema-snapshot`. The "capture degraded" banner told
  operators to take a fresh snapshot on the source and offered no way to do it.
  The action takes the snapshot **and restarts that server's capture stream** —
  a stream holds its resolver in memory and swaps it only on a DDL event, so
  writing a snapshot underneath a running stream changes nothing, and a refresh
  button without the reload would look like a fix while being a no-op.

### Changed
- **Time-travel and Restore are one screen** (#1298, #1303, #1304): reading a
  row as it stood at an instant is the step before deciding what to undo, and
  splitting them across two destinations made the operator enter the same target
  twice and carry a timestamp between screens by hand. The target is entered
  once and drives both halves; `/timetravel` redirects, so old bookmarks land on
  the merged page. The timeline gained **Use this moment**, which is the actual
  friction: pointing at the change that broke things instead of leaving to read
  its timestamp off Events and typing it back.

### Fixed
- **The full-table restore window reported the wrong start, and got worse the
  more baselines you took** (#1294, #1301): it reduced over the *newest* anchor
  per table, while baseline selection picks the newest snapshot **at or before**
  the instant asked for. So a table with baselines going back months was
  advertised as restorable only from the most recent one. It reduces over the
  earliest usable anchor now. (`broken_tables` correctly keeps using the newest.)
- **The Events view fanned out to S3 to serve a page live data already held**
  (#1295, #1302): a newest-first request for 100 events took ~20 s and pulled
  ~700 kB of archives that could not contribute. Under `DESC` with a filled
  page the archives are provably irrelevant — but only when live coverage is one
  contiguous range containing the cutoff. A restored index that interleaves
  archived hours between live ones breaks that, so the skip is conditioned on
  it.
- **The timeline's "Restore to this state" aimed at the wrong end of the
  window** (#1304): it set an upper bound, reversing everything *up to* that
  instant and landing the row before its entire recorded history — the opposite
  of what the button named, on the one screen whose job is deciding what to
  undo. Both call sites share one bridge now: `since = instant + 1s`, upper
  bound cleared. That is exact, not approximate — reconstruct applies events at
  or before the instant while recover reverses events at or after `since`, and
  `event_timestamp` is second-resolution, so nothing can hide in between.
- **`GET /api/coverage` was never classified in the route-permission table**
  (#1307): registered but unclassified, so it failed closed and every
  policy-carrying session got a 403 on the Overview's own coverage card. The
  completeness test's hand-maintained route mirror was missing it too, which is
  why nothing caught it; both lists now carry it.
- **"Capture degraded" could not be acted on, and stayed up after a working
  fix** (#1296, #1308): the tally behind it is monotonic and re-seeded across
  restarts on purpose, so a skip episode cannot be laundered by bouncing the
  daemon — which also means a real fix leaves the banner exactly where it was.
  It says so now, along with how to confirm a fix (the count stops rising) and
  how to reset the tally deliberately. The message also names what was skipped
  instead of describing the condition in the abstract.

## [0.50.1] - 2026-08-09

A console usability release. Everything below was found by using the console on
a real deployment rather than reading its code — including two things that had
been broken in plain sight since the sidebar last grew.

### Added
- **Refresh control on the Restore coverage card** (#1289, #1292): capture lag is
  the number an operator watches move, and it was fetched once per route load —
  seeing a new value meant reloading the page. The control refetches
  `/api/coverage` and rebuilds only that card, keeping the values on screen
  until the response lands and stamping them `as of <time>` so a refreshed
  reading is distinguishable from a stale one. A refresh that fails keeps the
  numbers and labels them, rather than blanking the card or letting them pass
  as current.
- **`ext.ConsoleBootAuthPathReceiver`** (#1293): the console now hands a
  credential backend the auth-file path it actually resolved. Installing a
  backend supersedes that file for login, so a backend keeping the operator's
  built-in credential working had to locate it independently — from inputs it
  cannot all see, since a path given as `--auth-file` never leaves this
  process's flag parsing. The two would open different files and the operator
  would be locked out of a console they configured correctly. Optional in both
  directions; the OSS build installs no backend and is unaffected.

### Fixed
- **The sidebar could not be scrolled, so its footer was unreachable** (#1290,
  #1291): `#app`'s grid row was auto-sized, and an auto row takes the tallest
  item's *content* height. `.main` is a scroll container and never stretched
  the row, but the sidebar is not one — so a sidebar taller than the viewport
  grew the row past `100vh` and `overflow: hidden` clipped the bottom off-screen
  with nothing to scroll. **Log out** and the `stream`/`version` rows became
  unreachable at normal zoom once an extension view and the Settings group were
  present; zooming the browser out was the only way to reach them.
- **Every dynamically-rendered icon was invisible** (#1292): icon constants are
  parsed as `image/svg+xml`, which applies no default namespace, and none
  carried an explicit `xmlns` — so each produced a namespace-less `<svg>` that
  occupied its CSS box and painted nothing. Visible as the extension nav item
  rendering label-only next to the icons inlined in the page. Fixed in the
  parser rather than per-constant, so a newly added icon cannot reintroduce it.

## [0.50.0] - 2026-08-08

Three threads. **Availability lag**: knowing capture didn't lose data is not the
same as knowing it is keeping up — this release measures the lag that decides
whether a recovery would be current. **Open the archive tier**: the Parquet
layout bintrail already writes becomes usable by your own OLAP tooling — view
definitions, refreshable baseline snapshots, a sandboxed SQL panel — without
bintrail becoming the query engine. **Name the remedy**: every gap error that
used to stop at a diagnosis now says what command fixes it.

### Added
- **Read→queryable lag, the number that decides recovery readiness** (#1223;
  #1240, #1242, #1244, #1248): the stream measures the time from reading a
  binlog event to that event being queryable in the index, `bintrail status`
  reports whether capture is keeping up (not just whether it lost data), the
  console coverage card says whether capture is alive, and the alerting docs
  center on what makes data recoverable.
- **`reconstruct --output-format parquet`** (#1169, #1241): full-table
  reconstruct can emit a discoverable baseline snapshot — `_SUCCESS`/`_MANIFEST`
  markers plus the binlog anchor in the Parquet footer — so a reconstruction
  becomes the next baseline. The cut is positional, never by timestamp: event
  timestamps are execution times, and a transaction straddling a timestamp cut
  would be lost between one fold and the next.
- **`bintrail baseline refresh`** (#1170, #1245): a new baseline = the old
  baseline + index deltas, with no mydumper run and without touching the source.
  Publication is all-or-nothing (a snapshot mixing two instants under one anchor
  is worse than none), refusals are verdict-typed (`refused-gap`,
  `refused-ddl`), and `--allow-gaps` stamps the footer so every descendant
  snapshot inherits the taint — a refresh cannot launder a gapped baseline.
- **`bintrail views`** (#1172, #1243): emits DuckDB view definitions over the
  Parquet archive layout (`events` plus one `state_<schema>_<table>` per table
  of the newest snapshot). Pure text — bintrail executes nothing; point your own
  DuckDB at your own lake.
- **Console: periodic baseline refresh from the daemon, opt-in** (#1171, #1247)
  and a **DuckDB-schema panel for external tooling** (#1246): `watch` can keep
  baselines moving forward on an interval (conservative DuckDB budget, never
  `--allow-gaps`), and the console offers the view definitions for copy-paste
  instead of becoming the engine.
- **Console: sandboxed server-side SQL panel** (#1177, #1254): read-only DuckDB
  over the archive tier with `external_access=false` as the load-bearing
  prohibition and a fail-closed directory/S3-prefix allowlist as the carve-out.
- **Shim: opt-in per-tenant `allowed_schemas` authorization** (#824, #1255).
- **Encrypted dumps are authenticated with an HMAC-SHA256 sidecar** (#960,
  #1253): `openssl enc` CBC provides no integrity, so tampering was previously
  undetectable; verification is refused-on-mismatch, not advisory.
- **PostgreSQL-native live-source verify** (#1024, #1262): `verify
  --source-dsn` against a PG source computes the consistency checksum with
  PG-native semantics instead of assuming MySQL formatting.
- **`query --query-hash`** (#1235): filter events by statement digest. Query-only
  by design — a digest names a statement *shape*, so a `recover` filtered by it
  would revert executions nobody named; it is also refused under `--profile`.
- **`archive reconcile --trust-empty-scan`** (#1282): per-backend escape from
  the prune dead end after a legitimate total wipe, where "empty scan" was
  indistinguishable from "backend unreachable".
- **Console shows the running version in the sidebar footer** (#1239).

### Fixed
- **MariaDB system versioning corrupted every downstream surface that touched a
  versioned table** (#863, #1263; #1276; #1277; #1269): baselines now exclude
  period columns from the parsed schema (mydumper emits their values; loading
  them back is an error), snapshots synthesize the hidden period columns of
  *implicitly* versioned tables so row images align, cascade synthesis gates
  system-versioned child edges via a metadata probe in both phases, and
  reconstruct/verify refuse generated PK members up front with a named verdict
  instead of failing mid-run.
- **Gap errors diagnosed without naming the remedy** (#961; #1268, #1271,
  #1278, #1279): the CLI `GapError` hint, the MCP error rewrite, the console's
  reconstruct 422 and the docs all now name `bintrail archive reconcile
  --repair` — the bare `reconcile` everyone reached for first is a dry-run that
  re-syncs nothing.
- **`allow_gaps: true` silently widened beyond what the caller conceded**
  (#1283, #1287, #1288): the console's `/api/reconstruct` now surfaces the two
  blind spots the override also disables as explicit warnings, MCP query/recover
  results name archive sources that were skipped rather than folding them into a
  smaller answer, and a reconstruct boundary-probe error no longer leaks the CLI
  `--at` flag to MCP clients.
- **A corrupted S3 baseline read fine until it didn't** (#698, #1284):
  `_MANIFEST` CRCs are now validated on the S3 baseline read paths; a mismatch
  is a refusal, and a canceled validation is never cached as a verdict.
- **`VALUES` found by substring broke baseline parsing of INSERTs containing the
  word** (#502, #1267): the keyword is now found at statement level.
- **Console events CSV export was a formula-injection vector** (#965, #1250):
  cells starting with `=`, `+`, `-`, `@` are neutralized on export.

### Changed

- **CI hardening** (#972, #1249; #973, #1251; #971, #1265): the `oss-firewall`
  tripwire extends beyond Go sources; actions are pinned to commit SHAs with
  write permissions scoped per job; and the demo image is boot-tested on both
  architectures before its tag is created and signed — a failed smoke leaves no
  public tag.
- **Console e2e now covers the primary read workflows over a real fixture**
  (#970, #686, #619; #1264).
- **Repository root tidied so the README is reachable without scrolling.** Pure
  file moves, no behaviour change: `CONTRIBUTING.md`, `SECURITY.md` and `CLA.md`
  moved to `.github/`; `SUPPORT.md`, `TELEMETRY.md` and `bintrail-spec.md` moved
  to `docs/`; the four named Dockerfile variants moved to `build/`;
  `THIRD-PARTY-NOTICES.deps.sha256` moved next to the script that generates it
  (`scripts/`). GitHub still renders the Contributing and Security-policy links
  because it looks for community-health files in `.github/` and `docs/` as well
  as the root. If you build images from source, the flag changes from
  `-f Dockerfile.bintrail-console` to `-f build/Dockerfile.bintrail-console`;
  the plain `docker build .` path is untouched, and `LICENSE`, `NOTICE`,
  `THIRD-PARTY-NOTICES`, `PRIVACY.md`, `install.sh`, `.env.example`,
  `docker-compose.yml` and every exported Go package stay exactly where they
  were. `PRIVACY.md` in particular cannot move: the shipped `.mcpb` bundles
  declare its absolute URL, and GitHub 404s a moved file rather than
  redirecting. The `drafts/` directory is no longer tracked.
- **End-to-end tests and bundle packaging consolidated.** `e2e_test.go` moved
  from the module root to `test/e2e/`, `e2e/shim/` to `test/shim/` (so every
  end-to-end harness lives under `test/`, alongside `test/console-e2e/`), and
  `packaging/` to `build/packaging/`. Test names are unchanged, so the CI
  false-green tripwires that assert `TestEndToEnd_fullPipeline` actually ran
  still match. If you run the shim harness by hand, the path is now
  `SHIM_E2E=1 go test -tags shim_e2e ./test/shim/...`.

## [0.49.0] - 2026-08-03

The **continuous backup assurance** epic (#1189). A recovery tool that is never
exercised is a claim, not a capability: the index could be silently missing
hours, the newest baseline could sit outside the window the deltas can bridge,
and nothing would say so until a restore was already needed. This release adds
the checks that run on their own, the channel that reports them, the rehearsal
that proves a restore end to end, and the path back when the index itself is
what was lost.

### Added
- **Scheduled verification with persisted run history** (#1191, #1200): `bintrail-console watch` verifies on a schedule and keeps every verdict, so "when did this last verify, and how has it been trending" has an answer. The history is a console-local file, never a table in the index database — scheduled verify covers registry servers and registry DSNs never receive DDL. A shutdown mid-run is recorded as such rather than persisted as a success, and a cycle that could not run a server records the skip instead of dropping it, so a schedule that never actually verifies is visible rather than silent.
- **Webhook notification channel** (#1192, #1201): edge-triggered notifications for permanently-lost events, verify problems and unhealthy rotation, on the same contract shape as the audit sink. Severity is part of the notification key, so a warning cannot suppress a critical for the same target, and an all-inconclusive run never auto-resolves a standing mismatch alert.
- **Baseline staleness verdict** (#1193, #1213): every baseline snapshot is graded — ok / aging / broken / unknown — against the oldest instant deltas are actually available from, which is what says whether a full restore is still possible. The floor has one strict implementation: partition existence is coverage, archives extend it backwards only when contiguous, and `aging` deliberately never alerts (until retention saturates it fires on every fresh install).
- **Restore coverage as a live RPO card** (#1194, #1220): the console overview shows the reconstructable window, capture lag and the continuity verdict, backed by a new `GET /api/coverage`. The window's upper edge is a per-partition probe rather than a whole-table `MAX`, and the full-table half reports `unknown` on error so a failure can never render as "nothing is broken".
- **`bintrail drill`** (#1195, #1222): an automated restore rehearsal into an operator-provided scratch MySQL — reconstruct, dump, load, and compare the loaded row count against exactly what was written. Per-table timings are real RTO data. It refuses a target holding any table in the drilled schemas, and a binlog-only fallback is a marked failure rather than a pass: a rehearsal without a baseline is never a PASS. See `docs/drill.md`.
- **`bintrail restore-index`** (#1196, #1229): rebuild a lost index database from the Parquet archive tier. The fresh-index guard probes `stream_state` and `schema_snapshots` as well as the events table — a surviving `stream_state` would make a restarted stream resume at the old position and fake continuity — and the report is an honest inventory: rows already loaded from a file that later failed are named as such rather than folded into a clean total. See `docs/index-recovery.md`.
- **`doctor --archive-s3`** (#1197, #1230): an advisory S3 Object Lock posture check — WARN, SKIP or PASS, never FAIL. It flags a bucket with lock disabled, one enabled without a default retention rule (uploads set no per-object retention, so archives land unlocked), and a default retention shorter than `--retain`. The audit behind it found no code path in bintrail that deletes archived data from S3 at all, and the SDK already sends the checksum a locked bucket requires. See `docs/object-lock.md`.
- **Continuity, verify and rotation health as Prometheus gauges** (#1203, #1205).
- **`assurance` package** (#1233, #1234): exported read-only accessors for the coverage, continuity, staleness and verify-history signals, for tooling and distributions that import the core as a module. Type aliases rather than restated structs, so a second implementation of any verdict cannot appear and an embedder's rendering cannot drift from `bintrail status`.
- **MCP `list_schema_changes` gains `snapshot_id` and an `uncovered_only` filter** (#1050, #1190).
- **PostgreSQL as a source is GA** (#597): docs updated, along with stale console claims.

### Fixed
- **Baseline staleness cried wolf on an index capturing more than one source** (#1219, #1231): live partitions are shared by every source, but `archive_state` rows are per-source and a baseline snapshot carries no source identity at all — so extending the floor backwards with the union handed one source's archive coverage to another. Taking the union is a missed alarm; refusing to extend is a false one on two sources that have both archived since day one. Unattributable archives now grade `unknown` instead of either, the watcher skips such a target rather than resolving a standing alert, and `bintrail status` says the check could not be evaluated instead of printing a bare verdict.
- **Statement-format DML drops were counted but not attributed** (#999, #1204): the capture-skip tally could not say which reason discarded events, and `--fail-on-gap` did not fail on them.
- **An unreadable capture-skip ledger was laundered into a clean "{}"** (#1206, #1208): a ledger that could not be read was rewritten as empty, erasing the record that anything had been skipped.
- **Not every capture-skip reason failed `--fail-on-gap`** (#1207, #1209): the contract now covers all of them, failing closed.
- **`query` overflowed on binlog positions above 2^63** (#1202, #1217): the scan is now `uint64`; the dead #986 type-matrix workaround is gone.
- **File-mode `index` misdiagnosed validation-excluded tables** (#1199, #1216): the diagnosis now names the real reason, and the DDL hook degrades the way the stream's does instead of failing differently.
- **A PostgreSQL-shaped snapshot on the MySQL path was diagnosed as corrupt** (#1198, #1215; #1009, #1188): `reconstruct`, the shim and `verify` now say the snapshot is for the other engine, which is the verdict that leads to the fix.
- **A mismatched render-GUCs stamp went unwarned in the shim and console folds** (#921, #1214).
- **A DDL auto-snapshot over invalid tables crash-looped the stream** (#1051, #1186): it degrades instead.
- **The statement-format DML alarm fired for schemas that were never captured** (#1000, #1185).
- **`status` reported by-design TRUNCATE rows as uncovered DDL** (#1049, #1187), and mislabeled the index as file mode while doing it.

## [0.48.0] - 2026-07-31

### Added
- **`recover_cascade` MCP tool** (#1128, #1183): the MCP `recover` tool is delta-only, so an AI agent asked to undo an FK `ON DELETE CASCADE` got incomplete reversal SQL with nothing telling it so. The sixth tool closes that on both MCP surfaces (standalone `bintrail-mcp` and the console's `/mcp`), reusing the same synthesis engine as the CLI and console. It is deliberately stricter than either: a partial synthesis without `allow_incomplete: true` is an error listing every caveat, and with it the payload carries `complete: false`, the caveat list, and the script's `!!! INCOMPLETE RECOVERY` banner — an incomplete synthesis presented as complete is worse on the AI surface than anywhere else. Phase-1 window-only synthesis works with no baseline configured; the console rejects baseline parameters like the `reconstruct` tool does.
- **Console `/api/verify` carries `reason` and `total`** (#1127, #1182): the status→bucket classification ("does this table count as pass, failure, or couldn't-tell") existed in three places that could diverge — a divergence here is false assurance. It now has one exported home (`verify.NormalizeStatus`), the console and supervisor copies are deleted, and the two machine-readable surfaces converge additively: the console emits the CLI's `reason` field name (keeping `detail` as a documented legacy alias) and its summary gains `total`. Unknown statuses bucket as errors on every surface, including the frontend fallback.

### Fixed
- **MariaDB 11.4+: a stream restart deleted every indexed row** (#1117, #1180): MariaDB 11.4 writes cache-buffered row events with `end_log_pos=0`, so every captured row stored `start_pos = 2^64-EventSize` (an underflow) and `end_pos = 0`. The resume dedup cut on `start_pos >= checkpoint`, matched all of them, and deleted the whole file's worth of already-indexed rows on **every** restart — including rows the source will never re-send. Positions are now recomputed (go-mysql's `FillZeroLogPos`) for streaming AND file-based indexing, the position-wraparound guard understands the recomputed sequence, a post-reconnect corrector undoes the ghost-FDE overshoot the recomputation introduces when a resume lands mid-transaction (reproduced live against MariaDB 11.4.12), and an underflowed position is now a hard error instead of silently stored corruption.
- **Full-table reconstruct cried wolf about a baseline gap on every healthy run** (#1163, #1173): the warning compared the baseline anchor against the table's FIRST event — which on a healthy run is expected to sit at a later position (it is simply the table's next write), so the warning fired on provably exact reconstructions, on the recovery path, where an operator is least able to judge it. The verdict is now decided against the index's earliest surviving event: at-or-before the anchor proves capture was already running when the baseline was taken (quiet); anything else emits one hedged warning saying exactly what could not be proven, carrying both coordinates. The proof is one-directional — it can silence, never assert — so no new false-alarm class appears.
- **A failed full-table reconstruct left a loadable, silently-truncated dump on disk** (#1162, #1168): the mydumper writer's cleanup FINALIZED chunks on an early return — terminating `";\n"`, flush, keep — so a mid-table error left a syntactically valid `.sql` prefix plus the schema file, with no in-file signal it was truncated. `myloader` and `cat *.sql | mysql` never read the directory's `_INCOMPLETE` marker. A failed table's artifacts are now discarded (in-progress chunk, rotated chunks, schema file), a successfully finalized writer is structurally undeletable, completed sibling tables survive, and duplicate `--tables` entries are deduped instead of racing two writers over the same filenames.
- **Console and MCP reconstruct missed binary primary keys the CLI resolved** (#1157, #1159, #1179): the fixed `BINARY(n)` pad-and-retry from 0.47.0 lived in the CLI layer, so `/api/reconstruct` and the MCP `reconstruct` tool answered "the row did not exist at that time" for the identical key the CLI resolved — two surfaces, same index, contradictory answers, no warning. The reconciler now lives inside `ReadBaselineRow` behind every surface, the event fetch respells the key on all of them (a lowercase or full-width hex spelling previously hit the baseline but silently matched zero deltas, presenting baseline-era state as current), and the declared width is resolved from the schema epoch in effect at the baseline instant, so a later `ALTER ... BINARY(16)→BINARY(32)` no longer breaks the lookup.
- **A console MCP token minted with `settings:read` could reach unredacted baseline data** (#1124, #1181): minting required only `settings:read` while the direct endpoints required per-capability permissions, so a restricted session could mint a token and obtain through `/mcp` what `/api/reconstruct` would refuse it — and baseline reads bypass RBAC redaction by design. A managed token now records its minter's permission grants and every `tools/call` is gated per tool; sessions are bound to the credential that created them (a leaked session id no longer inherits a stronger session's grants), idle sessions expire after 30 minutes, non-tool content methods deny by default, and legacy grant-less tokens keep full read access until rotated — rotation re-records from the current minter.
- **Two surfaces served historical row images with no audit emission** (#1123, #1178): `bintrail-pg flashback` resolves rows below the layer that audits, and `reconstruct --baseline-only` returned before the emission covering the command's other four modes — both now emit, driven by contract tests. The console flashback port's recorded actor is now prefixed `server:` (any username plus the shared token authenticates there; the username is a routing key, and a sink treating it as a person would attribute every read to a database name). A source-level backstop counts every `ext.Record` call site against an explicit map, so an unaudited new call site fails the build instead of shipping.
- **The `--suppress-triggers` note promised more than `session_replication_role` delivers** (#1121, #1174): the emitted text claimed the target's triggers "do NOT fire" — but `ENABLE ALWAYS` triggers still fire under `replica`, and `ENABLE REPLICA` triggers fire ONLY under it, so the flag can cause side effects a normal apply would not. Rows written while FK constraint triggers are skipped are also never re-validated at COMMIT: they stay violating permanently while the constraint remains marked valid. The emitted comments, flag help, and docs now state the real matrix, the permanence, when the double-apply rationale actually holds (only when the script's scope covers the tables those triggers write), and that the file must be applied in one session (`SET LOCAL` outside a transaction warns and does nothing). The dialect warnings also stopped hedging on file-indexed databases, whose source is provably MySQL-family.
- **The integration matrix failed intermittently on green code** (#1164, #1184): the supervisor teardown raced mysqld's asynchronous `GET_LOCK` release, a commit-timestamp assertion failed on a legitimate second-boundary straddle, fixed handshake deadlines aborted mid-auth under load, and one test took a machine-global lockfile any concurrent `bintrail dump` would collide with. Each fixed at the mechanism, not by re-running.

### Added
- **`status` reports a Capture health verdict** (#1152): a stream can discard 100% of row events — the #700 column-count guard rejecting rows against a corrupt snapshot, for instance — while `status` shows a fresh checkpoint and no gaps, the failure visible only as per-event WARNs in the daemon log. The parser now keeps per-reason skip tallies (column-count mismatch, table absent from the snapshot, no resolver, unhandled row event, statement-format DML), persists them to a new `stream_state.capture_skips` column, and surfaces them as a verdict alongside the #649 continuity one. After 100 consecutive skips a single ERROR with remediation is emitted, re-armed once an event is captured.
- **Console `/api/schemas` carries schema provenance** (#1071, #1150): two backward-compatible fields — `snapshot_only` marks schemas known from the latest schema snapshot but with no live events observed, and `snapshot_unavailable` says the snapshot half was skipped because the resolver failed to load, so an empty listing is diagnosable instead of silent. The picker labels snapshot-only entries and explains that queries may return nothing.

### Fixed
- **Binary-family primary keys resolve against the baseline** (#1155, #1156): a `BINARY`/`VARBINARY`/blob-prefix primary key captured correctly after #1132 but was unusable downstream — `verify` reported the table `inconclusive`, `reconstruct --pk` could not find the row, and a row that *did* have events was blamed on a PK-changing UPDATE that never happened. The two sides spelled the key differently and only one was ever encoded. The canonicalizer now hands the baseline's bytes to the same `event.formatPKValue` the indexer used at capture, rather than adding a second encoder. `BIT`, `JSON` and spatial primary keys remain unsupported.
- **A full-table merge whose baseline and events are keyed apart is refused** (#1158, #1161): when a fixed `BINARY(n)` key is spelled differently on the two sides, the join misses. An UPDATE leftover emits the stale baseline row *and* appends the event — a duplicate key a restore rejects with 1062, loud but late. A DELETE leftover is skipped while the stale row was already emitted, so **a row deleted before the target instant survived into a dump that restores cleanly and is simply wrong**. The guard sits in the shared merge core, so the mydumper writer, the shim's full-table `_snapshot` and `verify` all inherit it, at O(1) extra memory.
- **Backfilled events archived under the wrong hour label stay time-queryable** (#1037, #1153): events replayed after a capture stall land in the oldest live partition, so rotation archives them under a label that disagrees with the rows inside. Every time-scoped read that pruned by that label silently skipped the file — a restore-coverage hole opened precisely while recovering from a gap. Rotation now records the true `min_event_ts`/`max_event_ts` in `archive_state` and pruning is derived from content rather than the label.
- **Registry servers inherit the process baseline flags** (#1010, #1154): a server added from the console UI could never enable Time-travel, reconstruct or verify, because the add form has no baseline field and only the boot entry consulted `--baseline-dir`/`--baseline-s3`. The process-wide defaults now fill in for any registry entry carrying no baseline of its own, all-or-nothing per entry.
- **`verify --check recover` gives honest verdicts on legacy and young indexes** (#1151): a pre-migration `SchemaVersion` of 0 on either side is an unknown epoch, so a column-set difference degrades to unresolved instead of reporting a real historical DDL as a conclusive mismatch; hours predating the oldest partition the index has ever held are worded as such, with the remedy that works, instead of "rotated and not archived"; and the chain walk keeps one mismatch detail plus a count rather than an unbounded slice.
- **`recover-cascade` no longer reports a silent clean zero on three paths** (#1125, #1147): a parent whose referenced key was UPDATEd under ON UPDATE CASCADE and later DELETEd left every child orphaned while claiming Complete; a key-chain probe truncated at its page cap concluded "no chain" from a bounded fetch; each now flags Incomplete instead.
- **The audit seam cannot be broken by a third-party sink** (#1122, #1149): `ext.Record` recovers a panicking sink, the console and shim emission sites use uncancelable contexts so a client disconnect cannot make a sink drop exactly the aborted-mid-response reads an auditor wants, and a session minted with no identity records `session:unidentified` rather than attributing to the token.
- **Console frontend: honest search scope, keyboard reachability, rotation input validation** (#966, #968, #1148): free-text search refines client-side over one fetched page but presented its count as index-wide truth; action buttons were `onclick`-only anchors unreachable by keyboard; dialogs had no Escape-to-close or initial focus.
- **An oversized line in an env file no longer silently drops every variable after it** (#1145): the loader iterated `bufio.Scanner` at the default 64 KiB token limit and never checked `scanner.Err()`, so a long value made `Scan()` return false indistinguishably from EOF — an oversized base64 key could unset the `BINTRAIL_INDEX_DSN` below it. The buffer is now 1 MiB and a truncation warns loudly that later variables were not loaded.

## [0.46.0] - 2026-07-26

### Added
- **Transaction commit time in microseconds** (#1111): every indexed event carries a one-second `event_timestamp` — all the binlog's common header holds — so inside one second the index preserved event ORDER but not event TIME. MySQL 8.0.1+ already writes the commit instant in microseconds into the GTID event; it is now captured as a nullable `commit_ts_us` on `binlog_events`, carried per transaction like `connection_id`, and surfaced in `query --format json|csv`. No source configuration needed (`gtid_mode` may be OFF — 8.0 stamps the anonymous GTID event too); `NULL` on MariaDB, on MySQL < 8.0.1, and for rows indexed before the column existed. Archive reads substitute a typed NULL when the column is absent, so every Parquet archive already on disk keeps reading.
- **Extension seam for MCP tools** (#1113): `ext/mcpext` lets an embedding distribution add tools to every MCP server the core builds — the standalone `bintrail-mcp` binary and the console's `/mcp` endpoint alike — resolving their target through the same routing the built-in four use, so an extension tool reads the server the operator selected and inherits its posture (including the console's refusal of a tool-level `index_dsn`). No-op in the stock binaries.
- **MCP `reconstruct` tool** (#953, #1114): single-row point-in-time reconstruction and `--history` over MCP, deliberately stricter than the console's HTTP endpoint (`allow_gaps` defaults false, destructive-DDL and capture-gap checks on). The console binds its own baseline resolution and rejects client-supplied baseline paths.
- **`verify --check recover`** (#1001, #1115): a verification mode that walks each PK's event chain in time order and asserts every UPDATE/DELETE before-image equals the state the previous event left — the data `recover` dereferences, which the content modes never touch. Index-only, bounded by `--max-events`, and deliberately conservative: a chain starting mid-window is `inconclusive`, never a mismatch.
- **`verify --format json`** (#954, #1109): machine-readable verification reports, with the exit decision shared by both renderings so text and JSON can never disagree on it.
- **`recover-cascade` reverses ON UPDATE CASCADE / SET NULL** (#1002, #1116): a parent-key UPDATE now synthesizes the child rows the cascade rewrote, consulting the FK's `update_rule` — never merged with `delete_rule`. Restores stay PK-anchored and guarded, so they can decline to touch a row but never widen the blast radius.
- **Apply-side codegen switches on `recover`** (#1003, #1110): `--suppress-triggers` (PostgreSQL) emits `SET LOCAL session_replication_role = replica` after `BEGIN`; `--restore-auto-increment` (MySQL) appends a commented-out `AUTO_INCREMENT` reset after `COMMIT` (an ALTER's implicit commit would split the reversal's atomicity). Both opt-in and gated per dialect at generation time.
- **Audit seam coverage at every historical-read surface** (#945, #1119): `ext.Record` now fires from every surface that serves historical row data — CLI, MCP, shim, console — pinned by contract tests over a canonical surface/action set. Emission sits on the success path, outside every lock and transaction, and cannot fail a query.
- **Source jobs run on the `stream` and `agent` daemons** (#1105): the `ext.RegisterSourceJob` seam previously fired only under `up`, `watch` and the console monitor, so the two capture paths a plain deployment actually runs started no per-source background work at all.
- **Opt-in point-consistent baselines** (#1099): `bintrail dump` documents mydumper's NO_LOCK cross-table skew and gains a mode that takes the snapshot at one consistent point.
- **`stream` auto-discovers GTID mode** (#1140): a first run against a `gtid_mode=ON` source no longer needs `--start-gtid` to end up in GTID mode.

### Fixed
- **A binary primary key no longer kills the daemon** (#1132, #1134): PK bytes that `utf8mb4` cannot store are hex-encoded (`0x…`) instead of aborting the batch INSERT. Scope covers TEXT/BLOB as well; `docs` now state that `--pk` needs the `0x` spelling for binary keys (#1138, #1142).
- **BYOS reads accept pre-#1132 `pk_values` spellings** (#1137, #1141), so an index written by an older agent stays queryable.
- **`verify` no longer reports false mismatches on BINARY(n) and spatial columns** (#1135, #1143): the binlog trims `0x00` padding from fixed-width `BINARY(n)`, which read as drift against the source. Spatial columns are decoded rather than compared as opaque bytes.
- **Spatial and VECTOR values render correctly on the read surfaces** (#1144, #1146): the shim decodes spatial event values to raw bytes, and `recover` emits VECTOR as `X'hex'`.
- **Sub-second event times are floored, not rounded, into `event_timestamp`** (#1136, #1139): rounding up could place an event in the following second — and, at a partition boundary, in the following partition.
- **Identifiers and PK values interpolated into `--` comments are sanitized** (#1131, #1133) so a crafted value cannot break out of the comment in generated recovery SQL.
- **Console recover output is capped by BYTES, not just rows** (#849, #1096): a row-count cap alone let a wide-row reversal script reach the 2 GiB default budget on a shared daemon.
- **`recover-cascade` surfaces a stale baseline as a warning, not as incompleteness** (#618, #1094), and the CLI and console now share one baseline provider (#1101, #1102, #1106) so the two surfaces cannot diverge on lookup semantics.
- **Full-table `reconstruct` streams its event window instead of materializing it** (#1097, #1112): the window is paged through a keyset cursor, with the TOAST and PK-change guards moved onto the per-event fold where they still see before-images.
- **Full-table `reconstruct` bounds its DuckDB memory** (#1098), scales the volume warning by parallelism, and marks output completeness.

### Changed
- **DuckDB bumped to v2.5.6 (DuckDB 1.4.5 LTS)** (#1103), pinned off the 1.5 line.
- **`cmd/mcp-gateway` removed from the OSS module** (#1100): the hosted multi-tenant gateway is not part of the open-core product.
- **CI gates hardened**: the MySQL integration matrix can no longer pass by skipping (#1093), and releases are gated on the integration matrix for the tagged ref (#1095).

## [0.45.0] - 2026-07-24

### Added
- **Console authorization denials land on the audit seam; sessions carry the verified identity** (#1092): a console session now records the verified login identity it was minted for (the auth-file username, an external provider's identity, a credential-backend username — display/audit only, never an authorization input). With that in hand, every authorization denial is emitted on the `ext.AuditSink` seam: `authz.denied` (permission and unclassified-route refusals, with actor, method, path, and the missing permission) and `profile.denied` (nonexistent session profile, and each unredactable-surface refusal — time-travel, baseline listings, recover-cascade, verify, extension views — with the gate named). `/api/capabilities` also stops advertising `extension_views` to sessions whose policy lacks `extview:read` (their data routes would 403, so the nav item would be a lie). **The stock binary is unchanged**: with no sink installed `ext.Record` is a no-op, policy-less sessions are never denied, and nil policies see the ext-views listing exactly as before.

### Fixed
- **`--batch-size ≥ 4096` no longer crash-loops the indexer** (#1090, closes #956): 16 placeholders/row × 4096 rows exceeds MySQL's 65535 prepared-statement parameter cap, so every batch INSERT died with a cryptic Error 1390. Oversized batch sizes are now clamped to the derived maximum (4095) with a startup warning — a daemon with `BINTRAIL_BATCH_SIZE=5000` keeps running.
- **A corrupt schema snapshot with duplicated column rows no longer halts capture indefinitely** (#1091, closes #1033): pre-#844 indexes can carry snapshots whose columns were double-inserted by concurrent writers; the resolver reported 2× the real column count and the schema-drift guard then skipped 100% of row events (observed: ~2 days of binlog read and discarded). The snapshot loader now dedupes exact duplicates and fails loud on genuine conflicts.

## [0.44.0] - 2026-07-23

### Added
- **Per-session console authorization seams** (#1078, #1083, #1088): the web console's extension surface (`ext`) grows three seams an embedding distribution can use to scope what an individual login session may do and see. (1) A session can carry an **access policy** — a set of `verb:noun` permission strings plus an optional data-profile name — enforced per `/api` route by an ordered route→permission table (segment-exact `{}` placeholder matching, first-match-wins, unclassified route = 403 fail-closed); `/api/capabilities` reports the session's effective grants so the UI hides surfaces a scoped session cannot use. (2) A **pluggable credential backend** (`ext.ConsoleCredentialProvider`) can serve the built-in username/password login form in place of the single-user auth file, preserving every login hardening guarantee (uniform failures, rate limiting, body/content-type caps before the backend runs, setup stays built-in-only). (3) A session carrying a **data-profile name** has that profile resolved against the *selected server's* index at request time: its deny/redact rules are unioned onto the startup `--profile` floor on the events and recover reads (with `query_text` withheld and archives disabled per request), and the surfaces whose reads bypass redaction — time travel, baseline listings, recover-cascade, verify, extension views — are refused with 403. **The stock binary attaches no policies and installs no backend, so OSS behavior is unchanged**: the password login and the static token keep minting full-access sessions, byte-for-byte as before.
- **`doctor` warns when the index co-locates with the source** (#983, closes #978): a new WARN-only preflight compares the source and index DSN endpoints (pure DSN parsing, no live query) and flags the same `host:port` — an index sharing its source's disk defeats the recovery safety net if that disk dies. `docs/quickstart.md` Option B now carries the separate-server callout too.
- **`doctor` measures index disk capacity on the bundled compose stack** (#984, closes #948): the disk-capacity check could never actually measure the bundled `docker-compose.yml` deployment (the daemon reaches `index-mysql` over TCP in a separate container, so the loopback-only path always reported "unmeasurable"). The compose file now bind-mounts the index datadir read-only into the daemon container and the check reads free space from the mount, so FAIL/WARN thresholds fire on the exact deployment they exist to protect.

### Fixed
- **`stream --reset` now records the continuity loss it creates** (#1080, closes #1079): `--reset` no longer `DELETE`s the `stream_state` row up front. The old checkpoint is kept until the new start position resolves, and a reset that actually jumps over binlog history is durably stamped as `gap_lost` (stamp-first, then advance — the #402 invariant) so `status` and `--fail-on-gap` keep reporting the permanent loss instead of a clean slate.
- **A permanent-loss record can no longer vanish on a missing checkpoint row** (#1085, closes #1081): the `gap_lost` stamp is now an upsert. Previously, if `stream_state` was empty at stamp time (operator `DELETE`, a concurrent reset), the bare `UPDATE` matched zero rows and the next checkpoint write re-created the row with NULL loss columns — the checkpoint advanced past permanently lost events with no durable record.
- **`bintrail-pg reset` preserves continuity-loss records** (#1086, closes #1082): the PG sibling of #1080 — reset no longer `DELETE`s `stream_state` (which erased any recorded `gap_lost`), it clears the checkpoint fields while carrying the loss record forward.

## [0.43.0] - 2026-07-23

### Added
- **Web-console usage telemetry** (metadata-only, opt-out). When the console runs inside a reporting `watch` daemon, a **deliberate** UI action — `console-recover`, `console-recover-cascade`, `console-reconstruct` (time travel), `console-verify`, `console-baseline` — records one usage event so we can see which console features are used, the same signal a CLI command gives. Passive browsing (listing events, paging, status polling) records nothing. The action name is a **compile-time constant** on the route (never derived from the request), so no schema, table, primary key, or row value can reach the wire. Because the console lives in a months-long daemon holding a single `run_id`, these events are recorded **without any `run_id`** (like daemon beacons, via the new `Client.RecordDaemonCommand`), so console usage can never be stitched into a per-install timeline — they are day-granularity counts. No JavaScript beacon; the read-only `bintrail-console serve` wires no telemetry client and records no console actions (it still emits its own process-level `serve` command event, like any command). Honors every existing opt-out.

## [0.42.0] - 2026-07-22

### Added
- **Turn usage telemetry off from the web console** (#1577). The Storage page (under `bintrail-console watch`) grows a **Usage telemetry** card showing the current state and a one-click opt-out. Turning it off both persists the machine-wide choice to the consent file (honored by every bintrail process from its next run) **and** stops the running daemon's beacons immediately — no restart — via a new live-consent toggle on the telemetry client (`SetRuntimeConsent`, an atomically-flipped decision). When a higher-precedence control (`DO_NOT_TRACK`, `BINTRAIL_TELEMETRY`, or `--telemetry`) is in charge, the card explains that and leaves the toggle to that control. Writing the consent file is local machine config, not a data write, so the console's read-only-over-data boundary is unchanged. New `GET`/`POST /api/telemetry`.

### Changed
- **Usage telemetry is now active in official release builds** (#1577). The ingestion endpoint (`internal/telemetry.endpoint`) is set via `-ldflags` for the `bintrail`, `bintrail-console`, and `bintrail-pg` release binaries — `bintrail-mcp` stays uninstrumented. Delivery goes to `https://telemetry.dbtrail.com`, a metadata-only ingestion host separate from the authenticated API: the request carries no credential and no account identifier, source IPs are stripped at the edge before any storage, and nothing beyond the closed 13-field allowlist (documented in `TELEMETRY.md`) is ever transmitted. A plain `go build` and the entire test suite remain inert (`telemetry status` → `not compiled in`). Opt out at any time with `bintrail telemetry off`, `DO_NOT_TRACK=1`, `BINTRAIL_TELEMETRY=off`, or `--telemetry=off`; run `bintrail telemetry show` to print the exact bytes that would be sent.

## [0.41.0] - 2026-07-21

### Added
- **Opt-out usage telemetry — metadata-only, spool-based, off in one line** (#1054 epic; #1063/#1064/#1066/#1067/#1068/#1069): official release builds report coarse usage statistics — which command ran, `ok`/`error` with a bounded error class, a duration bucket, and minor-truncated version + OS/arch — to help prioritize the roadmap and catch reliability regressions. The wire payload is a **closed 13-field allowlist** (`internal/telemetry`); it never carries rows, schemas, table/column names, DSNs, hostnames, IPs, file paths, GTIDs, binlog positions, flag values, arguments, error strings, or any persistent identifier. `run_id` is ephemeral (per-process, absent from daemon beacons). Disable with `bintrail telemetry off`, `DO_NOT_TRACK=1`, `BINTRAIL_TELEMETRY=off`, or `--telemetry=off` (first that applies wins; `telemetry status` reports which did). New `bintrail telemetry {status,show,on,off}` on all three binaries — `show` prints the exact JSON that would be sent and sends nothing; `off` also discards anything already spooled locally. **No command ever touches the network on its request path**: events append to a local NDJSON spool (`~/.config/bintrail/telemetry-spool/`, `0600`) and are delivered by a later run, drop-on-fail, capped 5 MB/day and 7-day retention, multi-process-safe via claim-by-rename. Long-running daemons (`stream`/`agent`/`up`/`watch`) send at most one beacon per UTC day (first only after an hour, so a crash loop never beacons), off the replication path, disclosed with one WARN line at startup. CI detection and a missing home directory suppress reporting but can never enable it.
- **Telemetry is inert unless a build is official.** The ingestion endpoint is injected via `-ldflags` and empty by default, so a plain `go build` — and the entire test suite, including the E2E binary — produces a binary with no network path at all; `telemetry status` reports `not compiled in`. This is the one-line assertion a distribution packager can check.
- **Demo image never reports** (#1063): `ghcr.io/dbtrail/bintrail-demo` hard-disables telemetry in the image and its entrypoint, asserted by the smoke test against every process in the container. Landed before any send code existed.
- **CI trust guards** (#1061): `internal/telemetry` is proven to link **no** other package in this repository (so it cannot reach the code that knows DSNs, rows, or server identity — a structural guarantee the field allowlist alone cannot give); the request builder is proven to carry no credential; the single root hook is proven un-shadowable on all three binaries; and the MCP server is proven unable to link the telemetry package at all.
- **`TELEMETRY.md`** (#1062): every field with an example and rationale, the complete never-sent list, spool/delivery mechanics, control precedence, per-surface matrix, cloned-image hazard, ingestion commitments, GDPR Art 13 elements, and the explicit non-goal that telemetry is architecturally incapable of sales or lead-gen use. Anti-drift tests keep the document equal to the wire format.

### Changed
- **README/PRIVACY privacy wording corrected** (#1069): the README no longer claims "no telemetry, no analytics, no phone-home" (untrue of release builds once #1054 shipped) — it now discloses metadata-only telemetry and the one-line opt-out, and links `TELEMETRY.md`. `PRIVACY.md` clarifies that its "collects nothing" statement is scoped to the Claude Desktop extension, whose MCP bridge is structurally telemetry-free.

**Note:** the telemetry ingestion endpoint is intentionally left unset even in this release, so v0.41.0 binaries **collect and send nothing**. Enabling collection is a separate, later release decision.

## [0.40.0] - 2026-07-18

### Added
- **UI-managed MCP access token — generate, rotate, and revoke from Settings → Connect AI** (#1053, closes #1052): a fresh install now needs zero out-of-UI configuration to connect an AI client. The Connect AI page grows an **Access token** card: **Generate** mints a cryptographically random `bmt_`-prefixed token from an authenticated browser session and shows the plaintext exactly once (it is never stored — only its SHA-256 is persisted, in `~/.config/bintrail/console-mcp-token.yaml`, `0600`, atomic write, versioned envelope with the registry's read-only-if-newer contract); **Rotate**/**Revoke** apply immediately, with no restart, including to sibling console processes sharing the file (every check re-reads it, so a revoke that reports success actually revokes everywhere). The managed token is **scoped to `/mcp` alone** — it authenticates the read-only MCP tools and nothing else (not the browser API, not server registry CRUD, not its own rotation), so handing it to an AI client grants exactly what the UI says it grants. `--token` / `BINTRAIL_CONSOLE_TOKEN` keep working unchanged and are reported as environment-owned; the flashback MySQL-protocol port still requires the static token (a hash-only store cannot drive `mysql_native_password`). New `GET/POST/DELETE /api/mcp-token` endpoints (values never serialized; the plaintext appears only in the generate response). A corrupt or newer-version token file never blocks daemon startup — the managed token degrades with a loud log and the Generate button self-heals malformed files, while unreadable ones are refused rather than destroyed.



### Added
- **Privacy policy** (#1048): `PRIVACY.md` documents the Claude Desktop extension's data handling — the bundle is a local bridge that connects only to the user-configured endpoint, the token lives in the OS credential store, and the project collects nothing (no telemetry). Linked from the README and declared in the `.mcpb` manifest (`privacy_policies`), as required for Claude Connectors Directory listing of local extensions. `.mcpb` release assets now cover macOS too (`dbtrail-darwin-{arm64,amd64}.mcpb`, built natively per release since v0.39.0's post-release assets).

## [0.39.0] - 2026-07-17

### Added
- **The web console now serves MCP at `/mcp`** (#1042, epic #1038): the four read-only tools (`query`, `recover`, `status`, `list_schema_changes`) over Streamable HTTP, authenticated with the console's static access token (`Authorization: Bearer`, constant-time compare; the endpoint refuses with an actionable error when no token is configured — password login is a browser credential and cannot authenticate an MCP client). Per-server routing by URL path: `/mcp` targets the console's default server, `/mcp/{id-or-name}` a specific registry entry (`default` = the command-line entry). The endpoint carries the console's read boundary — the same result caps as the console API (1,000 events / 10,000 recover statements), the per-server no-archive posture, RBAC-aware redaction of captured statement text, and rejection of the `index_dsn`/`profile` tool parameters (connection routing and the RBAC posture belong to the console, not the caller). Works on standalone `serve` and on `watch`. The tool handlers moved to a shared internal package (`internal/mcptools`); the standalone `bintrail-mcp` binary's behavior is unchanged.
- **`bintrail-mcp --connect <url> [--token <t>]`: a stdio↔Streamable-HTTP bridge** (#1044). The process runs as a local stdio MCP server — what Claude Desktop launches — and forwards every request to a remote bintrail MCP endpoint (the console `/mcp`, or a `bintrail-mcp --http` server). The remote's tools are mirrored verbatim (schemas, descriptions, annotations) and re-synced on `tools/list_changed`, so the bridge never drifts from the server it fronts; the token is scoped to the configured endpoint's host so a cross-host redirect never receives it. Unreachable endpoints and rejected tokens produce a fast, one-line non-zero exit (surfaced in Claude Desktop's MCP logs) instead of a silent hang.
- **`.mcpb` bundles in releases: one-click Claude Desktop install** (#1044). Each release publishes MCP Bundle artifacts (`dbtrail-<os>-<arch>.mcpb`) that Claude Desktop installs on double-click, prompting for exactly two values — the console/MCP endpoint URL and the access token (stored as a sensitive value) — and running the bundled `bintrail-mcp` in bridge mode. No JSON editing, no DSN on the client. Bundles cover the release build matrix (Linux amd64/arm64); `make mcpb` builds a host-native bundle on other platforms.
- **Console "Connect AI" settings page** (#1046): assembles the MCP hookup for the selected server — the ready-to-copy `/mcp` URL (per-server form when several servers are registered), the `.mcpb` bundle download matching the running version (releases-page fallback on unversioned builds), and a raw-config snippet for other stdio MCP clients. Availability keys on a new `capabilities.mcp` flag (static token configured); without a token the page explains how to enable it instead of showing a URL that would only answer 403. The token value is never rendered. The page is available on both `serve` and `watch`; Storage/Rotation stay watch-only.

### Documentation
- New [Connect an AI assistant](docs/connect-ai.md) guide (#1047): the console-first 5-minute walkthrough — token, Connect AI page, bundle install — with a symptom-first troubleshooting table and the MCP surface's security model. README and the MCP reference lead with it.

## [0.38.0] - 2026-07-17

### Added
- **Extension source jobs (`ext.RunSourceJobs`) now run under `bintrail-console watch` and its per-source monitor supervisor**, not only under `bintrail up`. A registered `ext.RegisterSourceJob` now fires for `watch`'s main `--source-dsn` stream and for every source the console control plane starts monitoring, each bound to that source's lifecycle context — so a job stops when its source stops (a monitored source's Stop, daemon shutdown, or the supervised stream's own terminal exit) and starts once per (re)start, never per stream reconnect. The monitor binds the job's lifetime to the source's advisory lock: when a stream gives up or exits, its jobs are cancelled together with the lock's release, so a second daemon that re-acquires the freed lock never double-runs them. A console-monitored MariaDB source now also captures with the MariaDB GTID parser and reports the matching flavor to its source jobs. No behavior change for the stock binary: `RunSourceJobs` is a no-op with no registered jobs.

## [0.37.0] - 2026-07-17

### Fixed
- A source-only console extension view (one whose data endpoints read only the live source, never the index) now stays usable while the selected server's index is unreachable or not yet created. `ext.ConsoleQueryContext` resolves the source DSN from the registry **without** opening the index, and on a failed index open with a source configured it hands the provider a usable context (`DB` nil, a `Fetch` that surfaces the index error, `SourceDSN` populated) instead of failing the whole request — so source-only investigation keeps working during an index outage. An index-backed view must nil-check `DB` or read through `Fetch`; the contract is documented on the type. With no source configured, a failed index open stays an error.

## [0.36.0] - 2026-07-17

### Added
- **`ext.ConsoleView`: a pluggable extension-view seam for the web console.** Embedding distributions — builds that construct their console binary from the importable `consoleapp` package — can inject one additional console view (a nav item, a frontend ES module, and an authenticated data API) with `ext.SetConsoleView`, same startup-only contract as `ext.SetConsoleAuth`: call once from `main()` before `consoleapp.Main`. The view's static assets mount **unauthenticated** at `/ext/<id>/` (code always ships, like the console's own `app.js`); its data routes mount at `/api/ext/<id>/` behind the console's bearer-token middleware and are refused with 403 while an RBAC access-control profile is active (the console cannot guarantee a third-party handler honors table-deny / column-redaction rules). The data handler receives a per-request, per-selected-server index context — the selected server's index connection, the console's own cross-source (live + archive, gap-aware, RBAC-applied) fetch pipeline, and the selected registry entry's source DSN. `/api/capabilities` advertises the installed view (`extension_views`), and the SPA reveals a nav item + `ext-<id>` route that dynamically imports the module and calls its `render(mount, {apiBase, api})`. The provider id is constrained to `^[a-z0-9-]+$` and validated at mount time — an invalid id is skipped (logged, not mounted) rather than producing a broken route. The stock binary has no view: no nav item, `extension_views` omitted, and `/ext/*` / `/api/ext/*` are absent from the router.

### Changed
- The shared DuckDB resource-tuning helpers (`--ultrafast` / `--duckdb-threads` / `--duckdb-memory-limit` flag registration, resolution, and the archive-fetcher adapter) moved from `internal/cli` into a dedicated leaf package `internal/duckdbtuning`, so the public read-plane facade (`indexquery`) exposes them without importing the command layer. `indexquery`'s public API is unchanged (`AddDuckDBTuningFlags` / `DuckDBTuningFromFlags` / `TunedArchiveFetcher` remain, repointed); `internal/cli` keeps thin forwarders so its command layer is untouched.

## [0.35.0] - 2026-07-17

### Added
- **`consoleapp`: the bintrail-console command layer is now an importable package** (#882). `cmd/bintrail-console` is a thin `main()` over `consoleapp.Main(version, commitSHA, buildDate)` — the console sibling of the `cliapp` seam. Embedding distributions build their own console binary from `consoleapp` and install `ext` seams from `main()` before calling `Main`; without an importable entrypoint the console-side seams would be unreachable in every external build (`cliapp` deliberately does not link the web console). ldflags, Makefile, and packaging are unchanged (`main.Version` et al. stay on the thin main).
- **`ext.ConsoleAuth`: a pluggable external-auth provider seam for the web console** (#882). Embedding distributions — builds that construct their console binary from the importable `consoleapp` package — can install an external login flow (e.g. OIDC single sign-on) with `ext.SetConsoleAuth`, same startup-only contract as `ext.SetAuditSink`: call once from `main()` before `consoleapp.Main`. The console mounts the provider's handler unauthenticated at `/api/auth/ext/` (behind the host guard and security headers; the provider owns its own CSRF/state protection), advertises it in `GET /api/auth` (`sso_name`/`sso_start`), and the sign-in screen shows a "Continue with \<name\>" entry. A successful external login mints the same in-memory session a password login does (same TTLs, logout, and revocation; it cannot claim the first console password — that stays with the static token), and an installed provider counts as a valid sole credential for a non-loopback bind. The stock binary has no provider: the probe fields are absent, the button never appears, and `/api/auth/ext/*` stays 401 behind the credential middleware.

## [0.34.0] - 2026-07-16

### Added
- **Extension seams for embedding distributions** (#1029): the exported `indexquery` package (a read-plane facade over the index — query/merge/format/connect/schema helpers), `cliapp.AddCommands` (register extra top-level commands before `cliapp.Main`), and the `ext` registries `ext.RegisterSourceJob` (daemon-scoped source jobs, run at `up`'s wiring point), `ext.RegisterDoctorCheck` (extra preflight checks appended to `doctor` and `up`'s preflight), and `ext.RegisterAgentCommand` (agent WebSocket commands consulted for non-builtin types). All no-ops in the stock binary; same startup-only contract as the existing `ext` setters.

### Removed
- **The who-changed attribution surface is retired from the core distribution.** Removed: the `who-changed`, `user-activity`, and `connection-history` CLI commands; the console's Forensics view and its `/api/forensics/*` endpoints (and the `forensics` capability flag); the agent's `forensics_capabilities`/`forensics_enrich`/`forensics_activity`/`forensics_users`/`forensics_audit_log` WebSocket commands (those types now route to the `ext.RegisterAgentCommand` registry; unregistered they fail as unknown — the legacy `forensics_query` diagnostics command is unaffected); the doctor's `performance_schema (forensics)` and `Audit log plugin (forensics)` checks; the `connection_cache` session-identity poller, its table DDL, and the `--attribution-retention` flag / `BINTRAIL_ATTRIBUTION_RETENTION` env var; and `ext.SetForensicsEnabled` (the gate it set no longer exists). Existing installs keep any `connection_cache` table; the core no longer reads or writes it. **What stays in core:** the captured `query_text`/`query_hash`/`connection_id` columns and their RBAC redaction, the `Statement capture (query_text)` doctor check, and faithful indexing of all three fields. Embedding distributions provide the attribution surface through the seams above; it is available in the commercial distribution (dbtrail EE).

## [0.33.0] - 2026-07-14

### Added
- **Console PostgreSQL control-plane parity — monitor, baseline, and verify** (#1018). A PostgreSQL source added in the console can now be **monitored, baselined, and verified** from the console, exactly like a MySQL source — the write control-plane reaches capability parity with the read/time-travel side that already shipped (#595/#593). Parity is *capability* parity, not identical UI: PG capture uses a replication slot + publication where MySQL uses binlog/GTID, so the +Add form and capture path differ by construction (the form reveals PG-only slot/publication/database fields via the same class-toggle used for capability gating). The registry gains a generic source `flavor` (`mysql`|`mariadb`|`postgres`, additive — every existing entry keeps working as `mysql`) plus PG-only slot/publication fields; the monitor supervisor drives PG capture (`pgstreamrun` + PG doctor) under the same circuit-breaker/lifecycle as MySQL; the baseline trigger produces PG baselines in-process (COPY straight to Parquet, LSN-anchored — no mydumper subprocess); and "Run verification" reaches a real MATCH/MISMATCH for PG (baseline-anchored) instead of the previous misleading inconclusive. The core MySQL binary stays free of the PostgreSQL stack — the console's PG DSN logic is stdlib-only (`net/url`, no pgx) and only `bintrail-console` links the capture stack. **Deferred parity edge:** live-source verify (`verify --source-dsn`) stays MySQL-only (its consistency checksum is MySQL SQL); a PostgreSQL source is cleanly refused at the API, CLI, and engine layers with an actionable message pointing to the baseline-anchored path (the drift-free default and the full-parity path), rather than the old misleading inconclusive. Covered end-to-end in the PostgreSQL 14–17 CI matrix.
- **`bintrail-pg` index-rotation plane: a built-in rotation loop plus `rotate` / `archive reconcile` commands** (#951). A PostgreSQL-only install (`bintrail-pg`) previously had no index maintenance — it ran only the read plane and did a one-time 48-partition bootstrap with no rotation, so after ~2 days every event piled into the un-droppable `p_future` partition and the index grew without bound (the disk-full failure mode closed long ago for MySQL). `bintrail-pg stream` now runs the same built-in rotation loop as core `up` (safe-by-default `30d`, drop-only; `--rotate-*` flags, `off` to opt out), and `rotate` + `archive reconcile` (including the archive-then-drop `--archive-s3` path) move into a shared index-side command set registered on **both** `bintrail` and `bintrail-pg` — so a PostgreSQL operator can rotate and archive without the core MySQL binary. **Upgrade note:** an existing beta install that has already accumulated a large `p_future` keeps its full history until you choose a retention explicitly (the loop arms its drop guard only when `--rotate-retain` is set), so no history is dropped by surprise on upgrade.

### Changed
- **`--ssl-mode` now governs the index and source connections too, fail-secure** (#946, #947). Previously `--ssl-mode` encrypted only the binlog replication syncer — the **index write** connection (full before/after row images, i.e. your data, plus the index credentials) and the source helper connection went **cleartext even under `--ssl-mode=required`**. One `--ssl-mode` knob now governs all three stream-daemon connections with identical CA/verify/mTLS semantics: `required`/`verify-*` **fail closed** on a TLS-incapable server (with an actionable hint), and `preferred` does an explicit try-TLS → warn → retry-plaintext so every connection that might send data in the clear warns identically. The `preferred` downgrade is also narrowed to a genuine "server does not support TLS" condition — auth-denied, unreachable, and bad-position errors now propagate instead of silently retrying in cleartext. An explicit `tls=` in a DSN still wins as an operator override but now warns when it weakens a mandatory `--ssl-mode`. **Upgrade/compat note:** a deployment running `--ssl-mode=required`/`verify-*` against an index or source with TLS *disabled* now fails to connect on upgrade instead of silently going cleartext — this surfaces the exact misconfiguration `required` exists to prevent (MySQL 8.x ships TLS on by default, so the real blast radius is small). Under the default `preferred`, index connections that were always plaintext are now TLS-encrypted whenever the server supports it. Scope boundary: this covers `watch`'s embedded stream; the console's own multi-server index *reads* and the embedded flashback port still use the DSN's own `tls=` (documented in `docs/console.md`).

### Fixed
- **A stalled index write no longer freezes the streaming daemon invisibly; the capacity check is honest when disk-free is unmeasurable** (#948, #959). `config.Connect` set only a connect timeout, so a mid-statement network stall on the index link blocked on kernel TCP retransmission (~13–16 min) while `watch` still reported a healthy stream. Every index-side write on the streaming hot path — batch INSERT, the statement-digest phase, DDL markers, and the PostgreSQL checkpoint/health/snapshot writes — is now bounded by a per-statement deadline (`--write-timeout`, default `3m`), with a `DeadlineExceeded` hint that points at the knob (raise `--write-timeout` / lower `--batch-size`) instead of a phantom network fault; a non-positive timeout is rejected before any connection opens. The PostgreSQL durability invariant is preserved: a timed-out checkpoint stops the loop *before* acking the commit, so WAL is never released past an un-checkpointed position. Separately, `doctor`'s disk-capacity check now reports `StatusSkip` ("not measured", exit-neutral) instead of a false `Pass` when the index runs on a separate host/container whose volume can't be measured locally — a genuinely full *local* disk still FAILs.
- **Baseline no longer silently corrupts `--hex-blob` binary columns; GEOMETRY maps to bytes** (#503). A mydumper/mysqldump `--hex-blob` dump renders binary columns as `0x<hex>`; the type-blind baseline reader stored the literal ASCII (e.g. the 10 bytes `"0x612C6229"`) into the binary column instead of the 4 real bytes — a baseline holding the wrong value, so recovery/time-travel from it returned garbage (reproduced live against `main` before the fix). `convertValue` now decodes `0x<hex>` → bytes **for binary-family columns only** (a non-`--hex-blob` dump emits binary as `_binary "…"`, never bare `0x…`, and a non-binary `0x10` stays the integer 16), and the reader strips the introducer from the `_binary 0x<hex>` form. GEOMETRY/spatial types now map to a **binary** Parquet leaf (not the UTF-8 STRING default that would mangle WKB bytes) and route through the same decode path. *Residual:* an odd-length/invalid `0x…`, or an ASCII `"0x…"` dumped without `--hex-blob`, falls back verbatim rather than failing loud (distinguishing them needs reader provenance the digest tap can't carry) — both strictly better than the prior never-decode behavior; the exact spatial dump encoding is not verified end-to-end (mydumper is unavailable in the unit env), so the binary mapping is the safe floor.

## [0.32.0] - 2026-07-12

### Added
- **`bintrail-pg flashback`: single-row `AS OF` time-travel over the PostgreSQL wire protocol** (#1008). A PostgreSQL operator can now run interactive time-travel from `psql` (or any PostgreSQL driver) instead of the MySQL-wire shim — `psql "host=127.0.0.1 port=5433 user=<tenant> dbname=<schema>"` then `SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42` (or `_snapshot.` for the baseline-aware view, or the bare `SELECT * FROM orders AS OF '…' WHERE id = …` form). It serves the same out-of-band index and baseline the MySQL shim uses, so the original WAL/binlog files are never needed. The time-travel engine was already source-agnostic, so this reuses the shim parser and the fetch/reconstruct pipeline verbatim through a new **wire-neutral resolve seam** shared by both front-ends; the MySQL renderer's wire behavior (error codes and messages) is byte-identical after the refactor. Connect with `dbname` set to your PostgreSQL schema (e.g. `public`) and select flashback-vs-snapshot with the `_flashback.` / `_snapshot.` table prefix, exactly like the MySQL shim. Scope tracks PostgreSQL's current time-travel maturity: **single-row `AS OF` only** (a `WHERE <primary-key> = <value>` predicate) — full-table `AS OF` and `_diff` are refused with actionable remediation, never a silent partial (a PostgreSQL baseline still omits the `CREATE TABLE` metadata full-table reconstruct needs). Columns render as text; use the simple query protocol (`psql` does this natively, a `pgx` client sets `QueryExecModeSimpleProtocol` — the extended/prepared-statement protocol is declined with a resync, not a hang), and a `bytea` value containing a `0x00` byte truncates in `psql`/libpq but round-trips intact via a raw-bytes driver read. Auth mirrors the shim (a cleartext password per tenant from `--shim-config`), loopback-default; front a TLS terminator for a non-loopback bind. The PostgreSQL wire code (`internal/pgshim`, built on `github.com/jackc/pgx/v5/pgproto3` — already in the module graph, so no new dependency) is linked only by `bintrail-pg`, keeping the core MySQL binary free of the PostgreSQL stack; standalone `bintrail-pg shim` (MySQL wire) is untouched. Covered end-to-end in the PostgreSQL 14–17 CI matrix.

## [0.31.1] - 2026-07-12

### Fixed
- **Single-row `_snapshot` now folds the baseline for PostgreSQL sources** (#1006). The shim's single-row `_snapshot` — and the embedded `bintrail-console watch --flashback-listen` port (#996), which routes through the same code — silently degraded to the binlog-only `_flashback` path for **every** PostgreSQL source: the PK-type gate rejected PG's empty `schema_snapshots.data_type` (PG records `pg_type_oid`, not a MySQL type token), so a row present in a baseline but never touched within the retained binlog window resolved to zero rows and the baseline fold never ran. The gate now bypasses for a confirmed `postgres` source flavor, mirroring the offline `reconstruct` path — PG baselines store raw pgoutput text, so `ReadBaselineRow`'s bound `pkColumn = ?` is a string-identity match that can only recover the right row or find nothing, never a wrong row. When a PG source's flavor cannot be confirmed, the degrade log now names that as the cause instead of blaming the (empty) PK type. CLI `reconstruct` and the console `/api/reconstruct` endpoint were unaffected; full-table `_snapshot` for PG continues to fail loud (out of GA scope). Validated end-to-end against RDS PostgreSQL 16.14.

## [0.31.0] - 2026-07-11

### Added
- **`bintrail-console watch --flashback-listen <addr>`: one embedded MySQL-protocol time-travel port for every monitored server** (#996). `watch` can now serve the `_flashback` / `_snapshot` / `_diff` virtual schemas for every console-monitored server over a single MySQL-protocol port, routed by the connection username (registry id / display name / the `default` boot entry). This collapses the previous manual setup — discover each source's per-source index DB, build its `INDEX_DSN`, run a separate `bintrail shim` container per source — to zero: it reuses the console's `connManager`, which already resolves each server's per-source index and baseline. Auth is the console `--token` (the bcrypt password store can't drive native-password auth), so the port refuses to start without one; username validity is checked post-handshake so the error code can't enumerate servers. Enabled via `--flashback-listen` / `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` (default off); standalone `bintrail shim` is untouched, and there is no open-core surface change (the shim emits source-table rows only — no `connection_id` / `query_text`).
- **`LIMIT <n>` on `_flashback` / `_snapshot` `AS OF` queries** (#997). The time-travel grammar accepts an optional trailing `LIMIT <n>`, composing with a column list and a `WHERE` clause. Crucially for full-table `AS OF`: a `LIMIT` at or below the row cap lets the query **succeed** instead of tripping `ER_TOO_BIG_SELECT` — the "add a `LIMIT` to browse" remedy the cap error now suggests — while a `LIMIT` never *raises* the cap. Rows come back in the merge's order (not sorted — add your own `ORDER BY` downstream), and a `LIMIT n` can return fewer than `n` rows when some of the first `n` were deleted by the `AS OF` instant. `LIMIT 0`, negative, and out-of-range values are rejected as parse errors; `_diff` is out of scope.

### Changed
- **Full-table `_snapshot` `AS OF` (no `LIMIT`) now streams its resultset row-by-row instead of buffering it, lifting the 100,000-row cap** (#998). The baseline flows through the DuckDB merge cursor one row at a time, so peak shim memory is proportional to the rows *changed* since the baseline, not the table size — a multi-million-row table dumps without `ER_TOO_BIG_SELECT` or unbounded memory growth. A mid-stream merge failure surfaces as an error packet in place of the next row (no clean end-of-result), so a failed dump is never mistaken for a complete one. Streaming is used by both `bintrail shim` and the `watch --flashback-listen` port; the binlog-only full-table `_flashback` path and any `LIMIT`ed query keep the buffered cap (their fetch buffers regardless). One documented trade-off: streaming `SELECT *` fixes its column set from the table's current schema before the first row, so a column dropped between `AS OF` and now is not surfaced — the shim logs a warning naming it, and adding a `LIMIT` forces the buffered path that surfaces it.

### Fixed
- **Single-row `_flashback` point-lookups now cut at the transaction boundary, not the individual row** (#988). `event_timestamp` is a statement's execution time, not its transaction's commit time, so a `_flashback` lookup could surface a change from a multi-statement transaction that had not fully committed as of the `AS OF` instant — a row state that never existed. The single-row `_flashback` path now folds the PK's surviving events with `reconstruct.ApplyAt` after excluding a straddling trailing transaction whole (the same #783 machinery the single-row `_snapshot` path already used), instead of taking the latest event verbatim. It also fixes an empty-string-PK edge (`WHERE name = ''` against a NOT-NULL string PK) that could otherwise fold the whole table onto one state. The full-table `AS OF` paths still cut per row (tracked separately).

### Documentation
- **Honesty sweep across the source-support, managed-option, index-backup, and command-table docs** (#995). Six documentation edits aligning the docs with the code, no code changes: the README no longer over-claims equal support across managed MySQL flavors (RDS/Aurora read as verified, Cloud SQL for MySQL as expected-to-work) and adds a low-key pointer to the managed option and the ship-vs-operate boundary in `SUPPORT.md`; `install.md` notes MariaDB (alpha) and PostgreSQL (beta, via `bintrail-pg`) as alternative sources and adds the 7 shipped commands missing from the command table; the quickstart flags that its single-host example co-locates the index on the source; and `docker.md` gains a runnable backup/restore recipe for the bundled two-volume index.

## [0.30.0] - 2026-07-10

### Added
- **PostgreSQL rendering GUCs pinned on capture and baseline sessions** (#593 slice D). The baseline+delta reconstruct design rests on the identity *baseline COPY text ≡ pgoutput delta text* — the PK join and last-write-wins merge are exact string operations — but both sides render values through PostgreSQL's session-GUC-dependent output functions, and neither session pinned any GUC: the same logical value could render as different text on the two sides, silently breaking the merge for `timestamptz`/`timestamp`/`date`/`time`, `real`/`double precision`, `bytea`, and `interval`. Every connection that renders row text — the logical-decoding (walsender) session and the baseline COPY connections — is now pinned via startup parameters to `TimeZone=UTC`, `DateStyle=ISO`, `extra_float_digits=3`, `bytea_output=hex`, `IntervalStyle=postgres`; the pinned set is stamped into the baseline Parquet metadata (`bintrail.render_gucs`), and single-row `reconstruct` warns when it reads a pre-pin baseline. The full type matrix is now validated **through the single-row reconstruct fold** under deliberately skewed session GUCs on the PG 14–17 CI matrix (`TestOne_PGTypeMatrixThroughReconstructFold`); the single-row PG beta warning narrows to container types only.

  **Upgrade note:** on servers whose defaults differ from the pinned values, baselines taken **before** this release were rendered under the unpinned session defaults and their GUC-sensitive text will not text-join deltas captured **after** the upgrade (worst case, a `timestamptz`/`float` value silently stops matching in the fold). Re-run `bintrail-pg baseline` after upgrading. Events indexed before the upgrade keep their original rendering and cannot be re-rendered; a fold window spanning the upgrade on such a server can mix renderings.
- **`bintrail doctor` now gives an RDS/Aurora-aware binlog-retention verdict and a replication server-id collision check** (#812, #819). `checkBinlogRetention` read only `@@binlog_expire_logs_seconds`/`@@expire_logs_days`, which on RDS/Aurora can report a green `PASS '720h'` while `mysql.rds_configuration` `'binlog retention hours'` (NULL default) lets RDS purge the binlogs the stream needs after a restart; the check now probes that row first and WARNs with the `CALL mysql.rds_set_configuration('binlog retention hours', 48)` remediation on NULL or `< 48h`, falling through byte-for-byte to the old probe when the table/row is absent (self-managed MySQL). The new `checkServerIDCollision` derives the server-id from `--source-dsn` (deterministic sha256 of `host|user|dbname`), WARNs when it equals the source's `@@server_id` (which would flap/reject the replication connection), and notes that any bintrail instance on the same `--source-dsn` derives the same id. Both are advisory (WARN, never FAIL).
- **`bintrail doctor --proxysql-admin '<admin-dsn>'`: opt-in check that all six dbtrail ProxySQL routing rules (990001-990006) are present and active in `runtime_mysql_query_rules`** (#820). The `SELECT /*+ DBTRAIL_AT='...' */ ...` hint form of a time-travel query is valid vanilla MySQL, so on a routing miss (client on the direct MySQL port, rules lost after a ProxySQL restart without `SAVE MYSQL QUERY RULES TO DISK`, or a lower `rule_id` intercepting first) MySQL runs it against the live table and returns present-day data with no error — unlike the `_flashback.*` (1049) and bare `AS OF` (1064) forms, which fail loud. The new check is advisory by contract (every outcome short of all-six-live is a `WARN`, never `FAIL`, so the exit code is unaffected) and is off unless the flag is passed; `docs/time-travel-sql.md` now warns that the hint form is the only silently-degrading shape and recommends the fail-loud forms whenever the client can emit them.
- **PostgreSQL `TRUNCATE` is now recorded as a `schema_changes` audit marker** (#827). The pgcapture decoder previously dropped the `TruncateMessage` with only a warning, so nothing landed in the index — `list_schema_changes` showed no trace (indistinguishable from "nothing happened") and a `reconstruct` crossing the truncate would silently resurrect the pre-truncate rows. A `TruncateMessage` now maps to one `EventDDL(DDLTruncateTable)` marker per in-scope relation (stamped with the commit LSN/time and a synthetic `TRUNCATE TABLE schema.table` query), the source-neutral analog of the MySQL DDL path, surfaced by `list_schema_changes`. This is a durable record that the truncate happened, not a replayable change: `recover` has no per-row before-images and `reconstruct` does not yet consume the marker (a follow-up).
- **`bintrail-console watch` source streams now honor configurable TLS** (#879). The `watch` daemon's own stream and every console-managed ("+ Add server") source connected to the source MySQL with `ssl-mode=preferred` hardcoded — no certificate verification, silent plaintext fallback, no override — even though `bintrail stream`/`up` already expose the full `--ssl-*` set. `watch` now accepts `--ssl-mode`/`--ssl-ca`/`--ssl-cert`/`--ssl-key` (and `BINTRAIL_SSL_MODE`/`_CA`/`_CERT`/`_KEY`), and console server registry entries carry typed `ssl_mode`/`ssl_ca`/`ssl_cert`/`ssl_key` fields threaded into each supervised per-source stream. Additive: unset still defaults to `preferred`, and pre-existing registry files round-trip the new fields, so a plain UI edit no longer wipes a hand-configured verify-ca / mutual-TLS source connection.

### Changed
- **MariaDB 11.4 source fidelity is now covered end-to-end by a corruption-fidelity sweep** (#620). A new test matrix walks the full `source → index → recover` chain against a live MariaDB 11.4 source for UNSIGNED sign-bit, BIT(64), SET(64), JSON (stored as LONGTEXT on MariaDB, not native binary JSON), DECIMAL/DOUBLE precision, VARBINARY with embedded NUL, DATETIME(6)/TIMESTAMP microseconds, and charset pinning — closing gaps that previously had MySQL-only or no coverage. Every column value round-trips byte-identical; the one divergence found (binlog position tracking, not value corruption) was filed separately as #986.
- **The `shim` daemon now cancels in-flight time-travel queries when the client disconnects, enforces a per-query deadline, and caps concurrency** (#823). The MySQL protocol is strictly request/response, so a client that timed out and closed its socket (the classic 30s-ORM-retry loop) went unnoticed until the fetch completed, stacking orphaned 100k-row full-table merges and DuckDB/S3 fetches until the daemon OOMed or saturated the index DB shared with the streamer. Three bounds were added, all surfaced as typed MySQL wire errors: a dedicated socket-read pump observes a FIN/RST mid-query and aborts the in-flight `FetchMerged` immediately (and closes idle connections on SIGTERM so graceful shutdown no longer blocks); `--query-timeout` (default `5m`, `0` disables; env `BINTRAIL_SHIM_QUERY_TIMEOUT`) maps expiry to a `1317` `ER_QUERY_INTERRUPTED`; and `--max-connections` (default `100`, `0` unlimited; env `BINTRAIL_SHIM_MAX_CONNECTIONS`) plus a full-table concurrency cap refuse over-cap work with the same `1040` "Too many connections" ERR packet a real mysqld sends.
- **Single-row `reconstruct` (and the shim's `_snapshot`) over a PostgreSQL source now prints an honest beta warning at runtime** (#829). The path already ran generically for PG (`FindBaseline → ReadBaselineRow → FetchMerged → ApplyAt`/`BuildHistory`), but gave no signal that it is validated only for canonical, session-GUC-independent types (`int`/`numeric`/`bool`/`uuid`/`text`/`json`/`enum` and common PK types) and not yet proven end-to-end for GUC-sensitive and container types (`timestamptz`/`float`/`bytea`/`interval`/arrays). It now warns rather than refuses (a refusal would break the surface that does work); PG is detected via `SourceFlavor(db)=="postgres"` OR a non-zero baseline `LSN`. `--baseline-only` returns before the DB is opened and never warns.

### Fixed
- **MariaDB sources with `log_bin_compress=ON` now index their compressed row events instead of silently skipping them** (#520). `MARIADB_WRITE/UPDATE/DELETE_ROWS_COMPRESSED_EVENT_V1` events hit `handleRows`' warn-and-skip default arm, so their rows were never indexed — the last silent-data-loss item on the MariaDB beta gate. go-mysql already decompresses these into `*replication.RowsEvent`, so the fix adds the three types to the shared WRITE/DELETE/UPDATE dispatch used by both `bintrail index` and `bintrail stream`; all downstream guards (partial-image, column-count, schema-drift) apply unchanged. **Upgrade note:** row events skipped by prior versions on a `log_bin_compress=ON` source are not retroactively captured — re-index the affected binlogs to recover them.
- **`bintrail verify` no longer reports a false MISMATCH when an UPDATE carries unchanged ENUM/SET/BIT columns, and no longer masks a real divergence just because a table contains a deferred-typed column** (#769, #791). Under `row_image=FULL` an UPDATE's `row_after` carries every column; the event side rendered carried-but-unchanged ENUM/SET as ordinals and BIT as decimal text while both baselines carry the label string and raw `ceil(M/8)` bytes, so `UPDATE ... SET amount=amount+1` on a table with an ENUM produced a conclusive mismatch (#769). Conversely, the old live-mode gate downgraded every equal-row-count content difference to Inconclusive as soon as the table merely contained a deferred column, letting an in-place corruption of a plain INT/VARCHAR exit 0 (#791). Both modes now run `reconstruct.MapEventEnumLabels` and render BIT as MySQL's raw big-endian bytes, and a new `deferredReprUnresolved` gate fires only when a row image carries a deferred value the normalization passes provably could not resolve — so Inconclusive still beats a false MISMATCH, but a conclusive difference on non-deferred columns is no longer masked. Row-count differences remain conclusive.
- **`bintrail verify` (baseline-anchored default mode) no longer reports a false all-green when the snapshot contains tables that were never baselined** (#770). The verified table universe was derived solely from the baseline files on disk, so a table present in the schema snapshot but absent from the baselines was silently omitted from the report rather than flagged. `verify` now crosses the full snapshot table set against the baselines and reports each never-baselined table as `inconclusive` with an actionable reason.
- **`bintrail stream` in position mode no longer checkpoints a byte offset in the middle of a statement, ending a permanent crash-loop on resume** (#775). A statement larger than `binlog_row_event_max_size` (8 KiB default) is split by MySQL into several `ROWS_EVENT`s under one `TABLE_MAP`; the ticker checkpoint could persist an offset between two chunks. On crash and resume, `StartSync` from that mid-statement offset handed the live syncer a rows event with no preceding `TABLE_MAP` (`invalid table id, no corresponding table map event`), which closed the stream and restarted to the same checkpoint forever — escapable only via `--reset`, which discards events. The position checkpoint now advances only at a statement (`STMT_END_F`) / commit / DDL boundary, mirroring the commit-only GTID protection. GTID mode is unchanged.
- **Statement-format DML is no longer dropped silently during capture** (#776). `binlog_format` is validated once at startup against the global variable, but it is session-settable and `MIXED` falls back to `STATEMENT` for non-deterministic DML; under `STATEMENT`/`MIXED`, `INSERT`/`UPDATE`/`DELETE`/`REPLACE`/`LOAD DATA` are written as `QUERY_EVENT`s with no row image, and after `parseDDL` declined them both capture paths let them fall through with no log and no metric — a change invisible to the operator. Both the file parser and the stream parser now detect a DML keyword prefix and emit a loud `slog.Warn` plus a new Prometheus counter `bintrail_statement_dml_dropped_total`. The warn logs the keyword, file/pos, and `connection_id` only — never the statement text (which embeds row values); the stream is not aborted (warn + metric is the contract).
- **`bintrail index` no longer marks a file `completed` after silently skipping rows against a stale snapshot** (#778). In file mode the DDL auto-snapshot resolver swap runs consumer-side, so rows following an in-file `CREATE`/`ALTER TABLE` could decode against a stale resolver and be dropped as `table not in snapshot` or `column count mismatch`; `handleRows` returned `nil`, the file was marked `completed`, and re-indexing skipped it — a permanent, unmarked gap surfaced only as log warnings. A file-path-scoped guard now records such skips when the event is at-or-after the snapshot time and makes `ParseFile` return a hard error, so the file is marked `failed` and re-indexing after a fresh snapshot converges. Pre-snapshot events (historical re-index / stream backlog, no converging remediation) stay warn-only; the stream path is unchanged.
- **Rotation no longer drops a partition that grew after it was archived** (#779). `ArchivePartition` snapshots a partition, then uploads its Parquet to S3 — a possibly-minutes-long step — before `DROP PARTITION`; a backfilled gap replayed with original binlog timestamps lands in the oldest partition, so rows inserted after the archive `SELECT` and before the `DROP` could vanish from both the index and the archive. Before each drop, rotation now re-checks the partition's live `COUNT(*)` against the row count captured at archive time (`archive_state.row_count`); on any mismatch (or a missing/unverifiable count) it does not drop — it warns, increments `Deferred`, and discards the now-incomplete staged archive so `--retry` re-archives the full partition next cycle. Unchanged partitions drop exactly as before.
- **Position-mode stream resume now warns when a source rebuild is undetectable** (#780). `detectPositionGap` only flagged a regenerated binlog when the checkpoint position exceeded the file size, so a source rebuilt via `RESET MASTER` + restore whose same-named binlog regrew past the checkpoint offset passed the check and resumed reading a different binlog history silently (GTID mode is unaffected — the executed-set comparison diverges and detects the rebuild). The two position-mode resume-in-place outcomes now set a `RebuildUndetectable` flag, and `One` emits an escalated `slog.Warn` plus a printed banner recommending GTID mode (`--start-gtid`) or `--reset` if a rebuild is known. Non-blocking by design so legitimate position-mode resumes are not broken; the already-loud unfillable/purged and `pos > size` branches do not set the flag.
- **Full-table `reconstruct` (`--output-format mydumper`) now warns when the baseline anchor precedes the first indexed event by a gap** (#781). Single-row reconstruct already warned about this window (events in it are missing from the reconstruction), but the full-table path read the baseline binlog file/pos and never ran the check, producing a mydumper dump silently missing the gap with no signal. The check is now ported into `ReconstructTable`, preserving the same flavor-dependent comparison (MySQL/MariaDB two-key binlog, PostgreSQL numeric-LSN with lineage guard). It is warn-only and does not consult `--allow-gaps`; `schema`/`table` are attached so the warning is attributable when several tables reconstruct concurrently.
- **`bintrail reconstruct` now fails loud on a primary-key-changing `UPDATE` instead of silently producing a wrong dump** (#782). An `UPDATE` that changes a row's PK is stored keyed by its before-image PK, so folding it into the change map could resurrect a row a later `DELETE` removed (`UPDATE pk 1→2; DELETE pk=2`), duplicate a key into a `1062 Duplicate entry` that only surfaced at load time (`UPDATE pk 1→2; UPDATE pk=2`), or return a misleading `no row found in baseline` for single-row lookups — all silently. A new detector refuses up front at every full-table entry point (`reconstruct --output-format mydumper`, shim `_snapshot`, `verify`, and the no-baseline fallback) before any partial output is written, naming the table and the `old → new` key transition and pointing at re-running `bintrail baseline` at/after the PK change.
- **Single-row point-in-time reconstruction (`bintrail reconstruct`, the shim's `_snapshot`/`_flashback`, and the console's Time-travel) now cuts `--at` at the transaction boundary instead of half-applying a straddling transaction** (#783). `event_timestamp` is a row event's statement execution time, not its commit time, so a multi-statement transaction whose statements straddled `--at` was cut mid-transaction (`event_timestamp <= at`), producing a row state that never existed on the source — MySQL only makes a transaction's changes visible atomically at commit. Reconstruction now groups the trailing events by their indexed `gtid`, runs one bounded existence-check scoped to that exact GTID, and drops the entire trailing transaction if any of its events fall after `at` anywhere on the server, so it is never partially applied. Residual limitation, now documented in `docs/query-and-recovery.md`: the index stores `DATETIME(0)` (one-second resolution) with no true commit-time column, so sub-second ordering cannot be resolved.
- **`bintrail recover` no longer exits `0` after emitting a silently incomplete reversal** (#784). When any event could not be reversed — e.g. a `nil`/malformed stored `row_before`/`row_after` image — `recovery.GenerateSQLFromRows` demoted the failure to a `-- ERROR ...` SQL comment and kept going, so the remaining statements committed clean under the surrounding `BEGIN`/`COMMIT` and the CLI reported success. Generation now collects every un-generatable event and refuses the whole script before a single byte reaches the writer, returning a non-nil error that names each failed event and its reason (with `--output`, the target file is left empty); every caller (CLI, MCP, agent, console, cascade) propagates the failure and exits non-zero. The all-rows-succeed happy path is unchanged.
- **`bintrail recover --limit` now keeps the most recent events in the window instead of the oldest** (#785). `recover` never set an order on its fetch, so the query returned the oldest N events and reversed only that prefix — a truncated reversal script that silently undoes the beginning of the window rather than the end, an inconsistent recovery. The fetch now orders so `--limit` retains the newest events, and JSON output carries a `truncated` flag when the limit was hit. **Upgrade note:** the events selected by a limited `recover` change; re-run any previously truncated recovery to capture the intended (most-recent) events.
- **`bintrail recover` reversal scripts now pin a permissive `sql_mode` so they apply cleanly on targets with strict or `NO_BACKSLASH_ESCAPES` modes** (#786). The MySQL script pinned only `time_zone`, so a target session with `sql_mode=NO_BACKSLASH_ESCAPES` misparsed the backslash-escaped string literals `EscapeString` emits (silent corruption), and captured zero-dates (`0000-00-00`) were rejected under strict/`NO_ZERO_DATE` modes, aborting the whole transaction. The preamble now emits `SET sql_mode = 'NO_ENGINE_SUBSTITUTION';` (a value containing neither `NO_BACKSLASH_ESCAPES` nor any strict/zero-date mode) before the reversal statements; the PostgreSQL path is untouched.
- **`bintrail recover` no longer topples the whole reversal script on a `GEOMETRY` column, and no longer silently corrupts `BLOB`/`TEXT` values when the schema snapshot doesn't describe the table** (#788). A geometry value is stored as little-endian SRID + WKB and base64-encoded at rest; `recover` emitted the raw base64 as a literal, which a geometry column cannot load, failing the entire `BEGIN`/`COMMIT` block. Separately, when a resolver was loaded but did not describe the event's table, `BLOB`/`TEXT`/`BINARY` values were emitted verbatim as base64 (e.g. `aGVsbG8=` instead of `hello`) — a corruption that applies cleanly. Now the full spatial family (`POINT`, `LINESTRING`, `POLYGON`, …) decodes to `ST_GeomFromWKB(X'<wkb>', <srid>)`, and an event whose table is untyped by the event-time resolver is refused loudly rather than risking a base64 literal in a real column. A nil resolver (schemaless all-columns fallback), the PostgreSQL dialect, and a table merely absent from the *latest* snapshot (still typed from its own `schema_version`) stay permissive.
- **`bintrail recover` no longer reverses every byte-identical duplicate row when a table has no usable primary key** (#789). On a PK-less or unresolvable table, `pkWhereClause` falls back to an all-columns `WHERE`, which matches every duplicate of the touched row even though the row event reversed exactly one. Reversing one INSERT could then delete all identical rows. The fallback `DELETE`/`UPDATE` now carries `LIMIT 1` (MySQL dialect only — PK-scoped statements and the PostgreSQL dialect, where `LIMIT` on `DELETE`/`UPDATE` is invalid, are unaffected).
- **`bintrail verify` no longer reports a permanent false MISMATCH on every non-ASCII row of a legacy `latin1` (or other non-`utf8mb4`) table** (#792). The checksum scan pinned the session `time_zone` but not the result charset, so string columns were transcoded to the connection's `utf8mb4` while the baseline Parquet is written by mydumper under `SET NAMES binary` (raw bytes) — a `latin1` `é` (`0xE9`) hashed as `0xE9` on the baseline side but `0xC3 0xA9` on the live-scan side, a conclusive false MISMATCH even under a byte-correct restore. The scan now pins `character_set_results = binary` so string columns hash as their raw stored bytes on both sides. The digest contract tag is bumped `v1:` → `v2:`; a persisted pre-pin `v1:` baseline digest compared against a current `v2:` scan is now treated as Inconclusive with a "regenerate the baseline" hint (never a false MISMATCH), while row-count differences stay conclusive. **Upgrade note:** baseline digests written before this release carry the `v1:` tag and will read as Inconclusive against the new scan — regenerate the baseline to restore conclusive verification.
- **`bintrail verify` no longer reports a conclusive false-MISMATCH on tables carrying a spatial or `VECTOR` column** (#793). Spatial types (`geometry`/`point`/`linestring`/`polygon`/`multipoint`/`multilinestring`/`multipolygon`/`geometrycollection`) and MySQL 9.0+ `vector` are binary (WKB / packed floats) in the binlog event image, so they never matched the baseline/source rendering — the same base64-vs-raw representation gap already handled for `BLOB`/`BINARY`/`BIT`. These types are now added to the `isDeferredType` switch, so a false-MISMATCH becomes an honest `Inconclusive`; no working path changes output.
- **`bintrail verify` no longer reports false conclusive MISMATCHes on `TIME(fsp)` columns with whole-second values or on `FLOAT`/`DOUBLE` columns** (#794, #795). Same stored value, different text renderer on each side: go-mysql omits the fractional suffix on a zero-microsecond `TIME(3)` (`09:00:00` vs MySQL/mydumper's `09:00:00.000`), and MySQL's `my_gcvt` float text (`1e16`, `0.00001`) diverges from Go's `strconv` (`1e+16`, `1e-05`). Both are now normalized in `normalizeRenderedBytes` before comparison — TIME by trimming trailing fractional zeros, floats by reformatting through Go's shortest-round-trip formatter at the column's own width (32-bit FLOAT / 64-bit DOUBLE). FLOAT/DOUBLE stay out of the deferred-representation set, so a surviving divergence is still reported as a genuine (safe) mismatch.
- **Baseline-anchored `reconstruct`, the shim's `_snapshot`, `verify`, and cascade recovery no longer silently lose rows whose transaction executed just before a baseline dump but committed just after it** (#797). Row-event headers carry the statement's execution time, not its commit time, so a transaction straddling the dump's snapshot instant was invisible both to the dump's MVCC read and to a `Since`-only (`event_timestamp >=`) delta fetch — the row vanished from reconstruct/`_snapshot` and surfaced in `verify` as a causeless MISMATCH. A new `query.Options.SincePos` (the lower-bound analog of `UntilPos`) filters deltas by the exact binlog file+position the baseline Parquet already records, replacing the timestamp filter and widening the partition-pruning hint by an hour so the skewed partition isn't pruned away. Older baselines that never recorded a position fall back to the previous `Since`-only behavior — no forced re-baseline.
- **`bintrail baseline --retry` now re-converts a truncated Parquet file instead of skipping it as "already done"** (#798). The skip decision used `os.Stat` size only (`fi.Size() > 0`), so a nonzero-but-footerless `.parquet` left behind by an OOM/SIGKILL mid-conversion passed the check; `WriteManifest` then CRC'd the corrupt bytes and published `_SUCCESS`, and the corruption surfaced only at restore time. The skip now also requires that `ReadParquetMetadata` parses the footer — on a parse error the table is re-converted (`NewWriter` truncates via `os.Create`), while a valid Parquet merely lacking bintrail metadata keys still parses and is not needlessly re-converted.
- **`bintrail baseline` now converts dumps containing rows larger than 8 MB and rejects compressed dumps with clear guidance** (#801). mydumper emits one tuple per physical line, so a legal table with a `LONGBLOB`/`JSON` value over the fixed 8 MB `bufio.Scanner` buffer aborted the whole conversion with a raw `bufio.ErrTooLong` and no file or line context; the reader is now a growable `bufio.Reader` that expands to the line length (multi-MB tuples parse), and read errors are wrapped with file path and line number. Separately, a `--compress` dump (`.sql.gz`/`.sql.zst`/`.dat.gz`/`.dat.zst`) previously fell through the table classifier and surfaced later as an unhelpful `no tables found`; it is now rejected loudly with `re-run mydumper without --compress, or decompress the dump first`.
- **S3 uploads above 5 GiB no longer fail every cycle and stall rotation forever** (#804). Both upload paths (`storage.UploadFile` for rotation archives and baseline snapshots, `S3Backend.Put` for BYOS payloads and partition markers) used a single-shot `PutObject`, which S3 rejects with `EntityTooLarge` above 5 GiB — an hourly partition Parquet over that ceiling failed upload every cycle, so the partition never dropped, index and staging grew unbounded, and `rotate --retry` re-failed forever; multi-GB PUTs also restarted from scratch on every network blip. Both paths now route through the AWS SDK `feature/s3/manager` Uploader, which switches to a multipart upload (per-part retry and checksums) above ~5 MiB and falls back to a single `PutObject` for small bodies. Adding an `AbortIncompleteMultipartUpload` lifecycle rule on the archive bucket is recommended to reap parts orphaned by an interrupted upload.
- **BYOS agent now emits a durable, monotonic signal when a flush batch is permanently dropped instead of erasing the outage on the next successful flush** (#805). Previously the only signal was the per-sink `metadata_status`/`payload_status` bool, which flips back to `"ok"` on recovery — so after `flushBatch` truncated a batch that failed all retries (`batch = batch[:0]`), a monitor watching only that status never learned loss occurred, and the hosted metadata sink and client S3 payload sink can fail independently, leaving the index referencing row images absent from the bucket. `flushPipelineState` now carries cumulative, never-resetting counters surfaced on `agent.FlushStatus` and the `Heartbeat` wire contract as `metadata_lost_events`/`metadata_lost_batches`/`payload_lost_events`/`payload_lost_batches` (`omitempty`, additive), and any drop escalates a loud `slog.Error` carrying the running totals and the metadata-vs-payload skew. Batch truncation itself is unchanged; an on-disk spool to actually recover dropped batches is a follow-up.
- **BYOS partition-key creation no longer races between concurrent agents writing the same partition** (#806). `EnsurePartitionKey` used a check-then-write sequence against S3, so two agents could both observe the key absent and both write it, overwriting each other. The write now uses an S3 conditional put to close the check-then-write window.
- **`bintrail archive reconcile --deep` against a cross-region S3 bucket no longer marks every object deep-unverified** (#807). Each footer probe in `s3ParquetRowCount` opened a fresh DuckDB session and created its credential-chain secret with no region pin, so on a cross-region bucket every probe failed with 301/PermanentRedirect and the cron monitor stayed permanently red; a `--deep` over a year of hourly archives also paid extension setup and credential resolution roughly 8760 times. `scanS3Archive` now opens one region-pinned DuckDB session for the whole scan (region taken from the same resolution that backs the successful `ListObjectsV2` listing) and reuses it for every footer probe. Failure semantics are unchanged.
- **`bintrail dump` no longer destroys the `--output-dir` tree before validating anything** (#809). `runDump` ran `os.RemoveAll(--output-dir)` unconditionally and before source connectivity was checked, so a typo (`--output-dir /var/lib/mysql`) or a stray `BINTRAIL_OUTPUT_DIR` in a sibling `.bintrail.env` could wipe an arbitrary tree — including baselines that `reconstruct`/`verify` depend on — and a dump that then failed to connect had already erased the previous good dump. Dump now validates source connectivity before touching the directory, refuses to delete a non-empty directory that is not a recognizable prior dump (no `metadata`/`metadata.partial`/bintrail marker), and for a recognized prior dump renames it aside and deletes the backup only on success (restoring it on failure).
- **`bintrail init --format json` with `--s3-bucket`/`--s3-arn` now fails hard when S3 bucket setup/verify fails instead of silently reporting success** (#810). The stderr warning and remediation were gated behind non-JSON mode, so in JSON mode a failed `CreateBucket`/`HeadBucket` emitted no error in the payload (`s3_bucket` was simply omitted) and exited `0`, letting automation believe archiving was provisioned. JSON mode now adds an `s3_error` field to the output and returns non-zero on failure (stderr also gets `{"error":"..."}`). Text mode is intentionally unchanged — S3 setup stays best-effort there since the index tables are already created, warning to stderr and exiting `0`.
- **The source replication password is no longer exposed on mydumper's argv** (#811). `bintrail dump` and the console's in-process baseline dump both appended `--password <cleartext>` to mydumper's argv, leaving the credential visible to any host user via `ps aux` / `/proc/<pid>/cmdline` for the whole dump — and, in Docker mode, in `docker inspect` `Config.Env`. The password now never touches argv: in local mode it is passed via `MYSQL_PWD` in the child environment (mode-`0400` `/proc/<pid>/environ`), and in Docker mode it is written to a fresh `0600` MySQL option file (atomic `O_EXCL` create, deferred cleanup) bind-mounted read-only and read via `--defaults-file`. Username, host, and port stay on argv as before.
- **`bintrail doctor` source checks now honor the 30s timeout and flag `binlog_row_value_options=PARTIAL_JSON`** (#813, #777). The source probes (`checkLogBin`, `checkBinlogFormat`, `checkBinlogRowImage`, `checkBinlogRetention`, `checkSyncBinlog`, `checkStatementCapture`, `checkRowMetadata`) used `QueryRow` with no context, so a stalled source could hang `doctor` indefinitely past its deadline; each now threads the existing `ctx` via `QueryRowContext`. Separately, the new advisory `checkBinlogRowValueOptions` WARNs when `binlog_row_value_options=PARTIAL_JSON` — under which MySQL logs partial JSON updates as diffs bintrail can't apply and silently skips them at capture — SKIPs when the variable is absent (MySQL <8.0/MariaDB), and PASSes on the empty default.
- **`bintrail status` now shows a distinct `unavailable` continuity verdict when it can't read `stream_state`, instead of silently omitting the entire Stream section** (#815). A `LoadStreamState` read failure (transient timeout, revoked `SELECT`, an unexpected `loadSourceHealth` error) was degraded to a stderr `slog.Warn` and left `StatusData.Stream` nil — indistinguishable from an empty table — so both text and JSON dropped the always-present Continuity verdict and the `=== EVENTS PERMANENTLY LOST ===` banner, letting an operator read a `gap_lost`-stamped index as "no loss." Text now renders a visible `=== Stream ===` block with `Continuity: unavailable (could not read stream state: <err>)`, and JSON emits a distinct top-level `stream_error` object with `continuity.status = "unavailable"` (deliberately not a fabricated empty `stream`). The happy path — a genuinely empty `stream_state` — renders nothing, as before.
- **`bintrail rotate --retain` now rejects a malformed duration instead of silently shrinking the retention window** (#817). `ParseRetain` used `fmt.Sscanf("%d")`, which parses a leading integer without requiring the whole string be consumed, so `1.5d` silently became 1 day and `30 0d` became 30 days. It now uses `strconv.Atoi`, which rejects the unconsumed input and returns the existing invalid-format error.
- **The BYOS agent's `resolve_pk` handler now fetches each archived table once per batch instead of once per hash** (#818). Because Parquet archives carry no SHA2 index, the archive fallback fetched the entire `schema.table` and hashed `pk_values` client-side per request item and per archive source — a 50-hash `resolve_pk` against a large archived table meant 50 sequential full-table fetches on the agent running alongside the client workload. The client-side `pk_hash → pk_values` index is now memoized per `(source, schema, table)` within one call; fetch errors are not cached, and per-item source precedence (buffer → MySQL index → archives) is unchanged.
- **The time-travel shim now rejects a column-qualified `WHERE` when the table's primary key cannot be verified, instead of silently answering for the wrong row** (#821). When the resolver failed to load or the table was absent from every snapshot, `validatePKColumn` returned `nil`, letting the fetch join the literal against `binlog_events.pk_values` regardless of the column name typed — so `WHERE customer_id=1` against a table created after the last `bintrail snapshot` returned the row whose PK equals `1` with zero signal. Such a query is now rejected with `ER_PARSE_ERROR` naming the column, table, and reason. The no-`WHERE` full-table `AS OF` path returns before the resolver is consulted and stays permitted, so it keeps working before the next snapshot.
- **The shim's full-table `_snapshot` now fails loud when a baseline source is configured but the merge can't run, instead of silently returning a partial table** (#822). With `--baseline-dir`/`--baseline-s3` set, three cases previously degraded to the binlog-only path with only a server-side `Warn` — `FindBaseline` returning `ErrNoBaseline` (not yet baselined, or the table created after the last baseline), an unresolvable PK, or an unsupported PK column type — producing a table containing only rows with binlog activity in the window, indistinguishable from a complete one. Configuring a baseline source is now treated as an explicit opt-in to completeness: these cases return a `1526` (`ER_NO_PARTITION_FOR_GIVEN_VALUE`) wire error pointing at taking/re-taking a baseline and at `_flashback` for a binlog-only view. The no-baseline-source path (intended `_snapshot` → `_flashback` degrade) and single-row `_snapshot` are unchanged.
- **The `shim`'s `AS OF` / `_flashback` time-travel queries now resolve string primary keys containing backslashes and escape sequences correctly** (#826). `stripQuotes` removed only the outer quotes of a captured `WHERE` value, so a PK like `WHERE path = 'C:\temp'` was matched against the literal bytes `C:\temp` and never found the row. Quoted literals now pass through `unescapeStringLiteral`, applying standard MySQL string-literal semantics (`\n`, `\t`, `\0`, `\Z`, `\'`, backslash, LIKE escapes).
- **Full-table `reconstruct` over a PostgreSQL baseline now returns an actionable message instead of an impossible MySQL-only remediation** (#830). PG baselines deliberately omit `CreateTableSQL` (they embed an LSN anchor, not a mydumper `CREATE TABLE`), so the old error `lacks bintrail.create_table_sql metadata; re-run bintrail baseline` could never be satisfied — operators looped re-dumping baselines that would never carry the metadata. A PG gate now runs before the MySQL-only check and reports that full-table reconstruct is not yet supported for PostgreSQL sources (#597), pointing to single-row `reconstruct` or the shim `_flashback`. A genuine MySQL baseline missing `CreateTableSQL` still gets the unchanged re-baseline guidance.
- **`bintrail recover-cascade` no longer silently drops the second delete when the same parent PK is deleted, re-created, and deleted again inside the recovery window** (#831). `SynthesizeVictims` keyed its `visited`/`emitted` dedup globally by `schema.table|pk`, so the second root delete was discarded: children created between the two deletes were never reconstructed, a re-deleted child kept its stale first-delete image, and the run still exited 0 as "complete" with no `Incomplete` caveat. Dedup keys now carry the originating root timestamp so each root delete is walked with its own `[since, T]` window, and cross-root duplicates collapse keeping the newest image — recovery never double-INSERTs a PK and always restores the child's last known state.
- **`recover-cascade` no longer reports a provably-partial recovery as `Complete` when a child FK column was renamed** (#832). The victim scan matches child rows via the FK column name from the latest snapshot, but older events carry the column name in effect at event time; recovering a cascade older than a rename (e.g. `pid` → `parent_id`) made `JSON_EXTRACT` return NULL, matching 0 rows and reporting 0 victims with a clean `Complete()` — indistinguishable from "no children existed." On a zero-row scan the child-side now samples the row-images in the same window without the FK filter; if the FK column is absent from every sampled image, it records an `Incomplete` caveat (matching the parent side's existing `noref` caveat). A genuine no-children window with the column present stays `Complete`.
- **`bintrail recover-cascade` (and the console cascade path) now synthesizes cross-schema FK children instead of silently dropping them** (#833). The FK graph was loaded scoped to the child schema, so a child table in schema B with an `ON DELETE CASCADE`/`SET NULL` FK to a parent in schema A — legal and common in multi-tenant layouts — was never loaded: its cascade-deleted rows were not synthesized, no caveat was emitted, and the run exited `0` "complete" (silent data-loss). The console auto-route had the twin defect, falling back to plain `recover`. The FK graph is now loaded by the parent schema and walked transitively through cross-schema child frontiers, and parent detection is cross-schema-aware.
- **`bintrail recover-cascade` now selects the FK graph that was in effect at delete time rather than the newest snapshot** (#834). `LoadCascadeFKs` resolved the FK graph with `MAX(snapshot_id)`, so an `ON DELETE CASCADE` FK dropped after the delete (and re-snapshotted before the recover) made synthesis skip those children with no caveat, while an FK added after the delete synthesized victims that were never deleted. The graph is now anchored on the newest FK-bearing snapshot whose `snapshot_time` is at or before the earliest parent delete in the batch; when no snapshot predates the delete (e.g. a backlog re-index) the earliest recorded graph is used and the result is flagged `Incomplete` with a caveat — never silently.
- **`recover-cascade` no longer emits a dangling `SET FOREIGN_KEY_CHECKS=0` prologue with no matching re-enable when synthesis fails partway** (#835). `EmitSQL` streamed statements as it generated them, so a `buildStatement` failure on a synthesized victim row (e.g. a nil before-image) could demote to a `-- ERROR` comment while the FK-disabling prologue had already been written to the output — leaving the operator a script that disables foreign-key checks and never restores them. Generator output is now buffered and only flushed on full success; a refusal writes nothing partial and fails loud.
- **`reconstruct`/`recover` under `--no-archive` now correctly treat rotated archive-only hours as gaps** (#837). The query planner classified any hour present in `archive_state` as covered even when the caller excluded archives, so a `reconstruct --at ... --no-archive` over a range whose hours had been rotated to Parquet saw them covered, fired no `*GapError`, and returned an incorrect reconstructed state from live data only with no error (and under `--allow-gaps` / profile mode, no warning at all). The `NoArchive` fact is now threaded into `Plan`/`buildPlan`, which skip seeding coverage from excluded archives so those hours flow through the existing gap handling — `*GapError` under `AllowGaps=false`, a `slog.Warn` gap warning otherwise. Behavior is unchanged when archives are not excluded.
- **`bintrail-console serve --profile` now actually enforces RBAC and refuses an unknown profile** (#838). The console never set `query.Options.ProfileActive`, so a named profile that resolved to zero deny/redact rules — an empty profile or a typo like `--profile analysts_typo` — skipped the redaction pass entirely: the #699 contract (`query_text`/`query_hash` withheld under any named profile) was defeated, no table was denied, and the operator believed RBAC was active. `serve` now threads `ProfileActive` (set whenever a profile name is supplied, even zero-rule) into query options and the `NoArchive` coupling so archive rows can't leak `query_text`, and probes `query.ProfileExists` at startup to fail loud on a nonexistent profile instead of starting with RBAC that enforces nothing. `watch` takes no `--profile` flag and is unaffected.
- **`bintrail query --limit` over a Parquet snapshot now returns a deterministic subset** (#839). The snapshot scan applied `LIMIT` without a stable ordering, so repeated runs of the same limited query over an archived range could return different rows. The query now orders before truncating so the returned subset is reproducible.
- **Binlog-position comparison in `UntilPos` is now rollover-safe** (#840). Binlog filenames were compared lexically, which is wrong across a sequence-number rollover (e.g. `binlog.000009` vs `binlog.0000010`); a query bounded by an `until` position could include or exclude events at the boundary incorrectly after the source rotated past the width of the numeric suffix. The comparison now parses the numeric suffix instead of comparing strings.
- **`bintrail reconstruct --output-format mydumper` now refuses a full-table merge when a column was dropped after the baseline, instead of emitting a mixed-epoch dump** (#843). After a post-baseline `DROP COLUMN`, the full-table reconstruct NULL-filled the dropped column for rows touched after the drop while never-touched baseline pass-through rows kept its pre-drop value — all under a `CREATE TABLE` header that still declared the column, a state that never existed on the source and that silently re-exposed the dropped column's stale values (and spammed one warn line per affected row). It now scans the change map up front and refuses with an aggregated re-baseline error before any chunk file is written, matching the fail-loud posture already used for post-baseline added columns (#602). A genuinely-NULL value present in the after-image still merges as before.
- **Concurrent snapshot writers against one index no longer merge their rows under a single `snapshot_id` and silently lose events** (#844). `TakeSnapshot`/`WritePGSnapshot` allocated `snapshot_id` with `SELECT COALESCE(MAX(snapshot_id),0)+1` without `FOR UPDATE` — safe when CLI runs were serial, but under the `watch` daemon a DDL-hook auto-snapshot, a manual `bintrail snapshot`, and the console baseline trigger can run at once; two writers reading the same MAX merged both row sets under one id, after which `NewResolver` loaded doubled columns for every table and skipped ALL their events ("column count mismatch") while the checkpoint advanced. The allocation read now takes `FOR UPDATE` at both sites, so the second allocator blocks until the first commits and then takes the next id.
- **`bintrail stream` now refuses to resume in position mode when the saved binlog position exceeds 4 GiB instead of silently wrapping it** (#845). `stream_state.binlog_position` is stored as `BIGINT UNSIGNED`, but position-mode resume cast it through `uint32`, so a saved position above `math.MaxUint32` (a binlog larger than 4 GiB) wrapped to a wrong, smaller offset and resumed from the wrong place. The resume path now fails loud with an actionable error pointing the operator at GTID mode.
- **`bintrail recover` no longer emits an out-of-range negative literal for a `SET` column with 64 members and member 64 active** (#846). go-mysql decodes `SET` as `int64`, so a bitmask with the high bit set indexed as `-9223372036854775808`; `coerceUnsigned` reinterpreted `BIT` but not `SET`, so `recover` produced a negative value MySQL rejects as out of range and ordinal-to-label mapping could not resolve it. `SET` is now reinterpreted to `uint64` exactly like `BIT` — identity for smaller bitmasks, NULL passthrough unchanged.
- **The console no longer locks the local operator out of `/api/auth/login` when an attacker floods the global failure gate** (#847). `loginLimiter.Allow` checked the global 30-failures/min window before the per-IP window and, once tripped, returned `429` to every caller including loopback — so on a non-loopback bind a sustained 30 failed logins per minute kept the operator perpetually locked out, the exact self-DoS the throttle-only invariant forbids. Loopback peers are now exempt from the global gate only (the IP is the real socket peer from `net.SplitHostPort(r.RemoteAddr)`, never `X-Forwarded-For`/`X-Real-IP`, so it can't be spoofed); loopback per-IP throttling still applies, and the guard sits in `Allow` so login and both change-password paths inherit it.
- **`bintrail-pg` now refuses to start against a PostgreSQL publication that has a row filter or a column list, rather than silently capturing a subset** (#886). A `FOR TABLE ... WHERE (...)` row filter (PG15+) or `FOR TABLE orders (a, b)` column list passed every startup/doctor gate — even under `REPLICA IDENTITY FULL` — yet `pgoutput` drops filtered rows and unlisted columns and degrades filter-crossing updates to spurious INSERT/DELETE, while `pgbaseline` COPYs the whole table, so `reconstruct` would freeze filtered rows at snapshot state with no warning. `validatePublication` now queries `pg_publication_rel.prqual`/`prattrs` for the published tables and fails loud, naming the offending table(s) and reason (`row filter`/`column list`/both); `FOR ALL TABLES` short-circuits as safe, and the check no-ops on pre-PG15 servers where neither feature exists. Because it runs above the no-`--tables` early return and `CheckPublication` reaches it, `bintrail-pg doctor` gets the check for free.
- **`bintrail reconstruct` now reads baseline metadata from S3 baselines and refuses a no-baseline PostgreSQL full-table reconstruct** (#916). Single-row reconstruct read baseline metadata local-only, so for an `s3://` baseline the embedded coordinates stayed zero — disabling gap detection for every S3 baseline (MySQL and PG) and leaving beta-fidelity PostgreSQL output with no caveat when the flavor probe came back empty (an index with no `stream_state` row, an old schema, or a transient blip). Metadata is now read via the S3-capable `baseline.ReadParquetMetadataAny`, so the LSN clause and gap detection work against an S3 baseline's anchor (local behavior unchanged). Separately, a PostgreSQL full-table reconstruct of a table absent from the baseline previously fell through to a binlog-only (delta-only) report mislabeled as full-table; it now refuses by flavor in that branch too, mirroring the with-baseline gate.
- **`bintrail index`/`stream` now hard-fail the whole batch with an actionable error when a primary-key value would overflow the `pk_values` column, instead of silently making the row unqueryable** (#944). `binlog_events.pk_values` is `VARCHAR(512)` with `pk_hash` a stored `SHA2(pk_values, 256)` column; under non-strict `sql_mode` MySQL silently truncated an over-512-character PK, computing `pk_hash` over the truncated string while the read path binds the full value — a permanent mismatch that hid the row from `query`/`recover` forever. A new `checkPKValuesLength` guard (limit `event.MaxPKValuesLen = 512`, measured in runes not bytes so multibyte `utf8mb4` values don't false-trip) now aborts the batch before insert, naming the schema, table, actual rune count, and limit. The column was deliberately not widened (a `VARCHAR(512)`→`VARCHAR(3072)` change crosses MySQL's length-prefix boundary and forces a full table rebuild).
- **`bintrail query` / `bintrail recover --pk`/`--pks` no longer silently match zero rows for a single-column primary key containing a literal `|` or `\`** (#957). `binlog_events.pk_values` is stored pipe/backslash-escaped by the write path (`event.BuildPKValues`), but the `--pk`/`--pks` flags bound the raw flag value straight into the `pk_hash`/`pk_values` match, so a single-column PK holding a URL, slug, or file path matched nothing. Both `query` and `recover` now look up the target table's PK column count via a best-effort `metadata.Resolver` and, only when it is exactly 1, re-encode the value with `event.EscapePKValue`. Composite PKs (2+ columns) are left untouched, since the flag's own `|` is the user-typed delimiter between components (`--pk '12345|2'`).
- **Read-only commands run against an out-of-date index with a `SELECT`-only DSN now explain the privilege problem instead of failing with a bare MySQL 1142/1044 error** (#958). Every read entrypoint calls `EnsureSchema`, which issues `ALTER`/`CREATE TABLE`; on a read-plane DSN (the natural pairing with the RBAC `--profile` feature) against an index predating a schema-adding release, this hard-failed with an opaque `schema migration: %w` wrap naming neither cause nor fix. A new `indexer.WrapSchemaMigrationErr` detects MySQL errno 1142/1044 and rewrites them into a message telling the operator to run any capture-plane command (`index`, `stream`, `agent`, or `rotate`) once with a privileged DSN to migrate the schema, wrapping the original error via `%w` so `errors.Is`/`errors.As` still reach it. All other `EnsureSchema` failures pass through unchanged, so they are never misdiagnosed as a privilege issue. Applied across `query`, `recover`, `recover-cascade`, `verify`, `shim`, `reconstruct`, forensics reads, and both MCP tools; capture-plane commands still hard-fail as before.
- **The web console's Recover and its recovery preview no longer keep the OLDEST events of a truncated window instead of the newest** (#981, #967). `handleRecover` forced ascending order before fetching, so when the result hit `recoverDefaultLimit` (1000) it reverted only the oldest N events — leaving the newest still applied, a reversal that maps to no state that ever existed (the console counterpart of the CLI's #785). The console now fetches `Order: DESC` to keep the newest suffix, re-sorts ascending via `query.MergeResults` before generating the reversal, and surfaces a truncation warning so a partial recover is never presented as complete. The recovery preview (#967) previously showed the newest ≤100 matches (`/api/events`) while the actual recover used a disjoint oldest ≤1000 set; the preview now requests `limit=1000&order=desc` to match, and renders its own truncation warning.
- **The MCP `recover` tool now keeps the newest events when `limit` truncates the matched window, instead of the oldest** (#982). The tool's archive-merge and non-archive fetch paths never set `query.Options.Order`, leaving it at ASC, so a truncated window reverted only the oldest N events — leaving the newest still applied and generating a reversal that corresponds to no historically-consistent state. It now sets `Order: DESC` to keep the newest suffix and re-sorts ascending via `query.MergeResults` before generating SQL — the same fix already applied to the CLI in #927 (#785).

### Documentation
- **Fixed 11 documentation-audit gaps and added a `doctor` `sync_binlog` advisory** (#814). Docs corrected across `verify.md`, `query-and-recovery.md` (recover-cascade limitations, PK-less fallback NULL/duplicate hazard, a false rollback comment), `time-travel-sql.md` (zone-less `AS OF` literals are UTC, `SET time_zone` is silently ignored, 1s granularity), `postgres.md`, `rotation-and-status.md`, `indexing.md`, `capacity.md`, and CLI help text for `--since`/`--until`/`--at`. Additionally, `bintrail doctor` now emits a WARN (never FAIL) advisory when a source is not at `sync_binlog=1`, since such a source can silently drop committed transactions from the binlog tail on an OS crash.
- **Operator docs synced with the 2026-07-09 behavior changes** (#936). Twelve confirmed doc gaps (six of them stale wrong-claims) were corrected across `time-travel-sql.md`, `s3-iam-policy.md`, `upload.md`, `deployment.md`, `streaming.md`, `dump-and-baseline.md`, `indexing.md`, `guide.md`, `ddl-tracking.md`, `observability.md`, `console.md`, `rotation-and-status.md`, and `postgres.md` — covering the shim `_snapshot` fail-loud refusal, multipart-upload S3 IAM (`s3:AbortMultipartUpload`), position-mode checkpoint semantics, the dump output-dir guard and mydumper password handling, file-index fail-loud on a stale snapshot, the `bintrail_statement_dml_dropped_total` metric, PostgreSQL partial-publication refusal, the reversal `sql_mode` pin, the `unavailable` continuity verdict, `watch` source TLS flags, and the loopback login-gate exemption. `bintrail init --s3-bucket` also now prints `s3:AbortMultipartUpload` in the IAM policy, matching the docs.
- **Documented that S3 archiving and baseline reads use the bintrail service's ambient AWS credential chain (`AWS_*` in `.env`), not a per-source field** — this was previously undocumented in `docs/docker.md`. The reference table now lists the relevant env vars, plus a troubleshooting note tying the DuckDB credential-chain WARN and the console 403 `No credentials are provided` error back to the same root cause.
- **Fixed a wrong IAM action in the S3 docs and added a single copy-paste bucket policy** — `s3:HeadObject` does not exist (S3 authorizes `HeadObject` under `s3:GetObject`). New `docs/s3-iam-policy.md` gives one bucket-wide JSON policy covering archiving, baselines, upload, and S3 queries, linked from README/guide/upload docs, so operators no longer have to piece together per-feature IAM permissions.

## [0.29.2] - 2026-07-05

### Fixed
- **`bintrail up` no longer silently overrides env-bound TLS/GTID/gap-timeout stream settings** (#808). `populateStreamFlags` unconditionally reset `strmSSLMode`/`strmSSLCA`/`strmSSLCert`/`strmSSLKey`/`strmStartGTID`/`strmGapTimeout` to `up`'s hardcoded defaults, clobbering any value already set via the documented `BINTRAIL_SSL_MODE`/`_SSL_CA`/`_SSL_CERT`/`_SSL_KEY`/`_START_GTID`/`_STREAM_GAP_TIMEOUT` env-binding channel — the only way to configure these for `up`, which exposes no equivalent flags of its own. Most seriously, an operator setting `BINTRAIL_SSL_MODE=verify-ca` for an RDS/managed-Postgres-style TLS requirement would silently get `preferred` instead (no certificate verification, with a plaintext fallback on handshake failure), while `bintrail stream` with the identical env was unaffected. `populateStreamFlags` now only applies its default when the corresponding flag wasn't already `Changed` by env-binding.
- **Console's combined recover+cascade no longer synthesizes cascade-recovery rows for parent deletes outside the request's own scope** (#772). The internal parent-DELETE lookup ignored the recover request's GTID/changed-column filters and capped at a fixed 1000 rows regardless of the request's own `Limit`, so a narrowly-scoped recovery (e.g. undo one transaction by GTID) could synthesize INSERT/UPDATE cascade-restoration rows for unrelated parent deletes elsewhere in the table's history — producing orphaned children or FKs pointed at rows that were never actually being restored. The parent-DELETE set is now derived directly from the events already fetched for the recovery request, so it can never diverge from what the operator actually asked to recover, regardless of GTID/limit/event-type filtering.
- **`bintrail query --order desc` (and the console's default event view) no longer returns the OLDEST archived events for a range spanning multiple already-rotated hours** (#773). The S3 archive pipeline always scanned files oldest-first and applied an early-termination cutoff that's only valid for ascending order; a descending query over multiple archived hours could hit its `--limit` within the oldest hour and stop before reading the newer ones, silently presenting stale data as "most recent." Early termination is now skipped for descending queries.
- **`bintrail query`/`recover` with only `--until` set (no `--since`) no longer silently drops or loses archived S3 data** (#774). Omitting `--since` synthesized a hidden 31-day lookback window; a `--until` inside that window silently excluded any matching data older than 31 days, and a `--until` further back collapsed to a single bogus date prefix that could return zero rows for data that actually exists. An until-only query now lists the full archive range instead of guessing a start date.
- **Archiving a partition to Parquet is now crash-safe** (#802). `ArchivePartition` wrote directly to its final path with no atomic rename, so a process kill, OOM, or reboot mid-write could leave a truncated, footer-less file there; the rotation loop's `--retry` skip only checked that the file existed with a nonzero size, so on restart it would neither re-archive nor record an `archive_state` row, uploading the corrupt file to S3 as-is before dropping the live partition and pruning the local copy — permanently losing that hour's events behind an unreadable Parquet file. Archiving now writes to a temp file and renames it into place only once complete, and the retry-skip path additionally validates the existing file's Parquet footer (row count) before trusting it.
- **PostgreSQL baseline snapshots no longer risk excluding a concurrently-committing transaction from both the baseline and its delta-replay window** (#771). The baseline's embedded delta-replay anchor was `pg_current_wal_lsn()` read live inside the snapshot transaction; a transaction committing at that same moment can flush its commit WAL record (advancing that live LSN) before it's removed from the snapshot's MVCC visibility, so anchoring "replay deltas strictly after this point" on the live LSN could silently drop that transaction from both the baseline and any later delta replay. Baselines now anchor on the replication slot's own `confirmed_flush_lsn`/`restart_lsn` (read before the snapshot transaction opens), which is always at or before the live LSN — deltas replay from that inclusive floor instead. This lands ahead of the still-unbuilt PostgreSQL delta-consumer (full-table/single-row reconstruct); the corrected contract is documented everywhere it's asserted so that work picks it up correctly.

## [0.29.1] - 2026-07-05

### Fixed
- **Capture: TIMESTAMP columns were rendered in the capturing host's local timezone instead of UTC** (#757). Neither the file-based parser nor the two live `BinlogSyncerConfig`s pinned go-mysql's `TimestampStringLocation`, so a nil location made TIMESTAMP values format using the process's local `time.Local` — while DATETIME (decoded on a separate path) and the rest of the system (`verify`'s source-session pin, the shim's UTC-pinned DuckDB session, `reconstruct`'s PK canonicalization) all assume UTC at rest. On a non-UTC host this produced false MISMATCHes in `verify`, PK-matching failures in `reconstruct`, and a silent timezone shift when `recover`'s generated SQL was applied under a target session with a different `time_zone`. Both go-mysql entry points (`internal/parser/parser.go`'s `BinlogParser`, and the `BinlogSyncerConfig`s in `internal/streamrun/streamrun.go` and `cliapp/agent.go`) now pin `time.UTC`; `recover`'s MySQL-dialect script header also emits `SET time_zone = '+00:00';` so applying the reversal SQL under a non-UTC session can't reintroduce a shift.
  - **Upgrade note**: this changes the at-rest representation of newly-captured TIMESTAMP values on hosts that were NOT already running with `TZ=UTC` (e.g. most containers, which default to UTC, were unaffected). Events already indexed before upgrading keep whatever local-time string was captured at the time — this fix is capture-side only and does not rewrite historical data. There is no migration tool; if your index has non-UTC-host TIMESTAMP data mixed with UTC data, treat the switchover point as a schema-drift-style boundary when auditing `verify`/`reconstruct` results for tables with TIMESTAMP columns.
- **Capture: `latin1` CHAR/VARCHAR and BINARY/VARBINARY values were silently corrupted to U+FFFD at index time** (#756). go-mysql delivers those column types as raw Go strings with no transcoding; `marshalRow`'s `json.Marshal` call replaces any invalid-UTF-8 byte with the Unicode replacement character without error, which corrupted non-UTF8-charset CHAR/VARCHAR content (e.g. a legacy `latin1` table) and any BINARY/VARBINARY value with high-bit bytes (an MD5 digest, a binary UUID) at rest — `recover` then restored the corrupted value, and `verify` reported MISMATCH with the original bytes already gone from the index. `metadata.MapRow` now routes BINARY/VARBINARY through the same base64 storage path already used for BLOB/TEXT, and transcodes a non-UTF8 CHAR/VARCHAR value from its captured `CHARACTER_SET_NAME` (latin1/cp1252) when possible, failing loud instead of silently substituting U+FFFD when a byte has no defined cp1252 mapping.
- **Capture: `DEFAULT_GENERATED` columns (e.g. `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`) were misflagged as generated, so `recover` silently omitted them from reversal SQL** (#758). `TakeSnapshot` derived `is_generated` from a substring match on `EXTRA` (`strings.Contains(EXTRA, "GENERATED")`), which also matches MySQL's `DEFAULT_GENERATED` marker on any column with an ordinary expression default — not just true `VIRTUAL`/`STORED` generated columns. A DELETE's reversal INSERT would then be emitted without that column, silently resetting it to its default (e.g. `NOW()`) instead of restoring the original value. `is_generated` is now derived from a non-empty `GENERATION_EXPRESSION`, the same signal `internal/consistency` already used correctly.
- **Baseline: the mydumper-SQL reader discarded each INSERT's column list, so a table with a `STORED`/`VIRTUAL` generated column shifted every subsequent column's value (or NULLed the trailing one) in the baseline snapshot** (#767). mydumper/mysqldump exclude generated columns from their INSERT column-list and VALUES tuples, but `ParseSchema`'s column list included them, so `WriteRow`'s positional mapping ran misaligned — silently corrupting baseline data consumed by `reconstruct`/`_snapshot`/`verify` (the content-digest check doesn't catch this, since it hashes the same shifted values). `ParseSchema` now excludes `GENERATED ALWAYS AS (...) STORED/VIRTUAL` (and MariaDB's `AS (...) PERSISTENT`) columns to match what mydumper actually emits, and row-writing now validates column/value arity, failing loud on any residual mismatch instead of silently misaligning.
- **`bintrail query --archive-s3` no longer logs a misleading WARN when `s3:GetBucketLocation` is denied** (#734). The archive-region auto-detection probe is denied under bintrail's own documented minimal least-privilege IAM policy (`docs/upload.md`), which intentionally omits that permission — so the resulting `AccessDenied` is expected, not a misconfiguration. It's now logged at debug level, and only a genuinely unexpected `GetBucketLocation` error (network failure, `NoSuchBucket`, throttling) still raises a WARN. Behavior is unchanged either way: the resolved default region (already IMDS-aware per #697) is used as the fallback.
- **Indexer: a plain TEXT/BLOB value that happened to be a bare JSON scalar (`"false"`, `"true"`, `"null"`, or a numeric string like `"0"`) was silently corrupted into that JSON literal instead of stored as the original string** (#736). MySQL's binlog row format delivers TEXT/BLOB columns as `[]byte`, indistinguishable from a real JSON column at that layer, and `marshalRow` used to promote any valid-JSON `[]byte` to a raw JSON literal; this misfired on ordinary scalar-looking text content (e.g. a plugin storing the literal string `"false"` in a `LONGTEXT` column), producing a false MISMATCH in `verify` and, worse, a silently wrong value (`0` in place of `"false"`) in `recover`-generated SQL. `marshalRow` now only promotes a JSON object/array payload (mirroring the same guard already used on the baseline/Parquet read side); events indexed before this fix are additionally repaired at read time in `recover`/`reconstruct`/`verify`/the shim, wherever a decoded value is a stray Go bool/number instead of the expected string. A historical value that was originally the literal string `"null"` is not recoverable (indistinguishable from a genuine SQL NULL).

## [0.29.0] - 2026-07-03

### Added
- **Console: Forensics investigation view — who ran this?** (#708) — the embedded console now surfaces the OSS `internal/forensics` who-changed engine: four investigation modes (who-changed, user-activity, connection-history, ddl-history), a who-changed timeline with expandable forensic context, an aggregated fallback-SQL panel, and a capabilities/setup-guide banner when performance_schema or the audit plugin isn't fully configured. Per epic #701's decision D1, this also retires the console's own `connection_id` redaction on the events API in favor of the single `forensics.Enabled()` entitlement seam — `query_text`/`query_hash` remain gated under the separate, still-live #699 boundary. Forensic output (unredacted SQL text + session identity) is refused under an active RBAC profile, matching the Verify/recover-cascade precedent.
- **`bintrail-pg baseline` — native LSN-anchored PostgreSQL baseline producer** (#593). PostgreSQL sources can now take a consistent baseline directly from a live database (no `mydumper`-style dump step): a `pg_current_wal_lsn()` anchor is read inside the same `REPEATABLE READ, READ ONLY` transaction as the snapshot, embedded in each Parquet file's metadata, and used by `reconstruct`'s baseline-vs-first-event gap check (PG's textual LSN isn't lexically ordered, so this comparison is now flavor-aware rather than reusing the MySQL file+pos compare). Tables are discovered from `pg_publication_tables` so partition naming matches the delta path by construction; values are stored as raw PostgreSQL COPY-TEXT to stay byte-identical with the `pgoutput` rendering the delta path already indexes.
- **docs(pg): sequences after recovery** (#558) — a new `docs/postgres.md` section covering the SERIAL/IDENTITY sequence gotcha after restoring recovered rows (logical decoding never replicates sequence state), with a per-column and schema-wide `setval(MAX(id)+1)` recipe.
- **Managed-PostgreSQL smoke test** (#594) — an opt-in (`workflow_dispatch`-only) CI job and `scripts/managed-pg-smoke.sh` that provision an ephemeral Amazon RDS or Aurora PostgreSQL instance and exercise the full stream → capture → recover → teardown path against it. `docs/postgres.md` now states RDS and Aurora as smoke-validated and Cloud SQL as documented-but-not-validated; unvalidated Azure claims were removed.
- **Recovery: fail loud on a residual unchanged-TOAST marker** (#592) — every surface that could read the unchanged-out-of-line-TOAST sentinel (recover's SQL generation, `reconstruct`, the shim's resultset builders) now refuses loudly instead of leaking the internal marker into reversal SQL, PITR state, or a served column value. Under the required `REPLICA IDENTITY FULL` this sentinel should never actually persist for a supported source — this is a hardening backstop against a future capture bug, not a fix for an observed leak.

### Fixed
- **`SHOW TABLES FROM _flashback` (and `_diff`/`_snapshot`) only listed one table for PostgreSQL sources** (#603). A PG source allocates one snapshot id per table, so the shim's latest-snapshot resolver only saw the last table with DML. It now resolves the newest snapshot per (schema, table), unioned across all snapshots — uniform for both source families.
- **Forensics: the live performance_schema tier no longer overclaims `exact` confidence** — resolving an event's actor from the *current* holder of its connection id is not bounded against the event (connection-id reuse or `COM_CHANGE_USER` can attribute it to the wrong session), so this tier is now labeled `corroborated`, matching the equally-unbounded `connection_cache` tier. Only the audit log's CONNECT..DISCONNECT bounding and GTID joins keep `exact`.
- **Forensics: an oversized audit-log record (over 1MB) no longer truncates the rest of the file** — it's now skipped (and counted/logged distinctly) while parsing continues, instead of aborting the scan the way `bufio.Scanner`'s `ErrTooLong` used to.
- **Forensics: `connection_cache` retention sweeping and audit-log discovery robustness** — the retention sweep now runs on the audit-plugin-active branch too (previously only ran inside the poll loop, so once an operator installed an audit plugin, old connection-cache rows were never pruned); a sub-second `--attribution-retention` no longer truncates to `INTERVAL 0 SECOND` and wipes live sessions; an empty-but-validly-configured audit log (freshly rotated/enabled) is tolerated instead of hard-failing as an unknown format; and file rotation capping now keeps the newest files instead of the oldest.

## [0.28.0] - 2026-07-03

### Added
- **Public `ext` package: extension seams for embedding distributions** — an `AuditSink` interface (`ext.SetAuditSink`/`ext.Record`) recording data-access and script-generation operations, and `ext.SetForensicsEnabled` to override the forensics feature gate. Seams follow the `forensics.Enabled` convention: package-level vars set at startup, called at surface entry points. Wired surfaces: CLI `query`/`recover` and the MCP server's `query`/`recover` tools; the OSS binary installs no sink, so behavior is unchanged (one nil check per operation). Shim/console surfaces are follow-ups.

## [0.27.0] - 2026-07-03

### Changed
- **The bintrail CLI is now embeddable: every command moved from `cmd/bintrail` (package main) into a new public `cliapp` package** (#723). External Go modules can run the full CLI via `cliapp.Main(version, commit, date)`, which returns the process exit code (same 64/65 agent exit-code contract). `cmd/bintrail` is now a thin shim that passes its `-ldflags`-injected build metadata through — no build-script or packaging changes, and `bintrail` binaries behave identically.

### Added
- **CI: `oss-firewall` guard** (#727) — rejects any Go import or go.mod/go.sum reference to private `github.com/nethalo/*` modules, mechanically enforcing the open-core dependency direction (private imports public, never the reverse).

## [0.26.3] - 2026-07-02

### Fixed
- **S3 archive access (`verify`, archive queries) no longer fails with "region was not a valid DNS name" on an IAM-role-only EC2/ECS deployment with no `AWS_REGION` set** (#697). The AWS SDK's default config loader doesn't consult EC2/ECS instance metadata (IMDS) for a region, so it fell back to a permission-gated `GetBucketLocation` probe — one a minimal, least-privilege S3 policy commonly doesn't grant — leaving every S3 request with an empty, invalid region. Now falls back to the instance's IMDS region in that case, matching the fix already shipped for the baseline-upload S3 path (#610/#611). Also fixes the same reachable gap in the BYOS `bintrail agent --s3-bucket` storage backend.

## [0.26.2] - 2026-07-02

### Fixed
- **`bintrail verify` no longer reports a false MISMATCH on a JSON-valued TEXT column whose only difference is key order** (#692). A TEXT/LONGTEXT column storing `json_encode()`-style text (e.g. `wp_aiowps_audit_log.details`) preserves the source's original key order verbatim on disk; an event-touched row's image round-trips through Go's `map[string]any`, which loses that order, so the reconstructed value re-serialized alphabetically and compared byte-unequal to identical data. Baseline-anchored `verify` (the default mode) now re-renders both sides of a JSON object/array value into the same canonical form before comparing, refusing to do so (falling back to a raw byte comparison, so a genuine divergence still surfaces) on a duplicate object key, invalid UTF-8, or an unpaired UTF-16 surrogate escape — cases where canonicalizing could mask a real difference. Live-source `verify` had the same gap; see #696 below.
- **`bintrail verify` no longer reports a false MISMATCH between a MySQL zero-date value and a baseline's `NULL`** (#694). `internal/baseline`'s Parquet writer deliberately maps `0000-00-00`-family values to `NULL` (Go's time parser can't represent them); an event-touched row's image still carries the literal sentinel text, so comparing the two reported a mismatch for the same underlying data. Baseline-anchored `verify` now normalizes the sentinel text to NULL on both sides of the comparison — safe under verify's existing assumption that the binlog captured every write to the row, an accepted trade-off documented and pinned by a regression test rather than silently relied on.
- **`bintrail verify --source-dsn` (live-source mode) no longer has the same false-MISMATCH gaps as baseline-anchored mode** (#696, closes #693). The live-source comparison is asymmetric — one side is `verify`'s own reconstruct-and-render pipeline, the other is a raw MySQL scan (`internal/consistency.ConsistentTableChecksum`) that doesn't share any code with it. Both sides now apply the same JSON-key-order and zero-date normalization via a new `ConsistentTableChecksumNormalized` hook, so a table verified in either mode gets the same, correct answer.

## [0.26.1] - 2026-07-01

### Added
- **`docs/upgrade.md` — a comprehensive guide to upgrading an existing install** (#689). Covers version-checking, reading the changelog for BREAKING entries, the Docker Compose path (including the gotcha that `docker compose pull` only refreshes images, never `docker-compose.yml` itself — a new default like #690/0.26.0's `VERIFY_TRIGGER` silently never takes effect on an older compose file), standalone binary/package upgrades, building from source, what schema migration is automatic vs. the console registry's manual-touch case, upgrading a live `stream`/`watch` daemon without losing its checkpoint, and the no-downgrade policy. Linked from the README docs table.

### Changed
- **Console: user-facing copy rewritten in plain, direct language across the entire UI** (#691). Follows up the Verify Explain modal rewrite (#690) by applying the same voice everywhere — Overview, Events, Time-travel, Recover, Status, Storage (rotation/credentials/archiving/baselines/verify), server management, auth, the rotation dialog, and the command palette, plus the backend API error strings the frontend displays verbatim as toasts and capability gates. Real warnings (off-peak load, coverage gaps, non-downgradable) and domain nouns (baseline, binlog, snapshot) are kept intact — only CLI-flag references and internal jargon are cut from prose. Deliberately left technical: the Status page's database terminology (binlog file/position, LSN, WAL status, replica identity) and the PostgreSQL replication-health card, doctor-preflight remediation and the replication grant SQL (copy-pasteable commands), and DSN-only backend validation paths unreachable from the UI form.

### Fixed
- **Console: the Verification panel no longer floats detached from the rest of the Storage page** (#690). `ov-grid` is a 2-column grid with `align-items:start`; Baseline snapshots (one row per snapshot) routinely runs much taller than S3 archiving, so a 3rd grid item with no explicit placement landed under the short sibling with a large dead gap to its right where the tall one still extended. Verification now spans the full grid width in its own row.
- **Console: the Verify "Explain" mismatch drill-down renders a readable diff instead of raw CLI text** (#690). The modal showed `internal/verify`'s terminal-formatted `MismatchExplanation.Write()` output verbatim; the structured per-row diff data was already being fetched but never rendered. Now shows a plain-language card per differing row (changed/missing/extra) with a Column/Recovered/Baseline comparison table, with the raw CLI text still available behind a collapsed "Raw output" disclosure.

## [0.26.0] - 2026-07-01

### Added
- **Console: full lifecycle for `bintrail verify` — trigger, poll, explain** (#677). The Storage page gains a Verification panel that runs `bintrail verify`'s engine in-process for a monitored server instead of only reading results a CLI/cron/CI run produced elsewhere: trigger a run, watch per-table match/mismatch/inconclusive/error results land as they complete, and drill into a mismatch with an on-demand Explain (never precomputed — it re-reconstructs the table only when clicked). Mirrors the Baseline-trigger pattern (#613): both **baseline-anchored** (default, drift-free — needs a baseline destination and at least two snapshots) and **live-source** (needs the server's source DSN, reads the whole table off production, so the panel warns to run it off-peak) modes are supported. Verify's engine reads baseline/live state with no RBAC redaction, so the trigger and explain endpoints refuse outright while an RBAC profile is active — the same posture Time-travel already takes. Defaults ON in the bundled compose stack (`VERIFY_TRIGGER`, opt-out) since, unlike baseline creation, verify starts no subprocess and its default mode reads no live source; a bare `watch` invocation stays opt-in (`BINTRAIL_CONSOLE_VERIFY_TRIGGER=1`).

### Changed
- **Console: the Create-baseline button defaults to on (opt-out) in the bundled compose stack** (#676). `BINTRAIL_CONSOLE_BASELINE_TRIGGER` previously defaulted to empty/off in `docker-compose.yml`, so a fresh `install.sh` install never showed the button even with a source and baseline destination configured — the only way to discover it was to already know the undocumented env var existed. Now on by default; set `BASELINE_TRIGGER=0` in `.env` to opt out. The button stays double-gated per server (source DSN AND a baseline destination), so this is a no-op for any server without a destination configured. No backend change.
- **Console: the AWS credentials card leads with a plain-English summary, not four raw signals** (#681). On an IAM-role setup (EC2 instance profile, ECS task role, EKS IRSA) the Storage page's credentials card previously listed four env/config rows that all read "not set"/"absent" before the reassuring credential-chain note, reading as a wall of errors. The card now leads with one summary line (e.g. "Using an IAM role (ECS task role detected)") computed from signals `/api/storage` already exposed, folding the four raw rows behind a "Raw signals" disclosure. No backend change.

### Fixed
- **`bintrail index`/`stream` no longer format a fake real-looking GTID for a source with `gtid_mode=OFF`** (#678). A source with GTIDs disabled still wraps every transaction in an `ANONYMOUS_GTID_LOG_EVENT`, which go-mysql decodes into the *same* struct as a real `GTID_LOG_EVENT` — with a 16-zero-byte SID that passed `formatGTID`'s length check and formatted into a plausible-looking but meaningless `00000000-0000-0000-0000-000000000000:0`. `formatGTID` now recognizes the anonymous event type and returns empty, mirroring the existing "GTID not enabled" behavior, at both the file-based and streaming parse paths. MariaDB's GTID path was unaffected (already returns empty for the zero GTID via a different mechanism).
- **Console Overview's "latest window" now names its real 200-event scope instead of implying full index coverage** (#679). The subhead read "N delete(s) in the latest window" and a footer line labeled `COVERAGE` — both suggested a time-based view of the whole index, but Overview always fetches a fixed 200 most-recent events. The subhead now reads "... in the last 200 event(s)", and the footer (relabeled `WINDOW`) derives unconditionally from the fetched window's own first/last event instead of preferring the global index's earliest/latest whenever `/api/status` succeeded — so a busy index with more than 200 events no longer shows a misleadingly wide span.
- **Console Time-travel's PK and Table fields are now visually marked required**, matching Recover's genuinely-optional PK on the same widget (#680). Time-travel's PK field shared Recover's exact label and placeholder, where PK really is optional — a user coming from Recover reasonably assumed the same on Time-travel and hit an unmarked client-side "schema, table, and pk are required" guard. Table and PK now carry a small required marker on Time-travel only; Recover's fields (and Time-travel's Schema, required on both screens identically) are unchanged.

## [0.25.1] - 2026-06-30

### Fixed
- **Time-travel SQL (`_flashback`/`_snapshot`/`_diff`) returns BLOB and TEXT columns decoded instead of as their base64 text** (#661). The shim returns reconstructed rows over the MySQL wire protocol; the same base64 storage encoding behind #653/#660 meant a BLOB came back as ASCII base64 and a TEXT as its base64 string. The fix decodes BLOB/TEXT in the shim's single event-image pass — shared by `_flashback`, `_snapshot`, and `_diff`, single-row and full-table — before the row is emitted, applied to event images only and never to a baseline row, so a baseline value whose content is itself valid base64 survives verbatim. The column type is resolved at each event's own schema snapshot (mirroring the ENUM/SET decode), so a column widened `VARCHAR`→`TEXT` between events cannot mis-decode an older plain value; when the per-epoch type is unavailable the value is left as base64 rather than decoded against the latest snapshot (a wrong-schema decode would corrupt, not just mislabel).
- **Single-row `bintrail reconstruct` and the console Time-travel reconstruct return BLOB/TEXT decoded instead of as base64 text** (#666). #660 fixed the full-table mydumper path; the single-row path — `reconstruct` without `--output-format mydumper`, including `--history`, plus the console Time-travel surface — folds events onto the baseline without decoding, so an event-sourced BLOB/TEXT value emitted its base64 string. The fix decodes the delta events before the fold (event images only, never the baseline), typed at each event's schema snapshot so a `VARCHAR`→`TEXT` widening can't mis-decode an older plain value, with no decode against the latest snapshot when the per-epoch type is unavailable. The CLI `table`/`csv` formatters now render a decoded BLOB as base64 to match the JSON output.
- **Full-table `reconstruct --output-format mydumper` no longer mistypes BLOB/TEXT decode across a column-type change** (#668). #660 decoded the full-table writer path's BLOB/TEXT values, but typed every event from the *latest* schema snapshot rather than the snapshot in effect when each event was captured — so a column captured as `VARCHAR` (stored plain) and later widened to `TEXT` (stored base64) could have its old plain value wrongly decoded, corrupting any value that happened to look like valid base64 (e.g. `"test"`) to garbage bytes. The fix reuses the same epoch-aware decode `#666` already built (`DecodeEventBinaries`), run on the full event window before the writer's change map is built, instead of typing decode columns once from the latest snapshot.
- **`bintrail verify` no longer reports a false MISMATCH on tables with a TEXT column touched by an event** (#672). The full-table merge core `SnapshotFullTableImages` — shared by the shim's `_snapshot`, and by `verify`'s digest and `--explain` drill-down — never decodes BLOB/TEXT itself; every caller must decode its own event window first. The shim already did (#661); `verify`'s three call sites (live-source, baseline-anchored, and `--explain`) didn't, so an event-sourced TEXT value reached the comparison as raw base64 against the real decoded text — a guaranteed mismatch, surfaced as verify's core failure signal rather than a soft "inconclusive." The fix wires in the same `DecodeEventBinaries` call used by #668/#666. (TEXT was deliberately **not** added to verify's existing ENUM/SET/JSON/binary "deferred type" downgrade list — once decoded, TEXT is directly comparable to source text, and TEXT columns are common enough that deferring them would have hidden genuine recovery-chain divergences on most real tables, defeating the point of decoding them.)
- **`query`, `recover`, and `reconstruct` bound peak memory at scale** (#654). Three offline commands hold their working set in memory, so a BLOB/TEXT-heavy or very wide window could pressure RAM. Each gained a break-nothing safeguard: `query --limit 0` (an unbounded scan) still works but now prints a stderr warning; `recover` gained `--max-script-bytes` (default `2GB`; env `BINTRAIL_RECOVER_MAX_BYTES`; `0` = unlimited) to **refuse** rather than render a multi-gigabyte reversal script; and `reconstruct --output-format mydumper` gained `--warn-event-threshold` (default `5000000`; env `BINTRAIL_RECONSTRUCT_WARN_EVENTS`; `0` disables) that logs a loud warning above the threshold (it only warns, never refuses). See [docs/query-and-recovery.md §Memory Footprint](docs/query-and-recovery.md#memory-footprint).

## [0.25.0] - 2026-06-29

### Added
- **Opt-in `flashback` compose profile — a time-travel SQL terminal without ProxySQL** (#664). A `docker compose --profile flashback up` brings up the in-process MySQL-protocol shim (`_flashback`/`_snapshot`/`_diff` virtual schemas) directly, so you can run `SELECT … AS OF '<time>'` from any MySQL client without standing up the full ProxySQL routing layer — the lowest-friction way to try time travel against an existing index.

### Changed
- **The bundled index MySQL is tuned for high write volume** (#656, #657). `binlog_events` stores full before/after row images as JSON under a write-heavy, append-mostly load, so the `index-mysql` compose service now sets `--max-allowed-packet=1G` (the 64M default rejected large BLOB/JSON row images — a row's base64-inflated image could exceed it), `--skip-log-bin` (the index is a write-only sink — nothing replicates from it, and its own binlog was pure write amplification plus a per-commit fsync that cancelled `innodb_flush_log_at_trx_commit=2`), `--innodb-redo-log-capacity=2G` (the 100M default triggers checkpoint stalls under bursty large-row writes), and `--innodb-flush-method=O_DIRECT`. `config.Connect` now honors the server's `max_allowed_packet` instead of the driver's fixed 64 MiB client cap. BYO indexes (`INDEX_DSN`) are unaffected; `docs/deployment.md §3` documents the equivalent settings, including the `innodb_log_file_size → innodb_redo_log_capacity` rename in MySQL 8.0.30+.

### Fixed
- **Oversized BLOB/JSON row events fail loud instead of being silently dropped** (#652). A row image larger than the index server's `max_allowed_packet` (≈48 MB raw once base64-inflated, at the old 64M default) could not be inserted, and the failure was swallowed: the `stream` ticker/shutdown checkpoint warned-and-continued (auto-skipping the event), and offline `index` logged the error but exited 0 — a cron/CI wrapper read it as success. The flush error now propagates exactly like the batch-full and DDL flush paths already did: `stream` aborts loudly and replays from the last durable checkpoint on restart (transient errors lose nothing), and `index` returns non-zero with a `failed_files` count. Paired with the index-side ceiling raise above, the realistic case now succeeds; a genuinely un-indexable event aborts loudly rather than vanishing.
- **`recover` restores binary BLOB and TEXT columns correctly instead of emitting their base64 text** (#653). go-mysql delivers BLOB and TEXT columns (both `MYSQL_TYPE_BLOB`) as `[]byte`, which the indexer base64-encodes into the `binlog_events` JSON; on recovery the value came back as a base64 string and the reversal SQL wrote it verbatim — so a recovered BLOB became ASCII base64, not the original bytes, and a TEXT column likewise. The generated SQL now decodes by column type (from the schema snapshot): BLOB → `X'hex'`, TEXT → the original string, applied in the INSERT, UPDATE SET, and PK WHERE clauses (a BLOB/TEXT prefix PK was matching zero rows). VARCHAR/CHAR were always correct.
- **Full-table `reconstruct --output-format mydumper` restores BLOB/TEXT values touched by binlog events** (#660). The same base64 storage encoding corrupted reconstruct's mydumper output for any row changed since the baseline. The fix decodes the delta-event images up front, by provenance: baseline pass-through values (which DuckDB delivers as native `[]byte`/string from the Parquet scan) are never touched, only the event-sourced values are decoded — so a baseline TEXT value whose content is itself valid base64 survives verbatim.

## [0.24.0] - 2026-06-28

### Added
- **`bintrail verify` — prove a recovery would actually reproduce the source** (epics #631, #640). A new command answers the question the index could previously only *assume*: is what's in the Parquet/binlog index really what was in MySQL? It reconstructs each table from baseline + indexed binlog and compares an order-independent content fingerprint, byte-comparable by construction. Two modes: **baseline-anchored** (the default, drift-free — #643) compares the two most recent baselines — `reconstruct(previous baseline → the new baseline's exact binlog anchor)` against the new baseline, both at-rest at the *same* point — so it reads no live source, needs no off-peak window, and has zero production impact; **live-source** (`--source-dsn`) reconstructs to a consistent snapshot of the live server instead. Built on a point-in-time `ConsistentTableChecksum` primitive (#632), a content digest persisted in each baseline's Parquet metadata (#633), the reconstruct-and-compare capstone (#634), the baseline-pair comparison (#642), and a position-bounded reconstruct cut at an exact binlog coordinate (#646). Results are per table — match / mismatch / inconclusive — and never a false failure: no primary key, an index behind the source, a coverage gap, or a value class this version can't yet canonicalize (ENUM/SET/JSON/binary/float) all report inconclusive, never a mismatch. The run exits non-zero on any real mismatch or error. `--explain` (#644) adds a native row-level drill-down of each mismatch — which primary keys diverged (changed / missing / extra) and, for changed rows, the reconstructed value vs the new baseline's — computed in-memory from the same reconstruction the verdict came from, with no scratch database or external tool.
- **Stream continuity surfaced as a first-class "no data lost" signal** (#645). The capture stream's gap detector already recorded permanent event loss (`stream_state.gap_lost_*`, written when an unfillable gap forces an auto-advance) but it was buried. `bintrail status` now prints an always-present continuity verdict ("no gaps in the captured range" — a contiguity check, not a liveness one — or a loud "GAP LOST"), the status JSON gains a machine-readable `continuity.status` (`ok` / `gap_lost` / `unknown`), the console shows a green "✓ No data lost" badge mirroring the existing red gap-lost one, and `status --fail-on-gap` turns a permanent loss — or an inability to confirm the gap state (no stream row, a legacy index missing the columns) — into a non-zero exit for CI/cron. Opt-in: without `--fail-on-gap` the default still exits 0.
- **At-rest integrity for baseline Parquet files** (#636). Neither DuckDB's `parquet_scan` nor parquet-go validates a file's bytes on read, so a baseline silently corrupted on disk (bit-rot, a partial write) was read back as truth. Each baseline snapshot now carries a `_MANIFEST` sidecar recording a CRC-32C over every Parquet file's bytes, and the local read paths (full-table reconstruct, cascade recovery, and `query --include-snapshot`) re-hash and **fail loud** on a mismatch instead of returning garbage rows. The threat model is bit-rot / partial-write, not deliberate tampering (the manifest is a sibling file an attacker could also rewrite — true tamper-evidence needs signing, out of scope). A legacy snapshot with no manifest, a rotted/unparseable sidecar, or an unrecognized manifest version all degrade to "integrity not verified" rather than blocking recovery of intact data; only a confirmed CRC mismatch fails loud. Local baselines only for now — S3 read-validation is a follow-up (the manifest is already written and uploaded for S3 snapshots).

## [0.23.0] - 2026-06-26

### Added
- **Automatic retention/pruning for local baseline snapshots** (#616). Local baseline Parquet snapshots accumulated forever — `bintrail rotate` and the daemon's rotation loop only touch `binlog_events` partitions, and nothing reclaimed an old snapshot even after it was uploaded to S3, so a long-lived daemon silently filled its state volume with redundant copies. The new `--baseline-retain Nd/Nh` flag prunes them once redundant: on `bintrail baseline` (after a successful `--upload`) and continuously on `bintrail-console watch` (on the rotation cadence, covering the global `--baseline-dir` **plus every monitored server's own baseline dir** — the per-server dirs the console **Create baseline** button writes into). A snapshot is pruned only when ALL hold, so retention can never cost a recovery: a durable S3 copy is confirmed by an exact `_SUCCESS` HeadObject (a probe error or 404 keeps it — never delete the only copy); it is not the newest snapshot for any table (matching `reconstruct`'s per-table selection, so a present-time `at=now` reconstruct always resolves); it is complete and past the window; and a snapshot whose directory cannot even be listed is force-kept (it may be the newest readable copy of a table). Deletion is rename-then-`RemoveAll`, so a crash mid-delete self-excludes the half-removed tree from discovery rather than leaving a `_SUCCESS` directory with missing tables. The S3 copy is never pruned — only the redundant local one. Opt-in: without `--baseline-retain` nothing changes; env `BINTRAIL_BASELINE_RETAIN` / `BINTRAIL_CONSOLE_BASELINE_RETAIN`.

### Fixed
- **Time-travel `AS OF` now surfaces a column dropped by a later `ALTER`** (#600). Full-table `_flashback`/`_snapshot` shaped result columns from the LATEST schema snapshot and strict-projected onto it, silently dropping any column that existed at the `AS OF` instant but was dropped before now — even though its value is still captured in the index (so adding a `WHERE pk=…` changed which columns you saw, because the single-row path already unioned image-only keys). Full-table now converges on the single-row model: `SELECT *` derives columns as the latest snapshot order plus a sorted union of any image-only keys, so the dropped column reappears with its captured value and the WHERE-clause asymmetry is gone. No-drift queries stay byte-identical, and an explicit column projection is still honored verbatim (never widened).
- **`recover` fails loud when a referenced column was dropped or renamed after the event** (#601). Reversal SQL is built from the captured before/after row images, whose keys are the column names at event time; a column dropped or renamed between the event and now leaves the image referencing a column the table no longer has, so the generated statement fails to apply (`Unknown column`). The statement builders now report the exact columns each statement references, and the run fails loud — before a byte reaches the writer — when one is present in the event-time snapshot and absent from the latest. A column that drifted but is never emitted (e.g. living only in a PK-scoped `WHERE`) is correctly not flagged, so valid recoveries are never blocked. The apply-time sibling of #600/#602; covers the PostgreSQL delta-only `recover` surface too.
- **`reconstruct --output-format mydumper` fails loud on a column added after the baseline** (#602). Full-table reconstruct derives its output columns and `CREATE TABLE` header from the baseline Parquet; a column added to the source after the baseline lives only in the delta events' `row_after`, and projecting every row onto the baseline columns dropped that column's captured value silently — a dump that reloads cleanly but is missing a column. Surfacing it would require reconstructing the as-of-T DDL (machinery this offline path deliberately does not implement), so the run is now refused loudly up front (before any chunk file is written) rather than producing a silently-wrong dump.
- **`baseline` fails loud on an `INSERT`/`REPLACE` without a `VALUES` clause** (#468, shape 1). The SQL-dump reader silently skipped an `INSERT`/`REPLACE` line lacking `VALUES` — on a truncated dump whose last line is a partial `INSERT` header (cut off before `VALUES`), this dropped every row that statement carried and exited cleanly with a short count, producing a silently-incomplete baseline (and wrong Time-travel reconstructions). In a complete dump every `INSERT`/`REPLACE` keeps `VALUES` on the same physical line, so a missing `VALUES` is a truncation: the reader now errors instead of skipping, and the snapshot never gets its `_SUCCESS` marker. (Shape 2 — truncation landing exactly on a tuple boundary — remains tracked in #468.)

## [0.22.0] - 2026-06-25

### Changed
- **Console: cascade recovery is auto-detected inside Recover, not a separate tab** (#617). The standalone "Cascade recovery" tab is gone — when you generate undo SQL for a `DELETE` on a foreign-key **parent** whose children InnoDB cascade-deleted *below* the binlog (MySQL Bug #32506; MySQL ≤8.x / MariaDB), the console now detects it automatically (one index lookup of the recorded FK graph) and folds the invisible children into the **same** reversal script — re-inserting the parent once, re-creating the cascade-deleted children, and restoring `ON DELETE SET NULL`'d foreign keys, all wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. A *CASCADE detected* banner reports how many children and SET-NULL restores were included, and the `POST /api/recover` response gains `cascade_detected`/`victim_count`/`set_null_count`. Detection is best-effort and degrades safely: a PostgreSQL-sourced index is skipped (logical replication already captures cascade deletes as real events, so there is no blind spot to synthesize), an active RBAC redaction profile keeps synthesis disabled but **warns** that children are not included (never a silent parent-only "full restore"), and any detection/synthesis failure falls back to the plain recover with a visible warning rather than denying it. The scriptable `bintrail recover-cascade` CLI and the standalone `POST /api/recover-cascade` endpoint (with explicit `--lookback`/`--max-depth` knobs) are unchanged.

## [0.21.0] - 2026-06-25

### Added
- **Console: create a baseline snapshot from the web UI** (#613). The `bintrail-console watch` daemon gains an opt-in **Create baseline** button on the Storage → Baselines panel that runs the full dump→convert→upload pipeline for a monitored server **in-process** — the console image bundles `mydumper` (pinned to the same `v1.0.3-1` the compose baseline pipeline uses) and runs it as a local subprocess, then converts to Parquet and uploads to the server's S3 baseline prefix by calling `internal/baseline` directly. The console **never mounts the Docker socket**, so a console compromise cannot escalate to host-root, and the source DSN never leaves the process. Off by default — enable with `BINTRAIL_CONSOLE_BASELINE_TRIGGER=1`. The button is gated on the server having both a source and a baseline destination configured, runs one baseline at a time per server, and uses a consistent lock-free dump (`--sync-thread-lock-mode NO_LOCK --trx-tables`) so a least-privilege replication user (no `RELOAD`/`FLUSH_TABLES`) can dump, with the system schemas excluded. Validated end-to-end against a live Percona 8.0 source.

### Fixed
- **S3 uploads no longer fail with "region was not a valid DNS name" when `AWS_REGION` is unset** (#613). `storage.NewS3Client` relied on the AWS SDK resolving the region from the ambient chain, but the SDK does not fall back to EC2/ECS IMDS for the region — only `AWS_REGION`/`AWS_DEFAULT_REGION` and the shared config. In an IAM-role-only deployment with no `AWS_REGION` set (the bundled console on EC2), the region resolved empty and every S3 request failed. The client now best-effort queries the instance's IMDS region (2s timeout) when nothing else supplies one. Benefits every region-less S3 caller (baseline upload, archive upload, reconcile).

## [0.20.4] - 2026-06-24

### Fixed
- **Console Baselines / S3 time-travel no longer fail with "Can't find the home directory at '/home/bintrail'"** (#610). DuckDB caches its `httpfs`/`aws` extensions under `$HOME/.duckdb`, but the runtime images create the service user with `useradd --no-create-home`, so `$HOME` resolved to a directory that never existed — the first S3 baseline/archive read aborted the whole query (the console Storage → Baselines panel rendered only the error). The DuckDB session helpers now point `$HOME` at a writable directory when it is broken (the env is the only lever that reaches both an explicit `INSTALL` and the autoload a `CREATE SECRET`/`parquet_scan` triggers on its own pooled connection), and all five runtime Dockerfiles create `/home/bintrail`. Affects every DuckDB-over-S3 path: console `ListBaselines`, baseline reads, `query --include-snapshot`, `archive reconcile --deep`, and S3-direct `--ultrafast` reads.
- **The default baseline dump no longer fails for a least-privilege capture user** (baseline compose profile). `docker compose --profile baseline run --rm baseline` with no `BASELINE_SCHEMAS` dumped every schema including `sys`; a typical replication user (`REPLICATION SLAVE`/`CLIENT` + `SELECT`, no `SHOW VIEW`) cannot read the `sys` views, so mydumper died with `ERROR 1142: SHOW VIEW command denied ... sys.host_summary` and the profile exited 1. The default dump now excludes the system schemas (`mysql`/`sys`/`performance_schema`/`information_schema`) — they are unreadable by a least-privilege user and useless as a baseline anyway.

## [0.20.3] - 2026-06-24

### Fixed
- **Console Overview/Events no longer fails with `Error 1038 (Out of sort memory)` on wide rows** (#608). When the events list sorted the most-recent rows, MySQL carried the wide before/after JSON row images through the filesort as packed addon fields; a single fat row image — e.g. a WordPress `wp_options` autoload blob larger than the index MySQL's `sort_buffer_size` (the stock 256K on MySQL 8.x) — overflowed the sort buffer and killed the whole query, so the console landing page rendered only the error. The query engine now sorts and limits the **narrow key columns** (`event_id`, `event_timestamp`) alone, then joins back to fetch the wide columns for just those rows (late materialization) and re-establishes order in Go, so row width can no longer trip the sort. The bundled index MySQL also raises `sort_buffer_size` to 4M as defense-in-depth. An index does not fix this (on the RANGE-partitioned `binlog_events` the cross-partition merge still carries the wide columns); the bug is specific to MySQL 8.4 (the bundled index version — MySQL 8.0 degrades the sort instead of erroring). Affects every read surface (console, MCP, CLI `query`/`recover`).

## [0.20.2] - 2026-06-24

### Changed
- **Demo image is now multi-arch (amd64 + arm64)** (#605). `ghcr.io/dbtrail/bintrail-demo` now runs natively on ARM64 hosts (AWS Graviton, etc.) instead of failing with `exec format error`. The single-container demo previously bundled MySQL 8.0 community from Oracle's Debian apt repo, which ships no arm64 packages; it now bundles **Percona Server 8.0** — a drop-in MySQL with arm64 packages, identical `mysqld`/`mysql` binaries and ROW binlog format (the build gates on `mysqld --version` being Percona 8.0). The publish workflow builds each arch on a native runner and merges them into one manifest list (asserting both arches are present before signing), so `docker run` resolves the host's native entry with no QEMU emulation. Evaluation-only demo image; no change to the bintrail/bintrail-console/bintrail-pg binaries, which were already multi-arch.

## [0.20.1] - 2026-06-23

### Added
- **Console: source-aware presentation for PostgreSQL** (#595). The shared `bintrail-console` now adapts what it shows to the source family of the selected server (reported per-server via `/api/capabilities`, derived from the index's `stream_state.flavor`) — it stays index-only and never queries the source. For a PostgreSQL source the Status page labels the stream cursor as an **LSN** (instead of binlog file/position/GTID), it surfaces the durable permanent-loss record (an invalidated replication slot — the sibling of a MySQL binlog gap), and the Events page notes that actor attribution (who-changed) is unavailable upstream because `pgoutput` carries no backend connection id. Presentation only — it never hides a surface.
- **Console: PostgreSQL replication-health panel** (#599). The Status page gains a *Replication health* panel for PostgreSQL sources showing the replication slot's WAL-retention state (`wal_status`, retained WAL, the safe margin before invalidation) and whether every published table is at `REPLICA IDENTITY FULL`. The console stays index-only: the streaming daemon (`bintrail-pg stream` / `watch`) polls the source every ~30s and persists a snapshot to the index (`stream_state.source_health`), which the console renders. Because a snapshot can outlive a stopped daemon, the panel shows how recently it was checked and **degrades a stale snapshot (older than ~90s) to muted/warn** — a frozen reading never reads as live-healthy; a probe that cannot reach the source (e.g. a standby) shows *probe failing* with the reason rather than disappearing. For an on-demand, always-live check, use `bintrail-pg doctor`. Together with #595 this completes the source-aware console presentation tracked toward PostgreSQL GA in #597.

## [0.20.0] - 2026-06-23

### Added
- **PostgreSQL-as-source is now beta** (#527). The alpha→beta epic is complete — every data-safety gate is closed: type-faithful capture and recovery (#533), identity/generated recovery correctness (#557), replication-slot/WAL-retention monitoring (#532), per-table `REPLICA IDENTITY FULL` validation with primary-key-scoped recovery (#531), the standalone `bintrail-pg` binary across a PostgreSQL 14–17 CI matrix (#534), and DDL-drift handling (a mid-stream `ALTER` re-snapshots the table so post-`ALTER` rows are captured against the new shape). `recover` is the supported recovery surface; capture is type-faithful, `REPLICA IDENTITY FULL`-enforced, slot/WAL-monitored, and DDL-drift-safe — all verified end-to-end against live PostgreSQL. Remaining work (full-table `reconstruct` / time-travel via a PostgreSQL baseline, a managed-PostgreSQL smoke matrix, and source-aware console presentation) is tracked toward GA in #597. Read `docs/postgres.md` for the current limitations.
- **Capture-coverage preflight guards for PostgreSQL** (#555, #556, #559). `bintrail-pg` now surfaces three silent-loss classes loud instead of letting them slip past — both as warnings at `stream` startup and as `bintrail-pg doctor` checks:
  - an `UNLOGGED` table in capture scope under a `FOR ALL TABLES` publication (it writes no WAL, so logical decoding never captures it);
  - a foreign-key `ON DELETE CASCADE` / `SET NULL` **child** table whose parent is published but the child is not (a delete on the parent would rewrite the child, and that rewrite would not be captured);
  - a TimescaleDB hypertable chunk in the stream (`_timescaledb_internal._hyper_*`, out of scope — warned once per stream).

  Capture is never silently incomplete.

## [0.19.1] - 2026-06-23

### Added
- **Console: Cascade recovery tab** (#580). The web console gains a *Cascade recovery* tab (free `query_explorer` tier) that drives the `POST /api/recover-cascade` endpoint from 0.19.0: pick the parent table whose `ON DELETE CASCADE` / `SET NULL` delete cascaded and it generates the reversal SQL with Copy/Download — never executing it. Coverage is surfaced prominently: a partial recovery is flagged with an `INCOMPLETE` banner listing every caveat (and the caveats are also embedded in the SQL preamble), so it can never read as a full restore. The tab is capability-gated — hidden, with its route redirecting to the overview, whenever `recover-cascade` is unavailable (e.g. under an RBAC redaction profile). Completes the in-console cascade-recovery epic (#577, slices #578/#579/#580).

## [0.19.0] - 2026-06-22

### Added
- **PostgreSQL recovery SQL is now PostgreSQL-dialect and type-faithful** (#527; #533, #557, #573). Reversal SQL generated for a PostgreSQL-origin row is emitted in PostgreSQL dialect — identifiers double-quoted, string literals escaped under `standard_conforming_strings` (the script sets it defensively) — and 31 common types round-trip byte-for-byte: `numeric` beyond 2^53 and with scale, `bytea`, `json`/`jsonb`, arrays, ranges, `inet`/`cidr`/`macaddr`, the date/time family incl. `timestamptz` and `interval`, `money`, and user-defined enums. `GENERATED ALWAYS AS IDENTITY` columns recover correctly (`OVERRIDING SYSTEM VALUE` on a reverse-INSERT; omitted from a reverse-UPDATE `SET`, where PostgreSQL forbids them), `GENERATED BY DEFAULT` identity is kept (so a primary-key-changing UPDATE stays reversible), and `STORED` generated columns are omitted. The console, MCP, and agent recover surfaces all select the correct dialect from the index's source flavor. Verified by executing the generated reversal against live PostgreSQL 14–17.
- **`bintrail-pg doctor`** (#532). A preflight + health command for a PostgreSQL source. It checks the capture prerequisites (`wal_level = logical`, publication coverage, per-table `REPLICA IDENTITY FULL`) and the operational WAL-retention health that gates production safety: `max_slot_wal_keep_size` (a warning while unlimited — the disk-fill risk), and live replication-slot health (how much WAL the slot is retaining and its `wal_status`). An invalidated (`lost`) slot is a loud failure with the re-baseline recovery path. Text and JSON output.
- **`bintrail-pg reset`** (#532). Cleanly tears down a PostgreSQL capture for re-baselining: it drops the replication slot on the source **then** clears the index checkpoint (slot first, so an interrupted reset fails safe — never "checkpoint cleared, slot live"). `--force` confirms the destructive teardown; `--index-only` clears just the checkpoint when the slot is already gone or `lost`. The index's recovery data is never touched.
- **Durable "events permanently lost" surfacing in `status`** (#532). When a stream permanently loses data it cannot recover — a PostgreSQL replication slot invalidated or dropped, or (now also surfaced) a MySQL unfillable binlog gap — `bintrail status` shows a loud `EVENTS PERMANENTLY LOST` banner (text and JSON), durably, even after the capture process has exited. The index up to the gap remains fully usable for recovery; only resuming capture requires a re-baseline.
- **Console: reverse FK-cascade recovery over the web API** (#584). A `POST /api/recover-cascade` endpoint exposes the `recover-cascade` capability (recovering rows deleted by `ON DELETE CASCADE` and restoring foreign keys nulled by `ON DELETE SET NULL`) through the console, behind a capability gate.

With this release the PostgreSQL data-safety hardening (type fidelity, identity/generated correctness) and the replication-slot/WAL-retention operations are in place. PostgreSQL-as-source remains part of the ongoing alpha epic (#527) until it is promoted — read `docs/postgres.md` for the current limitations.

## [0.18.0] - 2026-06-22

### Added
- **Recover rows deleted by a foreign-key `ON DELETE CASCADE`, and restore foreign keys nulled by `ON DELETE SET NULL`** (#548; slices #549–#553). A new `recover-cascade` command reverses cascade side effects that InnoDB applies *below* the binary log: on MySQL 8.x and earlier (and all MariaDB), an FK cascade is enforced inside the storage engine, so only the parent `DELETE` is logged — the cascaded child deletes and SET-NULL updates are never recorded, and the normal delta-only `recover` has nothing to reverse. `recover-cascade` synthesizes the missing children from each child's last-known row image before the parent delete and emits restoring SQL — `INSERT`s for cascade-deleted rows, idempotent guarded `UPDATE`s (`… AND fk IS NULL`) for SET-NULL'd foreign keys — wrapped in `SET FOREIGN_KEY_CHECKS=0/1`. It is **dry-run only**: SQL is printed or written to `--output`, never executed. A baseline snapshot (`--baseline-dir`/`--baseline-s3`) extends recovery to children that have no binlog event within the lookback window. Coverage gaps (composite FKs, depth/candidate caps, archived-out windows) are reported and make the command exit non-zero unless `--allow-incomplete`. This is a free-core capability.
- **PostgreSQL recovery is now scoped to the primary key offline** (#531, #533, #570). The `bintrail-pg` capture stream persists a per-table PostgreSQL schema/type oracle into the shared MySQL index, so the offline `recover` and `reconstruct` commands build a primary-key-scoped `WHERE` for PostgreSQL-origin rows without a live source connection (previously they fell back to an all-columns match). A dedicated PostgreSQL-as-source operator guide ships too (#565). Part of the ongoing PostgreSQL alpha (#527) — read the limitations before relying on it.

## [0.17.0] - 2026-06-21

### Added
- **PostgreSQL as a replication source (alpha)** (#527, #530, #531, #534). A new standalone binary, `bintrail-pg`, captures a live PostgreSQL logical-replication stream (the built-in `pgoutput` plugin — nothing is installed in your source database) and indexes every row change into the **same MySQL index** as a MySQL/MariaDB source, so the existing `query`, `recover`, `reconstruct`, `status`, and `shim` commands work unchanged over PostgreSQL data. `bintrail-pg stream` takes a replication DSN plus a query DSN, validates `wal_level = logical` and per-table `REPLICA IDENTITY FULL` (the PostgreSQL analog of `binlog_row_image = FULL`), and resumes from a durable LSN checkpoint. Under `REPLICA IDENTITY FULL`, unchanged out-of-line TOAST values are recovered from the before-image. Ships as its own image `ghcr.io/dbtrail/bintrail-pg` and its own `bintrail-pg` deb/rpm; PostgreSQL 14/15/16/17 are exercised in CI against a live server. This is an **alpha** capability — the data-safety hardening that gates beta (a type-fidelity matrix, replication-slot/WAL-retention monitoring, and DDL-drift detection) is still in progress, so read the limitations before relying on it.

### Changed
- **The source-agnostic read/recovery commands moved to a shared internal package** (#528, #529). `query`, `recover`, `reconstruct`, `status`, and `shim` now live in `internal/cli` and are registered identically by both the `bintrail` (MySQL/MariaDB) and the new `bintrail-pg` (PostgreSQL) binaries, so the read and recovery surface is byte-identical across source families. The core `bintrail` binary deliberately does **not** link the PostgreSQL capture stack (a build guard enforces this), keeping its dependency surface unchanged. No user-facing behavior change for existing `bintrail` invocations.

## [0.16.2] - 2026-06-20

### Changed
- **`bintrail rotate` no longer requires `--bintrail-id` when archiving** (#539). It now defaults to the `bintrail_id` already recorded in `stream_state`. Precedence: an explicitly CLI-typed `--bintrail-id` > the `stream_state` id > a global `BINTRAIL_ID` environment value (last resort, with a warning) — so a single global `BINTRAIL_ID` can't silently become the archive write-key for every server.

### Fixed
- **MariaDB sources now receive a stable, unique `bintrail_id`** (#539). MariaDB has no `@@server_uuid`, so identity resolution previously failed and the source streamed with a NULL `bintrail_id`; two MariaDB servers archived to the same S3 location then collided under an empty `bintrail_id=/` prefix. bintrail now synthesizes a stable identity anchor from the source address (`host:port`), so distinct servers separate into distinct archive prefixes automatically. The MySQL identity path (via `@@server_uuid`) is unchanged.
- **`bintrail rotate --daemon` now fails loud at startup on an unresolvable archive `bintrail_id`** (#539). Previously a misconfigured archive daemon (no `--bintrail-id`, no `stream_state` id, no `BINTRAIL_ID`) logged an error every cycle but kept running and never rotated — silently filling the index disk. It now exits at startup on this permanent precondition error, while transient errors (e.g. a not-yet-ready index DB) still self-heal on the next tick.

## [0.16.1] - 2026-06-20

### Added
- **DBA-centric index metrics** (`bintrail_index_*`) (#351). `bintrail stream --metrics-addr` and the `bintrail-console watch` daemon now expose Prometheus gauges describing the **state of the index** a DBA cares about for recovery readiness — alongside the existing live-pipeline `bintrail_stream_*` metrics: oldest/newest event timestamp and retention horizon (how far back recovery reaches), event count, partition counts (`active`/`future`), `gap_hours` (hours rotated out of MySQL but not archived — holes in coverage), and `storage_bytes{location=mysql|parquet}`. A scraper refreshes them from a status snapshot every `--metrics-scrape-interval` seconds (default 60). `bintrail status` (text and JSON) now also surfaces the MySQL index storage size and per-table baseline Parquet size. New `docs/observability.md` documents every metric with PromQL recipes.
- **Third-party license notices shipped in release artifacts** (#428). Every distribution channel (tarball, `.deb`/`.rpm`, and the Docker images) now bundles a `THIRD-PARTY-NOTICES` file covering the statically-linked dependencies — DuckDB (MIT), `go-sql-driver/mysql` (MPL-2.0), the libduckdb-vendored C/C++ libraries (RE2, utf8proc, fmt, fast_float), and every linked Go module's license/NOTICE. A `make notices` target regenerates it, and a CI check fails if it drifts from the dependency graph.

### Changed
- **`bintrail index` now requires `--source-dsn`** (or an explicit `--skip-source-validation`) (#493). Previously, omitting `--source-dsn` silently skipped all source-server validation (including the `binlog_row_image = FULL` check). Indexing without a source connection now requires the explicit opt-out flag, under which the per-row partial-image guard (below) still applies. **This changes the behaviour of `bintrail index` invocations that omitted `--source-dsn`.**
- **`bintrail baseline` now fails loud on an unconvertible value** instead of silently writing NULL (#506, #503). A legal MySQL all-zero date (`0000-00-00` / `0000-00-00 00:00:00`) is mapped to NULL with a per-column warning (not an abort); a genuinely unrepresentable value now aborts the run with a clear error rather than publishing a lossy baseline.

### Fixed
- **Partial binlog row images are now detected and rejected** (#493). A per-session `SET SESSION binlog_row_image = MINIMAL`/`NOBLOB` (which bypasses the server-global check) writes partial before/after images, so absent columns were indexed as NULL — silently corrupting the images `recover` later trusts. The parser now fails loud on any non-FULL row image, covering both the file-index and live-stream paths (including the no-false-positive case of a `FULL` image with a VIRTUAL generated column).
- **`bintrail baseline` no longer silently loses `UNSIGNED` integers above the signed maximum** (#506). `BIGINT`/`INT UNSIGNED` values past the signed max failed conversion and were written as NULL; the schema parser now recognises the `unsigned` attribute and the writer round-trips the full unsigned range. The same `connection_id` (`INT UNSIGNED`) over-range loss on the archive and BYOS-buffer write paths is fixed too.
- **Incomplete baseline snapshots are now flagged and excluded from discovery** (#467). A baseline run that failed or was killed mid-conversion left a partial snapshot byte-indistinguishable from a complete one, which time-travel / `reconstruct` would then serve as the newest. Runs now write a `_SUCCESS`/`_INCOMPLETE` marker — written *before* the workers launch, so even an uncatchable crash (OOM/SIGKILL) leaves the snapshot positively flagged — and discovery skips incomplete snapshots. The S3 upload path is crash-safe (incomplete-marker first, `_SUCCESS` last).
- **S3 baseline staleness is now surfaced instead of silent** (#466). When an S3 baseline lookup falls back to an older snapshot (the requested table is missing from the newest one), it now logs a warning and the console surfaces a `stale_baseline` warning, rather than silently reconstructing from older data. A transient error on the advisory staleness scan no longer fails an otherwise-successful lookup.
- **`archive reconcile --deep` no longer silently downgrades to non-deep** (#469). When an S3 Parquet footer probe failed, the deep row-count verification was silently skipped for that object and a dry-run could exit 0 on objects it was asked to verify. Footer-probe failures are now counted, surfaced in the text and JSON report, and make the dry-run exit non-zero — so a scheduled `--deep` monitor can't go green on unverified objects.

## [0.16.0] - 2026-06-19

### Added
- **MariaDB as a replication source (alpha)** (#515, #516). bintrail can now stream and index from a **MariaDB** source — pass `--source-flavor mariadb` to `bintrail stream` (or run `bintrail index` over MariaDB binlog files). The index database stays MySQL; only the source changes. The parser handles MariaDB `domain-server-seq` GTIDs and skips MariaDB-only binlog events (`Annotate_rows`, `Gtid_list`, `Binlog_checkpoint`) transparently, and the source flavor is recorded in the checkpoint so a GTID resume re-parses the saved set correctly. Primary target is MariaDB 11.4. A dedicated operator page (`docs/mariadb.md`) covers setup, version support, limitations, and troubleshooting. This is an **alpha** capability with narrower topology coverage than the MySQL path — read the limitations before relying on it.
- **Real GTID gap detection for MariaDB sources** (#517, #518). MariaDB GTID-mode resume is promoted from degrade-to-warn to real purged-binlog gap detection. MariaDB has no `@@gtid_purged`, so the purge floor is derived from `BINLOG_GTID_POS` over the oldest surviving binlog; on resume a purged-binlog gap now raises the data-loss alarm (or, with `--no-gap-fill`, refuses to start) in **both position and GTID mode**, and multi-domain GTID sets are compared per domain.

### Fixed
- **An unfillable-gap auto-advance now records the data loss *before* advancing the checkpoint** (#518). The shared auto-advance path (MySQL and MariaDB) stamped `gap_lost_at` *after* advancing the checkpoint and only logged a warning on failure, so a transient index-DB error could leave an advanced checkpoint with no durable record of the loss — a healthy-looking stream that had silently skipped data. The durable record is now written first and fails loud, so the loss record can never desync from an advanced checkpoint.

## [0.15.0] - 2026-06-17

### Added
- **`--ultrafast` mode for the offline `query`, `recover`, and `reconstruct` commands** (#509, #510, #511). By default these commands run their internal DuckDB engine under a conservative, container-safe budget — 2 threads and a 4 GB memory limit, spilling to the OS temp directory when exceeded — so bintrail stays alive in small shared containers. On a dedicated host with spare RAM, `--ultrafast` lets DuckDB self-tune to the machine (all CPU cores, ~80% of physical RAM, still spilling before the limit) and reads S3 archives **directly via DuckDB's `httpfs` extension in a single parallel multi-file scan** instead of downloading each file to disk first, removing the double I/O on the S3 path. The bucket region is pinned in the credential secret so cross-region reads avoid a 301 redirect. Because `httpfs` holds each scanned file in memory outside DuckDB's `memory_limit`, the command logs the peak-RAM estimate (largest file × thread count) and `--duckdb-threads N` doubles as a memory-safety bound that caps how many files are scanned at once.
- **Granular DuckDB tuning flags** `--duckdb-threads` and `--duckdb-memory-limit` (e.g. `16GB`), and the env vars `BINTRAIL_ULTRAFAST`, `BINTRAIL_DUCKDB_THREADS`, `BINTRAIL_DUCKDB_MEMORY_LIMIT`. An explicit flag wins over `--ultrafast`, which wins over the default, so you can tune to your box without the all-or-nothing switch. `--duckdb-memory-limit` is validated up front (a percentage, bare number, zero, or negative value is rejected with a clear error rather than silently mishandled by DuckDB). These flags affect only the offline CLI commands; the long-lived `shim` and `bintrail-console` daemons keep the container-safe default.

## [0.14.1] - 2026-06-15

### Fixed
- **Recover and time-travel now preserve integer columns above 2^53** (#496). Row before/after images are stored as JSON; the read path decoded them with the default JSON number handling, which turns every number into a `float64` and silently rounds any integer above 2^53 — so `recover` SQL and `query`/CSV/shim output emitted the wrong value for a `BIGINT UNSIGNED` above 2^63 (or a large signed `BIGINT`) even though storage was exact. Numbers now decode as exact literals (`json.Number`) end-to-end through `recover`, `query`, the archived-Parquet read path, and the time-travel shim. This completes the #490 unsigned fix for `BIGINT` (storage was already correct; the read path was not).
- **`BIT(64)` columns with the high bit set are now indexed as their correct value** (#497). go-mysql decodes `BIT(N)` as a signed integer, so a `BIT(64)` whose top bit is set was stored as a negative number. BIT is now reinterpreted as unsigned (an identity for `BIT(1..63)`); combined with #496, a `BIT(64)` value above 2^53 survives exactly through recovery.

### Changed
- **Documented that `binlog_row_image = FULL` is a server-wide source requirement** (#492). The docs now state that `binlog_row_image = FULL` must be set **server-wide** — a per-session `SET SESSION binlog_row_image = MINIMAL`/`NOBLOB` writes partial row images that index as incomplete (`recover` can then emit NULLs / fail to match), which is out of support — and that `binlog_row_value_options` must not include `PARTIAL_JSON`. `SUPPORT.md` gains a "Source server configuration" section so issue triage can cite the boundary.

## [0.14.0] - 2026-06-14

### Added
- **One-command install script** (#483). A single `curl … | sh` installer fetches the correct release binary for the host platform with a branded onboarding flow, replacing the "find the right archive on the Releases page" step for first-time setup.

### Fixed
- **`bintrail baseline` no longer silently drops rows from multi-row mydumper INSERTs** (#495). mydumper ≥ 1.0 emits one row tuple per physical line (`VALUES(r1)` / `,(r2)` / `,(r3);`); the SQL reader only parsed the tuple on the `VALUES` line and skipped every continuation line, so a 1,000,000-row table produced a baseline with one row per `INSERT` *statement* (≈159 rows) while the command reported success. The reader is now stateful across lines and parses every tuple. A dump file that ends mid-statement, or an unexpected token where a tuple/terminator is expected, now fails loudly instead of reporting a short row count. Time-travel / `reconstruct` builds on the baseline, so this was silent data loss in the foundation of the feature.
- **`bintrail baseline` no longer corrupts binary, BLOB, BIT, and JSON columns** (#504). On default mydumper output, `BINARY`/`VARBINARY`/`BLOB` values (`_binary "…"`) and JSON values (`CONVERT("…" USING …)`) were routed through a quote-blind value reader: a raw `,` or `)` byte inside the value (near-certain for any real blob, e.g. a `BINARY(16)` UUID key) split the row into the wrong columns, and JSON columns stored the literal `CONVERT(…)` wrapper text instead of the document. `REPLACE INTO` dumps (mysqldump `--replace` / mydumper `--replace`) were skipped entirely, dropping every row. Charset introducers and the `CONVERT(…)` wrapper are now decoded through the real string parser, the `\0` and `\Z` (Ctrl-Z) escapes are handled, and `REPLACE INTO` is accepted. Verified byte-exact through `baseline` → Parquet → read-back against real mydumper v1.0.3 output.
- **Streaming no longer loses a transaction's row events when a checkpoint lands mid-transaction** (#491). The replication checkpoint could advance the stored GTID set in the middle of a transaction; on restart the stream resumed *after* that GTID and skipped the transaction's remaining row events. The GTID checkpoint now advances only at transaction commit boundaries (at-least-once), so an interrupted transaction is re-read in full rather than partially lost.
- **UNSIGNED integer columns with the high bit set are now indexed as their correct value** (#490). Large `UNSIGNED` values (e.g. a `BIGINT UNSIGNED` above 2^63, or any unsigned column whose top bit is set) were stored as the equivalent negative two's-complement number, so both indexed events and generated recovery SQL carried the wrong value. They are now decoded as unsigned.

### Changed
- **Documentation rebuilt to be operator-first and open-core-honest** (#484–#489). The README, `quickstart.md` (now UI-first: console then CLI), `guide.md` (refocused as a DBA incident playbook), and `mcp-server.md` were rewritten for the DBA/operator audience, cutting Go-internals and SaaS-only content.

## [0.13.4] - 2026-06-13

### Added
- **The source MySQL user's required grants are now spelled out where you create the connection** (#479). The console "+ Add server" form shows the exact `CREATE USER` / `GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT` to copy, right under the source fields — and `streaming.md`, `quickstart.md`, and `console.md` document the same, with the per-privilege rationale and a least-privilege (schema-scoped `SELECT`) variant. Previously the required grants appeared in no doc at all, so the very first step of monitoring a source was a guess.

### Fixed
- **A stopped monitored source's "Test" button no longer reports a scary `Unknown database` error** (#480). A source's per-source index database is created when monitoring *starts*; before that, Test pinged a database that doesn't exist yet and surfaced a raw `Error 1049 (42000): Unknown database 'bintrail_idx_…'` — read as a connection failure when the real state is just "not started." Test now recognises this and shows a neutral, actionable result: *"index database … not provisioned yet — click Start to create it and begin streaming."* A genuinely wrong index DSN on a non-monitored entry is still a hard error.
- **A failed monitor-start preflight is no longer a silent failure** (#481). Clicking "Start" on a source ran the doctor preflight and, on failure, returned the report only to the browser — the daemon logged nothing, not even under `--log-level debug`. An operator watching `docker logs` saw a source refuse to start with zero diagnostic trail. The start path now logs the request and its outcome: a `WARN` on a failed preflight names each failed check (e.g. `Source MySQL connection: dial tcp …: connection refused`), and the supervisor emits a per-check `DEBUG` line so the full preflight is visible from the host.
- **Four console monitor/index UX bugs that compounded into a UI that looked broken** (#482). (1) The primary button (Start, + Add server) turned invisible on hover — white text on a light-gray fill, because `.btn:hover` won the background. (2) A selected server whose index was unreachable made `/api/capabilities` fail, which hid the *entire* control plane (the Start button, the "+ Add server" monitor copy) — the process-level monitor capability now survives a broken selection. (3) A missing-index error rendered as a raw red wall; it's now an actionable empty state that clarifies the index database is created when monitoring starts and never lives on the source. (4) Editing a monitored source auto-expanded the optional "bring your own index" section, exposing a per-source index DSN the operator never typed; it now stays collapsed for source entries.

## [0.13.3] - 2026-06-12

### Added
- **Time-travel results render ENUM and SET values as labels — everywhere rows are reconstructed** (#472, #475, #476). Binlog ROW images store ENUMs as numeric ordinals and SETs as bitmasks, so a time-travel query answered `3` where the live row says `shipped` — two representations of the same column on the same connection. All reconstruction surfaces now map ordinals back to labels using the schema snapshot **in effect at each event's timestamp**: every shim query shape (`_flashback`/`_snapshot`/`_diff`, bare `AS OF`, the `DBTRAIL_AT` hint), the console Time-travel tab (state and history), `bintrail reconstruct` single-row output, and full-table mydumper output (which now writes labels, like a real dump). Decoding is per-event, so an ENUM reordered between two changes renders each change under its own definition instead of mislabeling older ordinals with the newest one; anything that doesn't match a known ordinal exactly (the definition shrank, unknown SET bits) passes through as the raw number — never a guessed label. The raw event-record surfaces — `bintrail query`, the MCP `query` tool, the console events view — deliberately keep the stored ordinal: it is the forensic ground truth.

### Fixed
- **`bintrail snapshot` no longer fails outright on tables with realistic ENUM columns** (#474). `schema_snapshots.column_type` was a `VARCHAR(128)`; a mundane 10-member ENUM renders a longer declaration, and under strict mode the resulting "Data too long" error aborted the **entire snapshot transaction** — not one column. The column is now `TEXT`, and existing indexes are widened automatically at startup with stored values preserved.
- **`bintrail-mcp` no longer reports an error when the client disconnects normally** (#473). Closing stdin is how every MCP stdio session ends, but the server logged `ERROR … server is closing: EOF` and exited 1 — so supervisors and exit-code checks recorded a failure on every normal disconnect. A clean disconnect now logs at INFO and exits 0; real transport faults still log ERROR and exit 1.
- **Docs caught up with the MCP server's fourth tool** (#471). Every enumeration said three tools (`query`, `recover`, `status`) — `list_schema_changes` (DDL history with full statements and binlog coordinates) existed in the server but appeared nowhere, including the connector-testing checklist whose "you should see three tools" step had become a false failure. Also documented: the `DBTRAIL_AT` hint form and relative time literals (`'5 minutes ago'`, `'now'`) in the time-travel walkthrough's grammar, and how `AS OF` SQL relates to the Docker Compose stack (the console image deliberately ships without the shim).

## [0.13.2] - 2026-06-11

### Fixed
- **A fresh install no longer shows a phantom server** (#470). On a source-less `bintrail-console watch` (the compose quickstart), the daemon's own index database appeared in the server switcher and the Servers dialog as `bintrail_index (cli)` — reading as a pre-existing server when nothing had been added yet. That boot entry is internal plumbing there (each added source streams into its own per-source database), so it is now **hidden entirely**: a fresh install shows "no servers yet" in the switcher and an empty Servers dialog, while the views keep rendering (an empty Overview) against the internal index underneath until the first server is added. Because that internal index is not guaranteed empty (e.g. restarting a previously source-ful daemon without its `SOURCE_DSN`), a note under the switcher attributes the data — "Showing the daemon's internal index — add a server to start monitoring" — and disappears once a server exists. The entry remains visible where it actually carries data: `bintrail-console serve`, and `watch` with `--source-dsn`, labeled by its database name and sorted last in the switcher. This supersedes 0.13.1's demotion (which changed only the default selection and kept the entry listed).
- **The Storage page is scannable** (#470). The paragraph-heavy panels from 0.13.1 are now one-line captions and structured empty states: a status-only Baselines card (`source` / `time-travel`), and a lead line plus two short numbered steps — with the compose one-liner as a copyable code block — instead of prose. Each "how to enable" hint is a single line and appears in exactly one place.

## [0.13.1] - 2026-06-11

### Added
- **A Settings → Storage page in the console** (#457). Under `bintrail-console watch` the sidebar gains a Settings group: **Storage** collects everything S3/baseline-related in one place — the effective rotation policy (with an edit shortcut), every monitored source's Archive-to-S3 destination (with a shortcut into its edit form), a read-only **Baseline snapshots** panel for the selected server (snapshot age, table count, and the binlog coordinates its deltas start from), and an **AWS credentials** card showing which ambient credential signals the daemon can see (presence and non-secret names only — the console never stores keys). **Rotation** gets a visible sidebar entry too; it was previously reachable only through the ⌘K palette. Two read-only endpoints back it: `GET /api/baselines` (per-server snapshot listing) and `GET /api/storage` (process-global credential signals).
- **The confusing `default (cli)` selector entry is demoted and relabeled** (#457). On a source-less `watch` daemon nothing ever streams into the boot index (each added source gets its own per-source database), yet every fresh browser tab landed on it — a permanently empty Status page. Fresh tabs now land on the first monitored server instead; the boot entry stays listed and selectable, labeled by its database name (e.g. `bintrail_index (cli)`) and sorted last. A failed server load also no longer renders as "No monitored sources yet".
- **A `baseline` profile in the Docker Compose stack — Time-travel snapshots with zero installs** (#458). `docker compose --profile baseline run --rm baseline` runs the pinned official mydumper image against your source and converts the dump with the core CLI image into `/var/lib/bintrail/baselines` on the state volume — exactly where the console reads baselines from (per-server "Baseline dir", or `BASELINE_DIR` in `.env` for the boot entry). `BASELINE_SOURCE_DSN`/`BASELINE_SCHEMAS` default to `SOURCE_DSN`/`SCHEMAS`, so the single-source stack needs no extra configuration; run it on demand or from cron. The dump step **fails loudly on empty or partial dumps** — mydumper exits 0 for a schema filter that matches nothing and for schemas the dump user cannot read, which would otherwise convert into an incomplete baseline and silently wrong Time-travel results.

### Fixed
- **S3 baseline reads now use the full AWS credential chain, like every other S3 access** (#459). Uploads and archived-event reads always used the AWS SDK chain (env keys, `~/.aws` profiles, EC2/ECS/EKS IAM roles), but the DuckDB-driven baseline paths (`--baseline-s3` finds/reads/listings, `query --include-snapshot`, S3 Parquet metadata, `archive reconcile --deep` footers) resolved env keys at best — a role-only host failed exactly there while everything else worked. They now set up the DuckDB `aws` extension's `credential_chain` secret automatically, best-effort with honest logging: hosts where the extension cannot install fall back to env-key resolution (a WARN fires when no env keys exist — the read is then doomed to a generic 403), and `BINTRAIL_DUCKDB_NO_AWS_EXT=1` skips the setup entirely for proxies that blackhole the extension registry (where the install attempt can stall for minutes). SSO-session profiles have a known upstream gap (duckdb-aws#125). Docs now describe the real per-path credential story, including a version/platform-correct airgapped recipe.
- **`bintrail baseline` no longer reports success for a dump with no table data** (#461). A metadata-only dump — easy to produce with mydumper exiting 0 — converted into nothing with a green `tables: 0`; the missing baseline surfaced weeks later as "no baseline snapshot found", or as Time-travel silently reconstructing from an older snapshot. Zero discovered tables and a `--tables` filter that matches nothing are now errors naming the likely cause; a cancelled run can no longer publish a partial snapshot under a successful exit; and reconstruct **warns when the requested table is absent from the newest snapshot** and an older one is used (local sources; the S3 path cannot detect this yet — #466).
- **`bintrail dump` no longer hands mydumper 0.11–0.17 flags they reject** (#460). The light-locking flags (`--sync-thread-lock-mode`/`--trx-tables`) exist since mydumper 0.18, not 0.11 as the version gate assumed — builds in between failed with "unknown option" while the docs claimed support. The gate now sits at 0.18 (older builds get the same heavier-locks fallback as 0.10), and the docs' pin examples no longer recommend a pre-0.18 image on the unprobed Docker path, which reproduced the bug verbatim.
- **`bintrail doctor`'s FK-cascade advice no longer contradicts the ingestion gate** (#465). The remediation said "bintrail will index events fine" — but `stream`/`watch`/`up` (and `index` runs given `--source-dsn`) refuse to start while cascade constraints exist. The text now states the real enforcement and names the paths that genuinely index despite cascades (`index` without `--source-dsn`, `agent`).

## [0.13.0] - 2026-06-10

### Added
- **Configure the built-in rotation policy from the console UI.** A new ⌘K → "Configure rotation…" panel (under `bintrail-console watch`) edits the daemon-global retention window, cycle interval, and future-partition headroom; changes apply **live** — the rotation loop re-reads them on its next cycle, no restart needed (a changed interval re-tunes the schedule). The policy is stored once in the local console registry (the only file the console writes) and falls back to the `--rotate-*` flags / `BINTRAIL_ROTATE_*` env when unset, which the panel shows as the effective default. Rotation is one shared schedule across every monitored source (the loop is a single ticker), so the window applies globally; disabling rotation entirely stays a daemon-level decision (`--rotate-retain off`). The standalone read-only console hides the panel and refuses the write — only the daemon running the loop accepts it.
- **Configure S3 archiving for a monitored source from the console UI.** A new "Archive to S3" field on a monitored server (under `bintrail-console watch`) takes an `s3://bucket/prefix/` destination; the daemon's built-in rotation then uploads that source's rotated partitions as Parquet **before** dropping them, so the forensic record survives the retention window and the console auto-discovers it on the next query — no extra setup. Partitions are staged locally (`--archive-staging-dir` / `BINTRAIL_CONSOLE_ARCHIVE_STAGING`), uploaded with the ambient AWS credential chain (`AWS_*` / `~/.aws` / instance role), then pruned. Archiving begins once a source's identity is resolved; until then it rotates drop-only and the protect-unarchived guard never drops un-uploaded data. The Docker Compose stack now passes `AWS_*` through from `.env` and stages under the state volume. (Archive S3 is the write side; the existing `Baseline S3` field is the read-side Time-travel input — they are distinct.)

## [0.12.0] - 2026-06-10

### Changed
- **BREAKING (console auth) — username+password is now the only login path; the access token is opt-in automation only.** This supersedes the additive token-or-password model from 0.11.0. On a fresh console the **first browser visit creates the password** (a loopback-only, self-disabling `POST /api/auth/setup` endpoint); every later visit signs in. **No access token is generated anymore** — set `--token` / `CONSOLE_TOKEN` explicitly only if a script needs the console API. A non-loopback bind with no credential is refused unless you set a password, a token, or `--allow-setup` (asserting the bind is access-controlled — what the Docker stack does, since it binds `0.0.0.0` in the container but publishes on the host's loopback). The Docker Compose entrypoint no longer generates or persists a token; on first `up` you create the password in the browser.
  - **Upgrading from 0.11.0?** If you used a password, nothing changes (you get the login form). If you relied on the auto-generated compose token (a bookmarked `?token=` URL, or token-based API automation with no explicit `CONSOLE_TOKEN`), that token is gone after this upgrade: the browser lands on the create-password screen instead, and automation must now pin `CONSOLE_TOKEN` in `.env`. Reset a forgotten password from the host shell: `docker compose exec -it bintrail bintrail-console user set-password`.
- New flag/env `--allow-setup` / `BINTRAIL_CONSOLE_ALLOW_SETUP` (on `serve` and `watch`) permits browser first-run setup on a non-loopback bind that is access-controlled by other means; a loud startup warning fires while setup is open off-loopback.

### Fixed
- **The console sign-in overlay no longer renders with its text clipped, and the login form is tightened** (#452, #453). The login gate and password dialog (new in 0.11.0) appended their content directly to a panel whose padding lived on sub-elements they didn't use, so the text jammed against the edges and `overflow: hidden` clipped it; the panel now carries its own padding. The submit button is full-width and the status message uses the UI font (not the servers-form monospace), and a 401 in token mode reads "This access token is no longer valid." instead of the session-flavored "Session expired" (token mode has no session).

## [0.11.0] - 2026-06-10

### Added
- **Optional username + password login for `bintrail-console`** (#451), layered on top of the existing access token — set one with `bintrail-console user set-password` (also `remove`/`status`) and a sign-in form replaces the `?token=` URL for humans. The credential is a single bcrypt (cost 12) hash in a `0600` YAML file (`~/.config/bintrail/console-auth.yaml`, override with `--auth-file`/`BINTRAIL_CONSOLE_AUTH`); the password is never accepted via flag or environment variable (it would leak through `docker inspect`/`ps`/`/proc`), only interactively or `--password-stdin`. A successful login mints an in-memory session (24 h absolute / 8 h idle, revoked on logout, password change, and restart) the browser uses as its bearer credential — nothing session-shaped is written to disk. **The static `--token` keeps working unchanged** as the automation credential; password auth is additive, single-user (multi-user/SSO stays in dbtrail). Login and password-change are bcrypt-verified in constant time with brute-force throttling (per-IP and global windows, `Retry-After`, no lockout) and no username enumeration. Rotate from the console (⌘K → "Change console password", which revokes every other session) or re-run `user set-password`. The same surface is available under `bintrail-console watch` via the `--console-auth-file` flag.
- **TLS for `bintrail-console`** (#451): `--tls-cert`/`--tls-key` (and `--console-tls-cert`/`--console-tls-key` on `watch`, or `BINTRAIL_CONSOLE_TLS_CERT`/`_KEY`) serve the console over HTTPS. Static certificate files only — rotation is a restart, no ACME. A password configured on a non-loopback bind over plain HTTP now warns loudly at startup; terminate TLS here or at a reverse proxy. `watch` also gains `--console-allowed-hosts` (`BINTRAIL_CONSOLE_ALLOWED_HOSTS`) so the reverse-proxy topology works there too, not only on `serve`.

### Changed
- **The Docker Compose console token now persists across restarts** (#451). The quickstart's auto-generated token is stored once in the `bintrail-state` volume instead of being regenerated on every container start, so bookmarked `?token=` URLs and token-based automation survive redeploys. Pinning your own `CONSOLE_TOKEN` in `.env` still wins. To expose the console beyond loopback, set a console password (`docker compose exec -it bintrail bintrail-console user set-password`, ideally behind TLS) or a stable token.
- Three new static security response headers on every console response — `Referrer-Policy: no-referrer` (keeps the bootstrap `?token=` out of `Referer`), `X-Content-Type-Options: nosniff`, and `X-Frame-Options: DENY`. Login additionally requires `Content-Type: application/json`, which a cross-site HTML form cannot send — closing login-CSRF without cookies or CORS.

### Fixed
- **The console SPA no longer 404s on reload or a deep link** (#449). The redesigned console routes with the History API (`/events`, `/recover`, …); reloading or opening one of those paths directly returned a bare `404 page not found`. The server now serves the app shell for extensionless non-asset paths and the frontend restores the view, while missing real assets still 404.
- **The add-server form no longer looks like it requires index-connection details** (#450). The always-optional "Index connection" section (it is auto-provisioned for a monitored source) is collapsed behind an "Advanced — bring your own index" toggle, and the required Name field moved out of it; the zero-terminal "+ Add server" path is now just a name plus the source.

## [0.10.1] - 2026-06-09

### Changed
- **New visual identity + brand prose: bintrail → dbtrail** (#448). README and docs now present the project as **dbtrail** with the new DB Trail header art; the web console ships the new app icon (favicon + sidebar mark) and lockup, its title/brand read "dbtrail console", and event/SQL exports download as `dbtrail-*`. **Every technical name is untouched**: binaries (`bintrail`, `bintrail-console`, `bintrail-mcp`), packages, image names, `BINTRAIL_*` env vars, config paths, compose service/volume names, and stored identifiers — existing installs and scripts keep working as-is. A naming note in [docs/install.md](docs/install.md) spells out the project-vs-engine distinction; prose referring to the managed service now says "dbtrail.com" to keep the open-core ship-vs-operate boundary unambiguous.

## [0.10.0] - 2026-06-09

### Changed
- **BREAKING for Go module consumers only: the module path is now `github.com/dbtrail/dbtrail`** (#447). The repository moved to <https://github.com/dbtrail/dbtrail> — the bintrail brand is retired and the project continues as **dbtrail open source**. Install with `go install github.com/dbtrail/dbtrail/cmd/bintrail@latest` from now on. On the old module path, builds pinned to ≤ 0.9.1 keep working forever (the proxy serves them immutably), while `@latest` now fails loudly with `module declares its path as: github.com/dbtrail/dbtrail` — the error itself points at the new path. Every old URL — clone, releases, raw compose — keeps working through GitHub's permanent redirects. **Nothing else changes**: binary names (`bintrail`, `bintrail-console`, `bintrail-mcp`), release artifact names (`bintrail_*`), deb/rpm package names, Docker image names (`ghcr.io/dbtrail/bintrail*`), `BINTRAIL_*` environment variables, config paths, and all on-disk data identifiers are exactly as before, so existing installs, scripts, and compose files keep working untouched.

## [0.9.1] - 2026-06-09

### Changed
- **The project is moving to `github.com/dbtrail/dbtrail`** — the bintrail brand is retired in favor of dbtrail open source. This is the final release under the `github.com/dbtrail/bintrail` module path; its `go.mod` now carries the Go module deprecation notice pointing at the new path, so `go install github.com/dbtrail/bintrail/...@latest` keeps working but prints the redirect. Nothing else changes in this release: binary names, package names, Docker image names, `BINTRAIL_*` environment variables, config paths, and all on-disk/data identifiers are untouched. GitHub serves permanent redirects from the old repository URLs (clone, releases, raw).
- GoReleaser `project_name` is pinned to `bintrail` so release artifact names (`bintrail_<version>_<os>_<arch>.tar.gz`, deb/rpm) stay stable across the repository rename instead of silently following the new repo name.

## [0.9.0] - 2026-06-09

### Changed
- **BREAKING — the web console is now its own binary, `bintrail-console`; the core `bintrail` CLI is UI-free** (#442, #445, #446). `bintrail console` → **`bintrail-console serve`** (same flags, same env vars) and `bintrail up --console` → **`bintrail-console watch`** (the combined preflight + init + stream + console + multi-server control-plane daemon; same `--console-*`, `--baseline-*`, and `--rotate-*` flags and `BINTRAIL_*` env vars). Core `bintrail up` is the classic stream-only quickstart again — every `--console*` flag is gone. Why: dbtrail (the SaaS) runs the bintrail executable and has its own UI, so the core carrying an embedded web console was dead weight and attack surface; the split follows the existing `bintrail-mcp` multi-binary pattern, and a structural test now guarantees the core binary never links the console again. The control-plane supervisor, its states (`pending`/`stalled`/`lost_position`), the circuit breaker, replica detection, and per-source rotation coverage all move to `watch` unchanged.
  - **Docker Compose users**: the quickstart now runs `bintrail-console watch` from the new `ghcr.io/dbtrail/bintrail-console` image. An OLD `docker-compose.yml` crash-loops after pulling this release (`unknown flag: --console`) — re-download the compose file; your `.env` and all data volumes (index data/secret, saved console servers) carry over unchanged.
- `bintrail config init` no longer advertises the `BINTRAIL_CONSOLE_*` variables — they belong to the `bintrail-console` binary, which reads the same `.bintrail.env`/`config.env` files.

### Added
- **`bintrail-console` ships as its own artifacts** (#446): a `ghcr.io/dbtrail/bintrail-console` Docker image (multi-arch, cosign-signed, entrypoint `bintrail-console`), a separate `bintrail-console` deb/rpm package (the `bintrail` package stays UI-free), and the binary in every release archive. Build from source with `make build-console` or `docker build -f Dockerfile.bintrail-console .`.

## [0.8.7] - 2026-06-08

### Added
- **`doctor` now projects index disk capacity** (#419): a new "Index disk capacity" check measures the index's real write rate from the last 24 hours of partition statistics (events/day × bytes/event — the docs/capacity.md formula with live numbers) and projects the steady-state footprint over the retention window (`--retain`, default `30d`; `off` if you don't rotate). When the index MySQL is on the same host (loopback/socket DSN), it also probes the datadir's free space: FAIL when the projection exceeds it (with the emergency-rotate remediation), WARN above 70%. With rotation disabled it warns that the index grows unbounded at the measured rate, including days-until-full when free space is measurable. Runs in `bintrail up`'s preflight with the configured `--rotate-retain` — the compose default gets the sizing preflight on every boot. Fresh indexes with under a few hours of history SKIP rather than guess.

### Changed
- **Support boundary rewritten to "ship vs operate"** (#422). Now that the bundled index is a real MySQL 8.4 system of record (#418), the docs that called it "evaluation-grade" would mislead. SUPPORT.md, the README callout, and install.md are re-scoped: bintrail **ships** the pinned 8.4 image (its build, tuned defaults, generated credentials, and documented `8.0→8.4` upgrade path are ours), but never **operates** your running instance — disk, backups, restore, corruption recovery, and *executing* upgrades are yours in the free core (dbtrail operates it for you in the paid tier). The supported surface is two CI-tested cells: any MySQL 8.0+ via `--index-dsn` (BYO) and the pinned 8.4 image we bundle. The bintrail *binary* still never installs or supervises a mysqld on the host — that line is unchanged; what changed is that the *project* now ships a containerized one. The dbtrail upsell verb is "operates", not "hosts"; the docker.md `8.0→8.4` note states loudly that the 8.4 datadir is non-downgradable.
- **The bundled compose index is now MySQL 8.4 LTS with a generated password** (#418). The quickstart's index graduates from an eval-grade `mysql:8.0` with a static `changeme` to a pinned `mysql:8.4.9` on a **new** `bintrail-index-data` volume (an 8.4 server auto-upgrades an 8.0 datadir irreversibly, so the old `index-mysql-data` volume is deliberately not reused — existing eval users re-index into the new volume, or `mysqldump`/reload to carry the old data). A one-shot `index-init` service generates a random root password into a `bintrail-index-secret` volume on first boot (or takes one from `INDEX_MYSQL_ROOT_PASSWORD` set before first boot); both the index MySQL and bintrail read it from there — no static default password on a volume holding the forensic record. 8.4 defaults to `caching_sha2_password` (native_password disabled); bintrail's driver negotiates it over the plaintext Docker network with no config change. The compose header now states the index volume is the operator's **system of record** (back it up; bintrail ships it but does not operate it). The 4-line quickstart and the BYO `INDEX_DSN` opt-out (contract floor still MySQL 8.0+) are unchanged. See [docs/docker.md](docs/docker.md).
- **BEHAVIOR CHANGE — `bintrail up` now rotates the index by default** (#420): a built-in loop drops partitions older than `--rotate-retain` (default `30d`) every `--rotate-interval` (default `1h`) and keeps `--rotate-add-future` (default 3) future hourly partitions ready — on the boot index database and on every per-source database the control plane provisions. Previously an unattended `up` (the compose quickstart) grew the index unbounded until the volume filled, taking the forensic record with it. The settings are announced loudly at boot; disable with `--rotate-retain off` (or `BINTRAIL_ROTATE_RETAIN=off`).
  - **Upgrading an existing deployment?** Two guards protect pre-existing history. (1) **Upgrade guard**: if you never set `--rotate-retain` and the index already holds history extending beyond *twice* the default window (>60d — the signature of a deployment that predates this feature), the loop refuses to drop anything, logs an Error each cycle, and waits for an explicit choice (`--rotate-retain 30d` to confirm, `90d` to keep more, `off` to disable). History between 30d and 60d old *is* dropped on upgrade — set the flag before upgrading if you need it. (2) **Archive guard**: if the index has any archiving history (`archive_state` rows — e.g. your own `rotate --archive-dir` cron), the built-in rotation only drops partitions that are already archived and defers the rest to your archiving flow — it is never the first to destroy unarchived data.
  - If rotation makes no progress it should have — failing, deferring to a stalled archiving flow, or any mix of the two — for 3 consecutive cycles, the loop escalates to an explicit Error: detection that the index is growing unbounded, since deleting unarchived data would be worse. The explicit `bintrail rotate` command is unchanged.

## [0.8.6] - 2026-06-07

### Fixed
- **Compressed transactions (`binlog_transaction_compression=ON`) are now indexed — previously every compressed transaction was silently skipped** (#414). MySQL 8.0.20+ wraps each transaction's events (BEGIN + table map + rows + commit) in a single zstd-compressed `Transaction_payload` event, with only the GTID event outside the wrapper — so bintrail saw the GTID, advanced the checkpoint, and indexed **zero rows**, with healthy-looking metrics over an empty index. All modes were affected equally (`index`, `stream`, `up`, `agent`). Both parser switches now recurse into the payload's pre-decoded inner events; indexed rows carry the payload event's file coordinates (inner headers have no usable positions — MySQL zeroes them, and deriving start_pos from them would underflow). Verified end-to-end against MySQL 8.0.46, where *every* transaction shape gets wrapped when compression is ON — even tiny or incompressible ones — so a compression-enabled source previously yielded nothing at all. If you enable compression: 1GB of binlog now represents ~2.5-4x more logical row-changes, so expect index partitions to scale accordingly (`performance_schema.binary_log_transaction_compression_stats` reports your actual ratio). Known edge: NONE-typed payload wrappers (never observed on 8.0.46) would fail loudly at parse, not silently.

## [0.8.5] - 2026-06-06

### Added
- **Per-source Prometheus metrics — N monitored streams are now individually observable** (#402). Every `bintrail_stream_*` metric carries a `source` label (the monitored entry's ID under the control plane; the resolved `bintrail_id` for a standalone `bintrail stream`), so counters no longer conflate across sources and the two gauges (`last_event_timestamp_seconds`, `replication_lag_seconds`) no longer clobber each other last-writer-wins. Under `bintrail up --console` the **daemon serves one `/metrics` endpoint** (`--metrics-addr`) covering all supervised streams — per-stream endpoints would fight over the bind. The bind is now synchronous and fail-fast, like the console's: an operator who asked for metrics gets a hard error on a bad address instead of a silently absent endpoint. Alert per source with `bintrail_stream_replication_lag_seconds{source="<entry-id>"}`.
- **The add-server preflight warns when the new source looks like a replica (or duplicate) of an already-monitored one** (#402). Monitoring a primary and its replica — or the same server twice — silently double-indexes every row change. The supervisor's doctor now compares the candidate's GTID lineage (`@@server_uuid`, `@@gtid_executed`) against each monitored entry's recorded identity and accumulated executed set, read from the per-source index databases (the other sources are never contacted): it catches *replica-of-monitored*, *primary-of-a-monitored-replica*, and *same-server-added-twice*. Warn-only per the approved decision — an amber card, never a block; needs `gtid_mode=ON` (explicit skip otherwise). Doctor warn cards are now actually **shown** when monitoring starts (previously the report was only rendered on a failed preflight, so a warn-only report started the stream and discarded the card).
- **Monitor states that stop lying: real `pending`, plus `stalled` and `lost_position`** (#402). The console badge previously said `RUNNING` the moment the supervisor launched the stream goroutine — while it was still connecting, snapshotting, and discovering its start position (the #407 CI-flake postmortem). `pending` now covers launch through the stream's first checkpoint (or first indexed batch); only proven liveness flips it to `running`. Two unhealthy-but-alive conditions that used to be invisible get their own states: `stalled` (connected but no checkpoint/batch progress for 5+ minutes — the checkpoint ticker fires even with zero events, so an idle-but-healthy source never trips it) and `lost_position` (an unfillable binlog gap forced an auto-advance: events in the gap are permanently lost — previously a single log line under a green badge). The lost-position record is **durable** (`stream_state.gap_lost_at`/`gap_lost_detail`): it survives daemon restarts (the advanced checkpoint means a restart sees no gap, so an in-memory flag would silently un-surface the loss) and is cleared only by an explicit monitor Stop — the operator's acknowledgment.
- **Crash-loop circuit breaker for supervised streams** (#402). A stream that crash-loops continuously for 6 hours (no healthy run in between) stops retrying and reports a permanent `failed` with a "gave up" message, releasing its advisory lock — a misconfigured source no longer retries forever. Press Start (or restart the daemon) to re-arm; a run that survives 10 minutes resets the breaker clock, so a flapping-but-recovering stream never trips it.

### Fixed
- **`bintrail doctor` no longer prescribes GRANTs when the schema is simply empty** (#402). Zero visible tables has two very different causes, and the doctor conflated them: the schema-visibility check now distinguishes "schema exists but contains no tables yet → create at least one table first" from "schema not visible → fix grants / check the name", and the snapshot error (`no columns found…`) gives the same guidance. Starting monitoring on an empty schema previously failed the initial snapshot and retry-looped with a remediation pointing at permissions the operator already had.
- **A stream's first checkpoint now persists the resolved start position** instead of an empty binlog file and position 0 — on a fresh start with an idle source, the previous ticker checkpoint wrote a checkpoint row that couldn't be resumed from.

## [0.8.4] - 2026-06-06

### Fixed
- **The `bintrail-demo` image build was broken by its own rename**: `.dockerignore` excludes `demo/` (the dev stack never belonged in the main image's build context), and the rename moved the demo image's build files exactly there — the v0.8.3 `demo-image` workflow failed with `"/demo/image/my.cnf": not found`. Re-included via a `!demo/image` negation (verified against BuildKit: a negated child of an excluded directory is honored).

## [0.8.3] - 2026-06-06

### Changed
- **`bintrail-appliance` is now `bintrail-demo`.** The single-container evaluation image is renamed: pull `ghcr.io/dbtrail/bintrail-demo` (the repo path moves from `appliance/` to `demo/image/`, the doc from `docs/appliance.md` to `docs/demo.md`, the workflow to `demo-image.yml`). "Appliance" said nothing about what it is; "demo" does. The old `bintrail-appliance` GHCR package keeps serving its already-published tags, but new releases publish only the new name.

## [0.8.2] - 2026-06-06

### Added
- **Zero-config install — `SOURCE_DSN` is no longer required to get started.** `bintrail up --console` now starts **source-less**: index init + web console + control plane, with sources added afterwards from the UI ("+ Add server" runs the preflight, provisions a per-source index, and starts streaming — and resumes them on restart). The Docker Compose quickstart drops to three commands with nothing to edit (`curl` the compose, `up -d`, open the URL from the logs); `SOURCE_DSN` and `CONSOLE_TOKEN` become optional `.env` knobs instead of prerequisites. On a fresh supervisor with nothing watched, the console opens the Servers screen automatically so "+ Add server" is the first thing the operator sees. Boot robustness: under `--console`, `up` now waits (up to 90s, with progress) for the index MySQL to accept connections instead of dying into a restart loop — the official mysql image briefly accepts-then-drops connections during first initialization, which previously caused ~5 container restarts and a new console token per restart on a cold compose boot.

## [0.8.1] - 2026-06-05

### Added
- **"+ Add server" now monitors for real — the control plane ships for one source at a time.** Under `bintrail up --console`, adding a server with a **source MySQL** in the console runs the `bintrail doctor` preflight inline (failures return as remediation cards in the form), provisions a dedicated per-source index database (`bintrail_idx_<id>` on the daemon's index server — created, table'd, and schema-migrated by the daemon, so the console's request handlers still never run DDL), and starts a supervised binlog stream: doctor green = streaming within the minute, zero terminal. The supervisor reconciles desired state at boot (restart the daemon and monitoring resumes from each stream's checkpoint), holds a per-entry advisory lock so a second daemon refuses to double-stream, retries failing streams with exponential backoff (15s→5m, visible as a `FAILED` badge with the scrubbed error), and refuses deletes or source re-pointing of a running entry (409) until an explicit Stop. New verbs `POST /api/servers/{id}/monitor/start|stop` and `GET /api/servers/{id}/monitor`; the `monitor` capability gates the UI. The standalone read-only `bintrail console` keeps none of this (403). Built on the `streamOne` extraction (#398) and the registry source fields (#399); per-source-database isolation is the approved architecture — each monitored source's checkpoints and snapshots live in their own DB, and the server switcher lists it like any connection.
- **Control-plane groundwork: source-monitoring fields in the console server registry.** Registry entries can now carry a source configuration — `source_dsn` (replication credentials, masked and keep-password-merged with exactly the index-DSN discipline; `source_dsn: ""` clears it), `source_server_id`, `schemas`, and `monitor_desired` — persisted via `POST/PUT /api/servers` and reported masked (`source_host`/`source_port`/`source_user` + `has_source_password`). `GET /api/capabilities` gains a `monitor` key, true only under `bintrail up --console` (the write-capable daemon); the standalone read-only console always reports false. No monitoring starts yet — the supervisor and its start/stop verbs are the next phase (the `streamOne` extraction in #398 is the engine they will run).
- **Zero-friction Docker Compose quickstart.** The root `docker-compose.yml` now delivers the full DBA experience in three commands: `cp .env.example .env` (set `SOURCE_DSN` — the only required value), `docker compose up -d`, open the console URL printed in the logs. It pulls the published `ghcr.io/dbtrail/bintrail` image (no Go toolchain, no local build — `build: .` remains a comment-toggle), runs `bintrail up --console` (preflight → init → auto-snapshot → live stream → web console) instead of a hand-rolled init/snapshot/stream script, bundles a persisted index MySQL, publishes the console on the host loopback with a per-boot generated token (pin `CONSOLE_TOKEN` to keep it stable), persists saved console connections in a `bintrail-state` volume, and no longer needs `SERVER_ID` (`up` derives one). The previously-referenced-but-missing `.env.example` now exists, root `.env` is gitignored, and the image pre-creates `/var/lib/bintrail` owned by the non-root user so the state volume is writable. README gains a "Run everything with Docker Compose" section; `docs/docker.md` updated to match.
- **Console server manager — add, edit, delete, and switch between servers from the UI.** The console header gains a server switcher and a Servers screen managing named connections to bintrail index databases. The registry is a **local YAML file** (`~/.config/bintrail/console-servers.yaml`, `0600`, written atomically; `--servers-file` / `BINTRAIL_CONSOLE_SERVERS`) — the only thing the console ever writes; the read-only-over-data contract is untouched, and servers added in the UI are **never schema-migrated** (the one `EnsureSchema` ALTER stays on the command-line DSN; a legacy registry index returns an actionable 422 instead). Selection rides a per-request `X-Bintrail-Server` header — stateless, so two browser tabs can watch two different servers — and connections open lazily on first selection (eager ping: a dead server fails on switch, not on the first query), single-flighted, and are closed on DSN-changing edits and on delete (a baseline-only edit keeps the connection and just recomputes its gates). Passwords never reach the browser: responses carry parsed non-secret DSN parts plus `has_password`, and an edit with a blank password keeps the stored one. Per-server Time-travel gating (the baseline/profile/archive gate is evaluated on the selected server, endpoint-enforced), a write-free test-connection probe (ping, version, latency, index-schema check, short timeout), and per-entry baseline/no-archive settings round it out. `--index-dsn` becomes optional once the registry has entries (it seeds a non-editable ephemeral `default` entry); `bintrail up --console` serves the same switcher via `--console-servers-file`. The registry file is versioned and forward-compatible: fields a newer bintrail writes (e.g. a future control plane's `source_dsn`) survive load→edit→save on an older binary, and a newer-versioned file loads read-only rather than being rewritten lossily.
- **`bintrail archive reconcile` — `archive_state` is now a rebuildable cache, not a fragile truth** (#392). Scans `--archive-dir` and/or `--archive-s3` for the self-describing Hive layout, derives the registry row each Parquet file implies, and diffs against `archive_state`: files without rows (`--repair` re-registers them — the post-index-loss rebuild), rows without files (`--prune` deletes the registry row; data files are never touched), and metadata drift (sizes always; row-count verification gated behind `--deep`, which reads Parquet footers). The default is a dry-run that exits non-zero on drift, so a cron invocation doubles as a drift monitor. Safety rules: classification is **backend-scoped** (a row referencing a backend this invocation didn't scan is reported as unverified and never pruned; repair never writes an unscanned backend's columns), rows younger than `--prune-min-age` (default 1h) are never pruned (concurrent-rotate margin), and reconcile stamps `s3_uploaded_at` whenever it confirms an S3 object — an unstamped S3 registration reads as an upload still in flight and blocks `rotate` from ever dropping that partition.
- **The README tagline is now real: bare `AS OF` on real table names through Time Travel SQL** (#385). `SELECT * FROM orders WHERE id = 1 AS OF '1 minute ago'` — time-travel syntax directly on the actual table, no `_flashback.` prefix, no hint comment — now parses in the shim (rewritten to the binlog-only `_flashback` semantics, exactly like the `/*+ DBTRAIL_AT */` hint form) and routes through a new ProxySQL rule (`rule_id` 990006) emitted by `bintrail proxysql-config`. The shape is deliberately conservative: `*`-only projection, and the `AS OF` clause must **end the statement** — both the shim's probe and the ProxySQL rule are end-anchored, which is the false-positive defense (the shim has no passthrough, so a benign query mis-routed there breaks; "AS OF" inside a string literal mid-query stays on passthrough, proven by an e2e guard test against real ProxySQL 2.6). Residual surface and the `\x3b` semicolon-escape rationale are documented in `docs/time-travel-sql.md` Limitations. The appliance's banner, smoke test, and the README "30-second evaluation" now use the bare form — the literal acceptance query of #350.

### Fixed
- **Row events immediately after a DDL are no longer silently lost** (#396). The auto-snapshot a streamed DDL triggers used to run on the CONSUMER side of the events channel, while the parser goroutine ran ahead through the buffer — so for `CREATE TABLE t; INSERT INTO t;` (every migration script ever) the parser had already decoded and skipped the INSERTs as "table not in snapshot" before the snapshot landed, and those rows were permanently missing from the index with only a stream-log WARN as a trace. The snapshot + resolver swap now runs in a **synchronous DDL hook on the parser itself** (`StreamParser.SetSyncDDLHook`): the binlog is sequential, so the parse loop blocks until the refreshed schema is in place and the very next row event decodes with the post-DDL resolver. `streamLoop` loses its dead `onDDL` parameter so the race cannot be reintroduced there. Locked by a parser-level ordering test and an integration regression that replays the exact burst against a live stream (4/4 trailing events indexed; 0/4 before the fix).
- **A failed `archive_state` read can no longer silently shrink the archive-source list** (#383, final piece). `ResolveArchiveSources` swallowed every registry-read failure — a permission error or timeout returned an empty list, and a per-row scan error silently dropped that source — while the planner kept claiming the affected hours as covered (it reads `archive_state` independently), leaving strict mode nothing to fail on. The resolver now returns an error for any registry-read failure, which strict-mode (`AllowGaps=false`) callers — `reconstruct` single-row and full-table, the shim's defaults — escalate, and permissive callers (`recover`, console events/recover, `bintrail query`, the MCP tools) log and proceed without archives, exactly mirroring their per-source fetch stance. The one deliberate exception: MySQL error 1146 (`archive_state` doesn't exist) stays a silent empty result — the table is legitimately absent on indexes created before the archive feature, and erroring would break working MySQL-only deployments.
- **A vanished S3 archive source now fails loudly instead of contributing silent emptiness** (#383, second half). `parquetquery.Fetch`'s S3 branch returned an empty success when the prefix listing matched zero files — but that listing is *date-scoped*, so zero files conflated "healthy source, legitimately empty range" with "registered source whose objects were deleted after `archive_state` was written". A zero-file scoped listing now triggers one unscoped probe of the base prefix (paginating past non-Parquet keys — a `MaxKeys=1` shortcut would false-report empty when the first key is a `_SUCCESS` marker or cosign `.sig`); a source with no Parquet anywhere returns the new `query.SourceEmptyError`, which strict-mode (`AllowGaps=false`) callers escalate exactly like any archive-source failure (#377) and `bintrail reconstruct` re-wraps with a hint pointing at `bintrail archive reconcile`. When the listing was never scoped (no usable time bounds), zero files already is the unscoped truth and errors directly.
- **An empty local archive tree no longer shadows its healthy S3 copy** (#383, first half). `ResolveArchiveSources` preferred the local path whenever the base directory merely *existed* — but `rotate` records BOTH the local path and the S3 location in the same `archive_state` row, so the standard cleanup pattern (archive locally → upload to S3 → prune the local Parquet files, leaving the tree) routed every query at a fileless local dir and never tried S3. Under strict mode (`AllowGaps=false`, post-#377) that aborted queries whose data was sitting healthy in S3. The resolver now prefers local only when the base actually holds a `.parquet` file (cheap walk, first hit wins) and otherwise falls back to the registered S3 copy, warning when an existing-but-fileless tree is skipped. A registered source is **never omitted** from the result — when nothing usable remains, the unusable local base is returned anyway, because the planner counts archived hours straight from `archive_state`: omission would hand strict mode a "covered" range with nothing left to fail on. (Still open in #383: the S3 branch's silent-empty on a no-match listing — the listing is date-scoped, so distinguishing a vanished source from a legitimately empty range needs an unscoped probe.)
- **`bintrail doctor` no longer fails its index checks when the index database doesn't exist yet** (#384). Both "Index MySQL connection" and "Index write access" pinged the full `--index-dsn` and hard-failed on MySQL error 1049 ("Unknown database") — directly contradicting their own remediation text ("The database does not need to exist yet — `bintrail init` will create it") and breaking `bintrail up`'s friction-free contract on a fresh server. On 1049 the checks now reconnect at the server level (DB name stripped from the DSN); the connection check passes with a "does not exist yet" note, and the write-access check verifies the `CREATE DATABASE` privilege via its existing probe — which now also **drops the probe-created database on exit** (best-effort: a drop failure is logged, and can only happen when the user also lacks DROP — already reported as FAIL by the table probe), so a successful diagnostic run leaves no server state behind. Non-1049 connection failures still fail as before. The appliance's seed no longer needs to pre-create `bintrail_index` (workaround removed; its boot now exercises this path for real).
- **The appliance-image workflow now triggers on `v*` tag pushes instead of `release: published`.** The release event never fired: GoReleaser creates the GitHub release using the default `GITHUB_TOKEN`, and events generated by that token never trigger other workflows (GitHub's anti-recursion guard) — discovered when v0.8.0 published everything except the appliance image. Manual `workflow_dispatch` runs now take an explicit existing tag as input, so a dispatch can never publish an unversioned branch build; `:latest` is skipped for prerelease tags (containing `-`).

## [0.8.0] - 2026-06-05

### Added
- **`ghcr.io/dbtrail/bintrail-appliance` — a single-container, evaluation-only demo** (#350). One `docker run --rm -p 6033:6033` boots MySQL 8.0 (source and index in the same instance), `bintrail up` streaming the bundled `demo` schema, the time-travel shim, ProxySQL preloaded with `bintrail proxysql-config`'s routing rules on `:6033`, and a traffic generator that mutates `demo.orders id=1` deterministically every cycle — so `SELECT * FROM _flashback.orders AS OF '1 minute ago' WHERE id = 1` (creds `demo`/`demo`) returns a previous row state within a minute of boot. Stateless by design (restart = fresh demo), amd64-only (MySQL's apt repo ships no arm64 for Debian; Apple Silicon runs it via Rosetta), published by a release-gated workflow separate from GoReleaser so an appliance build failure never blocks a release, cosign-signed. New `docs/appliance.md` + a "30-second evaluation" README section above the Quickstart. `appliance/smoke-test.sh` builds, boots, and asserts the acceptance flow end-to-end.
- **The shim's time literals now accept relative forms** — `'90 seconds ago'`, `'5 minutes ago'`, `'2 hours ago'`, `'1 day ago'` (case-insensitive, resolved against the wall clock at parse time) in every position a timestamp literal is accepted: `_flashback`/`_snapshot` `AS OF`, `_diff` `BETWEEN ... AND ...`, and the `/*+ DBTRAIL_AT='...' */` hint form. Weeks/months/years are deliberately absent (binlog retention is measured in hours and days, and month arithmetic is calendar-ambiguous). Absolute formats are unchanged; unparseable literals keep returning `ER_PARSE_ERROR` with the accepted-format list, now mentioning the relative form.
- **`bintrail up --console` can now serve the Time-travel (point-in-time reconstruct) surface** (#379). `up` gained `--baseline-dir` / `--baseline-s3` (env: `BINTRAIL_CONSOLE_BASELINE_DIR` / `BINTRAIL_CONSOLE_BASELINE_S3`, flag > env > default), threaded into the embedded console's configuration exactly as the standalone `bintrail console` does — so a single `bintrail up --console --baseline-dir <dir>` process streams live *and* answers baseline-gated reconstruct queries from the browser, instead of requiring a second standalone-console process on another port. The gating is reused verbatim (`baselineConfigured` in `internal/console/server.go`, enforced at the endpoint); `up` defines no `--profile`/`--no-archive`, so the gate reduces to baseline presence. The read-only, token-auth, and result-cap invariants are unchanged.

### Fixed
- **Strict-mode (`AllowGaps=false`) cross-source fetches now abort when *any* archive source fails to load, not only when every source fails** (#377). `query.FetchMerged` previously hard-failed only when *all* archive sources errored; with two or more sources (one per `bintrail_id` in `archive_state`), a single broken source was demoted to an `slog.Warn` and its deltas silently dropped — undetectable downstream, because the planner validates `archive_state` coverage *before* the fetch, so the affected hours still read as "covered". Affected strict-mode callers — `bintrail reconstruct` (single-row and full-table), the `bintrail shim` virtual schemas (default `--allow-gaps=false`), and the console's `GET /api/reconstruct` — could fold an incomplete delta set and report success. Any archive-source failure under strict mode is now a hard error naming the failed source. Permissive callers (`bintrail recover`, console events/recover, and `bintrail query`'s own warn-and-continue archive loop) keep their best-effort behavior unchanged.

### Changed
- **License changed from Business Source License 1.1 to the Apache License, Version 2.0.** `LICENSE` now carries the verbatim Apache-2.0 text and a new `NOTICE` file records the copyright attribution (`(c) 2025 Daniel Guzman Burgos`), as is conventional under Apache-2.0. The README and CONTRIBUTING license sections and the `.goreleaser.yaml` package/image `license` metadata (`nfpms`, OCI `org.opencontainers.image.licenses` labels) are updated to `Apache-2.0`. This drops the BUSL Additional Use Grant restrictions and the four-year Change Date — bintrail is now permissively licensed for any use, including commercial. The CLA already reserved the maintainer's right to relicense, so existing contributions are covered.

## [0.7.13] - 2026-06-01

### Added
- **Full-table `_snapshot` time-travel queries now reconstruct the complete table at AS OF** (#362). A no-WHERE `SELECT * FROM _snapshot.<table> AS OF '<ts>'` through `bintrail shim` previously returned only rows with binlog activity in the retained window, silently omitting rows that existed at that instant but were never touched. It now merges the `bintrail baseline` snapshot at-or-before AS OF with the post-snapshot binlog deltas across the whole table — never-touched baseline rows pass through, rows updated/inserted after the baseline take their latest image, and rows deleted after it drop out — reusing the same merge engine as the offline `bintrail reconstruct`. When a baseline merge isn't possible (no baseline source configured, an unsupported or unresolvable primary key, or no baseline at-or-before AS OF) it degrades to the binlog-only `_flashback` behaviour instead of failing, emitting a `Warn` so the degradation is visible. Buffered output stays bounded by the existing full-table row cap (`ER_TOO_BIG_SELECT`).

### Fixed
- **Temporal primary-key baseline lookups no longer silently miss on non-UTC hosts** (#359). Single-row `_snapshot` queries and `bintrail reconstruct --pk` bind the PK value as a string against the typed baseline Parquet column; for `DATETIME`/`TIMESTAMP` PKs (stored UTC-anchored, read back by DuckDB as `TIMESTAMP WITH TIME ZONE`) the string→timestamp cast used the host OS timezone, so on a non-UTC host the row silently failed to match and the lookup fell back to binlog-only. The DuckDB session is now pinned to UTC, making the match deterministic on any host, and `DATETIME`/`TIMESTAMP`/`DATE` PKs are now admitted by the single-row baseline lookup. Also corrects `docs/time-travel-sql.md`, which overclaimed that the no-WHERE form returns "every row that existed at that instant" for both virtual schemas (#356).

## [0.7.12] - 2026-05-29

### Added
- Two new commands collapse the onboarding wall from five steps to two: **`bintrail doctor`** and **`bintrail up`**. `doctor` runs preflight checks against the source MySQL (connection, `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL`, binlog retention, `REPLICATION SLAVE`/`CLIENT` grants, FK-cascade warning, schema visibility) plus optional index-DB write access when `--index-dsn` is given, and reports each as PASS/FAIL/WARN/SKIP with **copy-pasteable remediation** (the GRANT, `SET GLOBAL`, or my.cnf snippet that fixes it) instead of a one-line error. Exit code is 0 only when every required check passes; warnings never fail the run, so it is safe in CI (`--format json` for machine consumption). `up` chains preflight + `init` (idempotent table creation) + `stream` (auto-snapshots the schema if none exists, resumes from the last checkpoint) behind a single invocation; `--skip-doctor` bypasses the preflight. When `--server-id` is omitted it is derived deterministically from a SHA-256 of the source `host:user:dbname`, so the same DSN yields the same ID every run for clean restart/resume. The underlying `init`/`snapshot`/`index`/`stream` commands remain available unchanged.
- Release pipeline now publishes a **multi-arch Docker image** (`linux/amd64` + `linux/arm64`) to `ghcr.io/dbtrail/bintrail` on every `v*` tag, removing the Go-toolchain dependency for the most common install path. The `.goreleaser.yaml` gained `dockers`/`docker_manifests` (one image per arch stitched into a `:{version}` and `:latest` manifest), `nfpms` (deb + rpm assets attached to the GitHub release, no external repo required), `sboms` (syft SBOM per archive), and `signs`/`docker_signs` (cosign keyless signing of checksums and image manifests via GitHub OIDC). The published image uses a dedicated `Dockerfile.goreleaser` that copies the binaries GoReleaser already cross-compiled, rather than rebuilding from source like the manual-build root `Dockerfile` — the two coexist so `docker build .` keeps working. The release workflow adds `packages: write` + `id-token: write` permissions, QEMU/Buildx setup (the arm64 image's `RUN` layer executes under emulation on the amd64 runner), GHCR login, and the cosign/syft installers, and pins `goreleaser-action` to `~> v2` instead of `latest`. Homebrew tap publishing is staged but **deferred**: it needs a `dbtrail/homebrew-tap` repo plus a `HOMEBREW_TAP_GITHUB_TOKEN` PAT secret (the default `GITHUB_TOKEN` cannot push to a second repo) — the ready-to-enable `brews` block is committed commented-out in `.goreleaser.yaml`. Prereleases are gated so an RC tag is safe to test with: `release.prerelease: auto` marks `-rc*` tags as GitHub prereleases, and `skip_push: auto` on the `:latest` manifest keeps a prerelease from claiming the tag anonymous users pull (version-specific tags still push, so the RC fully exercises the pipeline). The full pipeline was validated end-to-end by the `v0.7.12-rc1` prerelease (multi-arch build, cosign signing, and GHCR push all succeeded; `:latest` correctly stayed put). Anonymous `docker pull` additionally requires a one-time flip of the GHCR package to public (see `docs/docker.md`).

## [0.7.11] - 2026-05-24

### Added
- `bintrail query` accepts `--order ASC|DESC` (default `ASC`) to control the sort direction applied **before** `--limit`, so `--order DESC --limit N` returns the **N newest** events instead of the N oldest re-sorted in display order. Pre-fix the SQL emitted `ORDER BY event_timestamp, event_id` (no direction = ASC) for every caller; the dbtrail SaaS agent applied an in-memory sort after the fact, which fooled the table renderer but returned the wrong page identity — `--order DESC --limit 100` against a table with 1M events would silently return events 1..100 sorted descending, never the latest 100. The fix is threaded through every code path that calls `ORDER BY` in the query pipeline so DuckDB archive reads, MySQL live queries, and the cross-source merge all honor the requested direction: `internal/query/query.go:buildQuery` (live MySQL — both the simple path and the LimitPerPK ROW_NUMBER variant), three `internal/parquetquery/parquetquery.go` builders (glob, file-list, single-file backwards-compat), and `internal/query/merge.go:MergeResults`/`MergeAndTrim` (cross-source dedup + final sort). Sort keys both follow the requested direction (`event_timestamp <dir>, event_id <dir>`) so the ordering is total and deterministic across timestamp collisions — the original outer ORDER BY emitted `event_timestamp ASC, event_id ASC` implicitly; reversing only one key would have silently picked wrong tie-break winners and broken the `LimitPerPK` reverse-walk precondition. The inner ROW_NUMBER `ORDER BY event_timestamp DESC, event_id DESC` clauses (live MySQL ROW_NUMBER and DuckDB QUALIFY) stay fixed at DESC regardless of caller direction because their semantic is "latest N events per PK", not "first N in requested direction". `MergeAndTrim` sorts ASC internally for the per-PK trim (preserving `LimitPerPK`'s precondition) then re-sorts in the caller's direction before applying the global cap so DESC + Limit slices from the correct end of the list. Validated by a new integration test asserting that ASC + Limit=N and DESC + Limit=N against a > 2N-row dataset return STRICTLY DISJOINT event_id sets — the disjoint-row-set assertion is what would have caught the bug originally. Unit tests pin every SQL-builder branch (live MySQL outer + inner, all three Parquet builders, MergeResults/MergeAndTrim direction handling); `OrderDirection` is exported so the SaaS layer normalises with the same rule. Companion to nethalo/dbtrail#1511 — the SaaS agent now forwards `--order=` to the CLI instead of post-sorting in memory (#1511).

### Fixed
- `bintrail agent` BYOS-mode `recover` commands over the WebSocket bridge now honour the `gtid` field in the SaaS-forwarded payload. `RecoverRequest` had no `GTID` field, so JSON unmarshal silently discarded `{"gtid": "<target>"}` and the handler fell back to time-only filtering — producing reversal SQL for unrelated events at the same timestamp. On a tool that emits production SQL, returning `success: true` with the wrong scope is a critical correctness/safety regression. `RecoverRequest.GTID` now carries the `json:"gtid,omitempty"` tag the SaaS sends; `DefaultHandler.HandleRecover` propagates it into `query.Options{GTID:...}`, which the three data sources (in-memory buffer, MySQL index DB, parquet archives) already honour — this change just routes the value through. Zero-value `TimeStart`/`TimeEnd` are now skipped instead of passed verbatim, matching the SaaS-side change that drops the 24h-default-window clamp when only `gtid` is supplied (a gtid can reference an event arbitrarily far in the past — clamping would silently drop it). A fail-loud guard at the top of `HandleRecover` rejects requests where both GTID and time window are empty: without it the previous code would build `query.Options{Limit: 1000}` with no predicates and happily return reversal SQL for the last 1000 events in the index — the exact silent-fallback shape that surfaced the bug originally. Regression pinned by `TestHandleRecover_filterByGTID` (three events at the same timestamp, only one carrying the requested GTID — reversal SQL references only that event's PK), `TestHandleRecover_gtidNoMatch` (non-existent GTID → no recovery statements, no fallback to full-table scope), `TestHandleRecover_gtidNoMatch_largeFallback` (1500 events in buffer above the handler's 1000-row cap, broad time window, non-matching GTID — verifies the filter wasn't bypassed at scale), `TestRecoverRequestJSON_gtid` (pins `"gtid"` lowercase wire tag so a future typo fails loud), and `TestRecoverRequestJSON_backwardCompat` (older SaaS callers without the key still decode). Go's `encoding/json` ignores unknown JSON keys, so this CLI change ships safely before the SaaS PR — newer agents reading older SaaS payloads see `GTID == ""` and behave as before; older agents reading newer SaaS payloads silently drop `gtid`, which is the bug being fixed and why `/deploy-agent` must roll this CLI to BYOS hosts before the SaaS-side PR can take effect. Refs `nethalo/dbtrail#1512` (#345).

## [0.7.10] - 2026-05-24

### Added
- `bintrail stream` now auto-discovers the source's current binlog position on first run when neither `--start-file`/`--start-pos` nor `--start-gtid` is provided AND no `stream_state` checkpoint exists. Mirrors the behaviour `bintrail agent` BYOS mode has had since `config.CurrentBinlogPosition` landed — removes the wrapper-script burden every fresh stream install was reinventing. Implementation is a new `resolveStartWithAutoDiscover` wrapper around the existing `resolveStart` that invokes the discovery callback only on the (saved=nil ∧ no-flags ∧ callback-present) tuple, so all previously-tested paths (`--start-file`/`--start-gtid` explicit, saved checkpoint resume, mutually-exclusive flag rejection, invalid GTID) remain unchanged. Test pinning covers both the `--start-file` and `--start-gtid` bypass branches so a future refactor cannot asymmetrically drop the GTID guard and silently overwrite an operator's declared base GTID (#312).
- `bintrail shim` accepts three time-travel query shapes that operators reach for by muscle memory from Oracle / SQL Server training, removing the demo footguns surfaced during interactive Percona Live sessions: (a) **column projection lists** — `SELECT id, name FROM _flashback.t AS OF '<ts>' WHERE id=1` now returns just the listed columns in the listed order; missing columns surface as NULL (matching MySQL's behaviour after `ALTER TABLE DROP COLUMN`); bare identifiers only — backticks, `schema.column`, and aliases fall through to the existing malformed-query error. (b) **`AS OF TIMESTAMP '<ts>'`** — the Oracle/SQL Server keyword form is now accepted alongside the plain `AS OF '<ts>'` shape. (c) **`SHOW [FULL] TABLES FROM _flashback|_diff|_snapshot`** — lists tables from the latest schema snapshot, sorted, with the `Tables_in_<virtual>` column header matching real MySQL's `Tables_in_<dbname>` convention. Pre-snapshot installs return an empty resultset (same UX as `SHOW TABLES` against a fresh DB) rather than an error. ProxySQL routing requires a new `rule_id 990005` emitted by `bintrail proxysql-config`; without it the query routes to real MySQL and gets `ER_BAD_DB`. Critical regression caught in post-merge review: the PK-filtered `runPointInTime` path silently bypassed user-supplied column lists because `imageToResult → orderColumns` is designed for `SELECT *` and drops missing-from-image keys + appends image-only keys alphabetically; new `imageToResultVerbatim` helper used when `q.Columns != nil` honours the user's projection exactly. Out of scope: `USE _flashback` (COM_INIT_DB routes by connection-default-hostgroup, not query rules), column lists in the hint-comment form, and `SHOW DATABASES` listing the three virtual schemas (#313, #314, #315).
- `bintrail proxysql-config --validate` is an opt-in pre-flight that connects to `BINTRAIL_SOURCE_DSN`, probes `mysql.user` for each tenant declared in `shim.yaml`, and warns when the backend's authentication plugin differs from `--backend-auth-plugin`. Warn-only by design: validation never blocks SQL generation, and probe failures (permission denied, connect refused, etc.) warn-and-continue so the operator can still produce setup SQL offline. Three semantic bugs caught in post-merge review and fixed before release: (a) the initial implementation probed the DSN's connection user (typically the bintrail admin/indexer account) instead of the tenant user ProxySQL re-handshakes with — checking the wrong identity entirely and leaving the risk `--validate` was meant to mitigate unmitigated in the standard topology; (b) filtered by the server's hostname against `mysql.user.host`, which is the CLIENT host pattern (where the user may connect FROM), not the server's hostname — a category error that made the `host = ?` branch effectively dead code; (c) `LIMIT 1` + no `ORDER BY` collapsed legitimate split-plugin grants (same user with `@'localhost'=native` and `@'%'=caching_sha2`) to one non-deterministic row, silently hiding the conflict. Final query iterates tenants and runs `SELECT host, plugin FROM mysql.user WHERE user = ?` (no host filter), collecting all matching rows into a map so the orchestrator warns on mismatch, split-plugin grants, and user-not-found (a separate diagnostic from permission-denied because the SQL we're about to write references a user that won't exist on the backend). Help text for `caching_sha2_password` also tightened to document the TLS / primed-cache precondition #310 left implicit (#327).
- `bintrail agent` now sends `X-Bintrail-Server-UUID` on every `POST /v1/events` metadata flush, mirroring the WS-dial reconcile header shipped in #317. Companion to `dbtrail/dbtrail#1504`: the SaaS ingest endpoint resolves the server by UUID before falling back to `bintrail_id`, so a dashboard-pre-registered row (`bintrail_id` NULL) is found on first ingest instead of triggering a duplicate `byos-<server-id>`. Without this agent change the SaaS fix is dormant. Empty `--server-uuid` preserves legacy behaviour — the header is omitted entirely (not sent with empty value) so an older backend isn't confused. Regression-guarded by `TestMetadataClientOmitsServerUUIDHeaderWhenEmpty` (#341).

### Fixed
- `bintrail shim` emits a periodic `INFO`-level summary of denied monitor-user probes — restores the observability #322 demoted to `DEBUG` without bringing the per-probe log flood back. Production runs at `--log-level info`, so credential probes spoofing the monitor username and ProxySQL misconfigurations (rotated `mysql-monitor_password`) produced zero shim-side signal between #322 and this fix. A mutex-guarded aggregator (count + bounded distinct-remotes set, cap 16, with `first_seen`/`last_seen` brackets) feeds a 5-minute ticker that emits one line per non-zero interval and resets. The `distinct_remotes` field is load-bearing: it tells "one IP hammering 3000 times" from "200 IPs probing once" — the difference between a misconfigured ProxySQL and a credential-stuffing burst. Two regressions caught in post-merge review and corrected: (a) the ticker goroutine was started fire-and-forget; Go does not wait for unjoined goroutines before process exit, so counts accumulated since the last tick were silently lost on every clean shutdown — fixed with `sync.WaitGroup` tracked from `runShim` and `wg.Wait()` after `serveLoop` returns; (b) the original `atomic.Int64` aggregator had no access to the connection so it couldn't capture the remote — fixed by changing `classifyHandshakeErr` to return `(level, msg, isMonitor)` and having `handleConn` call `monitorDeniedBump(remote)` (#326).
- `bintrail agent --server-uuid` is now canonicalized before forwarding to the `X-Bintrail-Server-UUID` header and WS-dial reconcile fields. `uuid.Parse` accepts uppercase, mixed-case, braced `{...}`, and `urn:uuid:...` forms, so two operators registering the same logical server from different copy-paste sources would send divergent header values to the SaaS — surfacing as a duplicate `byos-<server-id>` record alongside the pre-registered one with no operator-visible cause (the SaaS reconciles by exact-string match today). `validateServerUUID` now returns `(canonical string, error)` and `runAgent` assigns the canonical form (lowercase, hyphenated, no braces, no `urn:` prefix) back to `agtServerUUID`. Empty input remains empty for back-compat. The SaaS should still normalize on receipt as defense-in-depth, but normalizing in the agent — closer to the operator's input — is the right primary fix (#329).
- `config.CurrentBinlogPosition` now returns a domain-specific error naming `log_bin` and the remediation query (`SHOW VARIABLES LIKE 'log_bin'`) when binary logging is disabled on the source. Both `SHOW BINARY LOG STATUS` (8.4+) and `SHOW MASTER STATUS` (<8.4) return an empty resultset rather than a syntax error when `log_bin=OFF`, so `QueryRow.Scan` returns `sql.ErrNoRows` on both branches — the previous wrapped error surfaced as `"no rows in result set (fallback: no rows in result set)"`, zero hint at the cause and identical operator-hostile symptom to the crash-loop #320 was meant to retire. Affects every caller of the shared helper: `bintrail agent` BYOS-mode startup AND `bintrail stream` after its first-run auto-discovery (this release, #312). Initial implementation gated the domain-error branch behind `&&` (both queries returning `ErrNoRows`); production never hits the symmetric shape — on any real MySQL/Percona with `log_bin=OFF` it's asymmetric (5.7/8.0/Percona <8.4: `SHOW BINARY LOG STATUS` → 1064, `SHOW MASTER STATUS` → `ErrNoRows`; 8.4+: the opposite), so the gate was changed to `||`. The sqlmock test had passed because mocks can return any combination; a table-driven regression test now covers both real-world shapes plus the degenerate both-empty sanity case (#325).
- `bintrail agent`'s WebSocket reconnect loop now fails fast on synchronous 4xx HTTP-upgrade errors (bad/missing/revoked API key, malformed headers, future `unknown_server_uuid` rejections, etc.) instead of spinning until `--max-reconnect-attempts` exhausts. The dial path previously discarded the `*http.Response`, so `isTemporary` only saw the raw handshake error and classified it as transient. `dial()` now captures the response, wraps a 4xx in a new `*PermanentDialError` carrying `StatusCode` + underlying error, and `isTemporary` returns false for it. 5xx and pre-HTTP transport errors stay transient. `resp.Body` is closed defensively even though `coder/websocket` replaces it with an `io.NopCloser` on the error path — cheap insurance against a future lib change re-introducing a real `ReadCloser`. Partially addresses #328: item 1 (mapping a specific `unknown_server_uuid` close-reason string in `ClassifyClose`) awaits SaaS-side spec — leaving the issue open for the follow-up (#328).

### Changed
- `bintrail agent --server-uuid` help text and the cross-reference from `--server-id` refreshed to match the SaaS-side hardening in `nethalo/dbtrail#1490`. The previous `WARNING` text claimed UUIDs are "silently ignored" — no longer accurate: unmatched UUIDs are logged server-side and the SaaS refuses to bind. Same cluster: operators with a pre-registered UUID from the dashboard intuitively try `--server-id` and hit `strconv.ParseUint` rejection (a 2026-05-24 demo had three EC2 agents go into restart-loop after a sed-swap of `--server-id 202` → UUID), so `--server-id`'s help now cross-references `--server-uuid` for discoverability. Minimal-scope cross-ref — does NOT unify the flags. The `slog.Info` startup hint in `runAgent` also refreshed to drop the stale "no duplicate `byos-<server-id>` record was created" framing — that auto-create path no longer runs under the hardened SaaS (#336, #337).
- User-facing docs synced with the last 72h of merged behaviour changes. (a) `SHOW MASTER STATUS` references in `guide.md` and `streaming-101.md` replaced with `SHOW BINARY LOG STATUS` (MySQL 8.4 removed `SHOW MASTER STATUS`; #308 shipped `config.CurrentBinlogPosition` with the new form first). (b) The "first run requires `--start-file`/`--start-gtid`" prose in `guide.md`'s setup walkthrough and `streaming-101.md` Step 7 updated to reflect the auto-discovery shipped in #312 — the first run is now flag-free; `streaming.md`'s RDS-binlog-readiness section references the new error string the auto-discover path emits. (c) `time-travel-sql.md`'s "four statement shapes" section now mentions the three demo-footgun features that shipped in this release: column lists in `SELECT` (#313), `AS OF TIMESTAMP` keyword (#314), and `SHOW TABLES FROM` virtual schemas (#315). Also notes the `WHERE`-must-be-PK invariant from #296 since interactive users will hit it. No new docs or sections — only updates where existing prose was factually contradicted by what shipped (#344).

## [0.7.9] - 2026-05-23

### Fixed
- Follow-up to the 0.7.8 NULL-`binlog_file` fix. Deploy verification of 0.7.8 on a real tenant surfaced the same bug class one column over: the Explorer crashed with `Scan error on column index 2, name "start_pos": converting NULL to uint64 is unsupported`. Production indexes carry NULL across MANY columns declared NOT NULL simultaneously — not just `binlog_file` — so 0.7.8's targeted fix was incomplete. This release extends the defensive Scan to every NOT NULL column except `event_id` (AUTO_INCREMENT, cannot return NULL on read) in all three Scan sites: `internal/query/query.go:scanRows` (MySQL Fetch), `internal/archive/archive.go:ArchivePartition` (rotate Parquet writer, with `!Valid` propagated into the `nulls[]` slice so true NULL is preserved), and `internal/parquetquery/parquetquery.go:scanRows` (DuckDB Parquet reader). Columns newly defended: `start_pos`, `end_pos`, `event_timestamp`, `schema_name`, `table_name`, `event_type`, `pk_values`, `schema_version`. `ResultRow` public types stay plain (NULL → zero value, accepted asymmetry with the already-pointer `GTID`/`ConnectionID`). Mechanically extending NULL → zero-value mapping across more columns would convert three downstream sites from fail-loud (Scan error) to fail-silent (wrong answer), so this release also guards them: `internal/reconstruct/reconstruct.go:applyEvent` adds an explicit `case 0` that preserves state AND emits a `slog.Warn` naming the affected row (was previously a silent `default`-branch no-op that would have produced wrong PITR state); `internal/query/merge.go:LimitPerPK` gives empty-PK drift rows a per-row bucket (`\x00drift:<event_id>` — `\x00` cannot appear in real PK strings) so they no longer collapse onto bucket `""` and silently drop beyond the cap or collide with legitimate empty-PK rows; `internal/parquetquery/parquetquery.go:canTerminateEarly` filters out zero-timestamp rows before computing cutoff so a drift-row-heavy early Parquet file no longer silently terminates the multi-file scan and drops all later real data. Validated end-to-end: the new `TestFetch_allNullableNotNullColumns` integration test ALTERs every defended column to allow NULL, inserts a row with NULL in all of them, asserts Fetch returns it without crashing; verified to fail with the EXACT production error message from the 0.7.8 deploy when the `start_pos` defense is reverted. Each downstream guard has its own targeted regression test (`TestApplyEvent_driftRowPreservesState`, `TestLimitPerPK_driftRowsBucketIndependently`, `TestCanTerminateEarly` new subtests). Follow-up worth tracking separately: investigate the producer side — defensive scanning unblocks consumers but doesn't address how partial rows reach a NOT NULL schema in the first place (#318).

## [0.7.8] - 2026-05-23

### Fixed
- `bintrail query`, `bintrail rotate` (via the agent), and historical-partition reads via DuckDB no longer crash with `sql: Scan error on column index 1, name "binlog_file": converting NULL to string is unsupported` when the index contains a row whose `binlog_file` is NULL. Surfaced as a P1 in the dbtrail SaaS Explorer (`/app/explorer` → "Run Query" returned the raw scan error for any tenant whose index had a single NULL-binlog_file row — e.g., customer indexes created before the `binlog_file NOT NULL` migration, or rows inserted by external pipelines). The same root cause lived at three independent Scan sites: `internal/query/query.go:scanRows` (MySQL Fetch — the visible crash), `internal/archive/archive.go:ArchivePartition` (rotate-path Parquet writer — strictly worse blast radius since a failed rotate blocks partition pruning and grows the index unbounded), and `internal/parquetquery/parquetquery.go:scanRows` (DuckDB-backed reads of archived partitions — was latent until the archive fix turned it into a live crash by correctly preserving NULL through to Parquet). All three now scan `binlog_file` into a `sql.NullString` and assign `.String` to `ResultRow.BinlogFile`, matching the `gtid`/`connection_id` pattern already used in the same Scans. `internal/archive/archive.go` additionally propagates `!binlogFile.Valid` into the `nulls[1]` slot so the Parquet writer emits a true NULL instead of smuggling an empty string. `cmd/bintrail/reconstruct.go`'s baseline-vs-first-event gap check used `first.BinlogFile > bmeta.BinlogFile`; with the scan fixes, `first.BinlogFile == ""` would silently evaluate as "no gap" — added a Warn-and-skip guard mirroring the adjacent `bmeta.BinlogFile == ""` branch. Side-fix in `internal/testutil/testutil.go`: `InitIndexTables` was missing the `connection_id` column (added to migrations + `init.go` but never propagated), so every integration test in `internal/query/` was already broken with `Unknown column 'connection_id' in 'field list'` before this work. The schema declares `binlog_file NOT NULL` — defensive scanning is appropriate regardless. Validated end-to-end: `internal/parquetquery/parquetquery_archive_integration_test.go` drifts MySQL, runs `ArchivePartition`, and reads back via DuckDB `parquetquery.Fetch` — one test covers all three layers, verified to fail with the exact #318 error message when any of the three fixes is reverted (#318).

### Fixed
- `bintrail shim` `_flashback` / `_snapshot` / `_diff` (and the `/*+ DBTRAIL_AT */` hint-comment form) now reject queries whose `WHERE` column does not match the table's declared primary key with `ER_PARSE_ERROR` (1064) instead of silently returning the wrong row. The parser captured the column name into `PKColumn` but only used it to dispatch (point-lookup vs full-table); the literal value was joined against `binlog_events.pk_values` regardless of column name, so `WHERE customer_id=1` against a table PK'd on `id` returned the row with `id=1` — a schema-valid resultset for a question the user never asked. `validatePKColumn()` now runs once in `HandleQuery` after `Parse`, reusing the existing `resolverCache` (same TTL, same sticky-fallback, same `Debug`/`Warn` log split as `columnOrderFor`). Reject branches name both the expected and supplied column; composite PKs and PK-less tables get distinct 1064s. Permissive when no snapshot is loaded, the resolver load fails, or the table is missing from the snapshot — preserves the `columnOrderFor` graceful-degradation contract so a broken snapshot lookup never turns a working query into a parse error. Validated end-to-end on RDS MySQL 8.0.46 + ProxySQL 2.6.6: 5 non-PK reject paths across all 4 query shapes, 2 regression paths (correct PK, no-WHERE full-table) (#296).
- `docs/time-travel-sql.md`: post-walkthrough fixes from the same E2E run. (a) `bintrail proxysql-config` emits four routing rules (`990001-990004`) since #294 added the `DBTRAIL_AT` hint rule, not three as Step 3 and the Troubleshooting recipe claimed. (b) Step 2 Ubuntu install block now uses the `signed-by=/etc/apt/keyrings/proxysql.gpg` keyring pattern instead of the deprecated `apt-key add -` — required on Ubuntu 22.04+ and removed in 24.04. (c) `bintrail init-shim` and `bintrail proxysql-config` no longer prefixed with `sudo` (they don't need root and root-owned 0600 generated files broke the next-step `mysql … < proxysql-setup.sql` redirect when the operator's shell was unprivileged); a new Prerequisites bullet tells operators to `chown` their `/etc/bintrail/` to the operator user at install time. `CLAUDE.md`'s `proxysql-config` row updated to match (#297, #298, #299).
- `docs/streaming.md`: four RDS-specific gotchas surfaced during the same walkthrough that operators hit silently otherwise. (a) AWS RDS for MySQL only enables binary logging when `backup-retention-period >= 1`; with `0` (a reasonable choice for a demo) `bintrail stream` / `bintrail index` fail with `ERROR 1381 (HY000): You are not using binary logging` while `SHOW VARIABLES` still happily reports the custom parameter-group `binlog_format=ROW`. (b) `bintrail stream` registers as a binlog client via `COM_REGISTER_SLAVE` and is rejected by RDS read-replicas (`read_only=1`) with `ER_OPTION_PREVENTS_STATEMENT` (1290) — `--source-dsn` must point at the primary, not a replica. (c) After raising backup retention from `0` to `1`, `@@log_bin` flips to `1` within ~2 minutes but `SHOW BINARY LOGS` stays empty until RDS completes its first automated snapshot (another ~30s-1min); a `bintrail stream` launched into that window exits with `no start position specified` and looks like the binlog enablement didn't work. (d) RDS caps `binlog retention hours` at 720 (30 days); values above the ceiling are rejected with `ERROR 1644 (45000)` — longer historical reach is the index's job (with `bintrail baseline` for replay anchors before the index window), not the RDS binlog buffer (#300, #304, #305, #306).

## [0.7.6] - 2026-05-22

### Added
- **(security, breaking change)** `bintrail-mcp-gateway` requires a per-tenant `auth_secret` on the OAuth authorize page so guessing a tenant ID alone is no longer sufficient to obtain an access token. **Strict mode**: a tenant without an `AuthSecretHash` cannot authorize — `POST /oauth/authorize` returns 401 whether the secret is wrong, missing, or unset (single uniform reject, no wire-level oracle). `POST /admin/tenants` requires `auth_secret` in the body (400 otherwise) so an admin fat-finger never lands an unmigratable tenant. The secret is bcrypt-hashed (`bcrypt.DefaultCost`) before persistence; the cleartext is never echoed on responses or in logs, and the hash itself is excluded from JSON responses (`json:"-"`) so a stolen admin token doesn't leak the hash inventory. The authorize HTML form has a required "Tenant Secret" password input alongside the existing "Tenant ID" field. PUT preserves an unspecified `auth_secret` (no silent clear); rotation = PUT with a non-empty value. `tenant_id` is constrained to `^[A-Za-z0-9_-]{1,64}$` at create time to defend against log-injection. **Operator migration**: every active tenant must have an `auth_secret` set before this PR is rolled out — discover unset tenants via `slog.Warn("authorize rejected: tenant secret missing or incorrect", tenant_id=…, legacy_no_secret_configured=true)` on every authorize attempt against an unmigrated tenant. The earlier "gradual rollout" design (302+code for legacy tenants) shipped briefly in this PR's first commit but was reversed after the multi-agent review because it preserved the very tenant-ID-only-auth vulnerability #132 was filed to close, AND added a wire-level oracle attackers could use to enumerate and authorize as legacy tenants. New direct dependency: `golang.org/x/crypto/bcrypt` (#132).

### Fixed
- `bintrail shim` accepts the optimizer-hint comment form documented on dbtrail.com: `SELECT /*+ DBTRAIL_AT='<ts>' */ * FROM [<schema>.]<table> [WHERE <col> = <value>]`. Previously the shim's parser only recognised explicit virtual-schema FROM clauses (`_flashback.<t>`, `_snapshot.<t>`, `_diff.<t>`), so the docs-advertised hint form hit `ER_NOT_SUPPORTED_YET` (1235) — the very routing ProxySQL was configured for refused the query. The shim now detects the hint at the start of `Parse`, rewrites it to a `TypeFlashback` point-lookup against the original table name, and runs through the existing flashback pipeline. Friendly for ORMs (Hibernate, Sequelize, etc.) that can't easily rewrite the FROM clause in app code. Malformed hints (bad timestamp, missing FROM, etc.) return a non-`ErrNotTimeTravel` error so `HandleQuery` emits `ER_PARSE_ERROR` (1064), preserving the user-input-vs-server-fault distinction. Detection is gated by a cheap probe regex so non-hint queries pay only one extra token check (#288).
- `bintrail agent` now refuses to start in BYOS mode (`--source-dsn` + `--server-id` set) when no flush sink is configured. Previously the metadata/payload flush sinks were only initialized when `--s3-bucket` was set, so a customer following the public install docs without that flag would see a healthy-looking WebSocket connection while the in-memory buffer accumulated events with no durable destination and dropped everything on restart — the SaaS Explorer / recover / who-changed flows all returned empty with no operator signal. The agent now hard-fails at startup with an error naming both `--s3-bucket` and `BINTRAIL_S3_BUCKET`, surfacing the misconfiguration at the system boundary rather than as silent zero-data drift (#289).
- `bintrail shim` point-lookup `_flashback` / `_snapshot` now returns an empty resultset when the latest event at-or-before AS OF is a DELETE — matching the full-table reconstruction path (#276) and the Oracle `AS OF` semantic the docs already advertise (`docs/time-travel-sql.md`). Previously the PK-filtered path resurrected the row by returning the DELETE's `row_before` — a behaviour intentionally shipped in 0.7.5 ("point-lookup path is unchanged") but inconsistent with the no-`WHERE` path, the docs, and operator expectations. The forensic "what did this row look like right before deletion?" question is still answerable via `_diff`, which returns the full per-PK history including the DELETE's `row_before`. `docs/time-travel-sql.md`'s "Time-travel query returns empty" section now enumerates the DELETE cause and points operators at `_diff` for the distinction. The convergence is pinned by `TestSelectImage.delete_returns_nil` + `TestExtractFullTableImages` (#287).

## [0.7.5] - 2026-05-09

### Added
- `bintrail shim` reconstructs the full row state of a table at AS OF when the query omits `WHERE`: `SELECT * FROM _flashback.<table> AS OF '<ts>'` (and the same against `_snapshot.<table>`) returns every row that existed at the requested instant. DELETE events are correctly suppressed — rows that didn't exist at AS OF don't appear in the resultset (matches Oracle's `AS OF` semantic). The PK-filtered point-lookup path is unchanged. Buffered with a 100,000-row cap; overflow surfaces as `ER_TOO_BIG_SELECT` (1104) so monitoring can distinguish it from a real shim crash. Streaming the resultset (no cap) is deferred until an operator reports it as a bottleneck. JOINs, aggregations, and non-PK WHERE filters are intentionally out of scope — pipe the resultset to `duckdb`, `pandas`, or any downstream SQL tool (#276).
- `bintrail shim` accepts `--auth-method` (and `BINTRAIL_AUTH_METHOD`) to advertise `caching_sha2_password` or `sha256_password` instead of the historical `mysql_native_password` default. Useful on MySQL 8.4+ instances where `mysql_native_password` is disabled by policy. The default is unchanged so existing deployments see no behaviour difference. Implementation generates an in-memory self-signed RSA-2048 keypair once per process (the `cacheShaPassword sync.Map` and tlsConfig live on a single shared `*server.Server`, so SHA2 caching actually caches and a typo in the flag fails the daemon at startup, not silently per-connection). Requires ProxySQL 2.7+ between the application and the shim — the LTS 2.6 line isn't verified to negotiate SHA2 against backends. See `docs/time-travel-sql.md` Step 4 for the systemd recipe (#274).

### Fixed
- `bintrail shim` returns typed MySQL wire codes for client-input rejections so ORMs and monitoring can distinguish user input errors from server faults: `ER_PARSE_ERROR` (1064) for malformed time-travel queries (recognised virtual schema, bad shape / bad AS OF / missing `USE <db>`), `ER_NOT_SUPPORTED_YET` (1235) for non-time-travel queries routed to the shim. Real internal failures (DB timeout, archive S3 outage, build-resultset bug) keep returning `ER_UNKNOWN_ERROR` (1105) so the catch-all "the server is broken" signal is preserved (#277).
- `bintrail shim` returns `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526) for coverage-gap errors instead of the catch-all 1105, so an operator monitoring 1105 spikes can tell "the user asked for an AS OF outside index retention" apart from "the shim crashed". Wrap is localised in `runPointInTime` / `runDiff` via `errors.Is(err, *query.GapError)`; everything else still returns 1105 (#283).

## [0.7.4] - 2026-05-05

### Fixed
- `bintrail shim` no longer triggers ProxySQL to SHUNN its hostgroup when `monitor` (or any other unknown user) probes the listener. `TenantAuth.GetCredential` now returns `server.ErrAccessDenied` for unknown users, which `(*Conn).handshake` translates to `ER_ACCESS_DENIED_ERROR` (1045) on the wire. Previously the library raised `ER_NO_SUCH_USER` (1449) — ProxySQL classifies that as "backend broken" and SHUNNs the backend, making time-travel queries appear to silently return empty for ~N seconds at a time after every monitor probe. The shim's `handleConn` also classifies handshake errors via a new `classifyHandshakeErr` helper that uses `errors.Is` / `errors.As` against typed sentinels (`io.EOF`, `mysql.ErrBadConn`, `*mysql.MyError` with code 1045) instead of substring matches — so ProxySQL TCP probes drop to `Debug` and auth failures to `Info`, keeping the steady-state shim log clean (#262).
- `bintrail shim` now accepts fully qualified time-travel queries like `SELECT * FROM _flashback.orders AS OF '...' WHERE id = 1` without requiring a prior `USE <db>`. The shim derives each tenant's source schema from `source_dsn` (via `mysqldriver.ParseDSN`) and pre-seeds `Handler.db` after a successful handshake; an explicit later `USE` from the client still wins. A misconfigured tenant (unparseable or path-less DSN) falls back to the previous "issue USE first" behaviour with a per-tenant warning, and `runShim` emits a startup-time summary log listing the affected users so operators can spot configurations where most tenants will need to issue `USE` manually. New exported `shim.LoadTenantConfigs` returns the validated tenant slice (including `SourceDSN`); `shim.LoadTenants` is preserved as a thin wrapper so existing callers and tests are unchanged (#263).

## [0.7.3] - 2026-05-04

### Fixed
- `bintrail shim` no longer falsely classifies the current hour as a coverage gap. The shim was passing the user query's schema (e.g. `e2e_source`) as `DBName` to `query.FetchMerged`, which feeds the planner. The planner reads `information_schema.PARTITIONS WHERE TABLE_SCHEMA = ?`; with the wrong schema it returned zero partitions and treated every queried hour as rotated-out. Under `AllowGaps=false` (the default since 0.7.2) `_diff` queries aborted with a coverage-gap error; `_flashback` and `_snapshot` silently returned zero rows. The shim now plumbs the index DB name from the index DSN through a new `shim.Config.IndexDBName` field. As a hardening bonus, `bintrail shim` refuses to start if the index DSN cannot be parsed or omits the database — both conditions previously degraded silently. New integration tests (`-tags integration`) catch real `information_schema.PARTITIONS` shape drift the existing sqlmock tests cannot (#259, #260, #261).

## [0.7.2] - 2026-05-04

### Fixed
- `bintrail shim` now actually authenticates ProxySQL-forwarded connections. The previous TenantAuth implementation returned an empty cleartext from `GetCredential`, so the `mysql_native_password` handshake only succeeded when the client sent an empty password — every real ProxySQL → shim connection in 0.7.0 / 0.7.1 was rejected with `Access denied`. The shim now stores the cleartext password (read from a new `mysql_password` field in `shim.yaml`) and validates the client's scrambled response against it, the same way any MySQL server does (#254).
- `bintrail shim` virtual-schema queries no longer crash with `ArchiveFetcher is required when NoArchive is false`. `runPointInTime` and `runDiff` now wire `parquetquery.Fetch` as the archive fetcher — the same fetcher `bintrail query` and `bintrail recover` use — so S3 archive auto-discovery works out of the box without `--no-archive` (#255).
- `bintrail shim` no longer silently returns partial results when an archive source fails or the planner detects a coverage gap. The shim previously hard-coded `AllowGaps=true`, inheriting `bintrail recover`'s warn-and-continue behaviour — but on the wire-protocol path the connecting MySQL client has no stderr channel, so transient S3 throttling / IAM rotation / DuckDB load failures produced a successful-looking resultset missing rows the customer could not detect. The default is now strict: archive failures and planner-detected gaps abort the customer's query with a visible MySQL protocol error. Operators who explicitly want the previous warn-and-continue behaviour pass `--allow-gaps`. The library default in `internal/shim/handler.go`'s `NewHandler` is also flipped for consistency, and the new default is triple-pinned by tests so a future regression cannot silently revert it (#257, #258).

### Changed (breaking, but only relative to the non-functional 0.7.0 / 0.7.1 shim auth path)
- `shim.yaml`'s tenant block now requires `mysql_password` (cleartext) instead of `mysql_pass_sha1` (the SHA1 hex). `bintrail proxysql-config` recomputes the SHA1 from the cleartext at SQL-generation time, so operators no longer need to run a manual SHA1 recipe; the shim itself needs the cleartext to validate ProxySQL-forwarded auth (see #254). Existing `shim.yaml` files using only `mysql_pass_sha1` now error at startup with a clear migration message pointing at the new field. The legacy field is parsed (not rejected) by the strict YAML decoder so the error message can be specific.

## [0.7.1] - 2026-05-03

### Fixed
- `bintrail shim` no longer silently truncates `_diff` responses at 1000 rows. The arbitrary cap dropped a partial audit history with no signal to the customer for any PK that exceeded it within the requested window. A `_diff` query is already PK-scoped and time-windowed, so the cap was protecting against a non-issue; customers who hit a real problem can narrow the `BETWEEN` range. The `Options.Limit` doc comment in `internal/query/query.go` is also corrected (`0 → no limit`, not the stale `0 → default 100`) so the contract `_diff` now relies on is documented (#245, #249).
- `bintrail shim`'s accept loop now uses exponential backoff (100ms → 5s, doubling) instead of a fixed 100ms `time.Sleep`. A wedged listener (fd exhaustion, transient kernel state) previously emitted ~10 error log lines per second indefinitely while the process appeared alive; the new shape cuts steady-state log volume by ~50× without introducing a magic threshold. The sleep is also now a cancellable `select`, so a SIGTERM during a long backoff returns immediately instead of waiting up to 5s (#247, #251).

### Changed
- The image-selection branch of `internal/shim/handler.go` `runPointInTime` is extracted as a pure `selectImage([]query.ResultRow) map[string]any` helper. Behaviour is observably identical to the prior switch — `RowAfter` wins when present, falls back to `RowBefore` for DELETE events, returns nil for empty input or both-images-empty — but the priority rule is now testable without sqlmock or a real MySQL. Six table-driven cases lock the contract, including the `len() > 0` vs `!= nil` boundary that would silently regress DELETE handling on an empty non-nil RowAfter (#248, #250).

## [0.7.0] - 2026-05-03

### Added
- New `bintrail shim` subcommand: an in-process MySQL-protocol server that answers BYOS time-travel SQL queries (`_flashback.*`, `_diff.*`, `_snapshot.*` virtual schemas) by translating them into the existing `query.FetchMerged` engine. The shim sits behind ProxySQL on `127.0.0.1:3308` by default; ProxySQL is the password gate, the shim validates only that the connecting username appears in `shim.yaml`. Recognised statement shapes: `SELECT * FROM _flashback.<table> AS OF '<ts>' WHERE <col> = <value>`, the same shape against `_snapshot`, and `SELECT * FROM _diff.<table> BETWEEN '<t1>' AND '<t2>' WHERE <col> = <value>`. Time-range queries auto-discover S3 archives via `archive_state` so rotated-out hours resolve transparently.

### Changed
- The BYOS time-travel SQL story is now self-contained in the `bintrail` binary — there is no separate `dbtrail-shim` binary to download. `bintrail init-shim`'s output drops the `agent_url` and `agent_token` fields (no longer needed); `BINTRAIL_API_KEY` is no longer a precondition for scaffolding shim.yaml. The default `--listen` becomes `127.0.0.1:3308` so the shim is not reachable from the network unless the operator explicitly opens it. `docs/byos-time-travel-sql.md` rewritten to walk the customer through `bintrail shim` end-to-end. The systemd unit ships at `deploy/bintrail-shim.service` (renamed from `dbtrail-shim.service`).

### Removed
- The "Attach dbtrail-shim binaries to release" step in `.github/workflows/release.yaml`. That step pointed at a bucket that does not exist (the placeholder name in issue #236 was taken literally) and failed with HTTP 403 on every release. The shim now ships as a subcommand of the `bintrail` binary itself, so no cross-repo S3 pull-through is needed.

## [0.6.0] - 2026-05-02

### Added
- GitHub releases now include `dbtrail-shim` binaries (`dbtrail-shim-linux-amd64`, `dbtrail-shim-linux-arm64`, `dbtrail-shim-darwin-amd64`, `dbtrail-shim-darwin-arm64`) alongside the existing bintrail archives. The shim enables BYOS time-travel SQL via ProxySQL routing; binaries are pulled from the canonical SaaS build pipeline and re-published as release assets, so customers can `curl -LO https://github.com/dbtrail/bintrail/releases/latest/download/dbtrail-shim-linux-amd64` (#236).
- `bintrail init-shim` scaffolds a `shim.yaml` for the dbtrail-shim binary from the customer's existing `.bintrail.env`. Reads `BINTRAIL_SOURCE_DSN`, `BINTRAIL_SERVER_ID`, and `BINTRAIL_API_KEY` (validating all three are set and free of newlines, listing any that are missing in the error). Emits a deterministic YAML file with single-quoted scalars (safe against `:`, `#`, `'`, leading whitespace) and `mysql_user` / `mysql_pass_sha1` as TODO comments for the customer to fill in. Mirrors the patterns from `bintrail config init` and `bintrail generate-key`: refuse-overwrite, 0o600 perms, `--out -` for stdout, byte-identical re-runs (#237).
- `bintrail proxysql-config` reads `BINTRAIL_SOURCE_DSN` and a customer-edited `shim.yaml` and emits a deterministic, idempotent SQL script the customer applies to the ProxySQL admin port. The SQL configures `mysql_servers` (hostgroup 990 = real MySQL, 991 = shim), one `mysql_users` row per tenant in shim.yaml, and three `mysql_query_rules` (990001-990003) routing `\b_flashback\.`, `\b_diff\.`, `\b_snapshot\.` to the shim hostgroup. All numeric IDs sit in the 990* range to avoid colliding with operator-managed ProxySQL config; the DELETEs are scoped strictly to that range (mysql_users by `default_hostgroup = 990`) so a username collision with an operator user in another hostgroup surfaces as a loud PRIMARY KEY violation rather than silent destruction. The script wraps DML in `BEGIN`/`COMMIT` so a partial failure rolls back the whole change set; LOAD/SAVE run after COMMIT. Customer-supplied values flow through a single-quote-doubling `sqlQuote` helper, and shim.yaml is parsed with `yaml.UnmarshalStrict` so a typo like `mysql_user_name:` surfaces as "field not found" instead of "mysql_user is empty" (#238).
- New `docs/byos-time-travel-sql.md` walks a BYOS customer end-to-end from zero to a working `SELECT * FROM _flashback.t AS OF '...' WHERE id = N` on a fresh Ubuntu 22.04 / Amazon Linux 2023 host: shim binary download, `init-shim`, ProxySQL install, `proxysql-config`, systemd unit, application connection switch, sample flashback query, and troubleshooting (auth denied, missing query rules, shim port unreachable, agent unreachable, operator users in hostgroup 990). A copy of the systemd unit ships at `deploy/dbtrail-shim.service` (#239).

## [0.5.8] - 2026-04-15

### Added
- `bintrail query` accepts `--include-snapshot` + `--baseline <path-or-s3-url>` to merge a mydumper baseline Parquet as a third source alongside the live MySQL index and S3 archive. Baseline rows are emitted as synthetic `SNAPSHOT` events (new `parser.EventSnapshot = 6`) with the baseline's `snapshot_timestamp` metadata as their `event_timestamp`, so they flow through the existing `MergeAndTrim` pipeline and slot into sorted output before any subsequent binlog event for the same PK. Filter semantics: `--column-eq` hits the typed Parquet column via DuckDB `CAST(… AS VARCHAR) = ?` (index-friendly equality, no `JSON_EXTRACT`); `--event-type ≠ SNAPSHOT`, `--gtid`, `--changed-column`, `--flag` all exclude the snapshot source with a visible `slog.Info` reason; `--since`/`--until` compare against the baseline's recorded creation timestamp. `--include-snapshot` rejects combinations that would silently produce wrong data (`--pk`/`--pks`: snapshot rows have no `pk_values` in this release; `--profile`: RBAC rules are not applied to snapshot rows) and requires `--baseline` + `--schema` + `--table`. `--event-type SNAPSHOT` without `--include-snapshot` is rejected (previously silently returned zero rows). Unblocks dbtrail SaaS Phase 2 FK-aware cascade victim reconstruction for rows that existed before streaming began (#234).

## [0.5.7] - 2026-04-15

### Added
- `bintrail query` and `bintrail recover` now accept `--column-eq column=value`, a repeatable filter that matches events where a column inside `row_after` **or** `row_before` equals the given value. The OR across both sides covers DELETEs (value in `row_before`) and INSERTs (value in `row_after`) symmetrically. Repeating the flag composes AND. A column-name allowlist (`[A-Za-z0-9_]`) keeps the interpolated JSON path safe. The literal unquoted `NULL` sentinel matches rows where the column is explicitly JSON null (via `JSON_TYPE = 'NULL'`). The same filter is mirrored into the DuckDB archive path so merged live + archive queries stay consistent. MCP `query` and `recover` tools accept a matching `column_eq: [string]` parameter (#229).
- `bintrail query` and `bintrail recover` now accept `--pks` (comma-separated or repeatable) and `--limit-per-pk N` for batched multi-PK lookups. On the archive path this collapses N sequential DuckDB scans into one pass with `WHERE pk_values IN (...)` and `QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_timestamp DESC, event_id DESC) <= N`; on the MySQL side a ROW_NUMBER subquery enforces the per-PK cap server-side. The primary workload is dbtrail SaaS's "find latest DELETE per PK" auto-detection pass, which now completes in a single invocation instead of N shell-outs re-opening MySQL and re-scanning the same parquet files each time. `bintrail query --pks=a,b,c --format=json` emits a grouped shape `{"results": [{"pk": "X", "events": [...]}, ...]}` preserving input order, with empty groups for PKs that had no matches so callers can correlate inputs to outputs without a second lookup. `--pk` keeps its existing SHA2-indexed fast path. `--pk` and `--pks` are mutually exclusive; `--limit-per-pk` requires one of them; empty or whitespace-only PKs in `--pks` are rejected with a clear error, and duplicates are deduplicated with first-occurrence-wins ordering. Cross-source merge applies the per-PK cap before the global `--limit` trim via a new shared `query.MergeAndTrim` helper, so a large `--limit-per-pk` cannot starve later PKs under the ASC-sorted global cutoff (#231).

## [0.5.6] - 2026-04-14

### Fixed
- BYOS agent now detects two classes of silent source-identity misattribution. (1) The source `@@server_uuid` is re-read every 60 seconds and compared against the UUID captured at startup; if the source MySQL restarts with a regenerated `auto.cnf`, fails over behind a VIP, or resolves to a different instance, the agent exits with an actionable error instead of continuing to stamp the stale UUID on every `MetadataRecord`. Transient DB errors on the identity tick are tolerated with a warning. (2) `byos.EnsurePartitionKey` writes a `.bintrail-partition-key` marker to the customer S3 prefix on first run and validates it on every subsequent run; a mismatch (e.g. upgrading across the `--index-dsn` UUID-vs-numeric partition-key cutover) hard-fails with a message explaining the cutover and the operator's remediation options. An additional startup warning fires in the BYOS+S3-without-index-dsn path so the chosen partition key appears in the banner (#196, #198, #228).

### Changed
- S3 archive downloads for `query`/`recover` are now a single-pass prefetch pipeline instead of a per-batch download-then-query loop. Up to 2 files are prefetched in parallel while DuckDB queries the current one; queries remain strictly sequential (one DuckDB query at a time), so peak per-query RAM is unchanged, and peak temp files on disk drops from 4 to 3. Early termination is now checked after every file rather than at batch boundaries, so wide time-range queries with `--limit` stop as soon as the collected results cannot be displaced by any later file. S3 download errors that race with consumer cancellation are now logged at Debug for context errors and Warn for real failures (403, DNS, throttling) so production problems surface (#225, #227).

## [0.5.5] - 2026-04-13

### Fixed
- `bintrail baseline` now emits valid 0-row Parquet files for empty tables (tables with a schema DDL but no data rows in the mydumper dump). Previously, empty tables were silently skipped, causing `bintrail reconstruct` to fail with "no baseline snapshot found" for any table that was legitimately empty at dump time. Views are still correctly skipped via a `CREATE TABLE` vs `CREATE VIEW` heuristic on the schema SQL file (#226).
- `bintrail reconstruct` now handles missing baselines gracefully as defense-in-depth: when no baseline Parquet exists for a table, it emits a warning and treats the table as empty instead of aborting the entire reconstruct run. This covers pre-fix baselines and tables created after the last baseline snapshot (#226).

## [0.5.4] - 2026-04-13

### Changed
- S3 archive queries (`query --archive-s3`, `recover --archive-s3`) now use prefix-scoped S3 listing, batched concurrent downloads (4 at a time), chronological file ordering, and early termination when `--limit` is satisfied. A 24-hour query that previously took 18s+ now completes in under 5s; queries with small limits terminate after downloading only the first few files. The SaaS backend can remove its 144-chunk splitting logic entirely (#225).

## [0.5.3] - 2026-04-13

### Fixed
- `bintrail reconstruct` S3 baseline discovery now works with DuckDB v1.4.4, which changed `glob()` from a scalar function to a table function. The previous `SELECT unnest(glob('s3://...'))` syntax fails with a Binder Error; replaced with `SELECT * FROM glob('s3://...')` which works across DuckDB versions (#223).

## [0.5.2] - 2026-04-12

### Fixed
- `bintrail baseline` now recognizes mydumper 0.10.0's unchunked data-file naming (`<db>.<table>.sql`) in addition to the chunked format (`<db>.<table>.<chunk>.sql`) used by mydumper >= 0.11.0. Ubuntu 24.04's apt package ships mydumper 0.10.0 which produces single data files without a numeric chunk suffix; `DiscoverTables` previously required the chunk number and silently skipped these files, producing zero Parquet output. Together with #219 (mydumper version probing in `bintrail dump`), this unblocks the entire `dump → baseline → reconstruct --output-format mydumper` pipeline on Ubuntu 24.04 (#221).

## [0.5.1] - 2026-04-12

### Fixed
- `bintrail dump` no longer passes `--sync-thread-lock-mode` and `--trx-tables` to mydumper versions older than 0.11.0. These flags were introduced in mydumper 0.11.0 but were hardcoded in `buildMydumperArgs`, so `bintrail dump` failed immediately on Ubuntu 24.04's apt-installed mydumper 0.10.0 with `Unknown option --sync-thread-lock-mode`. The fix probes the local mydumper binary's version via `mydumper --version` and conditionally includes the flags only when the version is >= 0.11.0. Docker-mode invocations (`--mydumper-image`) always include the flags since the official Docker image ships a recent version. When the version cannot be determined, the flags are omitted conservatively with a `slog.Warn`. This unblocks the entire `dump → baseline → reconstruct --output-format mydumper` pipeline on Ubuntu 24.04 (#219).

## [0.5.0] - 2026-04-11

### Added
- `bintrail reconstruct --output-format mydumper` reconstructs entire tables at a target point in time and emits a mydumper-compatible dump directory (schema file, chunked INSERT files, and a `metadata` file with baseline binlog position). The output is restorable with plain `mysql < *.sql` or `myloader` with no further binlog replay needed — closes the gap between single-row investigation and full point-in-time recovery. The algorithm is merge-on-read: stream the baseline Parquet row-by-row via DuckDB, look up each row's PK in an in-memory change map built from the merged MySQL + archive event set, and emit SQL directly. No MySQL restore cycle, no InnoDB buffer warmup. Requires baselines written by this version (embeds the raw `CREATE TABLE` text as a new `bintrail.create_table_sql` Parquet metadata key); older baselines are rejected with a clear re-baseline message. New flags: `--output-format mydumper`, `--output-dir`, `--tables schema.table,...`, `--chunk-size` (default `256MB`), `--parallelism` (default `runtime.NumCPU()`). Strict `--allow-gaps=false` semantics inherited from single-row reconstruct — a coverage gap between baseline and target aborts unless the user explicitly accepts incomplete data. Supported PK types: integer (int/smallint/tinyint/mediumint/bigint), string (char/varchar/text/enum/set), DATETIME, TIMESTAMP, DATE, YEAR. Any PK column outside the allow-list (DECIMAL, BINARY, BLOB, BIT, JSON, spatial types, and any unknown type) causes reconstruct to hard-fail at start with a clear error message naming the column; #214 tracks expanding the supported set. UPDATE events that mutate the primary key itself are not handled — the change map is keyed by the before-image PK, so the after-image row may be dropped. Re-snapshot the baseline after PK-reshaping schema changes (#187).
- Full-table reconstruct now supports **DECIMAL and NUMERIC primary key columns**. Previously these were rejected at `ReconstructTable` entry because the #212 canonicalizer allow-list only covered integer, string, datetime, date, and year types. Investigation for #214 showed DECIMAL is the one type in the originally-listed set that can be added as a pure canonicalizer change without touching on-disk formats: go-mysql v1.13.0's `decodeDecimal` returns a pre-formatted Go string when `useDecimal` is false (the bintrail default — never set in `cmd/bintrail/stream.go` or `agent.go`), and the baseline writer stores DECIMAL as `parquet.String()`, which DuckDB reads back as a Go string. Both sides land on byte-identical strings for every representable value including zeros (`"0.00"` not `".00"` — `decodeDecimal` at `replication/row_event.go:1565-1567` explicitly writes `"0"` for zero-leading integer parts), so the canonicalizer branch is a type-check + pass-through. BINARY, VARBINARY, BLOB variants, BIT, and JSON remain deferred with explicit hard errors: each has a real representation mismatch between the indexer's `%v`-of-`[]byte` format and `baseline.parseSQLValue`, which does not decode MySQL hex literals (`0x...`), bit-string literals (`b'...'`), or convert JSON between raw-bytes and string forms. Fixing any of those requires a non-additive change to either `parser.BuildPKValues` or `internal/baseline/reader_sql.go`, tracked as separate follow-up issues. New integration test `TestRunReconstruct_fullTableRoundTrip_decimalPK` covers the full round-trip end-to-end (zero, positive, negative, INSERT-via-event); new regression test `TestRunReconstruct_rejectsRemainingUnsupportedPKTypes` is a table-driven pin against accidental allow-list expansion without a matching representation fix (#214).
- `bintrail baseline` now embeds the raw mydumper `<db>.<table>-schema.sql` contents in the Parquet file as `bintrail.create_table_sql`. Full-table reconstruct (#187) reads this back to emit a faithful `CREATE TABLE` in its schema file without re-synthesising from Parquet column types (which would lose indexes, foreign keys, charsets, engine, etc.).
- `recovery.FormatSQLValue`, `recovery.EscapeString`, `recovery.QuoteName` exported from `internal/recovery` so the full-table mydumper writer reuses the exact MySQL literal formatting used by reversal SQL. `FormatSQLValue` extended to handle `int64`/`int32`/`int`/`uint64`/`uint32`/`float32`/`time.Time`/`[]byte` (DuckDB scan types) in addition to the JSON-round-tripped types it already handled.
- `baseline.ReadParquetMetadataAny` reads Parquet metadata from either local paths or `s3://` URLs (using DuckDB's `parquet_kv_metadata` via the `httpfs` extension), so full-table reconstruct works against S3-resident baselines without adding a direct AWS SDK dependency.
- Benchmark harness for the full-table reconstruct merge loop at `internal/reconstruct/fulltable_bench_test.go`. Parameterised on baseline size × change rate (100k/1k, 1M/10k, 10M/100k at 1% change rate), reports `rows/sec` and `hit-ratio` as custom metrics via `b.ReportMetric`, and asserts the end-state counters on the first iteration so a regression in the merge loop surfaces as a benchmark failure instead of a silent throughput drop. This is the preparatory measurement work for #207 (row-group-level PK-range pruning). Run via `go test -bench=BenchmarkMergeBaselineIntoWriter -benchmem -run=^$ -count=3 ./internal/reconstruct/`. The 10M tier is gated behind `-short` because the synthetic fixture build alone is ~30s. **Initial numbers on Apple M1** (zstd-compressed 500k-row-group Parquet, single-column INT PK, evenly-spaced change events): 100k/1k ≈ 98ms / 1.02M rows/sec, 1M/10k ≈ 785ms / 1.27M rows/sec, 10M/100k ≈ 8.56s / 1.17M rows/sec. Throughput is linear across three orders of magnitude — the merge loop is not allocation-bound, so the speedup headroom #207 would unlock is bounded by Amdahl on a workload that already sustains >1M rows/sec. Extrapolated to a 50GB / ~500M-row baseline: roughly 7 minutes wall clock. This number goes in the #207 thread for the pruning decision (#207).

### Fixed
- `bintrail query` now **always surfaces archive fetch failures on stderr** and aborts immediately on context cancellation. The archive loop previously wrapped every `parquetquery.Fetch` error in a `slog.Warn` and continued, so expired AWS credentials, S3 `AccessDenied`, DuckDB `memory_limit` OOM, corrupted Parquet files, and `Ctrl-C` all produced partial/empty results with exit 0 and no visible signal at the default text log level. 0.4.8's #203 fix addressed the specific Binder Error trigger (pre-0.4.4 parquets missing the `connection_id` column) at the `parquetquery` layer, but left the surrounding silent-swallow anti-pattern untouched — any future archive failure mode would have reproduced the same six-days-of-empty-results outage. The fix extracts the entire archive fetch loop into `queryArchiveSources(ctx, sources, opts, fetch, stderr)` which (a) prints `Warning: archive query failed for <src>: <err>` to stderr regardless of log level via a `lineBreakReplacer` that collapses every line-terminator character (`\r\n`, `\r`, `\n`, `\v`, `\f`) to ` | ` so one failure = one stderr line, (b) still emits the structured `slog.Warn` with the raw (unsanitized) error for log-format=json consumers and full-fidelity debugging, and (c) runs a dual cancellation check — `ctx.Err()` AND `errors.Is(err, context.Canceled/DeadlineExceeded)` — so Ctrl-C short-circuits the query immediately instead of iterating every remaining source printing warnings. Non-cancellation errors keep the per-source "log and continue" semantics operators rely on — one broken archive still does not kill the whole query, only ones that are visibly broken. The helper takes `query.ArchiveFetcher` (the same named type `FetchMerged` uses) so signature drift with the shared pipeline becomes a compile error. Scoped to `cmd/bintrail/query.go` to preserve the existing (already-tested) `recover`, `reconstruct`, agent, and MCP-server paths untouched; the same silent-swallow still lives in `internal/query/fetchmerged.go`, `cmd/bintrail-mcp/main.go`, and `internal/agent/handler.go` as tracked follow-ups (#203).
- Full-table reconstruct now correctly handles **DATETIME/TIMESTAMP primary keys at every declared fractional precision** (0 through 6). The initial #187 implementation used `fmt.Sprintf("%v", ...)` to hash PK values on both sides of the merge, but the bintrail indexer stores DATETIME pk_values as go-mysql-formatted strings with the column's declared precision (`"14:30:45"` for DATETIME(0), `"14:30:45.123456"` for DATETIME(6)) while DuckDB's `parquet_scan` returns the same column as a `time.Time`. The resulting `%v` strings diverged and every event for a DATETIME-PK table silently missed the baseline, producing a dump with duplicate PK rows. **Schema change**: `schema_snapshots` gains a `column_type` column (e.g. `"datetime(6)"`) populated by `TakeSnapshot` from `information_schema.COLUMNS.COLUMN_TYPE`. `indexer.EnsureSchema` adds it idempotently on startup so existing installations upgrade transparently; `ReconstructTables` and the MCP server's `recoverTool` also run the migration eagerly because those code paths previously didn't. The new `canonicalizePKValue` helper in `internal/reconstruct` reads the declared precision from `ColumnType` and formats DuckDB values to match go-mysql's `formatDatetime` output exactly. Pre-#212 snapshots (empty ColumnType) fall back to a best-effort heuristic that works for DATETIME(0); full-table reconstruct emits an `slog.Warn` at the start of the run whenever a DATETIME/TIMESTAMP PK column has an empty ColumnType, instructing the operator to re-run `bintrail snapshot`. The canonicalizer **hard-fails** on nil PK values, missing PK columns, unknown DuckDB scan types, and any PK column type outside the supported allow-list (integers, strings, enum/set, datetime/timestamp/date, year) — all conditions that previously could silently corrupt the output. Any PK column outside the allow-list (including DECIMAL, BINARY, VARBINARY, BLOB, BIT, JSON, and spatial types) is rejected at `ReconstructTable` entry; #214 tracks extending support to those types. `MissingPKColumnError` is now a typed error exposing the offending column name via `errors.As`. `ReconstructTables` aggregates multi-table failures via `errors.Join` so every per-table error surfaces in the CLI exit wrap. 4 new integration tests pin the contract end-to-end: `TestRunReconstruct_fullTableRoundTrip_datetimePK` (DATETIME(0)), `TestRunReconstruct_fullTableRoundTrip_datetime6PK` (DATETIME(6) with mixed whole-second and microsecond values — the empirical validation of the precision-aware fix), `TestRunReconstruct_fullTableRoundTrip_varcharPK`, plus the pre-existing #187 INT PK round trip. A dedicated canonicalizer unit test suite covers every supported type, precision 0-6, fallback heuristic, and every hard-fail path (#212).
- `bintrail reconstruct --pk ...` now fetches events from both live MySQL partitions **and** Parquet archives, closing a latent correctness bug where single-row reconstruction silently missed events that had been rotated out of MySQL and archived. The previous code called `engine.Fetch` directly — bypassing the query planner, archive auto-discovery, and `MergeResults` pipeline already used by `bintrail recover`. Factored out a shared `query.FetchMerged` helper so `recover` and `reconstruct` share one pipeline, and the upcoming full-table reconstruct (#187) can reuse it. New `--no-archive` and `--allow-gaps` flags on `reconstruct` mirror `recover`'s surface area; `--allow-gaps` defaults to `false` because a silently incomplete row state is worse than a clear error for a recovery tool. Strict mode (`AllowGaps=false`) now aborts when the query planner fails, when no DBName is available for gap detection, or when every archive source fails — previously these conditions silently degraded to partial data. The query planner now runs regardless of `--no-archive` so users retain gap visibility even when opting out of archive queries (#209).

## [0.4.9] - 2026-04-10

### Added
- BYOS buffer now supports size-based eviction via `--buffer-max-events` and `--buffer-max-bytes` flags (e.g. `--buffer-max-bytes 256MB`). Previously the in-memory buffer only evicted by age (`--buffer-retain`), so a write burst within the retention window could grow RAM unbounded. When a cap is exceeded, the oldest events are evicted FIFO with a `slog.Warn` for operator visibility. New heartbeat fields `buffer_bytes` and `size_evictions` report buffer pressure to dbtrail. Both caps default to 0 (unlimited) for backward compatibility (#194).

## [0.4.8] - 2026-04-10

### Fixed
- Archive queries against pre-v0.4.4 parquet files no longer silently return 0 events. Older parquets lack the `connection_id` column added in 0.4.4; DuckDB threw a `Binder Error` when the per-file query SELECTed that column from a single file (where `union_by_name=true` is a no-op). The error was swallowed by the caller and the query returned empty. Fix: probe the parquet schema before building the SELECT and substitute `NULL::INT32 AS connection_id` when the column is absent. Applied to both the S3 per-file download path and the local glob path (#203).

## [0.4.7] - 2026-04-09

### Added
- `bintrail agent` now exits with distinct process codes when the dbtrail backend rejects the WebSocket with a permanent close: **64** for auth/config failures (`missing_credentials`, `invalid_key`, `wrong_tenant_mode`) and **65** for `rate_limited`. systemd units should add `RestartPreventExitStatus=64 65` to stop respawning on permanent failures — previously every fatal close exited 1 and the supervisor kept respawning a doomed agent. Unknown reason strings on a fatal close code fall through as a transient exit (safe to respawn) so backend contract drift never silently pins the agent into a fatal loop. The permanent-error log line now includes `close_code` / `close_reason` structured fields for grep/alerting. Recognizes both canonical short forms (`invalid_key`) and legacy human strings (`Invalid API key`) for back-compat with older dbtrail versions (#201, #202).

## [0.4.6] - 2026-04-08

### Fixed
- `bintrail rotate --add-future N` is now declarative: it maintains *at least* N future hourly partitions beyond the current hour (top-up only) instead of adding N new partitions per invocation. In daemon mode the old behaviour leaked `+N` partitions per cycle — a demo tenant running `--add-future=1 --interval=1h` accumulated 413 partitions over 17 days and blew up `SELECT DISTINCT` on `binlog_events` from sub-second to 23s. Semantics now match the documentation in `docs/deployment.md` (#199).

## [0.4.5] - 2026-04-07

### Removed
- BYOS agent no longer requires `--index-dsn` when `--s3-bucket` is configured (#197). The requirement was added in 0.4.1 to guarantee stable S3 partition keys via a locally-persisted `bintrail_id`, but it forced customers to provision a dedicated MySQL (a footgun the customer-facing setup docs never mentioned). With the source identity propagation shipped in 0.4.4 (#195), the dbtrail SaaS side now resolves a stable `bintrail_id` server-side from the `@@server_uuid` + host/port/user fields carried on every metadata record (architecture §22.11, nethalo/dbtrail#1179). The local customer agent falls back to `--server-id` for the S3 partition key and WebSocket heartbeat label, which are customer-local identifiers intentionally decoupled from the SaaS-resolved `bintrail_id`.

## [0.4.4] - 2026-04-07

### Added
- BYOS agent now propagates source server identity (`server_uuid`, `source_host`, `source_port`, `source_user`) on every metadata record sent to dbtrail — lets the SaaS side register the source server and resolve a stable `bintrail_id`, closing the identity-model gap between hosted and BYOS modes (#195)

### Changed
- BYOS agent fails loud at startup if source identity capture (`@@server_uuid` query or `--source-dsn` parse) fails, instead of silently emitting metadata with an empty `server_uuid` for the process lifetime (#195)

## [0.4.3] - 2026-04-06

### Added
- `bintrail stream --gap-timeout` flag (default 30s) configures the timeout for gap-detection queries (`SHOW BINARY LOGS`, `@@gtid_purged`, `@@gtid_executed`); raise this on managed MySQL instances with many binlog files where the default 10s was too tight (#190)
- `bintrail agent --max-reconnect-attempts` flag (default 10) bounds the WebSocket reconnect loop so the agent exits non-zero after consecutive failures, letting a process supervisor (e.g. systemd `Restart=on-failure`) respawn it (#191)

### Changed
- `bintrail stream` gap-detection query timeout default raised from 10s to 30s — the query only runs once per resume so a higher ceiling has no ongoing cost (#190)

### Fixed
- `bintrail agent` no longer stays "active" in systemd when its WebSocket connection dies but cannot reconnect — the new retry budget surfaces the failure as a process exit so systemd can respawn the agent and the dashboard sees a fresh, healthy connection (#191)

## [0.4.2] - 2026-04-05

### Fixed
- Release binaries now target glibc 2.17 (via Zig linker) — fixes `GLIBC_2.38 not found` on Amazon Linux 2023, RHEL 9, and other distros with glibc < 2.39

## [0.4.1] - 2026-04-03

### Fixed
- BYOS MetadataClient now sends `bintrail_id` (stable UUID) instead of numeric `@@server_id` in metadata records and WebSocket heartbeats — prevents misidentification when multiple MySQL servers share the default `server_id = 1`
- BYOS+S3 mode now requires `--index-dsn` to ensure stable `bintrail_id` resolution for S3 partitioning — prevents orphaned partitions that cannot be correlated with future runs

## [0.4.0] - 2026-04-01

### Added
- `bintrail agent` command — opens an outbound WebSocket to dbtrail for remote query, recovery, and forensics commands; no inbound ports required
- BYOS (Bring Your Own Storage) mode — parsed binlog events are split into metadata (sent to dbtrail API, zero row data) and payload (written as Parquet to customer S3, never leaves customer infrastructure)
- Abstract storage backend interface (`internal/storage`) with S3 implementation for BYOS payload writes
- In-memory event buffer for BYOS mode — keeps recent events in memory for fast local access while S3 remains authoritative
- BYOS flush pipeline with configurable interval (`--flush-interval`, default 5s) and retry with exponential backoff; flush health reported in agent heartbeat
- Agent pre-flight validation (`--validate` flag) — checks MySQL connectivity, replication privileges, S3 access, schema snapshot, dbtrail API auth, and WebSocket channel in one pass
- `connection_id` column in `binlog_events` — captures MySQL `pseudo_thread_id` from binlog `QueryEvent.SlaveProxyID`; included in all query output formats, archives, buffer, and BYOS metadata
- Auto-migration via `indexer.EnsureSchema()` for `connection_id` column on existing installations (instant DDL in MySQL 8.0+)
- systemd service unit (`deploy/bintrail-agent.service`) for bare metal agent installs

### Changed
- Module path moved from `github.com/nethalo/bintrail` to `github.com/dbtrail/bintrail`
- License changed from Apache 2.0 to Business Source License 1.1

## [0.3.2] - 2026-03-15

### Added
- Binlog gap detection on `bintrail stream` restart — position mode checks `SHOW BINARY LOGS`, GTID mode compares checkpoint against `@@gtid_purged` and `@@gtid_executed`
- Automatic gap filling for fillable gaps; unfillable gaps auto-advance to the earliest available position with a warning logged and checkpoint updated to prevent crash loops
- `--no-gap-fill` flag for `bintrail stream` to refuse starting when a gap is detected

## [0.3.1] - 2026-03-13

### Added
- Capture foreign key constraints in schema snapshots — `bintrail snapshot` now queries `INFORMATION_SCHEMA.KEY_COLUMN_USAGE` joined with `REFERENTIAL_CONSTRAINTS` and stores FK relationships in a new `fk_constraints` table, using the same `snapshot_id` as `schema_snapshots`
- New `fk_constraints` table created by `bintrail init` — no additional MySQL grants required
- Graceful upgrade path: existing installations that upgrade without re-running `bintrail init` get a warning instead of a failed snapshot

## [0.3.0] - 2026-03-12

### Added
- `list_schema_changes` MCP tool — queries the `schema_changes` table with filters (`schema`, `table`, `ddl_type`, `since`, `until`, `limit`), making DDL audit data queryable via MCP
- TRUNCATE DDL support — `parseDDL()` now detects `TRUNCATE [TABLE]` statements and records them in `schema_changes`
- Composite index `idx_schema_table` on `schema_changes` for efficient per-table lookups

### Changed
- Rotate now archives and drops one partition at a time instead of archiving all partitions first and then dropping in a single bulk `ALTER TABLE` — reduces disk space pressure during large rotations and ensures each partition is freed immediately after archiving

## [0.2.16] - 2026-03-10

### Added
- Truncation warning when query or recover results hit the limit — CLI prints to stderr, MCP tools append to the response text so the LLM knows to narrow the time range or increase the limit

## [0.2.15] - 2026-03-09

### Changed
- Bump DuckDB threads from 1 to 2 — archive Parquet files have 3-4 row groups (500K rows each) and DuckDB can only parallelize across row groups; with 6GB container memory there is enough headroom for two threads (125MB each)

## [0.2.14] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 1GB to 4GB — 190MB compressed Parquet files decompress to well over 1GB; 2GB containers still OOM-killed during scans

### Fixed
- Apply ORDER BY + LIMIT per archive file instead of scanning all rows — DuckDB's top-N optimization keeps only the LIMIT rows in memory during sort, then MergeResults merge-sorts the per-file results for the correct global top-K. Previously a single hour with 1.75M events was fully materialized in Go before applying the limit, causing both excessive memory usage and 2+ minute query times

## [0.2.13] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 1GB to 4GB — 190MB compressed Parquet files decompress to well over 1GB; 2GB containers still OOM-killed during scans

## [0.2.12] - 2026-03-09

### Changed
- Raise DuckDB memory limit from 256MB to 1GB for 2GB container environments — 190MB compressed Parquet files need more than 256MB to decompress and scan; the previous limit caused OOM kills even with single-file sequential processing

## [0.2.11] - 2026-03-09

### Fixed
- Remove ORDER BY from per-file S3 archive queries — forces DuckDB to buffer the entire result set for sorting, spiking memory. Sorting now happens in Go via `MergeResults` after all files are collected, letting DuckDB stream rows with minimal memory

## [0.2.10] - 2026-03-09

### Changed
- Download S3 Parquet files to local temp via AWS SDK before querying with DuckDB — eliminates httpfs extension which held entire S3 files in memory (outside `memory_limit` tracking), causing OOM kills even with conservative limits. Local reads use OS page cache (mmap), keeping memory usage predictable and low

## [0.2.9] - 2026-03-09

### Fixed
- Query S3 Parquet files one at a time instead of all at once — each file can require hundreds of MB when decompressed; sequential processing lets DuckDB release memory between files, fitting comfortably in container memory limits
- Reduce DuckDB to 1 thread (125MB baseline per thread per DuckDB docs) to maximize memory available for data

## [0.2.8] - 2026-03-09

### Fixed
- Pre-filter S3 archive files by Hive partition time range (`event_date`/`event_hour`) before passing to DuckDB — a 1-hour query now reads 1-2 files instead of all 10, dramatically reducing memory usage and S3 transfer
- Bump DuckDB `memory_limit` to 512MB (was 256MB) as a safety net for larger archive scans

## [0.2.7] - 2026-03-09

### Fixed
- Tune DuckDB for container environments — limit to 2 threads and disable `preserve_insertion_order` to reduce peak memory when querying S3 Parquet archives; prevents OOM kills (`exit=137`) and DuckDB out-of-memory errors on memory-constrained containers

## [0.2.6] - 2026-03-09

### Fixed
- Cap DuckDB memory at 256MB to prevent OOM kills in memory-constrained containers — DuckDB defaults to 80% of system RAM, which exhausts memory when reading S3 Parquet files; with the limit it spills to disk instead

## [0.2.5] - 2026-03-09

### Fixed
- Auto-detect S3 bucket region via `GetBucketLocation` — when `AWS_DEFAULT_REGION` differs from the bucket's actual region, `ListObjectsV2` and DuckDB `parquet_scan` both failed with 301 PermanentRedirect
- Set DuckDB `temp_directory` to OS temp dir — DuckDB creates a `.tmp` scratch directory in the CWD, which fails in containers where the working directory is read-only

## [0.2.4] - 2026-03-09

### Fixed
- Bypass DuckDB S3 glob expansion entirely — use AWS SDK `ListObjectsV2` to enumerate `.parquet` files, then pass explicit paths to `parquet_scan()`. DuckDB's glob fails on S3 paths containing `=` signs (Hive partition keys like `event_date=2026-03-09/`), silently returning zero results even with valid credentials and correct single-level globs

## [0.2.3] - 2026-03-09

### Fixed
- Load DuckDB `aws` extension for S3 credential resolution — without it, DuckDB attempts anonymous S3 access which silently returns zero results instead of using `AWS_ACCESS_KEY_ID` / `AWS_SESSION_TOKEN` from the environment

## [0.2.2] - 2026-03-09

### Fixed
- Use explicit single-level S3 globs (`/*/*/*.parquet`) instead of unsupported `**` recursive glob — DuckDB's httpfs extension does not support recursive globs on S3, causing "No files found" errors on archive queries

## [0.2.1] - 2026-03-09

### Fixed
- Enable `hive_partitioning` in `parquet_scan` for S3 archive queries — DuckDB's glob resolution failed on S3 paths containing `=` signs (Hive-partitioned directories like `event_date=2026-03-09/`)

## [0.2.0] - 2026-03-06

### Added
- MCP gateway with OAuth 2.1 for Claude Connector support
- Tenant provisioning admin API and backend health monitoring for MCP gateway
- Rate limiting and request logging for MCP gateway
- Auto-snapshot on DDL detection with restore coverage tracking
- `--reset` flag for `bintrail stream` to force new start position
- Seamless mode switching between position and GTID in `bintrail stream`
- Idempotent stream startup by preferring saved checkpoint over flags
- S3 upload retry with `--retry` flag for `baseline` and `rotate` commands
- Standalone `bintrail upload` command for S3 uploads
- At-rest encryption for mydumper dump files (`dump --encrypt`)
- Docker support: Dockerfile, docker-compose template, and mydumper Docker fallback
- `event_hour` Hive partition level in archive path
- `archive_state` table to track archived Parquet files
- Archive and S3 stats in `bintrail status` output
- `--format json` for all commands
- `--sync-thread-lock-mode` and `--trx-tables` flags for mydumper
- Stream state and `bintrail-id` in status output
- `/health` endpoint for `bintrail-mcp` HTTP server
- Debug logging for status command

### Fixed
- Always emit `TO_SECONDS` partition pruning hints for `since`/`until` queries
- Include `archive_state` data in status restore coverage
- Add missing Archives section to MCP status tool
- Emit GTID tracking events to prevent gaps in accumulated GTID set
- Place `--outputdir` last in mydumper args for Docker wrapper compatibility
- Skip shell script wrappers when resolving mydumper on PATH
- Remove `Truncate(time.Hour)` from rotate cutoff so hourly partitions drop correctly
- Parse mydumper 0.16+ metadata format
- Replace `UTC_TIMESTAMP()` with `CURRENT_TIMESTAMP` in `archive_state` DDL
- Show S3 upload progress in rotate output
- Create partitions from current hour forward
- Suppress usage output on command errors
- Normalize shortened UUIDs in GTID sets from RDS
- Allow gateway's own issuer origin in origin middleware

## [0.1.1] - 2026-03-01

### Fixed
- Nil dereference in `parser.New()` when resolver is nil (crash risk)
- MCP HTTP server now shuts down gracefully on SIGINT/SIGTERM
- `proxy.py` SSE stream parsing crash on non-UTF-8 bytes
- Unchecked `os.MkdirAll` error in E2E test

### Changed
- Unknown compression codecs now return an error instead of silently falling back to no compression; new `ValidateCodec()` function validates early in CLI layers
- `config.Connect()` injects a 10-second default TCP connect timeout when the DSN does not specify one
- `proxy.py` is now fully self-contained (inlined `log.py`); `log.py` removed
- Partial file cleanup errors in baseline are now logged

## [0.1.0] - 2026-03-01

### Added
- MySQL ROW-format binlog parser and indexer (`bintrail index`)
- Live replication indexing (`bintrail stream`) with Prometheus metrics
- Query engine with table/json/csv output (`bintrail query`)
- Reversal SQL generator (`bintrail recover`)
- Schema snapshot management (`bintrail snapshot`)
- Hourly partition rotation with retention policy (`bintrail rotate`)
- Partition archiving to Parquet (`rotate --archive-dir`, `--archive-s3`)
- DuckDB-backed Parquet archive querying (`query --archive-dir`, `--archive-s3`)
- mydumper integration (`bintrail dump`) and Parquet baseline converter (`bintrail baseline`)
- MCP server with stdio and HTTP transport modes (`bintrail-mcp`)
- RBAC foundation with table flags, profiles, and access rules (`bintrail flag`, `bintrail profile`, `bintrail access`)
- TLS/SSL support for stream mode
- Server identity system (`--bintrail-id`)
- GoReleaser-based release process with version injection
