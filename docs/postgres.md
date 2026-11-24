# PostgreSQL as a source

bintrail can capture from a **PostgreSQL** server while the index database stays
MySQL. PostgreSQL capture lives in its own binary, **`bintrail-pg`**.
Capture is type-faithful,
replication-slot/WAL-retention-monitored, and
DDL-drift-safe, all verified end-to-end against real PostgreSQL (15–18 in CI)
and smoke-validated on managed PostgreSQL (RDS / Aurora). It still has
documented limitations (below) — notably whole-table time-travel in the shim
and full-table `reconstruct` to a mydumper dump, which stay out of scope for
PostgreSQL — so read them before pointing it at production.

**Scope:** PostgreSQL is supported as a **source** (the database you capture
changes from). The **index** — where bintrail stores the indexed events — stays
**MySQL** (one index schema for every source family). Pointing the index at
PostgreSQL is not supported.

**Nothing is installed in your PostgreSQL server.** bintrail-pg connects as an
ordinary logical-replication **client** using PostgreSQL's built-in `pgoutput`
plugin. It does **not** install an output plugin, a `CREATE EXTENSION`, an
event trigger, or any other server-side component. You create one publication
(and set `REPLICA IDENTITY FULL` on the tables to capture); bintrail reads, and it never
writes to your source.

---

## Requirements at a glance

| On the PostgreSQL source | Required? | Notes |
|---|---|---|
| **PostgreSQL 15+** | Yes (declared) | 15/16/17/18 tested in CI. 13 and 14 may work but are past end of life, best-effort only. On 18, a table with a `VIRTUAL` generated column cannot be at `REPLICA IDENTITY FULL`, so it cannot be captured, and a table with a `STORED` one needs `publish_generated_columns = stored` on the publication to be ([why](#3-set-replica-identity-full-on-every-captured-table)). |
| **`wal_level = logical`** | **Yes** | Server-wide; needs a **restart**. Validated at startup — bintrail-pg refuses to start otherwise. |
| **A role with `REPLICATION`** | **Yes** | Used for the replication stream and to create the slot. |
| **`REPLICA IDENTITY FULL`** on every captured table, and on each partition of a partitioned one | **Yes** | A table that is not at `FULL` is not captured, while the other tables are. `doctor` fails on it with the statement to run, and a server is not added or started until it is fixed ([details](#3-set-replica-identity-full-on-every-captured-table)). |
| **A replica identity that holds the primary key** | **Yes** | True of every table left at PostgreSQL's default. A table set to `REPLICA IDENTITY USING INDEX` on an index that leaves a primary-key column out is not captured: `doctor` fails on it with the statement to run, and a server is not added or started until it is fixed. A capture that is already running skips that table while the other tables go on. |
| **Primary keys that are not `DEFERRABLE`** | **Yes** | The copy is kept by key, and a deferrable key lets two rows share one inside a transaction. `doctor` fails on a table with one (a server is not added or started with it). Capture itself never refuses to start over one: when it starts and each time PostgreSQL describes a table, it skips that table and captures the others. `server add` and the web interface's Start run `doctor` first, which is why they wait for the key to be fixed. See [the limit](#3-set-replica-identity-full-on-every-captured-table). |
| **A `PUBLICATION`** covering those tables | **Yes — you create it** | bintrail-pg validates it exists and covers your tables; it does **not** create it. |
| **A replication slot** | No — created for you | bintrail-pg creates the logical slot on first run and reuses it on restart. |
| `max_replication_slots` ≥ 1, `max_wal_senders` ≥ 1 | Yes | Defaults (10) are usually fine. A slot consumes one of each. |
| **`max_slot_wal_keep_size`** set (not `-1`) | **Strongly recommended** | The production safety valve (PG 13+). Left unlimited (`-1`, the default), a stalled slot pins WAL until the source disk fills — an outage. Set a bound (e.g. `'10GB'`); `bintrail-pg doctor` WARNs while it's unlimited. See [the slot section](#5-the-replication-slot--created-for-you). |

The index MySQL has the same requirements as for any source — see
[Streaming → index requirements](streaming.md).

---

## Install

DBTrail captures PostgreSQL in the same `bintrail-console watch` stack the
[Compose quickstart](install.md) stands up, in an installation of its own: an
installation serves PostgreSQL servers, or MySQL and MariaDB servers, not both
([one database family per installation](install.md#one-database-family-per-installation)).
There are two ways in: the **web interface** (recommended: zero binaries to place)
or the standalone **`bintrail-pg`** binary (for an existing index or a headless
CLI deployment).

### The web interface: "+ Add server" (recommended)

If you brought up the [Docker Compose stack](install.md) — the *same*
`install.sh` MySQL uses — you already have everything. Do the [source-side
setup](#postgresql-side-setup) below first, then:

1. Open the web interface (**http://127.0.0.1:8090**) and sign in.
2. **+ Add server** → **Open the full form** (the link at the foot of the
   Connect screen) → set **Source type** to **PostgreSQL**. The PostgreSQL-only
   fields appear: **Database**, **Replication slot**, and **Publication**.
3. Fill in host, port, user, password, the database, the slot name (created for
   you on first run), and the publication (the one you created above).
   Optionally restrict **Schemas**.
4. **Save.** DBTrail provisions a dedicated MySQL index for that source and
   starts capturing **in-process** (the daemon runs the same PostgreSQL
   preflight as `bintrail-pg doctor` — `wal_level`, publication coverage,
   replica identity, slot health — and surfaces any failure as a
   remediation card; a table that is not at `REPLICA IDENTITY FULL` is a
   failure, and the server is not added until it is fixed). Capture resumes
   automatically on restart.

The form also holds one box, unticked: **Let the PostgreSQL port send to this
PostgreSQL, read-only and as the source user above, a SELECT the copy does not
answer**. It is the question `server add` asks on a terminal
([forwarding with the capture account](#forwarding-with-the-capture-account)),
and nothing is sent to your PostgreSQL until somebody ticks it. Tick it when
adding the server, or later (**Servers**, **Edit**); unticking takes it back.
No second role is needed for it. A forwarding user of its own, for a port that
should send more than that, is behind **Advanced: use a separate forwarding
user** in the same form.

From a terminal, the same thing is one command (the publication is the one
you created above; the slot is created for you and named `bintrail_<database>`
unless you give `--slot`):

```bash
dbtrail server add --flavor postgres -h pg1.example.com -u dbtrail -p -d shop --publication bintrail_pub
```

It runs the same checks, prints each one that fails with its fix, and saves
nothing until they all pass. `--flavor postgres` has to be said: the question
DBTrail asks an address to learn what answers there is in MySQL's protocol.
What it prints at the end is the `psql` line for the PostgreSQL port, with the
port's password when this was the first server.

Once the server is added, on a terminal, the command asks one question: may
the PostgreSQL port send to your PostgreSQL, read-only, a `SELECT` whose every
table is a table of the copy, as the role DBTrail already connects with, when
the copy does not answer it
([forwarding with the capture account](#forwarding-with-the-capture-account))?
Enter is yes. Without a terminal (a script, or the Docker `dbtrail` command,
which sends the option file on standard input) nothing is asked and the answer
is no, and the command says so, unless `--capture-account` or this line in the
option file says yes:

```ini
[dbtrail-forward]
capture-account = yes
```

`--no-capture-account` (or `capture-account = no`) says no. `capture-account`
alone on its line is yes, as an option alone is in MySQL, even with a comment
after it; `capture-account =` with nothing after it says nothing. With no the copy
answers alone, and a statement it cannot answer gets an error;
`bintrail-console server forward-user <server> --capture-account` turns it on
later.

To let the port read more than that, give the server a
[forwarding user](#transactions-on-the-source-the-forwarding-user) of its own:
`--forward-user <role>` on the command line (its password is asked for), or
`user` and `password` under `[dbtrail-forward]` for a script. The question is
then not asked. `--no-forward` forwards nothing and asks nothing.

What is refused when a forwarding user is saved: the role DBTrail captures
with, an empty password, and a source the account cannot be laid over (one
reached through a socket directory, or whose URL names its user or host as a
parameter). The server then stays added and recording, the command says why
and exits 1, and `dbtrail server forward-user` sets it once that is fixed. No
login is tried with the role when it is saved, by `server add` or by
`server forward-user`: a wrong password, or a role that cannot log in, shows
with the first statement the port sends to your PostgreSQL. To check it, run
`BEGIN; SELECT 1; COMMIT;` through the PostgreSQL port, once the first copy is
ready.

The **index stays MySQL**; only the *source* is PostgreSQL. The source type is
fixed once saved — to change a server's capture engine, delete and re-create it.
The form lives in `bintrail-console watch` (the Compose default); the read-only
`serve` mode browses an existing PostgreSQL-sourced index but does not add
sources.

### Standalone binary / image

Prefer a headless deployment, or capturing into an index you already run?
`bintrail-pg` ships as its own artifact alongside the core `bintrail` binary.

**Docker image:**

```bash
docker pull ghcr.io/dbtrail/bintrail-pg:latest
docker run --rm ghcr.io/dbtrail/bintrail-pg:latest --version
```

**Linux packages** (`.deb` / `.rpm`, amd64 + arm64) are attached to every
[release](https://github.com/dbtrail/dbtrail/releases) as `bintrail-pg_*`:

```bash
# Debian/Ubuntu
sudo dpkg -i bintrail-pg_*_linux_amd64.deb
# RHEL/Fedora
sudo rpm -i bintrail-pg_*_linux_amd64.rpm
```

**From source:**

```bash
git clone https://github.com/dbtrail/dbtrail && cd dbtrail
make build-pg        # produces ./bintrail-pg (requires CGO — DuckDB is embedded)
```

`bintrail-pg` carries the PostgreSQL capture command (`stream`) plus the shared
read/recovery commands (`query`, `recover`, `reconstruct`, `status`, `shim`) —
the same ones the core binary exposes, working over the same index.

---

## PostgreSQL-side setup

Run these once on the source, as a superuser (or the managed-database master
user). The examples use a database named `shop` whose tables are in the
`public` schema, which is where PostgreSQL puts a table created without a
schema name. Replace `shop` with your database and `public` with the schema
your tables live in (`\dt *.*` in `psql` lists them with their schema).

### 1. Enable logical decoding (`wal_level = logical`)

Logical replication is impossible without it, and bintrail-pg refuses to start
if it is not set.

```sql
SHOW wal_level;        -- want: logical
ALTER SYSTEM SET wal_level = 'logical';
-- wal_level only takes effect after a RESTART (not a reload):
--   self-hosted:  restart the postgres service
--   managed (RDS/Aurora/Cloud SQL): set it in the parameter group and reboot
```

While you are there, make sure there is slot/sender headroom (defaults are
usually fine; a slot uses one of each):

```sql
SHOW max_replication_slots;   -- ≥ 1
SHOW max_wal_senders;         -- ≥ 1
```

### 2. Create a replication user

bintrail-pg connects with a role that has the **`REPLICATION`** attribute (this
is what lets it open a replication connection and create the slot). It only ever
reads.

```sql
CREATE ROLE dbtrail WITH LOGIN REPLICATION PASSWORD <choose a password>;
GRANT CONNECT ON DATABASE shop TO dbtrail;
-- Connected to shop: the first copy of the tables reads them.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbtrail;
```

Put a password of your own in quotes where it says `<choose a password>`. As
written, PostgreSQL refuses the line on purpose, so no role is created with a
password copied from this page.

- The role does **not** need to be a superuser.
- **Capturing changes needs no `SELECT`**: logical replication delivers the
  row data over the WAL. **The first copy does.** It reads every table of the
  publication with `COPY (SELECT ...)`, as this role, so without `SELECT` it
  would stop with `permission denied for table ...` (`42501`). `server add`
  checks it (**Read access to the published tables**) and warns about a role
  that cannot read one of them, showing the `GRANT` to run; DBTrail grants
  nothing by itself. It is a warning: the server is added and its changes are
  recorded, since that needs no `SELECT`. The copy is what is refused. A
  snapshot asks before it starts, and stops with the same statement before
  the slot or any file is created, until the permission is there. That also
  covers a table added to the publication later, which is read in full once,
  and a permission taken away since the server was added.
- **Disk for the first copy.** It is written into the server's Local folder as
  compressed Parquet. Before it starts, DBTrail compares the tables' size
  with the free space there: it refuses below 1 GiB free and warns below 1.3×
  the tables' size. See
  [Disk for a PostgreSQL snapshot](capacity.md#disk-for-a-postgresql-snapshot-the-snapshot-folder-is-the-one-that-fills).
- `GRANT SELECT ON ALL TABLES IN SCHEMA` covers the tables that exist when it
  runs. For the ones created later, run once
  `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dbtrail;`
  as the role that creates them: default privileges apply only to tables that
  role creates.
- For a schema other than `public`, the role also needs
  `GRANT USAGE ON SCHEMA <schema> TO dbtrail;` (every role has it on `public`
  by default).
- It also reads system catalogs (publications, replica identity, primary
  keys), which any role can read by default.
- **Managed PostgreSQL:** the master user already has the replication privilege
  (e.g. AWS grants `rds_replication`); you can capture as the master user, or
  create a dedicated role and grant it the provider's replication role
  (`GRANT rds_replication TO dbtrail;` on RDS/Aurora). See
  [Managed PostgreSQL](#managed-postgresql) below.

### 3. Set `REPLICA IDENTITY FULL` on every captured table

By default PostgreSQL puts only the **primary-key** columns of the earlier row
in the WAL for an `UPDATE` or a `DELETE` (`REPLICA IDENTITY DEFAULT`).
`REPLICA IDENTITY FULL` makes it log the whole earlier row, including a large
value stored out of line that the `UPDATE` did not touch. **DBTrail captures a
table only at `REPLICA IDENTITY FULL`.**

```sql
ALTER TABLE public.orders    REPLICA IDENTITY FULL;
ALTER TABLE public.customers REPLICA IDENTITY FULL;
-- repeat for every table you want to capture
```

It costs WAL: the table then logs the whole earlier row for every `UPDATE` and
`DELETE`. `FULL` needs no primary key. For a partitioned table,
`REPLICA IDENTITY` is set **per partition**: the statement on the parent does
not reach its partitions, and the parent and each partition need it. `doctor`
lists them.

A table that is not at `FULL` (at `DEFAULT`, `USING INDEX` or `NOTHING`, or a
partitioned table with a partition that is not) does **not** stop capture,
because one such table would otherwise stop every other table with it.
Instead:

- **Its changes are not captured**, its `INSERT`s included. They are dropped,
  never stored with part of the earlier row, and each one is counted.
- **The other tables keep being captured** normally.
- **`doctor` fails** on it, under **REPLICA IDENTITY FULL**, with one
  `ALTER TABLE` per table and partition (the first twenty, with a count of the
  rest). `server add` and the web interface's Start run `doctor` first, so a
  server is not added or started while one such table is in the publication.
  A capture that is already running, or that the daemon resumes when it
  restarts, goes on and skips the table alone.
- **The log says so**, once when capture starts and once each time PostgreSQL
  describes the table, with the exact statement to run, for example:

  ```
  level=WARN msg="pgcapture: table is not at REPLICA IDENTITY FULL; its changes are NOT captured (each one is counted as skipped in capture health) until it is, the other tables keep flowing. Run the statement in fix: capture of the table starts by itself at its next change, with no restart; the changes skipped before it are missing from the copy until the table's next full read, and from history for good. On PostgreSQL 18 run `bintrail-pg doctor` first: with a generated column that the publication does not publish, PostgreSQL refuses UPDATE and DELETE on a table at FULL" table=public.rf_def replica_identity="'d' (default: primary key only)" fix="ALTER TABLE \"public\".\"rf_def\" REPLICA IDENTITY FULL"
  ```

- **`status` shows capture as DEGRADED** under the reason
  `replica_identity_not_full` and names the table. The count is kept across
  restarts.
- **The web interface's Status page** lists the table in red, among the
  tables `NOT captured`.
- **After you run the `ALTER TABLE`**, PostgreSQL describes the table again
  before its next change, and capture of it resumes by itself, with no
  restart. For a partitioned table that happens once PostgreSQL describes it
  with every partition at `FULL` (before the first change made after the
  statement in the partition the change is in, and at the latest when capture
  restarts). Changes made before the `ALTER` stay missing: PostgreSQL re-sends
  them exactly as they were written, so they are skipped again on every
  re-read. The table's next full read brings its copy up to date; its history
  of that stretch stays missing, so nothing in it can be queried or undone.
- **A table put back at `DEFAULT` while capture runs** is skipped from its
  next change on, the same way, and resumes when it is at `FULL` again.
- **Its copy stays as the last update left it.** Once capture has dropped a
  change of the table made after its rows were last read from the source, a
  scheduled update of the server's snapshot brings the other tables up to date
  and publishes this one as the same file, with its footer: never rebuilt
  from the index, which misses the dropped changes. Whether a table's copy
  holds every change since its rows were read is decided from that footer,
  against the capture's record of dropped changes: by WAL position for a file
  that is a full read of the source, and, for a file an update built, by the
  time of its last read of the source, with a five-minute margin toward
  refusing (an update's floor can be past a drop the capture had not saved
  yet, and the two times come from two clocks). So the readers that ask it
  (fresh mode, the PostgreSQL port, reads at a past moment, `verify`) do not
  take such a table as current. Where it shows: the daemon logs a warning
  for each such table (`table published as its last read left it, not
  brought up to date`, with why), and the Status page lists it among the
  tables NOT captured.
  The snapshot pages still count it among the tables reused unchanged, and
  show it as `reused unchanged`. Fresh mode leaves it out, and statements on it read the
  copy as of that file (a server's lag limit, `max_behind_source_seconds`,
  sends them to the source when that is too old); with a forwarding user the
  PostgreSQL port sends them to the source (`table_not_captured`).
  This rests on the capture's record of dropped changes, which is saved with
  its checkpoint. Until the next checkpoint (0.3 s on a healthy capture, in
  one measurement) a table can still read as current. A capture that has
  saved its state and then cannot save it again leaves later drops
  unrecorded, so the port can keep answering that table from its copy with
  no notice ([#3202](https://github.com/dbtrail/dbtrail/issues/3202)).
  Where the file cannot be published again as it is (a snapshot read from S3,
  or with `--baseline-carry-forward-unchanged` off), the update is refused for all the
  tables (`refused-gap`, naming the table). A full read brings the copy up to
  date.
- **A read of one of its rows at a past moment is refused**: single-row
  `reconstruct` (the command line, the MCP tool, the web interface) and, on
  the PostgreSQL port, `AS OF`, `_snapshot` and `_flashback`. The refusal names
  the table, why, and what to run, for example:

  ```
  a read of public.pgrf_def at a past moment is refused: capture dropped changes of public.pgrf_def (replica_identity_not_full) since its rows were read from the source: replica identity 'd' (default: primary key only); DBTrail captures a table only at REPLICA IDENTITY FULL; fix: ALTER TABLE "public"."pgrf_def" REPLICA IDENTITY FULL. What DBTrail holds of the table misses those changes, so the row it would show could be wrong. Fix the table on the source (for a table that is not at REPLICA IDENTITY FULL: ALTER TABLE "public"."pgrf_def" REPLICA IDENTITY FULL), then take a full read of it: a moment after that full read can be read
  ```

  The capture's record keeps only its newest drop, so a moment before the
  first dropped change is refused as well. `_flashback` reads the recorded
  changes alone, with no snapshot, and stays refused for such a table.
  `verify` of the table, against the source and between two snapshots,
  reports it as not compared, with the same reason. `reconstruct --baseline-only` reads the snapshot alone: it
  is checked the same way when `--index-dsn` is given, and says it was not
  checked, and reads it, when no index is given, the one given cannot be
  read, or it is not this PostgreSQL capture's.
- **A table created in a `FOR ALL TABLES` publication** (or in a schema a
  `FOR TABLES IN SCHEMA` publication covers) starts at PostgreSQL's
  `REPLICA IDENTITY DEFAULT`: it is not captured until you run
  `ALTER TABLE ... REPLICA IDENTITY FULL` on it. Run it right after the
  `CREATE TABLE`, before its first rows, and nothing of it is missed.

> This is the direct counterpart of the MySQL/MariaDB `binlog_row_image = FULL`
> requirement.

**Upgrading from 0.138.0.** Version 0.138.0 captured a table that was not at
`FULL`, with the earlier row of each `UPDATE` and `DELETE` holding the key
only. From this version on such a table is not captured, and `doctor` fails on
it. What that means for a server that has one:

- The capture keeps running after the upgrade and takes the other tables. The
  table's changes from the upgrade on are skipped and counted, and `status`
  shows capture as DEGRADED under `replica_identity_not_full`.
- What 0.138.0 recorded of the table stays in its history, and undo still
  refuses its `UPDATE`s and `DELETE`s (`cannot undo a change in ... because the
  table was not at REPLICA IDENTITY FULL when the change was captured`).
  `verify` stops on those changes too; that does not mean the data is wrong.
- Its copy is not brought up to date from the index any more (see the list
  above): fresh mode leaves the table out, scheduled updates publish it as
  the last update left it, and a read of one of its rows at a past moment is
  refused.
- To capture it again, run the `ALTER TABLE` that `doctor` prints, then a full
  read of the table, so its copy holds the changes skipped in between.
- A server that is stopped cannot be started from the web interface while
  `doctor` fails on such a table: fix the table first, or take it out of the
  publication.

**A partitioned table published through its parent**
(`publish_via_partition_root`) is described to capture as the parent, and
PostgreSQL labels the earlier row of each change by the parent's setting,
whatever the partition the change was made in logged. When PostgreSQL
describes the table, capture reads from the catalog whether it is partitioned
and the identity of each partition, and captures the table only when the
parent and every partition are at `FULL`. The read takes no lock on any
partition, so a `VACUUM FULL` or a rewriting `ALTER` of one does not hold
capture up. Two things follow from reading today's catalog:

- An earlier row labelled whole is stored whole only when **none of its
  columns arrives empty**; one that holds an empty column is stored with its
  key only, and undo refuses that change. The reason: a change made in a
  partition before it was put at `FULL`, and read by a capture that was behind,
  arrives labelled whole and holding its key alone. **The cost**: a row of a
  partitioned table that really holds a NULL in some column is stored that way
  too, though its partition is at `FULL`. Nothing wrong is stored: NULL is
  never written where PostgreSQL did not send a value. A table that is **not**
  partitioned is not affected: its earlier rows are whole, NULLs included.
- A change whose earlier row arrives with its key only, or with none (an
  `UPDATE` made in such a partition before it was put at `FULL`), is skipped
  and counted under `replica_identity_not_full`, as for a table below `FULL`.

If the catalog read fails or times out, capture goes on and reads that table's
earlier rows as above; after a failed read it does not ask for any table's
partitions for a minute (one attempt a minute for all tables, each of three
seconds at most), and the log says so once, with the cause. If the catalog
then answers that a partition is not at `FULL`, the table is skipped from
there on.

> **PostgreSQL 18 and generated columns: read this before you set `REPLICA IDENTITY FULL` or create the publication.**
> On PostgreSQL 18, a table that is at `REPLICA IDENTITY FULL`, belongs to a
> publication, and has a generated column that the publication does not
> publish **refuses every `UPDATE` and `DELETE`**, from your own application
> too, with `Replica identity must not contain unpublished generated columns`.
> `INSERT` still works. It takes effect the moment both settings are in place,
> whether or not DBTrail is running.
>
> Find your generated columns first:
>
> ```sql
> SELECT c.relnamespace::regnamespace AS schema, c.relname AS "table", a.attname AS "column",
>        CASE a.attgenerated WHEN 's' THEN 'STORED' ELSE 'VIRTUAL' END AS kind
> FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
> WHERE a.attgenerated <> '' AND a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p');
> ```
>
> - **`STORED`**: make the publication publish it first, then set `FULL`.
>   Create the publication with `WITH (publish_generated_columns = stored)`
>   (step 4), or, for one that exists:
>   `ALTER PUBLICATION bintrail_pub SET (publish_generated_columns = stored);`
> - **`VIRTUAL`** (new in 18, and what `GENERATED ALWAYS AS (...)` creates there
>   when neither word is given): PostgreSQL cannot publish it, so that table
>   cannot be at `REPLICA IDENTITY FULL` while it is published, and **DBTrail
>   cannot capture it** while it has that column. Do not set `FULL` on it. Take
>   it out of the publication (a `FOR ALL TABLES` publication cannot leave a
>   table out: list the tables instead), or replace the column with a `STORED`
>   generated one and publish it.
> - A table that belongs to **another** publication as well is refused if that
>   one does not publish the column, whatever DBTrail's publication does.
> - **If your application is already refused**, this lets it write again at
>   once: `ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;` (DBTrail then
>   does not capture the table).
>
> `bintrail-pg doctor`, the web interface's checks when a server is added, and
> the capture when it starts all look for this and stop with the statements to
> run. For a table that is not at `FULL` yet, `doctor` prints the publication
> statement before the `ALTER TABLE` for a `STORED` column, and no `ALTER
> TABLE` at all for a `VIRTUAL` one. PostgreSQL 17 and older have no such rule.

**A table with no replica identity at all** (`REPLICA IDENTITY NOTHING`, or
`DEFAULT` with no usable primary key): PostgreSQL itself refuses `UPDATE` and
`DELETE` on it while it is in a publication, with `cannot update table "..."
because it does not have a replica identity and publishes updates`. DBTrail
does not capture its `INSERT`s either. Set `REPLICA IDENTITY FULL`, which needs
no primary key.

#### Other tables that are not captured

Capture checks each table as PostgreSQL describes it: when it starts, for a
table added to the publication later, and after a change of the table's
definition. These are **skipped** too, each under a reason of its own, while
every other table keeps being captured:

- **A replica identity that leaves a primary-key column out**
  (`replica_identity_index_without_primary_key`): in practice
  `REPLICA IDENTITY USING INDEX` on a unique index that does not include the
  primary key, on the table or on one of its partitions. Such a table is below
  `FULL`, so it fails the **REPLICA IDENTITY FULL** check as well; `doctor` also
  reports it under **Replica identity holds the primary key** as a failure.
  The statement is the same in both: `ALTER TABLE ... REPLICA IDENTITY FULL`.
- **A primary key declared `DEFERRABLE`** (`primary_key_deferrable`). `doctor`
  reports it under **Primary keys are not deferrable** as a failure. A capture
  that starts with such a table says so once and skips it; it does not refuse
  to start. The two fit together this way: capture never refuses to start over
  a deferrable key, but `server add` and the web interface's Start run
  `doctor` first, and its check fails, so they do not add or start the server
  until the key is fixed or the table is out of the publication. A capture that
  is already running, or that restarts by itself, goes on and skips the table.
- **A primary key that PostgreSQL's description of the table does not hold**
  (`primary_key_not_in_description`): capture is behind, and since the changes
  it is reading were written a primary-key column was **renamed** (the common
  case), or the key was moved to another column. No statement brings those
  changes back. The table is captured again from the changes made after the
  key was changed; the ones skipped before it are missing from the copy until
  the table's next full read, and from the history for good.

For each of them, as for a table below `FULL`: the changes are dropped and
counted, the log says so with the statement to run, `status` shows capture as
DEGRADED under the reason, the Status page lists the table in red, and after
the fix capture of the table resumes by itself; the changes skipped before it
are never recorded.

An index written by version 0.138.0 may hold skips under
`replica_identity_key_not_sent`: a change of a table below `FULL` whose earlier
row came without a value for a primary-key column. PostgreSQL is not known to
send one, and a table below `FULL` is not captured any more.

**The limit of the deferrable-key check.** A primary key cannot be made
deferrable in place (PostgreSQL refuses `ALTER TABLE ... ALTER CONSTRAINT` on a
primary key: it works on foreign keys only). It is dropped and added again, and
PostgreSQL then describes the table to capture before the table's next change,
so the table is skipped from there on without a restart. What the check reads
is the catalog as it is at the moment capture reads a change, not as it was
when the change was made. So when capture is behind:

- a key that was deferrable while some changes were made, and is not any more
  when capture reads them, is not seen, and those changes are captured;
- a key made deferrable after some changes were made and before capture reads
  them has those changes skipped too.

A change of a key that PostgreSQL does not describe the table again for is seen
at the next start of capture, where every table is described again.

### 4. Create a publication

A **publication** is PostgreSQL's list of which tables get streamed. You create
it; bintrail-pg validates that it exists and that it covers the tables you ask
for, but **does not create it for you** (we never run DDL on your source).
The publication must publish **all** of insert/update/delete (the default when
`publish` is left unset) and must not attach a PG15+ row filter (`WHERE (...)`)
or column list to any table — bintrail-pg refuses a partial publication at
startup and in `doctor`, because pgoutput would silently drop the excluded
changes.

It should also publish **truncate** (also the default). A publication created
`WITH (publish = 'insert, update, delete')` sends nothing for a `TRUNCATE`, so
DBTrail never learns that the table was emptied: the removed rows stay in the
copy and come back in every rebuild, and nothing in the recorded history says
they are gone. Every insert, update and delete is still captured, so this is a
**warning**, not a refusal: at the start of capture (once), in `doctor` and the
server checks (**Publication publishes TRUNCATE**), and on the PostgreSQL
health card of the web interface. A snapshot taken after the `TRUNCATE` starts
from what the table holds then; a rebuild of a moment before it still shows
the removed rows. bintrail-pg does not change the publication; run, as its
owner or a superuser:

```sql
ALTER PUBLICATION bintrail_pub SET (publish = 'insert, update, delete, truncate');
```

`SET (publish = ...)` changes only which operations are published: the
publication keeps its tables and its other options (`publish_via_partition_root`,
`publish_generated_columns`). The check runs when capture starts, when the
doctor runs, and with each health reading (every 30 seconds) while capture
runs, so a publication changed later is shown on the health card and logged
when it is first seen.

```sql
-- Specific tables (recommended — you control exactly what is captured):
CREATE PUBLICATION bintrail_pub FOR TABLE public.orders, public.customers;

-- On PostgreSQL 18, when a captured table has a STORED generated column
-- (see the note in step 3):
CREATE PUBLICATION bintrail_pub FOR TABLE public.orders, public.customers
  WITH (publish_generated_columns = stored);

-- …or everything (requires superuser):
CREATE PUBLICATION bintrail_pub FOR ALL TABLES;
```

To add a table later: `ALTER PUBLICATION bintrail_pub ADD TABLE public.items;`.
Set `ALTER TABLE public.items REPLICA IDENTITY FULL;` before adding it: a
table that is not at `FULL` is not captured (see step 3).

### 5. The replication slot — created for you

You do **not** need to create a replication slot. bintrail-pg creates a logical
slot (named by `--slot`) on first run and resumes from it on every restart.

> **Operational note (important):** a replication slot *retains WAL on the
> source* until its consumer confirms it. If bintrail-pg is stopped for a long
> time, the slot pins WAL and the source disk can fill. On PostgreSQL 13+ set
> `max_slot_wal_keep_size` as a safety valve so an abandoned slot is capped
> (it becomes `lost` instead of filling the disk; bintrail-pg then fails loud on
> resume rather than silently skipping data). To decommission a capture, use
> `bintrail-pg reset` (below) rather than dropping the slot by hand.

#### Checking slot health

`bintrail-pg doctor` reports the slot's live WAL-retention state on demand,
alongside the capture prerequisites (`wal_level`, publication coverage, replica
identity):

```bash
bintrail-pg doctor --query-dsn "$PG" --slot bintrail_shop --publication bintrail_shop_pub
```

- **`max_slot_wal_keep_size`** → WARN while unlimited (`-1`); set a bound to clear it.
- **Replication slot health** → shows `wal_status` and how much WAL the slot is
  retaining. The status progresses `reserved` (PASS) → `extended` / `unreserved`
  (**WARN** — retaining WAL and approaching the limit; doctor still exits 0) →
  `lost` (a loud **FAIL** with the re-baseline recovery path).
- **REPLICA IDENTITY FULL** → FAIL for each published table that is not at
  `FULL` (and for a partitioned one with a partition that is not): capture
  skips it. See [step 3](#3-set-replica-identity-full-on-every-captured-table).
- **Replica identity holds the primary key** → FAIL for a table whose replica
  identity leaves a primary-key column out: capture skips it.
- **Publication publishes TRUNCATE** → WARN if the publication leaves
  `truncate` out of `publish`: a `TRUNCATE` on the source is not recorded, and
  the rows it removed stay in the copy and in every rebuild. Doctor still exits
  0, and capture starts. The check prints the `ALTER PUBLICATION ... SET
  (publish = 'insert, update, delete, truncate')` to run (see
  [step 4](#4-create-a-publication)).
- **Read access to the published tables** → WARN if the role cannot read a
  table the first copy reads: no `USAGE` on its schema, or `SELECT` neither on
  the table nor on every column the copy reads. Doctor still exits 0, and
  capture starts: recording changes does not need it. A snapshot is refused
  until it is granted. Ownership, a superuser, a role
  inherited from another and `pg_read_all_data` count, as PostgreSQL itself
  counts them. The check prints one `GRANT` per table, each name quoted; past
  ten tables it prints `GRANT SELECT ON ALL TABLES IN SCHEMA` once per schema,
  which also covers the tables of that schema that are not published.
- **No UNLOGGED tables** → WARN if any captured table is `UNLOGGED` (under a
  `FOR ALL TABLES` publication, and under a `FOR TABLES IN SCHEMA` — PostgreSQL
  15+ — publication for its member schemas). UNLOGGED tables write no WAL, so
  their changes are never captured — `ALTER TABLE <t> SET LOGGED` if you need
  them, or ignore if the data is intentionally ephemeral.
- **FK cascade-child coverage** → WARN if a published table has a foreign-key
  `ON DELETE CASCADE` / `SET NULL` **child** that is *not* in the publication. A
  delete on the parent would rewrite that child, and the rewrite would not be
  captured: add the child to the publication, at `REPLICA IDENTITY FULL` like
  every captured table.

These coverage checks are also emitted as warnings by `bintrail-pg stream` at
startup, so capture is never silently incomplete even if you skip `doctor`.

A permanently-lost stream is also recorded durably: once a slot is invalidated or
dropped out from under a running capture, `bintrail status` shows a loud
**`EVENTS PERMANENTLY LOST`** banner even after the process has exited. In every
case the index up to the gap is still fully usable for recovery — recovery never
needs the slot; only *resuming capture* requires a re-baseline.

#### When capture was last caught up

With each of those health polls the capture also asks the source how far its
WAL is flushed. If the capture has received the source's changes up to that
point within a second of asking, it held everything the source had at that
moment, and it records the moment in the index at its next checkpoint. The web
interface and the router read it to say how far behind the source an answer
is. A capture that keeps up records one about every 30 seconds; one that is
more than a second behind records none, and shows as behind. It is one
`SELECT` on the health poll's connection.

#### A large transaction arrives whole, after it commits

PostgreSQL sends a transaction to capture only once it has committed (capture
asks for version 1 of the `pgoutput` protocol, which does not stream a
transaction that is still open). Nothing is missing from the copy while it is
open: its changes are not visible on the source either. The cost comes at the
commit. The whole transaction is then sent in one piece and indexed, and every
change committed after it waits behind it. A transaction that changed a few
million rows can leave the copy minutes behind, where the same rows in small
transactions would have gone through as they were made.

So what sets the delay is how MUCH one transaction changed, not how long it
stayed open: a transaction open for an hour that changed ten rows costs
nothing. While the large one goes through, the web interface shows the capture
as behind and the copy stays where it was; both recover by themselves once it
is in. If a job on the source changes rows in
bulk, committing in batches keeps the copy close.

---

## Running it

PostgreSQL needs **two connection strings** — this is a protocol constraint, not
a quirk:

- **`--repl-dsn`** — a **replication** connection. Its connection string must
  include `replication=database`. A replication connection runs in *walsender*
  mode and cannot run ordinary SQL, which is why a second connection is needed.
- **`--query-dsn`** — an ordinary connection, used for primary-key lookups in
  the catalog and the startup validations.

Both are standard PostgreSQL/libpq URLs (so TLS is configured the usual way, via
`sslmode=` in the DSN).

```bash
bintrail-pg stream \
  --index-dsn   'user:pw@tcp(index-host:3306)/binlog_index' \
  --repl-dsn    'postgres://dbtrail:pw@pg-host:5432/shop?replication=database&sslmode=require' \
  --query-dsn   'postgres://dbtrail:pw@pg-host:5432/shop?sslmode=require' \
  --slot        bintrail_shop \
  --publication bintrail_pub \
  --server-id   201 \
  --schemas     shop
```

That command: validates `wal_level` and the publication, and warns about any
table not at `REPLICA IDENTITY FULL` (not captured; the other tables are); creates the slot `bintrail_shop` if absent; bootstraps
the index tables if needed; then streams every row change into `binlog_events`.
Send `SIGINT`/`SIGTERM` for a graceful shutdown (it flushes the batch and writes
a final checkpoint).

### Flags

| Flag | Required | Meaning |
|---|---|---|
| `--index-dsn` | yes | The index MySQL database (env `BINTRAIL_INDEX_DSN`). |
| `--repl-dsn` | yes | PostgreSQL replication DSN, must carry `replication=database` (env `BINTRAIL_PG_REPL_DSN`). |
| `--query-dsn` | yes | PostgreSQL ordinary DSN for catalog/PK lookups (env `BINTRAIL_PG_QUERY_DSN`). |
| `--slot` | yes | Logical replication slot name; created if absent (env `BINTRAIL_PG_SLOT`). |
| `--publication` | yes | Publication name covering the tables to capture (env `BINTRAIL_PG_PUBLICATION`). |
| `--server-id` | yes | A unique number identifying this source in the index — must differ from every other source (env `BINTRAIL_SERVER_ID`). |
| `--schemas` | no | Only index these schemas (comma-separated). |
| `--tables` | no | Only index these tables (comma-separated, e.g. `public.orders`). |
| `--start-lsn` | no | Explicit start LSN; **first run only**, ignored once a checkpoint exists. |
| `--batch-size` | no | Events per batch insert (default 1000). |
| `--checkpoint` | no | Checkpoint interval in seconds (default 5). |
| `--partitions` | no | Index partitions for the one-time bootstrap (default 48). |
| `--metrics-addr` | no | Address to expose Prometheus metrics on, e.g. `:9090`; empty (the default) exposes none (env `BINTRAIL_METRICS_ADDR`). See [observability.md](observability.md#postgresql-capture-bintrail_pg_). |
| `--rotate-retain` | no | Built-in rotation: drop index partitions older than this (`Nd`/`Nh`; `off` disables). Default `48h` for a new index; an index created before that default changed keeps the window it was created under (env `BINTRAIL_ROTATE_RETAIN`). |
| `--rotate-interval` | no | Built-in rotation: how often a rotation cycle runs (default `1h`, env `BINTRAIL_ROTATE_INTERVAL`). |
| `--rotate-add-future` | no | Built-in rotation: keep at least N future hourly partitions ready (default `3`, env `BINTRAIL_ROTATE_ADD_FUTURE`). |

Every flag has a `BINTRAIL_*` environment equivalent and can be set in a
`.bintrail.env` file (see [the env-file convention](install.md)).

### Resuming and re-seeding

The durable checkpoint is the last committed **LSN**, stored in the index's
`stream_state`. On restart bintrail-pg resumes from it automatically — re-running
the same command is idempotent, and `--start-lsn` is ignored once a checkpoint
exists.

To start over from scratch you must drop the slot **and** clear the checkpoint —
because in PostgreSQL the *slot* governs the resume position, so clearing the
checkpoint alone does not rewind. `bintrail-pg reset` does both (it drops the slot
first, so an interrupted reset fails safe — "slot gone, checkpoint stale" rather
than "checkpoint cleared, slot live"):

```bash
bintrail-pg reset --query-dsn "$PG" --index-dsn "$IDX" --slot bintrail_shop --force
```

`--force` confirms the destructive teardown. If the slot is already gone or `lost`
(e.g. after a `max_slot_wal_keep_size` invalidation), pass `--index-only` to clear
just the checkpoint:

```bash
bintrail-pg reset --index-dsn "$IDX" --index-only --force
```

This removes only capture-resume state; the index's recovery data is untouched.
Under the hood it is the two-system teardown that otherwise has to be done by hand
(`SELECT pg_drop_replication_slot('bintrail_shop')` on the source + clearing the
position/counter columns of the `stream_state` row on the index — the row is
never DELETEd, so a recorded `gap_lost_at`/`gap_lost_detail` continuity-loss
record is never erased by a reset; a real-checkpoint reset replaces the
record's detail with the reset's own and withdraws any acknowledgement of the
earlier loss, a no-checkpoint reset preserves it verbatim).

**A reset that discards a real checkpoint is itself recorded as a permanent
continuity loss**: dropping and recreating the slot skips whatever the old slot
had not yet streamed, so the discarded LSN is stamped into
`gap_lost_at`/`gap_lost_detail` — `bintrail status` shows the
`EVENTS PERMANENTLY LOST` banner and `status --fail-on-gap` exits non-zero,
exactly as after a slot invalidation. After a reset, re-seed the baseline and
re-run `bintrail-pg stream`.

### Index rotation and retention

The index table `binlog_events` is split into **hourly partitions**. New hours
need new partitions or every event eventually lands in the catch-all `p_future`
partition, which can never be dropped — so without rotation the index grows until
the disk fills.

`bintrail-pg stream` runs a **built-in rotation loop** so this is handled for you:
by default it drops partitions older than **30 days** and keeps a few future
partitions ready, running one cycle at startup and then every hour. Tune it with
`--rotate-retain` / `--rotate-interval` / `--rotate-add-future` (or the
`BINTRAIL_ROTATE_*` env vars), or turn it off with `--rotate-retain off` if you
prefer to run retention separately.

The built-in loop is **drop-only** — it does not archive partitions to S3 before
dropping them. For an archive-then-drop retention policy, or for manual/offline
maintenance, run the standalone command against the same index (it is the same
index-side command the core `bintrail` binary exposes, now available on
`bintrail-pg` too):

```bash
# One-off: drop partitions older than 7 days
bintrail-pg rotate --index-dsn "$IDX" --retain 7d

# Daemon: archive each expired partition to S3, then drop it
bintrail-pg rotate --index-dsn "$IDX" --retain 30d --daemon \
  --archive-dir /var/lib/bintrail/archives --archive-s3 s3://my-bucket/archives/

# Re-sync the archive registry with the files actually on disk / in S3
bintrail-pg archive reconcile --index-dsn "$IDX" \
  --archive-dir /var/lib/bintrail/archives --archive-s3 s3://my-bucket/archives/
```

> **Upgrading an existing PostgreSQL install that predates built-in rotation.**
> If you ran `bintrail-pg stream` before this feature, its index has no future
> partitions and everything has piled into `p_future`. No data is lost — the loop
> refuses to drop deep history until you choose a retention explicitly, and it
> back-fills the missing partitions — but the first catch-up can be slow because
> each cycle rewrites the large `p_future`. To catch up in one pass instead, run a
> standalone rotation with enough headroom before (or alongside) the stream, e.g.
> `bintrail-pg rotate --index-dsn "$IDX" --add-future <hours-since-first-event>`,
> then set `--rotate-retain` explicitly (e.g. `30d`) so the loop begins pruning.

---

## Adding more servers

Capture each PostgreSQL source by running its **own** `bintrail-pg stream`
process with a **unique `--server-id`** and a unique `--slot`. There is no
shared daemon for PostgreSQL sources in this release — one process per source
(systemd unit, container, etc.).

You can **view and recover** PostgreSQL-captured data in the read-only web
interface (`bintrail-console`), which reads the shared index. The web interface presents
PostgreSQL sources natively — LSN/slot vocabulary, a lost-slot badge, a
connection-id note, and a live replication-health panel (slot WAL-retention, lag, RI-FULL); see
[PostgreSQL sources](console.md#postgresql-sources). What it does **not** drive is
*capture*: the "+ Add server" / `watch` control plane is MySQL-oriented, so run
`bintrail-pg stream` for the capture and use the web interface (or the `query`/`recover`
CLI) to browse and recover.

---

## Querying and recovering

Once events are flowing, the read/recovery commands work the same as for a MySQL
source — they read the flavor-agnostic index:

```bash
bintrail-pg query   --index-dsn '…' --schema shop --table orders --limit 20
bintrail-pg recover --index-dsn '…' --schema shop --table orders --pk 42 --dry-run
bintrail-pg status  --index-dsn '…'
```

(The core `bintrail` binary works against the same index too — the read plane is
identical across binaries.)

**What's recoverable:** every captured row change, with full before/after
images: DBTrail captures a table only at `REPLICA IDENTITY FULL`. Under it,
unchanged out-of-line TOAST values are present in the before-image, so a
reversal of a large-column row is correct. An UPDATE or a DELETE that version
0.138.0 recorded for a table that was not at `FULL` is refused
([step 3](#3-set-replica-identity-full-on-every-captured-table)). FK `ON DELETE CASCADE` / `SET NULL` cascades are visible in the stream
(PostgreSQL performs them as ordinary row changes), so `recover` undoes them
directly.

> **Baseline snapshots exist for PostgreSQL, with a narrower scope than
> MySQL.** `bintrail-pg baseline` takes a consistent Parquet snapshot directly
> from a live PostgreSQL source, anchored to the replication slot's own LSN
> floor. It feeds **single-row** `reconstruct` and the shim's **single-row**
> `_snapshot` — both fold a PG baseline with binlog deltas. This fold is
> **validated end-to-end for scalar types**: the canonical set (`int2/4/8`,
> `numeric`, `bool`, `uuid`, `text`/`varchar`/`char`, `json`, `jsonb`, enums)
> **and** the GUC-sensitive set (`timestamptz`, `timestamp`/`date`/`time`,
> `real`/`double precision`, `bytea`, `interval`), whose text rendering is
> pinned identically on the baseline COPY connections and the logical-decoding
> session (`TimeZone=UTC`, `DateStyle=ISO`, `extra_float_digits=3`,
> `bytea_output=hex`, `IntervalStyle=postgres`) — and user-defined
> **composite** types. Container types beyond that (arrays beyond the tested
> `integer[]`/`text[]`, range/multirange, `hstore`, geometric) remain
> untested through the fold — validate your own round-trip. **Re-baseline after upgrading:** baselines taken before the
> rendering-GUC pin were rendered under your server's session defaults; on a
> server whose defaults differ from the pinned values, re-run
> `bintrail-pg baseline` — the baseline↔delta merge is an exact text join
> (the reader warns when it meets a pre-pin baseline). Events indexed
> **before** the upgrade also keep their pre-pin rendering: on such a server a
> GUC-sensitive primary key (`timestamptz`, `float`) has two text identities
> across the upgrade, so `--pk` lookups and `--history` can split there;
> re-baselining resolves this going forward (deltas before the new anchor are
> not replayed).
> **Where the changes after a snapshot start.** A PostgreSQL snapshot records
> an LSN floor read just before it opens, and reads start there, not at the
> snapshot's time: a transaction committing at the instant the snapshot opens
> can be missing from it, and is then applied from the index. Such a commit
> is looked for at least a minute before the snapshot's time; one held longer
> than that across the snapshot's start (a synchronous standby that stopped
> answering) is not covered. A primary-key-changing UPDATE in that minute
> makes the shim's whole-table `_snapshot` and `verify` refuse, as one after
> the snapshot does.
> **Full-table** `reconstruct --output-format parquet` and `bintrail baseline
> refresh` work for PostgreSQL: they fold a snapshot forward to a point in
> time, and the result is a snapshot the next fold starts from (see
> [Full-table snapshots](#full-table-snapshots)). `--output-format psql`
> writes SQL files psql loads, and `bintrail-pg drill` rehearses a restore with
> them into a scratch PostgreSQL (see
> [Restore drills](drill.md#postgresql-sources)). `--output-format mydumper`
> stays out of scope: a mydumper dump is MySQL's. `verify` digests row content without emitting a dump, so
> both baseline-anchored and live-source `verify` are supported for PostgreSQL
> (see
> [verify — Notes per source](verify.md#notes-per-source)). `query` and
> `recover` (which work from the indexed deltas alone, no baseline required)
> round out the recovery surface — see [Limitations](#limitations) for the
> complete picture.

### Full-table snapshots
`bintrail reconstruct --output-format parquet --tables public.orders --at …`
and `bintrail baseline refresh` take a PostgreSQL snapshot (from
`bintrail-pg baseline`, or a previous fold) and the indexed changes after it,
and write a snapshot of the whole table at the requested point. Each one
records where the next fold starts (an LSN) and how far back in time the
changes after it can reach, so a chain of folds applies every transaction
exactly once.

A snapshot schedule set on a PostgreSQL server in the web interface does the
same on its own. The first snapshot is a full read of the source; each slot
after it builds the next snapshot from the previous one and the changes
captured since, without reading the source again. An update holds the changes
up to capture's last saved position, which capture saves every 10 seconds. A
table added to the publication after the previous snapshot has no file to
update, so the update publishes without it and names it, and the schedule then
takes one full read of the source to bring it into the copy, as it does for a
new table on a MySQL source: where the schedule may take full reads, and no
more than a few times for the same table.

In that same read of the source, each update also asks the catalog what
columns every table of the publication has now, and compares them with the
columns the table's file in the previous snapshot records: names, types, and
each column's position number, which tells a column that was dropped and added
again under the same name from the one the snapshot holds values for.
PostgreSQL's change stream describes a table only when it next carries a
change of it, so a column dropped, added, renamed or retyped on a table that
nothing has written to since is recorded nowhere else. A table that differs
stops the update the way a column change the stream did record stops it: no
snapshot is published by that update, the run names each such table with what
changed (`dropped amount`, `added region`, `retyped total`), and the schedule
takes a full read of the source in its place where it may take full reads.
Where it may not, updates stay refused, with the tables named, until a full
read is taken. An index, a default, `NOT NULL`, a comment or a grant changes
no column and stops nothing. Only the catalog is read: nothing is created or
written on the source.

Two limits. The catalog is read once, at the start of the update: a column
changed after that read and before the snapshot is published is in that
snapshot with its old columns, and the next update sees it. And when the
source cannot be reached for that read, the update still publishes from the
changes it captured, as it does for new tables, and the run says that neither
new tables nor changed columns were looked for. An update that has no captured
change to apply is skipped before it reads anything, so on a source with no
writes at all a changed column is seen at the first update after the next
write to any table. `bintrail baseline refresh` on the command line does not
read the source and makes no such comparison.

A full read copies tables in parallel, each on a connection of its own to the
source. One started by DBTrail itself (the Create button, a schedule,
`snapshot run`) copies four tables at once, as a MySQL full read does, so at
most four connections to the source copy tables at a time. `bintrail-pg baseline` takes its number from `--parallelism`, one per
CPU of the machine it runs on unless set.

A full read starts from the replication slot's position, so it needs the slot
to have one. On a busy source capture can take a while to get its slot:
PostgreSQL finishes creating a slot only when every write transaction that was
open when the creation began has ended. A snapshot asked for while capture is
creating the slot (the Create button, `snapshot run`, `bintrail-pg baseline`)
waits for it, up to 5 minutes, and then reads; it does not read while the slot
has no position, because a change committed before the slot got one would be
in neither the snapshot nor the captured changes. Past the 5 minutes the
snapshot is refused with nothing read, and running it again once capture is
attached works. A source whose slot already has its position is read at once,
as before. When there is no slot yet, the snapshot creates it, and that
creation has no limit. Neither has the one capture does when it starts: the
server shows **CREATING SLOT** with the oldest write transaction open (its
session and for how long; its user and database when it is in the captured
database; a prepared transaction without its name, which `pg_prepared_xacts`
lists), and capture starts by itself when that ends. While either waits, the
log says once a minute how long it has waited; the transaction holding it is
the one with the oldest `xact_start` in `pg_stat_activity` among those with a
`backend_xid`. A snapshot asked for while capture is creating the slot still
waits only the 5 minutes above.

The slot is looked at again when the tables are read. A snapshot whose slot
was removed meanwhile, or whose changes start before the position capture
started reading from, fails and is not kept. A snapshot taken by an earlier
version that starts before capture did is not updated: the update is refused
for its tables and the schedule reads the source again in its place.

What to know:

- **The output is a snapshot, or SQL psql loads.** A snapshot's values are
  PostgreSQL's own text, as `bintrail-pg baseline` writes them.
  `--output-format psql` instead writes one file per table,
  `<schema>.<table>.sql`, that `psql -v ON_ERROR_STOP=1 -f <file>` loads in one
  transaction: the settings the values were rendered under, a `CREATE TABLE`
  built from the snapshot's recorded shape (columns, types, `NOT NULL`,
  defaults, identity and `serial` sequences, `STORED` generated columns, the
  primary key), the rows as `COPY ... FROM stdin`, and each sequence moved past
  the loaded rows. Not made: other indexes and constraints, collations,
  comments, grants, and any type or sequence a default refers to that the table
  does not own; a snapshot taken before these facts were recorded is refused
  (take a new one). `--output-format mydumper` is refused. The web
  interface's **Build a .sql backup for any moment** on a PostgreSQL server
  builds these same files, and its download is one `.tar.gz` of them.
- **A change to the table's columns or primary key refuses**, a type-only
  change included: the fold compares the shape the snapshot recorded with every
  shape the stream saw up to the point, and the key the changes were captured
  under with the one rows are joined by. Take a new snapshot with
  `bintrail-pg baseline`.
- **A TRUNCATE, a DROP or a rename refuses**, including one that committed at
  the instant the snapshot opened: it writes no row changes, so the fold
  would bring the rows back.
- **A table taken out of the publication and added back refuses** across that
  stretch: its changes while it was out were never captured. This is seen only
  for a `FOR TABLE` publication with `track_commit_timestamp = on` (see
  [Schema changes on PostgreSQL](#schema-changes-on-postgresql)); otherwise
  the fold cannot tell, and the chain's "every transaction exactly once" holds
  only for what capture received.
- **An UPDATE that changes the primary key is applied as a delete of the old
  key and an insert of the new one**, the way MySQL and MariaDB do it, each
  ordered against every other change of that key by the commit LSN (and,
  inside one transaction, by the order the changes were made in). This covers
  a key changed twice, changed and changed back, moved onto a key that was
  deleted, and two rows swapping keys through a third value, each change in
  its own transaction, and several rows moved by one statement when no row
  lands on a key that changes again in that statement's transaction. One such change still refuses the
  table, with the reason in the message, when it cannot be proven whole: the
  change has no commit LSN in the index, its new row holds a large value
  PostgreSQL did not send (the table was not at `REPLICA IDENTITY FULL`, and
  the value belongs to the row at the old key), a key column
  is missing from either row, or the key the index stores for it is not the
  one its old row spells. One more case refuses out of caution: the old row
  has NULL in every column outside the key and the new row has a NULL outside
  the key too. Such an old row looks the same as one captured with the key
  only, where a large value the UPDATE did not touch would have been stored
  as NULL. And one case refuses because the changes alone cannot say what
  happened: inside one transaction, a key receives a row (an INSERT, or a
  row whose key changed to it) and is changed again later in that
  transaction, with a key change among those changes. When a primary key is
  `DEFERRABLE`, two rows can hold the same key value inside a transaction, so
  `UPDATE t SET id = id + 1` over rows 1, 2 and 3 is captured as "1 to 2, 2
  to 3, 3 to 4", the same as one row moved three times, and the two leave
  different rows. This also refuses a row whose key is changed twice in one
  transaction, and a row inserted and re-keyed in the same transaction. The
  same statements in separate transactions are applied. A refused change refuses even when a later change
  overwrote it, and one in the minute before the snapshot, which the fold
  reads again (see above), counts too.
- **A transaction whose changes of one key do not describe one row
  refuses.** A `DEFERRABLE` primary key is checked at the end of the
  statement or of the transaction, so until then two rows can hold the same
  key value, with no key change involved: a transaction inserts a new row 1
  while the old row 1 is still there and then deletes the old one. The
  captured changes are an INSERT and a DELETE of key 1, the last one is the
  DELETE, and a copy that kept the last change would have no row 1 where the
  source has the new one. The fold therefore checks, for the changes one
  transaction makes to one key, that each starts from the row the one before
  it left: an INSERT from no row, an UPDATE or a DELETE from exactly the row
  the previous change wrote. When that does not hold, or the rows cannot be
  compared, the table is refused with a message that names the key and the
  transaction's commit LSN. The ordinary transactions are applied as before:
  a row inserted and deleted again, inserted and updated, updated twice,
  deleted and replaced under the same key.
  A transaction that capture delivered twice after a restart is recognized
  and applied once, when it looks exactly like one: the key's first change
  of the transaction comes back, and the changes after it repeat the changes
  before it, one by one, for as long as those ran (the first delivery may
  have been cut short; the last one is whole). A first change that comes back
  followed by anything else refuses. Two limits: when a snapshot update holds
  so many changed rows that it moves them to disk and a transaction delivered
  twice has its changes on both sides of that move, it is refused, not
  checked; and a transaction delivered twice that holds a key-changing
  UPDATE is refused by the rule of the previous point (the new key receives
  the row twice).
  What is not seen, in every case because the rows are equal in every column,
  the key included: (1) a row inserted as an exact copy of one that already
  existed, followed by a delete of the original, which reads like inserting
  a row and deleting it again; (2) the same row inserted twice and one of the
  two deleted, or any series of changes made identically to two identical
  rows, which reads like a transaction cut short and then delivered whole:
  the source keeps one row and the copy none. Both need a `DEFERRABLE` key;
  the index does not record whether a key is deferrable. (3) On a database
  whose encoding is `SQL_ASCII`, text that is not valid UTF-8 is stored with
  replacement characters, so two rows that differ only in such bytes compare
  equal.
- **An unchanged-TOAST marker** is the place of a large value an UPDATE did
  not touch and PostgreSQL did not send. Under `REPLICA IDENTITY FULL`
  there is none to meet: capture replaces it with the real
  value from the old row of the same change, for an ordinary UPDATE and for
  one that changes the key. A change captured with the old row's key only
  (a table that is not at `FULL`) keeps the marker, and a fold gives the row the
  value of its newest earlier change, or of its row in the table's copy: in
  a snapshot update the table's previous file, in fresh mode the snapshot
  the statement reads (the table delta beside its file first, then the
  file), read by key for all such rows of a table at once and within the
  memory fresh mode may hold. What neither holds is refused by name (in
  fresh mode the table then reads as of its snapshot until the next one), as
  is a marker in any other place (beside a whole old row, in an INSERT, in
  the new row of a change of key); a value is never written as NULL in its
  stead.
- **A table with no change is carried forward** under
  `--carry-forward-unchanged`, as for MySQL: the new snapshot gets the previous
  file (a hard link where the filesystem allows one) and the table is not read
  or written again. Two cases still rewrite it. The first update after a full
  read rewrites every table once, because a full read's files do not record
  how far back their missing changes reach. And a table that has gone
  unchanged for long is rewritten before the index drops the changes its file
  starts from. Only the scheduled updates of the web console carry tables
  forward: `bintrail baseline refresh` on the command line does not know how
  long the index keeps its changes, and rewrites. A changed table keeps its
  file too, with small files of its changes beside it
  (`<table>.000000.pgdead` and `.pgnew`), unless `BINTRAIL_PG_TABLE_DELTAS=0`
  (`pg-table-deltas = off`) turns that off; it is written in full when its
  changes pass a quarter of its rows, when its file grows too old for the
  index to reach back, or when the delta cannot be written. Everything that
  reads the copy reads them with the file; a views file that follows later
  snapshots (`bintrail views` without `--pin-snapshot`) refuses such a table
  and says to pin it. `--table-deltas` is the MySQL setting.
- **Capture that is behind is safe for the chain, not for one file.** A
  snapshot folded while capture is behind holds the table as of the newest
  change the index has, under the requested point's name, and the run warns.
  The next fold applies what capture delivers later: the folded snapshot's time
  bound is set from the newest change the index holds, not from the point.
- **A transaction the stream has not checkpointed** is left whole to the next
  fold, never applied in part.
- One index database follows one PostgreSQL source: the fold reads where it
  stops from the whole index.

### SQL on the copy from `psql`

`bintrail-console pg-router` opens a PostgreSQL-protocol port, so a PostgreSQL
client runs read-only SQL on the copy of a PostgreSQL server:

```bash
bintrail-console pg-router --listen 127.0.0.1:5433
```

```
psql "host=127.0.0.1 port=5433 dbname=<server id or name> user=app"

SELECT status, count(*) AS n FROM orders GROUP BY status ORDER BY n DESC;
```

It is a process of its own. It runs beside `bintrail-console watch`, reads the
servers the web interface manages and never writes them, and shares nothing
at run time with the MySQL port (`bintrail-console router`): its own
listener, its own workers, its own memory.

- **The database name picks the server**: its id or its name, as the web
  interface shows them. The username is free, except that a server's
  application login, used as the username, opens that server and no other. With
  an application login the database name does not pick the server, but it is
  checked: a name that is **another server** (its id or its name) is refused
  with `28000` and an error that names the login's own server, for example
  `user "app" is the application login of server "prod" and opens that server
  only, and database "staging" is another server; connect with
  dbname=prod, or use the other server's login`. Before, such a connection was
  made to the login's own server and nothing said so. The login's own server
  (by id or name) connects, and so does a name that is no server at all (the
  database of your application behind a proxy, `postgres`, no database): it
  opens the login's own server, as before. Names are matched as written, so
  `STAGING` is not the server `staging`.
- **The password** is the one created for SQL clients in the web interface
  (`dbtrail port on`), the access token, or the application login's own. The
  port is open while the port for SQL clients is turned on in the web
  interface, and closes with every connection when it is turned off or its
  password is replaced. Switching Engine Routing, which is the MySQL port's,
  does not disconnect anyone here.
- **PostgreSQL servers only.** A MySQL or MariaDB server is refused with an
  error that names it; its copy is read through the MySQL port.
- **Who answers depends on what the server forwards with.** `server add` asks
  once whether the port may forward as the user DBTrail connects with;
  `--forward-user`, or `[dbtrail-forward]` in the option file, names a
  forwarding user of its own instead. The web interface's form has a box for
  the first, unticked until somebody ticks it, and the user's two fields
  behind **Advanced: use a separate forwarding user**. With the capture account, a `SELECT` whose every table
  is a table of the copy, and that the copy is not known to answer as
  PostgreSQL does, runs on your PostgreSQL, read-only
  ([forwarding with the capture account](#forwarding-with-the-capture-account)).
  With a forwarding user of its own, every statement the copy is not known to
  answer as PostgreSQL does, and a client's transaction block, run on your
  PostgreSQL as that user, read-only
  ([the forwarding user](#transactions-on-the-source-the-forwarding-user)).
  With neither (the answer no, `--no-forward`, or a script that said nothing)
  the copy answers every statement, nothing is sent to your PostgreSQL, and a
  statement the copy cannot answer gets an error, as does one it would answer
  with other rows than PostgreSQL (a `char(n)` column
  [compared, measured or concatenated](#which-side-answers-a-statement-on-the-postgresql-port),
  refused with `0A000`), until one is set
  (`bintrail-console server forward-user <server> --capture-account`, or
  `-u <user> -p` for a user of its own, or the box and the Forwarding user on
  the server's form in the web interface).
- **Fresh or fast, per server, as on the MySQL port.** In fresh mode, the
  default, each table's newest changes captured from PostgreSQL are laid over
  its snapshot, so a row written a moment ago shows without waiting for the
  next snapshot update: answers are about as far behind as capture. In fast
  mode an answer is as old as the copy's last snapshot update. Choose a
  server's mode on its Snapshots page or with
  `bintrail-console port mode <server> fresh|fast`; `BINTRAIL_SQL_TAILS=on`
  or `=off` where the PostgreSQL router starts (`SQL_FRESH` in the compose
  `.env`) decides for every server instead. `port mode` says what is saved
  for a PostgreSQL server; it cannot see what the PostgreSQL router applies.
  The recent changes are held in this process's memory, on top of each
  statement's own: up to a fifth of what it may use for each server in fresh
  mode (`BINTRAIL_SQL_TAIL_MAX_BYTES` sets another bound per server), so with
  several servers in fresh mode give the process the memory for all of them;
  `--workers` is not lowered for them.
  They are also kept as small files in the temporary directory, which the
  MySQL router on the same machine can share: neither removes the other's.
  What it gives and what it costs, as measured on 2026-11-17 (a table of 2
  and of 10 million rows of about 100 bytes, 400 transactions a second at the
  source, a snapshot update every 5 minutes, 8 statements at once):
  - **How far behind an answer is:** about 1.2 seconds at the median and
    never more than 3.2, at both sizes, with 10, 50 and 200 clients, and
    while a snapshot update ran.
  - **What a statement costs:** 2 to 3 times what it costs in fast mode. The
    size of the table counts for more than the mode: a statement on the
    10-million-row table took 0.4 to 0.6 seconds in fresh mode (0.13 to 0.35
    in fast), and 0.12 to 0.17 on the 2-million-row one (0.03 to 0.09).
  - **A statement that reads one row by its key reads the whole table too**:
    the copy has no index. With a forwarding user such a statement is
    answered by the source, whose plan for it is cheap (see
    [Which side answers a statement](#which-side-answers-a-statement-on-the-postgresql-port)).
  - **How many statements it answers** is set by `--workers`, which is 4
    out of the box, with 256 clients connected at most
    (`--max-connections`); in the compose `.env`, `PG_ROUTER_WORKERS` and
    `PG_ROUTER_MAX_CONNECTIONS`. The measurement above ran 8 at once.
  - **Switching a server from fresh to fast** makes the connections already
    open read the snapshot from their next statement on: an answer older
    than the one before it on the same connection, which is told once by a
    NOTICE.
  What fresh mode does not cover yet, where the answer comes from the
  snapshot alone. The client is told only by a NOTICE that comes with the
  answer (drivers that drop notices, or `client_min_messages` above
  `notice`, hide it), and the statement is not sent to the source for it:
  - **Right after a snapshot update**, each table's recent changes are
    gathered again from the new snapshot on, which takes a few seconds; until
    then statements on that table are answered from the snapshot.
  - **The first statement on a table** after the router starts, or after the
    server turns to fresh mode, starts gathering that table's changes: it,
    and any statement in the next moments, may still answer from the
    snapshot.
  - **A table with no write since capture started** has no recent changes to
    lay over it, which is the right answer for that table alone. But a
    statement answers fresh only when every table it reads does, so a join
    with such a table is answered from the snapshot for all of its tables.
  - **Waiting for the source** (`BINTRAIL_SQL_FRESH_WAIT`) does not apply to
    PostgreSQL yet: a statement does not wait for the newest changes to
    arrive before it reads.
- **During an update of the recent changes, a statement does not wait, and
  may answer from the version before that update, with no notice.** That is
  usually seconds behind, and a minute or more in a long update. On
  PostgreSQL there is no switch to make it wait. What does protect is partial:
  a connection the source already answered about a table waits for the copy
  to reach that answer, or goes to the source, so it does not read back in
  time; that does not cover an application that writes on one connection and
  reads on another. A table with no version yet (the first statement on it,
  the first after a new snapshot) does wait for the running update, for at
  most half of the statement's time limit (30 of 60 seconds by default); past
  that it goes on as described above. While the index holds no captured
  change at all, the first change can be missing from statements that run
  during the update that brings it.
- **If capture falls behind, the copy falls behind with it, as a replica
  does.** The port keeps answering from the copy and does not tell the
  client. A server's lag limit, `max_behind_source_seconds`, is off by
  default, as on the MySQL port. Set it with `PUT /api/servers/{id}/sql-mode`
  and `{"max_behind_source_seconds": 300}`, and with a forwarding user a
  statement on a copy further behind than that goes to the source
  (`copy_behind_source`, or `copy_age_unknown` when the lag cannot be told).
  Capture's state is read at most every 30 seconds, so a limit is not kept
  to the second.

  A statement that reads a table from its snapshot alone (every table read
  in fast mode; in fresh mode a table with no recent changes, or one whose
  changes are not ready) is measured by what the snapshot's files record:
  each file holds every change committed before an instant written in it,
  and the statement is as far behind as the oldest such instant of the
  tables it reads, by the router's clock. That number errs old, with the
  limits named at the end of this list:
  - It sits a minute before the moment the file was written at, so a
    snapshot just taken reads about a minute behind. A limit under that
    sends every such statement to the source.
  - A table with no change is published in the next snapshot as the file it
    was, with the instant of the update that last wrote it. A statement
    that reads it is as old as that update, however new the snapshot is,
    until the table is written again.
  - A source with no write since the snapshot makes the lag grow though
    nothing is missing, and so does a capture that stopped: capture's state
    is not asked for this number.
  - A full read's instant is the source's clock; an update's is the older of
    the commit time of the newest captured change and the clock of the
    machine that ran the update. So that a source clock ahead of the
    router's does not make a file look new, the instant is never later than
    when the file was last written, by the router's machine.
  - Three cases can still read younger than the data is. With a source clock
    ahead of the router's, a file that took minutes to write can read
    younger by up to the lesser of the clock's lead and the time the write
    took. A schema removed from a server and added back later leaves the
    changes made in between uncaptured, and nothing records that gap (fresh
    mode has the same limit). A publication that does not publish `TRUNCATE`
    keeps the rows a `TRUNCATE` removed, under a recent age.

  A file that does not record the instant has no age and its statements go
  to the source under any limit (`copy_age_unknown`): a full read taken by
  a version before 0.137.0 that no snapshot update has written again since,
  a copy that is not in a local folder. The next snapshot update or full
  read of the table records it. The same goes, for as long as it lasts, for
  a table capture lost or dropped changes of since its rows were read, a
  table outside the schemas the server captures now, a capture that has
  saved no state for more than a minute after the file was read or written,
  a server whose captured schemas are not known to the console, and an index
  that cannot be asked what capture lost. In fast mode, pick a limit above
  the time between snapshot updates plus a minute, or the copy answers only
  right after each update.
- **The copy answers what it answers as PostgreSQL does.** With the server's
  forwarding user, each statement is decided on its own: the copy answers it
  when everything in it is known to give PostgreSQL's answer there and its
  plan on the source is expensive, and the source otherwise, read-only (see
  [Which side answers a statement](#which-side-answers-a-statement-on-the-postgresql-port)).
  A transaction block the client opens, and a catalog statement the copy does
  not answer, go to the source too (see
  [Transactions on the source](#transactions-on-the-source-the-forwarding-user)).
  A server without a forwarding user is answered from the copy alone, and a
  statement the copy cannot answer gets its error. `round()` and `avg()` are
  written PostgreSQL's way on every connection (see
  [Which side answers a statement](#which-side-answers-a-statement-on-the-postgresql-port)):
  `round(avg(amount), 2)` of a `numeric` is `500.00`, as on the source, not
  `500`. A division or an `avg()` of a `numeric` written without a `round()`
  is the one place where the copy's text is not always the source's: the
  copy answers the same quotient, a `numeric`, with 16 decimals, where
  PostgreSQL writes 12 for a quotient of 10000 and more and 20 for one
  below 1 (see
  [Which side answers a statement](#which-side-answers-a-statement-on-the-postgresql-port)).
  Without a forwarding user, a `round()` or an `avg()` the copy would
  write otherwise (`avg()` of a `numeric`, `round()` of a `double precision`,
  a `round()` beside a `UNION`) is refused with error `0A000`, never answered
  with other digits. A statement that is not a
  read is refused with an error. Unqualified names
  resolve in `public` until the client sets `search_path` (see
  [Session settings](#session-settings-and-query-cancel-on-the-postgresql-port)). NULL order, integer division and text comparison follow
  PostgreSQL, and each type reads as [the table below](#how-each-type-reads-in-sql-on-the-copy)
  says; the differences listed there apply here too.
- **Each column carries its PostgreSQL type**, and each value is written the
  way PostgreSQL writes a value of that type (`t`/`f`, `{a,b}`,
  `2026-03-04 05:06:07+00`, `\xdead`, `-0`, `NaN`). A column of the table
  keeps the type, type modifier and size the source announces for it:
  `integer`, `bigint`, `smallint`, `numeric(p,s)`, `real`, `double precision`,
  `boolean`, `date`, `timestamp`, `timestamptz`, `time`, `timetz`, `uuid`,
  `bytea`, `text`, and arrays of one dimension of those. So `pgx` returns an
  `int64` or a `time.Time`, psycopg an `int`, a `Decimal` or a `datetime`, and
  JDBC's `getObject` a `Long`, a `BigDecimal` or a `Timestamp`, as they do from
  the source. When a driver asks for results in binary (`pgx` and JDBC do for
  common types, psycopg on a binary cursor), they are sent in binary, the
  same bytes the source sends. Some columns are announced as `text`, with the
  text PostgreSQL writes:
  - `json`, `jsonb`, `varchar(n)`, `char(n)`, enums and the other types the
    copy keeps as text (the [table below](#how-each-type-reads-in-sql-on-the-copy)
    lists them);
  - `interval`: JDBC reads a column of that type by asking the server's
    catalog about the type, which this port answers only for the types it
    announces, so `getObject` would fail; as text every driver reads it;
  - a value of a type the copy has and PostgreSQL does not (a struct, a map,
    an array of arrays);
  - time travel (`_flashback`, `_snapshot`) and `SHOW`.

  `infinity` and `-infinity` in a `date`, `timestamp` or `timestamptz` column
  read as the source writes them: the words in text, and in binary the ends of
  the count PostgreSQL sends (the largest and smallest 32-bit day for a `date`,
  64-bit microsecond for a timestamp), in arrays too.

  An expression's type is the one the copy computes it with, which is not
  always PostgreSQL's: a `round()` the copy answers is PostgreSQL's
  `numeric`, with exactly the decimals PostgreSQL writes, on every
  connection (`avg()` of a `numeric` or of whole numbers and a division of a
  `numeric`, which PostgreSQL writes with 12 to 20 decimals, are a `numeric`
  of 16 decimals on the copy; without a forwarding user such an `avg()` is
  refused), and an aggregate or a literal of a `numeric` keeps its
  precision and scale here (`numeric(10,2)`) where PostgreSQL reports none. A
  `numeric` declared without precision is a `numeric(38,18)` here and is
  written with its 18 decimals (`12345.678000000000000000` where the source
  writes `12345.678`). A value with no binary form for its type (a date past
  year 9999) is refused with error `0A000` when a client asks for binary, and
  sent as text when it asks for text.
- **A column with no name of its own is headed as PostgreSQL heads it**:
  `count(*)` is `count`, a function call takes the function's name, a cast of
  an expression takes the type's name (`int4`, `numeric`, `timestamptz`), and
  an operator or a constant is `?column?`. A few cases keep the copy's own
  heading, so give the column a name (`AS n`) when the heading matters there:
  a statement whose select list holds a `*` next to an unnamed expression, a
  cast of an expression to `text`, `varchar`, `char`, `real` or `float`, a
  column written with upper-case letters inside a cast, and `VALUES` lists.
- **Prepared statements and parameters work**: the port speaks the simple and
  the extended query protocol, so `psql`, JDBC, `pgx` in its default mode
  and psycopg 3 connect as they are. Each argument is written
  into the statement as a value before the copy runs it, and the statement then
  runs exactly as if it had been typed with that value; whatever the simple
  protocol refuses (a write, `SET ROLE`) is refused here with the same
  error. Three things to know:
  - The port does not work out a parameter's type from the statement, as
    PostgreSQL does: it tells the driver the type is unknown. An argument the
    driver sends without a type is read as the type its place needs
    (`id = $1` compares as a number), but where its place does not decide, as
    in `$1 + 1`, give it one: `$1::int + 1`.
  - When a driver asks for a statement's columns before running it (pgx does,
    JDBC sometimes), the port asks the copy for them without reading a row.
    A statement the copy cannot ask that way (`DESCRIBE`, the copy's own
    `SHOW TABLES`) is described as returning no rows; with `pgx` run those
    with `QueryExecModeSimpleProtocol`. `SHOW` of a setting is answered by
    the port, and described.
    The columns said this way keep the copy's own heading where the statement
    gives one no name (`count(*)` is `count_star()`), so name such a column
    (`AS n`) when the heading matters to a driver that describes first.
  - `BEGIN`, `COMMIT` and `ROLLBACK` move the transaction status the driver
    sees, and after an error inside a block every statement is refused until
    `ROLLBACK`, as on PostgreSQL. They do not make a snapshot: each statement
    reads the copy as it is when it runs. A driver that fetches a few rows at a
    time (JDBC with `setFetchSize` and autocommit off) works; the port still
    computes the whole answer first, under the same row cap.
- **The login is SCRAM-SHA-256.** The client proves it knows the password and
  the password itself never crosses the network, with TLS or without. The port
  never asks for the password in clear text, so a client needs SCRAM support:
  libpq 10 or later (`psql`, psycopg), JDBC 42.2 or later, or any `pgx`. Over
  TLS the port also offers SCRAM-SHA-256-PLUS, which binds the login to the
  certificate the client saw, so a client with `channel_binding=require`
  refuses anything that relays between it and the port.
- **Before the login.** A connection holds one of the port's places
  (`--max-connections`) from the moment it is accepted. It has **30 seconds**
  to finish logging in, and then it is closed. Until it has logged in, one
  message from it may be at most **64 KB** (a larger one ends the
  connection); after the login a statement may be up to 16 MB. When every
  place is taken and a new connection arrives, the port looks for a
  connection that has held its place for **more than 1 second without
  sending its proof of the password**: the one that has waited longest loses
  its place to the newcomer (one that sent nothing at all before one that
  started its login), and is closed without a word. A client that was only
  slow connects again. A connection whose proof reached the port keeps its
  place while the proof is checked and its server is opened, and a
  connection that has logged in is never closed to make room: when every
  place is a client's, the newcomer gets `53300` (the limit is reached), as
  before. The port still belongs behind a firewall or a proxy that limits
  who may connect. Closing the connection that waited longest protects the
  port from connections that sit idle; it does not protect it from a client
  that connects again as fast as it is closed, and it is not a limit per
  address.
- **TLS with the MySQL port's certificate.** Give the same files the MySQL port
  takes (`router --tls-cert`) with `--tls-cert` and `--tls-key`, and a client
  that trusts their CA connects with `sslmode=verify-ca`, or
  `sslmode=verify-full` with the name the certificate holds:

  ```
  psql "host=db.example.com port=5433 dbname=<server> user=app sslmode=verify-full sslrootcert=ca.pem"
  ```

  With no certificate set, a client that asks for TLS still gets it, with a
  certificate made up at start: `sslmode=require` works, and `verify-ca` and
  `verify-full` refuse it. The files are read when the process starts, so a
  renewed certificate is served after a restart; one that expired is served
  all the same, with a warning at start, and a file that cannot be read stops
  the process before it listens, with the reason. Clients that start TLS
  directly (`sslnegotiation=direct`, PostgreSQL 17) are taken too.
- **TLS is offered, never required**, as on the MySQL port: a client that asks
  for no TLS (`sslmode=disable` or `allow`) is answered in clear text. Its password still does not cross the
  network, but its rows do. A client that must not read rows in clear text
  sets `sslmode=require` or stricter; that is also what keeps something in
  the middle from turning TLS off.
- **Any address.** `--listen` takes any host:port, `0.0.0.0:5433` included. An
  address other machines reach is named at start, since whoever reaches it can
  try the port's password; keep it behind a firewall as you would the MySQL
  port.
- **Session settings and query cancel** are taken as PostgreSQL takes them;
  [below](#session-settings-and-query-cancel-on-the-postgresql-port).
- A result with more rows than the port's row cap, or with a cell longer than
  the cap on one cell, is refused, not cut: add a `LIMIT`, or select part of
  the long column.
- **Results being sent are held in memory up to 256 MB in all.** The port
  builds a result whole and keeps it until the client has read it. While it
  holds 256 MB of results for clients that have not read theirs yet, the next
  statement is refused with error `53000` before it runs; run it again once
  the other clients have read their answers. Time travel answers are counted
  too. A client that asks for a result and reads nothing of it for 60 seconds
  is disconnected, and what the port held for it is freed. There is no flag
  for this size; the MySQL port holds the same amount.
- **Time travel on the same connection**: one row as it was at a past moment,
  from the server's change history and snapshots.

  ```sql
  SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42;
  SELECT * FROM _snapshot.orders  AS OF '2026-07-01 09:00:00' WHERE id = 42;
  ```

  A whole table as of a moment is refused here (use `bintrail-pg reconstruct`
  or the web interface). A server that hides rows or columns refuses time
  travel on this port, with the reason: time travel reads the history
  directly, past the views that hide them.

What the process may use is said where it is started, with defaults that fit
beside capture on the recommended machine (8 cores). Each statement may take
the SQL memory, so four at once can take 8GB, plus the recent changes fresh
mode holds (up to a fifth of the process's memory for each server in fresh
mode): on a 16GB machine, set `--workers 2`.

Capture comes first on the cores. The compose service and the packaged
systemd unit run this process at a lower CPU priority than capture
(`cpu_shares: 256`, `PG_ROUTER_CPU_SHARES` in the compose `.env`;
`CPUWeight=25` in the unit), as they run the MySQL router: when every core is
busy, capture and the snapshot update are served first and the statements
here take longer. The process does not read that priority; `--workers` and
`--threads` stay as set. Started by hand it has the priority of whatever
starts it.

| Flag | Environment variable | Default | What it does |
|---|---|---|---|
| `--listen` | `BINTRAIL_PG_ROUTER_LISTEN` | `127.0.0.1:5433` | Where the port listens |
| `--tls-cert` | `BINTRAIL_PG_ROUTER_TLS_CERT` | none: a certificate made up at start | The certificate (PEM, its chain after it) a client that asks for TLS gets. Requires `--tls-key` |
| `--tls-key` | `BINTRAIL_PG_ROUTER_TLS_KEY` | none | Its private key (PEM, not encrypted with a passphrase) |
| `--workers` | `BINTRAIL_PG_ROUTER_WORKERS` | 4 | How many statements run on the copy at once (1 to 16). The rest wait their turn |
| `--threads` | `BINTRAIL_PG_ROUTER_THREADS` | 2 | Threads each statement may use (1 to 16) |
| `--sql-memory` | `BINTRAIL_CONSOLE_SQL_MEMORY` | 2GB, or the value saved in the web interface | Memory each statement may use |
| `--max-connections` | `BINTRAIL_PG_ROUTER_MAX_CONNECTIONS` | 256 | Clients connected at once (1 to 1024). One more is refused with an error |
| `--max-source-connections` | `BINTRAIL_PG_ROUTER_MAX_SOURCE_CONNECTIONS` | 16 | Connections to the servers' own PostgreSQL held at once, every server together (1 to 1024). A client holds one while a statement the source answers, its plan, or its transaction block runs there; past the limit the statement that needed one waits for one to be given back, up to 30 seconds, and is then refused (`53300`) |
| `--max-wait` | `BINTRAIL_PG_ROUTER_MAX_WAIT` | none | How long a statement waits for its turn before it is refused (`53000`, `copy_wait_timeout`; it is not sent to the source) |
| `--sql-port-max-rows` | `BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS` | the port's row cap | Most rows one statement returns |
| `--token` | `BINTRAIL_CONSOLE_TOKEN` | none | The access token, one more password. It does not open the port |
| `--servers-file` | `BINTRAIL_CONSOLE_SERVERS` | `~/.config/bintrail/console-servers.yaml` | The servers file the web interface manages. Read, never written |


### Session settings and query cancel on the PostgreSQL port

What a driver sets for its session, with `SET` or in its connection
parameters, is answered here as PostgreSQL answers it: the same `SET` and
`SHOW`, the same values, and a `ParameterStatus` to the client when a reported
setting changes. Nothing that would change an answer is taken and ignored:

| Setting | On this port |
|---|---|
| `TimeZone` (`SET TIME ZONE`) | **Applied.** The statement runs in the zone (a cast to `date`, `date_trunc`, a literal without an offset), and a `timestamptz` is written in it (`2026-01-01 05:00:00-05`). A name of the time zone database (`America/Bogota`; `america/bogota` is taken too), a name of UTC, or `GMT` with a whole-hour offset as JDBC writes one (`GMT-05:00`). An offset as a number (`SET TIME ZONE -5`) or an interval is refused: name the zone (`Etc/GMT+5` is five hours behind UTC). A zone the copy's engine does not know, or knows with other offsets than this host, is refused. One difference is known: the engine reads `Europe/Amsterdam` before 1937 without its local mean time (+00:19:32), so a cast or `date_trunc` of such an instant there is off by those minutes; the `timestamptz` itself is written right. |
| `search_path` (`SET SCHEMA`) | **Applied**, to one schema of the copy: unqualified names resolve there, `current_schema()` says it, time travel reads it. `"$user"` and `pg_catalog` may be in the path; a second schema, or one the copy does not have, is refused. |
| `statement_timeout` | **Applied.** A statement that runs past it stops with `57014 canceling statement due to statement timeout`. The port's own cap (five minutes) still applies. |
| `application_name`, `client_min_messages` | **Applied.** |
| `extra_float_digits` 1 to 3, `DateStyle` `ISO, MDY`, `client_encoding` `UTF8` (or `SQL_ASCII`), `lock_timeout`, `IntervalStyle` `postgres`, `bytea_output` `hex`, `standard_conforming_strings` `on`, read committed, read only | **Taken; they change nothing this port answers.** PostgreSQL 12 and later write the same digits for an `extra_float_digits` of 1, 2 or 3; the copy takes no lock. |
| `extra_float_digits` 0 or less, another `DateStyle`, `IntervalStyle`, `bytea_output` or client encoding, `standard_conforming_strings` off, `REPEATABLE READ`, `SERIALIZABLE`, `READ WRITE`, `SET ROLE`, `SET SESSION AUTHORIZATION` | **Refused, with the reason** (`0A000`): the port would answer otherwise than the setting says. Each statement reads the copy as it is when it runs, so no snapshot is kept across statements. |
| A name the port does not take | **Refused** (`42704`); `SHOW ALL` lists the ones it takes. |

A setting in the connection parameters (JDBC sends `TimeZone`, `DateStyle`
and `extra_float_digits` there; libpq sends `PGOPTIONS` as `-c name=value`) is
taken the same way, and is what `RESET` goes back to; a value refused there
ends the connection with the reason. In a transaction block a `SET` is undone
by `ROLLBACK` and by an error in the block, and a `SET LOCAL` ends with the
block, as on PostgreSQL. `SELECT version()` answers `PostgreSQL 14.0 ...`,
which is what SQLAlchemy (and Superset) read on connect.

**Query cancel works**: each connection is given a process number and a secret
key (`BackendKeyData`), and a cancel request with both, from `psql`'s Ctrl-C,
JDBC's `setQueryTimeout` or `cancel()`, psycopg's `cancel()` or `pgx`, stops
the statement the copy is running for that connection. It answers
`57014 canceling statement due to user request` and the connection stays
open. The request is taken in clear text and over TLS (`psql` 16 and psycopg
send it in clear text even on a TLS connection; it carries no password), and
also when the port has all the connections it takes.

Time travel reads its `AS OF` moment in UTC, so under another `TimeZone` it
is refused with the reason: `SET TimeZone = 'UTC'` first. Its columns are text,
written as the change history holds them. On `bintrail-pg flashback`, which
answers time travel only, `TimeZone` is UTC only, at connect too: start a JDBC
client with `-Duser.timezone=UTC`.

### Transactions on the source: the forwarding user

A transaction block a client opens on the PostgreSQL port (`BEGIN` or
`START TRANSACTION` to `COMMIT` or `ROLLBACK`) runs on the server's own
PostgreSQL, every statement in it, and so does a statement on the system
catalog that the port does not answer from the copy
([below](#listing-schemas-tables-and-columns-on-the-postgresql-port)), and
every statement outside a block that the copy is not known to answer as
PostgreSQL does
([below](#which-side-answers-a-statement-on-the-postgresql-port)). It needs the
server's **forwarding user**, a role of its own on the source:

```sql
CREATE ROLE app_ro LOGIN PASSWORD <a password>;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO app_ro;
```

`server add` takes it when the server is added: it asks for it on a terminal,
or reads `--forward-user app_ro`, or `[dbtrail-forward]` in the option file.
To set or change it on a server that is already added:

```bash
bintrail-console server forward-user <server> -u app_ro -p
```

or in the web interface, edit the server (**Advanced: use a separate forwarding
user**, then Forwarding user and password). It is
laid over the source's own address, database and parameters (`sslmode`
included), and must be another user than the one DBTrail captures with: the
port never runs anything as that one, which holds `REPLICATION` and reads every
published table, unless the server says yes to that
([below](#forwarding-with-the-capture-account)). Without a forwarding user, every statement, a transaction
block's too, is answered by the copy alone, as before. Changing the forwarding user, or removing the
server, closes the connections that use it; clients reconnect.

- **Read-only, three ways.** A statement that is not a read is refused before
  it is sent (`25006`, naming `INSERT`, `UPDATE`, `CREATE`...). The connection
  to the source is opened with `default_transaction_read_only = on`, so
  PostgreSQL itself refuses what the text does not show (a function that
  writes, a `WITH` that deletes, `SELECT ... INTO`, `nextval`). The port watches
  that setting, which PostgreSQL 14 and later report, and closes the connection
  if anything turns it off. A source older than 14 is not forwarded to.
  `BEGIN READ WRITE` and `SET default_transaction_read_only = off` are refused.
- **The text is read as PostgreSQL reads it, and what cannot be read is not
  sent.** Before a statement goes to the source the port cuts it into words the
  way PostgreSQL's own scanner does: a `--` comment ends at a line feed or a
  carriage return, block comments nest, an `E'...'` string and a dollar quote
  start only where PostgreSQL starts one, and a string written in two pieces
  a line apart is one string. A text it cannot cut with certainty (a quote or
  a comment left open, a character that is not SQL outside a string) is
  refused with `0A000` and nothing of it is sent.
- **`BEGIN`, `COMMIT`, `ROLLBACK` and `SET TRANSACTION` are sent in the port's
  own words**, written from what it read of yours (`BEGIN READ ONLY`, with the
  isolation level you named), never as your text. After its `BEGIN` the port
  asks the source whether the block is read-only, in the same round trip, and
  uses the block only when the answer is yes; otherwise the connection to the
  source is closed and the statement answers `08006`.
- **Refused by name**: `COPY` (`0A000`; read rows with `SELECT`), `LISTEN` (a
  notification would reach the client only when it next sent a statement),
  `NOTIFY` and `pg_notify()`, `DO`, `PREPARE`/`EXECUTE`, advisory locks,
  `set_config()`, `dblink`, and names written with `U&"..."` escapes. A
  transaction statement inside a longer text (`SELECT 1; COMMIT`) is refused:
  send it on its own.
- **Refused although it begins as a read** (`25006`): `SELECT ... INTO`, a
  `SELECT` that locks its rows (`FOR UPDATE`, `FOR SHARE`), a `WITH` that
  holds an `INSERT`, `UPDATE`, `DELETE` or `MERGE`, and `EXPLAIN` of anything
  but a query. PostgreSQL refuses each of them in a read-only transaction
  too; the port says so first, with the reason.
  In a `WITH`, an `EXPLAIN` or a `DECLARE`, a column or alias named `insert`,
  `update`, `delete` or `merge` has to be written in double quotes.
- **Functions that act beyond a read are refused by family** (`0A000`),
  schema-qualified or quoted alike: stopping or cancelling a session
  (`pg_terminate_backend`, `pg_cancel_backend`), the server's files
  (`pg_read_file`, `pg_ls_*`, `pg_stat_file`), large objects (`lo_*`), WAL,
  backups, replication slots and origins, `pg_logical_emit_message`,
  `pg_reload_conf`, the statistics resets (`pg_stat_reset*`), `nextval` and
  `setval`, `txid_current` and `pg_current_xact_id`, and `ts_rewrite`.
  `query_to_xml` and `ts_stat`, which run a text as a statement, are sent only
  when that text is a plain string written in the statement and is itself a
  read. Most of these an ordinary role cannot run anyway; the ones it can
  (`pg_logical_emit_message`, `txid_current`, cancelling its own sessions) are
  not stopped by a read-only transaction, which is why the port refuses them.
- **The source's answers as it gives them**: its rows as they come, never held
  whole in this process; its errors with their code, detail and position, which
  fail the block as on PostgreSQL; its notices (`RAISE NOTICE`). Savepoints
  (`ROLLBACK TO SAVEPOINT` brings a failed block back, and the session's
  settings to what they were at the savepoint, as psql's `ON_ERROR_ROLLBACK`
  and JDBC's `autosave` expect; a savepoint's name is told apart as
  PostgreSQL tells it, so `"A"` in quotes is not `a`),
  cursors and the extended protocol work as on the source: a statement a driver
  prepared outside the block (pgx, JDBC) is prepared on the source the first
  time it runs inside one. When the copy had described its columns with
  another type than the source sends in binary, that execution is refused with
  `cached plan must not change result type` (`0A000`), the words drivers
  prepare again on, never sent as the wrong value.
- **The session goes with it**: `TimeZone`, `search_path`, `statement_timeout`,
  `lock_timeout`, `extra_float_digits`, `client_min_messages` and the default
  isolation are set on the source before a statement is sent, and the formats
  the port writes in (`DateStyle` `ISO, MDY`, `IntervalStyle`, `bytea_output`,
  UTF8) are the source connection's from the start.
- **Cancel and disconnect reach the source.** A cancel request stops the
  statement there (`57014`); a client that leaves in the middle of a
  statement has it cancelled there. A source that does not stop a cancelled
  statement within ten seconds has its connection closed.
- **At most one connection to the source per client**, opened when a
  statement needs it and given back as soon as nothing of the client's lives
  there (the block ended, the batch synced, no portal open), so a connection
  pool's clients do not hold one each for good; at most
  `--max-source-connections` across every server. The next one is given the
  session's settings again, and a prepared statement is prepared there again
  the first time it runs; a cursor `WITH HOLD`, which would outlive the block,
  is refused. A connection that is lost, or whose read-only guard was turned
  off, is not replaced behind the client's back: a statement that needs the
  source answers `08006 ... reconnect` (a block open at that moment fails, and
  only its `ROLLBACK` is answered), while reads on the copy go on, except one
  the copy would answer with data older than what the source already
  answered that connection (see below), which gets the same `08006`.
- **Every wait for the source is bounded** by the port's five-minute cap on a
  statement, besides the client's `statement_timeout` and cancel. The wait
  for a connection there, when the port holds as many as it opens, is
  bounded by 30 seconds, the `statement_timeout` and a cancel request; one
  deadline covers every wait of a statement.
- **When the source cannot be reached**: `08001 cannot connect to the source`;
  when it refuses the forwarding user, `08004` with PostgreSQL's reason (not
  `28P01`, which would read as the client's own password being wrong). The
  `BEGIN` fails and the connection stays open on the copy.
- **TLS to the source** follows the forwarding user's URL: the source's own
  `sslmode` when the user was laid over it, `prefer` when none is written.
- `REPEATABLE READ` and `SERIALIZABLE` are still refused, as on the copy:
  inside a block the port still answers some statements itself (`SHOW`,
  the catalog statements it recognizes, time travel).


### Forwarding with the capture account

A server can forward without a second role: say yes to forwarding with the
role DBTrail captures with.

```bash
bintrail-console server forward-user <server> --capture-account
bintrail-console server forward-user <server> --no-capture-account   # takes it back
```

In the web interface it is the box on the server's form (**Servers**,
**Edit**, or the form a PostgreSQL is added with): **Let the PostgreSQL port
send to this PostgreSQL, read-only and as the source user above, a SELECT the
copy does not answer**. It saves the same answer the two commands do. A
session that may not change servers sees the box and cannot change it, and
the SQL clients page says, for the server picked in the sidebar, what the port
sends to its PostgreSQL.

It is saved on the server and off until somebody says yes: a server added
before this existed, or one nobody answered for, does not forward with it. A
forwarding user of its own, when the server has one, is used instead. An edit
that points the source at another user or address takes the yes back, and the
form says so: the box has to be ticked again for the new one.

Removing the forwarding user is not the same in the two places. In the web
interface, **Remove the forwarding account** says the port then stops
forwarding for the server, so it also takes the yes back, before the user is
removed: nothing is sent to your PostgreSQL afterwards, and the save says so
from what DBTrail answered. To forward as the source user instead, tick the
box once the removal is saved. On a terminal, `server forward-user <server>
--remove` removes the user alone: with a yes saved, the source user takes its
place, and the command says so and names `--no-capture-account`. A server
that hides tables or columns sends nothing to your PostgreSQL whatever is
saved, and both the form and the SQL clients page say that for it.

That role can read more than the copy holds (it can be the database owner, or
the master user on a managed service, and read the system catalog), so with
it the port sends a statement to your PostgreSQL only when all of this holds,
checked before anything of it is sent:

- it is one `SELECT` (or `WITH ... SELECT`) whose tables are all named in its
  `FROM` clauses, calling only built-in functions known to read no table: no
  function of yours, no `query_to_xml()`, no `pg_read_file()`, no `nextval()`,
  no function in `FROM`, no `LATERAL`, `ONLY`, `TABLE` or `INTO`, no locking
  read (`FOR UPDATE`, `FOR SHARE`), no name with its database;
- the session's `search_path` does not name `pg_catalog` or `pg_temp`;
- your PostgreSQL confirms, on the same connection and just before, that each
  name is a plain table (not a view, a partitioned or inherited table, or one
  with row-level security) and that no operator, cast or function of yours
  could run in it;
- every one of those tables is a table of the copy, by schema and name
  exactly, and none is in `pg_catalog`, `information_schema` or another
  `pg_` schema.

Anything else gets `0A000` with the reason, and nothing of it is sent:

```
ERROR:  this server forwards to its source with the account DBTrail captures with, so the port sends there only a SELECT whose every table is a table of the copy, and this statement reads a table the copy does not hold; nothing was sent to the source
```

The connection is still read-only, as with a forwarding user. What that
leaves out on purpose: a `SAVEPOINT`, a cursor and an `EXPLAIN` inside a
transaction block, and a statement sent in the middle of an extended batch
the port has already sent something of (it cannot ask your PostgreSQL first
there). For those, set a forwarding user of its own. A statement the copy
answers is answered by the copy as before; when the port would ask your
PostgreSQL for its plan first, that `EXPLAIN` is held to the same check, and
the copy answers what does not pass (`capture_gate` in the reasons below).
`SHOW dbtrail.router_status` says `forwarding_account | capture account,
read-only, tables of the copy only`, and the router's `/status` has the same
in `forwarding_account` for the server.

The port's connections then log in as the same role as capture. Each one is a
session of that role, so it counts toward the role's `CONNECTION LIMIT` and
toward your PostgreSQL's `max_connections`. The PostgreSQL router holds at
most `--max-source-connections` of them at once (16 unless set), across every
server. A role with a `CONNECTION LIMIT` lower than that plus what capture
itself holds can leave capture unable to log in again after a restart or a
dropped connection. Leave the role without a limit (the default), or give it
one with room for the router's maximum and capture's own connections. A
forwarding user of its own keeps the two apart.

### Which side answers a statement on the PostgreSQL port

With a forwarding user, a statement outside a transaction block is answered by
the copy only when everything in it is something the copy is known to answer
the way PostgreSQL does. What "known" means is measured, not assumed: the
comparison harness (`internal/pgcompare`) runs the same statements on a real
PostgreSQL and on its copy, and only what it found answering the same is let
through. When in doubt, the source answers: the cost is a slower answer, never
a different one.

The source answers:

- **A function, operator, cast or construct the copy answers differently or
  not at all.** The copy takes `count`, `sum`, `min`, `max`, `coalesce`,
  `nullif`, `greatest`, `least`, `length`, `substring` (with its start, and
  its length when given, written as whole numbers above zero:
  `substring(name, 1, 3)`, `substring(name FROM 2 FOR 3)`) and `substr` under
  the same rule (`substr(name, 1, 3)`), also of a `char(n)` column, whose
  padding the copy keeps: where the cut reaches it the copy answers `b  ` and
  PostgreSQL `b`, and a comparison of that cut (`substr(code, 1, 3) = 'b'`)
  can miss the row on the copy; a cut inside the part with data is the same
  on both. `trim`, `position`,
  `concat`, `abs`, `mod(x, n)` of `integer`, `bigint` or `numeric` columns and
  numbers by a whole number written in the statement (not 0 or -1), `ascii`
  of text whose first character is below 128, `bool_and`, `string_agg`, `row_number`, `rank`, `lag`, and
  `extract`, `date_part` and `date_trunc` of a year, month or day
  (`date_trunc` of a `timestamp` or a `timestamp with time zone` column,
  written with or without its table: a series by day over a `timestamptz`
  stays on the copy, in a session in UTC, the only one the copy answers in);
  `round(a / b, k)` and `round(avg(x), k)` (also `round(100 * avg(x), k)`, or
  any other power of ten) where `k` is written as a whole number from 0 to 15 (counting the
  power of ten of `avg`)
  and `a`, `b` and `x` are numerics or whole numbers, rounded as PostgreSQL
  rounds a `numeric`: half away from zero, to exactly `k` decimals (`50.00`);
  `round(x, k)` of any other `numeric` or whole number, with `k` from -38 to
  15 (`round(12.50, 3)` is `12.500`, `round(5, 2)` is `5.00`,
  `round(-5.0, -1)` is `-10`), and `round(x)` and `round(a / b)` of a
  `numeric`; a `CASE` or `COALESCE` around such a `round`, checked value by
  value (PostgreSQL writes `CASE WHEN c THEN round(x, 2) ELSE 0 END` as `0`
  where the copy would write `0.00`, so a row that takes the `0` sends the
  statement to the source);
  a division `a / b` and `avg(x)` written without a `round()`, of numerics
  and whole numbers: the exact quotient rounded to 16 decimals, a `numeric`
  as PostgreSQL's is. PostgreSQL writes its own with 16 significant digits or
  more, so the two are the same number with other decimals. The copy always
  writes 16; PostgreSQL's count depends on the value and on both sides: often
  16 for a quotient from 1 to 9999 (`7.5000000000000000`, the same text), 12
  for a larger one, 20 for a smaller one (`0.3333333333333333` for
  `0.33333333333333333333`), and 20 for `2.00 / 2`. A quotient that ends
  within 16 decimals is exact (`0.3 / 3` is `0.1000000000000000`). A quotient
  under 0.000001 that is not zero is answered by the source, because 16
  decimals would keep too few of its digits. A quotient that does not end is
  cut at the sixteenth decimal here and later on PostgreSQL, and what is
  worked out from it carries that: a comparison, a sort, a `sum` or a
  `GROUP BY` that turns on a digit past the sixteenth can come out otherwise
  on the copy. `amount / 3 = 33333.333333333333` can match a row on one side
  and not on the other, `a / b * b = a` can be true on one and false on the
  other, and a `GROUP BY a / b` of two quotients that differ only past the
  sixteenth decimal can be one group on one side and two on the other. Write `round(a / b, k)` or
  `round(avg(x), k)` for PostgreSQL's text exactly. The copy answers when
  the statement shows each side to be a `numeric` or a whole number:
  numbers, columns of whole numbers or of a `numeric` of at most 20 digits
  and 8 decimals, `+`, `-`, `*`, parentheses, `count`,
  `sum`, `min`, `max`, `abs`, `coalesce`, `nullif`, `greatest`, `least` and
  `length` over them. A divisor of zero is PostgreSQL's error, from the
  source;
  `floor(a / b)` of numerics, as PostgreSQL takes it (`95`, not `95.0`);
  `ntile(n)`; `extract(epoch from x)` of a timestamp (a `numeric` with six
  decimals, as on PostgreSQL 14 and later) and `to_timestamp(n)` of a whole
  number of seconds, so `to_timestamp(floor(extract(epoch from x) / 600) *
  600)` buckets as PostgreSQL does;
  the
  operators `=`, `<>`, `<`, `>`, `<=`, `>=`, `+`, `-`, `*`, `||`, `LIKE`
  (a pattern without a backslash), `IN`, `BETWEEN`, `IS [NOT] DISTINCT FROM`,
  `ANY`, `EXISTS`, an array subscript by a positive whole number; `::date` and `CAST(... AS date)`; `INTERVAL` literals, and `DATE`,
  `TIMESTAMP` and `TIME` literals written in one form: a date as `YYYY-MM-DD`,
  a time as `HH:MM` or `HH:MM:SS` with at most six decimals and an hour below
  24, a timestamp as the date alone or the date, a blank or a `T`, and the
  time (`DATE '2026-01-05'`, `TIMESTAMP '2026-01-05 10:30:00.123456'`). A
  string written in the statement and compared with a `date`, `timestamp`
  or `time` column (`=`, `<`, `BETWEEN`, `IN`) is held to the same form; a
  value that arrives as a parameter of a prepared statement is not read by
  this rule; joins, `GROUP BY`, `HAVING`, `DISTINCT [ON]`, `UNION`,
  `INTERSECT`, `EXCEPT`, `WITH`, window functions, `FILTER`, `LIMIT` and
  `OFFSET`. Anything else goes to the source. Some of what the harness found
  answering otherwise, and why each one goes there: a division of two whole
  numbers other than a column over a number (`sum(qty) / count(*)`:
  PostgreSQL divides them as whole numbers, and the copy's division answers
  a `numeric`), a division or an `avg()` with a side the statement does not
  show to be a `numeric` or a whole number (a `double precision` or a
  `real`, a `numeric` without precision or past 20 digits or 8 decimals, a name from a subquery or a `WITH`, `sum()` of a `bigint`, which
  PostgreSQL sums to a `numeric`, a function not in the list above, a
  `CASE`, a cast, a parameter), a division more than two deep in others
  (`a / b / c / d`), a division in `LIMIT` or `OFFSET` or inside `ceil` or
  `trunc` (the quotient could fall on the other side of the cut),
  `avg(DISTINCT x)` and an `avg` with `FILTER` or `OVER`, a `round`
  of a division of two whole numbers (PostgreSQL divides them as whole
  numbers), a `round` whose quotient lands so close to a half that
  PostgreSQL's own rounding of the division could reach it (the copy refuses
  it, and it goes to the source), `round()` of a
  `double precision` (it rounds a half to even) and of a whole number (a
  `double precision` on PostgreSQL), a `round` beside other values in one
  column (`UNION`, `VALUES`, `ARRAY`, `greatest`, `least`, `nullif`: PostgreSQL keeps each
  value's own decimals), a division by a zero written in the statement
  (PostgreSQL fails it), `~` and `SIMILAR TO`
  (the copy matched no row), `->` and `->>` (PostgreSQL answers `json`, and
  writes a nested value with spaces), a number with an exponent (a `numeric`
  on PostgreSQL), `::text` of a
  float, `substring` with a start of zero or below, or one that is not a
  number written in the statement (`substring('apple', -1, 3)` is `a` on
  PostgreSQL; the copy counts a negative start from the end and answers `e`;
  `left`, `right` and `overlay` always go to the source), a date or
  time literal in any other form (`'26-01-05'::date` is 2026-01-05 on
  PostgreSQL and the year 26 on the copy, a seventh decimal of a second is
  rounded there and cut here, `TIME '24:00:00'` is a time there and midnight
  here), arithmetic (`+`, `-`, `*`, `/`) and `sum()` over a `real` column
  (PostgreSQL computes it as a `double precision`, so `r + 1` with `r = -0.1`
  is `0.8999999985098839` there and `0.9` on the copy, and answers `sum(r)`
  as a `real`; a `real` that is only read, compared, counted or taken the
  `min` or `max` of stays on the copy, and so does arithmetic over a
  `double precision`, a whole number or a `numeric`), a `DATE`, `TIMESTAMP` or
  `TIME` literal written with a time zone
  (`TIMESTAMP '2026-03-02 10:00:00+02'`: the type holds none and PostgreSQL
  does not read it, so the literal is 10:00 there) and a string with one in
  either bound of a `BETWEEN` on a `timestamp` column, `upper`, `lower` and `ILIKE` (PostgreSQL folds case by the database's
  character type), `COLLATE`, `AT TIME ZONE`, `FOR UPDATE`, `jsonb` operators,
  `generate_series` and every function the list does not name.
- **The clock and the session**: `now()`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`,
  `LOCALTIMESTAMP`, `current_user` and the like. The copy's data is as old as
  its last update, and its clock and session are not the source's.
- **Text sorted or compared by order** (an `ORDER BY` of text anywhere but
  the main statement, `min()` of one, `name > 'B'`): PostgreSQL orders text by
  the database's collation, the copy by bytes. **The `ORDER BY` of the main
  statement is the exception, and the copy answers it**, by a text column,
  an expression, a position or an alias, with or without `LIMIT`: the same
  rows, sorted by bytes (`B, a, c` where PostgreSQL writes `a, B, c`), and
  with a `LIMIT` or an `OFFSET` the rows kept can be other rows. To have
  PostgreSQL's order, write `COLLATE "default"` on the key
  (`ORDER BY name COLLATE "default"`): the statement goes to the source. A
  column the copy holds as text and PostgreSQL does not (an enum, a domain,
  `money`, `inet`, `json`, a `numeric` wider than 38 digits) sorts by its
  meaning on PostgreSQL, so when a statement reads one, only a key that is a
  real text column written by its name stays on the copy. A
  `char(n)` column sorted, a window, `string_agg(x ORDER BY y)`, `DISTINCT ON`
  and an `ORDER BY` inside a subquery or a `WITH` stay with the source, where
  the order decides a value or which rows are counted. On a server without a
  forwarding user nothing changes. Text compared for equality, grouped or matched with `LIKE`
  stays on the copy. A sort key that is one call of `count`, `sum`, `rank`
  and the like is a number whatever text the statement reads elsewhere
  (`ORDER BY sum(amount) DESC` beside a selected name stays on the copy), and
  the field of a `date_trunc` or a `date_part` (`'day'`) is not text. The port finds the text by what the statement names,
  and reads three ways of sorting text that name none. A position behind a
  `*`: in `SELECT * FROM t ORDER BY 2`, one table and nothing else, the
  port looks the position up in the table's columns, so it goes to the
  source when the second column is text and stays on the copy when it is a
  number or a date; in any other statement with a `*` in a select list (a
  join, a subquery, a `WITH`, a `UNION`, parentheses around the `SELECT`),
  an `ORDER BY` by position goes to the source when the rows under the `*`
  may hold text, whichever column the position is, and stays when they hold
  none. A name the statement gives a value that is also a text column's
  name (`SELECT name AS name ... ORDER BY name`, `min(name) AS name`,
  quoted or not; a name given to a number, `sum(qty) AS qty`, stays). And a
  text column named like a word of `ORDER BY` (`ORDER BY last`, `ORDER BY
  first DESC`). A name the statement gives a value itself (`SUM(x) AS
  units`, a column of a `WITH` or a subquery, read as `s.units`) is judged by
  what it stands for: a count, a sum, an average, a rank, arithmetic, a
  `CASE` that answers only numbers, or a number column renamed is not text;
  anything else may be, and goes to the source when it is sorted or compared
  by order.
- **A `char(n)` column used where its trailing blanks count.** PostgreSQL
  pads a `char(n)` value with blanks (`'ab'` in a `char(5)` is `'ab   '`),
  ignores them when it compares, and drops them when the value becomes text
  (`length`, `||`, a comparison with a `text` column, a `UNION` with one).
  The copy holds the value as text with its blanks, and counts them: `WHERE
  code = 'ab'` finds three rows on PostgreSQL and none on the copy,
  `length(code)` is 2 there and 5 here. The port reads each column's type on
  the source from the snapshot, and the source answers every statement that
  names a `char(n)` column anywhere but in one place: as a whole item of the
  select list of a statement that is one `SELECT` and nothing else, no
  subquery, `WITH`, `UNION`, `INTERSECT` or `EXCEPT` (`SELECT id, code FROM t
  WHERE id = 7`, with or without the table's name before it and an alias
  after it), and as a whole key of that statement's `GROUP BY` (`SELECT
  status, count(*) FROM t GROUP BY status`, the key selected or not): every
  value of a `char(n)` has the same length, so both sides make the same
  groups. A `bpchar` declared without a length keeps each value's own blanks
  and is not grouped by the copy. There both sides write the same padded
  value, and nothing reads it again under another name: the alias written a second time, a `NATURAL
  JOIN`, the table's name used as a value (`count(DISTINCT t)`), `TABLE t`
  inside a larger statement and a list that renames the columns (`t AS
  a(i, x)`) go to the source as well. So a comparison (`=`, `<>`, `<`, `BETWEEN`,
  `IN`, `LIKE`, `IS DISTINCT FROM`, `IS NULL`), a join's condition or
  `USING`, a `GROUP BY` of an expression over it or of an array of `char(n)`, `ORDER BY` (a `char(n)` is
  text, sorted by the source's collation), a function, `||`, a cast, a `CASE` or an
  aggregate of such a column goes to the source. A `*` stays on the copy as
  a whole item of that one select list (`SELECT * FROM t WHERE id = 7`), and
  inside a `WITH` or a subquery in `FROM` when nothing goes on to read the
  `char(n)` column (`SELECT count(*) FROM (SELECT * FROM t) s`, `WITH x AS
  (SELECT * FROM t) SELECT id FROM x`). It goes to the source when the column
  is then named, when another `*` hands it to the client, when the row is
  used whole, when the block with the `*` has `DISTINCT`, `GROUP BY`, `ORDER
  BY` or a window, in a set operation, in a subquery used as a value, and in
  any shape the port does not follow. It also goes there when the statement
  casts to `char(n)`, takes arguments, or reads a table with a column of a
  domain, an enum or another type that is not built in. A `char(n)` column named in the last
  `SELECT` of a `WITH` goes to the source even when returned as it is: the
  bytes match, but the port would announce it as `text`, not `bpchar(n)`.
  Two more places where a table with a `char(n)` column stands without its
  padding counting, and the statement stays on the copy: as a later table of
  a `FROM` list written with commas (`FROM orders, order_line WHERE
  ol_o_id = o_id`), which joins by what its `WHERE` compares, as `JOIN ...
  ON` does; and under the `*` of `EXISTS (SELECT * FROM t WHERE ...)` or
  `NOT EXISTS`, whose select list PostgreSQL never reads. In both, the
  `char(n)` column itself named in the `WHERE` still sends the statement to
  the source. A list that mixes `JOIN ... ON` with commas is not followed
  and goes to the source, and so does a `*` in any other subquery (`IN`,
  `= ANY`, a subquery used as a value or compared as a row) and an `EXISTS`
  whose subquery has a `UNION`, `INTERSECT` or `EXCEPT`.
  The rule is by where the column stands,
  not by what it would give: `WHERE code IS NULL` answers the same on both
  sides and goes to the source all the same. Only the tables the statement
  reads count, so a table without a `char(n)` column is answered as before,
  a column of the same name included; when the port cannot tell which tables
  a statement reads (a function in `FROM`, `LATERAL`, a `WITH` that is not
  at the start of the statement, a table the copy does not hold), every
  table counts. Two kinds of
  column are treated more strictly, because two values PostgreSQL finds
  equal can be two values on the copy: a `bpchar` without a length, which
  keeps the blanks each value was written with, and every text column of a
  table whose snapshot was taken before DBTrail recorded column types (the
  next snapshot records them). Beside one of those, `DISTINCT` and `GROUP
  BY` go to the source too. An array of `char(n)` is treated as a `char(n)`.
  The snapshot records a **domain's** name, not what it is made of, so the
  port cannot tell a domain over `char(n)` from any other type.
- **A column the copy does not compare as PostgreSQL does.** That is a type
  that is not built in (a domain, an enum, a composite, a range of your own,
  an extension's type), a built-in type the copy holds as text (`inet`,
  `cidr`, `macaddr`, `json`, `jsonb`, `xml`, `money`, a `numeric` wider than
  38 digits, `tsvector`, `tsquery`, the geometric types, the ranges, `pg_lsn`,
  `bit`, `"char"`), `bytea` and `oid`. PostgreSQL compares them by value; the
  copy compares what is written, so `WHERE dc = 'a'` on a domain over
  `char(4)` found no rows where PostgreSQL finds nine. **A statement that
  compares, groups, joins, sorts, aggregates, applies `DISTINCT` to or passes
  to a function such a column is not answered by the copy**: it goes to the
  source, or is refused on a server without a forwarding user. The cost is
  plain: those reports run on the source, at the source's speed, even when the
  copy would have answered them right. **Only showing the column stays on the
  copy**, as a whole item of a plain select list or through its `*`
  (`SELECT id, doc FROM t WHERE id = 7`): both sides write the same value.
  The port still announces such a column as `text`. A statement that does not
  read the column stays on the copy (`SELECT id, total FROM t`, `SELECT
  count(*) FROM t`, a `*` inside a `WITH` whose outer query names other
  columns). An argument declared `bpchar` is the source's too: it compares
  without its trailing blanks.
  For a `char(n)` column, the type is
  the one the last snapshot recorded: a column changed from `text` to
  `char(n)` on the source is treated as `char(n)` from the next snapshot on.
  When the copy's list of columns cannot be read at all (a copy with more
  columns than one statement returns on this port), no statement that reads
  a table is answered by the copy, and the log says so once a minute.
- **A session `TimeZone` other than UTC**, which the harness has not measured.
- **What the copy cannot run, before the client is told anything**: a table
  the copy does not hold, a statement the copy refuses (a reserved word of its
  own dialect, a value it does not read, such as `NaN` in a numeric or
  `24:00:00`), a copy that is behind the server's lag limit. A
  result over the port's row or size cap is still refused, as before: the
  source would send it whole, past the caps. A column of a
  `numeric` declared without precision, which the copy writes with 18
  decimals, is sent to the source too.
- **Not a copy that is busy.** A statement headed for the copy that finds
  every worker of the copy taking a statement, and the line for one full or
  its wait over (`--max-wait`), is refused with `53000` and a message that
  says so (`the copy is busy (copy_wait_timeout): ...`). It is **not** sent
  to the source: the port is there to keep heavy reads off your PostgreSQL,
  and a busy copy is when there are many of them. Run the statement again
  in a moment; if it happens often, give the copy more workers
  (`--workers`, `PG_ROUTER_WORKERS` in the compose `.env`) or a longer wait.
  The connection stays usable, a transaction block is not touched (its
  statements are the source's), and a prepared statement is tried on the
  copy again at its next execution. What the source answers anyway still
  goes there: a statement its plan finds cheap, one the screen sends there.
  The same holds when the copy is too busy to list its columns, which the
  screen reads every 10 seconds per connection: the source's plan decides,
  and what it would send to the copy is refused. With no `--max-wait` (the
  default) a statement waits for its turn as long as its client stays
  connected, and the line holds 4096, more than `--max-connections` lets
  connect: so the refusal is in practice what `--max-wait` sets. The MySQL
  router does the same with error 1040.
- **What is not a query**: `EXPLAIN`, cursors and savepoints outside a block,
  `VALUES`, `TABLE`. A statement that writes is refused (`25006`). `SHOW
  TABLES` and `DESCRIBE`, the copy's own, stay on the copy.

**Of what the copy can answer, only what is expensive on the source goes to
the copy.** The copy has no index: a read by a primary key takes a
millisecond on PostgreSQL, and on the copy it reads the whole table, behind
every other statement waiting for a worker (measured: 15 seconds for one row
of a table of 2 million). So before a statement the copy can answer goes
there, the port asks the source for its plan, with `EXPLAIN (FORMAT JSON)` on
the forwarding user's connection (the statement itself does not run), and
sends it to the copy only when:

- the **Total Cost** at the top of the plan is **50,000 or more**
  (`--cost-threshold`, `BINTRAIL_PG_ROUTER_COST_THRESHOLD`), or
- the plan has a **sequential scan** (a `Seq Scan` node, at any depth: in a
  join, a subquery, a `WITH`) estimated at **1,000,000 rows or more**
  (`--scan-rows`, `BINTRAIL_PG_ROUTER_SCAN_ROWS`), whatever its cost. A
  scan run by parallel workers counts the rows of all of them.

Anything else is answered by the source (`cheap_plan` in `/status`). 0 turns
one rule off; both cannot be 0.

An expensive statement the copy could not return is answered by the source
too (`result_over_row_cap` in `/status`). The copy returns at most
`--sql-port-max-rows` rows for one statement (10,000 by default) and refuses a
larger result, it does not cut it. So when the plan says the statement
returns more rows than that, the port does not run it on the copy only to
refuse it: your PostgreSQL answers it, and sends every row. The plan's
estimate is used for one kind of statement, the one where it can be trusted:
one table read alone, whole or by an index, with nothing left to check row
by row (no `Filter` in the plan), under a `LIMIT` or a sort at most
(`SELECT * FROM orders`, `SELECT * FROM orders WHERE id > 5000 ORDER BY id`).
A `LIMIT` at or under the cap, written as a number, keeps the statement on
the copy. A prepared statement with parameters (`LIMIT $1`, `WHERE id > $1`)
is never sent to the source for its size: its plan is asked for once,
without its values, PostgreSQL then estimates by fixed guesses (a `LIMIT $1`
is taken to return a tenth of the rows), and the port does not take a guess
for the size of a result. It is tried on the copy, as before. For anything
else (a filter no index serves, a join, an aggregate, `GROUP BY`, `DISTINCT`,
a window, a `UNION`, a partitioned table) the estimate is a guess, so the
statement is tried on the copy as before, and one that does return more rows
than the cap still gets the copy's error (`54000`, "the result has more than
N rows"). A statement that the source answers this way runs there in full:
it is the read of a whole table or a wide index range that the client asked
for, and it loads the source as it would without the port. In the compose `.env` they are
`PG_ROUTER_COST_THRESHOLD` and `PG_ROUTER_SCAN_ROWS`. The defaults come from a
TPC-C source of 97 million rows on PostgreSQL 16: reads by a primary key cost
1 to 12, an order's lines by an index 12, the stock level join 712, a
`count(*)` of one warehouse's orders by an index 15,059, and `max(ts)` of a
heartbeat table 4,202 with a sequential scan of 201,680 rows; of 14
analytics reports, two cost 177 and 699 on PostgreSQL (a table of 2,000 rows,
an index range) and stay there, and the other twelve cost 98,367 to
13,351,258, several with sequential scans of 2.4 to 61 million rows.

What this costs and where it can go wrong:

- **One round trip to the source** for each statement the copy could answer,
  and a connection there for that moment (`--max-source-connections`), given
  back as soon as the copy is chosen. A client connection holds one there
  only while something of its own lives on it, so most statements open it,
  a login of the forwarding user, as a statement the source answers did
  already. With every one of them taken, the statement waits for one to be
  given back, as a statement waits in line for the copy: up to 30 seconds,
  or its `statement_timeout` when that is shorter, and it gets `53300` only
  then. A slot is given back as soon as a statement outside a block has its
  answer, so with 50 clients over the default 16 the wait is a few
  milliseconds; only blocks held open longer make it run out. A cancel
  request ends the wait, and its `statement_timeout` counts everything the
  statement did, the wait included. When the plan cannot be asked because
  the source cannot be reached (down, refusing the forwarding user, its
  connection lost, or no connection free in that wait), the copy answers
  the statement, as before routing by plan, with a NOTICE that routing by
  plan is off for the moment (`source_unreachable` in `/status`).
- **A statement shape the source answers is asked about once a minute, not
  every time**, when its answer cannot depend on its values: a connection
  remembers the verdict on `SELECT ... WHERE id = 5` for a minute, by its shape (values blanked)
  and the session's `search_path`, and sends `WHERE id = 6` the same way
  without asking. Only where every value is compared with `=` (a `>`, a
  `BETWEEN`, an `IN` list or a `LIMIT` can change the plan with the value,
  and are asked every time), and only for a plan ten times below both
  thresholds (a plan near a threshold can cross it for another value). A
  verdict for the copy is never kept: a table dropped and made again on
  the source would still be read from the copy's old rows. A plan the
  source refused, for the moment or for good, is asked about again. The
  MySQL port asks every time.
- **A connection does not read a value go back after the source answered
  it; the copy waits for it.** The copy is a second or two behind the source
  in fresh mode: a counter the source just showed a connection as 37413
  could read 37405 on the copy the next moment. So once the source answered
  a connection about a table, a statement for the copy that reads that
  table on it first reads where the source's WAL stands, then waits, up to
  6 seconds and never past its own `statement_timeout`, until capture's
  saved position has reached it (the
  commit of its last whole transaction, or on a quiet source the position
  the source's walsender last reported) and the copy's recent changes have
  been brought up to date after that; then the copy answers. When capture
  is not moving fast enough to get there in the time left (judged from how
  far its saved position moved in the last seconds), or the wait runs out,
  the source answers at once and the copy runs nothing (`copy_too_old` in
  `/status`). Measured on PostgreSQL 16 in fresh mode, the counter written
  every 20 ms and read 100 times from the source then for the copy on one
  connection: the copy answered all 100, none went back, and on a quiet
  source 10 of 10. The source's position is asked in the same write as
  every statement the source answers, right behind it, so it costs no round
  trip and no connection when the copy waits later; after an extended
  batch it is asked once the batch is over. Any row or statement's end the
  source sends counts, also when an error follows it. When no position of
  the source after its last answer is known (that read was refused, or the
  connection to the source was lost), the source answers the statement and
  the log says so once a minute: no clock of the source is compared with
  the router's. A table read from its snapshot alone (fast mode, a table
  with no recent changes) says nothing of how far it reaches, so on such a
  connection the source answers it. Until a statement has read the
  server's copy in fresh mode there is nothing to wait on: the copy runs to
  start its recent changes, and the source answers. This holds for
  every connection with a forwarding user, routing by plan or not: one that
  ran a transaction block on the source waits for the copy after its
  `COMMIT` too. A connection the source never answered, or answered only
  with an error, reads the copy without waiting. While a statement waits it
  holds its own connection to the port and nothing else: no worker of the
  copy, which has not been asked yet, and no connection to the source.
- **That holds table by table, not for the whole connection.** The port
  keeps, per connection, which tables the source answered it about, and a
  statement for the copy waits, or goes to the source, only when it reads
  one of them. A statement that reads other tables is answered by the copy
  at once, however far behind the copy is. What this means for a reader:
  two answers on one connection about *different* tables can be of
  different moments. An example: `SELECT balance FROM accounts WHERE id = 7`
  is cheap, so the source answers it, as of now; the next statement,
  `SELECT sum(amount) FROM payments`, is expensive, reads no table the
  source answered this connection about, and is answered by the copy, a
  second or two behind (in fast mode, as old as the last snapshot). A
  payment committed together with a change of that balance can be in the
  first answer and not in the second. A value of one table still never
  goes back: after the source showed `accounts`, a report that reads
  `accounts` waits for the copy as before. To have two tables read at one
  moment, read them in one statement, or in a transaction block, which the
  source answers whole.
- **Which tables a statement read is told only when it can be proven; when
  it cannot, the statement counts as having read every table**, and the
  rule is the connection's again, as before. It is proven in two steps.
  First its text: one `SELECT` (a `WITH` at its start, subqueries, joins and
  set operations included) whose tables are all named in `FROM`, calling
  only functions of PostgreSQL's own that read no table (`count`, `sum`,
  `coalesce`, `now`, `date_trunc` and the like). Then the source itself, in
  the same write as the statement and right behind it, with one query of
  its catalog: each name must resolve, under the session's `search_path`,
  to a plain permanent table that has no child table, is no partition or
  child itself, and has no row-level security, and the database must hold none of the things that run a
  user's code where the text shows none. The tables kept are the ones the
  source names in that answer. So each of these, answered by the source,
  marks every table: a view, a temporary or foreign table, a partitioned
  table or an inheritance parent (reading it reads its children), a
  partition or an inheritance child (the copy can hold its rows under its
  parent's name), a table with row-level security, a function in `FROM`, a function the port does
  not know to read no table (a function of yours, `nextval`,
  `query_to_xml`), a function written with its schema, `TABLE x`,
  `LATERAL`, `ONLY`, `TABLESAMPLE`, `FOR UPDATE`, a `WITH` that is not at
  the start of the statement, a cursor (`DECLARE`, `FETCH`), `SHOW`,
  `VALUES` alone, `EXPLAIN`, several statements in one text, text the port
  does not read whole (a dollar-quoted string, `E'...'`), a table name
  written outside ASCII without quotes, more than 32 tables in one
  statement, a `search_path` that names `pg_catalog` or `pg_temp`, and
  any statement at all while the database holds an operator or a cast
  written in SQL or PL/pgSQL, a domain with a constraint, or a function or
  aggregate of yours whose name the statement calls or writes after a dot
  (PostgreSQL calls `f(t)` for `t.f` when `t` has no column `f`). Past 64 tables a connection stops
  keeping names and every table is marked. In an extended-protocol batch
  the tables are confirmed once the batch is over (its `Sync`): a statement
  for the copy inside the same batch, before that, is held back. Names are
  compared without regard to case, and a name the statement for the copy
  writes without a schema counts for every schema that has a table of that
  name: the copy matches names that way, so more is held back, never less.
  Operators and casts an extension wrote in C are taken to read no table
  of yours.
- **An `EXPLAIN` the source refuses** (a table it does not have, a syntax it
  does not take) sends the statement to the source, which answers it with
  its own error (`explain_failed` in `/status`). So does a text of more than
  one statement, which is not asked about at all: `EXPLAIN` of the first
  would run the others.
- **The plan's numbers are estimates**, as fresh as the source's statistics
  (`ANALYZE`), and a sequential scan's rows are the rows it is estimated to
  return after its filter, not the rows it reads.
- **A prepared statement is asked about once**, at its first Describe or
  Bind, before any argument exists, and keeps that side. One with
  parameters is asked with `EXPLAIN (GENERIC_PLAN, FORMAT JSON)`, which
  PostgreSQL 16 and later take. **On PostgreSQL 15 a prepared statement with
  parameters goes to the source**, however expensive: that `EXPLAIN` is
  refused there. A statement first described in the middle of a batch that
  already went to the source is sent there for that batch, and asked again
  the next time.

**A server that hides tables or columns sends nothing to its source**, not a
statement the copy refuses, not a transaction block, not a catalog statement:
the rules are applied by the copy's views, and a statement run on the source
reads past them, whatever the forwarding user's grants (a view there runs with
its owner's rights, as a `SECURITY DEFINER` function does). Such a server is
answered by the copy alone, as without a forwarding user; a rule saved while a
block is open on the source fails that block. A statement that uses a
`char(n)` column where its trailing blanks count has no source to go to
there, on either kind of server, and is refused (`0A000`, with the column's
name) rather than answered with other rows: select the column and compare it
in the client, or give the server a forwarding user. `/status` says so for the server
(`forwarding_off`), and the log once.

A prepared statement keeps the side it was given once its columns were
described: a driver that prepared it once (pgx, JDBC) is not told other
columns later. One that the copy refused at its first execution runs on the
source from then on.

#### Which side answered a statement: `SHOW dbtrail.router_status`

On any connection to the port, this statement says which side answered that
connection's last statement and why:

```sql
SELECT count(*) FROM orders WHERE now() IS NOT NULL;
SHOW dbtrail.router_status;
```

```text
           name           |                                     value
--------------------------+--------------------------------------------------------------------------------
 server                   | a9d50e10e73cb3b4
 forwarding               | on
 sql_mode                 | fast
 copy_snapshot            | 2026-11-21T00:42:49Z
 copy_age_seconds         | 1
 last_answer              | source
 last_answer_reason       | veto
 last_answer_reason_means | something in the statement is not known to answer on the copy as on PostgreSQL
 last_answer_error        | no
 counting_since           | 2026-11-21T00:42:50Z
 statements_copy          | 1
 statements_source        | 12
 statements_refused       | 1
 reason_answers_the_same  | 1
 reason_veto              | 9
```

It is the PostgreSQL port's counterpart of the MySQL port's `SHOW ROUTER
STATUS`, with the same row names where they mean the same. The rows:

- `server`: the id of the server this connection reads.
- `forwarding`: `on` when statements of this connection may go to the source,
  `off` when the copy answers alone; `forwarding_off_reason` then says why (no
  forwarding user, or a server that hides tables or columns).
- `forwarding_account`: the account statements are sent with, `forwarding
  user <name>` or `capture account, read-only, tables of the copy only`
  ([above](#forwarding-with-the-capture-account)); `none` when there is none.
- `sql_mode`: `fresh` or `fast`.
- `copy_snapshot` and `copy_age_seconds`: when the server's newest snapshot was
  taken and how long ago that is, `unknown` before the first snapshot. In fresh
  mode an answer also reads the changes captured since, so the age of what a
  statement read is the last answer's own, below. A server whose copy cannot be
  read says why in `copy_unavailable_reason`.
- `last_answer`: `copy`, `source`, `refused`, or `none` before the first
  statement. `last_answer_reason` is one of the reasons in the table below,
  `last_answer_reason_means` the same in a sentence, and `last_answer_error`
  says whether the client got an error for it (`yes` after `copy` means the
  copy could not run it and nobody else could answer).
- When the copy answered: `last_answer_indexed_through` (every change capture
  had recorded up to this instant was in what the statement read) and
  `last_answer_age_seconds` (how long before the answer that was, rounded up).
  For an answer that read a table without its newest changes (fast mode,
  among others) they are what its snapshot files record, as in `/status`.
  Both read `unknown`, and `last_answer_age_unknown_reason` says why, for an
  answer whose snapshot files do not record how far they reach or that read
  no table.
- `counting_since`, `statements_copy`, `statements_source`,
  `statements_refused` and one `reason_<name>` per reason: the server's counts
  since the router started, the same numbers `/status` serves.

What to know about it:

- The port answers it itself, always: with or without a forwarding user,
  before the server has a snapshot, over the simple and the extended protocol
  (psql, psycopg, pgx, JDBC, prepared or not), inside a transaction block, and
  in a failed block too, where PostgreSQL answers nothing but `ROLLBACK`: it is
  how to read which side failed the block, and the block stays failed.
- It is never sent to the source and never run on the copy, and it is not an
  answer: asking twice says the same, and it is in no count. Statements the
  port answers itself (`SET`, `SHOW` of a setting, `BEGIN`, the catalog's
  listings, time travel) do not change the last answer either.
- Send it alone. A message that holds it with other statements (`SELECT 1;
  SHOW dbtrail.router_status` in one `psql -c`) is refused whole (`0A000`) and
  nothing in it runs.
- The name is not a setting: `SET`, `RESET` and `current_setting` of it are
  refused (`42704`). The dot in the name is on purpose: PostgreSQL keeps dotted
  names for extensions, so no setting of its own can have this one.
- `SHOW ROUTER STATUS`, the MySQL port's spelling, is refused here with the
  statement to send: `SHOW ROUTER STATUS is the MySQL port's statement; on
  this port it is SHOW dbtrail.router_status`.
- The time-travel port of `bintrail-pg flashback` routes nothing and does not
  answer it.

#### The port's status

```bash
bintrail-console pg-router --status-addr 127.0.0.1:9092 [--status-statements]
curl -s http://127.0.0.1:9092/status
```

`/status` is JSON: per server (by its id), how many statements each side
answered (`copy`, `source`, `refused`), why (`reasons`), and per side how many
finished, how many got an error and how long they took (`answers`), and its
mode: `sql_mode` (`fresh` or `fast`) and where that comes from,
`sql_mode_from` (`environment`, `saved` or `default`). Every PostgreSQL server
is listed with its mode, whether or not a statement reached it yet. The
document also says `fresh` (true when `BINTRAIL_SQL_TAILS` puts every server in
fresh mode) and `sql_mode_forced` (`fresh` or `fast` when that setting decides
for every server), as the MySQL router's does. How old the data of a server's
last answer was is `data_as_of` (every change capture had recorded up to
this instant, a commit time on the source, was in what it read) and
`data_age_seconds` (how long before the answer that was, by this machine's
clock), with `data_age_max_seconds` the worst since the process started. Only
an answer the client got counts, never a statement the source answered or one
that ended in an error. An answer that read a table without its newest
changes (fast mode, among others) is as old as the oldest instant its
snapshot files record, the one a lag limit is measured by (see the lag limit
above): a minute or more for a snapshot just taken, and as old as the update
that last wrote the file for a table with no change. When a file it read
records none, the answer has no age: `data_age_unknown` counts them,
so with any the worst is a floor, and `data_age_unknown_reason` says why the
last one has none. On a source with no write since, the age grows though
nothing is missing; this machine's clock ahead of the source's makes it
larger, and behind it smaller, down to 0. With
`--status-statements` each server also carries `statements`: a row per
statement shape and side, values blanked and table and column names kept. That
address asks for no password: whoever reaches it reads the counts, and with
`--status-statements` the names of the tables and columns your clients query.
`/metrics` on the same address is this process's Prometheus metrics.

An address that cannot be opened (something else is listening there) does not
stop the router: it prints a `WARNING` at start-up, writes it to its log, and
serves the PostgreSQL port without `/status` and `/metrics`. Free the address
or set another, then restart the router.

The reasons, with the MySQL port's names where they mean the same:

| Side | Reason | Why |
|---|---|---|
| copy | `answers_the_same` | everything in it answers on the copy as on PostgreSQL |
| copy | `copy_command` | a command of the copy's own (`SHOW TABLES`, `DESCRIBE`) |
| copy | `no_forward_account` | the server has no forwarding user: the copy answers alone |
| copy | `hidden_rules` | the server hides tables or columns: nothing is sent to its source |
| copy | `capture_gate` | the server forwards with the capture account, and the statement did not pass that account's check, so the copy answered it without asking the source for its plan |
| source | `cheap_plan` | its plan on the source is under both thresholds: the source answers it, by its indexes |
| source | `result_over_row_cap` | its plan on the source is expensive and says it returns more rows than the copy returns for one statement (`--sql-port-max-rows`): the copy would refuse the result, so it is not tried there |
| source | `explain_failed` | its plan could not be had (the source refused the `EXPLAIN`, a text of more than one statement): the source answers it, its error included |
| source | `in_transaction` | the client's transaction block |
| source | `veto` | a construct the copy answers differently, or is not known to answer the same |
| source | `session_differs` | a session setting the copy is not known to reproduce (`TimeZone`) |
| source | `not_a_select` | `EXPLAIN`, a cursor, a savepoint, `VALUES` |
| source | `catalog_not_on_copy` | a catalog statement the copy does not answer |
| source | `table_not_on_copy` | a table the copy does not hold |
| source | `copy_refused` | the copy could not run it |
| source | `copy_columns_differ` | a column the copy writes otherwise (a `numeric` without precision) |
| source | `copy_behind_source`, `copy_age_unknown` | the server's lag limit |
| source | `table_not_captured` | a table whose capture lost or dropped changes since its snapshot was read (a table capture skips, a capture ledger that does not say which tables or cannot be read, a lost slot, a snapshot published over a known loss), in fresh and fast mode: the copy holds that table as of its snapshot only. Without a forwarding user, or on a server that hides tables, the copy answers it from that snapshot with a notice that says so |
| refused | `read_only` | not a read |
| refused | `copy_queue_full` | for the copy, whose workers were all taken and the line for one full: refused with `53000`, not sent to the source; run it again |
| refused | `copy_wait_timeout` | for the copy, it waited for a worker as long as `--max-wait` lets it: refused with `53000`, not sent to the source; run it again |

Here `answers_the_same` counts the statements the copy answered: the ones it
answers as PostgreSQL does and whose plan on the source is expensive.

### Listing schemas, tables and columns on the PostgreSQL port

The statements tools send to list what a server holds are answered by the
port from the copy's real tables: psql's `\l`, `\dn`, `\dt` (with or without
a pattern), `\d`, `\d <table>` and `\d+ <table>`; JDBC's `getSchemas`,
`getCatalogs`, `getTables`, `getColumns` and `getPrimaryKeys` (what DBeaver and
Metabase call); SQLAlchemy's inspector (`get_schema_names`, `get_table_names`,
`get_view_names`, `get_columns`, `get_pk_constraint`, `has_table`,
`get_foreign_keys`, `get_indexes`, which Superset calls); and Grafana's table
and column lists. They were checked against PostgreSQL 16 and 18 with psql 13 to 18,
JDBC 42.7, SQLAlchemy 1.4 with psycopg2 and Grafana's queries: for the copy's
tables each prints what it prints for the source's, but for what follows.

Each major version of psql writes these commands as statements of its own, and
the port answers each one's: the commands above were run with psql 13.23,
14.25 (the one Ubuntu 22.04 installs), 15.20, 16.16, 17.12 and 18.7, and every
one of them lists the same schemas, tables and columns. Up to DBTrail
0.137.0 only psql 16 was answered in full: 13 and 14 were refused `\dt`, `\d
<table>` and `\l`, 18 `\d <table>` and `\l`, 15 and 17 `\l`.

- **What the copy can be read for, and only that**: the tables and columns a
  statement on the copy reads. A table or a column the server hides is not
  listed, and neither is a primary key one of whose columns it hides. The
  copy's own schema (`main`, where the change log is) is not listed.
- **Each column has the type a result announces for it** (see
  [above](#sql-on-the-copy-from-psql)), so
  what a tool lists is what it then reads: a `varchar(n)`, `jsonb`, enum or
  `interval` column is listed as `text`.
- **`NOT NULL`, the default and the primary key are the source's**, as the
  snapshot recorded them, and so is the table's OID. The primary key is named
  as PostgreSQL names one by default (`<table>_pkey`): the snapshot does not
  record its name. A snapshot taken before DBTrail recorded a table's shape
  lists the table without them (a warning in the log names it); the next
  snapshot records them.
- **What the copy does not have is not listed**: no other index, no foreign
  key, view, sequence, policy, publication, comment, enum or domain, no
  access method, and no named `NOT NULL` constraint (the list psql 18's `\d+`
  prints for a PostgreSQL 18; a column's `not null` is listed). The owner of everything is the user the client connected
  as, and the one database is the one it connected to, with collation `C`
  (text compares byte by byte here).
- **Any other statement on the system catalog is refused** (`0A000`) by name:
  `pg_catalog`, `information_schema` and the `pg_` catalog tables are not run
  on the copy, whose engine has a catalog of its own that would answer with
  its internal schemas and types. `SHOW TABLES` and `DESCRIBE <table>` list
  the copy's tables and columns in its engine's terms. A client version that
  writes these statements otherwise than the versions above is refused the
  same way: a psql older than 13 or newer than 18 may be, for the commands it
  writes differently, until its statements are added.
- `\d` of an index is refused: the copy has no index; `\d <table>` shows the
  primary key. A connection reads the catalog once for a burst of statements:
  a table or a column hidden while it is connected leaves its listing within
  10 seconds, and is not readable in the meantime: a statement that reads the
  table or the column fails as soon as the rule applies.
- **A rule saved while a client is half way through a result does not reach
  the rest of that result.** A client of the extended protocol can ask for
  some rows of a statement and for the rest later (a cursor, a driver's fetch
  size). The port computes the whole result when the statement runs and keeps
  it for that client, so the rows it asks for after a rule that hides a
  column was saved still carry that column: they are rows of a statement it
  was allowed to run when it ran it. Its next statement is answered under the
  rule.

### Interactive `AS OF` from `psql`

The `_flashback` / `_snapshot` / `AS OF` time-travel grammar is also available
over the **PostgreSQL wire protocol**, so you can run single-row time-travel
straight from `psql` (or any PostgreSQL driver) rather than the MySQL-wire shim:

```bash
bintrail-pg flashback \
  --index-dsn 'root:…@tcp(127.0.0.1:3306)/bintrail_index' \
  --shim-config shim.yaml \
  --baseline-dir ./baselines      # optional; enables _snapshot
  # --listen 127.0.0.1:5433 is the default
```

Then connect and query. **Set `dbname` to your PostgreSQL schema** (e.g.
`public`); choose flashback-vs-snapshot with the `_flashback.` / `_snapshot.`
table prefix in the query, exactly like the MySQL shim:

```
psql "host=127.0.0.1 port=5433 user=<tenant> dbname=public"

-- binlog-only view of one row at a past instant:
SELECT * FROM _flashback.orders AS OF '5 minutes ago' WHERE id = 42;

-- baseline-aware view (also resolves rows untouched in the retained window):
SELECT * FROM _snapshot.orders  AS OF '2026-07-01 09:00:00' WHERE id = 42;

-- the bare form on the real table works too:
SELECT * FROM orders AS OF '5 minutes ago' WHERE id = 42;
```

Scope and behaviour track PostgreSQL's current time-travel maturity:

- **Single-row `AS OF` only** — a `WHERE <primary-key> = <value>` predicate.
  Full-table `AS OF` is refused with remediation: the shim's whole-table read
  needs the `CREATE TABLE` metadata a PG baseline does not carry. A whole
  table at a point in time is `reconstruct --output-format parquet` (see
  [Full-table snapshots](#full-table-snapshots)).
- Columns render as **text**, on purpose: a row as it was can carry other
  columns, and other types, than the table has now. Read each one as a string,
  or let your driver text-parse it into a typed target. One caveat: a
  binary/`bytea` value containing a `0x00` byte truncates at the first NUL in
  `psql`/libpq (their text protocol uses NUL-terminated C strings); a driver that
  reads the raw value (e.g. `pgx` scanning into `[]byte`) round-trips it intact.
- Both the **simple and the extended query protocol** are served, so a driver
  in its default mode can prepare `SELECT * FROM _flashback.orders AS OF $1
  WHERE id = $2` and run it with arguments. An argument cannot hold a single
  quote (`'`) here: write such a value in the statement's text.
- **Login** is SCRAM-SHA-256 against each tenant's password in
  `--shim-config` (`shim.yaml`), as on `pg-router`: the password never crosses
  the network. A client that asks for TLS gets it, with `--tls-cert` and
  `--tls-key` (`BINTRAIL_PG_FLASHBACK_TLS_CERT`, `_KEY`) or with a certificate
  made up at start; one that asks for none is answered in clear text. The
  default listen address is loopback, and a bind other machines reach is
  warned about at start.

`bintrail-pg` still ships the MySQL-wire `shim` command as well, if you prefer
to reach the index from MySQL tooling. See
[Time-Travel SQL Setup](time-travel-sql.md) for the full grammar.

---

## Sequences after recovery

Restoring **row data** does not restore the **sequence cursor** behind a
`SERIAL` or `IDENTITY` column. This is the classic PostgreSQL dump/restore
gotcha, and it applies to bintrail-recovered data too — read this section if
your tables use auto-generated ids.

### Why this happens

PostgreSQL's logical decoding does not replicate sequence state. In the words
of the PostgreSQL docs: *"Sequence data is not replicated. The data in serial
or identity columns backed by sequences will of course be replicated as part
of the table, but the sequence itself would still show the start value on the
subscriber."*

For bintrail that means:

- The **id values in your rows are fully captured** — the materialized id
  rides in the row image, and `recover` puts it back verbatim. No data is lost.
- The sequence's own `last_value` is a **separate catalog object** bintrail
  never sees. Nothing in the index knows how far the sequence had advanced.

So after loading recovered rows into a target, the sequence can lag behind the
ids that are now in the table — and the next `INSERT` that draws from it fails
with a duplicate-key error on the primary key.

This is the same boundary MySQL has with `AUTO_INCREMENT`, with one twist:
in PostgreSQL, an INSERT that supplies an **explicit id** (which is exactly
what `recover`'s reversal SQL does) never advances the sequence.

### When you need to fix the sequence

- **Recovering into a fresh or restored target** (a different database, or the
  same database rebuilt from a backup): **always**. The target's sequence
  reflects the backup — or the start value — not the history you just replayed.
- **After the sequence was reset on the source** (`TRUNCATE … RESTART
  IDENTITY`, a manual `setval`, `ALTER SEQUENCE … RESTART`): **yes** — the
  recovered ids are ahead of the rewound cursor.
- **Applying `recover` output on the same live database where the rows were
  deleted**: usually **not needed**. The sequence advanced when the rows were
  first inserted, and a sequence never goes backward on its own — the cursor is
  still past the re-inserted ids. Running the fix anyway is cheap and harmless,
  so when in doubt, run it.

### The fix: `setval` to MAX(id)

After the recovered rows are loaded, point each sequence just past the highest
id actually in the table. `pg_get_serial_sequence` finds the sequence name for
you (it works for both `SERIAL` and `IDENTITY` columns):

```sql
SELECT setval(
  pg_get_serial_sequence('public.orders', 'id'),
  COALESCE((SELECT MAX(id) FROM public.orders), 0) + 1,
  false
);
```

The `+ 1, false` form makes the next `nextval` return exactly `MAX(id) + 1`,
and handles an empty table (next value is 1). Run one `setval` per
sequence-backed column — a table can have more than one.

Avoid running it while writers are actively inserting into the table (the
`MAX` and the `setval` are not atomic together); recovery windows are normally
quiesced anyway.

To fix **every owned sequence in a schema** at once, generate the statements
from the catalog (covers `SERIAL` and `IDENTITY` columns; run the output it
prints):

```sql
SELECT format(
  'SELECT setval(%L, COALESCE((SELECT MAX(%I) FROM %I.%I), 0) + 1, false);',
  quote_ident(sn.nspname) || '.' || quote_ident(seq.relname),
  att.attname, tn.nspname, tbl.relname
)
FROM pg_class seq
JOIN pg_namespace sn ON sn.oid = seq.relnamespace
JOIN pg_depend dep ON dep.objid = seq.oid AND dep.deptype IN ('a', 'i')
JOIN pg_class tbl ON tbl.oid = dep.refobjid
JOIN pg_namespace tn ON tn.oid = tbl.relnamespace
JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = dep.refobjsubid
WHERE seq.relkind = 'S'
  AND tn.nspname = 'shop';   -- your schema
```

### Out of scope: standalone sequences

A **standalone sequence** — one whose `nextval` values are used directly
(counters, ticket numbers) and never land in a captured table — is
**irrecoverable from bintrail**. Its values never entered the row history, so
there is nothing to derive a cursor from. If such sequences matter to you,
snapshot them by other means (e.g. include them in your regular dumps —
`pg_dump` records sequence state).

---

## Type support

bintrail-pg captures every value as its PostgreSQL text representation (the
`pgoutput` text format) and, on recovery, regenerates it as a standard-conforming
quoted literal that the target column's input function coerces back. Because the
value is stored verbatim as text — never reparsed through a numeric type in the
index — the `numeric`-via-`float64` precision class that the MySQL path had to guard
against simply cannot occur here.

The following types are **round-trip tested** two ways — insert → capture → index →
`recover` → re-execute the reversal against live PostgreSQL, asserting the column's
canonical `::text` is byte-for-byte identical (`TestOne_PGTypeRoundTripMatrix`), and
through the **baseline+delta reconstruct fold** under deliberately skewed session
GUCs (`TestOne_PGTypeMatrixThroughReconstructFold`) — in `internal/pgstreamrun`,
run across the PG 15/16/17/18 CI matrix:

| Category | Types | Notes |
|---|---|---|
| Integer | `smallint`, `integer`, `bigint` | exact |
| Arbitrary precision | `numeric` (`decimal` is its alias) | full **precision and scale** preserved — values > 2^53 and trailing zeros (`1.50`) survive |
| Floating point | `real`, `double precision` | rendered under pinned `extra_float_digits=3` (shortest-precise) on both capture and baseline sessions |
| Character | `text`, `varchar(n)`, `char(n)` | single quotes and backslashes escaped correctly (`standard_conforming_strings`); `char(n)` round-trips (trailing blanks are insignificant in `bpchar` — PostgreSQL trims them on cast to text) |
| Boolean | `boolean` | |
| UUID | `uuid` | |
| Binary | `bytea` | hex (`\x…`) form — guaranteed by the pinned `bytea_output=hex`, no longer default-dependent |
| JSON | `json`, `jsonb` | `jsonb` tested with an embedded quote |
| Date / time | `date`, `time`, `timestamp`, `timestamptz`, `interval` | rendered under pinned `TimeZone=UTC` / `DateStyle=ISO` / `IntervalStyle=postgres` on both capture and baseline sessions — no longer dependent on server defaults |
| Network | `inet`, `cidr`, `macaddr` | |
| Bit string | `bit(n)`, `varbit` | |
| Range | `int4range` | the other built-in range types share the same text mechanism |
| Array | `integer[]`, `text[]` | element quoting/escaping (e.g. a comma inside an element) preserved |
| Geometric | `point` | |
| Enum | user `ENUM` types | recovered **by label** — the target must declare the same enum type |
| Composite | user-defined composite types | field quoting preserved (embedded comma, doubled quote); the target must declare the same composite type |
| Money | `money` | round-trips, but its text form is **locale-dependent** (`$`-prefixed); recover into a target with the same `lc_monetary` |

The generated reversal SQL is PostgreSQL dialect (double-quoted identifiers,
standard-conforming string escaping, a `SET LOCAL standard_conforming_strings = on`
guard) — the dialect is selected automatically from the source recorded in the index,
so `bintrail-pg recover`, the web interface, and the MCP server all emit valid PostgreSQL
for a PostgreSQL source.

**Extension types** — PostGIS `geometry`/`geography` and pgvector `vector` — are
round-trip tested the same insert → capture → `recover` → re-execute way, in
dedicated CI cells running extension-bearing images
(`TestOne_PGExtensionTypeRoundTripMatrix`); see
[Extensions and custom types](#extensions-and-custom-types) below.

**Untested / best-effort:** `hstore` and other extension-provided types are covered
under [Extensions and custom types](#extensions-and-custom-types) below. Range
types beyond `int4range`, multirange, geometric types beyond `point`,
arrays beyond the tested `integer[]`/`text[]` (multi-dimensional, `NULL`-containing)
are not yet in the round-trip suite — they are captured as text and are expected to
coerce, but verify your own round-trip.

### How each type reads in SQL on the copy

A snapshot of a PostgreSQL source holds every value as the text PostgreSQL
wrote, which is what keeps recovery exact. SQL on the copy (the web
interface's SQL page, `bintrail views`, the views file inside each snapshot)
reads each column as the type the table declares, in the table's own column
order, from the shape the snapshot records beside the data. Nothing is
rewritten: a snapshot that records the shape reads this way whichever
version took it. One taken before the shape was recorded reads as text, in
alphabetical column order, until the next full snapshot; the log names such
tables once.

| PostgreSQL type | Reads as |
|---|---|
| `smallint`, `integer`, `bigint` | whole numbers of the same width |
| `numeric(p,s)` up to 38 digits | exact decimal with that precision and scale |
| `numeric` with no precision | exact decimal with 20 digits before the point and 18 after |
| `real`, `double precision` | floating point, `NaN` and `Infinity` included |
| `boolean` | boolean |
| `date`, `timestamp`, `timestamptz` | date and timestamps; `infinity` included |
| `time`, `timetz` | time of day, with its offset for `timetz` |
| `interval` | interval, with its months, days and time kept apart as PostgreSQL keeps them |
| `uuid` | uuid |
| `bytea` | bytes |
| `text`, `varchar(n)`, `char(n)` | text compared byte by byte, so `'Lima'` is not `'lima'` |
| an array of any type above but `bytea`, `numeric` with no precision, `time`, `timetz` and `interval` (`text[]`, `integer[]`, `date[]`, ...) | a list of that type: `labels[1]` is the first element, `'red' = ANY(labels)` works |
| everything else | the text PostgreSQL wrote |

"Everything else" is `json` and `jsonb`, enums, domains, ranges, a `numeric`
wider than 38 digits, with a negative scale or with a scale above its
precision, every extension type, and an array of any of these, of `bytea`,
of `numeric` with no precision, of `time`, `timetz` or `interval`.
`->>` reads a `json` column as it is; an array left as text is the text
`{a,b}`, so `labels[1]` is its first character, not its first element. A
column left as text still compares byte by byte. Each view names the columns
it left as text.

Two kinds of table are read as text throughout: one with two columns whose
names differ only in case (`"Id"` and `"id"`), which the copy cannot tell
apart, and any table in a views file that follows later snapshots
(`bintrail views` without `--pin-snapshot`), because a type written into
such a file would outlive a change of the column's type at the source.

A value its type here cannot hold fails the statement that reads it, with an
error that says so. It is never rounded and never read as NULL:

- `NaN` or `Infinity` in a `numeric` column.
- A `numeric` with no precision holding more than 18 decimals or more than
  20 digits before the point.
- A `date`, `timestamp` or `timestamptz` before the common era, or beyond the
  years the copy's types reach.
- A `bytea` not written as `\x` and hex digits.
- The `time` `24:00:00`, which the copy would show as midnight. A `timetz`
  of `24:00:00` reads as it is.
- An `interval` of `infinity` (PostgreSQL 17).
- An array of more than one dimension (`'{{1,2},{3,4}}'`), or one whose
  first element is not numbered 1 (`'[0:1]={1,2}'`).

The elements of a text array compare as the session compares text. On the
SQL card of a PostgreSQL server that is byte by byte. In a DuckDB of your
own with `default_collation = 'nocase'`, `labels[1] = 'RED'` matches `red`,
where a text column of the same views does not.

A statement that does not read that column is not affected, and neither is
recovery, which works from the text.

In the console's SQL card and the `sql` tool, a statement on a PostgreSQL
server runs by PostgreSQL's rules where the two engines differ in silence:
NULL is the largest value (last in `ORDER BY x`, first in `ORDER BY x DESC`),
`/` between two integers gives an integer cut toward zero (`9 / 2` is `4`,
`-9 / 2` is `-4`), and text compares byte by byte (`'a' = 'A'` is false). A
views file you run in a DuckDB of your own runs under that DuckDB's settings;
these three lines give it the same rules:

```sql
SET default_null_order = 'nulls_last_on_asc_first_on_desc';
SET integer_division = true;
SET default_collation = '';
```

Three things still answer differently from PostgreSQL, with no error:

- A division by zero is NULL, where PostgreSQL fails the statement.
- Text sorts by bytes. PostgreSQL sorts by the collation the database was
  created with, which for anything but `C` orders letters differently.
- `char(n)` compares with its padding, where PostgreSQL ignores trailing
  blanks. On the PostgreSQL port such a statement is not answered by the
  copy: it goes to the source, or is refused where nothing is sent there
  ([which side answers](#which-side-answers-a-statement-on-the-postgresql-port)).
  In the SQL card, the `sql` tool and a views file it is still the copy's
  answer.

A numeric with no precision prints with all 18 decimals (`12345.678` as
`12345.678000000000000000`); it compares, sorts and sums as the same number.

A `CASE`, `COALESCE`, `GREATEST` or `LEAST` whose values are numbers of
different scales can print more zeros on the copy than on PostgreSQL, for the
same value: PostgreSQL writes each value with its own decimals, the copy
gives them all one type and writes every one with that type's. Over a
`numeric(12,2)` column, `coalesce(sum(amount), 0)` of no rows is `0` on
PostgreSQL and `0.00` on the copy, and `CASE WHEN ... THEN amount ELSE 0 END`
writes `0.00` for the rows that take the `0`. The number is the same; a
client that reads it as a number sees no difference. Writing the other value
with the column's decimals (`coalesce(sum(amount), 0.00)`) makes both sides
print the same. (Around a `round()` the port does check this, value by
value, and sends a statement whose decimals would differ to the source.)

---

### A proxy in front of the port

A proxy such as ProxySQL logs into the port twice:

- **With your application's login**, for every statement it sends. Save that
  username and password as the server's **application login** (the same one
  the MySQL port takes): it opens that server, whatever database the proxy's
  client named, unless that name is another server of this DBTrail (its id or
  its name), which the port refuses with `28000` so that a login never reads
  one server under another's name. If your application's database is called
  like another server here, rename that server, or have the proxy connect
  with the login's own server as the database.
- **With its monitor login**, for its health checks (ProxySQL's
  `pgsql-monitor_username` and `pgsql-monitor_password`). The port takes the
  **monitor login** saved for the MySQL port: one username and password for
  both ports. A connection opened with it opens no server, whatever database
  it names (ProxySQL's is `postgres`), and answers only what the monitor
  sends: an empty query as its ping, `SET`, and `SELECT pg_is_in_recovery()`,
  which is `true` on this port, so a server in a replication hostgroup stays
  among the readers. Any other statement is refused (SQLSTATE `0A000`).

`SELECT pg_is_in_recovery()` is `true` on every connection: the port is
read-only, as a standby is.

### Names the copy reads as its own words

The copy's SQL has words PostgreSQL does not, and a table or column with one of
them as its name is refused by the copy with a syntax error when the name is
written without quotes. PostgreSQL takes the same statement. Measured against a
real PostgreSQL with the comparison harness (`internal/pgcompare`), 169
statements: 73 refused, none answered differently.

- **Tables** named `pivot`, `unpivot`, `qualify`, `asof`, `anti`, `show`,
  `describe` or `summarize`: `SELECT id, v FROM pivot` is refused, and so is
  `FROM pivot AS t`.
- **Columns** named `at`, `by`, `anti`, `semi`, `asof`, `positional`,
  `qualify`, `pivot`, `pivot_longer`, `pivot_wider`, `unpivot`, `unpack`,
  `glob`, `lambda`, `show`, `describe` or `summarize`: `SELECT id, at FROM t`,
  `WHERE describe IS NOT NULL` and `sum(by)` are refused.

Written another way, each of them answers as PostgreSQL does: the name in
double quotes (`"at"`, `FROM "pivot"`), the table with its schema
(`FROM public.pivot`), or the column with its table (`t.at`). `SELECT *` is not
affected.

On the PostgreSQL port each of these names, written without quotes where a
name stands (a select item, a table after `FROM` or `JOIN`, an operand), is
quoted before the copy reads it, with or without a forwarding user, so the
statement answers as PostgreSQL does. Where PostgreSQL reads the word as its
own syntax (`GROUP BY`, `ORDER BY`, `PARTITION BY`, `AT TIME ZONE`, `SHOW` at
the start) it is left alone, and so is the copy's own syntax that PostgreSQL
refuses (`QUALIFY` as a clause, `ASOF JOIN`): through a forwarding user that
goes to your PostgreSQL, which refuses it. A name the port does not quote (an
alias written right after a table, `FROM t at`) still gets the copy's syntax
error (SQLSTATE `42601`), or goes to your PostgreSQL through a forwarding
user.

## Extensions and custom types

- **bintrail installs nothing in your database.** Capture is via the built-in
  `pgoutput` plugin only — no output plugin, no `CREATE EXTENSION`, no event
  trigger. This is a deliberate red line, and it is what lets bintrail-pg work
  against managed PostgreSQL (which forbids custom extensions).
- **PostGIS is round-trip tested.** `geometry` and `geography` columns stream
  as hex-EWKB, which embeds the SRID and round-trips losslessly back into
  PostgreSQL — proven empirically by `TestOne_PGExtensionTypeRoundTripMatrix`
  against the `postgis/postgis` image in CI. The target just needs PostGIS
  installed. (`REPLICA IDENTITY FULL` is required, as for any table: see
  step 3.)
- **pgvector is round-trip tested.** `vector` columns stream as their bracketed
  text form (`[0.5,-1.25,3.75]`) and round-trip byte-for-byte — same test,
  against the `pgvector/pgvector` image in CI. The target needs the extension
  installed.
- **Other extension/custom types** (`hstore`, …) are captured **as their text
  representation**. Recovery into a target that has the same extension installed
  is expected to work; treat exotic types as best-effort and test your
  round-trip. (Enums, ranges and user-defined composite types are round-trip
  tested — see the [Type support](#type-support) matrix.)
- **TimescaleDB hypertables are out of scope.** Logical decoding emits the
  underlying *chunk* tables (`_timescaledb_internal._hyper_*`), not the
  hypertable, so a hypertable is not captured coherently in this release.
  bintrail-pg detects a chunk relation in the stream and **warns once** (so you
  are never silently indexing raw chunks under their physical names) — but it does
  not synthesize the logical hypertable.

---

## Limitations

- **`UNLOGGED` tables are not captured.** They bypass the WAL by design, so logical
  decoding never sees them. bintrail-pg **warns** when an UNLOGGED table is in
  capture scope (at `stream` startup and in `bintrail-pg doctor`) — under a
  `FOR ALL TABLES` publication and under a `FOR TABLES IN SCHEMA` (PostgreSQL
  15+) publication's member schemas alike — but the changes are still not
  captured, so don't rely on bintrail for UNLOGGED data.
- **A cascade child must be in the publication.** A foreign-key `ON DELETE CASCADE`
  / `SET NULL` is captured (PostgreSQL performs it as ordinary row changes) **only
  if the child table is published**. If a published parent has an unpublished
  cascade child, the cascade rewrites are not captured — bintrail-pg warns (startup
  + doctor); add the child to the publication.
- **`TRUNCATE` is recorded as an audit marker, not a reversible change.** A
  PostgreSQL `TRUNCATE` emits no per-row `DELETE`, so left uncaptured it would be
  indistinguishable from "nothing happened". bintrail-pg records it as a
  `schema_changes` row (`ddl_type = TRUNCATE TABLE`) — the same durable audit
  trail MySQL's DDL leaves — surfaced by the `list_schema_changes` MCP tool and in
  the `schema_changes` table. It is a *record that the truncate happened*, not a
  reversible event: `recover` has no per-row before-images to put back, and a
  `reconstruct` whose window crosses the truncate refuses instead of bringing the
  removed rows back. A multi-table `TRUNCATE a, b` records one marker per
  published table; a truncate of an out-of-scope table records nothing. All of
  this needs a publication that publishes `truncate`: without it PostgreSQL
  sends nothing and no marker is recorded (see
  [step 4](#4-create-a-publication)).
- **Other schema changes are inferred, not read.** See
  [Schema changes on PostgreSQL](#schema-changes-on-postgresql).
- **Sequence cursors are not captured.** The materialized id values in your rows
  *are* captured; the sequence's own `last_value` (a separate catalog object) is
  not — logical decoding does not replicate sequences. After a restore, re-point
  each `SERIAL`/`IDENTITY` sequence past `MAX(id)` — recipe and when it matters
  in [Sequences after recovery](#sequences-after-recovery).
- **`GENERATED ... AS IDENTITY` / generated columns** can need care on recovery
  (`GENERATED ALWAYS AS IDENTITY` rejects an explicit insert; `STORED` generated
  columns are absent from the stream before PostgreSQL 18). Treat as best-effort.
  The copy never holds a generated column: the database computes it. On
  PostgreSQL 18 a table with a `VIRTUAL` generated column cannot be captured,
  and one with a `STORED` one needs `publish_generated_columns = stored` on the
  publication (see step 3 of the setup).
- **Full-table `reconstruct` writes snapshots** (`--output-format
  parquet`, and `baseline refresh`) **or SQL psql loads** (`--output-format
  psql`); a mydumper dump stays MySQL's (see
  [Full-table snapshots](#full-table-snapshots)). `bintrail-pg drill`
  rehearses the restore into a scratch PostgreSQL from the psql files.
  `verify` is supported too: baseline-anchored, live-source, and
  recover-input modes are all supported for PostgreSQL sources (see
  [verify — Notes per source](verify.md#notes-per-source), including how the
  live-source coverage check differs). **Single-row** `reconstruct` and
  the shim's single-row `_snapshot` work against a PG baseline and are
  validated end-to-end for scalar types — including the GUC-sensitive set,
  rendered under session GUCs pinned identically at capture and baseline;
  container types remain best-effort (see
  [Querying and recovering](#querying-and-recovering)). (`recover` and
  `recover-cascade` work unconditionally, with no baseline needed; cascades
  are captured as ordinary row changes — see
  [Querying and recovering](#querying-and-recovering).)
- **No connection attribution.** `pgoutput` does not carry the backend PID, so
  the `connection_id` column is empty for PostgreSQL sources. Forensic
  attribution ("which session made this change") is therefore a MySQL-only
  capability today, and this is a **settled decision, not a pending gap**
  ([#1212](https://github.com/dbtrail/dbtrail/issues/1212)): the only way to
  recover the backend PID is to install something into your database, and
  bintrail installs nothing into a source. Everything else forensics offers —
  the full before/after row images, the change timeline, `--query-hash`
  grouping where the source logs statements — works identically on PostgreSQL.
- **One database per slot.** A logical slot is scoped to a single database; to
  capture multiple databases on one cluster, run one `bintrail-pg stream` (and
  slot/publication) per database.
- **No BYOS agent.** The web interface captures PostgreSQL sources (**+ Add
  server** → PostgreSQL); `bintrail agent` (BYOS) does not support PostgreSQL.

The data-safety gates for PostgreSQL are closed
([#597](https://github.com/dbtrail/dbtrail/issues/597)): the data-safety set
(type fidelity, identity/generated recovery, slot/WAL monitoring, RI-FULL
validation, DDL-drift handling, the silent-loss coverage guards above),
baseline-anchored single-row `reconstruct` / time-travel with the
baseline↔delta rendering-GUC identity and its fold-validated type matrix
(#593), the managed-PostgreSQL smoke matrix (RDS and Aurora — see
[Managed PostgreSQL](#managed-postgresql)), and source-aware web interface
presentation including the live replication-health panel (v0.20.1).
**Full-table** `reconstruct` writes snapshots (`--output-format parquet`, and
`baseline refresh`) or SQL psql loads (`--output-format psql`); a mydumper dump
stays out of scope (above). `verify`
(baseline-anchored, live-source and recover-input) is supported.

---

## Schema changes on PostgreSQL

PostgreSQL's change stream carries row changes, not schema statements: no
`CREATE TABLE`, `ALTER TABLE` or `DROP TABLE` ever reaches bintrail-pg as text.
What the stream does carry is a description of each table (its name and its
columns with their types) sent before the first change made under a new shape.
bintrail-pg compares each description with the last one it saw for the same
table, identified by the table's internal number (its OID, which survives a
rename), and records what changed in `schema_changes`:

| What the stream shows | Recorded as |
|---|---|
| Same table, new name or new schema | `RENAME TABLE` (one row under each name) |
| Same table, other columns | `ALTER TABLE`, listing the columns added, dropped and retyped (a renamed column reads as one dropped plus one added, and the text says so) |
| A new table under a name another table held, the old one gone | `DROP TABLE` then `CREATE TABLE` |
| A new table under a name another table held, the old one still there under another name | `RENAME TABLE` for the old one, then `CREATE TABLE` |
| A table that was not in the publication when capture started | `CREATE TABLE` (labelled when it is a partition), or `ALTER PUBLICATION` when the catalog shows it was added to the publication after it existed |
| A table taken out of the publication and added back | `ALTER PUBLICATION`, marked `gap=uncaptured`: its changes while it was out were never captured, and `reconstruct` and the snapshot-pair `verify` refuse a window across it |
| A `TRUNCATE` | `TRUNCATE TABLE` |

Nothing is installed in your database for this; it is read from the stream
alone.

**Which rows belong to which shape is exact.** PostgreSQL decodes every
change with the table as it was when that change was made, so a row is never
assigned to the wrong set of columns.

**When the change happened is a range, not an instant.** The stream says the
change happened after the last change made under the old shape and at or
before the first one under the new shape, nothing finer. Each inferred row
starts with a marker carrying that range, for example:

```
/* dbtrail: inferred from relation shape; oid=16411; instant=at-or-before; low=0/16B3748@2026-10-17T10:00:00Z; high=0/16B9A20@2026-10-17T10:20:00Z */ ALTER TABLE shop.orders: added note (text)
```

**The exact instant, when the server keeps commit times.** With
`track_commit_timestamp = on` (a server setting that needs a restart; on
managed PostgreSQL, check that your provider lets you set it; `bintrail-pg
doctor` says whether it is on), bintrail-pg reads from the catalog when the
statement that gave the table the shape the stream shows committed, and the
marker says `instant=exact; at=<time>`, the range still beside it. When several
changes fall in one range (a column added, then retyped), that is the last of
them. It does so only where the catalog can prove it:

- an added column, or a retyped one whose change rewrote the table, when the
  column's catalog row and the table's still agree and the column has no
  column-level grant (a later statement on that column, such as `SET
  STATISTICS` or `SET NOT NULL`, or on the table, such as a `GRANT`, moves one
  of them; a retype that does not rewrite the table, such as `varchar(10)` to
  `varchar(20)`, is never exact);
- a dropped column, whose catalog row is never written again;
- a rename or a schema move, from the table's row type, which a later `GRANT`
  or column change does not move;
- a new table, from its row type, when the table's earliest column carries the
  same time.

Not caught, and read as the change's time: an `ALTER ... OWNER TO` later in the
range for a rename, and one later statement that rewrites both a changed
column and the table (a multi-part `ALTER`). The catalog must also show the
table with the columns the stream does (when bintrail-pg is catching up past a
later change, it does not), and the time must lie inside the range. Anything
else stays "at or before". A renamed column is never exact. Reading the times
uses a connection of its own and never stops capture: if the catalog cannot
answer, bintrail-pg logs a warning (at most every ten minutes, with how many
reads failed) and dates the rows by range.

The row's `detected_at` is the exact instant when there is one; otherwise the
start of the range (the end, when the range has no start). Its stream position
is the end. `reconstruct` places an inferred row by its range either way. `reconstruct` refuses a point in
time whose window overlaps the range of an inferred `DROP TABLE` or `RENAME
TABLE`, since the table may or may not have changed yet at that point; the
refusal says so and says which points in time can still be rebuilt. A
`TRUNCATE TABLE` is reported by PostgreSQL with its exact transaction, so it
has no range and refuses exactly where it happened. A table that was not written
to for a while after the change has a wide range.

**Tables added to the publication.** A table added to a `FOR TABLE`
publication has a row in the catalog written when it was added, and with
`track_commit_timestamp = on` bintrail-pg reads its time. A table first seen
after capture started whose catalog row is later than the table itself is
recorded as added (`ALTER PUBLICATION`), not created; its changes from before
are outside what capture covers, like the changes before capture started. A
known table seen again after it was added back is recorded with the gap, so a
rebuild that would need the changes made while it was out refuses instead of
missing them. A table that no change has shown yet is caught too when it was
in the publication when capture started and its row is later than that. When
the catalog shows an add later than the change being read (bintrail-pg is
behind), the stretch is refused as well, since the table may also have been
out before. Changing a table's row filter or column list (PostgreSQL 15+)
rewrites the same row and reads the same way; the row says so. Not seen: a
table added back while `track_commit_timestamp` is off, or while the catalog
cannot be read (bintrail-pg warns); a table no change had shown, taken out and
added back while bintrail-pg was stopped; a table taken out again and not yet
added back; tables covered by a `FOR ALL TABLES` or `FOR TABLES IN SCHEMA`
publication or through a published parent (they are not added one by one); and
a server clock stepped back across the add, since the add is placed by time.
The live-source `verify` compares with the source as it is now and does not
read these rows: a gap shows there as a difference.

**What cannot be seen.** Anything that changes neither a column's name or type
nor the table's name or schema: indexes, constraints, defaults, `NOT NULL`,
ownership, grants, comments, `SET LOGGED` / `UNLOGGED`. A `DROP TABLE` that is
not followed by a new table of the same name leaves no row: the table simply
stops appearing. A table created while bintrail-pg was stopped is taken as
already existing when capture starts again, so it gets no `CREATE TABLE` row.
On the first restart after upgrading to a version that records table
identities, a table whose stored shape predates that is matched by name: if
its columns differ, the change is recorded as an `ALTER TABLE` whose text says
it could also have been a drop and a create, and `reconstruct` does not refuse
on it the way it does on a `DROP TABLE`. The statement text and the role that
ran it are never available.

**If detection cannot start.** What detection needs is read when capture
starts. If that read fails (for example a very slow index database),
bintrail-pg captures rows as usual and does not detect schema changes until
its next restart, and logs that at ERROR level. The table shapes seen during
that run are stored without their position in the stream, so the next start
compares against the last shapes detection did see, and a change made in
between is still found then (its range covers the whole run).

**After a restart.** PostgreSQL resends changes from the point it last
confirmed, which can be earlier than what bintrail-pg had already recorded.
Each table description is compared with the shape that table had just before
it in the stream, so a resent stretch is read exactly as it was the first time,
and a change already recorded is not recorded again.

**Changes that look like DDL but are not.** Narrowing a publication's column
list (`ALTER PUBLICATION ... SET TABLE t (a, b)`, PostgreSQL 15+), or on
PostgreSQL 18 turning `publish_generated_columns` on or off, changes the
columns the stream carries without changing the table. It is recorded as an
`ALTER TABLE`.

**Where names come from during catch-up.** When bintrail-pg is behind and a
name moved to a new table, whether the old table was dropped or renamed (and
to what) is read from the database as it is now, which can be later than the
point being processed. The row says so.

## Managed PostgreSQL

Logical replication works on managed offerings that expose it; the only setup
difference is *how* you set `wal_level` and grant replication.

| Provider | Validated? | `wal_level = logical` | Replication privilege |
|---|---|---|---|
| **Amazon RDS for PostgreSQL** | **Smoke-validated** (PostgreSQL 16) | Set `rds.logical_replication = 1` in the parameter group, then **reboot**. (A parameter group set at *instance creation* applies at the initial boot — no reboot needed.) | Master user has it (`rds_replication`); grant others with `GRANT rds_replication TO <role>;`. |
| **Amazon Aurora PostgreSQL** | **Smoke-validated** (16-compatible, Serverless v2) | Same, in the **cluster** parameter group. | Same as RDS. |
| **Google Cloud SQL** | Documented only — **not validated** | Set the `cloudsql.logical_decoding` flag on, then restart. | Use a user with `cloudsqlsuperuser` / the replication grant. |

**Smoke-validated** means the full pipeline was exercised end-to-end against a
real instance of that flavor: `bintrail-pg doctor` passes, `stream` creates its
slot and captures INSERT/UPDATE/DELETE into `binlog_events` with full
before/after images, and `recover` generates a correct reversal. The smoke is
`scripts/managed-pg-smoke.sh` (it runs against *any* PostgreSQL DSN — the
managed part is the provisioning), and a gated CI job
(`.github/workflows/managed-pg-smoke.yml`, `workflow_dispatch`-only) provisions
an ephemeral RDS or Aurora instance, runs it, and tears everything down.

Cloud SQL is *expected* to work — bintrail-pg is an ordinary
logical-replication client — but it has **not** been smoke-validated, so its
row is setup documentation, not a support claim. Extending the validated
matrix follows demand ([#535](https://github.com/dbtrail/dbtrail/issues/535)).

Two observations from the RDS/Aurora validation runs: the master user captures
out of the box (it already holds `rds_replication`), and both flavors default
`max_slot_wal_keep_size` to `-1` (unlimited) — `doctor` WARNs about it; set a
bound as you would for any production source.

In all cases bintrail-pg connects as a **client** — there is nothing to install
on the managed instance beyond the publication and `REPLICA IDENTITY FULL` on
the tables to capture, which are plain SQL.

---

## Troubleshooting

> Tip: run `bintrail-pg doctor` to catch most of the rows below *before* they cost
> you a debugging cycle — it checks `wal_level`, publication coverage, replica
> identity, `max_slot_wal_keep_size`, and live slot health in one shot.

| Symptom | Cause / fix |
|---|---|
| `wal_level is "replica", must be 'logical'` | Set `wal_level = logical` and **restart** the server (a reload is not enough). |
| `publication "…" does not exist — create it` | Create the publication first (step 4) — bintrail-pg never creates it. |
| `publication "…" does not cover requested table(s) […]` | Your `--schemas`/`--tables` include tables not in the publication; `ALTER PUBLICATION … ADD TABLE …` (or widen the publication). |
| `table is not at REPLICA IDENTITY FULL; its changes are NOT captured` | `ALTER TABLE <t> REPLICA IDENTITY FULL` (each partition too, for a partitioned table); capture of the table resumes by itself, and its next full read brings the copy up to date (step 3). On PostgreSQL 18, read the generated-columns note in step 3 first. |
| `cannot undo a change in … was not captured, because the table was not at REPLICA IDENTITY FULL` | The window holds an `UPDATE` or a `DELETE` that version 0.138.0 recorded while the table was not at `FULL`. It cannot be undone (step 3). |
| `table's replica identity does not hold its primary key; its changes are NOT captured` | `ALTER TABLE <t> REPLICA IDENTITY FULL`; capture of the table resumes by itself (step 3). |
| `table has a primary key declared DEFERRABLE; its changes are NOT captured` | Make the key not deferrable (`ALTER TABLE <t> DROP CONSTRAINT <key>, ADD PRIMARY KEY (...)`), or take the table out of the publication (step 3). |
| `publication "…" applies a row filter or column list to table(s) […]` | A PG15+ `WHERE (...)` row filter or column list makes pgoutput emit only a subset of changes (silent data loss). Recreate the publication — or `ALTER PUBLICATION … SET TABLE …` re-listing the tables — without `WHERE (...)` / column lists. |
| `publication "…" does not publish […] — those changes would be silently lost` | The publication was created with a narrowed `publish` option (e.g. `WITH (publish = 'insert')`). Run `ALTER PUBLICATION … SET (publish = 'insert, update, delete, truncate')` (it keeps the publication's tables and other options), or recreate it leaving `publish` unset (the default publishes all operations). |
| `replication slot "…" is invalidated (wal_status=lost; max_slot_wal_keep_size exceeded)` | The source dropped WAL the slot still needed (slot was stopped too long). The data since the checkpoint is gone — `bintrail-pg reset` (drops the slot + clears the checkpoint), re-seed, and raise `max_slot_wal_keep_size` / keep the consumer running. `bintrail status` shows this as a loud `EVENTS PERMANENTLY LOST` banner. |
| `no table was read: replication slot "…" is still being created after 5m0s` | A snapshot was asked for while capture was creating the slot, and the slot did not get its position in 5 minutes: write transactions open on the source since before the creation began are holding it back (`pg_stat_activity`: the oldest `xact_start` among the rows with a `backend_xid`). Nothing was read. Run the snapshot again once the slot has a `confirmed_flush_lsn` in `pg_replication_slots`. |
| `replication slot "…" no longer exists: it was removed while the snapshot was read` | The slot the snapshot started from was dropped before the read ended, so the changes made since may not be captured. The snapshot was not kept. Run it again once capture is attached. |
| `the snapshot is not published: capture of this server started reading the source at …, after …` | The slot was replaced while the tables were read, and capture started from the new one. The snapshot was not kept. Run it again. |
| `the table's snapshot holds its changes from … on, and capture of the server started reading the source at …, after it` | The snapshot was taken before capture had its slot (possible before this check existed), so changes between the two were never captured. The update is refused; a scheduled update is replaced by a full read of the source, and a snapshot taken by hand (`snapshot run`) repairs it too. |
| `whether the table's snapshot starts before capture of the server did could not be checked` | The update could not read where capture started from the index (`stream_state`), within 5 seconds. The update is refused rather than letting the table through, because once rewritten a gap in it could not be seen again; a scheduled update is replaced by a full read, as for any failed update. Check the index is reachable and the account can read `stream_state`. |
| `resuming from a saved checkpoint but replication slot "…" no longer exists` | The slot was dropped while a checkpoint still pointed at it. Creating a fresh slot would skip data, so bintrail-pg refuses — `bintrail-pg reset --index-only` to clear the checkpoint and start fresh if the gap is acceptable. |
| `must include replication=database` (connection error on `--repl-dsn`) | Add `replication=database` to the `--repl-dsn` connection string. |
| Permission denied opening replication / creating slot | The role needs the `REPLICATION` attribute (`ALTER ROLE <r> REPLICATION;`), or on managed PG the provider's replication grant. |

---

## See also

- [Install](install.md) — all install methods and the env-file convention.
- [Streaming](streaming.md) — index requirements and the streaming model.
- [Query & Recovery](query-and-recovery.md) — querying history and generating
  reversal SQL (flavor-agnostic — same for PostgreSQL, MySQL, and MariaDB).
- [MariaDB as a source](mariadb.md) — the sibling source.
