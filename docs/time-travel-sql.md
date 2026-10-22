# Time-Travel SQL Setup

This walkthrough takes you from zero to running a working time-travel query against your MySQL:

```sql
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

The query is answered by `bintrail shim`, an in-process MySQL-protocol server (a subcommand of the `bintrail` binary) that intercepts the virtual `_flashback`, `_diff`, and `_snapshot` schemas and resolves them against your DBTrail index plus any rotated archives (local directory or `s3://` prefix). ProxySQL sits in front of both your real MySQL and the shim, routing each query to the right backend. The shim only cares that the index exists and `archive_state` is current — whatever keeps `binlog_events` populated (typically `bintrail stream`).

```
┌─────────────┐     :6033       ┌──────────┐    real query     ┌────────────┐
│ your app    ├────────────────►│ ProxySQL ├──────────────────►│ MySQL      │
└─────────────┘                 │          │                   └────────────┘
                                │          │  _flashback.*     ┌────────────┐
                                │          ├──────────────────►│ bintrail   │
                                │          │  _diff.*          │ shim       │
                                │          │  _snapshot.*      │ (:3308)    │
                                └──────────┘                   └────────────┘
```

## Three ways to run time-travel SQL

There are three ways to put `AS OF` SQL in front of your data; they differ only
in *how the client connects*:

1. **The MySQL port of `bintrail-console`: one port for every monitored
   server (multi-source).** If you already run `bintrail-console watch`, turn
   the port on in the web interface (**Connect a SQL client**). It serves
   `_flashback` / `_snapshot` / `_diff` for *every* server in the
   web interface, routed by the connection username. No separate `bintrail shim`
   process, no hand-built index DSN. See [the MySQL port](#the-mysql-port-multi-source)
   below. Start here if you run `watch`. This port also answers **ordinary
   SQL on the copy** (joins, aggregations, anything the console's SQL card
   takes): see [Ordinary SQL on the copy](#ordinary-sql-on-the-copy).
2. **A dedicated terminal: point `mysql` straight at a standalone shim (no
   ProxySQL).** Simplest for a single index. The shim already speaks the MySQL
   protocol, so an analyst connects a `mysql` client directly to it and runs
   `_flashback` / `_snapshot` / `_diff` queries. Trade-off: that connection
   answers *only* time-travel queries (a normal `SELECT` against a real table
   returns `ER_NOT_SUPPORTED_YET`, 1235; the standalone shim has no copy to
   read). Use it when a person or tool just needs to read historical state
   from one index.
3. **Transparent routing with ProxySQL in front (the rest of this guide).** Needed
   only when an application's *normal* connection must mix live queries and
   `AS OF` queries on the same endpoint. ProxySQL routes virtual-schema queries
   to the shim and everything else to your real MySQL.

All three speak the **MySQL** protocol. A **PostgreSQL** operator can instead run
single-row `AS OF` over the **PostgreSQL wire protocol** from `psql` with
`bintrail-pg flashback` (same grammar, no MySQL client required). See
[Interactive `AS OF` from `psql`](postgres.md#interactive-as-of-from-psql).

<a id="the-embedded-port-multi-source"></a>
### The MySQL port (multi-source)

DBTrail has one MySQL port: **3309**. One endpoint time-travels any server
you monitor, with no `bintrail shim` container, no `--profile flashback` and
no per-source DSN to discover.

**Who serves it.** A process of its own, the router (`bintrail-console
router`), and nothing else. The capture process (`bintrail-console watch`)
opens no MySQL port. In the Docker Compose stack the router is the `router`
service; in the deb and rpm packages it is the `bintrail-router` service.
Both start it for you. See
[The router](#the-router-the-process-that-serves-the-port) for the process
itself, and
[Upgrading](#upgrading-from-a-version-with-the-port-inside-watch) if you
used `--flashback-listen` before.

**Turn it on in the web interface.** On **Settings → MySQL clients**, the
**Connect a SQL client** panel has a **Turn on** button. Turning it on
creates the port's own password and shows it once (it starts with `bfp_`);
**New password** replaces it, and **Turn off** closes the port and
disconnects its clients. No flag and no restart. The router follows that
saved setting: off, it does not listen; on, it listens. The setting and the
password are kept in `console-mysql-port.yaml` beside the servers file, so
they survive a restart. That file holds the password as the client types it
(MySQL-protocol authentication needs it that way), readable only by the
user DBTrail runs as.

**The address.** The router listens on `127.0.0.1:3309` unless it is started
with another address (`--listen`, or `BINTRAIL_ROUTER_LISTEN`). The panel
does not ask for an address: its ready-to-copy `mysql` line carries the one
the router says it listens on. When the router cannot be asked, the line
carries the usual port, 3309, and the panel says it is not a confirmed one.
The Docker Compose stack publishes the port on the host loopback as 3309:
connect from that machine, or through a tunnel from another one.

**Engine Routing.** The same panel has its switch (**Turn routing on**, **Turn
routing off**), and shows who answered for the server picked in the sidebar:
how many statements MySQL answered and how many the copy did. See
[Engine Routing](#engine-routing-mysql-answers-the-copy-takes-the-heavy-reads).

**Routing is by username.** Connect as the target
server: its registry **ID** (robust; the `X-Bintrail-Server` value shown in the
web interface) or its display **name**. The password is the one created in
the web interface. If the router was started with an access token (`--token`
/ `BINTRAIL_CONSOLE_TOKEN`), the token works as a password too, while the
port is on. A token does not open the port by itself.

```sh
# the web interface shows each server's id/name in the switcher
mysql -h 127.0.0.1 -P 3309 -u 7f4d577430b48821 -p
mysql> USE myapp;   -- optional: seeded from the server's source DSN when known
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

<a id="ordinary-sql-on-the-copy-embedded-port-only"></a>
### Ordinary SQL on the copy
The same connection runs ordinary read-only SQL over the server's Parquet
copy, the snapshots and the archived change log, exactly as the console's
**SQL** card does: same locked DuckDB child process, same views, the same
2 GB of memory and 60 seconds per statement by default. Two things differ:
the row cap (10,000 rows on this port by default, where the card shows
1,000), and how many statements run at once (below).
A statement that is not time travel is handed to it:

```sql
mysql> SHOW DATABASES;                       -- the copy's schemas, plus main (the events view)
mysql> SHOW TABLES;                          -- the views in the current schema
mysql> SHOW COLUMNS FROM orders;
mysql> SELECT status, count(*) FROM orders GROUP BY 1;
mysql> SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id LIMIT 20;
mysql> SELECT count(*) FROM events WHERE table_name = 'orders';   -- only with the change log on local disk, see below
```

What to know before relying on it:

- **The SQL dialect is DuckDB's, not MySQL's.** The client is the transport;
  the statement is what the SQL card takes. `DATE_FORMAT` and
  backtick-quoted names are not understood; DuckDB's `strftime` and double
  quotes are. `GROUP_CONCAT` runs (DuckDB has it), and the order of its list
  can differ from MySQL's; `string_agg` is DuckDB's own spelling. `USE <schema>` works and sets
  where unqualified names resolve; the connection starts in the server's
  source database when the registry knows it. Schema and table names match
  without regard to the case of ASCII letters (`USE SHOP` and `FROM Orders`
  find `shop` and `orders`), which MySQL on Linux does not do by default
  (`lower_case_table_names=0`), so a statement that names a table in the
  wrong case works here and fails there. Letters outside ASCII are not
  folded: a schema `Été` is not found as `été`, and a `USE` of a name that
  is not found leaves the connection where it was.
- **`events` needs the change log on local disk, and the bundled stack keeps
  it in S3.** The capture process uploads each archived hour to the server's S3 location
  and removes the local file once the upload is confirmed, so on that stack a
  statement that reads `events` is refused with an error that says so, from
  the first uploaded hour on. (An hour whose upload is not confirmed yet is
  still on local disk; a folder with only some hours is not treated as the
  change log while S3 has them all, so `events` is still refused.) The tables are not affected. What to use
  instead: for one row's history, `_diff` on this same connection
  (`SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345`),
  which reads the archives in S3; for counts and grouping over the whole
  history, your own DuckDB reading the bucket, with a `views.sql` downloaded
  with **Include the change log** and **Works on another machine** ticked
  ([Dashboards](dashboards.md)). With Engine Routing on, an ordinary
  statement is planned on MySQL first, so `events` there means a table of
  that name on the source, never the change log.
- **Tested with the `mysql` command-line client.** A graphical client
  (DBeaver, Workbench) probes `information_schema` the MySQL way and may show
  an incomplete table tree; that has not been tested.
- **How many at once.** The router, which serves this port, works out how
  many statements run on the copy at once from the cores and the memory it
  may use ([The router](#the-router-the-process-that-serves-the-port)); its
  startup lines and `SHOW ROUTER STATUS` say the number. Statements for the
  same server can run at the same time. The SQL card of the web interface
  does not share these workers: it runs in the capture process, two
  statements at once by default (`watch --sql-max-in-flight`). A statement
  on the port waits for a worker for as long as its client stays connected:
  many clients at once are slower, not refused. A client that disconnects
  while waiting, or while its statement runs, leaves at once and frees its
  place. To refuse a statement that has waited too long, start the router
  with `--max-wait` (`ROUTER_MAX_WAIT` in `.env`, e.g. `30s`): past that
  time the client gets MySQL error 1203 ("SQL on the copy is busy"), or
  error 1040 with Engine Routing on (see
  [A busy copy](#a-busy-copy-the-read-waits-its-turn)). The wait is in the
  router's metrics: `bintrail_sql_slot_wait_seconds` and `bintrail_sql_slot_waiting`
  ([Observability](observability.md)). A
  statement past its memory (2 GB by default) goes on using a temporary
  folder of its own on disk, more slowly, up to four times its memory (8 GB
  by default) and never more than a share of the free space there, and fails
  only past that. The folder is under the system's temporary directory
  (`TMPDIR`), is removed when the statement ends; one a crash left behind is
  removed once it is ten minutes old, when the process starts or with a later
  statement. Where that directory is in
  memory (a `tmpfs`), statements do not spill and fail past their memory as
  before; the settings panel (Memory for SQL on the copy) says which. One whose tables have more than 384 MB of changes not merged into
  them yet is answered from the newest earlier copy in which they fit, with
  a warning (SHOW WARNINGS) naming that copy's time; with no such copy it is
  refused before it runs. Under Engine Routing it is not answered from an
  earlier copy: it goes to MySQL like any other refusal. A statement that
  reads `events` is not either, since the change log is not pinned to a
  copy. On a host with memory to spare, `--sql-memory`
  (env `BINTRAIL_CONSOLE_SQL_MEMORY`, at least 512MB) raises both: the line
  of changes moves with it, 768 MB at 4GB. Without the flag it can be set in
  the web interface (Settings, MySQL clients, Memory for SQL on the copy); the
  flag, when given, wins. The SQL card takes a saved change with its next
  statement, no restart. The router reads the saved value once, when it
  starts, so a change reaches the port when the router is restarted. The
  SQL card shows the memory and the line in force. Each statement can take that much at once, times
  the router's workers. For a team or a
  dashboard tool, each reader's own DuckDB on the bucket is the way to scale
  reads (see [Dashboards](dashboards.md)): it runs on the reader's machine and
  adds no load to the capture host. The trade-off: bucket permissions replace
  the console's access rules, and the data is as fresh as the last snapshot
  and the last archived hour. The router's log at debug level, the
  `phases_ms` field of the SQL card's response, and the
  `bintrail_sql_statement_phase_seconds` histogram say where each
  statement's time went.
- **Text compares close to MySQL's default collation, not DuckDB's.**
  `'Paid' = 'paid'` and `'café' = 'cafe'` are true, `GROUP BY` and `SELECT
  DISTINCT` fold them, `ORDER BY` sorts them together, and NULLs sort first
  on an ascending `ORDER BY` and last on a descending one, as on MySQL
  (`utf8mb4_0900_ai_ci`). So are `'ß' = 'ss'`, full-width letters,
  `'æ' = 'ae'` and hiragana against katakana, and punctuation sorts before
  the digits. Not folded: `LIKE`, `REGEXP`, `count(DISTINCT ...)`,
  `instr`/`position`/`contains`, and the duplicate removal of `UNION`,
  `INTERSECT` and `EXCEPT`. A column MySQL
  declares under a `_bin` collation compares byte by byte here too
  (`code = 'ab'` does not match `'AB'`, and it groups and sorts by code
  point; an `ENUM` or `SET` column is the exception in `ORDER BY`, where
  MySQL sorts it by the position of the value in the column's definition
  and the copy by the text); a `_cs` column is still case-insensitive here. The same applies
  to the SQL card; a DuckDB of your own over the same files (see
  [Dashboards](dashboards.md)) keeps DuckDB's defaults.
- **`SELECT *` returns a table's columns in the table's own order**, the
  one MySQL returns, and so do `t.*`, a star over a join and `SHOW COLUMNS`.
  The snapshot files hold the columns sorted by name, and the copy used to
  return them that way, so a client that reads a row by position got other
  columns with no error. The order comes from the `CREATE TABLE` stored with
  each snapshot, so it is the order the table had at its last full snapshot,
  a column added with `AFTER` or `FIRST` included. Three kinds of table still differ
  from MySQL, and each table's view says which applies to it (in the file
  `bintrail views --pin-snapshot` writes, as a comment above the view):
  - **A table whose snapshot carries no `CREATE TABLE`** (every table of a
    PostgreSQL source, a snapshot written by a version before 0.5, a file
    whose footer cannot be read) has no order to go by: `SELECT *` returns
    its columns sorted by name. Name the columns to read it by position.
  - **A generated column** (`GENERATED ALWAYS AS ... STORED` or `VIRTUAL`,
    and a MariaDB system-versioning period column) is not in a snapshot, so
    the copy's `SELECT *` has one column fewer than MySQL's and naming the
    column is an error here.
  - **An invisible column** (MySQL 8.0.23+, MariaDB 10.3+) is in the
    snapshot and has no such attribute here: the copy's `SELECT *` returns
    it, in its declared place, where MySQL's leaves it out.

  One more difference is the join's and not the table's: a star over a join
  written with `USING` or `NATURAL` returns the join's columns first on
  MySQL, and where the left table has them here.

  Under Engine Routing none of these reaches the client, because MySQL
  answers such a statement: see below. The SQL card has no routing: it
  answers from the copy, with these differences. A views file that follows later snapshots
  (the default of `bintrail views`, see [Dashboards](dashboards.md)) keeps
  the files' order, sorted by name, for every table.
- **`SET time_zone`, `SET sql_select_limit` and `SET sql_mode` are applied
  or refused, never ignored.** They used to be answered with an empty OK and
  read by nothing, so a client that had set its zone got answers computed in
  UTC. On a connection that runs ordinary SQL on the copy (not under Engine
  Routing, where every `SET` is MySQL's) each one is now handled, in every
  spelling (`SET time_zone = ...`, `SET SESSION ...`, `SET @@session....`,
  several assignments in one `SET`, a prepared `SET`), and `SELECT
  @@time_zone`, `@@sql_mode` and `@@sql_select_limit` answer what the
  connection set:
  - **`time_zone`** becomes the copy's session zone for that connection.
    `NOW()` and the columns that are instants (a MySQL `TIMESTAMP`, the
    `events` view's `event_timestamp` and `commit_time`) are printed in it,
    a literal compared with one is read in it, and a `DATETIME` column stays
    the wall clock MySQL holds, in any zone. Applied: a zone name
    (`'America/Argentina/Buenos_Aires'`, case as in the time zone database),
    a whole-hour offset from `'-12:00'` to `'+14:00'`, `'UTC'`, `'SYSTEM'`
    (the port's own zone, UTC) and `DEFAULT`. Refused with error 1298: an
    offset with minutes (`'+05:30'`; use the zone name, `'Asia/Kolkata'`),
    `'-13:00'` and beyond, and a name that is not a zone. (A name this
    port's time zone database has and the copy's engine does not is accepted
    at the `SET` and refused, by name, by each statement that then runs on
    the copy.) Under a zone other
    than UTC, a table whose snapshot does not record its column types (a
    snapshot written before column types were embedded in it, or a
    PostgreSQL source) is refused with an
    error naming the table, because its `DATETIME` columns cannot be told
    from its `TIMESTAMP` ones; `SET time_zone = 'UTC'` reads it again. A
    statement that may read any table (`SHOW TABLES`, a query of the
    catalog) is refused the same way while the copy has such a table;
    `SHOW DATABASES` always answers.
  - **`sql_select_limit = N`** cuts a `SELECT` that has no `LIMIT` of its
    own at N rows, with no warning: the client asked for the cut. A `LIMIT`
    in the statement takes precedence, as on MySQL, and `SHOW` and
    `DESCRIBE` are not limited. It never raises the server's row cap: with N
    above the cap, the cap applies as if nothing were set (see the row cap
    below). `DEFAULT` and
    MySQL's own "no limit" value (18446744073709551615) remove it; 0 is
    refused.
  - **`sql_mode`** is recorded and reported back; no mode changes how the
    copy computes (the dialect is DuckDB's). A mode that changes how a
    statement's text is read is refused with error 1231, alone or in a list:
    `ANSI_QUOTES`, `PIPES_AS_CONCAT`, `NO_BACKSLASH_ESCAPES`,
    `HIGH_NOT_PRECEDENCE`, and the combinations that contain them (`ANSI`,
    `ORACLE`, `MSSQL`, `DB2`, `MAXDB`, `POSTGRESQL`). So is a name that is
    not a mode. `REAL_AS_FLOAT`, `ONLY_FULL_GROUP_BY`, the strict modes and
    the rest are accepted.

  A `SET` with several assignments is all or nothing: one refused assignment
  refuses the statement and applies none of it. `SET GLOBAL` (and `PERSIST`)
  of the three is refused: the port keeps settings per connection. The other
  variables a driver sets as it connects (`SET NAMES`, `autocommit`,
  `character_set_results`) are accepted as before, and may share a `SET`
  with these three. So may anything else in a `SET` the port already
  answered with an empty OK (one that opens with `SET NAMES`, `SET SESSION`,
  `SET @@session.` or one of the three, the shape a driver's connect
  statement has): its other assignments stay connection chatter, read by
  nothing, and only a `SET` the port refused before is still refused.
  `sql_mode` may be given as a string or computed from the current one with
  `CONCAT` and `REPLACE` over `@@sql_mode` and quoted strings, as Rails and
  several ORMs send it; any other expression is refused. The time-travel
  shapes read and print times in UTC (an `AS OF` literal without a zone is
  UTC, see [Step 6](#step-6--run-a-time-travel-query)), so on a connection
  whose `time_zone` is not UTC a time-travel statement is refused with error
  1235 naming the zone, and `SET time_zone = '+00:00'` runs it again. The
  zone reaches the tables and the `events` view by name: a statement that
  reads a snapshot file directly (`read_parquet(...)`) under a zone other
  than UTC gets its `DATETIME` columns as instants, shifted by the offset.
  The standalone `bintrail
  shim`, which runs no ordinary SQL, accepts the three as connection chatter,
  as it always did.
- **Read-only, one SELECT per statement.** Anything else is refused with
  1064.
- **A result with more rows than the row cap is an error, not a short
  answer.** The statement fails with error 1104, which names the cap
  (10,000 rows by default) and the way out: add a `LIMIT` at or under the
  cap, or narrow the statement. Nothing is returned, so an application
  cannot take the first rows for the whole result. The cap is the port's
  own: `router --sql-port-max-rows` (env `BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS`,
  `SQL_PORT_MAX_ROWS` in the compose stack's `.env`) sets it, and the SQL
  card's 1,000 rows do not move with it. A result is also refused, with the
  same error code, past 64 MB, whatever its row count. The whole result is held by the
  router while it is sent, and takes a few times its size
  while it is prepared (measured: a 64 MB result of 100,000 rows, about 320
  MB at its peak). The number of workers bounds the statements running, not
  the connections receiving a result, so the port has a bound of its own:
  a result is counted only while the port holds less than 256 MB of results
  being sent, over all its connections, so it never holds more than that
  plus one result. While it holds that much, a statement is refused, before
  it runs when the port can tell, with error 1203 and "run the statement
  again in a moment"; under Engine Routing MySQL answers it instead
  (`copy_results_held` in the counter). The router's log says so, once a
  minute at most. A result stops counting when it has been written to the
  client, so a connection left open holds nothing. A client that reads
  nothing of an answer for 60 seconds is disconnected, as MySQL does at its
  `net_write_timeout`, and the log names its address; one that reads
  slowly is not. A result of exactly the cap's
  size is whole and is returned. The one cut that is not an error is the one
  the connection asked for: after `SET sql_select_limit = N`, with N at or
  under the cap, a `SELECT` with no `LIMIT` of its own returns its first N
  rows, silently. With N above the cap the cap still applies, and a result
  past it is the same error, which says the limit is above the cap. A
  listing (`SHOW TABLES`, `DESCRIBE`) takes neither a `LIMIT` nor the select
  limit, so its error points at `information_schema`, which does. (Under
  Engine Routing a result past the cap is not an error: MySQL answers that
  statement instead.) A cell longer than the cell cap (1 MiB) is cut and
  marked, and raises a warning the client counts; `SHOW WARNINGS` reports
  it. The console's SQL card keeps its own behaviour: it returns the first
  rows up to the cap and says on the page that there were more.
- **The copy has to be on local disk**, as for the SQL card. A server whose
  snapshots `watch` writes keeps them in its local folder. For a server whose
  snapshots are only read from S3, `bintrail-console watch` brings the newest
  ones to disk itself (see [SQL on a server whose snapshots are only on
  S3](console.md#sql-on-a-server-whose-snapshots-are-only-on-s3)); until the
  first one is there, ordinary SQL is refused with 1235 and says the local
  copy is not ready yet. A server with archive access disabled, or whose copy
  defines no view yet, keeps the time-travel shapes and refuses ordinary SQL
  with 1235 and the reason.
- **A current database the copy does not have** (the server's source
  database before its first snapshot, a typo in `-D`) is left alone:
  unqualified names then resolve in `main`, `SHOW DATABASES` and `events`
  keep working, and a name that fails to resolve says which database is
  missing from the copy.
- **Access is all or nothing.** The port authenticates on its own password
  (or the access token), which has no data profile and no table or column
  rules, so none apply here. It is the same rule the SQL card follows for a
  token session. Give this port to people who may read every table of every
  server.
- Every statement is written to the audit trail when one is installed
  (`shim` / `sql.run`: the statement, the schema, the row count).
- Column types are mapped to MySQL's (DuckDB `INTEGER` arrives as `BIGINT`,
  `DECIMAL(p,s)` as `DECIMAL`, `TIMESTAMP` as `DATETIME`, `BOOLEAN` as 1/0,
  a `LIST`/`STRUCT` as JSON text, `MAP`/`HUGEINT`/`UUID`/`INTERVAL` as text).

<a id="read-routing-mysql-answers-the-copy-takes-the-heavy-reads-experimental"></a>
### Engine Routing: MySQL answers, the copy takes the heavy reads
Engine Routing is off until it is turned on. Off, the copy answers every
statement on the port, as described above, and nothing is sent to MySQL.

Turn it on in the web interface: **Settings**, **MySQL clients**, **Connect a
SQL client**, **Turn routing on** (or `PUT /api/flashback` with
`{"routing": true}`). It is a saved setting, `routing: true` in
`console-mysql-port.yaml`, the file beside the servers file that also holds
the port's switch and password. The router reads it and applies a change within a few seconds, closing the
connections that were open
([The router](#the-router-the-process-that-serves-the-port)). No flag turns
it on: `--route-max-copy-age` did in earlier versions and now only prints a
warning.

With Engine Routing on, a
connection on this port to a server that has a source DSN and a
[forwarding account](#the-forwarding-account) forwards every statement to
that MySQL over one upstream connection per client connection, with that
forwarding account, and MySQL's own answer comes back, errors included. A
server with no forwarding account is not forwarded for: the account
DBTrail captures with is never used by this port, so the copy answers that
server alone. Resultsets are streamed to the client as they arrive, never held
in the router. The one exception is a `SELECT` whose plan says it is
expensive: it runs on the copy, and if the copy rejects it (DuckDB does not
know the syntax, the table is not in the copy, the result exceeds the
port's row or cell cap), MySQL runs it. A copy that is busy does not reject
it: the read waits its turn. It does not fall back onto MySQL because of load
([A busy copy](#a-busy-copy-the-read-waits-its-turn)). Nothing the client sends
needs to change. For what MySQL answers, the
behaviour is MySQL's; for what the copy answers, it is DuckDB's, and the
list below of where the two differ is what the feature's own testing is
still growing.

The port is read-only unless the router is started otherwise, so the
`UPDATE` in this example is refused with error 1290. Started with
`--route-read-only=false`, it goes to MySQL.

```
mysql -h 127.0.0.1 -P 3309 -u <server> -p shop
mysql> SELECT * FROM orders WHERE id = 42;                    -- MySQL (a point lookup)
mysql> SELECT status, count(*) FROM orders GROUP BY status;  -- the copy (a full scan)
mysql> SELECT GROUP_CONCAT(status) FROM orders;              -- MySQL (the copy would answer differently)
mysql> UPDATE orders SET status = 'paid' WHERE id = 42;      -- refused (read-only), or MySQL
```

One connection can ask for the copy alone while routing is on: connect as
`<server>@copy` (for example `-u shop@copy`).

The decision, in order, for every statement:

1. Not a `SELECT` (a write, `SHOW`, `BEGIN`, `SET`, `USE`, a client's
   connection chatter): **MySQL**. A `SET` does not keep the connection on
   MySQL: what it did to the session is read when a statement next heads
   for the copy (step 7). Three statements do, for the rest of the
   connection, because they change what later statements mean in a way the
   port cannot read back: `CREATE TEMPORARY TABLE`, `LOCK TABLES` and
   `PREPARE`.
2. Inside an explicit transaction (`BEGIN` ... `COMMIT`): **MySQL**, so a
   transaction reads its own writes.
3. A construct the copy would answer *differently* without an error
   (`GROUP_CONCAT`, `NOW()` and the session-time-zone family, `STR_TO_DATE`,
   `DATEDIFF`, `WEEK`, `YEARWEEK` and `EXTRACT(WEEK ...)`, a recursive
   CTE (`WITH RECURSIVE`), `COLLATE`, `CAST AS UNSIGNED`, `DIV`, `RAND`, user and system
   variables, locking reads, `information_schema`, full-text `MATCH`,
   JSON functions and the `->`/`->>` operators, a backslash inside a string
   literal, optimizer hints), and a backtick-quoted name the port will not
   rewrite for the copy (see "One thing is translated" below): **MySQL**.
   So are three spellings the copy always refuses, so that it is not tried
   in vain: `LIMIT offset, count`, `ORDER BY NULL` and `_binary'x'` (same
   section below). This step comes before the next two, so a `LIMIT 0, 20`
   the `LIMIT` rules would have kept on MySQL is counted as `veto`.
4. The one shape that needs no plan: `SELECT <columns> FROM <one table>
   LIMIT <at most 1,000 rows, offset included>` with nothing else (no
   `WHERE`, join, `ORDER BY`, `GROUP BY`, subquery or function call):
   **MySQL**, decided on the text, with no `EXPLAIN`. On a base table MySQL
   stops after those rows whatever the table's size; its plan cost, which
   ignores `LIMIT`, would say otherwise. The text cannot tell a view from a
   table: a view that aggregates or joins is built whole first, and is
   still MySQL's here.
5. `EXPLAIN FORMAT=JSON` on the source. First the `LIMIT` rule with the
   plan in hand: a top-level `LIMIT` of at most 1,000 rows with no
   aggregate, `GROUP BY`, `HAVING`, `DISTINCT`, window function, `UNION` or
   second `LIMIT`, whose plan sorts nothing (`using_filesort: false`),
   filters nothing while scanning (no `attached_condition` on a table read
   by scan) and examines at most 1,000 rows per table scan
   (`rows_examined_per_scan`, which unlike the cost does honour `LIMIT`):
   **MySQL**. That is `ORDER BY id DESC LIMIT 2` on the primary key (cost
   272,000, two rows examined) or a filter an index serves. A filter no
   index serves falls through: under a full scan its estimate is the whole
   table, and under an index-served `ORDER BY` the estimate is only the
   `LIMIT` over a guessed selectivity, while a rare value walks the whole
   index. The same `LIMIT`, with no sort, is also **MySQL**'s when the plan
   reads only its result, however many rows sit behind it: one table,
   read through an index by key or by range with nothing left to check
   row by row (or read whole with no condition at all). Every row the
   index hands over is then a row of the answer, so the read stops once
   the `LIMIT` is met. `SELECT * FROM orders WHERE created_at >=
   '2026-01-01' AND created_at < '2026-04-01' LIMIT 500` over 246,857
   rows in the range carries a cost of 544,000 and took 24 ms on MySQL 8.4
   (1.6 ms with `ORDER BY created_at`), against 65 ms on the copy. The
   router takes this only when it can read it off the plan: every
   condition is a column compared with a constant (`=`, `<`, `<=`, `>`,
   `>=`, `BETWEEN`, `IN`), joined by `AND`, on a column of the index the
   plan uses, with a range on the last of those columns only. A second
   condition on any other column, a function, arithmetic, `OR`, `<>`, an
   index on a prefix of a text column, a call to a function the router
   does not know returns one value per row (it could be an aggregate, which
   reads everything for one row), a join, a subquery, a view with a
   filter of its own and a partitioned table are not read, and are decided
   as before: with a condition no index serves that matches nothing, the
   same statement reads the whole range (400 ms). Then a plan whose `query_cost` is at least
   `--route-cost-threshold` (default 10,000; a point lookup costs about 1, a
   full scan over 200,000 rows about 20,000) or that has a full table scan
   over at least `--route-scan-rows` rows (default 100,000) is the copy's
   if step 6 agrees; anything cheaper: **MySQL**.
   On a **MariaDB** source the plan is read in MariaDB's own spelling
   (`rows`, a `filesort` node, the shortcut message on a table), and the
   cost rule does not apply: MariaDB before 11.0 reports no cost, and from
   11.0 on reports one in its own unit (a full scan over 200,000 rows is
   about 32, where MySQL says 20,000), which `--route-cost-threshold`
   cannot be compared with. The scan rule applies, and so does one more: an
   index walked end to end over at least `--route-scan-rows` rows is the
   copy's (an index scan inside a subquery is not counted: for `EXISTS` it
   stops at the first entry). And a third, for joins: MariaDB answers a
   heavy join by walking the small table and reading the big one by key,
   so no table of the plan shows a scan while the join reads millions of
   rows. The rows the plan reads **across its joins** are estimated from
   it and compared with `--route-scan-rows`: down a list of joined tables,
   each table's `rows` (the rows read each time the table is entered)
   times the rows the tables before it produce (their `rows` times
   `filtered`), added up. The joins inside a derived table, a materialized
   subquery or a subquery that does not depend on the outer row are added
   once (one that reads a single table adds nothing); a subquery that
   does depend on it (MariaDB puts it behind a subquery cache) and a
   lateral derived table are counted once per outer row; a join with no
   index counts every pair of rows it compares, and a hash join reads its
   table once. A table the server leaves at the first match (a semi-join
   with nothing more to test, an anti-join, the index probe of an `IN` or
   `EXISTS` subquery) counts one row per entry, and passes on at most one
   row for each row that came in. Orders joined to customers and grouped
   by country reads about 2,090,000 rows by this count and goes to the
   copy; a join whose first table an index cuts to a few rows reads tens
   and stays on MariaDB. What is not counted:
   - **One table alone**, however many rows it reads through an index (a
     `count(*)` over a one-year range of an indexed date stays on MariaDB,
     where MySQL's cost sends it to the copy). A table the optimizer
     reads before the plan starts (`const`, `system`: a lookup by a whole
     primary key) is a value, not a table of the join: a point lookup
     joined to one large read is still one table.
   - **A join a `LIMIT` ends early.** MariaDB does not cut the plan's rows
     for a `LIMIT`. When the statement ends in a `LIMIT` (offset included)
     below `--route-scan-rows`, with no aggregate, `GROUP BY`, `DISTINCT`,
     window function or `UNION`, and the plan has no sort or temporary
     table over the join, the join stops after about that many rows and
     its estimate is left out: the scan rules decide alone. `LIMIT ?` in
     a prepared statement is a bound the router does not know, and is
     treated the same way. A sort of the first table alone, before the
     join, reads that table whole: its rows plus the `LIMIT` are compared
     with the threshold. A `LIMIT` inside a derived table is not seen:
     MariaDB's plan does not carry it, and that derived table's join is
     counted whole.
   - **A plan shape the estimate does not know.** A recursive CTE is the
     one known today. The scan rules alone decide it, and the reason in
     the debug log says the rows were not estimated and why.

   The estimate is the optimizer's, with one correction. For a filter no
   index serves on a table read whole, MariaDB reports that every row
   passes (`filtered: 100`; it has no statistics on the column). Taken
   as is, a table of 5,000 rows scanned under such a filter and joined
   by key to 20 rows each would count as 105,000 rows read, whatever the
   filter keeps. The router assumes a tenth of the rows pass, which is
   the guess MySQL makes for the same filter; the table's own rows are
   still counted in full. A filter that keeps more than a tenth is then
   undercounted: a join of 420,000 rows behind a filter that keeps a
   fifth stays on a MariaDB 10.11 source, which also reports about half
   the rows per key that 11.4 does for the same index. For that last
   reason a join near the threshold can be routed differently by the two
   versions. A statement only the cost would have sent to the copy still
   stays on the source: for example an index walked in order under a
   `LIMIT` with a filter no index serves. The study of which statements the copy answers the same way
   (the vetoes of step 4) was run against MySQL; on a MariaDB source Engine
   Routing is less measured.
   Last, for a plan the copy should take, **the size of the result**. The
   copy returns at most its row cap (10,000 rows by default) and refuses a
   larger result, so a statement that returns more would run on the copy,
   be refused, and run again on MySQL. When the plan reads only its result
   (as above; here a sort is allowed) and the statement has no aggregate,
   `GROUP BY`, `DISTINCT`, window function or `UNION`, the plan's row
   estimate is an estimate of the result, cut by the statement's own
   `LIMIT` when it has one. Above the row cap: **MySQL**, and the copy is
   not tried (`result_over_row_cap`). The estimate comes from the index
   itself and was off by 0.6 to 3.2 times in our measurements, in both
   directions; that is harmless here, because in this shape MySQL reads
   exactly the rows it returns: a result that turns out small was a small
   read. Where the result is not the rows read, nothing is assumed and the
   copy is tried as before: an aggregate over a million rows returns one
   row, and a filter no index serves is estimated by a fixed guess (MySQL
   put 221,203 rows on a statement that returned one). A join, a `GROUP
   BY` with many groups and a full scan under a filter can therefore still
   be tried on the copy, refused for size and run on MySQL
   (`copy_refused`). A connection that set `sql_select_limit` at or under
   the cap asked for the cut, and the copy answers it as before. On a
   **MariaDB** source this is seen for a table or an index read whole.
6. The age of the copy is not checked. A plan the copy should take goes on
   to the next step whatever the age of the copy's snapshot (see
   [No maximum copy age](#no-maximum-copy-age) below).
7. The session. MySQL answers a statement under the connection's session
   settings (its time zone, its SQL mode and so on); the copy answers the
   same only when it runs under the same ones. So before a statement goes
   to the copy, the port must know the source's session on that connection.
   It asks MySQL for it (one extra round trip, on the same connection to
   the source) when it does not know: on the first such statement of a
   connection, and on the first one after any statement that could have
   changed the session, which is every statement that is not a plain read
   (a `SET` of anything, a write, `CALL`, transaction control, a statement
   the port does not recognise), and, where MySQL reports its session
   changes (a stored function, below), a read that MySQL itself said
   changed one of these settings or a statement that failed there. A cheap
   statement never pays for it, and
   neither does a run of statements the copy answers. The question is never
   sent on the heels of a `SET` or a write: it goes out just before a
   statement that is headed for the copy (or before a time-travel
   statement, below), so `FOUND_ROWS()`, `ROW_COUNT()` and `SHOW WARNINGS`
   sent right after a statement read what MySQL has for that statement.

   If the copy reproduces the session, it runs under it: **the copy**, sent
   as written except for backtick-quoted names, which go in double quotes;
   the copy refusing it means **MySQL**. If it does
   not: **MySQL**, for this statement and the next ones, until the session
   changes again. The connection is not kept on MySQL for good: put the
   setting back and the copy answers again. The reason is counted as
   `session_differs`, and DBTrail's log names the setting and its value
   once per connection (`Engine Routing: the copy does not answer on this
   connection ...`).

   What is read, and what the copy reproduces:

   | Setting | The copy answers under | Otherwise MySQL answers |
   |---|---|---|
   | `time_zone` | `UTC`; a whole-hour offset from `-12:00` to `+14:00`; a zone name, when MySQL's time zone tables, the zone data on DBTrail's host and the copy's own agree on it (below); `SYSTEM` when the source host is in UTC | an offset with minutes (`+05:30`: use `Asia/Kolkata`); `SYSTEM` on a host in any other zone (MySQL does not name that zone in a way the copy can be set to: set `time_zone` to a zone name on the connection, or as the server's default); a zone the three do not agree on |
   | `sql_select_limit` | any limit of 1 or more, applied to the copy's answer | `0` |
   | `sql_mode` | any combination of `ONLY_FULL_GROUP_BY`, `STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`, `NO_ZERO_DATE`, `NO_ZERO_IN_DATE`, `ERROR_FOR_DIVISION_BY_ZERO`, `NO_ENGINE_SUBSTITUTION`, `NO_UNSIGNED_SUBTRACTION`, `NO_AUTO_VALUE_ON_ZERO`, `NO_DIR_IN_CREATE`, `TRADITIONAL` (and MariaDB's `NO_AUTO_CREATE_USER`, `NO_FIELD_OPTIONS`, `NO_KEY_OPTIONS`, `NO_TABLE_OPTIONS`, `SIMULTANEOUS_ASSIGNMENT`), the empty mode included | any other flag: `PAD_CHAR_TO_FULL_LENGTH`, `HIGH_NOT_PRECEDENCE`, `REAL_AS_FLOAT`, `TIME_TRUNCATE_FRACTIONAL`, `ALLOW_INVALID_DATES`, `IGNORE_SPACE`, `ANSI_QUOTES`, `PIPES_AS_CONCAT`, `NO_BACKSLASH_ESCAPES`, `ANSI`, MariaDB's `EMPTY_STRING_IS_NULL`, `TIME_ROUND_FRACTIONAL`, `ORACLE` |
   | `lc_time_names` | `en_US` | any other locale (`MONTHNAME`, `DAYNAME`) |
   | `div_precision_increment` | `4` (the default). The copy's `/` and `AVG` still return more decimals than MySQL's there: that is the documented precision difference, not something this setting removes | any other value: it moves MySQL's answer further from the copy's |
   | `sql_auto_is_null` | `0` | `1` |
   | `sql_big_selects` | `1` | `0` (MySQL refuses large reads; with `max_join_size` at its default it is 1) |
   | `character_set_results` | `utf8mb4`; `NULL` or `binary` (no conversion: each column comes in its own character set and says so, as from the copy) | `latin1`, `utf8mb3` and the rest: MySQL converts, the copy answers in utf8mb4 |
   | the connection's collation (`SET NAMES ... COLLATE`, `collation_connection`), which decides comparisons between two literals (a column carries its own) | one that compares like `utf8mb4_0900_ai_ci`: no case, no accents, `ß` equal to `ss`, a full-width letter equal to the plain one. Measured: `utf8mb4_0900_ai_ci`, `utf8mb4_unicode_ci`, `utf8mb4_unicode_520_ci`, MariaDB's `utf8mb4_uca1400_ai_ci` and the `_nopad_` variants of those. Under the ones that pad (`unicode_ci`, `unicode_520_ci`, `uca1400_ai_ci`, MariaDB's default) one difference stays: `'a' = 'a '` is true on MySQL and false on the copy, as for a column under a PAD SPACE collation | `utf8mb4_general_ci` and `utf8mb4_general_nopad_ci` (`ß` equals `s`, a full-width letter is another letter), `_bin`, `_cs`, `_as_cs` |

   These are session settings whose value MySQL reads when it answers a
   `SELECT` the vetoes of step 3 let through. `group_concat_max_len` and
   `timestamp` are not in the table because step 3 already keeps
   `GROUP_CONCAT` and `NOW()` and its family on MySQL; `default_week_format`
   because `WEEK` and `YEARWEEK` stay there too. `max_execution_time` is not
   read: under it MySQL stops a long `SELECT` that the copy would answer.

   **Zone names.** MySQL converts times with its own `mysql.time_zone`
   tables, the port prints them with the zone data of the host DBTrail runs
   on, and the copy's engine computes with the data built into it. The
   three are updated at different times, and a country that changes its
   daylight saving puts them an hour apart for months. So with the session
   the port asks MySQL for the zone's offset from UTC at noon UTC of one
   day in every week, from the start of last year to the end of the year
   after next, and uses the zone only if its own data and the copy's give
   the same offset at each of them. It asks the same for January and July
   of every fifth year from 1970 to 2020, because zone data also differs on
   the past (measured: MariaDB 11.4's tables and the host's on `EET` in
   1975 and `WET` in 1970, the copy's engine and the host's on
   `Africa/Monrovia` before 1972), and under such a zone an old `TIMESTAMP`
   would print one hour off. Not seen: a difference that begins and ends
   inside one week, and a past rule that held for less than five years and
   missed both months. An offset (`+02:00`) needs none of this.

   **MariaDB 10.11.** The port asks the source for MySQL's default
   collation, `utf8mb4_0900_ai_ci`. MariaDB 11.4 knows it; 10.11 does not
   and would leave the connection in `utf8mb4_general_ci`, under which the
   copy does not answer. So on a new connection to a MariaDB source the
   port asks which collation the session got, and when it is not the one
   it asked for it sends `SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci`
   before anything of the client's: one extra statement per connection on
   MariaDB 11.4 and later, two on 10.11, none on MySQL (the statement that
   asks for session tracking, below, is one more on all of them). The copy then
   answers a connection that names no collation on every supported
   version. A client that sets a collation itself keeps it: `SET NAMES
   utf8mb4` with no `COLLATE` means `utf8mb4_general_ci` on 10.11, and
   MySQL answers that connection (the log says so, once, with the
   setting). Send `SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci` instead.

   **A setting changed inside a stored function.** A `SELECT` that calls a
   stored function is a read by its text, and the function can run `SET`
   (measured: a function sets `time_zone = '+05:00'`, and the next expensive
   read was answered by the copy five hours off). So the port asks the
   source to say so itself. On each new connection to the source it sends
   one statement, `SET SESSION session_track_system_variables = '...'`,
   naming the settings of the table above (and `max_join_size`, which is
   one of the ways `sql_big_selects` changes). From then on the source marks
   the answer to any statement that changed one of them, on the packet that
   ends the answer, which it sends anyway: a statement that changes nothing
   costs no extra round trip. After a marked answer the port reads the
   session back before the copy answers again, as it does after a `SET`.
   It does the same after a statement that failed on the source: an error
   says nothing about the session, and a function that ran `SET` and then
   failed has set it. The `EXPLAIN` of step 5 is heard the same
   way, because a server can run a function while it plans (MariaDB does,
   for a deterministic function with constant arguments). Measured on MySQL
   8.0 and 8.4 and MariaDB 10.11, 11.4, 11.8 and 12.3; the account needs no
   privilege for it.

   When the source does not do this (a server, or a proxy in front of it,
   that does not offer session tracking, refuses the statement, or does
   not pass the marks on), the connection works as it did before and this
   one change is not seen on it: do not change session settings inside a
   function there. The port finds out on every connection, from the answer
   to its own statement: a source that tracks marks that answer too.
   DBTrail's log says so once per server, at warn level (`Engine Routing:
   this source does not tell the port when a statement changes a session
   setting ...`, with the reason), and `GET /api/flashback` carries the
   reason as `session_untracked` for that server.

   A source, or a proxy in front of it, can also agree to session tracking
   and then send data about the session that the port cannot read. A
   connection that fails that way while it opens is opened once more
   without asking, and works as before; one that fails later is lost like
   any connection whose packets cannot be read (error 2006), and the
   client's next connection works. Either way the port stops asking that
   server for session tracking until DBTrail restarts, and says so in the
   same warning.

   A client that sets `session_track_system_variables` itself (some
   connectors do when they connect) replaces the port's list. The port
   sees it the next time it reads the session and, when any of its
   settings is missing from the list, adds them back to the client's,
   with one more statement.

   Two things the port still does not see. A stored function that changes
   `session_track_system_variables` itself, inside a `SELECT`: from then
   on the source no longer marks its answers on that connection. And a
   prepared statement keeps, on MySQL, the reading it got
   under the SQL mode in force when it was prepared; the copy runs each
   execution under the session of that moment. They differ only for a
   statement prepared under a mode that changes how it is read
   (`HIGH_NOT_PRECEDENCE`, `ANSI_QUOTES`, ...) and executed after the mode
   was set back.

A time-travel statement on a routed connection reads and prints its times
in UTC, so it runs only when the source's session on that connection is in
UTC (the port asks, as in step 7, when it does not know); otherwise it is
refused with error 1235 naming the zone, whatever the connection did
before, and `SET time_zone = '+00:00'` runs it. When the source cannot be
asked:

- The connection had a session on the source and it is lost: the statement
  gets error 2006, like every statement on that connection, and the client
  reconnects.
- The source never let the connection in (it is down, or the forwarding
  account is refused): the statement runs, under UTC. Nothing the client
  sent ever reached MySQL, so there is no session there to differ from, and
  time travel is answered from the index alone. This is what keeps time
  travel working while the source is down.

<a id="past-the-limit-tables-that-have-not-changed"></a>
#### No maximum copy age

There is no limit on how old the copy may be. With Engine Routing on, an
expensive read goes to the copy whatever the age of its snapshot. Earlier
versions had a limit (`--route-max-copy-age`) and, past it, a check of which
tables had changed since their snapshot. Both are gone.

The copy answers with what capture has indexed, not only with its last
snapshot. For each table a statement reads, the newest change of each row the
index holds after the table's snapshot (the table's tail) replaces the
snapshot's row, so the answer is about as far behind as capture, usually
seconds, plus about a second more: the tails are brought up to date in the
background, a second after the previous update ends, while statements read
them, and a statement reads the newest version ready instead of waiting for
the index. While an update is slow, the answers are older by as long as it
takes.
A statement reads every table at one moment: if one table it reads
has no tail, or tails stopped at different moments, the statement reads every
table as it was at its snapshot, so a join never pairs a row of now with a
row of an hour ago. A table has no tail when:

- its primary key is not made of integers;
- its definition changed since its snapshot, capture lost events since, or
  the source server changed;
- more keys changed since its snapshot than its budget holds, or the tails
  together hold more memory than theirs (`BINTRAIL_SQL_TAIL_MAX_KEYS`,
  `BINTRAIL_SQL_TAIL_MAX_BYTES`, below): its tail then stops advancing
  until the next snapshot;
- capture does not record it, or `bintrail index` also wrote the index;
- its tail is being built (the first statement over it, or the first after
  a newer snapshot of it that the tail cannot be moved to), until the build
  is in;
- the last update of the tails failed or ran out of time: then no table has
  one, for every statement, until an update succeeds;
- the statement is answered from an earlier snapshot, because the newest
  holds more unmerged changes than it can read.

A statement whose tables the port cannot tell apart (a `WITH` that reads
another `WITH`, for one) reads no tails either.

`BINTRAIL_SQL_TAILS=off` turns tails off, and the copy answers as it was at
its last snapshot. The console's own SQL card always reads the snapshot.

Two budgets bound the tails, because they live in the router's memory:

- `BINTRAIL_SQL_TAIL_MAX_KEYS` is the most keys one table's tail holds,
  500,000 when unset. A table whose changes since its snapshot pass it
  freezes its tail: statements keep reading the changes it holds, nothing
  newer, until the next snapshot is published and the tail is rebuilt. The
  router logs it as "a table's tail is over its budget" with the reason. A
  source that changes more keys than that inside one refresh interval wants
  a higher value, or a shorter interval.
- `BINTRAIL_SQL_TAIL_MAX_BYTES` is the most memory every tail of one
  server together may hold, by the router's own estimate: one fifth of the
  memory the process may use when unset (none when that is unknown), a size
  such as `2GiB` to set it, `0` or `off` for no bound. A router that serves
  several servers gives each its own share. The table whose fold would pass
  it is the one that freezes, with a reason that says memory, not keys; and
  while the frozen tails keep the total at the bound, every other table's
  tail that changes freezes too, until the next snapshot rebuilds them.
  The estimate counts the rows the tails hold, not every byte Go keeps
  around them, so it is a budget to size, not a ceiling the process cannot
  pass.

A value of either that cannot be read is warned in the router's log when the
port first reads tails, and the default stands.

This is the trade a read/write split through a proxy already makes: on one
connection a cheap statement is answered by MySQL as it is now, and an
expensive one by the copy, as far as capture has indexed. The copy is an
asynchronous replica. Its age is a number to watch, not a rule:

- `SHOW ROUTER STATUS` shows `copy_snapshot` and `copy_age_seconds` for the
  server of that connection.
- `bintrail_read_routing_copy_snapshot_timestamp_seconds` is the same number
  for Prometheus. Alert on `time()` minus it, the way replica lag is alerted
  on ([Observability](observability.md#engine-routing-bintrail_read_routing_decisions_total)).
- For the last statement on that connection, `SHOW ROUTER STATUS` also says
  who answered it (`last_answer`: `copy`, `mysql`, `refused`, or `none`
  before the first). When the copy answered, it shows the instant the data
  it read had been indexed through (`last_answer_indexed_through`) and how
  old that was when the statement finished (`last_answer_age_seconds`). This
  is what capture had indexed by then, not the source at that instant. When
  it cannot be told, both read `unknown`. The number can be older than the
  truth but never newer. Statements on the `_flashback`, `_snapshot` and
  `_diff` schemas are not counted here, as they are not in the statement
  counts.

If an application cannot read a result as far behind as capture, have it
connect straight to MySQL and not through this port.

What this is and is not:

- **The copy's answer is as fresh as capture, or as its snapshot.** A heavy
  read served from the copy sees what capture has indexed, and when one of
  its tables has no tail, only what the last snapshots hold, however long
  ago that was ([No maximum copy age](#no-maximum-copy-age) above).
  That is why Engine Routing is off until you turn it on. A `SELECT` that
  must see the last second belongs in a transaction, which MySQL always
  answers.
- **The copy's grants are nobody's; the forwarded ones are the forwarding
  account's.** Forwarded statements run with the server's forwarding
  account, never with the account DBTrail captures with. Give this port
  to people who may do on MySQL whatever the forwarding account is granted.
  To bound it, give that account only the grants the port should have and
  keep the port read-only, as it starts (both below).
- **One thing is translated: backtick-quoted names.** DuckDB does not read
  MySQL's backtick-quoted names, and most ORMs and drivers quote every name
  that way. So a routed statement reaches the copy with each
  `` `name` `` written `"name"`, which is how the copy quotes a name:
  ``SELECT `orders`.`id` FROM `orders` `` is sent as
  `SELECT "orders"."id" FROM "orders"`. Every other byte is the client's.
  A backtick inside a string literal or a comment is left alone. MySQL
  always gets the statement as the client wrote it. Nothing else of MySQL's
  dialect is translated: a function, an operator or a clause the copy does
  not have is still refused by the copy and answered by MySQL. That costs
  the failed attempt on the copy before MySQL answers (37 to 55 ms measured
  on a statement MySQL answers in under 1 ms), so three spellings that ORMs
  and drivers send, and that the copy always refuses, stay on MySQL without
  trying the copy, as text and as prepared statements. The counter shows
  them as `mysql` / `veto`: before, `copy_refused` when the plan was
  expensive, and `cheap_plan` or `bounded_limit` when MySQL answered
  anyway.
  - `LIMIT` with the offset first and a comma: `LIMIT 0, 20`, `LIMIT ?, ?`
    (what SQLAlchemy sends), in the statement or in a subquery. The copy
    only reads `LIMIT 20 OFFSET 0`, which is not kept back;
  - `ORDER BY NULL` (what Django sends after `GROUP BY`, to ask for no
    sort), also as `ORDER BY NULL, id` and `ORDER BY NULL DESC`. The copy
    refuses to sort by a constant. A window written `OVER (ORDER BY NULL)`
    or `OVER (PARTITION BY a ORDER BY NULL)` is not kept back: the copy
    answers it, with the same rows. `NULL` further down the list (`ORDER BY id, NULL`) and
    other constants (`ORDER BY 'x'`, `ORDER BY 1.5`) are not recognized:
    the copy refuses them and MySQL answers after the attempt, as before;
  - `_binary` before a string (`= _binary'x'`, how some drivers write a
    bytes argument). `LIKE BINARY`, `BINARY col` and `= BINARY 'x'` were
    already kept on MySQL. A cast (`CAST(col AS BINARY)`) is not kept
    back: the copy answered the same in every statement compared, and
    refuses a cast of a number, a date or text outside ASCII.

  The rewrite
  is not attempted, and the statement stays on MySQL without trying the
  copy, when it would be a guess:
  - a name that holds a backtick (written doubled, `` `a``b` ``) or a double
    quote (`` `a"b` ``), or an empty name (` `` `);
  - a quoted name right before a parenthesis (`` `sum`(x) ``): MySQL
    refuses a quoted `COUNT` and takes a quoted `sum` for a stored
    function, where the copy would call its own. A common table expression
    with a column list (``WITH `t` (`a`) AS ...``) has the same shape and
    stays on MySQL too;
  - a quoted name right before a string literal (`` `text` 'Label' ``):
    the column `text` under the alias `Label` on MySQL, the constant
    `'Label'` of type `text` on the copy, and both answer. The same holds
    for `` `int` '5' ``, `` `date` '2024-01-01' `` and every other name the
    copy has a type for. The unquoted spelling (`text 'Label'`) is kept on
    MySQL too, by the two rules for a word before a string further down;
  - a quoted name right after `U&` (two columns and an operator on MySQL,
    one Unicode-escaped name on the copy);
  - a string, a quoted name or a comment that never ends, and a comment
    with another `/*` inside it (MySQL ends it at the first `*/`, the copy
    at the matching one);
  - a carriage return inside a `-- ` comment that is not the end of the
    line (MySQL ends the comment at the line feed, the copy at the carriage
    return), MariaDB's executable comment `/*M! ... */`, and a `$...$`
    pair such as `$$` (a name on MariaDB and MySQL 8.0, the start of a
    dollar-quoted string on the copy; MySQL 8.4 refuses it). These three stay on MySQL with or without quoted names;
  - a double-quoted string or a backslash inside a string, which were
    already kept on MySQL. A double-quoted name under the source's
    `ANSI_QUOTES` is not recognized as one: it stays on MySQL as well.

  The names of the result's columns are the source's. Left to itself the
  copy names a column its own way: an expression with no alias by its own
  text (`count_star()` where MySQL says `COUNT(*)`, `(count_star() + 1)`
  for `count(*) + 1`), a column as the table stores it where MySQL repeats
  the statement's spelling (``SELECT `ID` `` is `ID` on MySQL and `id` on the
  copy), a string literal with its quotes. A program that reads a result by
  column name would then find one name or another depending on who answered.
  So for a statement the copy answers, the port asks the source how it names
  the statement's columns (it prepares the statement there, which runs
  nothing, and closes it) and sends the copy's answer under those names. A
  prepared statement already has them, from its own `PREPARE`. The question
  costs one more exchange with the source, only for statements the copy
  answered, and has a time limit of its own. If the source
  cannot say (it refuses to prepare the statement, or counts other columns
  than the copy returned), the answer keeps the copy's names and the log
  says so, once per connection. The plain port without Engine Routing
  translates nothing: a client there writes the copy's dialect and reads
  the copy's names.
- **Where the copy answers differently without an error.** The veto list
  keeps the known cases on MySQL (`GROUP_CONCAT`, the `NOW()` family,
  `LIKE`/`REGEXP`, `COLLATE`, `DIV`, `||`, `^`, double-quoted string
  literals, a backslash inside a string literal (an escape on MySQL, a
  plain character on the copy: `'a\\b'` and `'it\'s'` name different
  strings), `--` with no space after it (two minus signs on MySQL, where
  `5--3` is 8; a comment on the copy), a `#` comment (a comment on MySQL; on the copy `#2` is the table's second column), `count(DISTINCT ...)`, `INSTR`/`LOCATE`,
  `UNION`, `INTERSECT` and `EXCEPT` when they remove duplicates (the copy
  compares the rows by bytes there, so `SELECT 'a' UNION SELECT 'A'` is one
  row on MySQL and two on the copy; `UNION ALL` is not kept back),
  variables, ...). Five more shapes are kept on MySQL, for text and
  prepared statements alike; each was measured on MySQL 8.4 and MariaDB
  11.4 against the copy:
  - a word that starts with a digit and is not a number. `0x10` and `0b101`
    are a hexadecimal and a bit literal on MySQL, and `2fa` or `1_000` can
    be the name of a column; the copy reads the digits as a number and the
    rest as its alias, so `SELECT 0x10` is `0` in a column named `x10`
    there and `SELECT 2fa FROM users` is the number 2 in a column named
    `fa`. The same goes for an underscore inside a number, which the copy
    takes for a digit separator (`1.5_5` is 1.5 under the alias `_5` on
    MySQL and 1.55 on the copy), and for a decimal that ends in a bare `e`
    (`1.5e`: MySQL refuses it, the copy answers 1.5 under the alias `e`).
    Numbers (`10`, `1.5`, `1e5`, `1e-5`) and the same name in backticks
    (`` `2fa` ``) are not kept back;
  - `x`, `b` or `e` written right against a string: `x'41'` is a
    hexadecimal string and `b'1'` a bit string on MySQL, the texts `x41`
    and `b1` on the copy, and `e'x'` is the column `e` under the alias `x`
    on MySQL and an escaped string on the copy;
  - an `INTERVAL` whose amount the two sides read differently, in three
    spellings. A quoted amount that is not a whole number: MySQL cuts it at
    the first character that is not a digit and the copy reads a number,
    so `INTERVAL '1e2' DAY` is one day on MySQL and a hundred on the copy
    (`'1.5'`, which both cut to one, is kept back with the rest; `'1'`,
    `'-2'` and `' 12 '` are not). An amount in parentheses, or a
    placeholder: MySQL rounds it and the copy cuts it, so `d + INTERVAL
    (1.5) DAY` is two days later on MySQL and one on the copy, and a
    prepared `INTERVAL ? DAY` bound to the text `'1.5'` is two days on
    MySQL 8.4 and one on the copy. So a prepared statement with `INTERVAL
    ?` always stays on MySQL. And a quoted amount with a two-part unit
    (`INTERVAL '90' MINUTE_SECOND`): the copy has no two-part units and
    reads the unit as the column's alias (`d + INTERVAL '1:30'
    MINUTE_SECOND` is 00:01:30 on MySQL and 01:30:00 on the copy). A bare
    number with a one-word unit (`INTERVAL 1 DAY`, `INTERVAL 7 DAY`) is
    not kept back;
  - `~`: bitwise NOT over 64 unsigned bits on MySQL (`~1` is
    18446744073709551614), `-2` on the copy, where between two operands it
    is also a regular expression match;
  - a `+` or a `-` next to something the statement itself says is a date
    or a time: `DATE '...'`, `TIMESTAMP '...'`, `TIME '...'`, `DATE(...)`,
    or a `CAST` to `DATE`, `DATETIME` or `TIME`, with or without
    parentheses around it. MySQL
    turns the date into the number its digits spell (`DATE '2026-01-01' +
    1` is 20260102, and `DATE '2026-02-01' - DATE '2026-01-31'` is 70); the
    copy answers the date `2026-01-02`, one day, or an interval. The date
    may sit inside something that is added to as a whole: `GREATEST(DATE
    '...', d) + 1`, `(SELECT DATE(ts) FROM t) + 1`, `CASE WHEN a THEN
    DATE(ts) END + 1`, `MAX(DATE(ts)) OVER () - 1`. So a `+` or `-` next to
    any pair of parentheses or any `CASE ... END` that holds such a date
    keeps the statement on MySQL too, and so does `AVG` over one (`AVG` of
    a date or a time is a number on MySQL, 100000.0000 for ten o'clock,
    and a date and time or a time on the copy). That
    rule does not know what the group returns, so it also keeps back
    statements both sides would answer alike, such as `SUM(IF(d >= DATE
    '...', amount, 0)) - 1` or `YEAR(DATE '...') + 1`. A `+` or `-`
    elsewhere in the statement does not count (`WHERE d >= DATE
    '2026-01-01' AND qty + 1 > 2` goes to the copy), and a date plus or
    minus `INTERVAL` is not kept back. The same arithmetic on a column is
    not in the statement's text: the copy declines it from the column's
    type, see "A statement that does arithmetic on a date column" below;
  - `|`, `&`, `>>` and the functions `BIT_COUNT`, `BIT_AND`, `BIT_OR` and
    `BIT_XOR`. MySQL and MariaDB compute them over 64 unsigned bits and the
    copy over signed numbers: `-1 | 0` is 18446744073709551615 on MySQL and
    `-1` on the copy, `-8 >> 1` is 9223372036854775804 and `-4`,
    `BIT_COUNT(-1)` is 64 and 32, and `WHERE n | 0 > 0` keeps other rows.
    Over no rows `BIT_AND` is 18446744073709551615 and `BIT_OR` and
    `BIT_XOR` are 0 on MySQL, and all three are `NULL` on the copy. On
    numbers that are not negative both sides agree, and the text does not
    say what a column holds, so every statement with one of them stays on
    MySQL. `<<` is not kept back: where it would differ (a negative
    number, a result past 31 bits) the copy refuses the statement and
    MySQL answers;
  - `CAST(... AS DATETIME)` and `CAST(... AS TIME)`, with or without a
    precision. A value with more decimals of a second than the type keeps
    is rounded by MySQL, cut by MariaDB and kept whole by the copy:
    `CAST('2026-01-01 10:00:00.6' AS DATETIME)` is `10:00:01` on MySQL 8.4,
    `10:00:00` on MariaDB 11.4 and `10:00:00.6` on the copy, and a
    `DATETIME(6)` column under the same cast likewise. `CAST(... AS DATE)`
    is the same day on both and is not kept back, and neither is an alias
    written `AS time`;
  - a string written as a date with a two-digit year: two digits, `-` or
    `/` or a space, one or two digits, the same again, a digit
    (`'26-01-15'`, `'26/1/5'`, `'26-01-15 10:00:00'`). MySQL and MariaDB read the year as 2026 (00 to
    69 are 2000 to 2069, 70 to 99 are 1970 to 1999) and the copy as the
    year 26: `DATE '26-01-15'` is `2026-01-15` on MySQL and `0026-01-15` on
    the copy, and `WHERE created_on = '26-01-15'` finds the row on one and
    nothing on the other. Whether the string is used as a date is not
    read: any string written that way keeps the statement on MySQL, and so
    does one bound to a prepared statement. A year of four digits is not
    kept back;
  - `DAYOFWEEK`, `WEEKDAY`, `MICROSECOND` and `EXTRACT(MICROSECOND ...)`.
    The copy numbers the days of the week another way (`DAYOFWEEK` of a
    Thursday is 5 on MySQL and 4 on the copy, `WEEKDAY` of it 3 and 4), and
    its microseconds hold the seconds too (`MICROSECOND` of `10:20:30` is 0
    on MySQL and 30000000 on the copy);
  - `CURRENT_USER` written without parentheses, and `CURRENT_ROLE` with or
    without them: the user and the role on MySQL and MariaDB
    (`root@localhost`; `NONE` on MySQL and `NULL` on MariaDB for the role),
    the copy's own (`duckdb`) there. With parentheses `CURRENT_USER()`,
    `USER()` and the others were kept on MySQL already, and so were `NOW()`
    and every other clock function, written either way, so the copy's
    clock and time zone never answer for the source's;
  - the name of a type on the copy right before a string, with a space, a
    comment or nothing between them: `text 'Label'`, `json'1'`, `datetime
    '2026-01-01'`, `uuid '...'`, `bool '1'`. MySQL and MariaDB read the
    column, or the alias, called `text`, shown under the name `Label`; the
    copy reads the constant `'Label'` of type `text`, and both answer:
    `SELECT text 'Label' FROM (SELECT 'body' AS text) t` is `body` on MySQL
    and `Label` on the copy. The words are every type name the copy's
    engine has (a test asks the engine for the list, so a new type cannot
    be missed). `DATE '...'`, `TIME '...'`, `TIMESTAMP '...'` and `INTERVAL
    '...'` are not kept back: MySQL reads those as the copy does, also over
    a table with a column called `date`. Write the alias with `AS` to have
    the copy answer;
  - a word the copy keeps for itself and MySQL takes for a name, written
    as a name without quotes: `at`, `end`, `offset`, `full`, `any`, `some`,
    `cast`, `do`, `only`, `array`, `semi`, `anti`, `isnull`, `notnull`,
    `pivot` and about thirty rarer ones (44 in all; `window` and `lateral`
    among them are a name on MariaDB only). The copy cannot read
    such a name bare. For a column it refuses the statement (`WHERE at >=
    '2026-01-02'` over a column called `at` is a syntax error there), which
    used to cost a failed attempt on the copy before MySQL answered, on
    every such statement. In two places it does not refuse, it answers
    something else. `FROM a full JOIN b USING (id)` is the table `a` under
    the alias `full`, joined, on MySQL (the rows both tables hold) and a
    `FULL OUTER JOIN` on the copy (every row of both); the same for `semi`,
    `anti`, `asof` and `positional`. And `SELECT v isnull FROM t` is `v`
    under the alias `isnull` on MySQL and the test `v IS NULL` on the copy.
    All of these stay on MySQL now. The copy still answers when the name is
    quoted (`` `at` ``), comes after a name and a dot (`ev.at`; after a
    number the dot is a decimal point, and `SELECT 1. isnull` stays on
    MySQL: `1` there, `false` on the copy), or is the alias of a
    column right after `AS` (`SELECT made AS at`), and where the word is
    the keyword on MySQL too: before a parenthesis (`CAST(`, `= ANY (`),
    the `END` of a `CASE`, `OFFSET` before a number, `ROWS ONLY`, and
    `WINDOW w AS (...)`. One statement the copy might have answered stays
    on MySQL for it: `AT TIME ZONE`. A column named with a word both sides
    reserve (`order`, `group`, `left`, `desc`) is not part of this: MySQL
    only takes it quoted.

  The
  copy itself compares text close to the way MySQL's default collation
  does: `'Paid'` and `'paid'`, `'café'` and `'cafe'` are equal in `WHERE`,
  `GROUP BY`, `SELECT DISTINCT`, `IN` and `ORDER BY`, and NULLs sort first
  on an ascending `ORDER BY` and last on a descending one (DuckDB's
  `default_collation`, set to `nocase.icu_noaccent`, and
  `default_null_order`, fixed in the copy's locked session). That
  collation also equates what MySQL's does beyond case and accents
  (`'ß' = 'ss'`, full-width letters, `'æ' = 'ae'`, `'ø' = 'o'`, hiragana
  against katakana) and sorts as it does (punctuation before the digits):
  of 57 pairs of strings measured against MySQL 8.4 one differs (a
  mathematical bold `𝐀` is an `A` on MySQL and not on the copy), and a
  list of 48 strings sorts in the same order on both. Those numbers hold
  for those pairs and that list, not for all text: the differences found
  outside them are listed below. ICU, which provides it, is part
  of the binary; nothing is downloaded. It has a price: comparing,
  grouping or sorting text costs about twice what DuckDB's built-in
  case-and-accent folding does, and about ten times on a column where
  every value is different (5 million rows of 32-character tokens on
  disk: an equality filter takes 1.8 s instead of 0.2 s). Columns with
  few distinct values, numbers, dates and `_bin` columns are not
  affected. A column MySQL declares under a `_bin` collation
  (`utf8mb4_bin`, `utf8mb4_0900_bin`, `latin1_bin`, ...), by its own
  definition or by its table's default, is compared byte by byte on the
  copy as well: the copy reads each column's collation from the `CREATE
  TABLE` stored with the snapshot. Close, not identical. What still
  differs, none of which can be caught per statement:
  - **The copy can return MORE rows than MySQL for these texts.** The
    copy holds them equal and MySQL's `utf8mb4_0900_ai_ci` does not, so a
    filter or a join on them matches rows MySQL leaves out, and a `GROUP
    BY` merges groups MySQL keeps apart, with no error:
    - `l` or `L` followed by a middle dot, against `l` alone: `'l·l' =
      'll'`, so `'col·lecció' = 'collecció'`.
    - A Thai or Lao consonant and a leading vowel, in either order:
      `'กเ' = 'เก'`, `'ກເ' = 'ເກ'`.
    - A letter written in two parts against the same letter in one:
      Cyrillic `и` followed by a combining breve against `й`, Arabic alef
      followed by a combining madda or hamza against `آ`, `أ`, `إ`.
    - One Javanese pair, the vowel sign tarung (U+A9B4) against its long
      form (U+A9B5).
    - 86 combining and format marks that the copy ignores and MySQL does
      not, all added to Unicode after version 9: the Gujarati sign shadda
      (U+0AFB), the Bengali sandhi mark (U+09FE), the combining dot above
      left (U+1DF8). `'a' || chr(7672) = 'a'` here.
    - Characters added to Unicode after version 9, which MySQL's `0900`
      collations do not know and so give a weight of their own each: 1,463
      of them are equal to another character on the copy. A Georgian
      Mtavruli capital (U+1CA0) is equal to the ordinary Georgian letter
      (U+10E0) here and not on MySQL.

    A MariaDB source agrees with the copy on every one of these (measured
    on 11.4 under `utf8mb4_uca1400_ai_ci`); the difference is against
    MySQL only. It is not new either: the collation the copy used before
    this one (`nocase.noaccent`) was wider in this direction (it ignored 1,327 combining marks MySQL does
    not, the Indic vowel signs among them; of 75,900 strings of one or two characters
    tested, it held 25,725 equal to another one that MySQL keeps apart,
    against 2,613 now). The first two families are the ones it did not
    have.
  - **Equalities MySQL has and the copy lacks, beyond the 57 pairs.** The
    copy returns fewer rows for these, never more: a small kana against
    the normal one (`'ぁ' = 'あ'`, `'ッ' = 'ツ'`), enclosed and
    mathematical letters (`'🅰' = 'a'`, `'𝒜' = 'A'`), a symbol against its
    spelling (`'℃' = '°C'`, `'№' = 'No'`), and the katakana middle dot in
    full width against half width (`'・' = '･'`) are equal on MySQL and on
    MariaDB and different here.
  - **Han characters outside the main block sort differently.** MySQL
    and MariaDB put `𠀀` (U+20000) after `中`, the copy before: over
    `𠀀`, `中` and `z`, `max(x)` is `𠀀` on MySQL and `中` on the
    copy. `ORDER BY`, `MIN`, `MAX` and `<` on such text differ.
  - **`_cs` columns.** A column MySQL declares `_cs`
    (`utf8mb4_0900_as_cs`) is case-insensitive on the copy. Bytes would
    compare it right and sort it wrong: `_cs` puts `a` before `B`, bytes
    do not.
  - **Trailing spaces.** A column under a PAD SPACE collation ignores
    trailing spaces on the source, so `'bob ' = 'bob'` there and not here.
    On MySQL that is every collation older than the `0900` ones
    (`utf8mb4_general_ci`, `utf8mb4_unicode_ci`, `utf8mb4_bin`,
    `latin1_*`); the default, `utf8mb4_0900_ai_ci`, does not pad. On
    MariaDB it includes the default collation (`utf8mb4_uca1400_ai_ci` on
    11.4), so on a MariaDB source every text column that was not declared
    with a `nopad` collation differs this way.
  - **`ENUM` and `SET` in `ORDER BY`.** MySQL and MariaDB sort them by the
    position of the value in the column's definition (`enum('z','a')`
    gives `z`, `a`); the copy sorts the text (`a`, `z`).
  - **MySQL against MariaDB.** The comparisons above were measured on
    MySQL 8.4 and again on MariaDB 11.4 under its default collation. The
    two agree on each of those 57 pairs except the two that differ by a
    trailing space (the point above), and on the order of every one of the
    48 strings except the empty one against a single space. So on a
    MariaDB source the copy differs on 3 of the 57 pairs (the bold letter
    and the two that differ by a trailing space) where it differs on 1 on
    a MySQL source. That holds for those pairs, not for all text. One case
    outside them: a Hangul syllable written as one character (U+AC00) and
    the same syllable written as its two parts (U+1100 U+1161) are equal
    on MySQL and on the copy and different on MariaDB 11.4. Other
    characters written in parts (`e` followed by a combining acute accent
    against `é`, and the same for `Å`, `ñ`, a voiced kana) are equal on
    all three.
  - **`_bin` outside UTF-8.** A `_bin` column in a multi-byte character
    set other than UTF-8 sorts by that character set's bytes on MySQL and
    by Unicode code point here. Equality is the same.
  - **A collation changed since the last full snapshot.** A column's
    collation is the one it had then: a refresh carries the table
    definition forward, so an `ALTER` that changes a collation is seen at
    the next full snapshot.
  - **A table with no definition to go by.** A table whose snapshot file
    carries no `CREATE TABLE`, or one that cannot be read, has no
    collations to go by, so all its text columns fold case, `_bin` ones
    included, and its decimal columns read as text. That is every table of
    a PostgreSQL source, a MySQL or MariaDB table whose snapshot was
    written by a version before 0.5, and a file whose footer could not be
    read at that moment. The router's log says so once per table (`this
    table's snapshot file carries no CREATE TABLE`), naming the first ten
    at warning level and the rest in the debug log, and counts the files
    it could not read; a new full snapshot of a MySQL or MariaDB source
    records the definition, and an unreadable file is tried again within
    minutes.
  - **`AVG` and `/` return a double** on the copy, where MySQL returns a
    `DECIMAL` with four decimals more than the operand has (for `DECIMAL`
    and integer operands; a `DOUBLE` operand gives a double on both):
    `AVG(amount)` over a `DECIMAL(12,2)` is `1.8` on the copy and
    `1.800000` on MySQL, `ROUND(AVG(points), 1)` is `0` and `0.0`,
    `qty / 3` is `1.3333333333333333` and `1.3333`. The same value in a
    different text and under a different column type, exact to about 15
    significant digits on the copy. It stays this way because nothing is
    rewritten for the copy and a double carries no scale to print by.
    Division and modulo by zero are `NULL` on both.
  - **`CASE` and `IF` over a `DECIMAL` and an integer.** A `DECIMAL`
    prints as on MySQL, with its scale and trailing zeros
    (`ROUND(SUM(amount), 2)` is `117329550.00` on both), with one
    exception: a `CASE` or `IF` that mixes a `DECIMAL` branch and an
    integer branch prints the integer rows as `4.00` on the copy and as
    `4` on MySQL 8.4, which declares the column with two decimals and does
    not pad them. MariaDB 11.4 prints `4.00`, as the copy does.
  - **A `DATE` plus or minus `INTERVAL`, selected.** `created_on + INTERVAL
    1 DAY` over a `DATE` column is the date `2026-01-02` on MySQL and the
    date and time `2026-01-02 00:00:00` on the copy, and so is
    `DATE_ADD(created_on, INTERVAL 1 DAY)`. The same day: inside a `WHERE`
    it compares the same on both, and a client that reads the value as a
    date and time gets the same one. It is not kept on MySQL: a date plus
    an interval is how most reports write a range, and keeping it back
    would keep them all off the copy.
  - **A `DATETIME` or a `TIMESTAMP` column turned into text.**
    `CONCAT(dt, '')` and `CAST(dt AS CHAR)` are `2026-01-01 10:00:00` on
    MySQL and `2026-01-01 10:00:00+00` on the copy, which holds the column
    as a moment with a zone. A `DATE` column gives the same text on both,
    and `LEFT`, `SUBSTRING` and `DATE_FORMAT` over a date are refused by
    the copy, so MySQL answers those.
  - **A `DATETIME(6)` or `TIMESTAMP(6)` value, selected.** MySQL prints
    every decimal the column declares (`10:00:00.600000`) and the copy
    drops the trailing zeros (`10:00:00.6`). The same moment.
  - **A two-digit year the text does not show.** A string with a two-digit
    year keeps a statement on MySQL when it is written in the statement or
    bound to it. One that comes out of a column or of an expression
    (`CONCAT('26', '-01-15')`) is read as the year 26 by the copy.

  If a workload depends on one of these, keep the copy for the reads where
  they do not matter, or leave routing off.
- **`SELECT *` is the copy's only where it returns MySQL's columns.** The
  copy lists a table's columns in the order its `CREATE TABLE` declares
  them. Three kinds of table do not have MySQL's columns on the copy: a
  table whose snapshot carries no `CREATE TABLE` (so every table of a
  PostgreSQL source), a table with a generated column, and a table with an
  invisible column. Over such a table the copy declines, and MySQL answers
  (`mysql` / `copy_columns_differ` in the counter, apart from the copy's
  faults under `copy_refused`):
  - **a star that expands that table**: `SELECT *` or `t.*` over it, in the
    statement or in a subquery or derived table of it. A star over ANOTHER
    table of the same statement does not count (`SELECT l.*, g.id FROM lines
    l JOIN gen g ...` is the copy's when `lines` has MySQL's columns), nor
    does a star directly under `EXISTS (...)`, nor a star over a derived
    table whose own columns are named (`SELECT * FROM (SELECT id, a FROM gen)
    x`). `count(*)` is not a star. Where the port cannot tell which table a
    star expands, every table the statement reads counts;
  - **a `NATURAL JOIN` anywhere in a statement that reads that table**, with
    or without a star: it pairs on every column the tables share by name, so
    over other columns it returns other ROWS. Write the join with `ON` or
    `USING` to keep it on the copy.

  Whatever the tables, an unqualified star over a join written with `USING`
  or `NATURAL` is MySQL's: MySQL puts the join's columns first and the copy
  does not (`t.*` over such a join is the same on both). The same statement
  with its columns named can still go to the copy. The order is the one the
  table had at its last full snapshot: a column moved or added since by an
  `ALTER` is seen at the next one, and until then a statement with a star
  over that table can return the columns as they were declared at that
  snapshot.
- **A statement that does arithmetic on a date column is MySQL's, and so
  is one that names a `TIME` or a `YEAR` column.** The copy holds a `DATE`,
  a `DATETIME` and a `TIMESTAMP` as what they are, where MySQL and MariaDB
  turn one into the number its digits spell wherever a number is asked for.
  Both answer, with no error: `created_on + 1` over a `DATE` column is the
  number 20260102 on MySQL and the date `2026-01-02` on the copy;
  `MAX(created_on) - MIN(created_on)` is the difference of two such numbers
  (102 from January 1 to February 3) and a count of days (33); `AVG` of a
  date column is a number on MySQL and a date and time on the copy. The
  statement's text does not say that `created_on` is a date, so the copy
  reads each table's column types from the `CREATE TABLE` stored with its
  snapshot, and under Engine Routing it declines and MySQL answers
  (`copy_columns_differ`):
  - **a statement where the name of a `DATE`, `DATETIME` or `TIMESTAMP`
    column of a table it reads stands next to a `+` or a `-`, or under
    `AVG`** (or MariaDB's `MEDIAN`), directly or inside something that is
    added to as a whole: `created_on + 1`, `1 + o.created_on`,
    `(created_on) - 1`, `GREATEST(created_on, d2) + 1`,
    `LAST_DAY(created_on) + 1`, `MAX(created_on) - MIN(created_on)`, `CASE
    WHEN a THEN created_on END + 1`, `AVG(created_on)`, with the name quoted
    or not and with or without the table's name in front. A `+` or `-` that
    `INTERVAL` follows does not count, nor one elsewhere in the statement:
    `SELECT amount + tax FROM orders WHERE created_on >= '2026-01-01'` is
    the copy's. Neither does one around a call that is a number on both
    sides whatever it is given: `YEAR`, `MONTH`, `DAY`, `DAYOFMONTH`,
    `DAYOFYEAR`, `QUARTER`, `HOUR`, `MINUTE`, `SECOND`, `EXTRACT`, `COUNT`
    and `SUM` (each measured equal on MySQL 8.4 and MariaDB 11.4; the copy
    refuses `SUM` of a date). So `YEAR(created_on) * 100 +
    MONTH(created_on)`, `COUNT(*) - COUNT(paid_at)` and `SUM(CASE WHEN
    created_on >= ... THEN amount END) - SUM(amount)` reach the copy. Any
    other call counts, so `IFNULL(created_on, d2) + 1` stays on MySQL, and
    so does one both sides would answer alike. `*`, `/`, `%`, `ABS`, `ROUND`
    and a comparison with a number are not part of it: the copy refuses
    those over a date, and MySQL answers;
  - **a statement with a subquery, a derived table or a `WITH` that holds
    a `+` or a `-`, or an `AVG`, anywhere**, when it names such a column or
    has a star (`SELECT *`, `t.*`, `TABLE t`) that could bring one in. An
    alias of the date used from outside its subquery (`SELECT d + 1 FROM
    (SELECT created_on AS d FROM orders) x`, `SELECT AVG(d) FROM (...) x`)
    and a column list over a star (`WITH q(a, b, c) AS (SELECT * FROM
    orders) SELECT b - 1 FROM q`: 20260100 on MySQL, 2025-12-31 on the copy)
    are a date under a name the text cannot follow. Three kinds of `+` and
    `-` do not count here: one that `INTERVAL` follows, the sign of a number
    (`amount > -1`, `BETWEEN -5 AND 5`, `1e-5`), and one between two things
    that are numbers whatever the names in them mean (a number, or a call of
    one of the functions above: `SUM(a) - SUM(b)`, `YEAR(x) + 1`). `amount -
    tax` counts: either name could be the date;
  - **a statement that names such a column and has a counted `+` or `-` at
    or after its first `GROUP BY`, `HAVING` or `ORDER BY`**: MySQL takes a
    select-list alias for its expression there (`SELECT d1 AS x, d2 AS y
    ... HAVING x - y > 5`). `ORDER BY total - 1` stays on MySQL for that
    reason, whatever `total` is;
  - **a statement that names a `TIME` or a `YEAR` column of a table it
    reads**, anywhere. The copy holds a `TIME` as text, so `tm >=
    '9:00:00'` compares letters there and finds nothing where MySQL finds
    every row after nine, and a `YEAR` as a plain number, so `yr = 26` is
    not the year 2026 there. On a table with such a column that is most of
    what an ORM sends. A star over such a table (`SELECT *`, `t.*`, `TABLE
    t`) stays on MySQL as well: it reaches the column without its name, so
    `SELECT * FROM v ORDER BY 2` sorts the times as text on the copy (two
    negative times in the other order, `100:00:00` before `99:00:00`), and
    a column list over the star (`(SELECT * FROM v) q(a, b)`) gives the
    column another name to compare by;
  - **any statement that reads a table with a date, time or year column
    whose name is not made of letters, digits, `_` and `$` alone** (a
    space, a dot), and a statement that is not valid UTF-8; a column of a
    type DBTrail does not know is treated as a date could be.

  The names are looked for as whole words, the way MySQL compares the names
  of columns: the same letters up to case, in any script, with accents kept
  (`AÑO` is the column `año`, `ano` is not). So another table's column of
  the same name, an alias or a function called that (a `YEAR` column named
  `year` keeps every statement that calls `YEAR()` over its table on MySQL)
  keeps the statement back too, and a name outside ASCII that is not such a
  column (an alias `número`) keeps nothing back. A prepared statement is read by its
  template, so `created_on + ?` stays on MySQL whatever is bound. To count
  the days between two dates write `DATEDIFF`, which MySQL always answers.
  On the port without routing, and in the browser, the copy answers these
  as it always did, with its own values.
- **A statement that names a generated column is MySQL's.** A snapshot
  holds no generated column (`STORED` or `VIRTUAL`, invisible or not), so
  the copy does not have it. A statement that names one does not always
  fail on the copy: the name is taken by whatever else answers to it there,
  and the copy returns other rows with no error. Measured on the copy with
  `gen(id, twice GENERATED ALWAYS AS (id * 2), a)` and `g2(id, twice, b)`:
  `SELECT o.id, (SELECT twice FROM gen g WHERE g.id = o.id) FROM g2 o`
  returns `g2.twice` for every row; `SELECT a AS twice FROM gen WHERE twice
  = 2` filters on the alias; `SELECT twice FROM gen AS twice` returns the
  whole row as one value; and a generated column named `user` or
  `current_date` is answered by the function of that name. One table is
  enough for the last three. So, under Engine Routing, the copy declines and
  MySQL answers (`copy_columns_differ`):
  - **a statement whose text holds the name of a generated column of a
    table it reads**, anywhere: in the select list, `WHERE`, `ORDER BY`,
    `GROUP BY`, `HAVING`, a window, `USING`, a subquery, with or without
    the table's name in front. The text is searched, not parsed, so the name
    inside a longer word, a string or a comment keeps the statement on MySQL
    too (a table with a generated column called `a` sends nearly every
    statement over it to MySQL). A statement over the same table that does
    not hold the name is the copy's, alone or joined to other tables;
  - **a statement with any character outside ASCII that reads a table with
    a generated column**, in a name, a string or a comment: which accented
    or look-alike letters a server takes for an ASCII one when it compares
    names depends on the server, so such a statement is not searched;
  - **any statement that reads a table whose missing columns are not known
    by name**: a table whose snapshot carries no `CREATE TABLE` (a full
    snapshot taken before v0.5.0, every refresh that builds on one, since a
    refresh keeps the definition of the snapshot it started from, and a file
    whose footer could not be read at that moment), a
    file that does not hold exactly the columns its `CREATE TABLE` lists, a
    column whose definition could not be read (a name holding a backtick),
    and a generated column whose name is not plain ASCII letters, digits,
    `_` and `$`. Not known is never read as "nothing is missing". Such a
    table used to be answered by the copy whenever the statement had no
    star: it is MySQL's now, until a new full snapshot records its
    definition. The Connect page counts those statements under "the snapshot
    holds no table definition", and DBTrail's log names each such table
    once (`carries no CREATE TABLE`);
  - a MariaDB table created `WITH SYSTEM VERSIONING` answers to `row_start`
    and `row_end` whether or not it declares them, and the copy holds
    neither: a statement over it that holds one of those names is MySQL's;
  - `_rowid`, the other name MySQL and MariaDB give a key made of one
    integer column, is in no snapshot: a statement that holds it is MySQL's,
    over any table. So is one that holds `my_row_id`, the key MySQL
    generates for a table created without one
    (`sql_generate_invisible_primary_key`), over a table whose snapshot does
    not hold that column: with
    `show_gipk_in_create_table_and_information_schema=OFF` the server lists
    it in no definition;
  - **a statement that reads a table whose name differs from another
    table's only by letter case** (`Gen` and `gen`, on a source with
    `lower_case_table_names=0`): the copy does not tell the two names apart
    and would read one table for both;
  - **a statement with the name of a column of a table it reads right
    before a string**, with a space, a comment or nothing between them:
    `SELECT status 'Label' FROM orders`. MySQL reads the column under the
    alias `Label`; the copy reads a constant of a type called `status`, has
    no such type and refuses. The copy is no longer tried for it, and the
    statement is counted as a decision about the table's columns instead of
    a refusal by the copy. (When the copy does have a type of that name,
    the statement is kept on MySQL from its text: see the list of
    constructs above.)

  An invisible column is not part of this: a snapshot holds it, and both
  sides resolve its name the same way (only `SELECT *` and `NATURAL JOIN`
  differ, above). The port with routing off and the SQL card decline
  nothing: there the copy answers as it always did, and a statement that
  names a generated column either fails (`column not found`) or is answered
  as described here.
- **The thresholds are knobs, not truths.** The optimizer's cost is its
  own estimate; it misleads on cached data and on skewed values (and on
  `LIMIT`, which is why steps 4 and 5 read the statement, not the cost).
  Start with the defaults, read the router's log (every decision is logged
  with its reason at debug; a copy that never answers, a failing EXPLAIN or
  an unreadable snapshot time is a warning, once per connection) and the
  audit trail (a copy-served statement carries `route: copy` and the
  reason), and move the thresholds.
- **The copy has a fixed number of workers.** A heavy read that
  arrives while every worker is busy does not go to MySQL: it waits its turn
  on the copy. See
  [A busy copy](#a-busy-copy-the-read-waits-its-turn) below for the rule and
  its numbers. The limits of the section above are the copy's; MySQL's are
  MySQL's.
- **The port accepts writes under routing only when read-only is turned
  off.** The router is read-only by default. Started with
  `--route-read-only=false`, `INSERT`, `UPDATE`, `DELETE`,
  DDL, `GRANT`: everything that is not a `SELECT` reaches the source, as
  the server's forwarding account: anyone holding a password of the port
  can do on the source what that account is granted. Each forwarded write is
  logged at info level with its leading keyword (never the statement). Two
  settings bound this, and they are meant to be used together: the grants
  of the forwarding account, and `--route-read-only`, so a write is
  refused before it is sent (the next two points).
- <a id="the-forwarding-account"></a>**The forwarding account is the only
  account the port uses, and a server needs one to be routed.** It is a
  second account on the server's source, used only by this port.
  Everything the port does on that server's MySQL (forwarded statements,
  the `EXPLAIN` the decision reads, prepared statements, `USE`) runs as
  that account. The account DBTrail captures with is never opened by
  the port: it reads whatever capture needs, which with `SELECT ON *.*`
  includes the `mysql` schema and the password hashes of every account,
  and a credential handed out to read history must not reach that. So a
  server with no forwarding account is not forwarded for: the
  copy answers it alone, the light reads too, and `SHOW ROUTER STATUS` says
  so. A forwarding
  account saved as the capture account itself, or one that cannot be read,
  counts as none. Its grants are the most the port can do there:

  ```sql
  CREATE USER 'report_ro'@'%' IDENTIFIED BY <choose a password>;
  GRANT SELECT ON shop.* TO 'report_ro'@'%';
  ```

  Set it on the server: in the web interface, **Forwarding user** and
  **Forwarding password** on the server's edit form (to remove it, tick
  **Remove the forwarding account** there; a save that does not touch
  these fields leaves the account as it is); in the API, `route_user` and
  `route_password` on `POST` / `PUT /api/servers` (fields left out keep
  what is saved; an empty `route_user` removes it; a password left out
  keeps the saved one; `route_dsn` takes a whole DSN instead, for an
  account reached at another address). The user name cannot contain a
  colon, and it cannot be the account the server captures with: that
  would separate nothing. That check compares user and address as they
  are written (host names without case or a trailing dot, IP addresses by
  value, `localhost`, `127.0.0.1` and `::1` as one); it does not resolve
  names, so the same server written as a name in one place and as an IP
  address in the other is not recognised. A source reached over a unix
  socket cannot carry a forwarding account: statements are forwarded over
  the network.

  It connects to the source's own address and database, and follows the
  source when those are edited. Whether the connection is encrypted is
  not part of the account: the port uses the server's TLS settings, the
  ones capture uses, with either account. An account given as a
  `route_dsn` to another address keeps that address, and the edit form and
  the **Connect a SQL client** panel show it.

  **Saving, changing or removing the forwarding account closes that
  server's open connections on the port**, and so does any other edit
  that changes the address or account the port forwards with, or
  deleting the server. Clients see a lost connection at once, reconnect, and get the
  account in force. An edit that changes neither closes nothing.

  A statement that was running on the source when its connection was
  closed does not stop there by itself: the source only notices a closed
  connection when the statement next touches the network, so a long
  statement would run to its end as the previous account, and a write
  would complete. The port therefore ends them: right after the edit it
  opens one short-lived connection with the PREVIOUS account and sends
  `KILL` for the source thread of each connection it closed (an account
  may kill its own threads). This takes about a second, runs in the
  background, and never delays or fails the edit. It cannot be done when
  the previous account can no longer log in (its password was changed on
  the source first, or it was dropped or locked) or the source cannot be
  reached: the log then says so at warning level, and a statement that
  was running keeps running as the previous account until it finishes or
  the source notices the closed connection. To be certain in that case,
  look at the source's process list. A transaction that was open is
  rolled back by the source in every case.

  **Test connection** on the server's form logs in with the forwarding
  account too, the saved one or the one being typed, and names it in its
  answer. A console started as `serve` has no port and so no client to
  log in with: there the answer says the login was not tried. If the
  source later turns the port's login away (a changed password, a host
  that is not allowed, a locked account, on MySQL an expired password),
  clients get error 2006 on every statement, and the **Connect a SQL
  client** panel says, under **The router's numbers**, that MySQL refused
  the router's login for that server; the router's log names the account
  and the source's own error. A **Test connection** of that forwarding
  account, on the server's form, tries the login. An expired password on MariaDB is
  different: MariaDB lets the account log in and answers every statement
  with its own error 1820 ("You must SET PASSWORD before executing this
  statement"), which the port passes to the client as it is, so nothing
  is shown on the panel.

  The password is stored in the registry file like the source's and never
  shown again. MySQL and MariaDB sources only. The **Connect a SQL
  client** panel names the user the port runs statements as. If the saved
  value cannot be read (a registry file edited by hand), the port does not
  forward for that server and never falls back to the capture account;
  the form and the panel say so, and other edits of the server still
  save. The server given on the command line (`--source-dsn`) is not
  routed, so it has no such setting.
- **The port is read-only unless you say otherwise.** The router starts
  with `--route-read-only` on. A statement that is not a read is
  refused with MySQL error 1290 and a message that names the flag, and it is
  never sent to the source, not even to be explained or prepared. `SHOW
  WARNINGS` right after shows that refusal. To forward writes, start the
  router with `--route-read-only=false` (or
  `BINTRAIL_CONSOLE_ROUTE_READ_ONLY=0`; `ROUTER_READ_ONLY=0` in the Docker
  stack's `.env`). It matters only with Engine Routing on: with it off,
  nothing is forwarded at all.
  - **Allowed**: `SELECT`, `WITH ... SELECT`, `TABLE`, `VALUES`, `SHOW`,
    `DESCRIBE`, `EXPLAIN` (and `EXPLAIN ANALYZE`, or MariaDB's `ANALYZE`, of a
    read), `USE`, `BEGIN`,
    `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE
    SAVEPOINT`, `SET TRANSACTION`, and a `SET` of session settings and user
    variables (`SET NAMES`, `SET autocommit`, `SET SESSION sql_mode`,
    `SET @x = ...`), which covers what drivers send when they connect.
    Prepared statements (the binary protocol) of those work too.
  - **Refused**: everything else. `INSERT`, `UPDATE`, `DELETE`, `REPLACE`,
    DDL (`CREATE TEMPORARY TABLE` included), `GRANT`, `KILL`, `CALL`, `DO`,
    `HANDLER`, `LOCK TABLES`, `LOAD DATA`, `XA`, text `PREPARE` / `EXECUTE`,
    and any statement the port does not recognise; `SET GLOBAL`, `SET
    PERSIST`, `SET @@global.x`, `SET PASSWORD`, `SET ROLE` and `SET
    gtid_next`; a read that writes or locks (`INTO OUTFILE`, `INTO
    DUMPFILE`, `INTO @variable`, `FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE
    MODE`, `GET_LOCK`, a sequence's `NEXTVAL`); `WITH ...` in front of a
    `DELETE`, `UPDATE` or `INSERT`; `EXPLAIN ANALYZE` of a write (it runs
    it); a line with more than one statement; and any statement holding
    MySQL's executable comment (`/*!50000 ... */`), since the server runs
    what is inside. A statement with `--` right before a non-ASCII byte is
    refused as well: the server reads that as the start of a comment under
    `latin1` and not under `utf8mb4`, and the port cannot know which. A `SET NAMES` to a character set other than `utf8mb4`,
    `utf8`, `latin1`, `ascii` or `binary` is refused too: under `gbk` or
    `sjis` the port could not tell where a string ends.
  - **What it cannot see.** The check reads the statement's text. A
    `SELECT` that calls a stored function, or reads a view built on one,
    looks like a read and is forwarded; if the function writes, MySQL runs
    it. The mode screens by statement class; the grants of the account the
    port forwards with are what bounds the rest, which is what the
    forwarding account above is for. Refusals are counted as
    `route="refused"`, `reason="read_only"`, and the **Connect a SQL
    client** panel says which mode the port is in.
- **The port reaches the source over the TLS capture uses.** Its
  connection to a server's MySQL follows that server's `ssl_mode` (and
  `ssl_ca`, `ssl_cert`, `ssl_key`), the same settings capture connects
  with: by default (`preferred`) it is encrypted whenever the source offers
  TLS and falls back to an unencrypted connection only when the source
  offers none, with a warning in the log; with `required`, `verify-ca` or
  `verify-identity` it is encrypted or it fails, and the client gets error
  2006. A `tls=` parameter inside the source DSN (only a DSN written by
  hand has one) wins over `ssl_mode`, as it does for capture, and when
  `ssl_mode` demands encryption the log says once per server that the
  DSN decides instead. `tls=preferred` there means the same as the mode
  `preferred`: encrypted when the source offers TLS, the logged fallback
  when it offers none. A TLS setting that cannot be used (a misspelled
  mode, a CA file that cannot be read) turns routing off for that server,
  and the **Connect a SQL client** panel says so.
- **A lost connection to the source is not hidden.** If the upstream
  connection drops (the source closes an idle connection, a network error,
  the query deadline), every later statement on that client connection
  fails with MySQL error 2006 ("MySQL server has gone away") until the
  client reconnects. The port never reconnects on its own: that would be a
  new session with the transaction rolled back, the `SET`s gone and the
  database reset, while the client believes nothing happened.
- Forwarded statements run under the port's query deadline
  (`QueryTimeout`, 5 minutes by default), the same as a copy statement.
- `SHOW WARNINGS` after a forwarded statement is MySQL's; after a statement
  the copy served it is the copy's.
- A server with no source DSN (the capture process's own command-line
  source, a server registered without one), or whose copy is unavailable,
  keeps the copy-only behaviour and says so once per connection in the
  router's log.

Servers added in the web interface mid-session are reachable within a few
seconds (the router reads the servers file again when it changes). The port
takes a password of its own, created when the port is turned on in the web
interface: MySQL-protocol auth cannot use the web interface's password
store. The default `127.0.0.1` bind keeps it host-local; do not expose it to
untrusted networks.

**The port does not filter by schema.** Anyone with the port's password (or
the access token, when the router has one) can read
the full history of every schema on every monitored server through it. There is
no `allowed_schemas` here. When you need per-schema filtering, run the standalone
`bintrail shim` (or `bintrail-pg flashback`) instead, with one tenant per
application and an `allowed_schemas` list on each (see
[Isolating tenants with `allowed_schemas`](#isolating-tenants-with-allowed_schemas)).

The web interface shows the port on **Settings → MySQL clients**, in the **Connect a
SQL client** panel: whether the port is on, its address, the user and password
rules, and a ready-to-copy `mysql` line for the server picked in the sidebar.
When the port is off, the panel turns it on.

`_snapshot.*` parity: each server reads the baseline configured on its registry
entry, exactly as the
web interface's Time-travel tab does, with one edge: a server configured with *both*
a local `--baseline-dir` **and** an `--baseline-s3` copy reads `_snapshot` only
from the local dir on this port. If local baselines have been pruned (retention)
while a durable S3 copy remains, use the web interface's Time-travel tab or a standalone
shim pointed at the S3 prefix for those tables. Single-source baseline configs,
the common case, have full parity.

#### What the port tells a driver about its session and about the server
MySQL sends two bytes of status with its handshake and with every answer:
whether the session is in autocommit mode, whether it is inside a
transaction, and a few more. Drivers act on them. PyMySQL and mysqlclient
send `SET AUTOCOMMIT` only when the last status says the mode differs from
the one they want; Connector/J with `useLocalTransactionState` skips a
`COMMIT` the flags say is not needed; Connector/J, the C library and the Go
driver read `NO_BACKSLASH_ESCAPES` from them to decide how to escape a
string. The port's flags describe the session the client really has:

- **Under Engine Routing**, the session on the source, as the source last
  reported it, on every answer, whoever produced it. A `SELECT` the copy
  answered, a time-travel statement, a `SHOW WARNINGS` or `USE` the port
  answered itself and a `PING` all say "in a transaction" between `BEGIN`
  and `COMMIT`, and "autocommit off" after `SET autocommit=0`. (One packet
  is a statement behind: the end-of-columns marker in the middle of a result
  forwarded from the source carries the state from before that statement;
  the packet that ends the result carries the state after it.) The flags
  passed on from the source are autocommit, in a transaction, in a read-only
  transaction, `NO_BACKSLASH_ESCAPES`, and four about the statement itself
  (no index used, no good index used, slow query, database dropped). The
  flags that announce something the port does not deliver are never sent:
  more result sets, an open cursor, session-state data.
- **On a connection that is not routed**, autocommit and never in a
  transaction. The copy has no transactions: `SET autocommit=0` is accepted
  as connection chatter and changes nothing, `SELECT @@autocommit` keeps
  answering 1 and the flags keep saying autocommit.
- **A `PING` on a routed connection is answered by the source** once the
  connection has a session there (from its first forwarded statement on).
  Only the source knows whether that session is still alive, and its answer
  carries the state as it is now: after an `INSERT` the source refused with
  autocommit off, an error that carries no flags, the source has opened a
  transaction and the `PING` says so. Before the first forwarded statement
  there is no session on the source; a `PING` then opens none and is
  answered by the port ("autocommit, no transaction", which is what a
  connection that has run nothing holds). A `PING` costs one round trip to
  the source and is bound by the port's statement deadline.
- **Once the connection to the source is lost, every command answers error
  2006** ("MySQL server has gone away"), not only the forwarded ones: the
  time-travel statements, `SHOW WARNINGS`, `USE` and `PING` too. The source
  ended the session (a `KILL`, a restart, an idle timeout), or the port's own
  statement deadline cut a statement, and the transaction the client was in
  went with it. An OK from the port could only say "autocommit, no
  transaction", which tells a driver that there is nothing to roll back and
  a connection pool that the connection is healthy. Reconnect to continue;
  on a connection that is not routed nothing changes. A source that never
  let the connection in (unreachable, or the login refused) is a different
  case, because no session existed and nothing was lost: forwarded
  statements and `PING` answer 2006, and the time-travel statements, which
  need no source, keep answering. That helps only a client whose connect
  sequence forwards nothing: under routing every `SET` goes to the source,
  so with the source unreachable a driver that sends a `SET` when it
  connects (PyMySQL with its default settings, Connector/J, mysqlclient)
  gets 2006 and cannot finish connecting to a routed server. What works
  while the source is down: the `mysql` client, a driver that sends no `SET`
  when it connects, or a port started without Engine Routing.
- **The handshake** announces autocommit and no `NO_BACKSLASH_ESCAPES`, as
  MySQL and MariaDB do by default. It is written before the port knows which
  server the client asked for, so it cannot carry that server's state, and
  the port opens its own connection to the source only for the first
  statement it forwards. For a source configured to open its sessions in
  another state the announcement is therefore wrong, and the port's answers
  carry the real state from that first forwarded statement on. Whether that
  is enough depends on when the driver looks:
  - **PyMySQL looks once, when it connects**, and compares the flag with the
    mode it was asked for. Against a source that opens sessions with
    autocommit off, a PyMySQL connection opened with `autocommit=True` is
    told "already on", sends no `SET`, and the session on the source stays
    with autocommit off: **every write is discarded when the connection
    closes, with no error**. Measured with PyMySQL 2.2.8, `autocommit=True`,
    one `INSERT`, then `close()`:

    | Source opens sessions with autocommit off through | Connected directly | Through the port | Through the port, with `init_command="SET autocommit=1"` |
    | --- | --- | --- | --- |
    | MariaDB 11.4, `autocommit=0` in its configuration | row kept | **row lost** | row kept |
    | MariaDB 11.4, `init_connect='SET autocommit=0'` | row lost | row lost | row kept |
    | MySQL 8.4, `init_connect='SET autocommit=0'` | row lost | row lost | row kept |
    | MySQL 8.4, `autocommit=0` in its configuration | row lost | row lost | row kept |

    Only the first row is the port's doing: MariaDB's own handshake says
    "off" there, and the port's cannot. In the other three the server itself
    announces autocommit when the client logs in (an `init_connect` runs
    after that, and MySQL announces autocommit whatever its configuration,
    MySQL bug 66884), so PyMySQL loses the row connected directly too.
  - **PyMySQL with its default (`autocommit=False`) now sends
    `SET AUTOCOMMIT = 0` when it connects**, because the handshake says
    autocommit is on, as it does connected to MySQL. Its writes and its
    `rollback()` then behave as on MySQL. Two things follow under Engine
    Routing. Every statement of such a connection, reads included, runs
    inside a transaction on the source, and inside a transaction nothing
    goes to the copy: **the copy does not answer a PyMySQL connection left
    on its defaults**. (Before the fix it did, by accident of the wrong
    flag, and that connection's `rollback()` undid nothing.) And each such
    connection opens its connection to the source when it connects, not at
    its first query. To let the copy answer the heavy reads of a reporting
    client, open its connection with `autocommit=True`.
  - **mysqlclient (and Django on it) is corrected**: it sends `SET NAMES`
    before it looks, the port forwards it, and the source's answer carries
    the real flag. Read from its source, not run.
  - **Connector/J** always sends `SET autocommit=1` when it connects, and
    the Go driver never looks; neither depends on the announcement.
  - **MySQL with `autocommit=0` in its configuration** announces autocommit
    in its handshake and in its first answer although `@@autocommit` is 0
    (measured on 8.4), so the port has nothing truer to pass on and cannot
    detect it.
  - **`NO_BACKSLASH_ESCAPES` in the source's global `sql_mode`**: MySQL and
    MariaDB announce it in their handshake, the port only from the first
    forwarded statement on. A driver that escapes strings itself by this
    flag (PyMySQL, mysqlclient, the C library, the Go driver with
    `interpolateParams`, Connector/J with client-side prepared statements)
    writes the first statement of a connection with backslash escapes the
    source does not read as escapes.

  What to do with such a source: have every client send `SET autocommit=1`
  (or `0`) itself when it connects (PyMySQL: `init_command`), which also
  makes the first statement one without string arguments; or make
  autocommit, and a `sql_mode` without `NO_BACKSLASH_ESCAPES`, the source's
  default. The port says it in its log, once per server, when the first
  answer of a session on the source shows a state that differs: `Engine
  Routing: the source opens its sessions with autocommit off, and the
  port's handshake tells every client autocommit on and backslash escapes`.
  (When a connection's first statement is itself a `SET` of `autocommit`
  or of `sql_mode`, the setting it sets is the client's and is not counted;
  the other one still is.) MySQL's
  `autocommit=0` cannot be detected, for the reason above.

Until this was fixed (#2110) the handshake announced status 0, and so did
most answers the port wrote itself. PyMySQL, whose default is autocommit off, read
that as "already off", did not send `SET AUTOCOMMIT = 0`, and the session on
the source stayed in autocommit: an `INSERT` followed by `rollback()` left
the row in the table.

**The server version in the handshake is always `8.0.11`**, whatever the
source is. It is fixed on purpose, for the same reason: the handshake is
written before the server is chosen, and one port serves every server.
What a client reads by asking is the source's own under Engine Routing
(`SELECT VERSION()`, `@@version` and `@@version_comment` are forwarded, so a
MariaDB source answers `11.4.x-MariaDB`), and the port's own on a
connection that is not routed (`@@version` is `8.0.11`, `@@version_comment`
is `DBTrail time-travel port`). The consequence is for a driver that
chooses its behaviour from the handshake version rather than from a query:

- Connector/J picks the names of the variables it reads when it connects
  from that version: told 8.0.11 it asks for `@@transaction_isolation` and
  not `@@tx_isolation`, and leaves out the query cache variables. MySQL 5.7.20
  and later, MySQL 8 and MariaDB 11.1 and later know that name; under Engine
  Routing, an older MariaDB (10.11 for example) or MySQL source does not,
  and Connector/J's connect statement fails there with error 1193. Not
  measured; read from the driver's source.
- A driver that recognises MariaDB by the `-MariaDB` suffix of the handshake
  version (mysql2 for Node, MariaDB's own connectors) treats the port as
  MySQL even when the source is MariaDB, and does not use its MariaDB-only
  protocol extensions. The port does not offer those extensions anyway.
- PyMySQL, mysqlclient, the Go driver and the frameworks on top of them
  (SQLAlchemy, Django, GORM) either ignore the handshake version or ask
  with `SELECT VERSION()`, and get the source's.

#### A busy copy: the read waits its turn

A heavy read that finds the copy busy is not sent to MySQL. It waits for the
copy, and the client sees a slower answer, not a different one. The rule,
as the router applies it:

- **How many run.** The router works out how many statements run on the
  copy at once from the cores and the memory it may use, unless you set the
  number with `--concurrency`
  ([The router](#the-router-the-process-that-serves-the-port)). Statements
  for the same server can run at the same time. `SHOW ROUTER STATUS` shows
  the number as `pool_workers`.
- **The rest wait.** A read that finds every worker taken waits until one
  comes free, then runs on the copy. The turn is in order of arrival.
- **A read waits for as long as its client stays connected.** There is no
  time limit by default, the way a statement waits for a thread on a busy
  MySQL: 256 clients sending heavy reads to a copy that runs 4 at once are
  all answered, slowly, and none gets an error. The read is not handed to
  MySQL: a port that exists to keep expensive reads off the source does not
  send them there when they are many.
- **A time limit, if you want one.** Start the router with `--max-wait 30s`
  (`BINTRAIL_ROUTER_MAX_WAIT`, or `ROUTER_MAX_WAIT` in the Docker stack's
  `.env`). A read that has waited that long gets MySQL error 1040, "too many
  connections", which drivers and pools retry (`copy_wait_timeout` in the
  counter, under `route="refused"`). At least `1s`: a refusal that cost the
  client no time would be repeated as fast as the client can send, and each
  attempt asks the source for a plan first. Empty or `0` is no limit.
- **The line is long.** Up to 4,096 statements may wait at once. One more
  gets error 1040 without waiting (`copy_queue_full`), or error 1203 on a
  connection that is not routed.
- **What else ends a wait.** The client disconnecting: the statement leaves
  the line at once and nothing runs, on the copy or on MySQL (`cancelled`
  in the counter). The router stopping does the same.
- **The wait is not counted as the statement running.** The port stops a
  statement after 5 minutes in all, and one on the copy after 60 seconds
  there. The 60 seconds start when the statement gets its turn, and the 5
  minutes stand still while it waits and go on from where they were.
- **What a waiting read holds.** Its client connection and its idle
  connection to MySQL, nothing else: no place on the copy, no process, no
  memory worth counting. That connection to MySQL is idle for the whole
  wait: one longer than the source's `wait_timeout` (8 hours by default)
  loses it, and the client then has to reconnect.
- **Waiting does not exempt a read from the copy's other refusals.** A read
  that waited, ran on the copy, and was then refused there (a result over
  the row cap, a construct DuckDB lacks) is run by MySQL afterwards under
  that refusal's own reason: it paid the wait, the attempt and MySQL's time.
- **On a connection that is not routed** (routing off, a server that cannot
  be forwarded for, a `@copy` user) there is no MySQL behind the statement.
  It waits the same way. Under `--max-wait`, past the wait the client gets
  error 1203.

What to watch ([Observability](observability.md)):
`bintrail_sql_slot_wait_seconds` is how long statements wait and how each
wait ended, `bintrail_sql_slot_waiting` is how many wait right now, and
`copy_queue_full` and `copy_wait_timeout` in
`bintrail_read_routing_decisions_total` count the reads refused because the
copy was busy. `SHOW ROUTER STATUS` shows `pool_running` and `pool_waiting`
at that moment. Those counts climbing means the port is asked for more heavy
reads than the router's workers serve: give the router more cores and
memory, or point some readers at their own DuckDB on the bucket
([Dashboards](dashboards.md)).

#### Seeing who answered
Every routing decision is counted, per server, since the router started.
Decisions, not successes: a statement MySQL then fails was still MySQL's.
The router does the counting, so the numbers are read from the router:

- `SHOW ROUTER STATUS` on any connection to the port
  ([The router](#the-router-the-process-that-serves-the-port)).
- The web interface: on **Settings → MySQL clients**, the panel **Connect a SQL
  client** shows, for the server picked in the sidebar, how many statements
  MySQL answered and how many the copy did, as two figures over a bar. The
  count per reason is folded under **Why each side**, and the router's other
  numbers under **The router's numbers**. It needs the
  capture process to know the router's address (`--router-url`; the Docker
  stack and the packages set it). **Refresh** re-reads the counts. The same
  numbers are in `GET /api/router`.
- Prometheus (`router --metrics-addr`):
  `bintrail_read_routing_decisions_total{server, route, reason}`. `server`
  is the registry id, `route` is `copy`, `mysql` or `refused` (a statement
  nobody ran), and `reason` is one of
  a closed set: `expensive_plan` (the copy answered), `cheap_plan`, `bounded_limit` (a small `LIMIT` MySQL answers
  without reading past it), `result_over_row_cap` (an expensive plan whose
  result is estimated above the copy's row cap: MySQL answered, and the copy
  was not tried), `not_a_select`, `write`, `session_setting`,
  `session_differs` (the source's session on that connection holds a setting
  the copy does not reproduce; the router's log names the setting and its
  value once per connection), `connection_pinned` (a `CREATE TEMPORARY
  TABLE`, `LOCK TABLES` or `PREPARE` ran earlier on that connection),
  `in_transaction`, `veto`, `explain_failed`,
  `copy_refused`, `copy_queue_full` (refused with error 1040: the copy was
  busy and the line for it was full, so this one
  did not wait), `copy_wait_timeout` (refused with error 1040: the copy was
  busy and the statement
  waited the whole `--max-wait` for its turn; never counted when no time
  limit is set), `cancelled` (refused: the client left, or the router was
  stopping, while the statement waited its turn or ran on the copy; it is
  not handed to MySQL), `copy_results_held` (the port was still
  sending as many results as it keeps in memory at once, and MySQL
  answered), `copy_columns_differ`
  (the copy works and declined a `SELECT *` or a `NATURAL JOIN` over a table
  whose columns there are not MySQL's, a statement that names a column
  the copy does not hold, one that does arithmetic on a date column, or one
  that names a `TIME` or `YEAR` column), `show_warnings`,
  `upstream_lost` (nobody answered: the port's connection to the source is
  lost or could not be opened, and the client got error 2006), and
  `read_only` (refused: not a read, on a read-only port, which is how the
  router starts unless told otherwise; the client got error 1290).
  Four more names are part of the set and are not counted by the port any
  more. `copy_age_unknown`, `copy_too_old` and `tables_unchanged` belonged
  to the maximum copy age, which no longer exists. `routing_off` is listed
  for completeness: a connection made while routing is off is not routed, so
  it counts no decision. A copy that never answers shows up as `copy_refused`
  or `session_differs` climbing while
  `expensive_plan` stays flat; `explain_failed` climbing means MySQL refuses to `EXPLAIN` what the
  client runs (a table it cannot see, a statement it cannot plan);
  `upstream_lost` climbing means the source is unreachable or the forwarding
  account's credentials are wrong. Each routed statement counts exactly once,
  a `USE` sent as a statement included. Not counted, because they are no
  routing decision: the schema seeded when the connection opens and the
  mysql client's `\u` (sent as `COM_INIT_DB`, not as a statement), `SHOW
  WARNINGS` right after a statement the copy answered (the copy's own
  warnings), and the `_flashback`/`_snapshot`/`_diff` time-travel shapes.

A server whose connections cannot route (no forwarding account, no source
database to forward to, or no SQL on the copy) is served from the copy
alone. `SHOW ROUTER STATUS` says so on its first line, the router's status
document marks that server `cannot_route`, and the metric has no series for
it.

#### Finding where the copy answers differently: `sql-compare`
Before turning routing on for a workload, play that workload through both
sides and read the differences:

```
bintrail-console sql-compare \
  --source-dsn 'app:pw@tcp(db.internal:3306)/shop' \
  --copy-dsn   '<server id or name>:<password>@tcp(127.0.0.1:3309)/shop' \
  --sample 200            # or --statements queries.sql (';'-terminated)
```

`--copy-dsn` is the MySQL port on a connection the copy answers alone, so
every statement runs on the copy: the port with routing off, or, with
routing on, the user written `<server>@copy`. `--sample N` takes the N most recent distinct `SELECT`s the source
ran (`performance_schema.events_statements_history_long`; the error names
the consumer to enable when it is off). The command is read-only: anything
that is not a plain read is skipped before it runs (writes, a `WITH` that
writes, `SELECT ... INTO`, locking reads, `GET_LOCK`/`SLEEP`/`BENCHMARK`,
two statements in one line); a stored function with side effects cannot be
screened by its text, so do not point `--sample` at a workload that calls
one. The run refuses to start when its connection to the copy port is routed (it
would be comparing MySQL with MySQL), and exits 1 when no statement reached
a comparison at all.

Per statement it prints `EQUAL`, `DIFFERENT` (and how: `columns`, `rows`,
`order`, `case`, `null`, `precision`, `text`, with the first differing cell;
`columns` is a different number of columns, a column both sides name
standing at another position, or a position where the two sides return two
different plain column names; it is checked before any cell, so that two
columns holding equal values, or a result with no rows, cannot hide it. A
position where either side's name is an expression's text is left to the
cells, since each engine names an expression its own way),
`NOT_ON_COPY` (the copy refused it: the router would forward it; the copy
is sent what the router would send it, backtick-quoted names in double
quotes, so a statement an ORM wrote is compared and not just refused),
`SOURCE_ERROR`, `INCONCLUSIVE` (the copy cut the result at its cap, the
source returned more than `--max-rows`, or the source's own answer changed
between two reads: a live write, not a difference) or `SKIPPED`, plus what
the router would do with it and why. The number that matters is the last
line: statements the router **would send to the copy** that answer
differently. Each is a veto to add or a difference to document; the command
exits 1 when there is at least one. `--format json` for scripts.

The copy answers from its last snapshot, so run this on a quiet source or
right after a snapshot: a copy that is behind the source shows up as a
`DIFFERENT`, since the tool does not model the port's copy-age check (nor
its "inside a transaction" forwarding or its check of the session's
settings, nor the copy refusing a `SELECT *` it cannot answer with MySQL's
columns: such a statement is reported `DIFFERENT (columns)` here and is
MySQL's under routing). To compare under a session time zone, give it to
both sides in the DSNs, the way a driver sets it when it connects
(`...?time_zone=%27Europe%2FMadrid%27` on `--source-dsn` and on
`--copy-dsn`). A table written
DURING the run shows up as `INCONCLUSIVE: source changed`. Same rows in a
different order are counted apart and never fail the run: ties in an `ORDER
BY` resolve differently on each engine, and so does a collation difference
in the sort key; read those by hand.

<a id="the-router-the-same-port-as-a-service-of-its-own-experimental"></a>
### The router: the process that serves the port

`bintrail-console router` is the process that serves the MySQL port. It runs
beside `bintrail-console watch`, the process that captures, and not inside
it. `watch` opens no MySQL port. If the router is not running, nothing
answers on 3309, even with the port turned on in the web interface.

```bash
bintrail-console router --listen 127.0.0.1:3309
mysql -h 127.0.0.1 -P 3309 -u <server id or name> -p
```

Who starts it:

- **Docker Compose**: the `router` service of `docker-compose.yml`. It starts
  with the stack. See [docker.md](docker.md#the-router-the-service-that-answers-the-mysql-port).
- **The deb and rpm packages**: the `bintrail-router` systemd service. The
  package enables and starts it. See [install.md](install.md#the-router-a-second-service-of-the-same-package).
- **Anything else** (ECS, a container you wire by hand): one more container or
  process that runs `bintrail-console router` and reads the same servers file
  and the same `console-mysql-port.yaml` as the capture process.

Why it is a process of its own:

- **Restarting capture does not drop its connections.**
- **A statement that runs out of memory cannot take capture with it.** It is
  another process, under its own ceiling.
- **It sizes itself.** How many statements run on the copy at once is worked
  out from the cores and the memory the router may use: one worker per two
  cores or per statement's memory, whichever is fewer, and the cores are
  divided among the workers. Nothing has to be set by hand. Give the service its own
  ceilings (`cpus` and `mem_limit` in Docker, `CPUQuota=` and `MemoryMax=`
  under systemd) and it uses all of them. With none it takes half of the
  machine, since capture uses the machine too. The numbers and where they
  came from are printed when it starts.
- **Or you say how many.** `--concurrency 16` (`BINTRAIL_ROUTER_CONCURRENCY`,
  `ROUTER_CONCURRENCY` in the Docker stack's `.env`) sets how many statements
  run on the copy at once, from 1 to 64, the way `innodb_thread_concurrency`
  does on a MySQL server. The number is taken as said. Above one per two
  cores each statement runs with fewer threads, down to one, so more run
  at once and each is slower; above the number of cores they take turns. Each statement keeps its own memory (2 GB by default, plus a
  quarter), so the startup lines print a `WARNING` with the sum when the
  number can ask for more memory than the router may use: lower the number,
  or the memory each may use. Two more things grow with the number. The temporary disk a
  statement may use past its memory is the free space divided among twice
  the number, and under 256 MB each there is none, which the router's log
  says at startup: a heavy statement that used to finish slowly then fails
  at its memory. And the router keeps up to that many idle worker processes
  ready between statements, a few tens of MB each. It does not change how
  many clients connect: any number does, and the ones past this number wait
  their turn.

It asks the capture process nothing. It reads the servers file the web
interface manages (`--servers-file`, the same path `watch` uses) and never
writes it, and it sees a server added, changed or removed there within a few
seconds, without a restart. When a server's forwarding account changes, the
connections that were using the previous one are closed.

**The switch and the password.** The router waits without listening until
the MySQL port is turned on in the web interface, accepts the password
created there, and closes its port, with every connection on it, when the
port is turned off there. Started with `--token` (or
`BINTRAIL_CONSOLE_TOKEN`), it accepts the token as a password too while the
port is on. The token does not open the port: one that nobody turned on must
not open because a token is in the environment the router shares with the
capture process. Replacing the password in the web interface closes the
connections that were open too: clients reconnect with the new one, and
whoever held the old one is out.

**Routing on or off.** The router reads one more thing from the port's saved
setting (`console-mysql-port.yaml`): `routing`. Absent or `false`, the copy
answers every statement and nothing is forwarded. `true`, statements are
routed as described under
[Engine Routing](#engine-routing-mysql-answers-the-copy-takes-the-heavy-reads).
The change is seen within a few seconds and closes the connections that were
open: who answers is decided when a connection is made, and one left open
would go on forwarding after routing was turned off. With or without a
token.

**The copy alone, for one connection.** Connect as `<server>@copy` and that
connection is answered from the copy alone, the light reads too, while the
others stay routed: `mysql -u shop@copy ...`. It only takes a connection out
of routing. `sql-compare` needs such a connection (`--copy-dsn
'shop@copy:<password>@tcp(...)/'`) when routing is on. A server whose own
name ends in `@copy` is found under its name first.

**Its numbers.** The router has no web page, so it answers for itself.
`SHOW ROUTER STATUS`, on any connection to it, lists the server's copy
snapshot and its age in seconds, how many of that server's statements the
copy and MySQL answered and how many were refused since the router started
(with a row per reason), and the pool: how many workers, how many are running
a statement and how many statements wait right now. It also says who answered
the last statement on that connection, and when the copy did, how far the data
it read had been indexed ([No maximum copy age](#no-maximum-copy-age)). On a
connection the copy answers alone, its first line says that nothing is routed
there and why.

```sql
SHOW ROUTER STATUS;
-- server                        4f1c2a9b0d3e5f67
-- copy_snapshot                 2026-10-10T03:00:00Z
-- copy_age_seconds              68412
-- last_answer                   copy
-- last_answer_indexed_through   2026-10-10T02:58:41Z
-- last_answer_age_seconds       68480
-- statements_copy               1841
-- statements_mysql              96233
-- pool_workers                  4
-- pool_running                  1
-- pool_waiting                  0
```

With `--metrics-addr` (or `BINTRAIL_ROUTER_METRICS_ADDR`; an address of its
own, not the one `watch` serves) it exports Prometheus metrics: the routing
decisions, the wait for a worker, and
`bintrail_read_routing_copy_snapshot_timestamp_seconds`, per server.
`time()` minus that one is the copy's age, the number to alert on here, the
way replica lag is alerted on. See [Engine Routing metrics](observability.md#engine-routing-bintrail_read_routing_decisions_total).
The Docker stack and the packages set this address for you.

The same address answers `GET /status` with the router's numbers as one JSON
document: for every server, how many statements the copy answered, how many
MySQL, how many were refused and for which reasons, and the time of the
copy's snapshot; and once for the whole process, the pool (workers, running,
waiting, threads and memory each). The snapshot times are the ones read for
the metric, every 30 seconds, and `copy_snapshots_read_at` says when: a
time much older than that means the reading stopped, and the snapshot times
are the last ones known. A server left out of a reading (its copy could not
be read, or it has none) has no `copy_snapshot`; the router's log at debug
level says which. Three yes-or-no fields say what the counts cannot:
`cannot_route` (the server cannot be forwarded for, so the copy answers it
alone), `account_refused` (the
source turned the router's login away, so clients get error 2006 while the
counts go on rising) and `session_untracked`. Three more say how far behind
capture the copy's answers were: `data_as_of`, the instant the data of the
copy's last answer for that server was indexed through, `data_age_seconds`,
how far behind that was when it ran, and `data_age_max_seconds`, the worst
since `counting_since`, all rounded up. The first two are absent when the
last answer's instant is not known (or the copy has answered nothing yet);
the third, when no answer's is. `data_age_unknown` counts the answers whose
lag could not be told, which the max cannot include (the panel then says
"at least"), `data_age_unknown_reason` says why when the last answer is one of them,
and `data_age_pending` says the last answer's lag is still being worked out
(the router places a snapshot in the index within 30 seconds);
the router's log says it at warning level, once a minute at most. Like
`/metrics` it asks for no password, so it carries counts, times and the
router's own settings only: never an account's name, nor what a source
answered a login. Those stay in the router's log.

**In the web interface.** Give the capture process that address
(`watch --router-url http://127.0.0.1:9091`, or
`BINTRAIL_CONSOLE_ROUTER_URL`; the Docker stack and the packages set it) and
its MySQL clients page shows a panel, The router: for the server picked in the
sidebar, who answered and why, the copy's lag (its last answer's, and the
max since the router started), the snapshot of the copy the router reads and its
age, and the workers. The capture process asks the router each time the
page is opened or Refresh is pressed; the router asks it nothing. When the
router does not answer, the panel says so and why, and shows no counts: a
router that is down is not a router nobody uses.

```sh
curl -s http://127.0.0.1:9091/status
```

**Without the index.** A server's index database can be away (restarting,
unreachable from the router's host) and the router still serves that server:
forwarding and the copy's tables do not read the index. Time travel
(`_flashback`, `_snapshot`, `_diff`) and the `events` view do, and answer
with an error until it is back.

**What it needs.** The servers file, a copy on local disk for each server it
should answer ordinary SQL for, and, with Engine Routing on, a network path to
each server's source. The router brings nothing from a bucket itself. A
server whose snapshots `watch` writes has its copy in its local folder. For a
server whose snapshots are only read from S3 it reads the local mirror `watch` keeps
beside the servers file
([SQL on a server whose snapshots are only on S3](console.md#sql-on-a-server-whose-snapshots-are-only-on-s3)),
so it has to see that folder: it does when it runs beside `watch`, as in the
bundled stack and the packaged service. A router on another machine needs
the folder that holds the servers file shared with it, or it has no copy to
answer for such a server.

**Its settings.** Each is a flag of `bintrail-console router` and an
environment variable. Most variables are the capture process's own names, so
one environment file serves both processes.

| Flag | Environment variable | Default | What it does |
|---|---|---|---|
| `--listen` | `BINTRAIL_ROUTER_LISTEN` | `127.0.0.1:3309` | Where the MySQL port listens |
| `--metrics-addr` | `BINTRAIL_ROUTER_METRICS_ADDR` | none | Where `/metrics` and `/status` are served |
| `--token` | `BINTRAIL_CONSOLE_TOKEN` | none | The access token, one more password for the port. It does not open the port |
| `--servers-file` | `BINTRAIL_CONSOLE_SERVERS` | `~/.config/bintrail/console-servers.yaml` | The servers file the web interface manages. Read, never written |
| `--route-read-only` | `BINTRAIL_CONSOLE_ROUTE_READ_ONLY` | on | Refuse every statement that is not a read. `--route-read-only=false` or `BINTRAIL_CONSOLE_ROUTE_READ_ONLY=0` forwards writes |
| `--route-cost-threshold` | `BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD` | 10,000 | Under Engine Routing, a `SELECT` whose plan costs at least this goes to the copy. 0 turns the rule off |
| `--route-scan-rows` | `BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS` | 100,000 | Under Engine Routing, a `SELECT` whose plan scans a whole table of at least this many rows goes to the copy. 0 turns the rule off |
| `--sql-memory` | `BINTRAIL_CONSOLE_SQL_MEMORY` | 2GB | Memory each statement on the copy may use (at least 512MB) |
| `--sql-port-max-rows` | `BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS` | 10,000 | Most rows one statement returns. A larger result is refused with error 1104 |

The router refuses to start with both `--route-cost-threshold` and
`--route-scan-rows` at 0, since no statement could then go to the copy.
`BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE`, `BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT` and
`BINTRAIL_CONSOLE_FLASHBACK_LISTEN` do not apply to it, and it says so at
startup when they are set.

**The memory of a statement.** With no `--sql-memory`, the router uses the
memory saved in the web interface (**Settings**, **MySQL clients**, **Memory for
SQL on the copy**), else the default. It reads it once, when it starts: how
many statements run at once is worked out from it, so a value taken later
could let them use together more than the service was given. A change saved
while the router runs is applied when the router is restarted, and the
router's log says so when it sees one. Its startup lines say where the
memory came from; a saved setting that cannot be read is said there, and the
default is used.

Forwarding to a replica is not something the router does. To read from
replicas, put ProxySQL in front of the source and point the server's source
address at it.

### Upgrading from a version with the port inside `watch`

Earlier versions opened the MySQL port inside the capture process
(`bintrail-console watch`, turned on in the web interface or with
`--flashback-listen`). Now only the router serves the port, on 3309.
What changes on an upgrade:

- **`--flashback-listen` / `BINTRAIL_CONSOLE_FLASHBACK_LISTEN` on `watch` no
  longer opens a port.** If it is still set, it does one thing, once: when
  the port has no saved setting yet, the port is saved as on, with a new
  password you can replace in the web interface. An installation that had
  the port keeps it. A port that already has a saved setting, on or off, is
  left as it is. `watch` prints a WARNING at startup that says what happened
  and asks you to remove the setting. The access token, if one is set, is
  still accepted as a password.
- **The port answers where the router listens: `127.0.0.1:3309` unless you
  say otherwise.** The address `--flashback-listen` named, and the address
  saved in the web interface, no longer decide where the port listens. If
  the port answered other machines before (`0.0.0.0:3309`, a LAN address)
  or used another port number (3308 in older examples), start the router
  with that address: `--listen`, or `BINTRAIL_ROUTER_LISTEN`. Otherwise
  those clients are refused after the upgrade.
- **Something must run the router.** The Docker stack needs the current
  `docker-compose.yml` ([docker.md](docker.md#upgrading-the-stack)). The
  packages start `bintrail-router` on the upgrade
  ([install.md](install.md#the-router-a-second-service-of-the-same-package)).
  Anywhere else, start `bintrail-console router` yourself.
- **`--route-max-copy-age` / `BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE` no longer
  turns Engine Routing on.** It only prints a WARNING. Engine Routing is the
  saved `routing` setting, and it is off until that is turned on. There is no
  maximum copy age any more: with routing on, an expensive read goes to the
  copy whatever the age of its snapshot.
- **`--route-read-only`, `--route-cost-threshold`, `--route-scan-rows` and
  `--sql-port-max-rows` given to `watch` no longer apply.** `watch` prints a
  WARNING that names them. Give them to the router, on its command line or
  in the environment both processes share.
- **The router refuses writes unless told otherwise.** Before, read-only was
  something you asked for. Now it is the default. To forward writes, start
  the router with `--route-read-only=false` or
  `BINTRAIL_CONSOLE_ROUTE_READ_ONLY=0`.

### The dedicated terminal

**With the Docker Compose stack**, the shim ships as the opt-in `flashback`
profile. It serves the boot `SOURCE_DSN` source, so set `SOURCE_DSN` in `.env`
and bring it up *with* the full stack (`up -d`, not `up -d shim`, so the
streaming `bintrail` service comes along — see [docker.md](./docker.md)):

```sh
SHIM_USER=analyst SHIM_PASSWORD='pick-a-strong-one' \
  docker compose --profile flashback up -d
```

**Standalone** (the shim is a subcommand of the core `bintrail` binary), run it
against your index — no ProxySQL, and the source MySQL need not even be
reachable (the shim reads the index). `init-shim` reads `BINTRAIL_SOURCE_DSN`
and `BINTRAIL_SERVER_ID` from the environment (or `.bintrail.env`):

```sh
export BINTRAIL_SOURCE_DSN='user:pass@tcp(your-db:3306)/yourdb' BINTRAIL_SERVER_ID=prod-1
bintrail init-shim --output shim.yaml          # then fill in mysql_user + mysql_password
bintrail shim --shim-config shim.yaml \
  --index-dsn 'user:pass@tcp(127.0.0.1:3306)/bintrail_index'
```

Either way, connect a plain `mysql` client to the shim's port (default `3308`)
and query the virtual schemas:

```sh
mysql -h 127.0.0.1 -P 3308 -u analyst -p
mysql> USE myapp;
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
mysql> SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';   -- full table (needs a baseline)
```

`_snapshot.*` (the complete table as it was) requires a baseline configured with
`--baseline-dir` / `--baseline-s3` (or `BASELINE_DIR` on the compose profile);
without one, `_flashback.*` returns only rows with binlog activity in the
retained window. The statement shapes and their semantics are identical on both
paths — see **Step 6 — Run a time-travel query** below.

---

The ProxySQL walkthrough below takes about 10 minutes on a fresh Ubuntu 22.04 or Amazon Linux 2023 host that already has a populated DBTrail index.

---

## Prerequisites

Before starting, you need:

- **A populated DBTrail index.** Some process is keeping `binlog_events` current — typically `bintrail stream`. If rotated hours have been archived, `archive_state` points at the local directory or `s3://` prefix where the Parquet files live. If you haven't set any of this up yet, see [`docs/streaming.md`](streaming.md) and [`docs/rotation-and-status.md`](rotation-and-status.md).
- **A `.bintrail.env` file** with `BINTRAIL_SOURCE_DSN`, `BINTRAIL_INDEX_DSN`, and `BINTRAIL_SERVER_ID` set. `bintrail config init` scaffolds one.
- **The `bintrail` binary** on the host. The shim is a subcommand — there is no second binary to download.
- **Root or `sudo` access** on the host.
- **A writable bintrail config directory.** The generator commands below (`bintrail init-shim`, `bintrail proxysql-config`) and the `mysql … < proxysql-setup.sql` redirect both run as the operator (not root), so the directory holding `.bintrail.env`, `shim.yaml`, and `proxysql-setup.sql` must be owned by the operator. If you keep these in a root-owned `/etc/bintrail`, run `sudo chown $(whoami):$(whoami) /etc/bintrail` once at install time.
- **The `mysql` client** installed on the host (used to apply ProxySQL config below).
- **A MySQL user your application will use to connect through ProxySQL.** This is *not* the replication user the streamer uses — it's a regular application user that ProxySQL authenticates against. Pick a username and a strong password; you'll need both below.

---

## Step 1 — Generate `shim.yaml`

`bintrail init-shim` scaffolds the file from your existing `.bintrail.env`:

```sh
cd /etc/bintrail   # or wherever your .bintrail.env lives
bintrail init-shim --output shim.yaml
```

The generated file has one tenant block populated from your `.bintrail.env`, plus two TODO lines for the application credentials:

```yaml
listen: '127.0.0.1:3308'

tenants:
  - server_id: '...'        # from BINTRAIL_SERVER_ID
    source_dsn: '...'       # from BINTRAIL_SOURCE_DSN
    # TODO: fill in your application's MySQL credentials
    # mysql_user: app_user
    # mysql_password: '<cleartext>'
```

Edit `shim.yaml`, uncomment the two TODO lines, and paste the values:

```yaml
    mysql_user: app_user
    mysql_password: 'your-app-password'
```

`bintrail proxysql-config` recomputes the SHA1 hash ProxySQL needs from `mysql_password` automatically — you do not need to run a manual SHA1 recipe.

> **Auth note**: both `bintrail shim` and ProxySQL validate the application's password against the same `mysql_password`. The default is `mysql_native_password`; `caching_sha2_password` is opt-in via `--auth-method` (see Step 4). The shim's listen address defaults to `127.0.0.1:3308` so it is not reachable from the network. Treat `shim.yaml` as you'd treat `.bintrail.env` — it contains a password and ships at 0o600.

### Isolating tenants with `allowed_schemas`

Authentication alone does not scope what a tenant can *query*: the shim answers
every time-travel query from one shared index, so in a multi-tenant `shim.yaml`
any authenticated tenant could `USE` (or fully qualify) another tenant's schema
and read its entire indexed history — including deleted rows.

Add the optional `allowed_schemas` list to each tenant to enforce isolation:

```yml
tenants:
  - mysql_user: app_a
    mysql_password: '...'
    source_dsn: 'a:pw@tcp(db-a:3306)/app_a'
    allowed_schemas: [app_a]
  - mysql_user: app_b
    mysql_password: '...'
    source_dsn: 'b:pw@tcp(db-b:3306)/app_b'
    allowed_schemas: [app_b, app_b_audit]
```

With `allowed_schemas` set, a `USE` of — or any time-travel query resolving to
— a schema outside the list is rejected with MySQL error 1044
(`ER_DBACCESS_DENIED_ERROR`), the same code a real mysqld returns for a
database the user has no grants on. The check applies to every statement
shape, including fully qualified forms that never issue `USE`
(`SELECT /*+ DBTRAIL_AT='…' */ * FROM other_schema.t`). Schema names are
matched case-insensitively.

The field is **opt-in**: a tenant without it keeps the historical
any-schema behaviour, so existing configs are unaffected. When `shim.yaml`
has more than one tenant and any of them lacks `allowed_schemas`, the shim
logs a startup warning that cross-schema isolation is not enforced.

**Both wire front-ends enforce it.** `bintrail-pg flashback` reads the same
`shim.yaml` and applies the same allowlist to the same resolved target schema,
including a fully qualified query that never selected a database; there the
refusal is `SQLSTATE 42501` (`insufficient_privilege`) rather than MySQL 1044,
and it is a query error — the connection stays usable. It emits the same
multi-tenant startup warning.

---

## Step 2 — Install ProxySQL

ProxySQL 2.6 (LTS) is the recommended release.

### Ubuntu / Debian

```sh
sudo apt-get update
sudo apt-get install -y wget lsb-release gnupg ca-certificates
sudo install -d -m 0755 /etc/apt/keyrings
wget -qO- https://repo.proxysql.com/ProxySQL/repo_pub_key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/proxysql.gpg
echo "deb [signed-by=/etc/apt/keyrings/proxysql.gpg] https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/$(lsb_release -sc)/ ./" \
  | sudo tee /etc/apt/sources.list.d/proxysql.list
sudo apt-get update
sudo apt-get install -y proxysql=2.6.*
sudo systemctl enable --now proxysql
```

### RHEL / Amazon Linux 2023

```sh
sudo tee /etc/yum.repos.d/proxysql.repo >/dev/null <<'EOF'
[proxysql_repo]
name=ProxySQL 2.6.x repository
baseurl=https://repo.proxysql.com/ProxySQL/proxysql-2.6.x/centos/9
gpgcheck=1
gpgkey=https://repo.proxysql.com/ProxySQL/repo_pub_key
EOF
sudo dnf install -y proxysql-2.6.*
sudo systemctl enable --now proxysql
```

After install, ProxySQL listens on:
- **`:6032`** — admin port (used to apply config). Default credentials are `admin / admin`. Change them in `/etc/proxysql.cnf` before exposing this port to anything other than localhost.
- **`:6033`** — MySQL protocol port your application connects to.

---

## Step 3 — Apply the ProxySQL config

`bintrail proxysql-config` reads `BINTRAIL_SOURCE_DSN` from `.bintrail.env` and `shim.yaml` from the previous step and emits a deterministic SQL script:

```sh
bintrail proxysql-config --output proxysql-setup.sql
```

The script tells you exactly how to apply it:

```text
ProxySQL setup SQL written to proxysql-setup.sql
Apply it: mysql -u admin -P 6032 -h <proxysql-host> < proxysql-setup.sql
```

If ProxySQL is on the same host (typical):

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

The script wraps its DML in `BEGIN`/`COMMIT` and finishes with `LOAD ... TO RUNTIME` and `SAVE ... TO DISK`, so the new routing is live immediately and survives a ProxySQL restart. **Re-running the script is safe** — it scopes its DELETEs to dbtrail-owned hostgroups (990, 991) and rule IDs (990001-990006), so it never touches operator-managed config.

Verify ProxySQL accepted the config — you should see exactly two rows, one for hostgroup 990 (your real MySQL — `hostname` reflects whatever you have in `BINTRAIL_SOURCE_DSN`) and one for hostgroup 991 (the shim, always `127.0.0.1:3308`):

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 -e \
  "SELECT hostgroup_id, hostname, port FROM runtime_mysql_servers WHERE hostgroup_id IN (990,991);"
```

---

## Step 4 — Run `bintrail shim` under systemd

Create `/etc/systemd/system/bintrail-shim.service`:

```ini
[Unit]
Description=bintrail shim - time-travel SQL backend for ProxySQL
Documentation=https://github.com/dbtrail/dbtrail/blob/main/docs/time-travel-sql.md
After=network-online.target proxysql.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/etc/bintrail
EnvironmentFile=/etc/bintrail/.bintrail.env
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml
Restart=on-failure
RestartSec=5s

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

> A copy of this unit ships at `deploy/bintrail-shim.service` in the DBTrail repo.

The unit reads `BINTRAIL_INDEX_DSN` from `/etc/bintrail/.bintrail.env` (the same file your other `bintrail` commands use) so the shim can answer queries against your index. The DSN must include the index database name (e.g. `…/bintrail_index`) — the shim refuses to start otherwise. Append `--allow-gaps` to `ExecStart` to warn-and-continue on archive failures or coverage gaps instead of returning a MySQL error to the client; the default is strict because the wire protocol has no warning channel.

**Auth method on MySQL 8.4+.** If your MySQL has `mysql_native_password` disabled (the default since 8.4), append `--auth-method=caching_sha2_password` to `ExecStart` (or `Environment=BINTRAIL_AUTH_METHOD=caching_sha2_password`):

```ini
ExecStart=/usr/local/bin/bintrail shim --shim-config /etc/bintrail/shim.yaml --auth-method=caching_sha2_password
```

Requires ProxySQL **2.7+** between the application and the shim — the LTS 2.6 line isn't verified to negotiate SHA2 against backends, so operators on 2.6 keep the default (`mysql_native_password`). The application user used by ProxySQL must match the chosen scheme: `IDENTIFIED WITH mysql_native_password BY <choose a password>` for the default path, `IDENTIFIED WITH caching_sha2_password BY <choose a password>` for the opt-in. `sha256_password` is also accepted by `--auth-method` if your environment requires it. The same 2.7+ requirement applies when ProxySQL fronts an **8.4 source** directly (its `caching_sha2_password` backend), set via `proxysql-config --backend-auth-plugin caching_sha2_password`.

> **DBTrail's own connections to MySQL 8.4 need no auth flag.** The ProxySQL requirement above is only about ProxySQL negotiating `caching_sha2_password` to a backend. DBTrail's *index* connection (`--index-dsn`) and its *source replication* handshake (`bintrail stream`/`up`) both complete `caching_sha2_password` over a plaintext network on their own — with no flag, no TLS, and no ProxySQL in the path. This is what the bundled MySQL 8.4 index uses by default.

Enable and start:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now bintrail-shim
sudo systemctl status bintrail-shim
```

You should see `active (running)`. Tail the log if not:

```sh
journalctl -u bintrail-shim -f
```

The shim should report `shim listening addr=127.0.0.1:3308 tenants=N` once it has loaded `shim.yaml`.

### Resource limits

The shim is a long-running daemon shared by every forensic session, so heavy or abandoned queries are bounded by three flags (all opt-out; the defaults are safe for a typical deployment):

- **`--query-timeout`** (default `5m`, env `BINTRAIL_SHIM_QUERY_TIMEOUT`) — per-query deadline covering the index fetch, the archive/DuckDB fetch, and the wait for a full-table slot. A query that exceeds it fails with MySQL error **1317** (`ER_QUERY_INTERRUPTED`). `0` disables the deadline.
- **`--max-connections`** (default `100`, env `BINTRAIL_SHIM_MAX_CONNECTIONS`) — concurrent client connections; a connection past the cap is refused with MySQL error **1040** (`Too many connections`), exactly like a real mysqld. `0` removes the cap.
- **`--max-fulltable-queries`** (default `4`, env `BINTRAIL_SHIM_MAX_FULLTABLE_QUERIES`) — concurrent full-table reconstructions (the heaviest path — a buffered `_flashback` or `LIMIT`ed query holds up to the 100,000-row cap; an uncapped streaming `_snapshot` holds only one row plus the changed-since-baseline set). Excess full-table queries wait for a slot; a waiter that outlives `--query-timeout` fails with MySQL error **1203** (`ER_TOO_MANY_USER_CONNECTIONS`). `0` removes the cap. PK point-lookups and `_diff` are never gated.

A client that disconnects mid-query (an ORM timeout, Ctrl+C in the mysql CLI) cancels the in-flight fetch immediately — the shim stops the index/S3 work instead of finishing a resultset nobody will read.

---

## Step 5 — Point your application at ProxySQL

Change your application's MySQL connection string from the real MySQL port (`:3306`) to ProxySQL's MySQL port (`:6033`). The credentials are the `mysql_user` / `mysql_password` pair from `shim.yaml` (cleartext — same value the shim and ProxySQL both validate against).

For example, with the Go MySQL driver:

```go
// before:
db, _ := sql.Open("mysql", "app_user:your-app-password@tcp(127.0.0.1:3306)/myapp")

// after:
db, _ := sql.Open("mysql", "app_user:your-app-password@tcp(127.0.0.1:6033)/myapp")
```

Normal queries (`SELECT * FROM orders WHERE id = 1`) still go to your real MySQL, transparently. Only queries that reference `_flashback.*`, `_diff.*`, or `_snapshot.*` are routed to the shim.

---

## Step 6 — Run a time-travel query

Connect through ProxySQL:

```sh
mysql -u app_user -p -h 127.0.0.1 -P 6033 myapp
```

Six statement shapes are recognised:

```sql
-- Row state at a point in time (point-lookup, fast):
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;

-- The same, on the REAL table name — the AS OF clause must END the
-- statement (#385). Rewritten internally to _flashback (binlog-only):
SELECT * FROM orders WHERE id = 12345 AS OF '2026-05-02 10:00:00';

-- Optimizer-hint form on the real table name (ORM-friendly: survives query
-- builders that would reject AS OF syntax). `*`-only — a column list here
-- is a parse error:
SELECT /*+ DBTRAIL_AT='2026-05-02 10:00:00' */ * FROM orders WHERE id = 12345;

-- Full-table reconstruction at AS OF (no WHERE). Against _snapshot (with a
-- baseline configured) this is every row that existed at that instant;
-- against _flashback it is only rows with binlog activity in the retained
-- window (see the _snapshot vs _flashback note under Limitations):
SELECT * FROM _snapshot.orders  AS OF '2026-05-02 10:00:00';
SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00';

-- Browse the first N rows of a full-table reconstruction. LIMIT lets a
-- full-table query succeed under the row cap; results come back in the
-- merge's order (no implicit ORDER BY):
SELECT * FROM _snapshot.orders AS OF '2026-05-02 10:00:00' LIMIT 100;

-- All events for one row in a time window:
SELECT * FROM _diff.orders BETWEEN '2026-05-01' AND '2026-05-02' WHERE id = 12345;
```

> **The hint form is the only one that degrades silently.** `/*+ DBTRAIL_AT=... */`
> is 100% valid vanilla MySQL (an unknown optimizer hint raises a warning, not an
> error), so if the query never reaches the shim — the client points at MySQL's
> port instead of ProxySQL's `:6033`, rules 990001-990006 are missing (e.g.
> ProxySQL restarted without `SAVE MYSQL QUERY RULES TO DISK`), or a
> lower-`rule_id` operator rule intercepts first — MySQL executes it against the
> live table and returns **present-day data with no error**. You believe you are
> reading the past; you are reading the present. The other shapes fail loud on
> the same misroute (`_flashback.*` → `ER_BAD_DB` 1049; bare `AS OF` → 1064), so
> prefer them whenever the client can emit them — reserve the hint form for
> ORM/query-builder paths that reject `AS OF` syntax, and verify routing after
> any ProxySQL change:
>
> ```sh
> bintrail doctor --source-dsn "$SRC" --proxysql-admin 'admin:<pass>@tcp(127.0.0.1:6032)/'
> ```
>
> The check confirms all six DBTrail rules are live in
> `runtime_mysql_query_rules` (advisory `WARN` when they aren't — it never
> changes doctor's exit code).

Every quoted time literal — in `AS OF`, `DBTRAIL_AT`, and the `BETWEEN` bounds — accepts four absolute formats (`'2026-05-02 10:00:00'`, RFC 3339 `'2026-05-02T10:00:00Z'`, zone-less `'2026-05-02T10:00:00'`, and date-only `'2026-05-02'`), plus `'now'` and relative forms `'<n> seconds|minutes|hours|days ago'` (e.g. `AS OF '5 minutes ago'`), resolved against the wall clock at parse time. Larger units (weeks, months) are deliberately not parsed — spell them as days.

**All three zone-less forms (`'2026-05-02 10:00:00'`, the zone-less RFC-3339-shaped literal, and date-only) are interpreted as UTC** — only the `Z`-suffixed RFC 3339 form is unambiguous by construction. There is no per-session override: a `SET time_zone = ...` sent by the client is accepted (returned `OK`, matching a real MySQL server's handshake noise) but has **no effect** on how the shim parses `AS OF` literals — it is treated as connection setup noise and silently ignored. If your monitoring or incident timeline is in local time, convert to UTC before writing the literal, or use the unambiguous `Z`-suffixed form.

**1-second granularity.** Timestamps are compared and stored at one-second resolution; a literal with sub-second precision has no finer effect than truncating to the second.

**Prepared statements are answered from a template.** A driver that prepares its statements (the binary protocol: Go's `database/sql` with arguments, MySQL Connector/J with `useServerPrepStmts=true`, Node's `mysql2` `execute`, .NET's `MySqlConnector`, the C API, PHP's `mysqlnd`) works against the port and against `bintrail shim`: `PREPARE` counts the `?` placeholders, `EXECUTE` writes the arguments into the statement as SQL literals and runs it exactly as if it had been sent as text, and the rows come back in the binary encoding with typed columns. That holds for the time-travel shapes (`SELECT * FROM _flashback.orders AS OF ? WHERE id = ?`) and for ordinary SQL on the copy alike. What to know:

- A template's prepare answer carries no column definitions (they are only known once the statement runs); drivers read them from the execute answer. A client that needs the result's shape before executing (the C API's `mysql_stmt_result_metadata`, PHP `mysqli` with native prepares) sees none.
- A template's result is buffered, never streamed: a whole-table `_snapshot` read through a prepared statement is held to the 100,000-row cap (error 1104 past it), where the same read as text streams past it.
- An error keeps the code the same statement gets as text, at `PREPARE` and at `EXECUTE`.
- An argument whose bytes are not valid UTF-8 reaches the copy as a `BLOB`; the time-travel shapes refuse it.
- Cursors are not served: an execution that asks for one (Connector/J with `useCursorFetch=true`, the C API's `STMT_ATTR_CURSOR_TYPE`) is refused with error 1105 naming the cause. Results are sent whole.
- With Engine Routing on, a statement that is not a time-travel shape is prepared on MySQL itself and executed there with MySQL binding the arguments, as if the client were connected to it: the port never writes a forwarded statement out as text, so a write stores exactly the value the client bound. The prepare answer then carries MySQL's own parameter and column definitions, errors are MySQL's, and a forwarded result is streamed row by row in MySQL's own binary encoding. Each execution goes through the routing rules above with its own arguments (the plan is read with them bound); only an execution the rules send to the copy is written out, for the copy alone, and falls back to MySQL if the copy refuses it. An execution stays on MySQL when the copy would read it differently: the database changed since the statement was prepared, or a string argument spells a non-integer number (`'12.7'`, compared as a number on MySQL). A statement that reaches the plan step holds a second statement on the source, the `EXPLAIN` of itself, which counts toward the source's `max_prepared_stmt_count`. A statement is tied to the connection it was prepared on: once the port's connection to the source is lost, its executions fail with 2006 like everything else.
- On a MariaDB source, a client that understands MariaDB's own column annotations (a `JSON` column, which MariaDB stores as text) sees such a column as plain text through the port: the bytes are the same, the annotation is not relayed.

On the `_flashback` / `_snapshot` shapes a column list may replace `*` and the optional `TIMESTAMP` keyword may follow `AS OF` (Oracle / SQL Server convention):

```sql
SELECT id, email, name FROM _flashback.users AS OF TIMESTAMP '2026-05-02 10:00:00' WHERE id = 1;
```

The column list accepts bare identifiers only; backticks, schema-qualified columns (`users.id`), and aliases (`id AS user_id`) are not yet parsed and surface as `ER_PARSE_ERROR`. Columns the row image is missing (e.g. dropped post-event) come back as `NULL` — matching MySQL's behaviour after an `ALTER TABLE DROP COLUMN`.

For a **full-table** `SELECT *` (no column list), DBTrail unions the columns present across the reconstructed rows, so a column that existed at the queried time but was **dropped afterward** still appears — with its historical values — rather than being silently hidden by the current (narrower) schema. (The one exception is the uncapped streaming `_snapshot` path described below: it fixes the column set from the table's *current* schema before the first row is sent, so a since-dropped column is not surfaced there — the shim logs a warning naming the omitted column, and adding a `LIMIT` forces the buffered path that surfaces it.)

The WHERE column must match the table's primary key. A WHERE on a non-PK column is rejected with a parser error rather than silently returning the wrong row.

Single-row `_flashback` / `_snapshot` point-lookups cut at the **transaction boundary**, not the individual event: if the row's most recent change belongs to a multi-statement transaction whose other statements commit *after* the AS OF instant, that whole transaction is excluded and the row resolves to its state *before* it — never a half-applied image that never existed at any real instant. (Full-table `AS OF` still cuts per row; see the transaction-boundary note in `query-and-recovery.md`.)

`SHOW TABLES FROM _flashback / _diff / _snapshot` returns every table in the current schema that DBTrail has schema knowledge of (the newest snapshot per table, so PostgreSQL-source indexes — which record one table per snapshot — list all their tables too). A table that was since dropped at the source still appears: its indexed history remains queryable with `AS OF`. This lets an interactive `mysql>` session explore the virtual schemas:

```sql
USE myapp;
SHOW TABLES FROM _flashback;
```

`_diff` returns the full per-PK event history within the requested window — there is no implicit row cap. If a single hot row produced thousands of events, you'll get all of them in one response; if that's too much for one query, narrow the `BETWEEN` range.

`LIMIT <n>` bounds a full-table `AS OF` resultset (`SELECT * FROM _snapshot.orders AS OF '…' LIMIT 100`). It composes with a `WHERE` clause and a column list, and it is the quickest way to *browse* a large table: a `LIMIT` at or below the row cap lets the query succeed instead of tripping the cap. Rows come back in the merge's internal order (roughly primary-key order for `_snapshot`), **not** a sorted order — add your own `ORDER BY` downstream if you need one — and a `LIMIT n` can return fewer than `n` rows when some of the first `n` were deleted by the AS OF instant. A `LIMIT` never *raises* the cap.

Full-table `_snapshot` with **no** `LIMIT`, run over a live shim (`bintrail shim`, or the MySQL port of `bintrail-console router`), **streams** its resultset row-by-row over the wire and is **not** row-capped: the baseline flows through the merge cursor one row at a time, so peak memory is proportional to the rows *changed* since the baseline, not the table size: a multi-million-row table dumps without `ER_TOO_BIG_SELECT`. If the merge fails mid-stream, the connection receives an error packet in place of the next row (no clean end-of-result), so a failed dump is never mistaken for a complete one. The 100,000-row cap still applies to the binlog-only full-table `_flashback` path (whose fetch is buffered) and to any `LIMIT`ed query; exceeding it surfaces as `ER_TOO_BIG_SELECT` (code 1104) with a hint to add a `LIMIT`, narrow the AS OF range, or add a PK filter.

DELETE events are correctly suppressed — rows that did not exist at the AS OF instant don't appear in the resultset (same semantic as Oracle's `AS OF`). For ad-hoc filtering, joins, or aggregations, pipe the resultset to `duckdb`, `pandas`, or any tool that consumes a `SELECT *` stream — the shim deliberately stays a forensic point-lookup + full-table tool, not a SQL planner.

The shim resolves the row by replaying the relevant binlog events from your DBTrail MySQL index. If the timestamp falls outside the index's retention (because hourly partitions have been rotated to S3), the shim auto-discovers the Parquet archives via `archive_state` and merges results from both sources — same machinery `bintrail query` and `bintrail recover` already use.

---

## Troubleshooting

### `ERROR 1045: Access denied for user 'app_user'@'…'`

ProxySQL is rejecting your credentials. Confirm your app is connecting with the cleartext value of `mysql_password` from `shim.yaml`. If `shim.yaml` was edited, re-apply the ProxySQL config so the regenerated SHA1 reaches the live `mysql_users` table:

```sh
rm -f proxysql-setup.sql
bintrail proxysql-config --output proxysql-setup.sql
mysql -u admin -p -h 127.0.0.1 -P 6032 < proxysql-setup.sql
```

If the username comes through but the connection still fails, check `bintrail shim`'s log: it logs which usernames are in the allowlist at startup, and a connection from an unknown username is rejected.

### `_flashback.t doesn't exist` (or query goes to MySQL instead of the shim)

The query rule isn't matching. Inspect the routing:

```sh
mysql -u admin -p -h 127.0.0.1 -P 6032 \
  -e "SELECT rule_id, match_pattern, destination_hostgroup FROM runtime_mysql_query_rules WHERE rule_id BETWEEN 990001 AND 990006;"
```

You should see six rows targeting hostgroup 991 (one each for `_flashback.*`, `_diff.*`, `_snapshot.*`, the `/*+ DBTRAIL_AT=... */` hint-comment shape, `SHOW TABLES FROM` the virtual schemas, and the end-anchored bare `... AS OF '<ts>'` shape). If they're missing, re-apply `proxysql-setup.sql`. If they're present but the query still goes to MySQL, double-check that no operator rule with a smaller `rule_id` is intercepting `_flashback.*` first (ProxySQL evaluates rules in `rule_id` order). `bintrail doctor --proxysql-admin '<admin-dsn>'` runs the six-rules check for you (advisory `WARN` when any is missing or inactive).

Note this failure mode is only *visible* for the `_flashback.*`/`_diff.*`/`_snapshot.*` and bare `AS OF` shapes, which error out when misrouted to MySQL. The `/*+ DBTRAIL_AT */` hint form produces **no error at all** on the same misroute — MySQL treats the hint as unknown-and-ignorable and returns current data (see the warning under Step 6). If time-travel results ever look suspiciously like the present, check the rules above before trusting them.

### `connection refused` on the shim's port

`bintrail shim` isn't running, or it's listening on a different port than `shim.yaml`'s `listen` directive.

```sh
systemctl status bintrail-shim
ss -tlnp | grep 3308
```

If `bintrail-shim` is dead, `journalctl -u bintrail-shim -n 100` shows why. Common causes: missing or unreadable `shim.yaml`, missing `BINTRAIL_INDEX_DSN`, a `mysql_password` value that's not a valid YAML string (quote it).

### MySQL error codes the shim returns

The shim emits typed wire codes so ORMs and monitoring can distinguish *user input* errors from *server fault* errors — a 1105 spike no longer means "any time-travel query failed". Codes you may see:

- **1064 `ER_PARSE_ERROR`** — a query mentions `_flashback` / `_snapshot` / `_diff` but doesn't match any supported shape (missing `AS OF`, missing `BETWEEN`, missing `USE <db>`, unparseable timestamp). Same code MySQL itself returns for any SQL syntax error.
- **1235 `ER_NOT_SUPPORTED_YET`**: a non-virtual-schema query reached a shim that has no copy to run it on: the standalone `bintrail shim` (typically a direct connection to `:3308` bypassing ProxySQL; hostgroup routing is misconfigured), or the MySQL port for a server with no copy on local disk yet (its snapshots only read from S3 and not mirrored yet) or whose archive access is off (the message says which).
- **1526 `ER_NO_PARTITION_FOR_GIVEN_VALUE`** — two causes, distinguished by the message. Either the AS OF or BETWEEN range falls outside what this index retains (rotated out of MySQL with no archive coverage) — narrow the time range or check `archive_state` and the shim's `--allow-gaps` flag. Or a **full-table `_snapshot`** query found the configured baseline unusable (no baseline snapshot at-or-before the AS OF instant, or a primary key the baseline merge can't canonicalize) and refused rather than return a partial table — create or re-run `bintrail baseline` so a snapshot covers the AS OF, or query `_flashback` for a binlog-only view.
- **1045 `ER_ACCESS_DENIED_ERROR`** — credential mismatch (see the section above).
- **1317 `ER_QUERY_INTERRUPTED`** — the query exceeded `--query-timeout` (message names the flag), or its client disconnected / the shim shut down mid-query. Narrow the AS OF range, filter by PK, or raise the timeout.
- **1203 `ER_TOO_MANY_USER_CONNECTIONS`** — too many concurrent full-table time-travel queries; the query waited for a slot until `--query-timeout`. Retry later or raise `--max-fulltable-queries`.
- **1040 `ER_CON_COUNT_ERROR`** — the `--max-connections` cap was reached; the connection was refused before the handshake.
- **1104 `ER_TOO_BIG_SELECT`** — a buffered full-table query (`_flashback`, or any query with a `LIMIT`) returned more than 100,000 rows. Narrow the AS OF, add a `WHERE <pk> = <value>` to fall back to the point-lookup path, or query `_snapshot` without a `LIMIT`, which streams with no cap when a baseline source is configured.
- **1105 `ER_UNKNOWN_ERROR`** — real internal failure (DB timeout, archive S3 outage, build-resultset bug). This is the catch-all "the server is broken, retry" signal; persistent 1105s warrant inspecting the shim log.

### Time-travel query returns empty

Three causes produce an empty `_flashback` / `_snapshot` resultset:

1. The row had no event at-or-before the requested timestamp.
2. The latest event at-or-before the timestamp is a DELETE — the row did not exist at AS OF (Oracle `AS OF` semantic; matches the full-table path).
3. A coverage gap or archive-fetch failure under `--allow-gaps`. Without that flag the shim returns a typed MySQL error instead — `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526) for coverage gaps, `ER_UNKNOWN_ERROR` (1105) for archive-fetch failures — so on the default strict configuration the empty resultset never indicates a gap or an archive outage.

To distinguish cases 1 and 2, query `_diff` for the per-PK history: it returns every event (including the DELETE's `row_before`), so a row that was deleted produces at least one row in the diff resultset while a row that never existed produces zero. Or check the indexer is keeping up:

```sh
journalctl -u bintrail-stream -n 200
```

The DBTrail index retains the most recent hours via partition rotation; older data is in S3 (auto-discovered via `archive_state`). See [`docs/rotation-and-status.md`](rotation-and-status.md) for rotation and archive cadence.

### Operator already has users in hostgroup 990

`bintrail proxysql-config` scopes its DELETE to `mysql_users WHERE default_hostgroup = 990` — any pre-existing user in that hostgroup will be removed when the script is applied. If you have application users you want to keep separate from DBTrail-managed routing, place them in a different hostgroup before running the script. Hostgroup 990 is reserved for DBTrail; see the comment header at the top of the generated `proxysql-setup.sql` for the full list of resources the script manages.

---

## Limitations

- **Single source MySQL per shim.** The current `bintrail shim` is one-tenant-per-instance. If you have multiple source MySQLs you want time-travel SQL against, run one shim per instance with separate listen ports and separate ProxySQL hostgroups.
- **No TLS termination on the shim port.** `bintrail shim` accepts plain MySQL protocol on `127.0.0.1:3308` by default. If you need TLS between ProxySQL and the shim, terminate at ProxySQL or via an `stunnel` sidecar.
- **`_snapshot` is baseline-aware; `_flashback` is binlog-only.** Start `bintrail shim` with `--baseline-dir <dir>` or `--baseline-s3 s3://bucket/prefix/` (the snapshots produced by `bintrail baseline`) to enable it. Both the **single-row** (`WHERE <pk> = <value>`) and **full-table** (no WHERE) `_snapshot` shapes then seed row state from the baseline at-or-before the AS OF instant and apply post-snapshot binlog events on top — so a row that existed at AS OF but was never touched within the retained binlog window still resolves, and full-table `_snapshot` returns the table's complete row state at AS OF (never-touched baseline rows, rows updated/inserted after the baseline, with rows deleted after it dropped). `_flashback` deliberately stays binlog-only — its full-table form returns only rows with binlog activity in the retained window — so the two schemas have distinct, observable semantics. With no baseline source configured, `_snapshot` degrades to the binlog-only `_flashback` behaviour (a `Debug` log notes the fallback). The baseline match is supported for **integer, `YEAR`, `DECIMAL`/`NUMERIC`, string (`CHAR`/`VARCHAR`/`TEXT`/`ENUM`/`SET`), and `DATETIME`/`TIMESTAMP`/`DATE` PKs** (the shim pins the DuckDB session to UTC so temporal-PK matches resolve deterministically on any host timezone). **`FLOAT`/`DOUBLE`, `TIME`, `BIT`, `JSON`, and spatial PK types can't be matched against the baseline** (their round-trip between the stored key and the Parquet column is unverified, so the match is refused rather than risked), and neither can a table whose primary key can't be resolved from the indexed snapshot. **`BINARY`/`VARBINARY`/`BLOB` PKs are full-table only**: the full-table shape canonicalizes keys through the offline merge and handles them since [#1155](https://github.com/dbtrail/dbtrail/issues/1155), but the single-row shape has no width to pad a fixed `BINARY(n)` key to — that key is stored padded and captured stripped — so it keeps falling back to binlog-only for the whole family (a `Warn` notes it) rather than risk silently missing a row. Use `bintrail reconstruct --pk`, which reconciles both spellings, for a single binary-keyed row. The **single-row** shape still falls back to binlog-only in these cases (a `Warn` notes the unsupported PK type). The **full-table** shape does not: with a baseline source configured, it **refuses with `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526)** instead of silently returning a partial table (only rows with binlog activity in the window, indistinguishable from a complete one) — the error message points at `_flashback` for a binlog-only view. Full-table `_snapshot` also refuses with the same code when **no baseline snapshot exists at-or-before the AS OF instant**: take a baseline covering that instant (`bintrail baseline`), or query `_flashback`. For tables keyed by an unsupported PK type, or to write the reconstructed table to mydumper-format files, use the offline `bintrail reconstruct` command (its full-table mode streams the window a page at a time and keeps a per-touched-row change map, warning past `--warn-event-threshold` — see [Memory Footprint](query-and-recovery.md#memory-footprint)). Full-table `_snapshot` with no `LIMIT` streams the reconstructed table row by row and is not row-capped; with a `LIMIT` it is buffered and bounded by the same row cap as `_flashback` (see the next limitation).
- **Full-table `_flashback`, and any `LIMIT`ed full-table query, is buffered.** These paths buffer up to 100,000 rows per query and surface overflow as `ER_TOO_BIG_SELECT` (1104). Full-table `_snapshot` with no `LIMIT` streams instead and has no row cap ([#998](https://github.com/dbtrail/dbtrail/issues/998), see [Step 6](#step-6--run-a-time-travel-query)); without a baseline source it degrades to the binlog-only path and the cap applies. PK-filtered point-lookups are unaffected.
- **`_snapshot` and `_flashback` take each row's latest change in binary log order.** That is commit order, not the time a change's statement started, so a row that two sessions changed at once (one waited on the other's row lock) comes back with the value the database holds ([#2156](https://github.com/dbtrail/dbtrail/issues/2156)). Where the index cannot show that order for some rows (see [query-and-recovery.md](query-and-recovery.md), "Most recent" is the order of the binary log), those rows are taken at their latest change by time and the result carries one warning: the client's "1 warning", and `SHOW WARNINGS` answers it with code 1105 and a text that starts with `order of changes unproven`. The single-row `_snapshot` read and both `_flashback` reads (one row or the whole table) take a row's changes in the same order, with the same warning where it cannot be shown. `AS OF` still selects changes by the time their statement started: a change that started before the instant and was committed after it is part of the answer, and with binary log order it is applied last. `_diff` lists a row's changes by statement time.
- **`_snapshot` refuses across a TRUNCATE/DROP/RENAME.** `TRUNCATE TABLE`/`DROP TABLE`/`RENAME TABLE` and MariaDB's `CREATE OR REPLACE TABLE` emit no row events, so a baseline merge spanning one of these statements would silently resurrect pre-DDL rows as if they still existed at AS OF. Both the single-row and full-table `_snapshot` paths check `schema_changes` for such a statement between the baseline snapshot and AS OF, by its time and by its binlog position (a statement indexed late carries a time from before the snapshot, [#1912](https://github.com/dbtrail/dbtrail/issues/1912)), and return `ER_UNKNOWN_ERROR` (1105) naming the DDL type and timestamp instead ([#764](https://github.com/dbtrail/dbtrail/issues/764)); re-baseline the table after the DDL to resume. `_flashback` is unaffected: it never reads a baseline.
- **`_snapshot` refuses when the source's binary log started again after the baseline.** Both `_snapshot` paths read the changes since the baseline from its binlog position. After a `RESET MASTER` (`RESET BINARY LOGS AND GTIDS`) or a failover to another server, every later change sorts before that position, so the answer would be the baseline's old rows. When the baseline records the newest change the index held (every snapshot written since [#2160](https://github.com/dbtrail/dbtrail/issues/2160)), the single-row and full-table paths run the same check as a snapshot update and return `ER_NO_PARTITION_FOR_GIVEN_VALUE` (1526), the code of the other unreadable-history refusals, with "the changes since the snapshot cannot be found by binlog position", "A new full snapshot is needed.", how to take one, and `_flashback` as the read that still answers (it reads by time) ([#2174](https://github.com/dbtrail/dbtrail/issues/2174)). The check asks one question of what the query reads: is there a change of this table, indexed after the baseline's mark, recorded between the earliest time the query reaches back to (an hour or more before the baseline) and AS OF, that sorts before the mark in the binary log? The query would drop such a change. A restart after AS OF, or one whose later changes touched only other tables, refuses nothing, so the history before it stays readable. Capture moving to another server refuses only when capture recorded the move after the baseline was taken and at or before AS OF (a new `server_uuid` at the same address, or a new server record on the way to the one capture reads now); with no record of when, it refuses. That record is the time capture noticed the new server, not the time of the switch: a failover while capture was behind, to a server whose files are numbered higher, is not seen. The first statement for a baseline and table reads that table's changes since the baseline once; later ones ask only about what was indexed since. Hours of the window that rotation already moved into Parquet archives are checked too ([#2186](https://github.com/dbtrail/dbtrail/issues/2186)), since the query reads them from the archives with the same position filter: an archive whose newest change was indexed at or before the baseline's mark is skipped (`archive_state` records it), and every other archive file of the window is read for this table, only the columns the question needs. A file found clean is not read again for the same baseline and table. An archive with no `archive_state` row is not checked: `bintrail archive reconcile --prune` removes the row of an archive whose file is gone from every backend, and from then on nothing records that the hour held a change. Not checked: a baseline without that record, which includes every snapshot made with the CLI `bintrail baseline` (only the web interface's and the daemon's snapshots and snapshot updates record it). A failed read of whether `bintrail index` wrote into the index is an error, not a skip. When the check cannot tell whether the binary log started again (`bintrail index` also wrote into the index; the event the baseline's mark names was deleted while older ones remain, as a restarted capture's cleanup does; the mark's id now names another event, after the index was rebuilt; the mark does not read; an archived hour of the window has no file the port can open), the statement answers as before and carries a warning that `SHOW WARNINGS` returns, code 1105, starting "binlog numbering not checked:" with the reason ([#2186](https://github.com/dbtrail/dbtrail/issues/2186)). The PostgreSQL wire front-end logs that note instead. The PostgreSQL wire front-end returns the same refusal with SQLSTATE `22023`, as for a coverage gap. `_flashback` is unaffected: it never reads a baseline.
- **No JOINs, aggregations, or non-PK WHERE filters inside the shim.** Run them outside on the resultset (`duckdb`, `pandas`, `awk`). The shim's job is to deliver correct historical row state; SQL execution against that state is the operator's tool of choice.
- **ENUM/SET labels are decoded with the snapshot in effect at each event.** Binlog row images store ENUMs as ordinals and SETs as bitmasks; the shim (and the web interface's Time-travel tab and `bintrail reconstruct`) map them back to labels using the schema snapshot whose capture time most recently precedes the event, so an enum reshaped between two events renders each event under its own definition. Remaining caveats: events older than the *first* snapshot decode with that first snapshot, and a change made between an ALTER and the next snapshot decodes with the pre-ALTER definition (stream mode auto-snapshots on DDL, so that window is normally seconds). An ordinal beyond the selected definition is returned as the raw number: the forensic ground truth, also visible in `bintrail query`'s JSON output, which is deliberately left unmapped.
- **ProxySQL itself is not provisioned by DBTrail.** `bintrail proxysql-config` only writes routing rules; you install and harden ProxySQL itself (admin password, frontend TLS, monitoring) using the standard ProxySQL docs.
- **The bare `AS OF` rule (990006) has a small residual false-positive surface.** The rule is end-anchored — only statements that *finish* with `AS OF '<text>'` route to the shim, so `AS OF` inside a string literal mid-query stays on passthrough (covered by an e2e guard test against real ProxySQL). The irreducible residue: a benign statement whose **final token** is a string literal of the exact form `AS OF '<text>'` would route to the shim and fail (the shim has no passthrough). If you hit that in practice, parenthesise or reorder the predicate — or delete rule 990006 from `mysql_query_rules` and use the `_flashback.`/hint forms instead. Note ProxySQL's `$` anchor assumes the default `re_modifiers` (CASELESS, no multiline); adding `GLOBAL`/multiline modifiers to the rule weakens the anchor to end-of-line.
- **The bare `AS OF` form is `*`-only and trailing-only.** Column lists stay on the `_flashback`/`_snapshot` virtual schemas, and the AS OF clause must end the statement (an AS-OF-before-WHERE variant would forfeit the end anchor — the false-positive defense above). The bare form rewrites to `_flashback` (binlog-only); for baseline-aware lookups use `_snapshot`.
