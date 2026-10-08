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

1. **Embedded in `bintrail-console watch` — one port for every monitored server
   (multi-source).** If you already run the daemon, turn the port on in the
   web interface (**Connect a SQL client**), or start it with
   `--flashback-listen`, and it serves `_flashback` / `_snapshot` / `_diff` for *every* server in the
   web interface, routed by the connection username. No separate `bintrail shim`
   process, no hand-built index DSN. See [the embedded port](#the-embedded-port-multi-source)
   below. Start here if you run `watch`. This port also answers **ordinary
   SQL on the copy** — joins, aggregations, anything the console's SQL card
   takes — see [Ordinary SQL on the copy](#ordinary-sql-on-the-copy-embedded-port-only).
2. **A dedicated terminal — point `mysql` straight at a standalone shim (no
   ProxySQL).** Simplest for a single index. The shim already speaks the MySQL
   protocol, so an analyst connects a `mysql` client directly to it and runs
   `_flashback` / `_snapshot` / `_diff` queries. Trade-off: that connection
   answers *only* time-travel queries (a normal `SELECT` against a real table
   returns `ER_NOT_SUPPORTED_YET`, 1235; the standalone shim has no copy to
   read). Use it when a person or tool just needs to read historical state
   from one index.
3. **Transparent routing — ProxySQL in front (the rest of this guide).** Needed
   only when an application's *normal* connection must mix live queries and
   `AS OF` queries on the same endpoint. ProxySQL routes virtual-schema queries
   to the shim and everything else to your real MySQL.

All three speak the **MySQL** protocol. A **PostgreSQL** operator can instead run
single-row `AS OF` over the **PostgreSQL wire protocol** from `psql` with
`bintrail-pg flashback` — same grammar, no MySQL client required. See
[Interactive `AS OF` from `psql`](postgres.md#interactive-as-of-from-psql).

### The embedded port (multi-source)

`bintrail-console watch` already holds the time-travel engine and, through its
control plane, a resolved connection to every monitored server's *own* per-source
index (`bintrail_idx_<id>`). `--flashback-listen` exposes a MySQL-protocol port
wired to that same map, so one endpoint time-travels any server you monitor —
no `bintrail shim` container, no `--profile flashback`, no per-source DSN to
discover:

```sh
bintrail-console watch \
  --index-dsn 'root:pw@tcp(127.0.0.1:3306)/bintrail_index' \
  --console-token "$BINTRAIL_CONSOLE_TOKEN" \
  --flashback-listen 127.0.0.1:3308            # or env BINTRAIL_CONSOLE_FLASHBACK_LISTEN
```

**Or turn it on in the web interface, with no flag and no restart.** When the
daemon was started without `--flashback-listen`, the **Connect a SQL client**
panel has an address field and a **Turn on** button. Turning it on creates the
port's own password and shows it once; **New password** replaces it, and
**Turn off** closes the port. The setting and the password are kept in
`console-mysql-port.yaml` beside the servers file, so they survive a restart.
That file holds the password as the client types it (MySQL-protocol
authentication needs it that way), readable only by the user DBTrail runs as.
An address given with the flag or the environment variable decides instead,
and the panel then shows it as fixed. A newly installed Docker Compose stack publishes the port
on the host loopback as 3309 (an older install needs the `ports:` line added to
its docker-compose.yml); turn it on with the address
`0.0.0.0:3309` (the default offered there) and connect from that machine, or
through a tunnel from another one.

**Routing is by username, auth is the access token.** Connect as the target
server — its registry **ID** (robust; the `X-Bintrail-Server` value shown in the
web interface) or its display **name**, with the access token as the password:

```sh
# the web interface shows each server's id/name in the switcher
mysql -h 127.0.0.1 -P 3308 -u 7f4d577430b48821 -p"$BINTRAIL_CONSOLE_TOKEN"
mysql> USE myapp;   -- optional: seeded from the server's source DSN when known
mysql> SELECT * FROM _flashback.orders AS OF '2026-05-02 10:00:00' WHERE id = 12345;
```

### Ordinary SQL on the copy (embedded port only)
The same connection runs ordinary read-only SQL over the server's Parquet
copy, the snapshots and the archived change log, exactly as the console's
**SQL** card does: same locked DuckDB child process, same views, same caps
(2 threads, 2 GB by default, 60 seconds, 1,000 rows, one query at a time per server).
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
  the statement is what the SQL card takes. `DATE_FORMAT`, `GROUP_CONCAT` as
  MySQL spells them, and backtick-quoted names are not understood; DuckDB's
  `strftime`, `string_agg` and double quotes are. `USE <schema>` works and sets
  where unqualified names resolve; the connection starts in the server's
  source database when the registry knows it. Schema and table names match
  without regard to the case of ASCII letters (`USE SHOP` and `FROM Orders`
  find `shop` and `orders`), which MySQL on Linux does not do by default
  (`lower_case_table_names=0`), so a statement that names a table in the
  wrong case works here and fails there. Letters outside ASCII are not
  folded: a schema `Été` is not found as `été`, and a `USE` of a name that
  is not found leaves the connection where it was.
- **`events` needs the change log on local disk, and the bundled stack keeps
  it in S3.** `watch` uploads each archived hour to the server's S3 location
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
  ([Dashboards](dashboards.md)). With read routing on, an ordinary
  statement is planned on MySQL first, so `events` there means a table of
  that name on the source, never the change log.
- **Tested with the `mysql` command-line client.** A graphical client
  (DBeaver, Workbench) probes `information_schema` the MySQL way and may show
  an incomplete table tree; that has not been tested.
- **How many at once.** Two statements run at once per daemon by default
  (`--sql-max-in-flight` raises it on a host with cores to spare), shared between
  the SQL card and this port, and **one at a time per server on this port**:
  two people querying the same server at the same moment means the second one
  waits for the first to finish. A statement waits up to 30 seconds for its
  slot and then gets MySQL error 1203 ("SQL on the copy is busy"); with 16
  statements already waiting it gets 1203 at once. A client that disconnects
  while waiting, or while its statement runs, leaves at once and frees its
  place. The line of 16 is one for the whole daemon: statements from the SQL
  card wait in it too. With read routing on, a statement that would get 1203
  is answered by MySQL instead (see
  [A busy copy](#a-busy-copy-the-read-waits-its-turn)). The wait is in the
  metrics: `bintrail_sql_slot_wait_seconds` and `bintrail_sql_slot_waiting`
  ([Observability](observability.md)). The daemon that serves them is the one
  capturing changes, which is why the limits are small. A
  statement past its memory (2 GB by default) goes on using a temporary
  folder of its own on disk, more slowly, up to four times its memory (8 GB
  by default; the folder is under the system's temporary directory and is
  removed when the statement ends), and fails only past that. One whose tables have more than 48 MB of changes not merged into
  them yet is answered from the newest earlier copy in which they fit, with
  a warning (SHOW WARNINGS) naming that copy's time; with no such copy it is
  refused before it runs. Under read routing it is not answered from an
  earlier copy: it goes to MySQL like any other refusal. A statement that
  reads `events` is not either, since the change log is not pinned to a
  copy. On a host with memory to spare, `--sql-memory`
  (env `BINTRAIL_CONSOLE_SQL_MEMORY`, at least 512MB) raises both: the line
  of changes moves with it, 96 MB at 4GB. Without the flag it can be set in
  the web interface (Settings, MCP Server, Memory for SQL on the copy) and
  applies to the next statement, no restart; the flag, when given, wins. The
  SQL card shows the memory and the line in force. Each statement can take that much at once, times
  `--sql-max-in-flight`, on the host that captures. For a team or a
  dashboard tool, each reader's own DuckDB on the bucket is the way to scale
  reads (see [Dashboards](dashboards.md)): it runs on the reader's machine and
  adds no load to the capture host. The trade-off: bucket permissions replace
  the console's access rules, and the data is as fresh as the last snapshot
  and the last archived hour. The daemon's log at debug level, the
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

  Under read routing none of these reaches the client, because MySQL
  answers such a statement: see below. The SQL card has no routing: it
  answers from the copy, with these differences. A views file that follows later snapshots
  (the default of `bintrail views`, see [Dashboards](dashboards.md)) keeps
  the files' order, sorted by name, for every table.
- **`SET time_zone`, `SET sql_select_limit` and `SET sql_mode` are applied
  or refused, never ignored.** They used to be answered with an empty OK and
  read by nothing, so a client that had set its zone got answers computed in
  UTC. On a connection that runs ordinary SQL on the copy (not under read
  routing, where every `SET` is MySQL's) each one is now handled, in every
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
  answer.** The statement fails with error 1104, which names the cap (1,000
  rows by default) and the way out: add a `LIMIT` at or under the cap, or
  narrow the statement. Nothing is returned, so an application cannot take
  the first 1,000 rows for the whole result. A result of exactly the cap's
  size is whole and is returned. The one cut that is not an error is the one
  the connection asked for: after `SET sql_select_limit = N`, with N at or
  under the cap, a `SELECT` with no `LIMIT` of its own returns its first N
  rows, silently. With N above the cap the cap still applies, and a result
  past it is the same error, which says the limit is above the cap. A
  listing (`SHOW TABLES`, `DESCRIBE`) takes neither a `LIMIT` nor the select
  limit, so its error points at `information_schema`, which does. (Under
  read routing a result past the cap is not an error: MySQL answers that
  statement instead.) A cell longer than the cell cap (1 MiB) is cut and
  marked, and raises a warning the client counts; `SHOW WARNINGS` reports
  it. The console's SQL card keeps its own behaviour: it returns the first
  rows up to the cap and says on the page that there were more.
- **The copy has to be on local disk**, as for the SQL card. A server whose
  copy is only on S3, or with archive access disabled, or whose copy defines
  no view yet, keeps the time-travel shapes and refuses ordinary SQL with
  1235 and the reason.
- **A current database the copy does not have** (the server's source
  database before its first snapshot, a typo in `-D`) is left alone:
  unqualified names then resolve in `main`, `SHOW DATABASES` and `events`
  keep working, and a name that fails to resolve says which database is
  missing from the copy.
- **Access is the token's, all or nothing.** The port authenticates on the
  console token, which has no data profile and no table or column rules, so
  none apply here — the same rule the SQL card follows for a token session.
  Give this port to people who may read every table of every server.
- Every statement is written to the audit trail when one is installed
  (`shim` / `sql.run`: the statement, the schema, the row count).
- Column types are mapped to MySQL's (DuckDB `INTEGER` arrives as `BIGINT`,
  `DECIMAL(p,s)` as `DECIMAL`, `TIMESTAMP` as `DATETIME`, `BOOLEAN` as 1/0,
  a `LIST`/`STRUCT` as JSON text, `MAP`/`HUGEINT`/`UUID`/`INTERVAL` as text).

### Read routing: MySQL answers, the copy takes the heavy reads (experimental)
With `--route-max-copy-age` set (for example `--route-max-copy-age 15m`), a
connection on this port to a server that has a source DSN forwards every
statement to that MySQL over one upstream connection per client connection,
with the server's forwarding account when it has one and the source DSN's
credentials otherwise, and MySQL's own answer comes back, errors
included; resultsets are streamed to the client as they arrive, never held
in the daemon. The one exception is a `SELECT` whose plan says it is
expensive: it runs on the copy, and if the copy rejects it (DuckDB does not
know the syntax, the table is not in the copy, the result exceeds the
port's row or cell cap), MySQL runs it. A copy that is busy does not reject
it: the read waits its turn, and reaches MySQL only after 30 seconds or when
16 statements are already waiting
([A busy copy](#a-busy-copy-the-read-waits-its-turn)). Nothing the client sends
needs to change. **This is experimental**: for what MySQL answers, the
behaviour is MySQL's; for what the copy answers, it is DuckDB's, and the
list below of where the two differ is what the feature's own testing is
still growing.

```
bintrail-console watch ... --flashback-listen 127.0.0.1:3308 --route-max-copy-age 15m
mysql -h 127.0.0.1 -P 3308 -u <server> -p<token> shop
mysql> SELECT * FROM orders WHERE id = 42;                    -- MySQL (a point lookup)
mysql> SELECT status, count(*) FROM orders GROUP BY status;  -- the copy (a full scan)
mysql> SELECT GROUP_CONCAT(status) FROM orders;              -- MySQL (the copy would answer differently)
mysql> UPDATE orders SET status = 'paid' WHERE id = 42;      -- MySQL
```

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
   (the vetoes of step 4) was run against MySQL; on a MariaDB source read
   routing is as experimental, and less measured.
   Last, for a plan the copy should take, **the size of the result**. The
   copy returns at most its row cap (1,000 rows by default) and refuses a
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
6. For a plan the copy should take, how fresh the copy is. Its snapshot's
   age unknown: **MySQL**. Its snapshot at most `--route-max-copy-age` old:
   on to the next step. Older than that: the copy still answers when every
   table the statement reads has had no change since its snapshot, and
   **MySQL** answers otherwise (see
   [Past the limit](#past-the-limit-tables-that-have-not-changed) below).
   (Freshness is checked after the plan on purpose: it costs a snapshot
   listing, and past the limit a few reads of the index, which the cheap
   reads must not pay.)
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
   once per connection (`read routing: the copy does not answer on this
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
   DBTrail's log says so once per server, at warn level (`read routing:
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

#### Past the limit: tables that have not changed

`--route-max-copy-age` is one age for the whole server: the age of the newest
snapshot. A table nobody has written since its snapshot reads the same on the
copy as on MySQL however old that snapshot is, and a table written a second
ago does not, whatever the server's age says. So when the snapshot is older
than the limit, the port does not send every heavy read to MySQL. It asks,
for that statement, whether the tables it reads have changed since their
snapshot, and the copy answers when none has.

The tables are the ones the statement names, as the copy reads it. Every one
of these must hold, and when any does not the statement goes to MySQL, as it
did before:

- **Capture is known to be up to date, as of a moment within the limit.**
  The index only knows the changes capture has delivered, so "the index holds
  no change of this table" means nothing while capture is behind or stopped.
  DBTrail asks the source what it has executed (its GTID set) and compares it
  with the position capture has saved. When capture's position includes
  everything the source had at one of those reads, capture was complete as of
  that read. That moment has to be at most `--route-max-copy-age` ago. The
  limit therefore keeps its meaning: the copy's answer is never older than
  it.
- **The index holds no change of the table since its snapshot.** "Since" is
  decided by binlog position, not by time: each table file of a snapshot
  records the position it was read at, and the index is asked for a row
  change of that table at or after that position, the same question a
  snapshot refresh asks. A transaction that ran shortly before the snapshot
  and committed after it counts as a change. (The index is searched from an
  hour before the last change the snapshot holds of that table, or before
  the full read its rows came from, on the source's own clock, so a refresh
  taken while capture was behind does not hide what capture indexed later.
  A statement that began earlier still, and committed after the snapshot, is
  dated before that hour: when the index holds any change of that kind, the
  table's older changes are searched too, by position alone. An index that
  `bintrail index` has ever loaded binlog files into cannot tell that from
  one row, so there the older changes are searched on every statement: a
  table with a long history in the index then runs into the two-second
  budget and follows the age rule.) So does any
  schema change that names the table (`ALTER`,
  `TRUNCATE`, `DROP`, `RENAME`), which changes a table without a row change:
  those are placed by position alone, however long the statement ran.
- **The index still holds everything since that position.** If rotation has
  already dropped the partitions that cover it, changes may exist that the
  index no longer has. A table whose file was carried over from an older
  snapshot is judged from that file's own position, not from the newest
  snapshot's. This is the limit to know: a snapshot refresh leaves a table
  with no changes exactly as it was, position included, so a table that has
  been quiet for longer than the index keeps its changes (`--rotate-retain`,
  48 hours unless set) cannot be vouched for from the index, and follows the
  age rule. A full snapshot gives every table a new position.
- **Capture lost nothing in between.** A binlog gap, or an event capture read
  and dropped (the capture health on the Overview), since the table's rows
  were last read from the source, and the copy does not answer.
- **The table can only change through its own row changes.** A table with a
  foreign key that says `ON DELETE` or `ON UPDATE` `CASCADE`, `SET NULL` or
  `SET DEFAULT` changes when its parent does, and MySQL does not write those
  changes to the binlog. Such a table always follows the age rule. So does a
  table outside what capture records (`--schemas`, `--tables`, a server's
  schema filter).
- **The snapshot is one DBTrail can vouch for**: taken at one point in time
  (not with the `no-lock` mode, which reads each table at a different
  moment), with its binlog position recorded, and not built over a known gap
  in capture.
- **The source's binary log kept one numbering since the snapshot.** After a
  `RESET MASTER`, a failover to another server or a new `log_bin` name, the
  source's binary log starts again, and its changes sort before the
  snapshot's binlog position, where "no change after the position" proves
  nothing. The copy does not answer when a change reached the index after
  the one the snapshot records as its newest (its event mark) and sorts
  before it, when the newest change sorts before the position of a snapshot
  a refresh wrote, or when capture found another server at the source's
  address.

What the answer is, then: what MySQL held for those tables at the moment
capture was last confirmed complete. A change the source makes after that
moment is not in it, exactly as a change made after a young snapshot is not
in the copy's answer under the age rule. The next statement over that table
goes to MySQL once the change reaches the index.

This rule trusts the index to hold every change capture was given. What the
index never received, and has no record of not receiving, it cannot see:

- a write made with binary logging off (`SET sql_log_bin = 0`);
- a statement-format write issued while the default database is a system
  schema or one outside the capture's filters;
- a table excluded from capture and included again in between, and a capture
  restarted from a later position than it had reached;
- a row capture could not decode and skipped with only a warning in the log
  (a value in a legacy character set that does not convert);
- a schema change (`TRUNCATE` included) whose record could not be written to
  the index at that moment, which is also only a warning in the log;
- a gap in capture whose record was cleared by stopping capture on that
  server (Stop clears it) before a new full snapshot was taken;
- a change a snapshot refresh itself left out of the file it wrote: a
  refresh that ran while capture was more than about two hours behind can
  miss changes it should have folded in. The position in that file's footer
  says they are in it, so nothing after it looks changed;
- a statement that began before the oldest hour the index still holds and
  committed after the table's snapshot, when rotation dropped that hour
  WITHOUT archiving it. While the hour it began in is in the index the
  statement is found, however long it ran. When rotation archives the hour
  before dropping it, the archive's record of the newest change it holds
  says so, and the statement goes to MySQL (below); dropped with no archive,
  and until rotation also drops the hour of the snapshot, it is not found.
  The same goes for a change dated that far back because the source's clock
  runs behind.

None of these is new, and the copy was already wrong about such a change
before this rule. The copy's age is the stamp of its newest snapshot, and a
scheduled snapshot is, whenever it can be, a refresh built from the index
with no read of the source. A refresh renews the age and still lacks what
the index never received. So such a change stays missing from the copy until
the next FULL snapshot, the one that reads the source again, under the age
rule and under this one alike.

What this rule adds is the stretch in which the copy answers at all. Under
the age rule alone the copy stops answering heavy reads when its newest
snapshot passes the limit, and starts again at the next snapshot. Under this
rule it also answers in between, for tables the index shows no change of.
Where every scheduled snapshot is a refresh, this makes no difference to how
long such a change is missing. Where every snapshot is a full read, or
nothing is scheduled, it does make one: the limit used to cap how stale the copy's
answer about such a change could be, and for these tables it no longer
does. On a source where those changes happen, taking FULL snapshots more
often is what shortens it; refreshing more often does not, under either
rule. A source that filters its binary log (`binlog-do-db`,
`binlog-ignore-db`) is detected, and this rule is never applied to it.

When it applies and what it costs:

- Only a MySQL source captured in GTID mode (checked on every statement: a
  capture restarted in binlog-position mode, or from an earlier point and
  not yet back where it was, stops being vouched for at once),
  whose index is not on the source server itself. A MariaDB source, a source in binlog-position mode, one with
  tagged GTIDs, one that filters its binary log, and one that executed
  transactions capture never read (a dump loaded with
  `SET @@GLOBAL.gtid_purged`) always follow the age rule. The account DBTrail
  captures with needs `REPLICATION CLIENT` on the source, which capture
  already requires, to read the binary log's filters.
- Only statements whose plan is expensive, and only past the limit. A cheap
  read, and any read while the snapshot is within the limit, costs nothing
  more than before.
- The source is asked for its GTID set at most once every 30 seconds per
  server, the same read the Overview makes to say whether capture is up to
  date. The statement that triggers a read waits for it (two short
  connections, bounded at three seconds) before it is decided. On a source that is being written, one read is not enough: capture
  saves its position every few seconds, so the source is always a little
  ahead. The first read leaves a sample and a later read confirms capture
  reached it. The first heavy read after a quiet spell therefore goes to
  MySQL, and the ones that follow half a minute later go to the copy. With a
  limit under about a minute this rule rarely applies.
- Each such statement reads the index a few times (capture's state, the
  partition list, the schema changes since the snapshot, and two lookups per
  table). Measured on an index of 3 million events over a week of hourly
  partitions, on MySQL 8.4: about 5 ms in all for one table and 7.5 ms for
  three, whether they changed or not. There are two slow cases. One is a
  table that received a very large load in the two hours before its
  snapshot and nothing since: the lookup walks those index entries every
  time. For half a million of them that took 0.25 s when the table's file
  came from a snapshot refresh and 1.5 s when it came from a full read of
  the source (a refresh records how far into the index it got, which lets
  MySQL skip the rows without reading them; a full read has nothing to
  record). The other only exists while the index holds a statement that
  began long before some snapshot and committed after it: then each table's
  older changes are searched as well, across everything the index still
  holds of that table. That costs nothing for a table with few of them and
  grows with their number: 1.5 s for half a million with a full read's file
  (0.25 s with a refresh's), so a table with more than about 700,000 older
  changes and a full read's file does not fit. All the reads of one
  statement share a two-second budget; past it the statement goes to MySQL,
  so a statement over two such tables at once is not vouched for either.
  Once a table is found changed after its snapshot, that is remembered for
  that snapshot file and the index is not asked again: the statements that
  follow go to MySQL at once.
- The archives rotation wrote are read too. A change indexed late lands in an
  old hour, the next one rotation archives and drops, and then the index no
  longer shows it. Each archive records the newest binlog position it holds,
  so the statement goes to MySQL when an archive of an hour older than the
  lookups reach, written since the table's snapshot (less an hour for clock
  differences), holds a change at or after the snapshot's position, does not
  record its newest position (written by an older version, or registered by
  `archive reconcile --repair` or `restore-index`), or records it in a binary
  log with another base name. An archive holds every table of its hour, so
  another table's late change in it counts as well. Only the archives written
  since the oldest of the statement's snapshots are read: measured with a
  year of hourly archives (8,760) on MySQL 8.4, 0.5 ms a statement; on an
  index no version with this check has migrated yet (it adds an index on
  `archive_state.archived_at`), about 3 ms. An index with no `archive_state`
  at all, or one that cannot be read, is not vouched for.
- The statement takes one of the copy's slots while it is checked, as a heavy
  read within the limit does.

A statement the copy answers this way is counted under `tables_unchanged`
(below), apart from `expensive_plan`, so a copy that keeps answering from an
old snapshot is visible. One that goes to MySQL is counted under
`copy_too_old` as before, and the debug log line of that decision says which
table changed or what could not be confirmed.

What this is and is not:

- **The copy's answer is as fresh as its snapshot.** A heavy read served
  from the copy does not see what changed since the last snapshot; that is
  what `--route-max-copy-age` bounds, and why there is no default that turns
  this on. Past that age the copy only answers over tables with no change
  since their snapshot, and then its answer is what the source held at a
  moment no longer ago than the same limit (next section). A `SELECT` that
  must see the last second belongs in a transaction, which MySQL always
  answers.
- **The copy's grants are nobody's; the forwarded ones are the registry's.**
  Forwarded statements run with the server's forwarding account, or with
  the source DSN's account when the server has none. Give this port to
  people who may do on MySQL whatever that account can. To bound it, give
  the server a forwarding account with only the grants the port should have
  and start the port with `--route-read-only` (both below).
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

  What a client can still see through the rewrite: the copy finds a column
  whatever the case it is written in, as MySQL does, but names the result
  column as the table stores it, where MySQL names it as the statement
  wrote it (``SELECT `ID` `` returns a column called `ID` on MySQL and `id`
  on the copy; the same holds without quotes). An alias keeps the case it
  was written in on both, with two exceptions: MySQL drops the spaces at
  the start of an alias (``AS ` a` `` is `a` on MySQL and ` a` on the copy),
  and a column of a subquery selected in another case than its alias was
  declared in (`` `a` `` over ``AS `A` ``) is named as selected on MySQL and
  as declared on the copy. The plain port without read routing translates
  nothing: a client there writes the copy's dialect.
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
    read at that moment. The daemon's log says so once per table (`this
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
  snapshot, and under read routing it declines and MySQL answers
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
  enough for the last three. So, under read routing, the copy declines and
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
  Start with the defaults, read the daemon's log (every decision is logged
  with its reason at debug; a copy that never answers, a failing EXPLAIN or
  an unreadable snapshot time is a warning, once per connection) and the
  audit trail (a copy-served statement carries `route: copy` and the
  reason), and move the thresholds.
- **One query at a time per server on the copy, still.** A heavy read that
  arrives while the copy is busy is not refused, and it does not go to MySQL
  either: it waits its turn on the copy. See
  [A busy copy](#a-busy-copy-the-read-waits-its-turn) below for the rule and
  its numbers. The limits of the section above are the copy's; MySQL's are
  MySQL's.
- **The port accepts writes under routing.** `INSERT`, `UPDATE`, `DELETE`,
  DDL, `GRANT`: everything that is not a `SELECT` reaches the source. On a
  server with no forwarding account that is the registry's source account,
  the one the daemon captures with: anyone holding the access token can
  then do on the source what that account can, including killing the
  capture's own connection or changing its password. Each forwarded write
  is logged at info level with its leading keyword (never the statement).
  Two settings close this, and they are meant to be used together: a
  forwarding account, so the port has its own grants, and
  `--route-read-only`, so a write is refused before it is sent (the next
  two points).
- **A forwarding account gives the port its own grants.** Each server can
  carry an optional second account on its source, used only by this port.
  With one set, everything the port does on that server's MySQL (forwarded
  statements, the `EXPLAIN` the decision reads, prepared statements, `USE`)
  runs as that account, and the capture account is never opened by the
  port. Its grants are then the most the port can do there:

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
  that changes the address or account the port forwards with (the
  source's own, on a server with no forwarding account), or deleting the
  server. Clients see a lost connection at once, reconnect, and get the
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
  client** panel says which account was refused and the source's own
  error, until a connection logs in again or a **Test connection** of
  that forwarding account works. An expired password on MariaDB is
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
- **`--route-read-only` makes the routed port read-only.** With it (or
  `BINTRAIL_CONSOLE_ROUTE_READ_ONLY=1`), a statement that is not a read is
  refused with MySQL error 1290 and a message that names the flag, and it is
  never sent to the source, not even to be explained or prepared. `SHOW
  WARNINGS` right after shows that refusal. The flag needs
  `--route-max-copy-age`; `watch` refuses to start with it alone, before it
  connects to anything.
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
- A server with no source DSN (the daemon's own command-line source, a
  server registered without one), or whose copy is unavailable, keeps the
  copy-only behaviour and says so once per connection in the daemon's log.

Servers added in the web interface mid-session are reachable immediately (the registry
is read live). A token is **required** — MySQL-protocol auth cannot use the
web interface's password store, so set `--console-token` / `BINTRAIL_CONSOLE_TOKEN`;
`watch` refuses to open the port otherwise. The default `127.0.0.1` bind keeps
it host-local; do not expose it to untrusted networks.

**The port does not filter by schema.** Anyone with the access token can read
the full history of every schema on every monitored server through it. There is
no `allowed_schemas` here. When you need per-schema filtering, run the standalone
`bintrail shim` (or `bintrail-pg flashback`) instead, with one tenant per
application and an `allowed_schemas` list on each (see
[Isolating tenants with `allowed_schemas`](#isolating-tenants-with-allowed_schemas)).

The web interface shows all of this on **Settings → MCP Server**, in the **Connect a
SQL client** panel: whether the port is on, its address, the user and password
rules, and a ready-to-copy `mysql` line for the server picked in the sidebar
(the token itself is never displayed). When the port is off, the panel turns
it on; when its address was given at startup, the panel shows it as fixed.

`_snapshot.*` parity: each server reads the baseline configured on its registry
entry (or the daemon's `--baseline-dir` / `--baseline-s3`), exactly as the
web interface's Time-travel tab does, with one edge: a server configured with *both*
a local `--baseline-dir` **and** an `--baseline-s3` copy reads `_snapshot` only
from the local dir on this port. If local baselines have been pruned (retention)
while a durable S3 copy remains, use the web interface's Time-travel tab or a standalone
shim pointed at the S3 prefix for those tables. Single-source baseline configs —
the common case — have full parity.

#### What the port tells a driver about its session and about the server
MySQL sends two bytes of status with its handshake and with every answer:
whether the session is in autocommit mode, whether it is inside a
transaction, and a few more. Drivers act on them. PyMySQL and mysqlclient
send `SET AUTOCOMMIT` only when the last status says the mode differs from
the one they want; Connector/J with `useLocalTransactionState` skips a
`COMMIT` the flags say is not needed; Connector/J, the C library and the Go
driver read `NO_BACKSLASH_ESCAPES` from them to decide how to escape a
string. The port's flags describe the session the client really has:

- **Under read routing**, the session on the source, as the source last
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
  when it connects, or a port started without read routing.
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
    `rollback()` then behave as on MySQL. Two things follow under read
    routing. Every statement of such a connection, reads included, runs
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
  answer of a session on the source shows a state that differs: `read
  routing: the source opens its sessions with autocommit off, and the
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
What a client reads by asking is the source's own under read routing
(`SELECT VERSION()`, `@@version` and `@@version_comment` are forwarded, so a
MariaDB source answers `11.4.x-MariaDB`), and the port's own on a
connection that is not routed (`@@version` is `8.0.11`, `@@version_comment`
is `DBTrail time-travel port`). The consequence is for a driver that
chooses its behaviour from the handshake version rather than from a query:

- Connector/J picks the names of the variables it reads when it connects
  from that version: told 8.0.11 it asks for `@@transaction_isolation` and
  not `@@tx_isolation`, and leaves out the query cache variables. MySQL 5.7.20
  and later, MySQL 8 and MariaDB 11.1 and later know that name; under read
  routing, an older MariaDB (10.11 for example) or MySQL source does not,
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
as the daemon applies it:

- **How many run.** Two statements run on the copy at once for the whole
  daemon (`--sql-max-in-flight`), and on this port one at a time per server.
  So sixteen connections reading the same server share ONE place on the
  copy, whatever `--sql-max-in-flight` says.
- **The rest wait.** A read that finds its place taken waits until one comes
  free, then runs on the copy. The turn is not strictly in order of arrival:
  when a place comes free, every waiting statement that can use it tries,
  and one gets it.
- **Up to 16 wait, for at most 30 seconds.** The line holds 16 statements
  for the whole daemon, every server and the web interface's SQL card
  together. A read that arrives with 16 already waiting does not wait:
  MySQL runs it at once (`copy_queue_full` in the counter). A read that has
  waited 30 seconds without getting a place is run by MySQL then
  (`copy_wait_timeout`), so its client waited 30 seconds plus MySQL's own
  time. Neither number is a setting.
- **What else ends a wait.** The client disconnecting: the statement leaves
  the line at once and nothing runs. The daemon stopping does the same.
- **What a waiting read holds.** Its client connection and its idle
  connection to MySQL, nothing else: no place on the copy, no process, no
  memory worth counting.
- **Waiting does not exempt a read from the copy's other refusals.** A read
  that waited, ran on the copy, and was then refused there (a result over
  the row cap, a construct DuckDB lacks) is run by MySQL afterwards under
  that refusal's own reason: it paid the wait, the attempt and MySQL's time.

Why waiting and not MySQL: measured with 16 connections sending a mix with
heavy reads to one server ([#2080](https://github.com/dbtrail/dbtrail/issues/2080)),
no read left the line for MySQL, the mean wait for the copy was 1.6 seconds,
and the median heavy read took 1.2 seconds through the port, wait included,
against 22 seconds sent straight to MySQL. Waiting for the copy was far
faster than not waiting. At 64 connections the line overflowed: about 8 % of
the heavy reads went to MySQL and piled up there, with a 99th percentile of
94 to 120 seconds, while the median heavy read through the port stayed at
1.2 to 1.4 seconds.

What to watch ([Observability](observability.md)):
`bintrail_sql_slot_wait_seconds` is how long statements wait and how each
wait ended, `bintrail_sql_slot_waiting` is how many wait right now, and
`copy_queue_full` and `copy_wait_timeout` in
`bintrail_read_routing_decisions_total` (and in the "Who answered" block of
the web interface) count the reads that reached MySQL because the copy was
busy. Those two climbing means this port is asked for more heavy reads than
one copy serves: point some readers at their own DuckDB on the bucket
([Dashboards](dashboards.md)), or accept that the overflow runs at MySQL's
speed.

#### Seeing who answered
Two surfaces count every routing decision, per server, since the daemon
started — decisions, not successes: a statement MySQL then fails was still
MySQL's.

- The web interface: on **Settings → MCP Server → Connect a SQL client**, the
  block *Who answered* shows, for the server picked in the sidebar, how many
  statements MySQL answered and how many the copy did, the rule in force, and
  (under *Why each side*) a count per reason. With routing off it says so and
  names the flag. **Refresh** re-reads the counts. The same numbers are in
  `GET /api/flashback` under `routing`.
- Prometheus (`watch --metrics-addr`):
  `bintrail_read_routing_decisions_total{server, route, reason}` — `server`
  is the registry id, `route` is `copy`, `mysql` or `refused` (a statement
  the read-only port did not run), and `reason` is one of
  a closed set: `expensive_plan` (the copy answered, its snapshot within
  the limit), `tables_unchanged` (the copy answered although its snapshot is
  older than the limit, because the tables the statement reads have not
  changed since their snapshot), `cheap_plan`, `bounded_limit` (a small `LIMIT` MySQL answers
  without reading past it), `result_over_row_cap` (an expensive plan whose
  result is estimated above the copy's row cap: MySQL answered, and the copy
  was not tried), `not_a_select`, `write`, `session_setting`,
  `session_differs` (the source's session on that connection holds a setting
  the copy does not reproduce; DBTrail's log names the setting and its
  value once per connection), `connection_pinned` (a `CREATE TEMPORARY
  TABLE`, `LOCK TABLES` or `PREPARE` ran earlier on that connection),
  `in_transaction`, `veto`, `explain_failed`,
  `copy_age_unknown`, `copy_too_old`, `copy_refused`, `copy_queue_full` (the
  copy was busy and 16 statements were already waiting for it, so this one
  did not wait), `copy_wait_timeout` (the copy was busy and the statement
  waited 30 seconds for its turn), `copy_columns_differ`
  (the copy works and declined a `SELECT *` or a `NATURAL JOIN` over a table
  whose columns there are not MySQL's, a statement that names a column
  the copy does not hold, one that does arithmetic on a date column, or one
  that names a `TIME` or `YEAR` column), `show_warnings`,
  `upstream_lost` (nobody answered: the port's connection to the source is
  lost or could not be opened, and the client got error 2006),
  `read_only` (refused: not a read, on a port started with
  `--route-read-only`; the client got error 1290), and
  `routing_off` (listed for completeness: a daemon binds the router only
  with routing on). A copy that never answers shows up as `copy_refused`,
  `copy_too_old`, `copy_age_unknown` or `session_differs` climbing while
  `expensive_plan` and `tables_unchanged` stay flat; `explain_failed` climbing means MySQL refuses to `EXPLAIN` what the
  client runs (a table it cannot see, a statement it cannot plan);
  `upstream_lost` climbing means the source is unreachable or the registry's
  source credentials are wrong. Each routed statement counts exactly once,
  a `USE` sent as a statement included. Not counted, because they are no
  routing decision: the schema seeded when the connection opens and the
  mysql client's `\u` (sent as `COM_INIT_DB`, not as a statement), `SHOW
  WARNINGS` right after a statement the copy answered (the copy's own
  warnings), and the `_flashback`/`_snapshot`/`_diff` time-travel shapes.

A server whose connections cannot route — no source database to forward to
(the command-line boot entry, a server registered without a source), or no
SQL on the copy — is served from the copy alone, as before. The block says
so for that server once a connection has found it out, and the metric has no
series for it.

#### Finding where the copy answers differently: `sql-compare`
Before turning routing on for a workload, play that workload through both
sides and read the differences:

```
bintrail-console sql-compare \
  --source-dsn 'app:pw@tcp(db.internal:3306)/shop' \
  --copy-dsn   '<server id or name>:<token>@tcp(127.0.0.1:3308)/shop' \
  --sample 200            # or --statements queries.sql (';'-terminated)
```

`--copy-dsn` is this port with routing **off**, so every statement runs on
the copy; `--sample N` takes the N most recent distinct `SELECT`s the source
ran (`performance_schema.events_statements_history_long`; the error names
the consumer to enable when it is off). The command is read-only: anything
that is not a plain read is skipped before it runs (writes, a `WITH` that
writes, `SELECT ... INTO`, locking reads, `GET_LOCK`/`SLEEP`/`BENCHMARK`,
two statements in one line); a stored function with side effects cannot be
screened by its text, so do not point `--sample` at a workload that calls
one. The run refuses to start when the copy port has read routing on (it
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
- With read routing on (`--route-max-copy-age`), a statement that is not a time-travel shape is prepared on MySQL itself and executed there with MySQL binding the arguments, as if the client were connected to it: the port never writes a forwarded statement out as text, so a write stores exactly the value the client bound. The prepare answer then carries MySQL's own parameter and column definitions, errors are MySQL's, and a forwarded result is streamed row by row in MySQL's own binary encoding. Each execution goes through the routing rules above with its own arguments (the plan is read with them bound); only an execution the rules send to the copy is written out, for the copy alone, and falls back to MySQL if the copy refuses it. An execution stays on MySQL when the copy would read it differently: the database changed since the statement was prepared, or a string argument spells a non-integer number (`'12.7'`, compared as a number on MySQL). A statement that reaches the plan step holds a second statement on the source, the `EXPLAIN` of itself, which counts toward the source's `max_prepared_stmt_count`. A statement is tied to the connection it was prepared on: once the port's connection to the source is lost, its executions fail with 2006 like everything else.
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

Full-table `_snapshot` with **no** `LIMIT`, run over a live shim (`bintrail shim`, or `bintrail-console watch --flashback-listen`), **streams** its resultset row-by-row over the wire and is **not** row-capped: the baseline flows through the merge cursor one row at a time, so peak memory is proportional to the rows *changed* since the baseline, not the table size — a multi-million-row table dumps without `ER_TOO_BIG_SELECT`. If the merge fails mid-stream, the connection receives an error packet in place of the next row (no clean end-of-result), so a failed dump is never mistaken for a complete one. The 100,000-row cap still applies to the binlog-only full-table `_flashback` path (whose fetch is buffered) and to any `LIMIT`ed query; exceeding it surfaces as `ER_TOO_BIG_SELECT` (code 1104) with a hint to add a `LIMIT`, narrow the AS OF range, or add a PK filter.

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
- **1235 `ER_NOT_SUPPORTED_YET`** — a non-virtual-schema query reached a shim that has no copy to run it on: the standalone `bintrail shim` (typically a direct connection to `:3308` bypassing ProxySQL; hostgroup routing is misconfigured), or the embedded port for a server whose copy is only on S3 or whose archive access is off (the message says which).
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
