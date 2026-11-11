# How long a snapshot blocks writes

A snapshot (a `bintrail dump`, or a snapshot taken from the console) runs mydumper, and every
point-consistent lock mode takes a lock so that all of mydumper's threads start reading at the same
instant. This page has the measurements of what that lock costs the application writing to the
source, per lock mode and per server.

## What the numbers say

**The lock itself is short.** From the moment `FLUSH TABLES WITH READ LOCK` (`ftwrl`) or
`LOCK TABLE` (`lock-all`) is granted until mydumper releases it, the median run held it for 13 to
73 ms, and no run held it longer than 141 ms. That window is mydumper's worker threads opening their
snapshots one after another. It did not grow with the write load; the size of the data was not
varied.

One lock is not short: on MySQL 8.0 and 8.4, `ftwrl` first takes `LOCK INSTANCE FOR BACKUP` and holds
it for the **whole dump**. It does not stop writes, but no `ALTER TABLE` or other schema change can
run until the dump ends.

**What can be long is the wait before the lock is granted** (measured before #2530: the `ftwrl` column is what a snapshot that asks for the lock anyway costs; see [`ftwrl` waits for a quiet source first](#ftwrl-waits-for-a-quiet-source-first) for what it does now), and what stops during that wait depends
on the mode and on the server:

| Running on the source when the snapshot starts | `ftwrl` on MySQL 8.0 / 8.4 | `ftwrl` on MariaDB 10.11 | `lock-all` (MySQL, RDS, MariaDB) |
|---|---|---|---|
| A long query reading a dumped table | The lock waits until the query ends. **Writes to the table that query reads stop for the whole wait** (12.8 s in the test). Writes that touch only other tables continue, but an application whose transactions also touch that table stops entirely: the sysbench load committed nothing for 11 s. | No wait. | No wait. |
| An open transaction that wrote to a dumped table and has not committed | No wait. | No wait. | The lock waits until the transaction commits (12.7 to 13.1 s in the test). On MySQL and RDS, **writes to some of the other dumped tables stop for the whole wait**: the ones the lock already took. The transaction's own table keeps accepting writes. On MariaDB no writes stopped. |
| An online `ALTER TABLE` on a dumped table | The dump does not start until the `ALTER` finishes (26 to 57 s in the test), but writes continue: the wait is on `LOCK INSTANCE FOR BACKUP`, which blocks schema changes, not writes. | **Every write on the server stops** while the lock waits for the `ALTER`, tables outside the snapshot included. If the `ALTER` runs past 60 s, mydumper cancels its own lock request and the snapshot fails. Seen in 3 of 3 runs, with an `ALTER` that ran 79 to 84 s: 61 s with no committed write anywhere on the server. | The lock waits 0.03 to 2.4 s. On MariaDB it already waited 0.4 to 0.7 s with no `ALTER` at all. |

So:

- Take snapshots when no long queries, long transactions or `ALTER TABLE` are running.
- On MariaDB with `ftwrl`, never let a snapshot start during an `ALTER TABLE`.
- `safe-no-lock` aborted in 5 of 5 runs on every server with a steady write load of 300 transactions
  per second, within the first second of the dump. It never wrote a snapshot that was not
  point-consistent, and it never finished one either.

The 60 s in the MariaDB case is mydumper's `--ftwrl-max-wait-time` default, which DBTrail does not
change: at that point mydumper sends `KILL QUERY` to its own `FLUSH TABLES WITH READ LOCK` and stops
with `Couldn't acquire global lock, snapshots will not be consistent - ERROR 1317: Query execution
was interrupted`.

## `ftwrl` waits for a quiet source first

Since #2530, a `ftwrl` snapshot taken by `bintrail dump`, the console or the compose `baseline`
profile does not ask for the lock
while a statement has been running on the source for 1 s or more. Before it starts mydumper, DBTrail
reads the source's process list, and while such a statement runs it waits and looks again every
10 s, for up to 10 minutes. If the source was never quiet in that time, the snapshot fails without
having locked or read anything, and says why (which connection, which account, for how long; never
the statement's text). In the console's automatic lock mode, that refusal is retried once with
`lock-all`, as a refused `ftwrl` is; a lock mode you chose is not. The snapshot's instant is taken
after the wait, so a snapshot that waited is not labelled with a time before its data. `lock-all`
does not wait: a running query does not hold it up.

The compose `baseline` profile runs mydumper from mydumper's own image, so its wait is a step of its
own, `baseline-quiet`, that runs before the dump. A refusal there stops the run before mydumper
starts. What the step says, the refusal's reason and the cases where it could not see the source and
let the dump go ahead (no `PROCESS`, no connection, an unreadable process list), is in
`docker compose --profile baseline logs baseline-quiet`, which `docker compose run` does not show.
Between that step ending and mydumper asking for the lock a container has to start, a second or two
in which a statement that begins and then runs long is not seen. The step needs a `bintrail` image
newer than 0.128.0 ([docker.md](docker.md) has the symptom of an older one).

What counts: statements whose command is `Query`, `Execute` (a prepared statement, which is how the
Go driver, PHP's mysqli and Connector/J with server-side prepare send a statement with parameters;
mydumper's own check, `--long-query-guard`, counts only `Query`) or `Killed` (a statement killed but
still rolling back), on the whole server, since `FLUSH TABLES WITH READ LOCK` flushes every table.
What never counts: idle connections, a binlog dump (DBTrail's own capture among them), a replica's
own threads and the check's own connection. On MariaDB, whose `FLUSH TABLES WITH READ LOCK` did not
wait for a running `SELECT` in the measurements above, a statement that only reads (`SELECT`,
`SHOW`, `EXPLAIN`, `DESCRIBE`, `DO`) does not count; writes and DDL do, and so does a statement
whose text the account cannot see. Only the `ALTER` case was measured on MariaDB.

Why waiting is the only protection, measured on MySQL 8.0.46 with the mydumper DBTrail ships, a
blocker `SELECT SLEEP(n) FROM t1 LIMIT 1` started 2 s before the snapshot, and single-row writes into
`t1` and into a second dumped table `t2`:

| What the snapshot did | Longest write pause on `t1` | On `t2` |
|---|---|---|
| Asked for the lock (before #2530), blocker running 100 s | **97.9 s**: the whole blocker. At 60 s mydumper cancelled its first `FLUSH TABLES` and went on to `FLUSH TABLES WITH READ LOCK`, which waited for the rest | 0.5 s |
| Asked with `lock_wait_timeout = 5` on its sessions, blocker 40 s | **38.0 s**. Each attempt timed out after 5 s and mydumper retried, "continuing anyways" | 0.0 s |
| Asked, and its lock request was killed after 5 s, mydumper with it, blocker 40 s | **37.9 s**. Once a flush has started, new statements on a table in use wait for the statement using it to end, whether or not the flush is still waiting | 0.0 s |
| Waited for a quiet source, blocker 30 s | **0.0 s**. The snapshot started when the blocker ended, 30 s later, and finished. Measured with mydumper's own check set to wait the same way; DBTrail now does the check itself, for the reasons above | 0.0 s |

What it does not cover: a statement that started less than 1 s before the lock is asked for and then
runs long. And the process list shows other accounts' statements only to an account with the
`PROCESS` privilege: without it, only the snapshot account's own statements are seen, so the
recommended grants include `PROCESS`, and a snapshot whose account lacks it logs a warning that says
so. A second full read of the same source by another tool is a long-running statement like any
other, and is waited for.

## How it was measured

- **mydumper** v1.0.3-1 (the build DBTrail ships), in its Docker image, with the arguments DBTrail
  passes: `--threads 4 --compress-protocol --complete-insert --sync-thread-lock-mode <MODE>
  --trx-tables --database sbtest`. On MySQL and RDS it ran as the container's first process, and
  after a `safe-no-lock` abort it never exited (the measurement killed it at 180 s); on MariaDB it
  ran behind `docker run --init` and exited within a second. The console is not affected, because it
  runs mydumper as a child process; `bintrail dump` through Docker is, see #2039.
- **Data**: the dumped schema held four sysbench tables of 500,000 rows, one 4,000,000-row table
  (`big`, the target of the `ALTER`) and a small `probe` table.
- **Write load**: sysbench `oltp_write_only`, 8 threads, fixed at 300 transactions per second.
  "Quiet" means no sysbench: only the probes below, about 1,300 to 1,600 single-row writes per
  second (about 300 on RDS, where every write crossed the network).
- **Probes**, each a loop of single-row autocommit writes that records how long every write took:
  two into the dumped table the blocker uses (`sbtest1`, or `big` for the `ALTER`), two into another
  dumped table (`probe`), and one into a table outside the dumped schema. The outside probe is the
  control: a pause that shows up there too comes from the machine being busy, not from the lock,
  except when the lock is server-wide (MariaDB `ftwrl` behind an `ALTER`).
- **Blockers**, started 2 s before the dump: `SELECT SLEEP(15) FROM sbtest1 LIMIT 1`; a transaction
  that ran `UPDATE sbtest1 ... WHERE id=1` and committed 15 s later; and
  `ALTER TABLE big ADD INDEX (c, pad), ALGORITHM=INPLACE, LOCK=NONE`.
- **Lock timings** come from `performance_schema.events_statements_history_long`, recording only
  the dump's own user so the load cannot push its statements out. "Lock wait" is from the first
  `FLUSH`/`LOCK TABLE` statement until the lock is granted; "lock held" is from then until
  `UNLOCK TABLES`.
- **Servers**: MySQL 8.0.46 and 8.4.9 in Docker on a laptop; MariaDB 10.11 in Docker on an
  `m7g.large` EC2 instance with the load on the same machine; RDS for MySQL 8.4 on `db.t4g.medium`
  with binary logging on, loaded from an EC2 instance in the same region. Each cell ran 3 times
  (`safe-no-lock` 5 times); tables show the median and, in parentheses, the worst run.

Absolute latencies belong to these machines and are not comparable across servers. On the laptop,
every probe, the control included, paused for 0.2 to 4 s at some point during every `ftwrl` and
`lock-all` dump: that is the dump competing for the same CPU and disk, which the control probe shows. Compare
each row's three pause columns with each other, not with another server's.

## Results

"Longest write pause" is the longest stretch with no committed write on that probe while the dump
ran. "Write latency" is the `probe` table's probe. "Seconds at 0 tx/s" counts the seconds in which
sysbench committed nothing. The `safe-no-lock` rows show only the outcome: on MySQL and RDS the
dump aborted within 75 ms and the rest of the window was a mydumper that had stopped working.

#### MySQL 8.4

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms (`probe`) | Seconds at 0 tx/s |
|---|---|---|---|---|---|---|---|
| ftwrl | quiet | 3/3 | 8 (67) | 21 (27) | 245 (502) / 222 (502) / 249 (489) | 3 (3) / 15 (22) | – |
| ftwrl | write load | 3/3 | 27 (38) | 15 (19) | 255 (353) / 258 (352) / 258 (354) | 3 (3) / 14 (15) | 0 (0) |
| ftwrl | + long SELECT | 3/3 | 12.8 s (12.8 s) | 14 (23) | 12.8 s (12.8 s) / 265 (319) / 263 (319) | 2 (2) / 9 (14) | 11 (11) |
| ftwrl | + open write trx | 3/3 | 4 (7) | 14 (17) | 257 (273) / 263 (271) / 273 (381) | 3 (3) / 14 (14) | 0 (0) |
| ftwrl | + online ALTER | 3/3 | 6 (7), after 29.4 s (30.4 s) on the backup lock | 14 (17) | 2.3 s (2.4 s) / 1.9 s (2.1 s) / 2.3 s (2.4 s) | 3 (3) / 16 (16) | 1 (2) |
| lock-all | quiet | 3/3 | 12 (25) | 13 (15) | 212 (249) / 212 (248) / 212 (249) | 3 (3) / 12 (13) | – |
| lock-all | write load | 3/3 | 51 (89) | 22 (31) | 336 (394) / 317 (396) / 337 (424) | 3 (3) / 14 (16) | 0 (0) |
| lock-all | + long SELECT | 3/3 | 15 (23) | 21 (23) | 593 (1693) / 569 (1691) / 594 (1692) | 3 (3) / 30 (33) | 0 (1) |
| lock-all | + open write trx | 3/3 | 12.8 s (12.8 s) | 13 (17) | 269 (379) / 12.8 s (12.8 s) / 288 (387) | 3 (3) / 13 (14) | 0 (0) |
| lock-all | + online ALTER | 3/3 | 301 (2409) | 73 (141) | 1.6 s (2.9 s) / 1.0 s (2.6 s) / 1.6 s (2.9 s) | 4 (4) / 48 (140) | 1 (2) |
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | – | – |

#### MySQL 8.0

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms (`probe`) | Seconds at 0 tx/s |
|---|---|---|---|---|---|---|---|
| ftwrl | quiet | 3/3 | 7 (15) | 13 (24) | 726 (1453) / 485 (1480) / 630 (1480) | 3 (3) / 11 (20) | – |
| ftwrl | write load | 3/3 | 9 (13) | 16 (17) | 606 (638) / 602 (640) / 610 (640) | 3 (4) / 13 (13) | 0 (0) |
| ftwrl | + long SELECT | 3/3 | 12.8 s (12.8 s) | 13 (16) | 12.8 s (12.8 s) / 664 (986) / 658 (996) | 2 (3) / 9 (20) | 11 (11) |
| ftwrl | + open write trx | 3/3 | 5 (26) | 18 (19) | 756 (1179) / 814 (1227) / 814 (1234) | 3 (4) / 30 (31) | 0 (0) |
| ftwrl | + online ALTER | 3/3 | 11 (11), after 50.3 s (57.0 s) on the backup lock | 14 (14) | 1.8 s (2.1 s) / 1.9 s (3.0 s) / 2.5 s (3.0 s) | 3 (3) / 15 (18) | 1 (1) |
| lock-all | quiet | 3/3 | 18 (34) | 13 (14) | 547 (577) / 576 (692) / 577 (693) | 3 (10) / 9 (46) | – |
| lock-all | write load | 3/3 | 31 (124) | 14 (16) | 572 (1959) / 560 (1998) / 572 (1998) | 4 (4) / 15 (28) | 0 (1) |
| lock-all | + long SELECT | 3/3 | 29 (50) | 15 (16) | 629 (653) / 626 (653) / 629 (651) | 3 (4) / 10 (13) | 0 (0) |
| lock-all | + open write trx | 3/3 | 12.8 s (12.8 s) | 14 (16) | 559 (688) / 12.8 s (12.8 s) / 561 (688) | 3 (3) / 10 (12) | 0 (0) |
| lock-all | + online ALTER | 3/3 | 586 (2397) | 19 (25) | 2.0 s (2.9 s) / 1.7 s (4.0 s) / 2.0 s (3.0 s) | 6 (7) / 85 (187) | 1 (5) |
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | – | – |

The MySQL 8.0 runs overlapped the laptop's disk filling up, so their pauses are larger across the
board, the control included; the lock timings are unaffected.

#### Amazon RDS for MySQL 8.4 (`lock-all` only: `ftwrl` cannot run on RDS)

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms (`probe`) | Seconds at 0 tx/s |
|---|---|---|---|---|---|---|---|
| lock-all | quiet | 3/3 | 15 (27) | 50 (54) | 146 (240) / 231 (240) / 156 (240) | 15 (16) / 99 (101) | – |
| lock-all | write load | 3/3 | 95 (158) | 68 (119) | 139 (181) / 177 (236) / 75 (139) | 13 (16) / 37 (48) | 0 (0) |
| lock-all | + long SELECT | 3/3 | 70 (100) | 57 (83) | 74 (98) / 111 (151) / 63 (101) | 15 (16) / 45 (46) | 0 (0) |
| lock-all | + open write trx | 3/3 | 12.8 s (12.9 s) | 52 (60) | 118 (121) / 12.9 s (13.0 s) / 93 (106) | 15 (16) / 48 (53) | 0 (0) |
| lock-all | + online ALTER | 3/3 | 46 (113) | 62 (63) | 153 (218) / 164 (166) / 166 (229) | 24 (25) / 125 (129) | 0 (0) |
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | – | – |

#### MariaDB 10.11

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms (`probe`) | Seconds at 0 tx/s |
|---|---|---|---|---|---|---|---|
| ftwrl | quiet | 3/3 | 3 (8) | 38 (38) | 162 (184) / 140 (185) / 162 (833) | 3 (3) / 9 (9) | – |
| ftwrl | write load | 3/3 | 3 (3) | 40 (41) | 225 (386) / 181 (195) / 180 (198) | 3 (3) / 9 (12) | 0 (0) |
| ftwrl | + long SELECT | 3/3 | 5 (10) | 38 (43) | 354 (1035) / 200 (1041) / 201 (1035) | 3 (3) / 10 (11) | 0 (0) |
| ftwrl | + open write trx | 3/3 | 3 (5) | 40 (55) | 400 (884) / 154 (885) / 154 (897) | 3 (3) / 11 (12) | 0 (0) |
| ftwrl | + online ALTER | **0/3** | lock never granted, cancelled at 61 s | – | **61.0 s (61.1 s) / 61.1 s (61.1 s) / 61.1 s (61.1 s)** | 11 (38) / 61.1 s (61.1 s) | **60 (61)** |
| lock-all | quiet | 3/3 | 542 (701) | 40 (43) | 621 (655) / 151 (253) / 136 (287) | 2 (3) / 8 (8) | – |
| lock-all | write load | 3/3 | 7 (236) | 38 (45) | 368 (437) / 145 (195) / 196 (199) | 3 (3) / 10 (10) | 0 (1) |
| lock-all | + long SELECT | 3/3 | 117 (845) | 40 (41) | 310 (864) / 150 (227) / 190 (310) | 3 (3) / 11 (12) | 0 (0) |
| lock-all | + open write trx | 3/3 | 12.7 s (13.1 s) | 41 (47) | 487 (1493) / 255 (1545) / 255 (1494) | 2 (2) / 8 (9) | 0 (1) |
| lock-all | + online ALTER | 3/3 | 506 (959) | 39 (45) | 204 (570) / 155 (162) / 191 (205) | 3 (4) / 81 (82) | 0 (0) |
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | – | – |

## What was not measured

- An `ALTER TABLE` that runs longer than 60 s on MySQL with `ftwrl`: the longest wait on the backup
  lock was 57 s, so whether mydumper's long-query guard also cancels that wait is not known.
- An `ALTER TABLE` that **starts** while the dump is running, rather than before it, and on MariaDB
  an `ALTER` shorter than 60 s (the wait should then end when the `ALTER` does).
- A long query on a table **outside** the dump. `FLUSH TABLES` is server-wide, so on MySQL it most
  likely makes `ftwrl` wait the same way.
- Sources other than the four above (Percona Server, Aurora, MariaDB other than 10.11), and
  mydumper builds other than v1.0.3-1.
- Which of the other dumped tables `lock-all` takes before it waits. In these runs the tables that
  stopped were the ones whose names sort before the blocked table, which matches the server taking
  table locks in name order; that order is a server detail and was not verified beyond these runs.
