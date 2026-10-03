# How long a snapshot blocks writes

A snapshot (a `bintrail dump`, or a snapshot taken from the console) runs mydumper, and every
point-consistent lock mode takes a lock so that all of mydumper's threads start reading at the same
instant. This page has the measurements of what that lock costs the application writing to the
source, per lock mode and per server.

## What the numbers say

**The lock itself is short.** From the moment it is granted until mydumper releases it, it was held
for 13 to 23 ms on a local MySQL, about 40 ms on MariaDB and 50 to 70 ms on Amazon RDS. It is a few
round trips between mydumper and the server, so it follows the network, not the size of the data or
the write load.

**What can be long is the wait before the lock is granted**, and what stops during that wait depends
on the mode and on the server:

| Running on the source when the snapshot starts | `ftwrl` on MySQL 8.0 / 8.4 | `ftwrl` on MariaDB 10.11 | `lock-all` (MySQL, RDS, MariaDB) |
|---|---|---|---|
| A long query reading a dumped table | The lock waits until the query ends. **Writes to the table that query reads stop for the whole wait** (12.8 s in the test); writes to other tables continue. | No wait. | No wait. |
| An open transaction that wrote to a dumped table and has not committed | No wait. | No wait. | The lock waits until the transaction commits (12.7 to 12.9 s in the test). On MySQL and RDS, **writes to some of the other dumped tables stop for the whole wait**: the ones the lock already took. The transaction's own table keeps accepting writes. On MariaDB no writes stopped. |
| An online `ALTER TABLE` on a dumped table | The dump does not start until the `ALTER` finishes (29 to 57 s in the test), but writes continue: the wait is on `LOCK INSTANCE FOR BACKUP`, which blocks schema changes, not writes. | **Every write on the server stops** while the lock waits for the `ALTER`, tables outside the snapshot included. After 60 s mydumper cancels its own lock request and the snapshot fails. Seen in 3 of 3 runs: 61 s with no committed write anywhere on the server. | The lock waits 0.05 to 2.4 s. |

So:

- Take snapshots when no long queries, long transactions or `ALTER TABLE` are running.
- On MariaDB with `ftwrl`, never let a snapshot start during an `ALTER TABLE`.
- `safe-no-lock` aborted in 5 of 5 runs on every server under a steady write load (300 transactions
  per second), within the first second of the dump. It never wrote a snapshot that was not
  point-consistent, and it never finished one either.

The 60 s in the MariaDB case is mydumper's `--long-query-guard` default, which DBTrail does not
change: at that point mydumper sends `KILL QUERY` to its own `FLUSH TABLES WITH READ LOCK` and stops
with `Couldn't acquire global lock, snapshots will not be consistent - ERROR 1317: Query execution
was interrupted`.

## How it was measured

- **mydumper** v1.0.3-1 (the build DBTrail ships), started exactly as DBTrail starts it:
  `--threads 4 --compress-protocol --complete-insert --sync-thread-lock-mode <MODE> --trx-tables
  --database sbtest`.
- **Data**: the dumped schema held four sysbench tables of 500,000 rows, one 4,000,000-row table
  (`big`, the target of the `ALTER`) and a small `probe` table.
- **Write load**: sysbench `oltp_write_only`, 8 threads, fixed at 300 transactions per second.
  "Quiet" means no sysbench: only the probes below, a few hundred single-row writes per second.
- **Probes**, each a loop of single-row autocommit writes that records how long every write took:
  two into the dumped table the blocker uses (`sbtest1`, or `big` for the `ALTER`), two into another
  dumped table (`probe`), and one into a table outside the dumped schema. The outside probe is the
  control: a pause that shows up there too comes from the machine being busy, not from the lock.
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
every probe, the control included, paused for 0.2 to 4 s at some point during every dump, in every
mode: that is the dump competing for the same CPU and disk, which the control probe shows. Compare
each row's three pause columns with each other, not with another server's.

## Results

"Longest write pause" is the longest stretch with no committed write on that probe while the dump
ran. "Seconds at 0 tx/s" counts the seconds in which sysbench committed nothing.

#### MySQL 8.4

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms | Seconds at 0 tx/s |
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
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | 2 (2) / 9 (9) | 0 (0) |

#### MySQL 8.0

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms | Seconds at 0 tx/s |
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
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | 2 (2) / 6 (7) | 0 (0) |

The MySQL 8.0 runs overlapped the laptop's disk filling up, so their pauses are larger across the
board, the control included; the lock timings are unaffected.

#### Amazon RDS for MySQL 8.4 (`lock-all` only: `ftwrl` cannot run on RDS)

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms | Seconds at 0 tx/s |
|---|---|---|---|---|---|---|---|
| lock-all | quiet | 3/3 | 15 (27) | 50 (54) | 146 (240) / 231 (240) / 156 (240) | 15 (16) / 99 (101) | – |
| lock-all | write load | 3/3 | 95 (158) | 68 (119) | 139 (181) / 177 (236) / 75 (139) | 13 (16) / 37 (48) | 0 (0) |
| lock-all | + long SELECT | 3/3 | 70 (100) | 57 (83) | 74 (98) / 111 (151) / 63 (101) | 15 (16) / 45 (46) | 0 (0) |
| lock-all | + open write trx | 3/3 | 12.8 s (12.9 s) | 52 (60) | 118 (121) / 12.9 s (13.0 s) / 93 (106) | 15 (16) / 48 (53) | 0 (0) |
| lock-all | + online ALTER | 3/3 | 46 (113) | 62 (63) | 153 (218) / 164 (166) / 166 (229) | 24 (25) / 125 (129) | 0 (0) |
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | 7 (8) / 15 (17) | 0 (0) |

#### MariaDB 10.11

| Mode | Running when the dump starts | Dumps OK | Lock wait, ms | Lock held, ms | Longest write pause, ms: table the blocker uses / other dumped table / outside the dump | Write latency p50 / p99, ms | Seconds at 0 tx/s |
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
| safe-no-lock | write load | 0/5, aborted 5/5 | – | – | – | 2 (2) / 9 (9) | 0 (0) |

## What was not measured

- An `ALTER TABLE` that runs longer than 60 s on MySQL with `ftwrl`: the longest wait on the backup
  lock was 57 s, so whether mydumper's long-query guard also cancels that wait is not known.
- An `ALTER TABLE` that **starts** while the dump is running, rather than before it.
- Sources other than the four above (Percona Server, Aurora, MariaDB other than 10.11), and
  mydumper builds other than v1.0.3-1.
- Which of the other dumped tables `lock-all` takes before it waits. In these runs the tables that
  stopped were the ones whose names sort before the blocked table, which matches the server taking
  table locks in name order; that order is a server detail and was not verified beyond these runs.
