# When DBTrail reads your whole database

A **full read** makes a snapshot the expensive way: mydumper reads every row of every table on your MySQL, and in the default lock mode it takes a lock first. On a large database it runs for hours. An **update** makes the next snapshot from the previous one plus the changes DBTrail already recorded, and does not read your database at all.

DBTrail takes one full read to start, and updates after that. This page lists every case in which a scheduled run reads the whole database again, what on your side causes each, and what you can do about it.

## The short answer

- **One full read**, when a server's first snapshot is taken.
- **Then an update every 5 minutes**, when the default schedule starts (see [the schedule a new server starts with](#the-schedule-a-new-server-starts-with)), or at the interval you pick on the Snapshots page.
- **No full read on a timetable**, unless you set one: the **Read the whole database again every** row of the schedule form is empty by default.
- **A full read by itself** in the cases listed [below](#every-case). The Snapshots page says, for every run, which kind it was and why, and what the next one will be.

## What a snapshot is, and where it is kept

A snapshot is the state of every table at one moment, one Parquet file per table, in a folder named after that moment. It is a complete copy to read, but it does not cost a complete copy on disk:

- A table with no changes since the previous snapshot is the same file, shared between the two folders as a hard link.
- A table with changes keeps its previous file and gets a small pair of files beside it holding what changed. The table file is rewritten only once a day, or when the pairs outgrow it.

**Every server keeps its snapshots in a local folder.** An S3 destination is a second place, beside the local folder, never instead of it. A server with no local folder gets no snapshot, and the Snapshots page says why.

**Sending a snapshot to S3 does not remove the local one.** How many stay on the local disk is decided by two different settings, and which one applies depends on whether the server has an S3 destination ([#2658](https://github.com/dbtrail/dbtrail/issues/2658) proposes a single one):

| The server | What removes local snapshots | Out of the box |
|---|---|---|
| has no S3 destination | **Keep the newest** (`local_keep_newest`), a count, per server | 3 for a server added from the web interface |
| has an S3 destination | `--baseline-retain`, an age, for the whole daemon, and only once the snapshot is confirmed in S3 | off: nothing is removed |

**How often a snapshot goes to S3.** On a server with an S3 destination, the schedule form has **Send to S3 every**: each update, 1 hour, 6 hours or 24 hours. It starts at "each update", so a 5-minute schedule uploads every 5 minutes until you change that row. Pick a longer interval: the updates in between stay on the local disk, each one still continues from the newest local snapshot, and the bucket gets one snapshot per interval. See [Refreshing on a schedule](dump-and-baseline.md#refreshing-on-a-schedule).

DBTrail never removes a snapshot from the bucket. See [Pruning old local snapshots](dump-and-baseline.md#pruning-old-local-snapshots---baseline-retain).

## The schedule a new server starts with

A server added from the web interface starts updating every 5 minutes by itself as soon as its first snapshot exists, when both of these are true:

- it keeps only its newest snapshots (**Keep the newest** has a number, 3 by default), and
- it has no S3 destination.

That is how DBTrail knows the snapshots cannot pile up: with a count, the local folder is bounded. With an S3 destination nothing bounds the local folder (see the table above) and nothing ever bounds the bucket, so the schedule is yours to turn on, on the Snapshots page. A server whose folder already held snapshots when you added it gets no count, so those are not removed, and no schedule by itself either.

## Every case

| A run reads the whole database when | What causes it | What to do |
|---|---|---|
| there is no snapshot to update | the first run of a server | nothing; it happens once |
| you asked for one | the **Read database now** button, or **Read the whole database again every** on the schedule | leave the timetable empty unless you need it (see [foreign keys](#what-does-not-cause-a-full-read)) |
| the previous snapshot could not be listed in S3 | the bucket answered with an error: permissions changed, credentials expired, the network. Only for a server whose snapshots go to S3; an unreadable local folder skips the run and says so | fix the access; the run's reason carries the bucket's error |
| a table was recreated, renamed, truncated or changed shape | `ALTER TABLE` that adds, drops or retypes a column, `TRUNCATE`, `DROP`, `RENAME`, a partition dropped, truncated or exchanged. This is what an online schema change tool and a restore do: see [the next section](#operations-that-end-in-a-full-read) | plan for one full read after it |
| some changes were never recorded (a capture gap) | DBTrail was stopped longer than MySQL keeps its binary logs, so MySQL removed changes before they were read | keep binary logs longer than DBTrail could be down (`binlog_expire_logs_seconds`, or `binlog retention hours` on RDS). Stopping and starting the server in the web interface acknowledges the loss: that silences the alarm and does not make the update skip the gap. One full read after the gap does, and the updates after it are not refused for it |
| the binary log started again from another numbering | `RESET MASTER` (`RESET BINARY LOGS AND GTIDS`), a failover to another server, a new `log_bin` name | nothing; one full read after it |
| tables were created after the previous snapshot | a `CREATE TABLE`. An update leaves a new table out, since there is no earlier copy of it to update | nothing; one full read brings them in |
| an update would take clearly longer than a full read | measured: the changes waiting, at the rate of recent updates, against how long the last full read took. A full read is chosen only past twice that and 30 s more. The last full read of a large database took hours, so this takes a very large backlog. On a server with no per-event rate, the same margin applies to the cheapest recent update alone: when that one took that much longer than the last full read, a full read is chosen, and no count of the changes is needed. When one of the numbers is missing, for example on a server that has not timed an update yet, no full read is chosen on the estimate and the update runs, however old the previous snapshot is: its age alone never chooses a full read | nothing; it is the cheaper of the two |
| nothing was recorded since an old snapshot, there is evidence that capture is missing changes, and no estimate says an update is the cheaper one | the previous snapshot is over 2 hours old (or 6 schedule intervals, when that is longer), the index holds no change since it, and one of these: the source reports transactions capture has not recorded; capture's own record says changes were lost (a capture gap since the snapshot, rows it dropped that no full read has read since); capture holds transactions the source does not have; or a loss cannot be ruled out (the record of dropped rows cannot be read, the index is older than the record of capture gaps, a GTID set does not parse). The index is missing changes, or may be, and an update would publish the old rows under a new date. This applies to a server with no per-event update rate or no full read on record, and to one whose cheapest update takes a little longer than its last full read. On a server with a rate whose cheapest update is shorter than its last full read, an update with nothing to apply is estimated cheaper than a full read and runs, whatever the source says. A source that cannot be asked, for one of the reasons listed [below](#a-quiet-server-whose-source-cannot-be-asked), is not evidence, and is not read in full | check that capture is running; the run's reason says what was found |
| a table changed more rows than one update can hold | up to 2 million changed rows per run are held in memory; past that they go to temporary disk in 64 groups, and the update is refused only when one group alone passes the limit, at about 64 million changed rows of one table in one update | nothing; rare |
| the update hit an internal error | a defect in DBTrail. The run is marked failed and capture keeps running | report it, with the reason the Snapshots page shows |

Three rules sit on top of the table:

- **One refused table stops the update of every table.** An update publishes all tables at one moment or nothing, so a single `RENAME` on one table ends in a full read of the whole server, not of that table ([#2640](https://github.com/dbtrail/dbtrail/issues/2640)).
- **At most one full read a day in place of a failed update.** A cause that a full read does not cure would otherwise read production at every slot. After one such read, the next failed update inside 24 hours is recorded as skipped, with the time the next read is allowed, and the copy stays at its last snapshot until then. Full reads you asked for, and the ones that bring in new tables, neither count nor wait.
- **A daemon that may not take full reads takes none.** Full reads need the setting that shows the **Read database now** button (`BASELINE_TRIGGER`, on by default in the compose stack). Without it the slot is skipped, the Snapshots page says why, and the copy waits.

## A quiet server whose source cannot be asked

No snapshot is made at a scheduled time when all of these are true ([#2659](https://github.com/dbtrail/dbtrail/issues/2659)):

- the previous snapshot is over 2 hours old (or 6 schedule intervals, when that is longer);
- the index holds no change since it;
- the source cannot be asked whether capture recorded everything it wrote, for one of these reasons and no other: a PostgreSQL source; a source that reports no GTID set (MariaDB, MySQL without GTIDs); capture in binlog-position mode, or with no GTID position saved; an index on the same server as the source, or one that cannot be told apart from it; a server with no source connection; a source that does not answer in time; an index with no capture on record at all;
- no estimate says an update is the cheaper one (the same servers as the row above: no per-event update rate or no full read on record, or a cheapest update a little longer than the last full read).

DBTrail reads nothing and updates nothing at that slot. A full read would read every row with nothing saying a change was missed, at every slot of a quiet server. An update would publish the old rows under a new date, with no way to confirm that nothing changed. The copy keeps the data it has.

- **The Snapshots page** shows the slot with no snapshot made, with the reason, and says that the next run will make none either.
- **The daemon log** says it once when it starts, once a day while it lasts, and once when it ends, not at every slot.
- **It ends by itself** when a change is recorded (the next slot updates) or when the source can be asked (the next slot follows what it answers).
- **The update still runs when the snapshot cannot wait**: it is close to the oldest change the index keeps (48 hours by default, `--rotate-retain`), a table was created since it, or a capture loss is on record since it. Left alone past that point, the snapshot could not be updated when a change did arrive. So on a server that stays quiet the snapshot is replaced by one with the same data about once every 0.8 of what the index keeps, and the skipping resumes two hours later. Which of the three it was is said before the run, on the Snapshots page as the next run's reason, and once in the daemon log. The run itself is recorded as an ordinary update, with no reason.
- **A snapshot waiting for its S3 slot is still sent.** With **Send to S3 every** set, a skipped slot sends the newest snapshot kept on the local disk once its upload time comes, as a quiet update does.

What to do depends on the reason the page shows:

- a PostgreSQL source, a source with no GTIDs, capture in binlog-position mode, an index on the source server: nothing. These sources can never be asked;
- a source that does not answer, or a server with no source connection: fix the connection. The next slot asks again;
- an index with no capture on record: capture has never recorded anything for this server. Check that capture is running. The slots stay skipped until it records a change.

To keep a dated snapshot on such a server whatever the source says, set **Read the whole database again every** on the schedule.

## Operations that end in a full read

Measured on MySQL 8.0.46, a 50,000-row table, with `bintrail baseline refresh` (the update a schedule runs) after each operation:

| What was done on the source | What the update did |
|---|---|
| 100 rows changed | updated |
| `pt-online-schema-change` adding a column | refused: the table changed shape |
| `pt-online-schema-change` that only rebuilds the table (`ENGINE=InnoDB`) | refused: `RENAME TABLE` |
| a restore that drops and recreates the table (what `mysqldump` writes by default) | refused: `DROP TABLE` |
| a restore that deletes every row and inserts them again, with no DDL (`mysqldump --no-create-info`) | updated, all 50,000 rows |

So it is not the number of rows that forces a full read. It is that the table was recreated or renamed, which MySQL records as one statement with no row changes to apply. An online schema change always ends with a rename, whatever it altered. `gh-ost` works the same way (it copies into a new table and renames it) and was not measured.

**Before a schema change with one of these tools, or a restore:**

- Expect one full read of the whole server at the next scheduled run after it. With the default lock mode it first waits for the source to have no long-running statement ([`ftwrl` waits for a quiet source first](snapshot-lock-measurements.md#ftwrl-waits-for-a-quiet-source-first)).
- Group them. Two schema changes hours apart on the same day are one full read and then a copy that stays at that snapshot until the next day, because of the once-a-day rule; done together they are one full read and an up-to-date copy.
- A plain `ALTER TABLE` that adds, drops or retypes a column has the same result as the tool: the update is refused and a full read follows.

## What does not cause a full read

- **Many changed rows, with no DDL.** A mass `UPDATE`, a `DELETE` of half a table, a reload that keeps the table: all applied by the update, up to the limit in the table above.
- **A stop shorter than MySQL's binary log retention**, as long as the backlog fits the update: capture catches up and the update applies it. Two rows of the table above still apply to a long stop, a backlog measured to take clearly longer than a full read and a table with more changed rows than one update can hold, and so does one refusal: a stop longer than the index keeps its changes (48 hours by default, `--rotate-retain`) with no archive of the dropped hours, where the update is refused for the hours the index no longer holds.
- **Rows a foreign key cascades to.** On MariaDB, and on MySQL before 9.6, the server deletes or updates the child rows of an `ON DELETE CASCADE` or `ON UPDATE CASCADE` itself and writes nothing for them to the binary log. That is not a capture gap, DBTrail does not see it, and no full read follows. The cost is on the other side: the copy keeps those child rows until its next full read. On such a source that is the one reason to set **Read the whole database again every**. Capture warns at startup when the source has such keys.

## Where each of these shows

- **The Snapshots page**: every run with its kind (update or full read) and its reason, the next run and why, and a skipped slot with what skipped it.
- **The daemon log**: `snapshot schedule:` lines say when a full read is taken in place of an update, and with which numbers.
- **`bintrail baseline refresh`** from a shell prints the same verdict per table (`refreshed`, `refused-ddl`, `refused-gap`, `refused-renumbered`): see [a newer baseline without a new dump](dump-and-baseline.md#step-3-optional-a-newer-baseline-without-a-new-dump).
