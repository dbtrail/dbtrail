# Restore drills — `bintrail drill`

`verify` proves your index and baselines are consistent **without** doing a
restore. `drill` goes one level up: it performs an *actual* restore into a
scratch MySQL (or, for a PostgreSQL source, a scratch PostgreSQL — see [PostgreSQL sources](#postgresql-sources)) and checks it — a fire drill. Run it monthly and you always
know two things a backup tool must be able to answer: *does my restore
actually work*, and *how long does it take* (a measured RTO, not a guessed
one).

## What one run does

1. **Reconstruct** the selected tables at `--at` (default: now) into a
   mydumper-format dump — baseline snapshot + indexed binlog deltas, the
   same engine as `reconstruct --output-format mydumper`.
2. **Load** the dump into the scratch server given by `--target-dsn`. The
   load is refused if the target already has ANY table in the drilled
   schemas — that makes pointing it at a real server hard by default.
3. **Check**: each loaded table's `COUNT(*)` must equal the exact number of
   rows the dump writer emitted. Any load error or count mismatch fails the
   table.
4. **Report** per-table pass/fail with reconstruct and load timings, and
   exit non-zero on any failure. `--format json` for cron/automation.

The intermediate dump lives in a temp directory: removed on success, **kept
on failure** so you can inspect exactly what didn't load. `--output DIR`
pins it and always keeps it.

A table with **no usable baseline** (drill would fall back to binlog-only
reconstruction, i.e. an empty starting table) always **fails** with an
explicit reason — a rehearsal that never touched a baseline must not read
PASS.

## What drill proves — and what it doesn't

Drill proves the **restore pipeline**: the dump loads, it contains exactly
what the dump writer emitted, and you know the duration. Value-level
fidelity of reconstructed content against
the source is [`verify`](verify.md)'s job — it compares
normalized content digests and explains mismatches row by row. The two are
complementary: `verify` says *the data is right*, `drill` says *the restore
works and takes N seconds*.

## The scratch server

`drill` never launches or supervises a MySQL itself — it only loads into a
DSN you provide. Any throwaway instance works. The repo ships an opt-in
compose profile with the pinned MySQL 8.4 and a disposable volume:

```bash
docker compose --profile drill up -d drill-mysql

bintrail drill \
  --index-dsn  "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
  --baseline-dir /var/lib/bintrail/baselines \
  --tables shop.orders,shop.users \
  --target-dsn "root:${DRILL_MYSQL_PASSWORD:-drill}@tcp(127.0.0.1:13307)/"

# Wipe ONLY the scratch afterwards. NEVER `docker compose down -v` here —
# that removes every volume in the compose file, INCLUDING the index
# datadir (your system of record).
docker compose --profile drill rm -sf drill-mysql
docker volume rm "$(basename "$PWD")_drill-scratch-data"
```

## PostgreSQL sources
For tables captured from PostgreSQL, run the drill with `bintrail-pg` and
point `--target-dsn` at a scratch **PostgreSQL**, written as a URL
(`postgres://...`). The target picks the dump: each table is reconstructed
as the SQL file `reconstruct --output-format psql` writes, and loaded the
way `psql -f` would run it, inside the file's one transaction. A load that
fails anywhere leaves no table behind. The checks are the same: the target
must hold nothing in the drilled schemas (no table, sequence or view), and
each table's `count(*)` must equal the rows the file holds.

```sh
docker compose --profile drill up -d drill-postgres

bintrail-pg drill \
  --index-dsn  "root:pw@tcp(127.0.0.1:3306)/bintrail_index" \
  --baseline-dir /var/lib/bintrail/baselines \
  --tables public.orders \
  --target-dsn "postgres://postgres:${DRILL_POSTGRES_PASSWORD:-drill}@127.0.0.1:15433/postgres?sslmode=disable"

docker compose --profile drill rm -sf drill-postgres
docker volume rm "$(basename "$PWD")_drill-postgres-data"
```

The SQL file makes what the snapshot recorded: columns, types, NOT NULL,
defaults, identity and sequences, generated columns and the primary key.
Other indexes, constraints and grants are not rebuilt (see
[PostgreSQL: Full-table snapshots](postgres.md#full-table-snapshots)).
`bintrail` (the MySQL binary) refuses a `postgres://` target and names
`bintrail-pg`; a PostgreSQL-captured table into a MySQL target is refused
with the same advice.

## The monthly runbook

```bash
# cron: first Sunday, 04:00 — non-zero exit means the drill FAILED
bintrail drill --format json \
  --index-dsn "$INDEX_DSN" --baseline-dir /var/lib/bintrail/baselines \
  --tables shop.orders,shop.users,shop.payments \
  --target-dsn "root:drill@tcp(scratch:3306)/" \
  || notify-oncall "restore drill failed"
```

The JSON carries `rows_written`/`rows_loaded`, `reconstruct_seconds` and
`load_seconds` per table — chart the durations over time and you have an
RTO trend, not a hope.

## Flags

| Flag | Meaning |
|---|---|
| `--index-dsn` | The bintrail index (required). |
| `--target-dsn` | The scratch MySQL to load into (required; refused if the target already holds any table in the drilled schemas). |
| `--tables` | Comma-separated `schema.table` list (required). |
| `--at` | Point in time to restore to (default now; accepts the same forms as `reconstruct --at`). |
| `--baseline-dir` / `--baseline-s3` | Where baselines live (one required — a full-table restore starts from a baseline). |
| `--output` | Keep the dump here (default: temp dir, removed on success, kept on failure). |
| `--format` | `text` (default) or `json`. |
| `--ultrafast`, `--duckdb-threads`, `--duckdb-memory-limit` | The shared DuckDB tuning budget (see [query-and-recovery.md](query-and-recovery.md)). |
