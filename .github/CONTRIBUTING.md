# Contributing to DBTrail

Thank you for your interest in contributing. This document covers how to get set up, the conventions used in the codebase, and the pull request process.

## Prerequisites

- Go 1.25.11 or later (the version pinned in `go.mod`)
- A C toolchain (`CGO_ENABLED=1`): the query engine links DuckDB
- A MySQL 8.0+ instance for integration testing (unit tests run without a DB)
- `git`

## Getting started

```sh
git clone https://github.com/dbtrail/dbtrail
cd dbtrail
go mod download
go build ./...
go test ./...
```

All unit tests pass without a running database. If you want to test against a real MySQL instance, see [Integration testing](#integration-testing) below.

## Project layout
```
cmd/bintrail/          The main CLI. One file per command.
cmd/bintrail-console/  The web interface and the `watch` daemon (capture + web interface in one process).
cmd/bintrail-mcp/      The MCP server (read-only tools for AI clients).
cmd/bintrail-pg/       The PostgreSQL source CLI.
cliapp/, consoleapp/   Exported entry points the binaries are built from.
internal/              Core packages. Each package has a _test.go alongside it.
  parser/              Binlog parsing.
  indexer/             Writes parsed events into the index.
  query/, recovery/    Reading the index and generating reversal SQL.
  reconstruct/         Point-in-time state: a baseline snapshot plus the changes after it.
  console/             The web interface (HTTP API and browser assets).
  mcptools/            The MCP tool implementations, shared by bintrail-mcp and the web interface.
  cliutil/             Shared filter parsers (ParseEventType, ParseTime, IsValidFormat).
docs/                  User documentation. Start with docs/guide.md.
docs/migrations/       Reference DDL. Tables are created by `bintrail init`, not this file.
test/                  End-to-end, shim and browser test suites.
```

## Making changes

### Adding a new command

1. Create `cmd/bintrail/<command>.go` in `package main`.
2. Declare a `var <cmd>Cmd = &cobra.Command{...}` and register it in `init()` with `rootCmd.AddCommand(...)`.
3. Name flag variables with a short prefix matching the command (e.g. `rotRetain` for `rotate`, `stIndexDSN` for `status`).
4. Reuse `cliutil.ParseEventType` and `cliutil.ParseTime` from `internal/cliutil` if the command accepts time or event-type filters. The MCP server uses the same helpers.
5. Use `mysql.ParseDSN(dsn)` to extract the database name from a DSN; don't use `SELECT DATABASE()`.

### Coding conventions

- Use `any` instead of `interface{}`.
- Use `min()`, `max()` built-ins instead of manual comparisons.
- Use `range N` instead of `for i := 0; i < N; i++`.
- Use `slices.Reverse`, `slices.Sort`, etc. from the standard library.
- Use `strings.SplitSeq` where applicable.
- Keep command files self-contained. Avoid creating shared packages for logic that is only used in one place.

### The web interface: what earns a place on screen

The browser interface (`internal/console/assets`) grew by accretion. Each
feature arrived as a card with a paragraph explaining itself, and the result
had to be studied rather than used: two settings whose names both promised
"backups, automatically" for unrelated things, a card with two paragraphs of
prose and two buttons whose difference was a third concept, a verdict with no
remedy. Every one of them was defensible on its own; the sum was the problem.
Five rules, so it does not grow back that way (#1528):

1. **A card earns its place by answering a question the operator has at that
   moment.** If it answers a question they would only have while reading the
   documentation, it belongs in the documentation.
2. **The name says what the control does.** If two controls need a paragraph
   to be told apart, they are named wrong, or they should be one control.
3. **Explanation is not documentation.** A line of help under a control is
   fine. A paragraph means the control is not self-evident, and the fix is
   the control, not the paragraph.
4. **Every state names its remedy or says nothing.** A verdict the operator
   cannot act on is noise.
5. **Nothing new without something removed.** A new surface comes with a
   proposal for what it replaces or absorbs. The one exemption is a first-run
   surface that disappears after the first event (#1606).

Two corollaries, each bought with a defect:

- **Do not fold the thing the page is named after.** A `<details>` is for a
  detail: raw signals, a glossary, an advanced field, the technical variant.
  The page's own subject is a card. Scheduling a backup and restoring to a
  moment both sat behind a line of small caps on the Backups page, with prose
  above them (#1528); `internal/console/assets_backup_cards_1528_test.go`
  pins those two, and pins that a card in alarm says so in words and not in
  red alone.
- **Name the screen. Never "here", never "this page".** The documentation page
  and the screen it describes carry the same name, so a deictic word resolves
  differently depending on which side the reader is standing on. Say "on the
  Backup settings screen". In user-facing text the product is **DBTrail** and
  the thing on screen is the **web interface**, never "the console". That is
  the binary's name (`bintrail-console`), and a user does not read it (#1683).
  No test enforces this one, unlike the corollary above it: it is a habit, and
  a string sweep is where a word quietly comes back.

The measure of a change here is fewer words on screen and fewer clicks to the
same answer, not more features.

### Working with the database

- **Never insert `pk_hash` explicitly** — it is a generated stored column (`SHA2(pk_values, 256)`).
- **PK lookups** must use both `pk_hash = SHA2(?, 256)` (for the index scan) and `pk_values = ?` (as a hash collision guard).
- **Partitions**: partitioned by `RANGE (TO_SECONDS(event_timestamp))` — use `TO_SECONDS()`, not `UNIX_TIMESTAMP()` (MySQL 8.0 rejects timezone-dependent functions when `time_zone=SYSTEM`). The catch-all `p_future VALUES LESS THAN MAXVALUE` must always exist. When adding new partitions use `REORGANIZE PARTITION p_future INTO (... new partitions ..., PARTITION p_future VALUES LESS THAN MAXVALUE)`.
- **`schema_snapshots`**: `snapshot_id` is a group identifier (shared by all rows of one snapshot), not the auto-increment row PK (`id`). `NewResolver(db, 0)` loads the latest snapshot.

### JSON and type handling

After a JSON round-trip (`json.Unmarshal` into `map[string]any`), all numbers are `float64`. Use the integer-detection pattern from `formatValue` in `recovery.go` when formatting values for SQL:

```go
if val == math.Trunc(val) && math.Abs(val) < 1e15 {
    return strconv.FormatInt(int64(val), 10)
}
```

MySQL JSON column values returned by go-mysql are raw JSON bytes (`[]byte`). Promote them to `json.RawMessage` before marshalling to avoid base64 encoding — see `marshalRow` in `indexer/indexer.go`.

## Writing tests

- Unit tests should not require a live database. Use in-memory data structures and test the pure helper functions.
- Recovery tests use `newGen()` (`New(nil, nil)`) — nil DB and nil resolver triggers the all-columns WHERE fallback, which is safe for unit tests.
- Do not hardcode UNIX timestamps for specific future dates. Compute them at test time:
  ```go
  ts := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC).Unix()
  ```
- Use the `assertSQL(t, stmt, want)` / `assertContains(t, s, want)` helpers where they exist in the same test file.
- Test files in `cmd/bintrail/` are `package main` and have direct access to all unexported helpers — use this for testing command-level helpers without exporting them.

Run the full suite before opening a PR:

```sh
go test ./...
go vet ./...
```

## Integration testing

Integration tests require a MySQL instance with:

```sql
SET GLOBAL binlog_format       = ROW;
SET GLOBAL binlog_row_image    = FULL;
```

The recommended approach is a local Docker container:

```sh
docker run -d --name bintrail-test-mysql \
  -e MYSQL_ROOT_PASSWORD=testroot \
  -p 13306:3306 \
  mysql:8.0 \
  --binlog-format=ROW \
  --binlog-row-image=FULL \
  --log-bin=binlog \
  --server-id=1

# Wait for MySQL to be ready. Probe host TCP (the same path the tests use),
# not `docker exec ... mysqladmin ping` — that answers over the container's
# local socket, which can succeed against the mysql image's TEMPORARY
# first-init server before the real one is listening on the mapped port
# (see .github/workflows/ci.yml's MySQL-start steps and issue #949).
consecutive=0
until [ "$consecutive" -ge 3 ]; do
  if docker run --rm --network host mysql:8.0 \
       mysql --protocol=TCP -h 127.0.0.1 -P 13306 -u root -ptestroot -e "SELECT 1" >/dev/null 2>&1; then
    consecutive=$((consecutive + 1))
  else
    consecutive=0
  fi
  sleep 1
done

# Initialise the index
bintrail init --index-dsn "root:testroot@tcp(127.0.0.1:13306)/binlog_index"
```

Run integration tests (requires the container above):

```sh
go test -tags integration ./... -count=1

# With coverage report
go test -tags integration -coverprofile=cover.out ./... -count=1
go tool cover -func=cover.out
```

### The correctness oracle

`internal/oracle` is a safety net over the whole copy pipeline (#2499): a
deterministic generator runs random DML on a real MySQL source (five tables
in a schema of their own: integer, composite and text primary keys, DECIMAL
and DATETIME columns, a unique secondary key; updates, inserts, deletes,
reinserts of deleted keys, key-changing updates, multi-row statements,
transactions, bursts; keys drawn from a small set with two hot keys), through
the real capture (`streamrun.One`). At the end of every step it reads the
source's binlog position, waits until capture's checkpoint reaches it, reads
every table, applies one step to the copy by seed — a snapshot cut or a
refresh with table deltas (`reconstruct.ReconstructTables`) — and reads the
copy the way the MySQL port does (the real views SQL, executed in DuckDB).
Three checks, in order: every primary key appears once in the copy, the row
count equals the source's, and every row equals the source's row in canonical
text. A failure prints the seed, the step, the table, the key, both rows, the
snapshot's files with their sizes and the one-line command that replays it.

```sh
go test -tags integration -run TestOracle -count=1 ./internal/oracle/
```

- `BINTRAIL_ORACLE_SEEDS=N` runs seeds 1..N (default 3); `BINTRAIL_ORACLE_SEED=N`
  replays one seed, which is what a failure message tells you to run.
- `BINTRAIL_ORACLE_STEPS=N` is the number of steps per seed (default 12).
- `BINTRAIL_ORACLE_TABLES=t_int,t_text` narrows the run to some tables.
- `BINTRAIL_ORACLE_REQUIRE=1` makes a missing MySQL a failure instead of a skip.
- It uses `BINTRAIL_TEST_DSN` like every other integration test.

The port's tails (the changes captured after a table's published cut, laid
over it by the console's keeper as a chain of files) are the second slice,
and it lives beside the keeper because the keeper is package-private:

```sh
BINTRAIL_ORACLE_REQUIRE=1 go test -tags integration -run TestOracleTails -count=1 ./internal/console/
```
It takes the same variables, runs on request only (one of
`BINTRAIL_ORACLE_SEEDS`/`BINTRAIL_ORACLE_REQUIRE` set), and adds a third
step, `tail`: the REAL keeper follows the real index and the copy is read as
a statement on the MySQL port reads it (`statementTails`). Two scenarios:
`mixed` draws snapshot, refresh and tail steps (a tail over a fresh snapshot
and over a delta chain, rebuilt on a new cut, keys changed in two tail steps,
deleted then re-inserted across them); `chain` runs tails almost only so the
chain of files grows to its merge, and asserts the merge happened. A table
the port answers as of its published cut (no tail) is compared with the rows
read at that cut, and the run asserts WHY there is no tail: a key the tail
does not support (only integer keys have one, the product's own rule), or no
change to the table since the cut. Any other reason fails the run naming it, so tails
silently off cannot pass.

One rule: **a seed that fails never widens the oracle.** The product is fixed,
or an issue is filed with the seed; the oracle itself does not learn to
tolerate the difference. The generator's unit tests (same seed, same
statements; keys stay in the set; a reinsert only after a delete) run without
MySQL.

## Pull request checklist

- [ ] `go test ./...` passes
- [ ] `go vet ./...` is clean
- [ ] New behaviour is covered by tests
- [ ] User-facing docs under `docs/` are updated if behaviour or flags changed
- [ ] Commit messages are clear and describe *why*, not just *what*

## Commit style

Short imperative subject line, 72 chars max. Body is optional but encouraged for non-trivial changes:

```
Add changed-column filter to query engine

JSON_CONTAINS is used instead of a string search so the filter matches
exact column names and not substrings (e.g. "status" must not match
"order_status").
```

## Contributor License Agreement

By opening a pull request, you agree to the terms of the [Contributor License Agreement](https://github.com/dbtrail/dbtrail/blob/main/.github/CLA.md). First-time contributors will be asked to confirm via [CLA Assistant](https://cla-assistant.io/) before their PR can be merged.

## License

This project is licensed under the [Apache License, Version 2.0](https://github.com/dbtrail/dbtrail/blob/main/LICENSE). By contributing, you grant the maintainer the rights described in the [CLA](https://github.com/dbtrail/dbtrail/blob/main/.github/CLA.md), which include the right to use your contributions under the current and future license terms.
