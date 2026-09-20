# Contributing to DBTrail

Thank you for your interest in contributing. This document covers how to get set up, the conventions used in the codebase, and the pull request process.

## Prerequisites

- Go 1.24 or later (the codebase uses `range N`, `min()`, `slices`, and `strings.SplitSeq`)
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
cmd/bintrail/      One file per command.
cmd/bintrail-mcp/  MCP server exposing query, recover, and status as read-only tools.
internal/          Core packages. Each package has a _test.go alongside it.
  cliutil/         Shared filter parsers (ParseEventType, ParseTime, IsValidFormat) — used by
                   both cmd/bintrail/ commands and cmd/bintrail-mcp/.
  observe/         Structured logging (slog) setup and Prometheus metrics for stream.
  status/          Shared index status types and display helpers.
migrations/        Reference DDL — tables are created by `bintrail init`, not this file.
```

Read `CLAUDE.md` for a detailed map of the architecture, key patterns, and gotchas discovered during development.

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
- Use `strings.SplitSeq` (Go 1.24) where applicable.
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
  above them (#1528); a guard in `assets_backup_cards_1528_test.go` pins those
  two.
- **Name the screen. Never "here", never "this page".** The documentation page
  and the screen it describes carry the same name, so a deictic word resolves
  differently depending on which side the reader is standing on. Say "on the
  Backup settings screen". In user-facing text the product is **DBTrail** and
  the thing on screen is the **web interface**, never "the console". That is
  the binary's name (`bintrail-console`), and a user does not read it (#1683).

The measure of a change here is fewer words on screen and fewer clicks to the
same answer, not more features.

### Working with the database

- **Never insert `pk_hash` explicitly** — it is a generated stored column (`SHA2(pk_values, 256)`).
- **PK lookups** must use both `pk_hash = SHA2(?, 256)` (for the index scan) and `pk_values = ?` (as a hash collision guard).
- **Partitions**: partitioned by `RANGE (TO_DAYS(event_timestamp))` — use `TO_DAYS()`, not `UNIX_TIMESTAMP()` (MySQL 8.0 rejects timezone-dependent functions when `time_zone=SYSTEM`). The catch-all `p_future VALUES LESS THAN MAXVALUE` must always exist. When adding new partitions use `REORGANIZE PARTITION p_future INTO (... new partitions ..., PARTITION p_future VALUES LESS THAN MAXVALUE)`.
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

## Pull request checklist

- [ ] `go test ./...` passes
- [ ] `go vet ./...` is clean
- [ ] New behaviour is covered by tests
- [ ] `CLAUDE.md` is updated if you introduced a new pattern, gotcha, or architectural decision
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

By opening a pull request, you agree to the terms of the [Contributor License Agreement](CLA.md). First-time contributors will be asked to confirm via [CLA Assistant](https://cla-assistant.io/) before their PR can be merged.

## License

This project is licensed under the [Apache License, Version 2.0](../LICENSE). By contributing, you grant the maintainer the rights described in the [CLA](CLA.md), which include the right to use your contributions under the current and future license terms.
