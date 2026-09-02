# Using Claude with dbtrail (MCP)

dbtrail ships an MCP server, `bintrail-mcp`, that lets **Claude** — in Claude
Code, Claude Desktop, or claude.ai — search your change history and draft
recoveries in plain English. Once connected, you can ask:

> "What got deleted from the `orders` table in the last 10 minutes?"
>
> "Generate the SQL to bring back customer 42."
>
> "What did customer 42's row look like at 3pm yesterday?"
>
> "What schema changes happened this week?"

It exposes six **read-only** tools — `query`, `recover` (generates reversal SQL,
never runs it), `recover_cascade` (reversal SQL for foreign-key cascade side
effects), `reconstruct` (a row's state at a point in time), `status`, and
`list_schema_changes` — and never writes to your database.

> **First time?** If you run the web console, the shortest path is the
> [5-minute Connect-AI guide](connect-ai.md) — console URL + token + a
> one-click bundle, no JSON and no DSN. This page is the full reference.

---

## Before you start

You need two things on the machine where Claude runs:

1. **The `bintrail-mcp` binary.** It ships in the release archives, the
   `.deb`/`.rpm` (`bintrail` package), and the Docker image — or install it
   directly:
   ```sh
   go install github.com/dbtrail/dbtrail/cmd/bintrail-mcp@latest
   ```
2. **Your index DSN** — the same `BINTRAIL_INDEX_DSN` your dbtrail stack uses,
   e.g. `user:pass@tcp(127.0.0.1:3306)/binlog_index`.

Using the [one-click bundle](#claude-desktop-one-click) or
[bridge mode](#bridge-mode---connect)? Skip both — you only need the remote
endpoint's URL and token; the DSN stays on the server side.

---

## Connect Claude

Pick the setup that matches where you run Claude. **Claude Code is the simplest —
start there.**

### Claude Code (local)

Add an `.mcp.json` at your project root (or `~/.claude.json` to use it
everywhere):

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "bintrail-mcp",
      "env": { "BINTRAIL_INDEX_DSN": "user:pass@tcp(127.0.0.1:3306)/binlog_index" }
    }
  }
}
```

Restart Claude Code. The `query`, `recover`, `recover_cascade`, `status`,
`list_schema_changes`, and `reconstruct` tools appear — now ask: *"What changed in the orders table in the last hour?"*

> Working inside the dbtrail **source repo**? Use
> `"command": "go", "args": ["run", "./cmd/bintrail-mcp"]` instead — no separate
> install needed.

### Claude Desktop (one-click)

Every release publishes **MCP Bundle** artifacts — `dbtrail-<os>-<arch>.mcpb`
files on the [releases page](https://github.com/dbtrail/dbtrail/releases) —
that Claude Desktop installs without any JSON editing:

1. Download the `.mcpb` matching the machine where Claude Desktop runs.
2. Double-click it (or Claude Desktop → **Settings → Extensions**, drop the
   file in).
3. Fill in the two-field form:
   - **Console / MCP endpoint URL** — your bintrail MCP endpoint, e.g.
     `http://localhost:8090/mcp` (a `bintrail-mcp --http` server; keep the
     `/mcp` path)
   - **Access token** — sent as an `Authorization: Bearer` header; stored by
     Claude Desktop as a sensitive value

Under the hood the bundle runs the bundled `bintrail-mcp` in bridge mode
(`--connect`, next section), so it works against a LAN/VPN/tunneled deployment
— the endpoint never needs to be publicly exposed. No DSN appears anywhere:
the remote endpoint owns the database connection.

Release bundles currently cover the release build matrix (Linux amd64/arm64).
On macOS or Windows, build a host-native bundle from source with `make mcpb`
(output in `dist/mcpb/`), or configure bridge mode by hand as shown below.

### Bridge mode (`--connect`)

`bintrail-mcp --connect <url>` is what the bundle runs, and you can use it
directly with any stdio MCP client: the process serves MCP over stdio locally
and proxies every request to a remote bintrail Streamable-HTTP MCP endpoint,
forwarding the remote's tools verbatim.

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "bintrail-mcp",
      "args": ["--connect", "http://192.168.1.10:8080/mcp", "--token", "YOUR-TOKEN"]
    }
  }
}
```

`--token` is optional (a plain `bintrail-mcp --http` server today has no
auth); when set, it is sent as `Authorization: Bearer`. `--connect` cannot be
combined with `--http` or `--tenant-dsns`, and no DSN is needed — the remote
end resolves it. If the endpoint is unreachable or the token is rejected, the
bridge exits non-zero with a one-line error (visible in Claude Desktop's MCP
logs) instead of hanging.

### Claude Desktop (local index)

Same idea, in Claude Desktop's config file
(`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "bintrail": {
      "command": "bintrail-mcp",
      "env": { "BINTRAIL_INDEX_DSN": "user:pass@tcp(127.0.0.1:3306)/binlog_index" }
    }
  }
}
```

Restart Claude Desktop.

### Index on another machine (Claude Desktop)

If the index runs on a different host, start the server **there** over HTTP:

```sh
BINTRAIL_INDEX_DSN='user:pass@tcp(127.0.0.1:3306)/binlog_index' \
  bintrail-mcp --http :8080
```

Then connect from your laptop with the one-click bundle or
[bridge mode](#bridge-mode---connect) above (`--connect
http://192.168.1.10:8080/mcp`). If you'd rather not install a binary locally,
the Python `proxy.py` does the same job — see
[proxy.py (remote bridge)](#proxypy-remote-bridge) below.

### claude.ai and Claude mobile

claude.ai and the mobile apps connect **outbound** to a public HTTPS URL and
authenticate with OAuth. Neither `bintrail-mcp` nor `bintrail-console` speaks
OAuth, so this path needs an OAuth-capable gateway in front — an **advanced,
optional** setup. Most people use Claude Code or Claude Desktop above instead;
those reach a private index over stdio or bridge mode and need no public
endpoint at all.

- **dbtrail's hosted gateway (managed-service customers).** dbtrail operates one
  at `https://mcp.dbtrail.com/mcp` and provisions your tenant. This requires a
  dbtrail account — it is a managed service, not part of this repository.
- **Your own gateway.** This repository does not ship one. The binaries here are
  `bintrail`, `bintrail-console`, `bintrail-mcp`, and `bintrail-pg` — none of
  them terminate OAuth. If you want the claude.ai path on your own
  infrastructure, put an OAuth-capable reverse proxy (any of the usual
  identity-aware proxies) in front of `bintrail-mcp --http` or the console's
  `/mcp` endpoint, and have it forward the authenticated request. For a shared
  backend serving several indexes, `bintrail-mcp --tenant-dsns` resolves a
  per-tenant DSN from an `X-Bintrail-Tenant` request header your proxy sets.

Once such a gateway is reachable at a public URL: in **claude.ai → Settings →
Integrations → Add custom integration**, enter the gateway URL (your domain, or
`https://mcp.dbtrail.com/mcp` for managed customers), complete the OAuth login,
and the six tools appear. Works from web, Desktop, and mobile; tokens refresh
automatically.

---

## The six tools

| Tool | CLI equivalent | What it does |
|---|---|---|
| `query` | `bintrail query` | Search indexed row changes with filters |
| `recover` | `bintrail recover --dry-run` | Generate reversal SQL (never executes it) |
| `recover_cascade` | `bintrail recover-cascade --dry-run` | Generate reversal SQL for foreign-key `ON DELETE`/`ON UPDATE` cascade side effects InnoDB ran below the binlog — the child rows plain `recover` cannot see. Searches the Parquet archives like `recover` (the server's `no_archive` posture confines it to the live index; an unreadable archive is a tool error). Fails with the reasons when the synthesis is provably partial, unless `allow_incomplete` is set |
| `reconstruct` | `bintrail reconstruct` | A single row's full state at a point in time (needs a baseline) |
| `status` | `bintrail status` | Indexed files, partitions, and summary |
| `list_schema_changes` | reads `schema_changes` (see [DDL tracking](./ddl-tracking.md)) | DDL changes recorded while indexing/streaming, with the full statement, binlog coordinates, and the covering `snapshot_id` (`null` = no auto-snapshot; a TRUNCATE row's null is by design and carries a `snapshot_note` saying so) |

All six are read-only — annotated `ReadOnlyHint: true` and `IdempotentHint: true`,
so the client knows they're safe to call repeatedly and never modify state.

If your client lists tools beyond these six, you are running a build that
registers extras through the extension seam (`ext/mcpext`) — a distribution
that wraps this core, not the stock binary. Such tools resolve their index
through the same routing as the built-in six, so they read the server you
selected and inherit its posture, including the console's refusal of a
tool-level `index_dsn`.

The same six tools are also served by the web console at `/mcp` (Streamable
HTTP, console-token auth, per-server routing by URL path) — if you already run
`bintrail-console`, you may not need this binary at all; see
[console.md](console.md#mcp-endpoint).
`list_schema_changes` accepts `schema`, `table`, `ddl_type`
(`CREATE`/`ALTER`/`DROP`/`RENAME`/`TRUNCATE`, prefix-matched so `ALTER` matches
`ALTER TABLE`), `since`, `until`, `limit` (default 100), and `uncovered_only`
(exactly the rows behind the `status` tool's "DDL(s) detected without
auto-snapshot" warning — `snapshot_id` is `null` AND the DDL is not a
`TRUNCATE TABLE`, whose null is by design since it changes no structure; a
TRUNCATE row in the plain listing carries a `snapshot_note` saying so, and
`ddl_type: TRUNCATE` lists them all); results come back
newest-first, and changes inside the same second are ordered by binlog
coordinate (`binlog_file`, then `binlog_pos`), so a migration's burst of DDLs
reads in the order it ran and a `limit` that cuts inside the burst keeps the
same rows on every call. One caveat: `schema_changes` records no source
identity, so in an index fed by more than one source, or by a Postgres source
(whose LSN file names are not zero-padded), same-second changes have a
repeatable order, not their true one. The sort runs over the filtered rows, so
narrow with `since`/`until` on a large table.

Prompts that work well:

> "Someone deleted rows from `users` this afternoon — show me what and when."
>
> "Generate the SQL to undo the bad UPDATE on order 1234."
>
> "What was customer 42's row before the last change?"

---

## Tool parameters and behavior (reference)

The `query` and `recover` tools share the CLI's filters and validation. Beyond
the basics (`schema`, `table`, `pk`, `event_type`, `gtid`, `since`, `until`),
both accept:

- `pks` — multiple primary key values (each pipe-delimited for composite
  keys); requires `schema` and `table`, mutually exclusive with `pk`
- `pk_min` / `pk_max`: an inclusive primary key range, either bound alone or
  both, as integer strings. Only for tables whose primary key is one integer
  column: the tool checks the key against the schema snapshot first and
  refuses a composite or non-integer key with the table's actual key shape.
  Requires `schema` and `table`, mutually exclusive with `pk`/`pks`. The
  range cannot use the key index, so it scans the partitions the time filters
  keep: pair it with `since`/`until`. See
  [Primary key ranges](query-and-recovery.md#primary-key-ranges---pk-min----pk-max)
- `limit_per_pk` — cap events per `pk_values` to the latest N (0 = unlimited);
  requires `pk` or `pks`. The cap always keeps each row's *latest* N whatever
  `order` says, so with it active a truncated `ASC` answer loses events at
  both ends of the window (the warning says so instead of naming a kept end);
  under `DESC` the newest events are still intact and older ones drop from
  inside the window
- `column_eq` — repeatable `column=value` equality filters
- `flag` — table/column flag filter
- `profile` — RBAC table-deny + column-redaction
- `no_archive` — disable Parquet archive auto-routing (see below)

`query` also takes `changed_column` — events that touched a given column.
`recover` does not: a changed-column filter selects row *versions*, and
reversing a filtered subset of a row's history can produce a state that never
existed — the same reason the `bintrail recover` CLI has no `--changed-column`
flag (a `recover` call passing `changed_column` is rejected by the tool's
input schema).

`query` also takes `query_hash` — the 64-character statement digest of an event
you already have, returning every event that statement produced across every
table it touched. It selects a statement *shape* (literals are normalised
away), so it is a read filter only: `recover` does not accept it, because a
reversal scoped to a shape would undo executions nobody named. It is refused on
surfaces that withhold statement text (the console's `/mcp`) and whenever a
`profile` is active — the digest is blanked on every returned event there, so
filtering on it would confirm what is withheld.

The `pk` parameter takes the stored `pk_values` spelling — for a binary PK
whose bytes are not valid UTF-8 that is `0x` + uppercase hex, see
[Binary primary keys](query-and-recovery.md#binary-primary-keys-the-0x-hex-spelling).

`query` additionally takes `format` (`json`, `table`, or `csv`) and `order`
(`ASC`, the default, or `DESC`). The direction decides which end a truncating
`limit` keeps: `ASC` returns oldest-first and keeps the **oldest** matching
events, `DESC` returns newest-first and keeps the **newest** — so "the last N
events" is `order: DESC` with `limit: N`, no `since` guessing required, and
the truncation warning names the end it kept. Time values (`since` / `until`)
accept MySQL datetime (`2006-01-02 15:04:05`), RFC 3339, or date-only
(`2006-01-02`) — the same formats as the CLI.

**Query row ceiling ([#654](https://github.com/dbtrail/dbtrail/issues/654)).** The
`query` tool caps an explicit `limit` at a hard ceiling (default **1,000,000
rows**; env `BINTRAIL_MCP_QUERY_MAX_LIMIT`) so an oversized request can't exhaust
the long-lived server's memory; when it caps, the returned text says so. A `limit`
of `0` or omitted falls back to the tool default (100), not unbounded — the
unbounded path is the `bintrail query` CLI, not the agent-facing tool. The
`recover` tool is **not** capped this way: it refuses oversized output on a memory
budget instead (see [Query and Recovery](query-and-recovery.md#recovery)).

**Archive auto-discovery.** `query` and `recover` automatically discover Parquet
archive sources from `archive_state` in the index database (or from the
`BINTRAIL_ARCHIVE_S3` + `BINTRAIL_ID` env vars). When archives are found, results
from MySQL and Parquet are merged, deduplicated, and sorted before output or SQL
generation. Pass `no_archive` to disable this.

When archives misbehave, the two tools diverge on purpose
([#1285](https://github.com/dbtrail/dbtrail/issues/1285)): `query` degrades and
says so — the result carries trailing `Warning: archive_source_skipped: …` lines
for sources that failed to read, `Warning: archive_discovery_failed: …` when
discovery itself failed, and `Warning: archive_scan_incomplete: …` when the
misfiled-archive registry scan failed — while `recover` **refuses** to generate
a script in any of those cases: a reversal missing part of the matched events is
a partial undo, not a smaller one. The refusal names the escape hatch — retry
with `no_archive: true` to generate from the live index only, accepting that
archived events will not be reversed.

**Large reversal scripts.** An MCP client caps how big a tool result can be, and
a cascade touching thousands of rows produces a script well past that cap. Both
`recover` and `recover_cascade` handle this the same way
([#1438](https://github.com/dbtrail/dbtrail/issues/1438)), and neither ever
truncates: a script that fits comes back whole, exactly as before, and a script
that does not is **withheld** with its statement count, its size, and
instructions.

- `summary_only` returns the counts (and, for `recover_cascade`, the whole
  coverage report) with no script. It answers "how big is this, is it complete"
  before you decide to pull the script at all.
- `sql_offset` / `sql_limit` fetch the script in pieces. They count
  **statements**, not bytes, and chunks cut on statement boundaries, so no chunk
  ever splits a statement — a `;` and a newline are both ordinary characters
  inside a captured value. Concatenating the chunks in order reproduces the
  script byte for byte, with the `BEGIN`/`COMMIT` framing and the preamble in
  the first and last chunk exactly once. **No chunk is runnable on its own.**
- Every chunk carries `script_id`. Chunks are stateless rebuilds, so a chunk
  fetched after an event was indexed, a baseline published or archives rotated
  belongs to a *different* build; a changed `script_id` means the fetch must
  restart at `sql_offset: 0`. Concatenating across ids assembles a script that
  never existed.
- `sql_limit` is reduced when the chunk would overflow the response. The result
  says so and gives `next_sql_offset`; nothing is dropped.

`recover` returns plain SQL text for a whole script, as it always has, and
switches to a JSON envelope (whose `sql` field holds the exact bytes) for a
summary or a chunk — a client has to be able to tell a chunk from a complete
script by reading a field, which a SQL comment cannot do. `recover_cascade` is
JSON in every case.

For a script too large to be comfortable over MCP at all, `bintrail recover` and
`bintrail recover-cascade` from the CLI write the whole thing to a file in one
go.

**Index DSN.** The server connects to the index via the `BINTRAIL_INDEX_DSN`
environment variable (set once at startup) or the per-call `index_dsn` parameter,
which overrides the env var. Set the env var at startup so callers don't repeat
it on every call.

### `reconstruct` (time travel)

`recover` is **delta-only**: it reverses events it actually has, so it can't
answer "what did this row look like at 3pm" for a row nobody touched in the
retained window. `reconstruct` folds a **baseline snapshot** (from
[`bintrail baseline`](dump-and-baseline.md)) with the events after it, so every
column resolves — the same engine behind `bintrail reconstruct` and the console's
Time-travel tab.

Parameters: `schema`, `table` and `pk` (required; pipe-delimited for composite
keys), `at` (defaults to now), `history` (every transition instead of one state),
and `allow_gaps`.

Baseline location: `baseline_dir` (a local directory) or `baseline_s3`
(`s3://bucket/prefix`) per call, falling back to the `BINTRAIL_BASELINE_DIR` /
`BINTRAIL_BASELINE_S3` env vars — set those at startup, like the index DSN. When
the console serves `/mcp` these parameters are **rejected**: the baseline is that
server's own configuration, and an MCP client must not point the console at
arbitrary storage.

**`allow_gaps` defaults to `false`, unlike `query`'s degrade-with-warnings
posture (`recover` likewise refuses on archive trouble — see Archive
auto-discovery above).** A hole in the
captured history makes a reconstruction *silently wrong* (a state that never
existed), not merely incomplete, so the tool aborts with an actionable error and
you opt in explicitly. It also refuses outright — no override — when a
`TRUNCATE`/`DROP`/`RENAME` hit the table inside the window: no archive can refill
that, and the fold would resurrect rows that are gone.

The same `allow_gaps: false` default covers two further refusals, both of which
`allow_gaps: true` overrides — and when you override one, the reason comes back
as a `capture_gap:` entry in `warnings`, so a known-incomplete result is never
returned as a clean one:

- the stream recorded events **permanently lost at the source** inside the
  window (`gap_lost_at`, shown as `GAP LOST` by
  [`bintrail status`](rotation-and-status.md)) — no archive can refill those;
- the index is too old to answer the question: its `stream_state` predates the
  gap-tracking columns, so a permanent loss cannot be ruled out. Migrating the
  index schema (any indexing or streaming command does it) enables the real
  check.

Archive sources come from `archive_state` (not the `BINTRAIL_ARCHIVE_S3` env
pair), because gap detection reads the same registry — run
[`bintrail archive reconcile`](rotation-and-status.md) if a source drifted.

Returns JSON: `state` (or `history`), `found`, `deleted`, `baseline_time`,
`event_count`, and any `warnings` (a coverage gap or permanent capture loss
you allowed, a [stale-baseline fallback](query-and-recovery.md), a suspected
PK-changing UPDATE the fold can't follow, or — under `allow_gaps` — an archive
source that failed or could not be discovered, and coverage the planner could
not verify).

---

## proxy.py (remote bridge)

When the index runs on a different machine than Claude Desktop, `proxy.py`
bridges Claude Desktop (which speaks stdio) to a remote `bintrail-mcp --http`
server:

```
Claude Desktop  →  proxy.py (stdio, runs locally)  →  bintrail-mcp --http :8080  →  MySQL
```

It's a single Python file with zero dependencies (stdlib only, Python 3.7+).

1. Copy `proxy.py` (from `cmd/bintrail-mcp/proxy.py`) to your laptop.
2. Add it to `~/Library/Application Support/Claude/claude_desktop_config.json`:

   ```json
   {
     "mcpServers": {
       "bintrail": {
         "command": "python3",
         "args": ["/Users/you/proxy.py"],
         "env": { "BINTRAIL_SERVER": "http://192.168.1.10:8080/mcp" }
       }
     }
   }
   ```

3. Test connectivity before restarting Claude Desktop:

   ```sh
   BINTRAIL_SERVER=http://192.168.1.10:8080/mcp python3 ~/proxy.py <<'EOF'
   {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}
   {"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
   EOF
   ```

   Two JSON responses = working. No response or a connection error = check the
   server address and firewall.

**Stale session after a restart:** when `bintrail-mcp --http` restarts, existing
sessions are invalidated, but `proxy.py` still holds the old `Mcp-Session-Id` and
tool calls start failing. Fix: restart Claude Desktop (that restarts the proxy
and clears the session) — no need to restart the HTTP server.
