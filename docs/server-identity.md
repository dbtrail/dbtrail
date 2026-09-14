# Server Identity and RBAC Flags

This page explains two related features that help you manage multi-server deployments and control access to sensitive data: the `bintrail_id` server identity system and the RBAC flag mechanism.

---

## Server Identity (`bintrail_id`)

### The Problem

When multiple source MySQL servers share a single DBTrail index database, it becomes impossible to tell which indexed events came from which server — the `binlog_events` table stores filenames like `binlog.000042` which are identical across unrelated servers.

The server identity system assigns a stable UUID (`bintrail_id`) to each source server and records it alongside every piece of state that DBTrail writes.

### How It Works

Every command that connects to a source server (`snapshot`, `index`, `stream`) resolves a server identity before doing any work: it reads the source server's connection details (host, port, user), looks them up in the `bintrail_servers` table, and records the resulting `bintrail_id` for the indexed file or stream.

The result is one of five outcomes ("resolution rules"):

| Rule | Condition | Action |
|------|-----------|--------|
| 1. **No-op** | UUID and connection details match existing record | Use the existing `bintrail_id`, do nothing |
| 2. **Migration** | UUID matches but host/port/user changed | Update `bintrail_servers`; log change to `bintrail_server_changes` |
| 3. **UUID regeneration** | host+port+user matches but UUID changed | Update UUID; log change to `bintrail_server_changes` |
| 4. **New server** | No matching record | Insert new row; assign fresh UUID |
| 5. **Conflict** | Same host+port+user, different UUID with a different existing record | **Error — refuses to auto-resolve** |

Rule 5 (conflict) detects a cloned server — two physical machines presenting the same connection string but different UUIDs. This is almost certainly a misconfiguration and DBTrail refuses to guess which server is authoritative. You must manually resolve the conflict in `bintrail_servers`.

### Database Tables

**`bintrail_servers`** — one row per source server:

| Column | Description |
|--------|-------------|
| `bintrail_id` | Stable UUID assigned to this server |
| `host` | Hostname from the source DSN |
| `port` | Port from the source DSN |
| `user` | MySQL user from the source DSN |
| `first_seen` | When this server was first registered |
| `last_seen` | When `ResolveServer` last ran for this server |

**`bintrail_server_changes`** — audit trail for identity changes:

| Column | Description |
|--------|-------------|
| `bintrail_id` | UUID of the affected server |
| `changed_at` | Timestamp of the change |
| `change_type` | `"migration"` or `"uuid_regeneration"` |
| `old_value` | Previous value |
| `new_value` | New value |

### Where `bintrail_id` Appears

Once resolved, the `bintrail_id` is recorded in:

- **`index_state`** — the `bintrail_id` column records which server produced each indexed binlog file
- **`stream_state`** — the `bintrail_id` column records which server the stream is connected to
- **Parquet archives** — the Hive partition key `bintrail_id=<uuid>/` in archive paths (see [Rotation and Status](./rotation-and-status.md#archiving-partitions-to-parquet))

The `bintrail_id` is logged at `slog.Info` level when a command starts, so it appears in structured logs.

### `bintrail status` per-server view

The `bintrail status` command shows the `BINTRAIL_ID` column in the Indexed Files table and groups the Summary section by server:

```
=== Indexed Files ===
FILE              STATUS     EVENTS  STARTED_AT           COMPLETED_AT         ERROR  BINTRAIL_ID
────              ──────     ──────  ──────────           ────────────         ─────  ───────────
binlog.000042     completed  12345   2026-02-19 10:00:00  2026-02-19 10:00:42  -      3e11fa47-71ca-11e1-9e33-c80aa9429562
binlog.000043     completed  8901    2026-02-19 10:00:43  2026-02-19 10:01:12  -      3e11fa47-71ca-11e1-9e33-c80aa9429562
binlog.000001     completed  200     2026-02-19 10:00:00  2026-02-19 10:05:00  -      bbbbbbbb-0000-0000-0000-000000000002
binlog.000002     completed  0       2026-02-19 11:00:00  -                    -      -

=== Summary ===
Server 3e11fa47-71ca-11e1-9e33-c80aa9429562
  Files:  2 completed, 0 in_progress, 0 failed
  Events: 21246 indexed

Server bbbbbbbb-0000-0000-0000-000000000002
  Files:  1 completed, 0 in_progress, 0 failed
  Events: 200 indexed

Server (unknown)
  Files:  1 completed, 0 in_progress, 0 failed
  Events: 0 indexed
```

`-` in the `BINTRAIL_ID` column (or `COMPLETED_AT`) means the value is NULL. Rows with a NULL `bintrail_id` were written before the server identity system was introduced.

---

## RBAC Flags

### Overview

The flag system lets you label tables and columns with named flags (e.g. `billing`, `pii`, `sensitive`). These flags are the foundation for role-based access control: access rules can then restrict which profiles may query data carrying each flag.

**Current state**: Fully implemented and enforced. The flag registry (`table_flags`, `profiles`, `access_rules` tables, managed by the `bintrail flag`, `bintrail profile`, and `bintrail access` CLIs) is in place, and **profile enforcement is live**: pass `--profile <name>` to `query` or `recover` and the read path withholds denied tables and redacts (NULLs) flagged columns at query time. See [`bintrail profile` / `bintrail access`](#bintrail-profile-and-bintrail-access-commands) below.

**From the console**: the same three tables can be authored from the web
console's **Settings > Access profiles** page (`bintrail-console serve` or
`watch`), against the server selected in the sidebar. The page and the CLI
run the same code, so a profile authored in either place is the same rows
and is refused for the same reasons; the console additionally requires the
`settings:write` permission and records every change on the audit seam. See
[console.md](console.md#the-access-profiles-page).

### Database Tables

**`table_flags`** — the flag registry:

| Column | Description |
|--------|-------------|
| `schema_name` | Source database name |
| `table_name` | Table name |
| `column_name` | Column name, or `''` for a table-level flag |
| `flag` | Flag name (e.g. `"pii"`, `"billing"`) |
| `created_at` | When the flag was added |

A unique constraint on `(schema_name, table_name, column_name, flag)` prevents duplicates.

**`profiles`** — named access profiles (e.g. `dev`, `marketing`, `super-admin`).

**`access_rules`** — maps a profile to a flag with `allow` or `deny` permission. Deleting a profile cascades to its access rules.

### `bintrail flag` Command

#### Add a flag

```sh
# Table-level flag (omit --column)
bintrail flag add billing \
  --schema    mydb \
  --table     orders \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"

# Column-level flag
bintrail flag add pii \
  --schema    mydb \
  --table     customers \
  --column    email \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

Adding a flag that already exists is a no-op (`ON DUPLICATE KEY UPDATE flag = flag`).

#### Remove a flag

```sh
bintrail flag remove pii \
  --schema    mydb \
  --table     customers \
  --column    email \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index"
```

#### List flags

```sh
# All flags
bintrail flag list --index-dsn "..."

# Filter by schema
bintrail flag list --schema mydb --index-dsn "..."

# Filter by schema + table
bintrail flag list --schema mydb --table customers --index-dsn "..."
```

Example output:

```
SCHEMA   TABLE      COLUMN    FLAG      CREATED
──────   ─────      ──────    ────      ───────
mydb     customers  (table)   billing   2026-03-01 05:00:00
mydb     customers  email     pii       2026-03-01 05:00:00
mydb     orders     (table)   billing   2026-03-01 05:00:00
```

`(table)` in the COLUMN field means the flag applies to the table as a whole.

### `bintrail profile` and `bintrail access` commands

Flags label *what* is sensitive; **profiles** and **access rules** decide *who*
may see it. A profile is a named role; an access rule maps a profile to a flag
with `allow` or `deny`. At query time, `--profile <name>` withholds denied tables
and redacts (NULLs) denied columns.

```sh
# 1. Create a profile (a role)
bintrail profile add marketing --description "Marketing analysts" --index-dsn "$IDX"

# 2. Grant/deny that profile access to a flag
bintrail access add --profile marketing --flag pii --permission deny --index-dsn "$IDX"

# 3. Run a query/recover as that profile — pii-flagged columns come back NULL,
#    deny-flagged tables are withheld
bintrail query  --profile marketing --schema mydb --table customers --index-dsn "$IDX"
bintrail recover --profile marketing --schema mydb --table orders   --index-dsn "$IDX" --dry-run
```

`profile remove <name>` deletes a profile and cascades to its access rules;
`profile list` and `access list [--profile <name>]` show what's configured.
`access remove --profile <name> --flag <flag>` drops a single rule. All take
`--index-dsn`.

### `--flag` Filter on `query` and `recover`

Once flags are applied, you can filter events to only those from flagged tables or columns:

```sh
bintrail query \
  --index-dsn "..." \
  --flag      billing
```

This returns events from any table or column that has the `billing` flag. The filter matches if the event's `(schema_name, table_name)` has a `billing` flag at the table level, column level, or both. Events are never duplicated even when a table has multiple matching flag rows.

The `--flag` parameter is also available on the MCP `query` and `recover` tools:

```
"Show me all billing-related changes today"
```
