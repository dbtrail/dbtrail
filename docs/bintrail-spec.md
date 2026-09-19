# Bintrail — Architecture & Implementation Spec

> **Historical document.** This was the original design spec. The canonical reference is `CLAUDE.md` (maintainer notes, not published). Key differences from this spec: partition expression uses `TO_DAYS()` not `UNIX_TIMESTAMP()`, `schema_snapshots` has separate auto-increment `id` PK (not `snapshot_id`), `stream` command and MCP server were added post-spec, observability (slog + Prometheus) was added post-spec. See [README](../README.md) for the current command reference.

## Overview

**Bintrail** is a CLI tool written in Go that parses MySQL ROW-format binary logs, indexes every row event into a MySQL table with full row data, and provides query and recovery capabilities. The index is self-contained — it stores complete before/after images so recovery does not depend on binlog files still existing on disk.

Repository: private repo, separate from `dbsafe`.

## Hard Requirements

- `binlog_format = ROW` only (STATEMENT not supported)
- `binlog_row_image = FULL` only (MINIMAL not supported — tool should detect and refuse)
- Index stored in MySQL with range partitioning on timestamp for easy rotation
- File-based binlog parsing (not replication protocol — parsing `.binlog` files from disk)
- Go language, using `go-mysql-org/go-mysql` for binlog parsing

## Project Structure

```
bintrail/
├── cmd/
│   └── bintrail/
│       └── main.go                # CLI entrypoint (cobra)
├── internal/
│   ├── parser/
│   │   └── parser.go             # Binlog file parser using go-mysql
│   ├── metadata/
│   │   └── metadata.go           # Schema snapshot loader from information_schema
│   ├── indexer/
│   │   └── indexer.go            # Batch writer to MySQL index table
│   ├── query/
│   │   └── query.go              # Query engine over the index
│   ├── recovery/
│   │   └── recovery.go           # Reversal SQL generator from stored row data
│   └── config/
│       └── config.go             # Configuration and connection settings
├── migrations/
│   └── 001_create_tables.sql     # DDL for index tables
├── go.mod
├── go.sum
└── README.md
```

## CLI Commands

### `bintrail init`

Creates the index tables and metadata tables in the target MySQL instance.

```
bintrail init \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --partitions 7          # number of daily partitions to create
```

### `bintrail snapshot`

Takes a schema snapshot from the source MySQL server and stores it in the index database. Must be run before `index` (or automatically triggered by `index` if no snapshot exists).

```
bintrail snapshot \
  --source-dsn "user:pass@tcp(127.0.0.1:3306)/" \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --schemas "mydb1,mydb2"          # optional filter, default: all user schemas
```

### `bintrail index`

Parses binlog files and populates the index.

```
bintrail index \
  --index-dsn "user:pass@tcp(127.0.0.1:3306)/binlog_index" \
  --source-dsn "user:pass@tcp(127.0.0.1:3306)/" \
  --binlog-dir /var/lib/mysql \
  --files "binlog.000042,binlog.000043"   # specific files, or:
  --all                                    # all binlog files in the directory
  --batch-size 1000                        # events per batch INSERT (default: 1000, max 3855)
  --schemas "mydb1,mydb2"                  # optional filter: only index these schemas
  --tables "mydb1.orders,mydb1.items"      # optional filter: only index these tables
```

Behavior:
- If no snapshot exists, automatically run `snapshot` first using `--source-dsn`
- Before parsing, validate `binlog_row_image=FULL` on the source server (query `SHOW VARIABLES LIKE 'binlog_row_image'`)
- On TABLE_MAP_EVENT: compare column count with snapshot; warn if mismatch
- Track which files have been indexed to avoid re-processing (store in a `binlog_index_state` table)
- On DDL detection (QUERY_EVENT with ALTER/CREATE/DROP TABLE): log a warning suggesting re-snapshot

### `bintrail query`

Search the index. Output is a formatted table to stdout.

```
# By specific row (PK lookup — single PK)
bintrail query \
  --index-dsn "..." \
  --schema mydb --table orders --pk '12345'

# By specific row (composite PK — pipe-delimited, same order as table PK columns)
bintrail query \
  --index-dsn "..." \
  --schema mydb --table order_items --pk '12345|2'

# By table + time range
bintrail query \
  --index-dsn "..." \
  --schema mydb --table orders \
  --since "2026-02-19 14:00:00" --until "2026-02-19 15:00:00"

# By event type
bintrail query \
  --index-dsn "..." \
  --schema mydb --table orders --event-type DELETE \
  --since "2026-02-19 14:00:00"

# By GTID (what did this transaction touch?)
bintrail query \
  --index-dsn "..." \
  --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42"

# By changed column
bintrail query \
  --index-dsn "..." \
  --schema mydb --table orders --changed-column status \
  --since "2026-02-19 14:00:00"

# Output formats
  --format table        # default: human-readable table
  --format json         # JSON output
  --format csv          # CSV output
  --limit 100           # max rows returned (default: 100)
```

### `bintrail recover`

Generate reversal SQL from the index.

```
# Recover deleted rows
bintrail recover \
  --index-dsn "..." \
  --schema mydb --table orders --event-type DELETE \
  --since "2026-02-19 14:00:00" --until "2026-02-19 14:05:00" \
  --output recovery.sql

# Reverse updates to a specific row
bintrail recover \
  --index-dsn "..." \
  --schema mydb --table orders --pk '12345' --event-type UPDATE \
  --output recovery.sql

# Reverse an entire transaction
bintrail recover \
  --index-dsn "..." \
  --gtid "3e11fa47-71ca-11e1-9e33-c80aa9429562:42" \
  --output recovery.sql

# Dry run (show what would be generated without writing file)
  --dry-run
```

Reversal logic:
- DELETE → generates `INSERT INTO schema.table (col1, col2, ...) VALUES (row_before values);`
- UPDATE → generates `UPDATE schema.table SET col1=before_val1, col2=before_val2 WHERE pk_col=pk_val;`
- INSERT → generates `DELETE FROM schema.table WHERE pk_col=pk_val;`

### `bintrail rotate`

Drop old partitions.

```
bintrail rotate \
  --index-dsn "..." \
  --retain 7d           # keep last 7 days (supports: Nd for days, Nh for hours)
  --add-future 3        # add 3 new future partitions
```

### `bintrail status`

Show index state: which files are indexed, partition info, row counts.

```
bintrail status --index-dsn "..."
```

## DDL — Index Database Tables

Database name: `binlog_index` (configurable)

### Main events table

```sql
CREATE TABLE binlog_events (
    event_id        BIGINT UNSIGNED AUTO_INCREMENT,
    binlog_file     VARCHAR(255)    NOT NULL,
    start_pos       BIGINT UNSIGNED NOT NULL,
    end_pos         BIGINT UNSIGNED NOT NULL,
    event_timestamp DATETIME        NOT NULL,
    gtid            VARCHAR(255)    DEFAULT NULL,
    schema_name     VARCHAR(64)     NOT NULL,
    table_name      VARCHAR(64)     NOT NULL,
    event_type      TINYINT UNSIGNED NOT NULL COMMENT '1=INSERT, 2=UPDATE, 3=DELETE',
    pk_values       VARCHAR(512)    NOT NULL COMMENT 'PK values in ordinal order, pipe-delimited. e.g. 12345 or 12345|2',
    pk_hash         VARCHAR(64) AS (SHA2(pk_values, 256)) STORED,
    changed_columns JSON            DEFAULT NULL COMMENT 'list of columns that changed (UPDATEs only)',
    row_before      JSON            DEFAULT NULL COMMENT 'full row before image (UPDATE, DELETE)',
    row_after       JSON            DEFAULT NULL COMMENT 'full row after image (INSERT, UPDATE)',
    PRIMARY KEY (event_id, event_timestamp),
    INDEX idx_row_lookup (schema_name, table_name, event_timestamp),
    INDEX idx_pk_hash (schema_name, table_name, pk_hash, event_timestamp),
    INDEX idx_gtid (gtid)
) ENGINE=InnoDB
  PARTITION BY RANGE (UNIX_TIMESTAMP(event_timestamp)) (
    PARTITION p_default VALUES LESS THAN MAXVALUE
);
```

Note: `pk_hash` is a generated column included in the CREATE TABLE above. It is automatically computed from `pk_values` on INSERT. No separate ALTER TABLE needed.

Note: `pk_values` is a plain `VARCHAR(512)`, NOT JSON. PK values are stored as a pipe-delimited string in column ordinal order. Column names are not stored in `pk_values` — they come from the schema snapshot when needed.

Examples:
- Single PK `id=12345` → `pk_values = '12345'`
- Composite PK `id=12345, seq=2` → `pk_values = '12345|2'`

`pk_hash` is a generated column (`SHA2(pk_values, 256)`) included directly in the CREATE TABLE above. Because `pk_values` is a simple string, the hash is trivially deterministic — no JSON key ordering or type representation issues.

**Escaping:** If a PK value contains the pipe `|` or backslash `\` character, escape them: `|` → `\|`, `\` → `\\`. In practice PK columns are almost always integers or UUIDs, so this is rarely needed.

**Query pattern — always use both pk_hash AND pk_values to guard against hash collisions:**

```sql
SELECT * FROM binlog_events
WHERE schema_name = 'mydb'
  AND table_name = 'orders'
  AND pk_hash = SHA2('12345', 256)     -- index scan (fast)
  AND pk_values = '12345'               -- collision guard (exact match)
  AND event_timestamp BETWEEN ... AND ...;
```

**Building pk_values in Go:**

```go
// BuildPKValues produces a pipe-delimited string of PK values in ordinal order.
// pkColumns must be sorted by ordinal position (from schema snapshot).
func BuildPKValues(pkColumns []metadata.ColumnMeta, row map[string]interface{}) string {
    var parts []string
    for _, col := range pkColumns {
        val := fmt.Sprintf("%v", row[col.Name])
        val = strings.ReplaceAll(val, `\`, `\\`)
        val = strings.ReplaceAll(val, `|`, `\|`)
        parts = append(parts, val)
    }
    return strings.Join(parts, "|")
}
```

### Schema snapshot table

```sql
CREATE TABLE schema_snapshots (
    snapshot_id     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    snapshot_time   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    schema_name     VARCHAR(64) NOT NULL,
    table_name      VARCHAR(64) NOT NULL,
    column_name     VARCHAR(64) NOT NULL,
    ordinal_position INT UNSIGNED NOT NULL,
    column_key      VARCHAR(3)  NOT NULL COMMENT 'PRI, UNI, MUL, or empty',
    data_type       VARCHAR(64) NOT NULL,
    is_nullable     VARCHAR(3)  NOT NULL,
    column_default  TEXT DEFAULT NULL,
    INDEX idx_snapshot_table (snapshot_id, schema_name, table_name)
);
```

### Index state table (tracks which files have been indexed)

```sql
CREATE TABLE index_state (
    binlog_file     VARCHAR(255) PRIMARY KEY,
    file_size       BIGINT UNSIGNED NOT NULL,
    last_position   BIGINT UNSIGNED NOT NULL COMMENT 'last parsed position',
    events_indexed  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status          ENUM('in_progress','completed','failed') NOT NULL,
    started_at      DATETIME NOT NULL,
    completed_at    DATETIME DEFAULT NULL,
    error_message   TEXT DEFAULT NULL
);
```

## Key Go Interfaces

### Parser

```go
package parser

// Event represents a single parsed binlog row event
type Event struct {
    BinlogFile  string
    StartPos    uint64
    EndPos      uint64
    Timestamp   time.Time
    GTID        string        // may be empty if GTID not enabled
    Schema      string
    Table       string
    EventType   EventType     // INSERT=1, UPDATE=2, DELETE=3
    RowBefore   map[string]interface{}  // nil for INSERT
    RowAfter    map[string]interface{}  // nil for DELETE
}

type EventType uint8

const (
    EventInsert EventType = 1
    EventUpdate EventType = 2
    EventDelete EventType = 3
)

// Parser reads binlog files and emits events through a channel
type Parser struct {
    binlogDir string
    metadata  *metadata.Resolver
}

// ParseFile parses a single binlog file and sends events to the channel.
// The channel-based design allows the indexer to consume events as they
// are parsed without buffering the entire file in memory.
func (p *Parser) ParseFile(ctx context.Context, filename string, events chan<- Event) error

// ParseFiles parses multiple binlog files sequentially.
func (p *Parser) ParseFiles(ctx context.Context, filenames []string, events chan<- Event) error
```

### Metadata Resolver

```go
package metadata

// TableMeta holds the column mapping for a table
type TableMeta struct {
    Schema     string
    Table      string
    Columns    []ColumnMeta   // ordered by ordinal_position
    PKColumns  []string       // column names that form the primary key
}

// ColumnMeta holds info for a single column
type ColumnMeta struct {
    Name            string
    OrdinalPosition int
    IsPK            bool
    DataType        string
}

// Resolver provides table metadata lookups
type Resolver struct {
    tables map[string]TableMeta  // key: "schema.table"
}

// NewResolver loads all table metadata from the snapshot in the index database
func NewResolver(db *sql.DB, snapshotID int) (*Resolver, error)

// Resolve returns metadata for a given schema.table.
// Returns error if table not found in snapshot.
func (r *Resolver) Resolve(schema, table string) (*TableMeta, error)

// MapRow maps a binlog row ([]interface{}) to a named map using column metadata.
// Converts ordinal positions (@1, @2, ...) to column names.
func (r *Resolver) MapRow(schema, table string, row []interface{}) (map[string]interface{}, error)
```

### Indexer

```go
package indexer

// Indexer consumes parsed events and writes them to the index table
type Indexer struct {
    db        *sql.DB
    batchSize int
}

// Run consumes events from the channel and batch-inserts into MySQL.
// It flushes on reaching batchSize or when the channel is closed.
func (idx *Indexer) Run(ctx context.Context, events <-chan parser.Event) (int64, error)
```

### Recovery

```go
package recovery

// Generator produces reversal SQL from indexed events
type Generator struct {
    db *sql.DB
}

// QueryOptions specifies which events to generate recovery SQL for
type QueryOptions struct {
    Schema        string
    Table         string
    PKValues      string             // pipe-delimited PK values, e.g. "12345" or "12345|2"
    EventType     *parser.EventType  // nil = all types
    GTID          string
    Since         *time.Time
    Until         *time.Time
    ChangedColumn string
    Limit         int
}

// GenerateSQL queries the index and produces reversal SQL statements.
// Returns the SQL as a string (or writes to io.Writer).
func (g *Generator) GenerateSQL(ctx context.Context, opts QueryOptions, w io.Writer) (int, error)
```

## Data Flow

```
                          ┌──────────────────────────────────┐
                          │  Source MySQL Server              │
                          │  (information_schema)             │
                          └──────────┬───────────────────────┘
                                     │ snapshot command
                                     ▼
┌────────────────┐         ┌──────────────────────────────────┐
│  Binlog files  │         │  Index MySQL Database            │
│  on disk       │         │  (binlog_index)                  │
│                │         │                                  │
│  binlog.000042 │         │  ┌──────────────────────────┐   │
│  binlog.000043 │         │  │ schema_snapshots         │   │
│  ...           │         │  │ (column metadata)        │   │
└───────┬────────┘         │  └──────────────────────────┘   │
        │                  │                                  │
        │ index command    │  ┌──────────────────────────┐   │
        │                  │  │ binlog_events            │   │
        ├─────parse───────►│  │ (partitioned, full data) │   │
        │                  │  └──────────────────────────┘   │
        │                  │                                  │
        │                  │  ┌──────────────────────────┐   │
        │                  │  │ index_state              │   │
        │                  │  │ (tracking progress)      │   │
        │                  │  └──────────────────────────┘   │
        │                  └──────────────────────────────────┘
        │                            │
        │                            │ query / recover commands
        │                            ▼
        │                  ┌──────────────────────────────────┐
        │                  │  stdout / .sql file              │
        │                  │  (query results, reversal SQL)   │
        │                  └──────────────────────────────────┘
```

## Go Dependencies

```
github.com/go-mysql-org/go-mysql    # binlog parsing
github.com/go-sql-driver/mysql      # MySQL driver
github.com/spf13/cobra              # CLI framework
```

No other external dependencies unless needed. Keep it minimal.

## Behavior Notes

### Indexing

1. Validate source server has `binlog_row_image = FULL`. If not, refuse to index with clear error message.
2. Load schema snapshot (or take one if none exists).
3. For each binlog file:
   a. Check `index_state` — skip if already completed.
   b. Mark as `in_progress`.
   c. Parse events via `go-mysql`. For each TABLE_MAP + ROWS event pair:
      - Resolve column names from metadata snapshot
      - Validate column count matches TABLE_MAP_EVENT
      - Build the Event struct with named before/after maps
      - Compute `changed_columns` for UPDATEs (diff before vs after)
      - Compute `pk_values` as pipe-delimited string from PK columns in ordinal order
      - Send to indexer channel
   d. On QUERY_EVENT containing DDL: log warning "DDL detected: {sql}. Consider re-running snapshot."
   e. On completion, mark as `completed` in `index_state`.
   f. On error, mark as `failed` with error message.

### Querying

- Build SQL dynamically from QueryOptions
- PK lookup uses `pk_hash = SHA2(?, 256) AND pk_values = ?` — hash for index scan, exact match for collision guard
- changed_column filter uses `JSON_CONTAINS(changed_columns, '"column_name"')`
- Results formatted as table, JSON, or CSV per --format flag

### Recovery SQL Generation

- Query the index using same filters as `query`
- For each matching event, generate reversal:
  - DELETE → `INSERT INTO {schema}.{table} ({columns}) VALUES ({row_before values});`
  - UPDATE → `UPDATE {schema}.{table} SET {col=before_val, ...} WHERE {pk_col=pk_val AND ...};`
  - INSERT → `DELETE FROM {schema}.{table} WHERE {pk_col=pk_val AND ...};`
- Wrap output in a transaction: `BEGIN; ... COMMIT;`
- Proper value escaping for all data types (strings, NULLs, dates, binary data)
- When reversing multiple UPDATEs on the same row, apply in reverse chronological order

### Partition Management

- `init` creates N daily partitions starting from today
- `rotate` drops partitions older than the retention period
- `rotate --add-future N` adds N new partitions for upcoming days
- Tool should warn if events are arriving with timestamps outside any partition range

## Error Handling

- Binlog file not found → clear error, continue with next file if --all
- Column count mismatch → warning per table, skip events for that table
- Connection lost to index DB → retry with backoff, fail after N retries
- Malformed binlog event → log warning with file+position, skip event, continue
- GTID not enabled on source → gtid field is NULL, still index everything else

## Testing Considerations

- Unit tests: metadata resolver mapping, changed_columns diffing, recovery SQL generation
- Integration tests: parse a known binlog file, verify indexed events match expected output
- Generate test binlogs by running known DML against a local MySQL instance and capturing binlogs
