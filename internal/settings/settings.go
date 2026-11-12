// Package settings is the one list of the settings DBTrail's programs read
// from the environment (#2671): every BINTRAIL_* name any of them reads, what
// kind of name it is, and whether its value is a secret.
//
// It is the ground a later option file (dbtrail.cnf) stands on: the file will
// accept an option only when this list has it as a Setting, and `dbtrail
// config show` will hide the value of every Secret. Until then the list reads
// nothing and changes nothing; a test keeps it equal to the names the code
// reads, in both directions, so it cannot drift.
//
// It has no dependencies, so every program can use it, bintrail-mcp
// included.
package settings

import (
	"slices"
	"strings"
	"sync"
)

// Kind says what a name is for.
type Kind int

const (
	// Setting is an operator's setting.
	Setting Kind = iota
	// Internal is set by DBTrail itself, or by its own compose file or
	// wrapper, for another of its processes. An operator never sets it, and
	// an option file must never be able to.
	Internal
	// Retired is read only to say it no longer does anything.
	Retired
	// Test is read by the test harness only.
	Test
)

func (k Kind) String() string {
	switch k {
	case Setting:
		return "setting"
	case Internal:
		return "internal"
	case Retired:
		return "retired"
	case Test:
		return "test"
	}
	return "unknown"
}

// Entry is one name.
type Entry struct {
	// Env is the environment variable, BINTRAIL_ included.
	Env  string
	Kind Kind
	// Secret: the value holds a password, a token, a key, or a URL or DSN
	// that carries one. It is never shown.
	Secret bool
}

// Option is the entry's name in an option file: the variable without
// BINTRAIL_, in lower case, with dashes, as MySQL writes its options
// (BINTRAIL_STATUS_LOG_INTERVAL is status-log-interval).
func (e Entry) Option() string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(e.Env, "BINTRAIL_")), "_", "-")
}

var (
	mu    sync.Mutex
	added []Entry
)

// Register adds names a program built on this repository reads of its own.
// It is for a build that embeds these programs and adds features; nothing in
// this repository calls it. A name already listed is ignored.
func Register(entries ...Entry) {
	mu.Lock()
	defer mu.Unlock()
	for _, e := range entries {
		if _, ok := lookupLocked(e.Env); !ok {
			added = append(added, e)
		}
	}
}

// All is every name, sorted by variable.
func All() []Entry {
	mu.Lock()
	defer mu.Unlock()
	out := append(slices.Clone(table), added...)
	slices.SortFunc(out, func(a, b Entry) int { return strings.Compare(a.Env, b.Env) })
	return out
}

// Lookup finds a name by its variable.
func Lookup(env string) (Entry, bool) {
	mu.Lock()
	defer mu.Unlock()
	return lookupLocked(env)
}

func lookupLocked(env string) (Entry, bool) {
	for _, list := range [][]Entry{table, added} {
		for _, e := range list {
			if e.Env == env {
				return e, true
			}
		}
	}
	return Entry{}, false
}

// table is every BINTRAIL_* name this repository's programs read, kept equal
// to the code by TestTableIsWhatTheCodeReads.
var table = []Entry{
	// Capture, the index and their connections.
	{Env: "BINTRAIL_INDEX_DSN", Secret: true},
	{Env: "BINTRAIL_SOURCE_DSN", Secret: true},
	{Env: "BINTRAIL_SOURCE_FLAVOR"},
	{Env: "BINTRAIL_SCHEMAS"},
	{Env: "BINTRAIL_TABLES"},
	{Env: "BINTRAIL_ID"},
	{Env: "BINTRAIL_SERVER_ID"},
	{Env: "BINTRAIL_SERVER_UUID"},
	{Env: "BINTRAIL_BATCH_SIZE"},
	{Env: "BINTRAIL_COMMIT_WITHIN"},
	{Env: "BINTRAIL_START_GTID"},
	{Env: "BINTRAIL_STREAM_GAP_TIMEOUT"},
	{Env: "BINTRAIL_SSL_MODE"},
	{Env: "BINTRAIL_SSL_CA"},
	{Env: "BINTRAIL_SSL_CERT"},
	{Env: "BINTRAIL_SSL_KEY"},
	{Env: "BINTRAIL_METRICS_ADDR"},
	{Env: "BINTRAIL_METRICS_SCRAPE_INTERVAL"},
	{Env: "BINTRAIL_ROTATE_RETAIN"},
	{Env: "BINTRAIL_ROTATE_INTERVAL"},
	{Env: "BINTRAIL_ROTATE_ADD_FUTURE"},
	{Env: "BINTRAIL_STATUS_LOG_INTERVAL"},
	{Env: "BINTRAIL_COLUMN_EQ"},

	// Archives, snapshots and S3.
	{Env: "BINTRAIL_ARCHIVE_DIR"},
	{Env: "BINTRAIL_ARCHIVE_S3"},
	{Env: "BINTRAIL_ARCHIVE_S3_REGION"},
	{Env: "BINTRAIL_BASELINE_DIR"},
	{Env: "BINTRAIL_BASELINE_S3"},
	{Env: "BINTRAIL_BASELINE_RETAIN"},
	{Env: "BINTRAIL_BASELINE_REFRESH_INTERVAL"},
	{Env: "BINTRAIL_BASELINE_TABLE_DELTAS"},
	{Env: "BINTRAIL_BASELINE_CARRY_FORWARD_UNCHANGED"},
	{Env: "BINTRAIL_KEY_INDEX"},
	{Env: "BINTRAIL_S3_BUCKET"},
	{Env: "BINTRAIL_S3_REGION"},
	{Env: "BINTRAIL_S3_ARN"},
	{Env: "BINTRAIL_S3_PREFIX"},
	{Env: "BINTRAIL_S3_ENDPOINT"},
	{Env: "BINTRAIL_S3_PATH_STYLE"},
	{Env: "BINTRAIL_ICEBERG_WAREHOUSE"},
	{Env: "BINTRAIL_ICEBERG_CATALOG_CREDENTIAL", Secret: true},
	{Env: "BINTRAIL_ICEBERG_CATALOG_TOKEN", Secret: true},

	// Queries, recovery and DuckDB.
	{Env: "BINTRAIL_ULTRAFAST"},
	{Env: "BINTRAIL_DUCKDB_THREADS"},
	{Env: "BINTRAIL_DUCKDB_MEMORY_LIMIT"},
	{Env: "BINTRAIL_DUCKDB_NO_AWS_EXT"},
	{Env: "BINTRAIL_RECOVER_MAX_BYTES"},
	{Env: "BINTRAIL_RECONSTRUCT_WARN_EVENTS"},
	{Env: "BINTRAIL_RECONSTRUCT_FETCH_BATCH"},
	{Env: "BINTRAIL_MCP_QUERY_MAX_LIMIT"},

	// The agent and its buffer.
	{Env: "BINTRAIL_API_KEY", Secret: true},
	{Env: "BINTRAIL_AGENT_ENDPOINT"},
	{Env: "BINTRAIL_AGENT_MAX_RECONNECT_ATTEMPTS"},
	{Env: "BINTRAIL_BUFFER_RETAIN"},
	{Env: "BINTRAIL_BUFFER_MAX_EVENTS"},
	{Env: "BINTRAIL_BUFFER_MAX_BYTES"},
	{Env: "BINTRAIL_FLUSH_INTERVAL"},

	// The time-travel shim.
	{Env: "BINTRAIL_AUTH_METHOD"},
	{Env: "BINTRAIL_SHIM_QUERY_TIMEOUT"},
	{Env: "BINTRAIL_SHIM_MAX_CONNECTIONS"},
	{Env: "BINTRAIL_SHIM_MAX_FULLTABLE_QUERIES"},

	// The console daemon (serve, watch).
	{Env: "BINTRAIL_CONSOLE_LISTEN"},
	{Env: "BINTRAIL_CONSOLE_TOKEN", Secret: true},
	{Env: "BINTRAIL_CONSOLE_URL"},
	{Env: "BINTRAIL_CONSOLE_SERVERS"},
	{Env: "BINTRAIL_CONSOLE_AUTH"},
	{Env: "BINTRAIL_CONSOLE_MCP_TOKEN_FILE"},
	{Env: "BINTRAIL_CONSOLE_TLS_CERT"},
	{Env: "BINTRAIL_CONSOLE_TLS_KEY"},
	{Env: "BINTRAIL_CONSOLE_ALLOWED_HOSTS"},
	{Env: "BINTRAIL_CONSOLE_ALLOW_SETUP"},
	{Env: "BINTRAIL_CONSOLE_STARTUP_PAGE"},
	{Env: "BINTRAIL_CONSOLE_BASELINE_DIR"},
	{Env: "BINTRAIL_CONSOLE_BASELINE_S3"},
	{Env: "BINTRAIL_CONSOLE_BASELINE_RETAIN"},
	{Env: "BINTRAIL_CONSOLE_BASELINE_STAGING"},
	{Env: "BINTRAIL_CONSOLE_BASELINE_TRIGGER"},
	{Env: "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE"},
	{Env: "BINTRAIL_CONSOLE_ARCHIVE_STAGING"},
	{Env: "BINTRAIL_CONSOLE_VERIFY_TRIGGER"},
	{Env: "BINTRAIL_CONSOLE_VERIFY_INTERVAL"},
	{Env: "BINTRAIL_CONSOLE_VERIFY_TABLES"},
	{Env: "BINTRAIL_CONSOLE_NOTIFY_WEBHOOK", Secret: true},
	{Env: "BINTRAIL_CONSOLE_SQL_MEMORY"},
	{Env: "BINTRAIL_CONSOLE_SQL_MAX_IN_FLIGHT"},
	{Env: "BINTRAIL_CONSOLE_ROUTER_URL"},
	{Env: "BINTRAIL_CONSOLE_FLASHBACK_LISTEN"},
	{Env: "BINTRAIL_MEMORY_SHARE"},

	// The router: the MySQL port.
	{Env: "BINTRAIL_ROUTER_LISTEN"},
	{Env: "BINTRAIL_ROUTER_METRICS_ADDR"},
	{Env: "BINTRAIL_ROUTER_CONCURRENCY"},
	{Env: "BINTRAIL_ROUTER_MAX_WAIT"},
	{Env: "BINTRAIL_ROUTER_STATEMENT_TABLE"},
	{Env: "BINTRAIL_ROUTER_TLS_CERT"},
	{Env: "BINTRAIL_ROUTER_TLS_KEY"},
	{Env: "BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS"},
	{Env: "BINTRAIL_CONSOLE_ROUTE_READ_ONLY"},
	{Env: "BINTRAIL_CONSOLE_ROUTE_COST_THRESHOLD"},
	{Env: "BINTRAIL_CONSOLE_ROUTE_SCAN_ROWS"},
	{Env: "BINTRAIL_SQL_TAILS"},
	{Env: "BINTRAIL_SQL_TAIL_MAX_KEYS"},
	{Env: "BINTRAIL_SQL_TAIL_MAX_BYTES"},

	// PostgreSQL sources (bintrail-pg).
	{Env: "BINTRAIL_PG_REPL_DSN", Secret: true},
	{Env: "BINTRAIL_PG_QUERY_DSN", Secret: true},
	{Env: "BINTRAIL_PG_SLOT"},
	{Env: "BINTRAIL_PG_PUBLICATION"},
	{Env: "BINTRAIL_PG_START_LSN"},
	{Env: "BINTRAIL_PG_FLASHBACK_LISTEN"},
	{Env: "BINTRAIL_PG_SHIM_CONFIG"},
	{Env: "BINTRAIL_PG_BASELINE_DIR"},
	{Env: "BINTRAIL_PG_BASELINE_S3"},

	// Usage telemetry.
	{Env: "BINTRAIL_TELEMETRY"},
	{Env: "BINTRAIL_TELEMETRY_DIR"},
	{Env: "BINTRAIL_TELEMETRY_DEBUG"},

	// Set by DBTrail for another of its processes, never by an operator.
	// The compose file passes these two: the file's own version, and where
	// its index volume is mounted read-only.
	{Env: "BINTRAIL_COMPOSE_VERSION", Kind: Internal},
	{Env: "BINTRAIL_INDEX_DATADIR_RO", Kind: Internal},
	// A process starting a SQL worker or a verify child marks it with these.
	{Env: "BINTRAIL_SQL_SANDBOX_WORKER", Kind: Internal},
	{Env: "BINTRAIL_VERIFY_CHILD_LOG_LEVEL", Kind: Internal},

	// Read only to say they no longer do anything.
	{Env: "BINTRAIL_CONSOLE_SQL_PANEL", Kind: Retired},
	{Env: "BINTRAIL_CONSOLE_ROUTE_MAX_COPY_AGE", Kind: Retired},

	// The test harness (internal/testutil, internal/oracle).
	{Env: "BINTRAIL_TEST_DSN", Kind: Test, Secret: true},
	{Env: "BINTRAIL_TEST_MARIADB_DSN", Kind: Test, Secret: true},
	{Env: "BINTRAIL_TEST_GTID_SOURCE_DSN", Kind: Test, Secret: true},
	{Env: "BINTRAIL_TEST_PG_DSN", Kind: Test, Secret: true},
	{Env: "BINTRAIL_REQUIRE_MYSQL", Kind: Test},
	{Env: "BINTRAIL_REQUIRE_MARIADB", Kind: Test},
	{Env: "BINTRAIL_REQUIRE_GTID_SOURCE", Kind: Test},
	{Env: "BINTRAIL_REQUIRE_POSTGRES", Kind: Test},
	{Env: "BINTRAIL_REQUIRE_PGEXT", Kind: Test},
	{Env: "BINTRAIL_ORACLE_SEED", Kind: Test},
	{Env: "BINTRAIL_ORACLE_STEPS", Kind: Test},
}
