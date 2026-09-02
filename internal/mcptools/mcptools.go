// Package mcptools implements the read-only Bintrail MCP tools — query,
// recover, status, list_schema_changes, and the opt-in reconstruct (#953) and
// recover_cascade (#1128) — decoupled from how the index connection is
// obtained.
//
// Two surfaces consume it:
//
//   - cmd/bintrail-mcp (standalone server): the index is resolved per tool
//     call from a DSN (tool-level index_dsn parameter, BINTRAIL_INDEX_DSN env
//     var, or a multi-tenant override), a fresh connection is opened and
//     closed around every call, and the idempotent schema migration runs on
//     it (the server owns no long-lived connection).
//   - internal/console (/mcp endpoint): the index is a long-lived connManager
//     bundle owned by the console, the DSN parameters are rejected (an
//     authenticated MCP client must not be able to point the console at an
//     arbitrary DSN), and the console's read boundary applies — result caps,
//     process-global RBAC posture, and query_text/query_hash withheld to
//     match the events API's eventDTO.
//
// The seam is ResolveTarget: a callback mapping the (possibly empty) tool
// index_dsn argument to a Target carrying the connection plus the posture the
// serving surface imposes on it.
package mcptools

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/ext/mcpext"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
)

// DefaultQueryLimit and DefaultRecoverLimit are the per-tool defaults applied
// when the caller passes no limit (or a non-positive one). They match the CLI
// defaults and the console API's default caps.
const (
	DefaultQueryLimit   = 100
	DefaultRecoverLimit = 1000
)

// Target is one resolved index a tool call runs against: the open connection
// plus the read posture the serving surface imposes on it.
type Target struct {
	// DB is the open index connection. Never nil on a successful resolve.
	DB *sql.DB
	// DBName is the index database name, required by the status tool (the
	// query planner and partition inspection scope to it). May be empty on
	// the standalone surface when the DSN carries no database name — the
	// status tool then refuses with an actionable error.
	DBName string
	// SourceDSN is the captured source's DSN when the serving surface knows
	// one. The built-in tools never use it — they read the index only — but
	// it is handed to extension tools (ext/mcpext), which may need the live
	// source. Empty means "no source available", not an error.
	SourceDSN string
	// CloseDB is true when the connection was opened for this call and the
	// handler must close it (standalone). False when the connection is owned
	// by a long-lived pool (console connManager bundles).
	CloseDB bool
	// EnsureSchema runs the idempotent indexer schema migration before the
	// query/recover engines touch binlog_events (they SELECT
	// post-initial-schema columns). True on the standalone surface, which
	// opens a fresh connection per call. False on the console: the boot entry
	// is migrated by the cmd layer at startup, and registry servers are
	// deliberately NEVER migrated (the console's read-only contract confines
	// DDL to the command-line DSN).
	EnsureSchema bool
	// EnvArchiveDiscovery consults BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID before
	// falling back to archive_state auto-discovery (standalone behavior).
	// False on the console, whose archive posture is per-server.
	EnvArchiveDiscovery bool
	// NoArchive disables Parquet archive auto-discovery outright — the
	// console bundle's per-server posture (its own flag OR an active RBAC
	// profile, which archives do not enforce).
	NoArchive bool
	// DenyTables / RedactColumns / ProfileActive are RBAC rules the surface
	// attaches to every query (the console's process-global profile). The
	// standalone surface leaves them empty and loads rules from the tool's
	// profile parameter instead.
	DenyTables    []query.SchemaTable
	RedactColumns []query.SchemaTableColumn
	ProfileActive bool
	// Resolver is a preloaded schema resolver for recovery WHERE clauses.
	// Consulted only when ResolverLoaded is true (the console preloads its
	// per-bundle resolver, which may legitimately be nil); otherwise the
	// recover tool loads the latest snapshot best-effort per call.
	Resolver       *metadata.Resolver
	ResolverLoaded bool
	// RedactStatementText blanks query_text/query_hash on every fetched row
	// before formatting, so the query tool's output carries the same field
	// content as the console events API (whose eventDTO omits statement text,
	// #699). Set by the console surface only.
	RedactStatementText bool
	// FindBaseline is the reconstruct tool's baseline lookup on surfaces that
	// own baseline routing (the console binds its bundle's findBaseline, which
	// carries the #766 local→S3 fallback). Left nil on the standalone surface,
	// which binds a source from the tool parameters or the environment instead
	// — see resolveBaselineLookup.
	FindBaseline FindBaselineFunc
	// BaselineConfigured gates the reconstruct tool for THIS target on surfaces
	// that own baseline routing: the console's per-server signal, which already
	// folds in "a baseline location is set" AND "archives are enabled" AND "no
	// RBAC profile is active" (see internal/console's newBundleDerived). Unused
	// on the standalone surface, whose gate is whether a baseline source
	// resolved at all.
	BaselineConfigured bool
}

// release closes the connection when this call owns it.
func (t *Target) release() {
	if t.CloseDB && t.DB != nil {
		_ = t.DB.Close()
	}
}

// stripStatementText enforces RedactStatementText on a fetched row set.
func (t *Target) stripStatementText(rows []query.ResultRow) {
	if !t.RedactStatementText {
		return
	}
	for i := range rows {
		rows[i].QueryText = nil
		rows[i].QueryHash = nil
	}
}

// archiveSources returns Parquet archive source paths for this target, plus
// whether DISCOVERY itself failed. On failure the fetch layer stays
// permissive (serve without archives rather than fail outright) but the
// signal must reach the client: the query tool renders an
// archive_discovery_failed warning and the recover tool refuses (#1285 —
// before this, a discovery failure was slog-only and a result missing every
// archived event read as complete). NoArchive is (nil, false): archives are
// OFF by request, nothing failed.
func (t *Target) archiveSources(ctx context.Context) ([]string, bool) {
	if t.NoArchive {
		return nil, false
	}
	if t.EnvArchiveDiscovery {
		return EnvArchiveSources(ctx, t.DB)
	}
	return stateArchiveSources(ctx, t.DB)
}

// ResolveTarget maps the tool-level index_dsn argument (empty on surfaces
// that reject it) to the Target the call runs against.
type ResolveTarget func(ctx context.Context, argDSN string) (*Target, error)

// Config assembles an MCP server over the read-only tools.
type Config struct {
	// Version is the reported mcp.Implementation version.
	Version string
	// Instructions is the server-level usage hint sent to clients.
	Instructions string
	// Resolve obtains the Target for each tool call.
	Resolve ResolveTarget
	// AllowDSNParam accepts the tool-level index_dsn parameter (standalone).
	// When false the parameter is rejected with an explicit error — the
	// serving surface owns connection routing (console).
	AllowDSNParam bool
	// AllowProfileParam accepts the tool-level profile parameter
	// (standalone). When false it is rejected — the surface's RBAC posture
	// is fixed at process start (console).
	AllowProfileParam bool
	// Reconstruct registers the reconstruct (time-travel) tool. Opt-in per
	// surface rather than unconditional in NewServer: a surface that cannot
	// supply a baseline lookup (neither Target.FindBaseline nor the
	// baseline_dir/baseline_s3 parameters) would otherwise advertise a tool
	// that can only ever error.
	Reconstruct bool
	// RecoverCascade registers the recover_cascade tool (#1128). Opt-in per
	// surface like Reconstruct, but the condition is NOT baseline presence:
	// cascade recovery degrades meaningfully without a baseline (Phase-1
	// window-only synthesis still recovers children with an indexed event in
	// the lookback window), so every surface that serves recover-cascade at
	// all registers it, and the Phase-2 baseline fallback engages per call
	// only when a baseline is available (Target.FindBaseline on routed
	// surfaces; the baseline parameters/environment on the standalone one).
	RecoverCascade bool
	// AllowBaselineParams accepts the reconstruct tool's baseline_dir /
	// baseline_s3 parameters, with BINTRAIL_BASELINE_DIR / BINTRAIL_BASELINE_S3
	// as the fallback (standalone). When false they are rejected exactly like
	// index_dsn is, and the lookup comes from Target.FindBaseline instead — the
	// serving surface owns baseline routing (console).
	AllowBaselineParams bool
	// QueryMaxLimit returns the hard row ceiling for the query tool. nil
	// means EnvQueryMaxLimit (the standalone default, #654).
	QueryMaxLimit func() int
	// RecoverMaxLimit caps the recover tool's limit; 0 means uncapped
	// (standalone behavior — the CLI is the bounded-review path there).
	RecoverMaxLimit int
	// MaxScriptBytes overrides the reversal-script payload budget
	// (recovery.Generator.SetMaxScriptBytes) the recover tool enforces before
	// rendering. 0 means "leave it alone" — the Generator already defaults to
	// recovery.DefaultMaxScriptBytes (2 GiB, #654) on its own, so standalone
	// bintrail-mcp's behavior is unchanged whether or not this field is set.
	// The console sets it to its own tighter, shared-daemon budget (#849,
	// internal/console's recoverMaxScriptBytes) — the console's /mcp endpoint
	// is the same 2 GiB-by-default gap #849 closed for /api/recover, reached
	// by the SAME row-count cap (RecoverMaxLimit) without a byte cap.
	MaxScriptBytes int64
	// AuditSurface tags ext.Record audit events; "" means "mcp".
	AuditSurface string
}

func (c Config) queryMaxLimit() int {
	if c.QueryMaxLimit != nil {
		return c.QueryMaxLimit()
	}
	return EnvQueryMaxLimit()
}

func (c Config) auditSurface() string {
	if c.AuditSurface == "" {
		return "mcp"
	}
	return c.AuditSurface
}

// scriptBudgetOverride reports whether the recover tool should call
// Generator.SetMaxScriptBytes, and with what value. ok is false when
// c.MaxScriptBytes is unset (<= 0) — the caller must then leave the
// Generator's own constructor default (recovery.DefaultMaxScriptBytes, 2 GiB)
// untouched, which is what standalone bintrail-mcp relies on.
//
// This is deliberately its own method, not an inline `if cfg.MaxScriptBytes >
// 0 { gen.SetMaxScriptBytes(...) }` at the call site (#849 code review): "0 =
// don't touch it" and "0 = call SetMaxScriptBytes(0)" are NOT the same thing
// — SetMaxScriptBytes(0) explicitly DISABLES the budget guard
// (recovery.Generator.SetMaxScriptBytes: "n <= 0 disables the guard
// (unlimited)"). A future refactor that simplified the call site to
// `gen.SetMaxScriptBytes(cfg.MaxScriptBytes)` unconditionally would silently
// make the standalone posture (MaxScriptBytes never set) render with NO
// budget at all, re-opening the exact class of bug #654/#849 close. Isolating
// the decision here lets TestConfigScriptBudgetOverride pin it directly
// (Config{}.scriptBudgetOverride() must be (0, false)) without needing an
// impractical multi-gigabyte test payload to observe the difference
// end-to-end.
func (c Config) scriptBudgetOverride() (int64, bool) {
	if c.MaxScriptBytes > 0 {
		return c.MaxScriptBytes, true
	}
	return 0, false
}

// NewServer builds an MCP server exposing the read-only tools bound to cfg:
// query, recover, status and list_schema_changes always, plus reconstruct when
// cfg.Reconstruct is set and recover_cascade when cfg.RecoverCascade is set.
// All tools are annotated read-only + idempotent.
func NewServer(cfg Config) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "bintrail",
		Version: cfg.Version,
	}, &mcp.ServerOptions{
		Instructions: cfg.Instructions,
	})

	mcp.AddTool(server, &mcp.Tool{
		Name: "query",
		Description: "Search indexed MySQL binlog events with filters. " +
			"Returns matching events showing row changes (before/after images), timestamps, and metadata. " +
			"Use json format for full row data or table format for a human-readable summary.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Search binlog events",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, MakeQueryTool(cfg))

	mcp.AddTool(server, &mcp.Tool{
		Name: "recover",
		Description: "Generate reversal SQL to undo matching binlog events (dry-run only). " +
			"Produces a BEGIN/COMMIT-wrapped SQL script that reverses events in reverse chronological order (most recent first): " +
			"DELETE->INSERT, UPDATE->reverse UPDATE, INSERT->DELETE. " +
			"Review carefully before applying to production. " +
			"The script comes back as plain SQL while it is small. Past the response size limit it is NOT returned: " +
			"the result is a JSON summary naming the statement count and how to fetch the script in statement-aligned " +
			"chunks with sql_offset/sql_limit, which concatenate in order into the exact script. Use summary_only to ask " +
			"how large a reversal is before pulling it.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Generate recovery SQL",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, MakeRecoverTool(cfg))

	mcp.AddTool(server, &mcp.Tool{
		Name: "status",
		Description: "Show the current state of the binlog index: " +
			"which files have been indexed, partition layout with estimated row counts, " +
			"and aggregate summary of indexed events.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "Index status",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, MakeStatusTool(cfg))

	mcp.AddTool(server, &mcp.Tool{
		Name: "list_schema_changes",
		Description: "List DDL schema changes (CREATE, ALTER, DROP, RENAME, TRUNCATE) " +
			"recorded during binlog indexing or streaming. " +
			"Returns the full DDL statement, binlog coordinates, timestamp, and the " +
			"covering snapshot_id (null = no schema snapshot was taken after the DDL) " +
			"for each change; set uncovered_only to list just the changes without a snapshot." +
			" Results come back newest first; changes inside the same second are ordered by binlog file, then position. " +
			"In an index fed by more than one source, or by a Postgres source (whose LSN file names are not zero-padded), " +
			"same-second changes have a repeatable order, not their true one.",
		Annotations: &mcp.ToolAnnotations{
			Title:          "List schema changes",
			ReadOnlyHint:   true,
			IdempotentHint: true,
		},
	}, MakeSchemaChangesTool(cfg))

	// Opt-in (#953): only surfaces that can supply a baseline lookup advertise
	// it — see Config.Reconstruct.
	if cfg.Reconstruct {
		mcp.AddTool(server, &mcp.Tool{
			Name: "reconstruct",
			Description: "Reconstruct a single row's full state at a point in time (time travel). " +
				"Folds a baseline snapshot with the indexed events after it, so columns never touched " +
				"in the retained window resolve correctly, unlike `recover`, which only reverses events it has. " +
				"Use history=true for every state transition up to that time. " +
				"Requires a baseline snapshot produced by `bintrail baseline`.",
			Annotations: &mcp.ToolAnnotations{
				Title:          "Reconstruct row state at a point in time",
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
		}, MakeReconstructTool(cfg))
	}

	// Opt-in (#1128): registered by every surface that serves recover-cascade.
	// Unlike reconstruct this does NOT depend on a baseline — Phase-1
	// window-only synthesis works without one — see Config.RecoverCascade.
	if cfg.RecoverCascade {
		mcp.AddTool(server, &mcp.Tool{
			Name: "recover_cascade",
			Description: "Generate reversal SQL for rows hit by a foreign-key ON DELETE / ON UPDATE " +
				"CASCADE or SET NULL (dry-run only). InnoDB runs FK cascades below the binlog, so the plain " +
				"`recover` tool reverses only the parent change and silently misses the cascade-deleted child " +
				"rows and rewritten/nulled child FKs; this tool synthesizes those from the index and reverses " +
				"them too. If the synthesis is provably partial the call fails with the reasons unless " +
				"allow_incomplete is set; always check the `incomplete` list in the result. " +
				"Review carefully before applying to production. " +
				"Past the response size limit the `sql` field is omitted and the result says how to fetch the script " +
				"in statement-aligned chunks with sql_offset/sql_limit, which concatenate in order into the exact " +
				"script; summary_only asks for the counts and coverage alone.",
			Annotations: &mcp.ToolAnnotations{
				Title:          "Generate FK-cascade recovery SQL",
				ReadOnlyHint:   true,
				IdempotentHint: true,
			},
		}, MakeRecoverCascadeTool(cfg))
	}

	// Extension tools (mcpext.Register) are added AFTER the built-ins, so a
	// provider that reuses a core tool name collides visibly instead of
	// silently replacing it. They resolve their target through the same
	// Config.Resolve this surface uses, which is what keeps an extension tool
	// reading the index the operator selected rather than one of its own
	// choosing. No-op in the stock binaries.
	mcpext.RunProviders(server, extToolContext(cfg))

	return server
}

// extToolContext adapts this surface's ResolveTarget into the seam's resolver.
// Two things are deliberately preserved across the adaptation: the schema
// migration the surface would run for its own tools (so an extension tool sees
// the same columns), and connection OWNERSHIP — Close closes a per-call
// standalone connection and does nothing for a console-pooled one, so a
// provider has one correct thing to call either way.
func extToolContext(cfg Config) mcpext.ToolContextFunc {
	return func(ctx context.Context, argDSN string) (mcpext.ToolContext, error) {
		if !cfg.AllowDSNParam {
			// The console rejects DSN parameters on its own tools for the same
			// reason: an authenticated MCP client must not be able to point the
			// daemon at an arbitrary database. Extension tools inherit that.
			argDSN = ""
		}
		t, err := cfg.Resolve(ctx, argDSN)
		if err != nil {
			return mcpext.ToolContext{}, err
		}
		closeFn := func() {}
		if t.CloseDB {
			closeFn = func() { t.DB.Close() }
		}
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				closeFn()
				return mcpext.ToolContext{}, fmt.Errorf("schema migration: %w", err)
			}
		}
		return mcpext.ToolContext{
			DB:        t.DB,
			DBName:    t.DBName,
			SourceDSN: t.SourceDSN,
			Close:     closeFn,
		}, nil
	}
}

// DSNTarget adapts a DSN resolution function (override > tool arg > env var,
// on the standalone surface) into a ResolveTarget that opens a fresh
// connection per call with the standalone posture: caller-closed, schema
// migration on, env-var archive discovery.
func DSNTarget(resolve func(argDSN string) (string, error)) ResolveTarget {
	return func(ctx context.Context, argDSN string) (*Target, error) {
		dsn, err := resolve(argDSN)
		if err != nil {
			return nil, err
		}
		cfg, err := mysqldriver.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid DSN: %w", err)
		}
		db, err := config.Connect(dsn)
		if err != nil {
			return nil, err
		}
		return &Target{
			DB:     db,
			DBName: cfg.DBName,
			// The standalone server has no per-request server selection, so
			// its source — when the operator configured one — is the process
			// environment's. Only extension tools read it; the built-in tools
			// never touch the source (reconstruct included — it folds a
			// baseline snapshot with indexed events, never the live server).
			SourceDSN:           os.Getenv("BINTRAIL_SOURCE_DSN"),
			CloseDB:             true,
			EnsureSchema:        true,
			EnvArchiveDiscovery: true,
		}, nil
	}
}

// ─── Tool argument types ─────────────────────────────────────────────────────

// QueryArgs are the query tool's parameters.
type QueryArgs struct {
	IndexDSN      string   `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema        string   `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table         string   `json:"table,omitempty" jsonschema:"Filter by table name"`
	PK            string   `json:"pk,omitempty" jsonschema:"Filter by primary key value (pipe-delimited for composite keys e.g. 123 or 123|2)"`
	PKs           []string `json:"pks,omitempty" jsonschema:"Filter by multiple primary key values (each pipe-delimited for composite keys); requires schema and table; mutually exclusive with pk"`
	PKMin         string   `json:"pk_min,omitempty" jsonschema:"Inclusive lower bound on the primary key, as an integer string. Only for tables whose primary key is one integer column (signed or unsigned); a composite or non-integer key is refused with the table's actual key shape. Requires schema and table; cannot be combined with pk or pks. A range cannot use the key index, so it scans the partitions the time filters keep: pair it with since and until."`
	PKMax         string   `json:"pk_max,omitempty" jsonschema:"Inclusive upper bound on the primary key, as an integer string; same rules as pk_min, and either bound works alone."`
	LimitPerPK    int      `json:"limit_per_pk,omitempty" jsonschema:"Cap returned events per pk_values to the latest N (0 = unlimited); requires pk or pks"`
	EventType     string   `json:"event_type,omitempty" jsonschema:"Filter by event type: INSERT UPDATE or DELETE"`
	GTID          string   `json:"gtid,omitempty" jsonschema:"Filter by GTID (e.g. uuid:42)"`
	Since         string   `json:"since,omitempty" jsonschema:"Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until         string   `json:"until,omitempty" jsonschema:"Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ChangedColumn string   `json:"changed_column,omitempty" jsonschema:"Filter UPDATE events that modified this column"`
	QueryHash     string   `json:"query_hash,omitempty" jsonschema:"Filter to the events produced by one statement digest: the 64-character query_hash of an event you already have. It identifies a statement SHAPE so every execution of that statement in the window matches. Unavailable on surfaces that withhold statement text."`
	ColumnEq      []string `json:"column_eq,omitempty" jsonschema:"Filter events where a column in row_after or row_before equals the given value. Each entry is column=value. Repeat for AND. Literal NULL matches JSON null."`
	Flag          string   `json:"flag,omitempty" jsonschema:"Filter events from tables or columns carrying this flag"`
	Format        string   `json:"format,omitempty" jsonschema:"Output format: json table or csv (default: json)"`
	Limit         int      `json:"limit,omitempty" jsonschema:"Maximum number of events to return (default: 100)"`
	Order         string   `json:"order,omitempty" jsonschema:"Sort direction: ASC (default) or DESC. Under ASC results return oldest-first and a truncating limit keeps the OLDEST matching events; under DESC newest-first and limit keeps the NEWEST. For 'the last N events' use DESC with limit N instead of guessing a since window."`
	Profile       string   `json:"profile,omitempty" jsonschema:"Apply RBAC access rules for this profile (table-level deny and column-level redaction)"`
	NoArchive     bool     `json:"no_archive,omitempty" jsonschema:"Disable auto-routing to Parquet archives (MySQL-only results)"`
}

// RecoverArgs are the recover tool's parameters.
//
// Deliberately NOT here, matching the CLI recover surface (see
// TestRecoverToolParams_matchCLIRecoverFlags):
//
//   - changed_column: a changed-column filter names row VERSIONS, and
//     reversing a filtered subset of a row's history can produce a state that
//     never existed — which is why CLI recover has never accepted
//     --changed-column (it is query-only). The MCP recover tool exposed it
//     anyway until #962. It is REMOVED rather than kept-and-rejected: the
//     inferred input schema sets additionalProperties:false, so a client that
//     still sends changed_column gets a loud schema-validation error naming
//     the property instead of a silently ignored filter (the dangerous
//     outcome would be a reversal covering more events than the client
//     believes it scoped).
//   - query_hash: same shape-vs-instance reasoning — see
//     TestRecoverArgs_hasNoQueryHashParam.
type RecoverArgs struct {
	IndexDSN   string   `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema     string   `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table      string   `json:"table,omitempty" jsonschema:"Filter by table name"`
	PK         string   `json:"pk,omitempty" jsonschema:"Filter by primary key value (pipe-delimited for composite keys)"`
	PKs        []string `json:"pks,omitempty" jsonschema:"Filter by multiple primary key values (each pipe-delimited for composite keys); requires schema and table; mutually exclusive with pk"`
	PKMin      string   `json:"pk_min,omitempty" jsonschema:"Inclusive lower bound on the primary key, as an integer string. Only for tables whose primary key is one integer column (signed or unsigned); a composite or non-integer key is refused with the table's actual key shape. Requires schema and table; cannot be combined with pk or pks. A range cannot use the key index, so it scans the partitions the time filters keep: pair it with since and until."`
	PKMax      string   `json:"pk_max,omitempty" jsonschema:"Inclusive upper bound on the primary key, as an integer string; same rules as pk_min, and either bound works alone."`
	LimitPerPK int      `json:"limit_per_pk,omitempty" jsonschema:"Cap reversed events per pk_values to the latest N (0 = unlimited); requires pk or pks"`
	EventType  string   `json:"event_type,omitempty" jsonschema:"Filter by event type: INSERT UPDATE or DELETE"`
	GTID       string   `json:"gtid,omitempty" jsonschema:"Filter by GTID (e.g. uuid:42)"`
	Since      string   `json:"since,omitempty" jsonschema:"Filter events at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until      string   `json:"until,omitempty" jsonschema:"Filter events at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	ColumnEq   []string `json:"column_eq,omitempty" jsonschema:"Filter events where a column in row_after or row_before equals the given value. Each entry is column=value. Repeat for AND. Literal NULL matches JSON null."`
	Flag       string   `json:"flag,omitempty" jsonschema:"Filter events from tables or columns carrying this flag"`
	Limit      int      `json:"limit,omitempty" jsonschema:"Maximum number of events to reverse (default: 1000)"`
	Profile    string   `json:"profile,omitempty" jsonschema:"Apply RBAC access rules for this profile (table-level deny and column-level redaction)"`
	NoArchive  bool     `json:"no_archive,omitempty" jsonschema:"Disable auto-routing to Parquet archives (MySQL-only results)"`
	// The transport parameters (#1438), identical to recover_cascade's. They
	// change what this response CARRIES, never what is generated. Setting any
	// of them switches the result from plain SQL text to a JSON envelope whose
	// `sql` field holds the exact bytes — the same switch the size cap makes —
	// so a client can tell a chunk from a whole script programmatically, which
	// a SQL comment cannot do.
	SummaryOnly bool `json:"summary_only,omitempty" jsonschema:"Return the statement count and script size without the SQL script. Answers how large the reversal is before deciding to pull it."`
	SQLOffset   int  `json:"sql_offset,omitempty" jsonschema:"0-based index of the first STATEMENT to return, for fetching a large script in pieces. Chunks cut on statement boundaries and concatenate in order into the exact script."`
	SQLLimit    int  `json:"sql_limit,omitempty" jsonschema:"How many statements to return from sql_offset. Reduced (never silently) when the chunk would exceed the response size limit; the result says so and gives the next offset."`
}

// recoverResult is the recover tool's JSON envelope, used ONLY when the script
// is not carried whole: a summary, or one chunk of a paginated fetch (#1438).
// The plain-SQL-text result is unchanged for every call that fits, so the
// common path is byte-for-byte what it always was.
//
// It carries no counts beyond the script's own shape — the tool's product is
// the script, and the cascade tool is the one with a coverage report.
type recoverResult struct {
	SQL            string `json:"sql,omitempty"`
	StatementCount int    `json:"statement_count"`
	ScriptID       string `json:"script_id,omitempty"`
	ScriptBytes    int    `json:"script_bytes,omitempty"`
	SQLFrom        int    `json:"sql_from,omitempty"`
	SQLTo          int    `json:"sql_to,omitempty"`
	SQLMore        bool   `json:"sql_more,omitempty"`
	NextSQLOffset  int    `json:"next_sql_offset,omitempty"`
	SQLNote        string `json:"sql_note,omitempty"`
}

// StatusArgs are the status tool's parameters.
type StatusArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
}

// SchemaChangesArgs are the list_schema_changes tool's parameters.
type SchemaChangesArgs struct {
	IndexDSN string `json:"index_dsn,omitempty" jsonschema:"MySQL DSN for the index database. Overrides BINTRAIL_INDEX_DSN env var. Rejected on servers that route connections themselves (the console /mcp endpoint)."`
	Schema   string `json:"schema,omitempty" jsonschema:"Filter by database schema name"`
	Table    string `json:"table,omitempty" jsonschema:"Filter by table name"`
	DDLType  string `json:"ddl_type,omitempty" jsonschema:"Filter by DDL type: CREATE ALTER DROP RENAME or TRUNCATE"`
	Since    string `json:"since,omitempty" jsonschema:"Filter changes at or after this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Until    string `json:"until,omitempty" jsonschema:"Filter changes at or before this time (YYYY-MM-DD HH:MM:SS or RFC 3339)"`
	Limit    int    `json:"limit,omitempty" jsonschema:"Maximum number of changes to return (default: 100)"`
	// UncoveredOnly narrows results to DDLs with no covering snapshot — the
	// rows the status tool's "N DDL(s) detected without auto-snapshot" warning
	// counts, so an agent can go straight from that warning to the exact rows.
	UncoveredOnly bool `json:"uncovered_only,omitempty" jsonschema:"Return only changes with no covering schema snapshot; exactly the ones counted by the status tool's uncovered-DDL warning. TRUNCATE TABLE rows are excluded like the warning excludes them: TRUNCATE changes no table structure, so no snapshot is needed and their null snapshot_id is by design. To list TRUNCATEs, drop this flag or filter ddl_type TRUNCATE"`
}

// rejectSurfaceParams enforces the surface's parameter policy: a non-nil
// result is the tool error to return. The messages are deliberately explicit
// — an agent must learn the surface's routing model, not retry blindly.
func rejectSurfaceParams(cfg Config, indexDSN, profile string) *mcp.CallToolResult {
	if !cfg.AllowDSNParam && indexDSN != "" {
		return ErrorResult(errors.New(
			"index_dsn is not accepted here: this server routes connections itself " +
				"(select a server via the /mcp/{id-or-name} URL path; connections are managed in the console)"))
	}
	if !cfg.AllowProfileParam && profile != "" {
		return ErrorResult(errors.New(
			"profile is not accepted here: the RBAC posture is fixed by the serving process configuration"))
	}
	return nil
}

// ─── Tool handler factories ──────────────────────────────────────────────────
//
// Each factory returns a closure over the Config: the target resolution seam
// plus the surface's caps and parameter policy.

// MakeQueryTool returns the query tool handler.
func MakeQueryTool(cfg Config) func(context.Context, *mcp.CallToolRequest, QueryArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args QueryArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, args.Profile); res != nil {
			return res, nil, nil
		}
		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		// Same idempotent migration as the recover tool below: the query
		// engine SELECTs post-initial-schema binlog_events columns
		// (query_text/query_hash, #699). Standalone-only — the console never
		// migrates its servers (see Target.EnsureSchema).
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				return ErrorResult(indexer.WrapSchemaMigrationErr(err)), nil, nil
			}
		}

		opts, err := BuildQueryOptions(FilterParams{
			Schema:        args.Schema,
			Table:         args.Table,
			PK:            args.PK,
			PKs:           args.PKs,
			PKMin:         args.PKMin,
			PKMax:         args.PKMax,
			LimitPerPK:    args.LimitPerPK,
			EventType:     args.EventType,
			GTID:          args.GTID,
			Since:         args.Since,
			Until:         args.Until,
			ChangedColumn: args.ChangedColumn,
			ColumnEq:      args.ColumnEq,
			Flag:          args.Flag,
			Limit:         args.Limit,
		}, DefaultQueryLimit)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		// Set after BuildQueryOptions rather than through it: the recover tool
		// shares that builder and must NOT gain this filter (a digest names a
		// statement shape, so a reversal scoped to one would undo executions
		// nobody asked about — see query.Options.QueryHash).
		queryHash, err := query.NormalizeQueryHash(args.QueryHash)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		if queryHash != "" && t.RedactStatementText {
			// A surface that strips the digest from every event it returns
			// cannot also let a client test candidate digests against it: that
			// hands back the withheld association one guess at a time.
			return ErrorResult(errors.New("query_hash filtering is unavailable on this surface: the statement digest is withheld from every returned event, so filtering on it would confirm what is withheld")), nil, nil
		}
		opts.QueryHash = queryHash

		// Order is set after BuildQueryOptions for the same reason as
		// query_hash: the recover tool shares that builder and pins its own
		// Order=DESC (#785/#927 — a truncated reversal must keep the newest
		// events), so the parameter must not exist there. Validated at the
		// boundary because OrderDirection treats garbage as ASC: a client
		// typo like "descending" would silently answer with the oldest rows,
		// the exact wrong end for the question DESC exists to ask (#1439).
		if args.Order != "" && !strings.EqualFold(args.Order, "ASC") && !strings.EqualFold(args.Order, "DESC") {
			return ErrorResult(fmt.Errorf("invalid order %q; must be ASC or DESC", args.Order)), nil, nil
		}
		opts.Order = args.Order

		// Hard ceiling on an explicit, oversized limit (#654). BuildQueryOptions
		// already coerces limit<=0 to the default, so this only bounds a large
		// EXPLICIT value an agent might pass. It is applied here, per-tool, on the
		// local opts — NOT in the shared BuildQueryOptions, because the recover
		// tool must refuse on size, not silently cap a read.
		ceiling := cfg.queryMaxLimit()
		requestedLimit := opts.Limit
		ceilingApplied := false
		if c, did := ApplyQueryCeiling(opts.Limit, ceiling); did {
			opts.Limit = c
			ceilingApplied = true
		}

		// The surface's RBAC posture always applies (the console's
		// process-global profile); the per-call profile parameter — standalone
		// only — layers the named profile's rules on top.
		opts.DenyTables = t.DenyTables
		opts.RedactColumns = t.RedactColumns
		opts.ProfileActive = t.ProfileActive
		if args.Profile != "" {
			denyTables, redactCols, err := query.LoadProfileRules(ctx, t.DB, args.Profile)
			if err != nil {
				return ErrorResult(fmt.Errorf("load profile rules: %w", err)), nil, nil
			}
			opts.DenyTables = denyTables
			opts.RedactColumns = redactCols
			opts.ProfileActive = true
		}

		// pk_min/pk_max: check the table's key shape and pick the cast BEFORE
		// any fetch (#1440), and after the posture above so a denied table
		// is refused without its key being described.
		if err := t.resolvePKRange(&opts); err != nil {
			return ErrorResult(err), nil, nil
		}

		format := args.Format
		if format == "" {
			format = "json"
		}
		if !cliutil.IsValidFormat(format) {
			return ErrorResult(fmt.Errorf("invalid format %q; must be json, table, or csv", format)), nil, nil
		}

		engine := query.New(t.DB)

		// Skip archive auto-discovery when no_archive is set or when an RBAC
		// profile is active (archive queries do not enforce rules). The
		// target's own posture (console per-server no-archive, which already
		// folds the process profile in) is enforced inside archiveSources.
		var archSources []string
		var discoveryFailed bool
		if !args.NoArchive && args.Profile == "" {
			archSources, discoveryFailed = t.archiveSources(ctx)
		}

		var buf bytes.Buffer
		var n int

		// Archive sources whose fetch failed and was skipped (#1285) — surfaced
		// as trailing warning lines so a result missing every event held by a
		// source does not read as complete to an MCP client.
		var skippedArchives []string
		var misfiledScanFailed bool
		// Duplicate event_ids whose live and archive copies disagreed in the
		// merge (#1325) — same blindness: the merge layer slog.Warns, but an
		// MCP client sees only the result text.
		var divergedEvents int
		if len(archSources) == 0 && !t.RedactStatementText {
			// Fast path: no archives and no post-fetch redaction — fetch and
			// format in one step.
			n, err = engine.Run(ctx, opts, format, &buf)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
		} else {
			// Fetch from live index (+ archives when present), merge, redact,
			// then format.
			fetchOpts := opts
			if len(archSources) > 0 {
				fetchOpts.ExtraArchiveHours, misfiledScanFailed = misfiledArchiveHours(ctx, t.DB, opts.Since, opts.Until)
			}
			results, err := engine.Fetch(ctx, fetchOpts)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
			for _, src := range archSources {
				ar, err := parquetquery.Fetch(ctx, fetchOpts, src)
				if err != nil {
					slog.Warn("archive query failed, skipping", "source", src, "error", err)
					skippedArchives = append(skippedArchives, src)
					continue
				}
				results = append(results, ar...)
			}
			if len(archSources) > 0 {
				// MergeAndTrimReport, not MergeResultsReport: each source
				// applied its own per-PK cap independently (SQL window /
				// DuckDB QUALIFY), so the union can still exceed limit_per_pk
				// per PK — the same post-merge re-trim the CLI query path
				// does, carrying the #1325 divergence count through.
				results, divergedEvents = query.MergeAndTrimReport(results, opts.Limit, opts.LimitPerPK, opts.Order)
			}
			t.stripStatementText(results)
			n, err = query.Format(results, format, &buf)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
		}

		text := buf.String()
		if n > 0 && format != "json" {
			text += fmt.Sprintf("\n%d row(s)\n", n)
		}
		text += QueryResultNotice(ceilingApplied, requestedLimit, ceiling, n, opts.Limit, opts.LimitPerPK, opts.Order)
		text += ArchiveSkipNotice(discoveryFailed, skippedArchives)
		text += EventDivergenceNotice(divergedEvents)
		if misfiledScanFailed {
			text += "\nWarning: " + archiveScanIncompleteWarning() + "\n"
		}

		ext.Record(ctx, ext.AuditEvent{
			Surface: cfg.auditSurface(),
			Action:  "query.run",
			Actor:   ext.ProcessActor(args.Profile),
			Schema:  args.Schema,
			Table:   args.Table,
			Detail:  map[string]string{"results": strconv.Itoa(n), "format": format},
		})

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	}
}

// MakeRecoverTool returns the recover tool handler.
func MakeRecoverTool(cfg Config) func(context.Context, *mcp.CallToolRequest, RecoverArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args RecoverArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, args.Profile); res != nil {
			return res, nil, nil
		}
		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		// Run the idempotent schema migration before NewResolver. Since
		// #212 NewResolver reads schema_snapshots.column_type and fails on
		// pre-migration databases with Error 1054. Standalone-only — see
		// Target.EnsureSchema.
		if t.EnsureSchema {
			if err := indexer.EnsureSchema(t.DB); err != nil {
				return ErrorResult(indexer.WrapSchemaMigrationErr(err)), nil, nil
			}
		}

		// No ChangedColumn here on purpose — see the RecoverArgs doc comment:
		// the field does not exist on the recover surface, matching CLI recover.
		opts, err := BuildQueryOptions(FilterParams{
			Schema:     args.Schema,
			Table:      args.Table,
			PK:         args.PK,
			PKs:        args.PKs,
			PKMin:      args.PKMin,
			PKMax:      args.PKMax,
			LimitPerPK: args.LimitPerPK,
			EventType:  args.EventType,
			GTID:       args.GTID,
			Since:      args.Since,
			Until:      args.Until,
			ColumnEq:   args.ColumnEq,
			Flag:       args.Flag,
			Limit:      args.Limit,
		}, DefaultRecoverLimit)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		// The surface's hard cap on the reversal window (console: same cap as
		// the /api/recover endpoint). The truncation warning below fires when
		// the capped limit is reached, so a clipped window is never silent.
		if cfg.RecoverMaxLimit > 0 && opts.Limit > cfg.RecoverMaxLimit {
			opts.Limit = cfg.RecoverMaxLimit
		}
		// When --limit truncates the matched window it must keep the most
		// RECENT events (#785/#927): the ASC default would keep the OLDEST
		// prefix, undoing old events underneath later un-reverted ones — a
		// state that never historically existed. Rows are re-sorted ascending
		// after the fetch, before generation (see below).
		opts.Order = "DESC"

		opts.DenyTables = t.DenyTables
		opts.RedactColumns = t.RedactColumns
		opts.ProfileActive = t.ProfileActive
		if args.Profile != "" {
			denyTables, redactCols, err := query.LoadProfileRules(ctx, t.DB, args.Profile)
			if err != nil {
				return ErrorResult(fmt.Errorf("load profile rules: %w", err)), nil, nil
			}
			opts.DenyTables = denyTables
			opts.RedactColumns = redactCols
			opts.ProfileActive = true
		}

		// Same shape check as the query tool (#1440), before any fetch and
		// after the posture: a reversal over a lexicographic "range" would
		// undo rows nobody named.
		if err := t.resolvePKRange(&opts); err != nil {
			return ErrorResult(err), nil, nil
		}

		// Schema resolver for PK-only WHERE clauses: preloaded by the surface
		// (console bundles), else loaded best-effort per call.
		resolver := t.Resolver
		var resolverErr error
		if !t.ResolverLoaded {
			resolver, resolverErr = metadata.NewResolver(t.DB, 0)
			if resolverErr != nil {
				resolver = nil
			}
		}

		// Fetch events from live index + archives. Archive skip conditions
		// mirror the query tool above.
		engine := query.New(t.DB)
		var archSources []string
		var discoveryFailed bool
		if !args.NoArchive && args.Profile == "" {
			archSources, discoveryFailed = t.archiveSources(ctx)
		}
		if discoveryFailed {
			// Unlike the query tool — a browsing surface that degrades with a
			// warning — recover REFUSES (#1285 review): a reversal script
			// generated without knowing the archives could silently omit
			// events, and undoing a partial subset can produce a state that
			// never existed. This matches CLI recover (a failing source is
			// fatal under its default) and the reconstruct tool's strict
			// default; no_archive is the client-reachable escape hatch on
			// THIS surface (a tool parameter, not a CLI flag).
			return ErrorResult(fmt.Errorf(
				"archive source discovery failed; a reversal script generated without the archives could silently omit events. " +
					"Retry with no_archive: true to recover from the live index only (events already rotated to archives will NOT be reversed), " +
					"or fix archive discovery and retry")), nil, nil
		}

		// Failed-and-skipped archive sources collect here and REFUSE after the
		// fetch loop — see the discovery refusal above for why recover never
		// warns-and-continues (#1285).
		var skippedArchives []string
		var rows []query.ResultRow
		// Diverging duplicates in the merge (#1325) stay a WARNING here, not a
		// refusal like the archive-trouble gates above: both copies of the
		// event exist and one — the live index's, the same one recover would
		// use with no archives at all — is used, so the script is not
		// knowingly incomplete. But the disagreement itself is a data-integrity
		// finding the reviewing operator must see: the kept copy's row images
		// become the reversal SQL.
		var divergedEvents int
		if len(archSources) > 0 {
			fetchOpts := opts
			extraHours, misfiledScanFailed := misfiledArchiveHours(ctx, t.DB, opts.Since, opts.Until)
			if misfiledScanFailed {
				// Third gate, same posture as the two around it: the registry
				// scan that catches misfiled (backfilled) archives failed, so
				// the per-source fetch may silently prune files whose events
				// are in the window — a knowingly-possibly-incomplete reversal
				// (the #1037 class).
				return ErrorResult(fmt.Errorf(
					"the archive registry could not be checked for misfiled (backfilled) archives; a reversal script generated without them could silently omit events. " +
						"Retry with no_archive: true to recover from the live index only (archived events will NOT be reversed), or repair the index and retry")), nil, nil
			}
			fetchOpts.ExtraArchiveHours = extraHours
			rows, err = engine.Fetch(ctx, fetchOpts)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
			for _, src := range archSources {
				ar, err := parquetquery.Fetch(ctx, fetchOpts, src)
				if err != nil {
					slog.Warn("archive query failed, skipping", "source", src, "error", err)
					skippedArchives = append(skippedArchives, src)
					continue
				}
				rows = append(rows, ar...)
			}
			// MergeAndTrimReport, not MergeResultsReport: each source applied
			// its own per-PK cap independently, so the union can still exceed
			// limit_per_pk per PK — mirror the CLI recover merge
			// (FetchMerged), carrying the #1325 divergence count through.
			rows, divergedEvents = query.MergeAndTrimReport(rows, opts.Limit, opts.LimitPerPK, opts.Order)
		} else {
			rows, err = engine.Fetch(ctx, opts)
			if err != nil {
				return ErrorResult(err), nil, nil
			}
		}

		if len(skippedArchives) > 0 {
			return ErrorResult(fmt.Errorf(
				"archive source failed and its events cannot be fetched: %s. "+
					"Refusing to generate a knowingly incomplete reversal script: undoing a partial subset of the matched events can produce a state that never existed. "+
					"Retry with no_archive: true to recover from the live index only (events held by the failed archive will NOT be reversed), or repair the archive source and retry",
				strings.Join(skippedArchives, ", "))), nil, nil
		}

		// The fetch above ran Order=DESC so --limit kept the newest suffix of
		// the window (#785/#927). Restore ascending order: GenerateSQLFromRows
		// expects ASC input and reverses it internally to undo most-recent
		// first.
		rows = query.MergeResults(rows, 0, "ASC")

		gen := recovery.NewForDialect(t.DB, resolver, recovery.DialectForIndex(t.DB))
		// #849: the console sets cfg.MaxScriptBytes to its own shared-daemon
		// budget; standalone bintrail-mcp leaves it 0, so the Generator keeps
		// its own DefaultMaxScriptBytes (2 GiB) exactly as before this field
		// existed. See Config.scriptBudgetOverride for why this must stay an
		// "only call when set" gate rather than an unconditional
		// SetMaxScriptBytes(cfg.MaxScriptBytes).
		if v, ok := cfg.scriptBudgetOverride(); ok {
			gen.SetMaxScriptBytes(v)
		}
		var buf bytes.Buffer
		n, stmtEnds, err := gen.GenerateSQLFromRowsIndexed(rows, &buf)
		if err != nil {
			// A *recovery.ScriptBudgetError gets a message built from its typed
			// fields rather than its own Error() verbatim — that text ends with
			// "raise/disable the budget (0 = unlimited)", advice that presumes a
			// Go caller of SetMaxScriptBytes, not an MCP client. Point at the one
			// escape hatch that's actually reachable from here on EITHER surface
			// (console or standalone): `bintrail recover` from the CLI, which does
			// expose --max-script-bytes (#849 code review: writeRecoverError in
			// internal/console/api.go does the equivalent for the HTTP endpoints;
			// this is the MCP-tool sibling of that fix).
			var be *recovery.ScriptBudgetError
			if errors.As(err, &be) {
				return ErrorResult(fmt.Errorf(
					"refusing to generate the reversal script: the matched events hold ~%.1f MiB of row data, "+
						"over the %.0f MiB budget for a single recovery. Narrow the recovery filter "+
						"(schema/table/pk/time range) to shrink the window, or use `bintrail recover` from the CLI "+
						"for large recoveries (it supports --max-script-bytes to raise or disable this budget)",
					float64(be.EstimatedBytes)/(1<<20), float64(be.Budget)/(1<<20))), nil, nil
			}
			return ErrorResult(err), nil, nil
		}

		text := buf.String()
		if resolverErr != nil {
			text += fmt.Sprintf("\n-- Note: schema snapshot unavailable (%v); WHERE clauses use all columns.\n", resolverErr)
		}
		if n > 0 {
			text += fmt.Sprintf("\n-- %d reversal statement(s) generated.\n", n)
		}
		if n >= opts.Limit {
			// The fetch ran Order=DESC (#785/#927), so the cut kept the newest
			// events — say so, matching the CLI recover warning (#1439).
			text += fmt.Sprintf("\n-- Warning: results truncated at %d rows; only the most recent events of the window are reversed. Use a narrower since/until range or increase the limit to see more.\n", opts.Limit)
		}
		if divergedEvents > 0 {
			// SQL-comment form so the warning survives inside the script text an
			// agent may hand to an operator verbatim (the truncation warning
			// above is the precedent).
			text += "\n-- Warning: " + eventDivergenceWarning(divergedEvents) + "\n"
		}

		// What this response carries of the script (#1438). The notes appended
		// above sit past the last statement's end offset, so they ride the FINAL
		// chunk and concatenating the chunks reproduces `text` exactly.
		script, serr := deliverScript(text, stmtEnds, args.SummaryOnly, args.SQLOffset, args.SQLLimit)
		if serr != nil {
			return ErrorResult(serr), nil, nil
		}

		// See the recover_cascade sibling for why a chunk fetch records too,
		// and why a summary does not: bytes of row data, not calls, are what
		// this audits.
		if script.Served {
			detail := map[string]string{
				"statements": strconv.Itoa(n),
				"dry_run":    "true", // MCP recover always returns the script, never applies it
				"gtid":       args.GTID,
			}
			if script.Chunked {
				detail["chunk"] = fmt.Sprintf("statements %d-%d of %d", script.From, script.To, n)
				detail["script_id"] = script.ScriptID
			}
			ext.Record(ctx, ext.AuditEvent{
				Surface: cfg.auditSurface(),
				Action:  "recover.generate",
				Actor:   ext.ProcessActor(args.Profile),
				Schema:  args.Schema,
				Table:   args.Table,
				Detail:  detail,
			})
		}

		// The whole script goes back as plain SQL, exactly as it always has.
		// Anything else — a summary, or one chunk — goes back as JSON, because
		// a chunk MUST be distinguishable from a complete script by a client
		// that reads fields rather than prose, and a SQL comment cannot do
		// that. Putting the marker inside the SQL would also break the promise
		// that concatenating chunks reproduces the script byte for byte.
		if script.Served && !script.Chunked {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: script.SQL},
				},
			}, nil, nil
		}
		payload, err := json.MarshalIndent(recoverResult{
			SQL:            script.SQL,
			StatementCount: n,
			ScriptID:       script.ScriptID,
			ScriptBytes:    script.ScriptBytes,
			SQLFrom:        script.From,
			SQLTo:          script.To,
			SQLMore:        script.More,
			NextSQLOffset:  script.NextOffset,
			SQLNote:        script.Note,
		}, "", "  ")
		if err != nil {
			return ErrorResult(fmt.Errorf("encode recover result: %w", err)), nil, nil
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: string(payload)},
			},
		}, nil, nil
	}
}

// MakeStatusTool returns the status tool handler.
func MakeStatusTool(cfg Config) func(context.Context, *mcp.CallToolRequest, StatusArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args StatusArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, ""); res != nil {
			return res, nil, nil
		}
		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		if t.DBName == "" {
			return ErrorResult(fmt.Errorf("DSN must include a database name")), nil, nil
		}

		data, err := status.CollectStatus(ctx, t.DB, t.DBName)
		if err != nil {
			return ErrorResult(err), nil, nil
		}

		var buf bytes.Buffer
		data.Write(&buf)

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: buf.String()},
			},
		}, nil, nil
	}
}

// MakeSchemaChangesTool returns the list_schema_changes tool handler.
func MakeSchemaChangesTool(cfg Config) func(context.Context, *mcp.CallToolRequest, SchemaChangesArgs) (*mcp.CallToolResult, any, error) {
	return func(ctx context.Context, req *mcp.CallToolRequest, args SchemaChangesArgs) (*mcp.CallToolResult, any, error) {
		if res := rejectSurfaceParams(cfg, args.IndexDSN, ""); res != nil {
			return res, nil, nil
		}
		// Validate inputs before connecting.
		sinceT, err := cliutil.ParseTime(args.Since)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid since: %w", err)), nil, nil
		}
		untilT, err := cliutil.ParseTime(args.Until)
		if err != nil {
			return ErrorResult(fmt.Errorf("invalid until: %w", err)), nil, nil
		}

		if args.DDLType != "" {
			upper := strings.ToUpper(args.DDLType)
			switch upper {
			case "CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE":
				// valid
			default:
				return ErrorResult(fmt.Errorf("invalid ddl_type %q; must be CREATE, ALTER, DROP, RENAME, or TRUNCATE", args.DDLType)), nil, nil
			}
		}

		limit := args.Limit
		if limit <= 0 {
			limit = 100
		}

		t, err := cfg.Resolve(ctx, args.IndexDSN)
		if err != nil {
			return ErrorResult(err), nil, nil
		}
		defer t.release()

		q := "SELECT id, detected_at, schema_name, table_name, ddl_type, ddl_query, binlog_file, binlog_pos, gtid, snapshot_id FROM schema_changes WHERE 1=1"
		var params []any

		if args.Schema != "" {
			q += " AND schema_name = ?"
			params = append(params, args.Schema)
		}
		if args.Table != "" {
			q += " AND table_name = ?"
			params = append(params, args.Table)
		}
		if args.DDLType != "" {
			// Match prefix: "ALTER" matches "ALTER TABLE", etc.
			q += " AND ddl_type LIKE ?"
			params = append(params, strings.ToUpper(args.DDLType)+"%")
		}
		if sinceT != nil {
			q += " AND detected_at >= ?"
			params = append(params, *sinceT)
		}
		if untilT != nil {
			q += " AND detected_at <= ?"
			params = append(params, *untilT)
		}
		if args.UncoveredOnly {
			// The status package's predicate, verbatim: this filter promises
			// "the ones counted by the status tool's uncovered-DDL warning",
			// and a re-written clause here diverged once (#1436: a bare
			// IS NULL listed the TRUNCATEs the warning deliberately excludes,
			// 9 rows against a count of 1).
			q += " AND " + status.UncoveredDDLWhere
		}
		// detected_at has one-second resolution and a migration lands dozens of
		// DDLs inside one second, so the timestamp alone leaves the group in
		// storage order (oldest first, the opposite of the DESC promise). The
		// binlog coordinate is the true order within a MySQL source (MySQL binlog
		// file names are zero-padded, so lexicographic is chronological); id is
		// the final deterministic tail so a limit cutting inside the group is
		// repeatable (#1441). Two shapes the stored data cannot order truly, only
		// repeatably: across sources (the table carries no source identity and
		// the file sequences are unrelated) and a Postgres source, whose
		// binlog_file is the commit LSN as "%X/%X" and NOT zero-padded, so
		// "0/9FFFFFF" sorts after "0/10000000". Position alone would be right for
		// Postgres and wrong for MySQL, and no single expression serves both
		// without a source column, so the MySQL shape wins here.
		q += " ORDER BY detected_at DESC, binlog_file DESC, binlog_pos DESC, id DESC LIMIT ?"
		params = append(params, limit)

		rows, err := t.DB.QueryContext(ctx, q, params...)
		if err != nil {
			if strings.Contains(err.Error(), "doesn't exist") || strings.Contains(err.Error(), "1146") {
				return ErrorResult(fmt.Errorf("schema_changes table not found; run `bintrail init` to create it")), nil, nil
			}
			return ErrorResult(fmt.Errorf("query schema_changes: %w", err)), nil, nil
		}
		defer rows.Close()

		type schemaChange struct {
			ID         int64  `json:"id"`
			DetectedAt string `json:"detected_at"`
			Schema     string `json:"schema_name"`
			Table      string `json:"table_name"`
			DDLType    string `json:"ddl_type"`
			Statement  string `json:"statement"`
			BinlogFile string `json:"binlog_file"`
			BinlogPos  int64  `json:"binlog_pos"`
			GTID       string `json:"gtid,omitempty"`
			// SnapshotID is the auto-snapshot taken after this DDL. Deliberately
			// NOT omitempty: an explicit null is the "uncovered DDL" signal the
			// status tool's warning points at (#1050) — except on TRUNCATE
			// rows, where SnapshotNote says the null is by design (#1436).
			SnapshotID *int64 `json:"snapshot_id"`
			// SnapshotNote disambiguates the one ddl_type whose null
			// snapshot_id means "no snapshot needed" rather than "no snapshot
			// taken": without it the same null carries two opposite meanings.
			SnapshotNote string `json:"snapshot_note,omitempty"`
		}

		var results []schemaChange
		for rows.Next() {
			var sc schemaChange
			var detectedAt time.Time
			var gtid sql.NullString
			var snapshotID sql.NullInt64
			if err := rows.Scan(&sc.ID, &detectedAt, &sc.Schema, &sc.Table, &sc.DDLType, &sc.Statement, &sc.BinlogFile, &sc.BinlogPos, &gtid, &snapshotID); err != nil {
				return ErrorResult(fmt.Errorf("scan: %w", err)), nil, nil
			}
			sc.DetectedAt = detectedAt.UTC().Format("2006-01-02 15:04:05")
			if gtid.Valid {
				sc.GTID = gtid.String
			}
			if snapshotID.Valid {
				sc.SnapshotID = &snapshotID.Int64
			} else if sc.DDLType == "TRUNCATE TABLE" {
				// The one null that is not a gap (#1436): every capture path
				// records TRUNCATE with snapshot_id = NULL on purpose, and
				// the status warning excludes it for the same reason.
				sc.SnapshotNote = "no snapshot needed: TRUNCATE changes no table structure"
			}
			results = append(results, sc)
		}
		if err := rows.Err(); err != nil {
			return ErrorResult(fmt.Errorf("rows iteration: %w", err)), nil, nil
		}

		out, err := json.MarshalIndent(results, "", "  ")
		if err != nil {
			return ErrorResult(fmt.Errorf("marshal: %w", err)), nil, nil
		}

		text := string(out)
		n := len(results)
		if n > 0 {
			text += fmt.Sprintf("\n\n%d schema change(s)", n)
		} else {
			text = "No schema changes found."
		}
		if n >= limit {
			// Results come back newest-first, so the cut kept the newest
			// changes (#1439).
			text += fmt.Sprintf("\nWarning: results truncated at %d rows, keeping the NEWEST changes; older ones were dropped. Use a narrower since/until range or increase the limit to see more.", limit)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: text},
			},
		}, nil, nil
	}
}

// ─── Shared helpers ──────────────────────────────────────────────────────────

// ErrorResult wraps an error as a tool-level MCP error result.
func ErrorResult(err error) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: err.Error()},
		},
		IsError: true,
	}
}

// EnvArchiveSources returns Parquet archive source paths for use with
// parquetquery.Fetch. It checks env vars BINTRAIL_ARCHIVE_S3 + BINTRAIL_ID
// first (for explicit configuration — both must be set), then falls back to
// auto-discovery from archive_state in the index database. The second return
// is true when discovery is UNRELIABLE: the archive_state read failed (see
// stateArchiveSources), or the env pair is half-set — explicit archive intent
// the fallback would otherwise silently discard. The fully-env-configured
// path never fails discovery.
func EnvArchiveSources(ctx context.Context, db *sql.DB) ([]string, bool) {
	archiveS3 := os.Getenv("BINTRAIL_ARCHIVE_S3")
	bintrailID := os.Getenv("BINTRAIL_ID")
	if archiveS3 != "" && bintrailID != "" {
		base := strings.TrimSuffix(archiveS3, "/") + "/bintrail_id=" + bintrailID
		return []string{base}, false
	}
	if archiveS3 != "" || bintrailID != "" {
		slog.Warn("partial archive env var config; both BINTRAIL_ARCHIVE_S3 and BINTRAIL_ID must be set",
			"BINTRAIL_ARCHIVE_S3", archiveS3, "BINTRAIL_ID", bintrailID)
		// A half-set env pair is explicit archive intent that cannot be
		// honored. With a rebuilt/empty archive_state — exactly the situation
		// where the env pair is the documented remediation — the silent
		// fallback would read as complete: the #1285 blind spot one env var
		// away. Fall back to state discovery for whatever it finds, but flag
		// discovery unreliable so the query tool warns and recover refuses.
		s, _ := stateArchiveSources(ctx, db)
		return s, true
	}
	return stateArchiveSources(ctx, db)
}

// misfiledArchiveHours wraps query.MisfiledArchiveHours (#1037). The second
// return is true when the registry scan FAILED: the per-source fetch would
// then prune by hour label only and may silently skip misfiled (backfilled)
// archives whose events are in the window. The query tool degrades with an
// archive_scan_incomplete warning; the recover tool refuses — the same
// knowingly-possibly-incomplete class as its other two archive gates. Never
// fails without a time filter (since/until both nil): nothing is pruned then.
func misfiledArchiveHours(ctx context.Context, db *sql.DB, since, until *time.Time) ([]time.Time, bool) {
	// AllArchives (#1232): a widening pruning hint, safe over-broad and
	// unsafe narrow — see the agent handler's call site.
	hours, err := query.MisfiledArchiveHours(ctx, db, since, until, query.AllArchives())
	if err != nil {
		slog.Warn("could not check archive_state for misfiled archives (backfilled events); time-scoped archive pruning may skip them", "error", err)
		return nil, true
	}
	return hours, false
}

// stateArchiveSources auto-discovers archive sources from archive_state. On a
// registry read failure it returns (nil, true): the fetch layer stays
// permissive, but the caller must surface the signal — the query tool warns,
// the recover tool refuses (#1285).
func stateArchiveSources(ctx context.Context, db *sql.DB) ([]string, bool) {
	sources, err := query.ResolveArchiveSources(ctx, db)
	if err != nil {
		slog.Warn("archive auto-discovery failed; proceeding without archives", "error", err)
		// true = discovery FAILED — distinct from "no archives exist" (nil,
		// false); the caller owes the client a warning or a refusal.
		return nil, true
	}
	return sources, false
}

// DefaultQueryMaxLimit is the hard row ceiling for the MCP query tool (#654):
// a backstop against a pathological explicit limit OOMing the long-lived server.
// ~1M rows is multiple GB worst-case at the project's per-row sizing — far above
// any legitimate agent query, yet bounded. The unbounded escape hatch is the
// `bintrail query` CLI, so this is deliberately not disengageable via env.
const DefaultQueryMaxLimit = 1_000_000

// EnvQueryMaxLimit returns the standalone MCP query-tool row ceiling.
// BINTRAIL_MCP_QUERY_MAX_LIMIT raises or lowers it; an empty/invalid/<=0 value
// falls back to the default rather than disabling the ceiling (the CLI is the
// unbounded path, not the agent-facing tool).
func EnvQueryMaxLimit() int {
	if v := os.Getenv("BINTRAIL_MCP_QUERY_MAX_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return DefaultQueryMaxLimit
}

// ApplyQueryCeiling caps limit to max, returning the (possibly capped) limit and
// whether a cap was applied. max <= 0 disables capping.
func ApplyQueryCeiling(limit, max int) (int, bool) {
	if max > 0 && limit > max {
		return max, true
	}
	return limit, false
}

// QueryResultNotice composes the trailing warning appended to a query-tool
// result. When the ceiling fired it SUPERSEDES the generic truncation notice —
// telling an agent to "increase the limit" would be wrong, since the limit it
// asked for is exactly the one that was capped (and after a cap n == limit makes
// the generic arm true too, so order matters). Returns "" when no notice applies.
func QueryResultNotice(ceilingApplied bool, requestedLimit, ceiling, n, limit, limitPerPK int, order string) string {
	switch {
	case ceilingApplied:
		return fmt.Sprintf("\nWarning: requested limit %d exceeds the MCP query ceiling of %d rows; capped to %d. "+
			"Narrow your filters/time range, or run the `bintrail query` CLI for an unbounded export.\n", requestedLimit, ceiling, ceiling)
	case n >= limit && limitPerPK > 0:
		// The per-PK cap's inner ordering is pinned DESC (latest N per
		// pk_values, whatever the final direction), so what the cap can drop
		// depends on the outer direction. Under DESC the globally newest
		// event is always bt_rn=1 in its own partition and row 0 of the
		// page — the newest end is provably intact, and claiming it was
		// dropped is the same defect class this notice exists to remove.
		// Under ASC (or empty) the outer limit cuts the newest end while the
		// cap cuts old events per row: both ends.
		if query.OrderDirection(order) == "DESC" {
			return fmt.Sprintf("\nWarning: results truncated at %d rows, keeping the NEWEST matching events, "+
				"and limit_per_pk kept only the latest %d events per row, so older events were dropped from "+
				"inside the window as well. Use a narrower since/until range or increase the limit to see more.\n",
				limit, limitPerPK)
		}
		return fmt.Sprintf("\nWarning: results truncated at %d rows, and limit_per_pk kept only the latest %d "+
			"events per row, so events were dropped at both ends of the window. "+
			"Use a narrower since/until range or increase the limit to see more.\n", limit, limitPerPK)
	case n >= limit:
		// The direction names which END the trim kept (#1439): "truncated"
		// alone left a client that asked for the last N unable to tell
		// whether the newest events survived the cut or fell off it.
		kept, dropped := "OLDEST", "newer"
		if query.OrderDirection(order) == "DESC" {
			kept, dropped = "NEWEST", "older"
		}
		return fmt.Sprintf("\nWarning: results truncated at %d rows, keeping the %s matching events; %s ones were dropped. "+
			"Use a narrower since/until range or increase the limit to see more.\n", limit, kept, dropped)
	}
	return ""
}

// archiveSourceSkippedWarning is the ONE wording for a skipped archive
// source, shared by the reconstruct tool's warnings array and
// ArchiveSkipNotice — two byte-identical copies would drift silently, and
// both the tests and any client matching on the wording treat the mirroring
// as a contract.
func archiveSourceSkippedWarning(src string) string {
	return "archive_source_skipped: events held only by this source are missing from the result: " + src
}

// archiveDiscoveryFailedWarning is the shared wording for "discovery itself
// failed": no source list exists, so archived events may silently be absent.
func archiveDiscoveryFailedWarning() string {
	return "archive_discovery_failed: no archives were read and archived hours may still be counted as covered; the result may be incomplete"
}

// archiveScanIncompleteWarning is the wording for a failed misfiled-archive
// registry scan (#1037): archives WERE read, but time-scoped pruning may have
// skipped backfilled files — softer than archive_discovery_failed.
func archiveScanIncompleteWarning() string {
	return "archive_scan_incomplete: the archive registry could not be checked for misfiled (backfilled) archives; time-scoped pruning may have skipped archived events in the window"
}

// eventDivergenceWarning is the ONE wording for a merge divergence (#1325):
// two copies of the same event_id — the live index's and an archived
// partition's — disagreed, and one was silently chosen. Shared by the query
// tool's trailing notice, the recover tool's SQL-comment warning and the
// reconstruct tool's warnings array, same single-wording contract as
// archiveSourceSkippedWarning. Deliberately no remediation command and no
// flag-shaped tokens: per-event detail (event_id, schema, table, pk) is in the
// server log, which is where the diagnosis lives.
func eventDivergenceWarning(n int) string {
	return fmt.Sprintf("event_divergence: %d duplicate event(s) disagreed between the live index and an archive copy; the first copy fetched (normally the live index) was used. "+
		"An archived partition should be a byte-for-byte copy of the index rows; a mismatch means the index row changed after archiving, or two index generations wrote under the same bintrail_id. "+
		"Per-event detail is in the server log", n)
}

// EventDivergenceNotice renders the query tool's trailing warning line for a
// merge divergence (#1325). Empty when nothing diverged — the cry-wolf rule:
// an agreeing duplicate (the normal archived-but-not-dropped overlap) must
// render nothing.
func EventDivergenceNotice(n int) string {
	if n == 0 {
		return ""
	}
	return "\nWarning: " + eventDivergenceWarning(n) + "\n"
}

// ArchiveSkipNotice renders the query tool's trailing warning lines for
// archive trouble (#1285): a discovery failure and/or per-source fetch
// failures. An MCP client sees only the result text, so a response missing
// archived events must not read as complete — the blind spot the reconstruct
// tool's warnings already close, mirrored via the shared helpers above.
// Trailing plain-text lines are the QueryResultNotice precedent (JSON bodies
// included). Each line names only the SOURCE — which for a local archive is a
// server-side path, exactly as the ratified reconstruct precedent already
// exposes — and never the fetch ERROR: error detail (DuckDB internals,
// remediation hints, anything the no-CLI-flag-leak rule covers) stays in the
// server log. The recover tool does not use this: it REFUSES on archive
// trouble instead (see MakeRecoverTool).
func ArchiveSkipNotice(discoveryFailed bool, skipped []string) string {
	var b strings.Builder
	if discoveryFailed {
		b.WriteString("\nWarning: " + archiveDiscoveryFailedWarning() + "\n")
	}
	for _, s := range skipped {
		b.WriteString("\nWarning: " + archiveSourceSkippedWarning(s) + "\n")
	}
	return b.String()
}

// FilterParams are the shared filter parameters the query and recover tools
// hand to BuildQueryOptions. Both tools map their (deliberately separate) arg
// structs into this one builder so the cross-field validation stays in one
// place. ChangedColumn is query-only: the recover handler has no field to
// plumb into it (see the RecoverArgs doc comment), and QueryHash is
// deliberately absent — the query tool layers it on AFTER the builder so the
// recover tool can never gain it through here.
type FilterParams struct {
	Schema        string
	Table         string
	PK            string
	PKs           []string
	PKMin         string
	PKMax         string
	LimitPerPK    int
	EventType     string
	GTID          string
	Since         string
	Until         string
	ChangedColumn string
	ColumnEq      []string
	Flag          string
	Limit         int
}

// cleanPKs mirrors the CLI's pks hygiene (internal/cli cleanPKList): trim
// whitespace, reject empty entries loudly, and drop duplicates while keeping
// the first occurrence's order. MCP clients send a JSON array so there is no
// comma-splitting concern, but a whitespace-only entry is still a client bug
// worth naming rather than a silently narrower filter.
func cleanPKs(pks []string) ([]string, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(pks))
	out := make([]string, 0, len(pks))
	for _, pk := range pks {
		trimmed := strings.TrimSpace(pk)
		if trimmed == "" {
			return nil, fmt.Errorf("pks: empty or whitespace-only PK value")
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out, nil
}

// buildPKRange is the pre-fetch half of pk_min/pk_max (#1440): the pairing
// rules and the integer syntax of each bound. The range it returns is
// UNRESOLVED (no cast yet); the handler completes it with
// Target.resolvePKRange once it holds the schema snapshot, and every engine
// refuses a range that skipped that step.
func buildPKRange(p FilterParams) (*query.PKRange, error) {
	if p.PKMin == "" && p.PKMax == "" {
		return nil, nil
	}
	if p.Schema == "" || p.Table == "" {
		return nil, fmt.Errorf("pk_min/pk_max require both schema and table")
	}
	if p.PK != "" || len(p.PKs) > 0 {
		return nil, fmt.Errorf("pk_min/pk_max cannot be combined with pk or pks; use the range or the exact keys")
	}
	var lo, hi *big.Int
	var err error
	if p.PKMin != "" {
		if lo, err = query.ParsePKBound(p.PKMin); err != nil {
			return nil, fmt.Errorf("pk_min: %w", err)
		}
	}
	if p.PKMax != "" {
		if hi, err = query.ParsePKBound(p.PKMax); err != nil {
			return nil, fmt.Errorf("pk_max: %w", err)
		}
	}
	r, err := query.NewPKRange(lo, hi)
	if err != nil {
		return nil, fmt.Errorf("pk_min/pk_max: %w", err)
	}
	return r, nil
}

// resolvePKRange completes a pk_min/pk_max request against the schema
// snapshot: the table's key must be one integer column, and the cast both
// engines compare through is chosen from its signedness. It always loads the
// LATEST snapshot, never the surface's preloaded Target.Resolver: the console
// opens that one once per bundle and carries it across rebuilds, so on a
// long-lived daemon it can be days old, and a table snapshotted since would
// read "not found" while a BIGINT to BIGINT UNSIGNED change would pick the
// signed cast (the same staleness the #957 note in internal/cli/query.go
// refuses to trust). No snapshot means a refusal, not a guessed cast.
//
// Call it AFTER the surface's RBAC posture is on opts: a denied table is
// refused without consulting the snapshot, so the refusal cannot describe
// the key of a table the profile withholds.
func (t *Target) resolvePKRange(opts *query.Options) error {
	if opts.PKRange == nil {
		return nil
	}
	for _, dt := range opts.DenyTables {
		if strings.EqualFold(dt.Schema, opts.Schema) && strings.EqualFold(dt.Table, opts.Table) {
			return fmt.Errorf("pk_min/pk_max: the active profile does not allow reading %s.%s", opts.Schema, opts.Table)
		}
	}
	resolver, err := metadata.NewResolver(t.DB, 0)
	if err != nil {
		return fmt.Errorf("pk_min/pk_max need the schema snapshot to check the primary key type: %w", err)
	}
	tm, err := resolver.Resolve(opts.Schema, opts.Table)
	if err != nil {
		return fmt.Errorf("pk_min/pk_max: %w", err)
	}
	if err := opts.PKRange.ResolveCast(tm); err != nil {
		return fmt.Errorf("pk_min/pk_max: %w", err)
	}
	return nil
}

// BuildQueryOptions converts the shared tool filter parameters into a
// query.Options, validating cross-field requirements and applying the default
// limit to a non-positive request.
func BuildQueryOptions(p FilterParams, defaultLimit int) (query.Options, error) {
	if p.PK != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, fmt.Errorf("pk requires both schema and table")
	}
	if len(p.PKs) > 0 && (p.Schema == "" || p.Table == "") {
		return query.Options{}, fmt.Errorf("pks requires both schema and table")
	}
	if p.PK != "" && len(p.PKs) > 0 {
		return query.Options{}, fmt.Errorf("pk and pks are mutually exclusive; use one or the other")
	}
	pks, err := cleanPKs(p.PKs)
	if err != nil {
		return query.Options{}, err
	}
	if p.LimitPerPK < 0 {
		return query.Options{}, fmt.Errorf("limit_per_pk must be >= 0")
	}
	if p.LimitPerPK > 0 && p.PK == "" && len(pks) == 0 {
		return query.Options{}, fmt.Errorf("limit_per_pk requires pk or pks")
	}
	pkRange, err := buildPKRange(p)
	if err != nil {
		return query.Options{}, err
	}
	if p.ChangedColumn != "" && (p.Schema == "" || p.Table == "") {
		return query.Options{}, fmt.Errorf("changed_column requires both schema and table")
	}
	if len(p.ColumnEq) > 0 && (p.Schema == "" || p.Table == "") {
		return query.Options{}, fmt.Errorf("column_eq requires both schema and table")
	}

	et, err := cliutil.ParseEventType(p.EventType)
	if err != nil {
		return query.Options{}, err
	}
	sinceT, err := cliutil.ParseTime(p.Since)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid since: %w", err)
	}
	untilT, err := cliutil.ParseTime(p.Until)
	if err != nil {
		return query.Options{}, fmt.Errorf("invalid until: %w", err)
	}
	parsedEq, err := query.ParseColumnEqs(p.ColumnEq)
	if err != nil {
		return query.Options{}, err
	}

	limit := p.Limit
	if limit <= 0 {
		limit = defaultLimit
	}

	return query.Options{
		Schema:        p.Schema,
		Table:         p.Table,
		PKValues:      p.PK,
		PKValuesIn:    pks,
		PKRange:       pkRange,
		LimitPerPK:    p.LimitPerPK,
		EventType:     et,
		GTID:          p.GTID,
		Since:         sinceT,
		Until:         untilT,
		ChangedColumn: p.ChangedColumn,
		ColumnEq:      parsedEq,
		Flag:          p.Flag,
		Limit:         limit,
	}, nil
}
