package mcptools

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// recoverToolMockCols is the binlog_events SELECT column list the recover
// tool's fetch expects — matches the console's own recover fixtures
// (internal/console/api_test.go) so a mocked row scans cleanly through
// query.Fetch.
var recoverToolMockCols = []string{
	"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
	"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
	"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
	"commit_ts_us",
}

// newRecoverToolTarget builds a Config+Target pair around a sqlmock DB,
// bypassing DSN routing and schema migration (NoArchive/ResolverLoaded mirror
// the console's own posture in internal/console/mcp.go) so MakeRecoverTool
// exercises the real fetch → GenerateSQLFromRows path.
func newRecoverToolTarget(db *sql.DB, maxScriptBytes int64) Config {
	return Config{
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{
				DB:             db,
				NoArchive:      true,
				ResolverLoaded: true, // Resolver stays nil: all-column WHERE fallback, no metadata query
			}, nil
		},
		MaxScriptBytes: maxScriptBytes,
	}
}

// resultText extracts the text of a tool result's first content item.
func resultText(res *mcp.CallToolResult) string {
	if res == nil || len(res.Content) == 0 {
		return ""
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		return ""
	}
	return tc.Text
}

// makeRowsWithStatement builds two rows: one carrying captured statement
// text, one without (the nil case must stay nil after stripping).
func makeRowsWithStatement(qt, qh string) []query.ResultRow {
	return []query.ResultRow{
		{EventID: 1, QueryText: &qt, QueryHash: &qh},
		{EventID: 2},
	}
}

// rejectingConfig is the console-style posture: DSN/profile parameters
// refused. Resolve fails the test if a handler reaches it despite the
// rejection — the whole point is that the surface never dereferences a
// client-supplied DSN.
func rejectingConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Resolve: func(ctx context.Context, argDSN string) (*Target, error) {
			t.Fatalf("Resolve must not be called for a rejected parameter (argDSN=%q)", argDSN)
			return nil, nil
		},
		AllowDSNParam:     false,
		AllowProfileParam: false,
	}
}

func TestIndexDSNParamRejected(t *testing.T) {
	cfg := rejectingConfig(t)
	ctx := context.Background()

	cases := []struct {
		tool string
		call func() (isErr bool, text string)
	}{
		{"query", func() (bool, string) {
			res, _, _ := MakeQueryTool(cfg)(ctx, nil, QueryArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
		{"recover", func() (bool, string) {
			res, _, _ := MakeRecoverTool(cfg)(ctx, nil, RecoverArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
		{"status", func() (bool, string) {
			res, _, _ := MakeStatusTool(cfg)(ctx, nil, StatusArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
		{"list_schema_changes", func() (bool, string) {
			res, _, _ := MakeSchemaChangesTool(cfg)(ctx, nil, SchemaChangesArgs{IndexDSN: "u:p@tcp(1.2.3.4:3306)/x"})
			return res.IsError, resultText(res)
		}},
	}
	for _, tc := range cases {
		isErr, text := tc.call()
		if !isErr {
			t.Errorf("%s: index_dsn must be rejected when AllowDSNParam is false", tc.tool)
		}
		if !strings.Contains(text, "index_dsn is not accepted") {
			t.Errorf("%s: rejection must be explicit, got: %s", tc.tool, text)
		}
	}
}

func TestProfileParamRejected(t *testing.T) {
	cfg := rejectingConfig(t)
	ctx := context.Background()

	res, _, _ := MakeQueryTool(cfg)(ctx, nil, QueryArgs{Profile: "auditor"})
	if !res.IsError || !strings.Contains(resultText(res), "profile is not accepted") {
		t.Errorf("query: profile must be rejected when AllowProfileParam is false, got: %s", resultText(res))
	}
	res, _, _ = MakeRecoverTool(cfg)(ctx, nil, RecoverArgs{Profile: "auditor"})
	if !res.IsError || !strings.Contains(resultText(res), "profile is not accepted") {
		t.Errorf("recover: profile must be rejected when AllowProfileParam is false, got: %s", resultText(res))
	}
}

func TestParamsAcceptedOnStandalonePosture(t *testing.T) {
	// With both params allowed, rejection must not fire; the call proceeds to
	// Resolve (which errors here, proving the gate was passed).
	cfg := Config{
		Resolve: func(ctx context.Context, argDSN string) (*Target, error) {
			if argDSN != "dsn-under-test" {
				t.Errorf("argDSN = %q, want it passed through", argDSN)
			}
			return nil, context.Canceled // any error: stops before touching a DB
		},
		AllowDSNParam:     true,
		AllowProfileParam: true,
	}
	res, _, _ := MakeQueryTool(cfg)(context.Background(), nil, QueryArgs{IndexDSN: "dsn-under-test", Profile: "p"})
	if !res.IsError {
		t.Fatal("expected the resolver error to surface")
	}
	if strings.Contains(resultText(res), "not accepted") {
		t.Errorf("params must not be rejected on the standalone posture: %s", resultText(res))
	}
}

func TestNewServerRegistersTools(t *testing.T) {
	s := NewServer(Config{Version: "test"})
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
}

// TestRecoverToolMaxScriptBytesEnforced is the #849 (item 1) repro: the
// console's /mcp recover tool is a FOURTH script-rendering code path (besides
// /api/recover, its auto-cascade branch, and /api/recover-cascade) that a
// code review found still left at recovery.DefaultMaxScriptBytes (2 GiB)
// despite already sharing the console's row-count cap (RecoverMaxLimit). A
// small cfg.MaxScriptBytes must make the tool refuse over-budget rows with a
// *recovery.ScriptBudgetError surfaced as an MCP tool error — proving
// mcptools.go actually calls gen.SetMaxScriptBytes(cfg.MaxScriptBytes) rather
// than leaving the Generator's own default in place. It also pins the code
// review's follow-up on this same tool: the surfaced message must not leak
// ScriptBudgetError.Error()'s raw "raise/disable the budget (0 = unlimited)"
// phrasing — advice aimed at a Go caller of SetMaxScriptBytes, not an MCP
// client — and must instead point at the `bintrail recover` CLI escape hatch
// (mirroring internal/console/api.go's writeRecoverError for the HTTP paths).
func TestRecoverToolMaxScriptBytesEnforced(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rowAfter := []byte(`{"id":42,"blob":"` + strings.Repeat("x", 4096) + `"}`) // well over a 1024-byte budget
	rows := sqlmock.NewRows(recoverToolMockCols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, rowAfter, int64(0), nil, nil, nil,
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	cfg := newRecoverToolTarget(db, 1024) // tighter than the 4 KB+ row payload
	res, _, _ := MakeRecoverTool(cfg)(context.Background(), nil, RecoverArgs{Schema: "app", Table: "users"})

	if !res.IsError {
		t.Fatalf("expected the tool to refuse over budget, got: %s", resultText(res))
	}
	text := resultText(res)
	for _, want := range []string{"refusing to generate the reversal script", "bintrail recover"} {
		if !strings.Contains(text, want) {
			t.Errorf("error text missing %q: %s", want, text)
		}
	}
	if strings.Contains(text, "0 = unlimited") {
		t.Errorf("error text must not leak the CLI-only '0 = unlimited' phrasing: %s", text)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestRecoverToolMaxScriptBytesZeroKeepsDefault is a sanity check that the
// standalone posture (Config.MaxScriptBytes left at its zero value) does not
// spuriously refuse an ORDINARY recovery. It does NOT by itself distinguish
// "the code never called SetMaxScriptBytes, so the Generator kept its 2 GiB
// constructor default" from "the code called SetMaxScriptBytes(0), which
// explicitly DISABLES the budget guard (unlimited)" — an ordinary small row
// succeeds either way, and reproducing the actual >2 GiB payload needed to
// tell them apart end-to-end is impractical in a unit test. That distinction
// is what actually matters (a silent regression from "2 GiB default" to
// "unlimited" would defeat #654's guard for every standalone caller) and is
// pinned directly, without a giant payload, by TestConfigScriptBudgetOverride
// below, which asserts on the decision function itself
// (Config.scriptBudgetOverride) rather than its rendering side effect.
func TestRecoverToolMaxScriptBytesZeroKeepsDefault(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := sqlmock.NewRows(recoverToolMockCols).AddRow(
		int64(1), "bin.000001", int64(4), int64(40), ts,
		nil, nil, "app", "users", int64(parser.EventInsert), "42",
		nil, nil, []byte(`{"id":42,"email":"a@x"}`), int64(0), nil, nil, nil,
	)
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	cfg := newRecoverToolTarget(db, 0) // standalone posture: field left unset
	res, _, _ := MakeRecoverTool(cfg)(context.Background(), nil, RecoverArgs{Schema: "app", Table: "users"})

	if res.IsError {
		t.Fatalf("standalone posture (MaxScriptBytes=0) must not refuse an ordinary recovery: %s", resultText(res))
	}
	if !strings.Contains(resultText(res), "DELETE FROM") {
		t.Errorf("expected a DELETE in the undo SQL, got:\n%s", resultText(res))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestConfigScriptBudgetOverride directly pins the #849 code-review follow-up:
// Config.MaxScriptBytes <= 0 (the standalone bintrail-mcp posture, which
// never sets the field) must report ok=false — "leave the Generator's own
// default alone" — never (0, true), which the recover tool would turn into
// gen.SetMaxScriptBytes(0), and SetMaxScriptBytes treats n<=0 as "disable the
// guard" (unlimited), not "no override." A future edit that collapsed
// scriptBudgetOverride's `if MaxScriptBytes > 0` into always returning
// (c.MaxScriptBytes, true) would flip standalone bintrail-mcp from a 2 GiB
// budget to no budget at all, and this test — unlike
// TestRecoverToolMaxScriptBytesZeroKeepsDefault, which only observes an
// ordinary-sized row succeeding either way — would catch it immediately.
func TestConfigScriptBudgetOverride(t *testing.T) {
	cases := []struct {
		name      string
		maxBytes  int64
		wantValue int64
		wantOK    bool
	}{
		{"unset (standalone posture)", 0, 0, false},
		{"negative", -1, 0, false},
		{"explicit positive (console posture)", 32 << 20, 32 << 20, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{MaxScriptBytes: tc.maxBytes}
			v, ok := cfg.scriptBudgetOverride()
			if ok != tc.wantOK || v != tc.wantValue {
				t.Errorf("scriptBudgetOverride() = (%d, %v), want (%d, %v)", v, ok, tc.wantValue, tc.wantOK)
			}
		})
	}
}

func TestStripStatementText(t *testing.T) {
	qt, qh := "UPDATE t SET secret = 1", "abc123"
	rows := makeRowsWithStatement(qt, qh)

	off := &Target{RedactStatementText: false}
	off.stripStatementText(rows)
	if rows[0].QueryText == nil {
		t.Fatal("strip must be a no-op when RedactStatementText is false")
	}

	on := &Target{RedactStatementText: true}
	on.stripStatementText(rows)
	for i, r := range rows {
		if r.QueryText != nil || r.QueryHash != nil {
			t.Errorf("row %d: query_text/query_hash must be blanked, got %v/%v", i, r.QueryText, r.QueryHash)
		}
	}
}

// ─── list_schema_changes: snapshot_id coverage (#1050) ───────────────────────

// schemaChangesMockCols matches the tool's schema_changes SELECT column list.
var schemaChangesMockCols = []string{
	"id", "detected_at", "schema_name", "table_name", "ddl_type", "ddl_query",
	"binlog_file", "binlog_pos", "gtid", "snapshot_id",
}

// newSchemaChangesTarget builds a Config around a sqlmock DB so
// MakeSchemaChangesTool exercises its real SELECT → scan → JSON path.
func newSchemaChangesTarget(db *sql.DB) Config {
	return Config{
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{DB: db}, nil
		},
	}
}

func TestSchemaChangesTool_snapshotIDRoundTrip(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	detected := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("FROM schema_changes WHERE 1=1 ORDER BY detected_at DESC, binlog_file DESC, binlog_pos DESC, id DESC LIMIT").
		WillReturnRows(sqlmock.NewRows(schemaChangesMockCols).
			// Covered DDL: auto-snapshot 7 was taken after it.
			AddRow(1, detected, "shop", "orders", "ALTER TABLE",
				"ALTER TABLE orders ADD COLUMN note TEXT", "binlog.000001", 4, "uuid:1", 7).
			// Uncovered DDL: snapshot_id is NULL — must surface as explicit null.
			AddRow(2, detected, "shop", "orders", "TRUNCATE TABLE",
				"TRUNCATE TABLE orders", "binlog.000001", 900, nil, nil))

	res, _, err := MakeSchemaChangesTool(newSchemaChangesTarget(db))(context.Background(), nil, SchemaChangesArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected tool error: %s", resultText(res))
	}
	text := resultText(res)
	if !strings.Contains(text, `"snapshot_id": 7`) {
		t.Errorf("covered DDL must carry its snapshot_id, got:\n%s", text)
	}
	if !strings.Contains(text, `"snapshot_id": null`) {
		t.Errorf("uncovered DDL must carry an explicit null snapshot_id, got:\n%s", text)
	}
	// The null row above is a TRUNCATE, the one ddl_type whose null is by
	// design: without the note the same null means both "no snapshot taken"
	// and "no snapshot needed" (#1436).
	if !strings.Contains(text, `"snapshot_note": "no snapshot needed`) {
		t.Errorf("a TRUNCATE's null snapshot_id carries no snapshot_note, so the null keeps two meanings:\n%s", text)
	}
	if strings.Count(text, "snapshot_note") != 1 {
		t.Errorf("snapshot_note must appear on the TRUNCATE row only, got:\n%s", text)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestSchemaChangesTool_uncoveredOnlyFilter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	detected := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	// The expectation only matches when the SQL carries the STATUS package's
	// predicate — IS NULL plus the TRUNCATE exclusion (#1436). A bare IS NULL
	// here is the drift this pin exists to catch: the filter promises exactly
	// the rows the status warning counts, and the two clauses diverged once
	// (9 listed against a count of 1, the delta being TRUNCATEs whose null is
	// by design).
	mock.ExpectQuery(`FROM schema_changes WHERE 1=1 AND \(snapshot_id IS NULL AND ddl_type <> 'TRUNCATE TABLE' AND ddl_query NOT LIKE '/\* dbtrail: inferred from relation shape;%'\) ORDER BY detected_at DESC, binlog_file DESC, binlog_pos DESC, id DESC LIMIT`).
		WillReturnRows(sqlmock.NewRows(schemaChangesMockCols).
			AddRow(2, detected, "shop", "orders", "ALTER TABLE",
				"ALTER TABLE orders ADD COLUMN note TEXT", "binlog.000001", 900, nil, nil))

	res, _, err := MakeSchemaChangesTool(newSchemaChangesTarget(db))(context.Background(), nil, SchemaChangesArgs{UncoveredOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("unexpected tool error: %s", resultText(res))
	}
	if !strings.Contains(resultText(res), `"snapshot_id": null`) {
		t.Errorf("uncovered row must round-trip a null snapshot_id, got:\n%s", resultText(res))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
