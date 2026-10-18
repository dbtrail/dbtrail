package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// sqlCall records what a stub RunSQL was asked.
type sqlCall struct {
	statement, schema string
	maxRows           int
}

// stubSQL answers every statement with ans (or err), recording the calls.
func stubSQL(ans SQLAnswer, err error) SQLRunFunc {
	return stubSQLRecording(ans, err, nil)
}

func stubSQLRecording(ans SQLAnswer, err error, calls *[]sqlCall) SQLRunFunc {
	return func(_ context.Context, statement, schema string, maxRows int) (SQLAnswer, error) {
		if calls != nil {
			*calls = append(*calls, sqlCall{statement, schema, maxRows})
		}
		return ans, err
	}
}

// sqlToolConfig serves every call from t.
func sqlToolConfig(t *Target) Config {
	return Config{Resolve: func(context.Context, string) (*Target, error) { return t, nil }}
}

func oneSQLRow() SQLAnswer {
	return SQLAnswer{Result: sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "n", Type: "BIGINT"}},
		Rows:    [][]any{{json.Number("1")}},
		Elapsed: 12 * time.Millisecond,
	}}
}

// decodeSQLResult reads a successful sql answer.
func decodeSQLResult(t *testing.T, res *mcp.CallToolResult) map[string]any {
	t.Helper()
	if res.IsError {
		t.Fatalf("sql tool failed: %s", resultText(res))
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(resultText(res)), &out); err != nil {
		t.Fatalf("answer is not JSON: %v\n%s", err, resultText(res))
	}
	return out
}

func TestSQLTool_refusesBadArgumentsBeforeRunning(t *testing.T) {
	cases := []struct {
		name string
		args SQLArgs
		want string
	}{
		{"empty", SQLArgs{}, "statement is empty"},
		{"blank", SQLArgs{Statement: "  \n\t "}, "statement is empty"},
		{"negative max_rows", SQLArgs{Statement: "SELECT 1", MaxRows: -1}, "max_rows must be between 1 and 500"},
		{"max_rows over the cap", SQLArgs{Statement: "SELECT 1", MaxRows: SQLDefaultMaxRows + 1}, "max_rows must be between 1 and 500"},
		{"statement too long", SQLArgs{Statement: "SELECT '" + strings.Repeat("x", SQLMaxStatementBytes) + "'"}, "longer than the 64 KiB"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []sqlCall
			resolved := false
			cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
				resolved = true
				return &Target{RunSQL: stubSQLRecording(oneSQLRow(), nil, &calls)}, nil
			}}
			res, _, _ := MakeSQLTool(cfg)(context.Background(), nil, tc.args)
			if !res.IsError || !strings.Contains(resultText(res), tc.want) {
				t.Fatalf("got %q (IsError=%v), want an error containing %q", resultText(res), res.IsError, tc.want)
			}
			if resolved || len(calls) != 0 {
				t.Errorf("resolved=%v calls=%d: a bad argument must be refused before the server is touched", resolved, len(calls))
			}
		})
	}
}

func TestSQLTool_passesTheCapAndTheSchema(t *testing.T) {
	cases := []struct {
		name       string
		args       SQLArgs
		wantRows   int
		wantSchema string
		wantEchoed string
	}{
		{"default cap", SQLArgs{Statement: "SELECT 1"}, SQLDefaultMaxRows, "", "SELECT 1"},
		{"asked cap", SQLArgs{Statement: "SELECT 1", MaxRows: 7}, 7, "", "SELECT 1"},
		{"cap at the ceiling", SQLArgs{Statement: "SELECT 1", MaxRows: SQLDefaultMaxRows}, SQLDefaultMaxRows, "", "SELECT 1"},
		{"schema trimmed", SQLArgs{Statement: "SELECT 1", Schema: "  shop \n"}, SQLDefaultMaxRows, "shop", "SELECT 1"},
		// The statement is never trimmed or rewritten: what is echoed is
		// what ran, multi-line, with its comments and surrounding space.
		{"statement verbatim", SQLArgs{Statement: "\n  SELECT n -- the count\n  FROM shop.orders;  "}, SQLDefaultMaxRows, "", "\n  SELECT n -- the count\n  FROM shop.orders;  "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []sqlCall
			res, _, _ := MakeSQLTool(sqlToolConfig(&Target{RunSQL: stubSQLRecording(oneSQLRow(), nil, &calls)}))(context.Background(), nil, tc.args)
			out := decodeSQLResult(t, res)
			if len(calls) != 1 {
				t.Fatalf("RunSQL called %d times, want 1", len(calls))
			}
			c := calls[0]
			if c.maxRows != tc.wantRows || c.schema != tc.wantSchema || c.statement != tc.wantEchoed {
				t.Errorf("RunSQL got (%q, %q, %d), want (%q, %q, %d)", c.statement, c.schema, c.maxRows, tc.wantEchoed, tc.wantSchema, tc.wantRows)
			}
			if out["statement"] != tc.wantEchoed {
				t.Errorf("echoed statement = %q, want %q", out["statement"], tc.wantEchoed)
			}
			if got, _ := out["schema"].(string); got != tc.wantSchema {
				t.Errorf("echoed schema = %q, want %q", got, tc.wantSchema)
			}
		})
	}
}

func TestSQLTool_refusesUnderAccessRules(t *testing.T) {
	cases := map[string]*Target{
		"profile":  {ProfileActive: true},
		"deny":     {DenyTables: []query.SchemaTable{{Schema: "shop", Table: "users"}}},
		"redacted": {RedactColumns: []query.SchemaTableColumn{{Schema: "shop", Table: "users", Column: "email"}}},
	}
	for name, tgt := range cases {
		t.Run(name, func(t *testing.T) {
			rec := audittest.Install(t)
			var calls []sqlCall
			tgt.RunSQL = stubSQLRecording(oneSQLRow(), nil, &calls)
			res, _, _ := MakeSQLTool(sqlToolConfig(tgt))(context.Background(), nil, SQLArgs{Statement: "SELECT * FROM shop.users"})
			if !res.IsError || !strings.Contains(resultText(res), "access rules are active") {
				t.Fatalf("got %q, want the access-rules refusal", resultText(res))
			}
			if len(calls) != 0 {
				t.Error("the statement ran under access rules")
			}
			if n := len(rec.Events()); n != 0 {
				t.Errorf("a refused call recorded %d audit events, want 0", n)
			}
		})
	}
}

func TestSQLTool_noRunnerAndRunnerErrors(t *testing.T) {
	res, _, _ := MakeSQLTool(sqlToolConfig(&Target{}))(context.Background(), nil, SQLArgs{Statement: "SELECT 1"})
	if !res.IsError || !strings.Contains(resultText(res), "not available on this server") {
		t.Errorf("no RunSQL: got %q", resultText(res))
	}
	res, _, _ = MakeSQLTool(sqlToolConfig(&Target{RunSQL: stubSQL(SQLAnswer{}, errors.New("query refused: not a SELECT"))}))(context.Background(), nil, SQLArgs{Statement: "DELETE FROM t"})
	if !res.IsError || resultText(res) != "query refused: not a SELECT" {
		t.Errorf("runner error: got %q, want its text unchanged", resultText(res))
	}
}

func TestSQLTool_rowCapTruncation(t *testing.T) {
	ans := oneSQLRow()
	ans.Result.Rows = [][]any{{json.Number("1")}, {json.Number("2")}}
	ans.Result.Truncated = true
	out := decodeSQLResult(t, mustSQL(t, ans, SQLArgs{Statement: "SELECT n FROM t", MaxRows: 2}))
	if out["truncated"] != true || out["row_count"] != float64(2) {
		t.Errorf("truncated=%v row_count=%v, want true and 2", out["truncated"], out["row_count"])
	}
	notice, _ := out["notice"].(string)
	if !strings.Contains(notice, "first 2 rows") || !strings.Contains(notice, "statement above") {
		t.Errorf("notice = %q, want it to count the rows and point at the statement", notice)
	}
}

func TestSQLTool_sizeBudgetCutsRows(t *testing.T) {
	big := strings.Repeat("x", 300<<10) // four of these pass 1 MiB
	ans := oneSQLRow()
	ans.Result.Rows = nil
	for range 5 {
		ans.Result.Rows = append(ans.Result.Rows, []any{big})
	}
	out := decodeSQLResult(t, mustSQL(t, ans, SQLArgs{Statement: "SELECT body FROM t"}))
	if out["truncated"] != true || out["row_count"] != float64(3) {
		t.Fatalf("truncated=%v row_count=%v, want true and 3", out["truncated"], out["row_count"])
	}
	if rows, _ := out["rows"].([]any); len(rows) != 3 {
		t.Errorf("rows = %d, want 3", len(rows))
	}
	if notice, _ := out["notice"].(string); !strings.Contains(notice, "1024 KiB") || !strings.Contains(notice, "statement above") {
		t.Errorf("notice = %q", notice)
	}

	// The budget is the whole answer, not the rows alone: with a long
	// statement echoed and rows that fill the budget exactly, what the
	// client reads still fits.
	long := "SELECT body FROM t -- " + strings.Repeat("c", SQLMaxStatementBytes-40)
	ans.Result.Rows = nil
	for range 64 {
		ans.Result.Rows = append(ans.Result.Rows, []any{strings.Repeat("z", 16<<10)})
	}
	res := mustSQL(t, ans, SQLArgs{Statement: long})
	if n := len(resultText(res)); n > SQLMaxResultBytes {
		t.Errorf("answer is %d bytes, over the %d budget", n, SQLMaxResultBytes)
	}
	if out := decodeSQLResult(t, res); out["truncated"] != true || out["statement"] != long {
		t.Errorf("truncated=%v, statement echoed=%v", out["truncated"], out["statement"] == long)
	}

	// A first row alone over the budget: zero rows, an empty list (never
	// null), and a notice that says to select less.
	ans.Result.Rows = [][]any{{strings.Repeat("y", SQLMaxResultBytes+1)}}
	out = decodeSQLResult(t, mustSQL(t, ans, SQLArgs{Statement: "SELECT body FROM t"}))
	rows, ok := out["rows"].([]any)
	if !ok || len(rows) != 0 || out["truncated"] != true {
		t.Fatalf("rows=%#v truncated=%v, want [] and true", out["rows"], out["truncated"])
	}
	if notice, _ := out["notice"].(string); !strings.Contains(notice, "fewer or narrower columns") {
		t.Errorf("notice = %q", notice)
	}
}

func TestSQLTool_emptyAnswerAndCopyTime(t *testing.T) {
	at := time.Date(2026, 10, 18, 3, 0, 0, 0, time.UTC)
	out := decodeSQLResult(t, mustSQL(t, SQLAnswer{CopyUpdatedAt: &at, OlderCopy: "answered from an earlier copy"}, SQLArgs{Statement: "SELECT 1 WHERE false"}))
	if rows, ok := out["rows"].([]any); !ok || len(rows) != 0 {
		t.Errorf("rows = %#v, want []", out["rows"])
	}
	if cols, ok := out["columns"].([]any); !ok || len(cols) != 0 {
		t.Errorf("columns = %#v, want []", out["columns"])
	}
	if out["truncated"] != false {
		t.Errorf("truncated = %v, want false", out["truncated"])
	}
	if _, has := out["notice"]; has {
		t.Errorf("notice on an uncut answer: %v", out["notice"])
	}
	if out["copy_updated_at"] != "2026-10-18T03:00:00Z" || out["older_copy"] != "answered from an earlier copy" {
		t.Errorf("copy_updated_at=%v older_copy=%v", out["copy_updated_at"], out["older_copy"])
	}

	out = decodeSQLResult(t, mustSQL(t, oneSQLRow(), SQLArgs{Statement: "SELECT 1"}))
	if v, has := out["copy_updated_at"]; !has || v != nil {
		t.Errorf("copy_updated_at = %v (present=%v), want an explicit null when there is no snapshot", v, has)
	}
}

func TestSQLTool_auditRecordsTheStatementAndServer(t *testing.T) {
	rec := audittest.Install(t)
	cfg := sqlToolConfig(&Target{RunSQL: stubSQL(oneSQLRow(), nil), ServerID: "s1"})
	cfg.AuditSurface = "console-mcp"
	decodeSQLResult(t, mustSQLCfg(t, cfg, SQLArgs{Statement: "SELECT n FROM t"}))
	evs := rec.Events()
	if len(evs) != 1 {
		t.Fatalf("recorded %d events, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Surface != "console-mcp" || ev.Action != "sql.run" {
		t.Errorf("pair = %s/%s", ev.Surface, ev.Action)
	}
	if ev.Detail["sql"] != "SELECT n FROM t" || ev.Detail["rows"] != "1" || ev.Detail["truncated"] != "false" || ev.Detail["server"] != "s1" {
		t.Errorf("detail = %v", ev.Detail)
	}
}

func TestNewServer_sqlToolIsOptIn(t *testing.T) {
	names := func(cfg Config) map[string]*mcp.Tool {
		t.Helper()
		ctx := context.Background()
		st, ct := mcp.NewInMemoryTransports()
		srv := NewServer(cfg)
		ss, err := srv.Connect(ctx, st, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ss.Close()
		cs, err := mcp.NewClient(&mcp.Implementation{Name: "t"}, nil).Connect(ctx, ct, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		list, err := cs.ListTools(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]*mcp.Tool{}
		for _, tool := range list.Tools {
			m[tool.Name] = tool
		}
		return m
	}
	if _, ok := names(Config{})["sql"]; ok {
		t.Error("sql registered without Config.SQL")
	}
	tool, ok := names(Config{SQL: true, Servers: &ServerRouting{Names: []string{"prod"}}})["sql"]
	if !ok {
		t.Fatal("sql not registered with Config.SQL")
	}
	if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || !tool.Annotations.IdempotentHint {
		t.Error("sql must be annotated read-only and idempotent")
	}
	schema, _ := json.Marshal(tool.InputSchema)
	if !strings.Contains(string(schema), `"server"`) || !strings.Contains(string(schema), `"statement"`) {
		t.Errorf("routed input schema lacks server or statement: %s", schema)
	}
	if !strings.Contains(tool.Description, `"prod"`) {
		t.Error("routed description does not list the servers")
	}
}

func mustSQL(t *testing.T, ans SQLAnswer, args SQLArgs) *mcp.CallToolResult {
	t.Helper()
	return mustSQLCfg(t, sqlToolConfig(&Target{RunSQL: stubSQL(ans, nil)}), args)
}

func mustSQLCfg(t *testing.T, cfg Config, args SQLArgs) *mcp.CallToolResult {
	t.Helper()
	res, _, err := MakeSQLTool(cfg)(context.Background(), nil, args)
	if err != nil {
		t.Fatalf("handler returned a protocol error: %v", err)
	}
	return res
}
