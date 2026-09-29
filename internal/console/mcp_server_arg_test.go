package console

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── One /mcp connection reaches every registered server (#1434) ────────────
//
// Edge cases, written before the code:
//   - server omitted: the session's own server answers (the /mcp/{sel} path
//     choice, or the console default on bare /mcp), and nothing is appended;
//   - a registry NAME, a registry ID, and the reserved boot id "default" all
//     route, with the same rules as the /mcp/{sel} path;
//   - the hidden boot (source-less watch) is not addressable as "default";
//   - a name equal to ANOTHER server's id: the id wins, like the path;
//   - an unknown name is a tool error naming the choices, never a CLI flag;
//   - a server whose index will not connect: a tool error naming the server,
//     with no DSN password, and not cached (the retry fails the same way);
//   - the audit record names the ROUTED server, not the session's;
//   - a managed token's grants apply to the routed target unchanged;
//   - the tool descriptions list the server names; "default" only when the
//     boot entry is selectable.

type routedFixture struct {
	s                        *Server
	ts                       *httptest.Server
	prodID, stgID            string
	bootMock, prodMock, stgM sqlmock.Sqlmock
}

func newRoutedFixture(t *testing.T, hideBoot bool) *routedFixture {
	t.Helper()
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	prod, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(127.0.0.1:1)/idx_prod"})
	if err != nil {
		t.Fatal(err)
	}
	stg, err := reg.Add(ServerEntry{Name: "staging", DSN: "u:p@tcp(127.0.0.1:1)/idx_stg"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		token:        "t",
		cm:           newConnManager(reg, false),
		mcpTokenPath: filepath.Join(t.TempDir(), "mcp-token.yaml"),
		sessions:     newSessionStore(),
		loginLimiter: newLoginLimiter(),
	}
	s.managedTok.initFromDisk(s.mcpTokenPath, nil)
	f := &routedFixture{s: s, prodID: prod.ID, stgID: stg.ID}

	mockBundle := func(dbName string) (*bundle, sqlmock.Sqlmock) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return &bundle{db: db, dbName: dbName, engine: query.New(db), noArchive: true}, mock
	}
	var b *bundle
	s.cm.boot, f.bootMock = mockBundle("idx_boot")
	s.cm.hideBoot = hideBoot
	// Seeded as already-open bundles, so routing is proven without a MySQL:
	// which mock receives the query is which server answered.
	b, f.prodMock = mockBundle("idx_prod")
	s.cm.bundles[prod.ID] = b
	b, f.stgM = mockBundle("idx_stg")
	s.cm.bundles[stg.ID] = b

	s.mux = s.buildHandler()
	f.ts = httptest.NewServer(s.mux)
	t.Cleanup(f.ts.Close)
	return f
}

func expectSchemaChanges(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("FROM schema_changes").WillReturnRows(sqlmock.NewRows([]string{
		"id", "detected_at", "schema_name", "table_name", "ddl_type", "ddl_query",
		"binlog_file", "binlog_pos", "gtid", "snapshot_id",
	}))
}

func routedCall(t *testing.T, session *mcp.ClientSession, tool string, args map[string]any) (*mcp.CallToolResult, []string) {
	t.Helper()
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool %s %v: %v", tool, args, err)
	}
	var texts []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			texts = append(texts, tc.Text)
		}
	}
	return res, texts
}

func assertMet(t *testing.T, name string, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("%s: %v", name, err)
	}
}

func TestMCPServerArg_omittedUsesSessionServer(t *testing.T) {
	f := newRoutedFixture(t, false)
	expectSchemaChanges(f.prodMock)
	session := mcpGrantConnect(t, f.ts.URL+"/mcp/prod", "t")
	res, texts := routedCall(t, session, "list_schema_changes", map[string]any{})
	if res.IsError {
		t.Fatalf("omitted server failed: %v", texts)
	}
	if len(texts) != 1 {
		t.Errorf("omitted server must not append an attribution block: %v", texts)
	}
	assertMet(t, "prod", f.prodMock)
}

func TestMCPServerArg_routesByNameIDAndDefault(t *testing.T) {
	f := newRoutedFixture(t, false)
	cases := []struct {
		arg      string
		mock     sqlmock.Sqlmock
		answered string
	}{
		{"staging", f.stgM, "staging"},
		{f.stgID, f.stgM, "staging"}, // an id is attributed by the name a reader knows
		{"default", f.bootMock, "default"},
		{" prod ", f.prodMock, "prod"},
	}
	// Session opened on prod: every routed call goes somewhere else, or
	// explicitly back to prod, and never falls through to the session's.
	session := mcpGrantConnect(t, f.ts.URL+"/mcp/prod", "t")
	for _, tc := range cases {
		expectSchemaChanges(tc.mock)
		res, texts := routedCall(t, session, "list_schema_changes", map[string]any{"server": tc.arg})
		if res.IsError {
			t.Errorf("server %q: %v", tc.arg, texts)
			continue
		}
		if len(texts) != 2 || texts[1] != "Answered by server: "+tc.answered {
			t.Errorf("server %q: want attribution to %q, got %v", tc.arg, tc.answered, texts)
		}
		assertMet(t, tc.arg, tc.mock)
	}
}

func TestMCPServerArg_hiddenBootIsNotDefault(t *testing.T) {
	f := newRoutedFixture(t, true)
	session := mcpGrantConnect(t, f.ts.URL+"/mcp", "t")
	res, texts := routedCall(t, session, "list_schema_changes", map[string]any{"server": "default"})
	if !res.IsError || !strings.Contains(strings.Join(texts, " "), "unknown server") {
		t.Errorf("hidden boot must not be addressable as default, got IsError=%v %v", res.IsError, texts)
	}
}

func TestMCPServerArg_unknownNameIsToolError(t *testing.T) {
	f := newRoutedFixture(t, false)
	session := mcpGrantConnect(t, f.ts.URL+"/mcp", "t")
	for _, arg := range []string{"nope", "PROD", "prod\nstaging"} {
		res, texts := routedCall(t, session, "list_schema_changes", map[string]any{"server": arg})
		text := strings.Join(texts, "\n")
		if !res.IsError {
			t.Errorf("server %q: want a tool error, got %v", arg, texts)
			continue
		}
		if !strings.Contains(text, "unknown server") || !strings.Contains(text, `"prod"`) || !strings.Contains(text, `"staging"`) {
			t.Errorf("server %q: error must say unknown and list the names, got %q", arg, text)
		}
		if strings.Contains(text, "--") {
			t.Errorf("server %q: an MCP error must not name a CLI flag: %q", arg, text)
		}
		if strings.Contains(text, "Answered by") {
			t.Errorf("server %q: nothing answered, got %q", arg, text)
		}
	}
}

func TestMCPServerArg_idBeatsSameName(t *testing.T) {
	f := newRoutedFixture(t, false)
	// A third server whose display NAME is staging's ID: the path selector
	// matches ids first, and the argument must agree with it.
	if _, err := f.s.cm.reg.Add(ServerEntry{Name: f.stgID, DSN: "u:p@tcp(127.0.0.1:1)/idx_x"}); err != nil {
		t.Fatal(err)
	}
	expectSchemaChanges(f.stgM)
	session := mcpGrantConnect(t, f.ts.URL+"/mcp/prod", "t")
	res, texts := routedCall(t, session, "list_schema_changes", map[string]any{"server": f.stgID})
	if res.IsError || len(texts) != 2 || texts[1] != "Answered by server: staging" {
		t.Errorf("id must win over a same-named server, got IsError=%v %v", res.IsError, texts)
	}
	assertMet(t, "staging", f.stgM)
}

func TestMCPServerArg_unreachableServer(t *testing.T) {
	f := newRoutedFixture(t, false)
	// Port 1 on loopback refuses at once; not seeded, so it really dials.
	if _, err := f.s.cm.reg.Add(ServerEntry{Name: "broken", DSN: "u:hunter2secret@tcp(127.0.0.1:1)/idx_broken?timeout=2s"}); err != nil {
		t.Fatal(err)
	}
	session := mcpGrantConnect(t, f.ts.URL+"/mcp/prod", "t")
	for attempt := range 2 {
		res, texts := routedCall(t, session, "list_schema_changes", map[string]any{"server": "broken"})
		text := strings.Join(texts, "\n")
		if !res.IsError {
			t.Fatalf("attempt %d: unreachable server must be a tool error, got %v", attempt, texts)
		}
		if !strings.Contains(text, `server "broken"`) {
			t.Errorf("attempt %d: error must name the server, got %q", attempt, text)
		}
		if strings.Contains(text, "hunter2secret") {
			t.Errorf("attempt %d: error leaked the DSN password: %q", attempt, text)
		}
		if strings.Contains(text, "Answered by") {
			t.Errorf("attempt %d: nothing answered, got %q", attempt, text)
		}
	}
}

func TestMCPServerArg_descriptionsListNames(t *testing.T) {
	for _, hide := range []bool{false, true} {
		f := newRoutedFixture(t, hide)
		session := mcpGrantConnect(t, f.ts.URL+"/mcp", "t")
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(tools.Tools) != len(mcpToolPerms) {
			t.Errorf("hide=%v: %d tools, want %d (the argument adds no tool)", hide, len(tools.Tools), len(mcpToolPerms))
		}
		for _, tool := range tools.Tools {
			if !strings.Contains(tool.Description, `"prod"`) || !strings.Contains(tool.Description, `"staging"`) {
				t.Errorf("hide=%v %s: description does not list the servers: %q", hide, tool.Name, tool.Description)
			}
			if got := strings.Contains(tool.Description, `"default"`); got == hide {
				t.Errorf("hide=%v %s: lists \"default\" = %v, want %v", hide, tool.Name, got, !hide)
			}
			raw, _ := json.Marshal(tool.InputSchema)
			if !strings.Contains(string(raw), `"server"`) {
				t.Errorf("hide=%v %s: schema has no server property", hide, tool.Name)
			}
		}
	}
}

// TestMCPServerArg_auditTargetIsRoutedServer drives the query tool (an
// audited data read) and asserts the record names the routed server, the
// session's own server when the argument is omitted, and the console default
// on bare /mcp.
func TestMCPServerArg_auditTargetIsRoutedServer(t *testing.T) {
	rec := audittest.Install(t)
	f := newRoutedFixture(t, false)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	expectRow := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows(cols).AddRow(
			int64(1), "bin.000001", int64(4), int64(40), ts,
			nil, nil, "app", "users", int64(parser.EventInsert), "42",
			nil, nil, []byte(`{"id":42}`), int64(0), nil, nil, nil,
		))
	}
	cases := []struct {
		path, arg  string
		mock       sqlmock.Sqlmock
		wantServer string
	}{
		{"/mcp/prod", "staging", f.stgM, f.stgID},
		{"/mcp/prod", "", f.prodMock, f.prodID},
		{"/mcp", "", f.bootMock, bootServerID},
		{"/mcp", "prod", f.prodMock, f.prodID},
	}
	for _, tc := range cases {
		rec.Reset()
		expectRow(tc.mock)
		session := mcpGrantConnect(t, f.ts.URL+tc.path, "t")
		args := map[string]any{"schema": "app", "table": "users"}
		if tc.arg != "" {
			args["server"] = tc.arg
		}
		res, texts := routedCall(t, session, "query", args)
		if res.IsError {
			t.Fatalf("%s server=%q: %v", tc.path, tc.arg, texts)
		}
		var got []ext.AuditEvent
		for _, ev := range rec.Events() {
			if ev.Action == "query.run" {
				got = append(got, ev)
			}
		}
		if len(got) != 1 {
			t.Fatalf("%s server=%q: %d query.run records, want 1", tc.path, tc.arg, len(got))
		}
		if s := got[0].Detail["server"]; s != tc.wantServer {
			t.Errorf("%s server=%q: audit server = %q, want %q", tc.path, tc.arg, s, tc.wantServer)
		}
		assertMet(t, tc.path+" "+tc.arg, tc.mock)
	}
}

// TestMCPServerArg_grantsApplyToTarget: a managed token's grant cap decides
// the call before routing, so pointing it at another server neither widens
// nor narrows what it may do.
func TestMCPServerArg_grantsApplyToTarget(t *testing.T) {
	f := newRoutedFixture(t, false)
	mint := func(perms ...ext.Permission) string {
		t.Helper()
		sess, _, err := f.s.sessions.IssueWithPolicy("minter", &ext.AccessPolicy{Permissions: append([]ext.Permission{ext.PermSettingsRead}, perms...)})
		if err != nil {
			t.Fatal(err)
		}
		rec := doJSON(t, f.s, "POST", "/api/mcp-token", sess)
		if rec.Code != 200 {
			t.Fatalf("mint = %d: %s", rec.Code, rec.Body.String())
		}
		var minted struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil || minted.Token == "" {
			t.Fatalf("mint response: %v (%s)", err, rec.Body.String())
		}
		return minted.Token
	}

	viewer := mcpGrantConnect(t, f.ts.URL+"/mcp/prod", mint())
	res, texts := routedCall(t, viewer, "list_schema_changes", map[string]any{"server": "staging"})
	if !res.IsError || !strings.Contains(strings.Join(texts, " "), "forbidden") {
		t.Errorf("settings:read token routed to staging: want forbidden, got %v", texts)
	}

	expectSchemaChanges(f.stgM)
	analyst := mcpGrantConnect(t, f.ts.URL+"/mcp/prod", mint(ext.PermQueryExecute))
	res, texts = routedCall(t, analyst, "list_schema_changes", map[string]any{"server": "staging"})
	if res.IsError || len(texts) != 2 || texts[1] != "Answered by server: staging" {
		t.Errorf("query:execute token routed to staging: want an answer from staging, got IsError=%v %v", res.IsError, texts)
	}
	assertMet(t, "staging", f.stgM)
	res, texts = routedCall(t, analyst, "recover", map[string]any{"server": "staging"})
	if !res.IsError || !strings.Contains(strings.Join(texts, " "), "recover:execute") {
		t.Errorf("query-only token routed to staging must stay forbidden recover, got %v", texts)
	}
}
