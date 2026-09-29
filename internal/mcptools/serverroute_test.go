package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// ─── Optional server argument (#1434) ────────────────────────────────────────
//
// Edge cases, written before the code:
//   - no routing configured (standalone): no `server` property anywhere, tool
//     descriptions unchanged, audit Detail carries no "server" key;
//   - routing configured: every core tool takes an optional `server`, the
//     schema still refuses unknown properties (recover keeps refusing
//     changed_column), the tool count does not change;
//   - server omitted, "" or only spaces: the call runs exactly as before (no
//     request in the context, no attribution line);
//   - " prod " is trimmed to "prod";
//   - a named call that resolves: one extra content block AFTER the tool's
//     own, naming the server that answered; block 0 is byte-identical;
//   - a named call that fails to resolve: the error, and no attribution
//     (nothing answered);
//   - a server name with a newline or quote cannot break the description
//     onto a new line.

// connectRouted opens an in-memory MCP session over NewServer(cfg).
func connectRouted(t *testing.T, cfg Config) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	clientT, serverT := mcp.NewInMemoryTransports()
	ss, err := NewServer(cfg).Connect(ctx, serverT, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

func listToolMap(t *testing.T, cs *mcp.ClientSession) map[string]*mcp.Tool {
	t.Helper()
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	out := map[string]*mcp.Tool{}
	for _, tool := range res.Tools {
		out[tool.Name] = tool
	}
	return out
}

// schemaOf decodes a listed tool's input schema into a plain map.
func schemaOf(t *testing.T, tool *mcp.Tool) map[string]any {
	t.Helper()
	raw, err := json.Marshal(tool.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func schemaProps(t *testing.T, tool *mcp.Tool) map[string]any {
	t.Helper()
	props, _ := schemaOf(t, tool)["properties"].(map[string]any)
	return props
}

func allToolsConfig(resolve ResolveTarget) Config {
	return Config{Resolve: resolve, Reconstruct: true, RecoverCascade: true}
}

func TestServerArg_standaloneSchemaUnchanged(t *testing.T) {
	cs := connectRouted(t, allToolsConfig(func(context.Context, string) (*Target, error) {
		return nil, errors.New("unused")
	}))
	tools := listToolMap(t, cs)
	if len(tools) != 6 {
		t.Fatalf("standalone tool count = %d, want 6", len(tools))
	}
	for name, tool := range tools {
		if _, ok := schemaProps(t, tool)["server"]; ok {
			t.Errorf("%s: standalone schema grew a server property", name)
		}
		if strings.Contains(tool.Description, "server argument") || strings.Contains(tool.Description, "Servers:") {
			t.Errorf("%s: standalone description mentions server routing: %q", name, tool.Description)
		}
	}
}

func TestServerArg_routedSchema(t *testing.T) {
	cfg := allToolsConfig(func(context.Context, string) (*Target, error) { return nil, errors.New("unused") })
	cfg.Servers = &ServerRouting{Names: []string{"prod", "stag\"ing\nnext"}}
	tools := listToolMap(t, connectRouted(t, cfg))
	if len(tools) != 6 {
		t.Fatalf("routed tool count = %d, want 6 (the server argument must not add tools)", len(tools))
	}
	for name, tool := range tools {
		s := schemaOf(t, tool)
		props, _ := s["properties"].(map[string]any)
		if _, ok := props["server"]; !ok {
			t.Errorf("%s: routed schema has no server property", name)
		}
		if req, _ := s["required"].([]any); len(req) > 0 {
			for _, r := range req {
				if r == "server" {
					t.Errorf("%s: server must be optional", name)
				}
			}
		}
		if ap, ok := s["additionalProperties"]; !ok || ap == true {
			t.Errorf("%s: routed schema must still refuse unknown properties, additionalProperties = %v", name, ap)
		}
		if _, ok := props["index_dsn"]; !ok {
			t.Errorf("%s: routed schema lost the embedded tool parameters (no index_dsn)", name)
		}
		if !strings.Contains(tool.Description, `"prod"`) {
			t.Errorf("%s: description does not list the server names: %q", name, tool.Description)
		}
		if strings.Contains(tool.Description, "\nnext") {
			t.Errorf("%s: a name with a newline broke the description: %q", name, tool.Description)
		}
	}
	if _, ok := schemaProps(t, tools["recover"])["changed_column"]; ok {
		t.Error("routed recover schema exposes changed_column (#962)")
	}
}

// TestServerArg_routedSchemaRejectsUnknownProperty drives a real call: an
// unknown property must still be refused by schema validation on a routed
// surface, exactly as on the standalone one.
func TestServerArg_routedSchemaRejectsUnknownProperty(t *testing.T) {
	cfg := allToolsConfig(func(context.Context, string) (*Target, error) {
		t.Error("Resolve reached despite an invalid argument")
		return nil, errors.New("unused")
	})
	cfg.Servers = &ServerRouting{Names: []string{"prod"}}
	cs := connectRouted(t, cfg)
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "recover",
		Arguments: map[string]any{"server": "prod", "changed_column": "x"},
	})
	if err == nil && (res == nil || !res.IsError) {
		t.Fatal("recover with changed_column on a routed surface was accepted")
	}
}

// schemaChangesTarget is a Target over a sqlmock that answers the
// list_schema_changes query with no rows.
func schemaChangesTarget(t *testing.T, id, name string) (*Target, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return &Target{DB: db, ServerID: id, ServerName: name}, mock
}

func expectNoSchemaChanges(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("FROM schema_changes").WillReturnRows(sqlmock.NewRows([]string{
		"id", "detected_at", "schema_name", "table_name", "ddl_type", "ddl_query",
		"binlog_file", "binlog_pos", "gtid", "snapshot_id",
	}))
}

func callSchemaChanges(t *testing.T, cs *mcp.ClientSession, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "list_schema_changes", Arguments: args})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	return res
}

func allText(res *mcp.CallToolResult) []string {
	var out []string
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			out = append(out, tc.Text)
		}
	}
	return out
}

func TestServerArg_omittedOrBlankRunsAsBefore(t *testing.T) {
	for _, args := range []map[string]any{{}, {"server": ""}, {"server": "   "}} {
		target, mock := schemaChangesTarget(t, "id-prod", "prod")
		expectNoSchemaChanges(mock)
		var gotReq string
		cfg := Config{Resolve: func(ctx context.Context, _ string) (*Target, error) {
			gotReq = RequestedServer(ctx)
			return target, nil
		}}
		cfg.Servers = &ServerRouting{Names: []string{"prod"}}
		res := callSchemaChanges(t, connectRouted(t, cfg), args)
		if res.IsError {
			t.Fatalf("args %v: %v", args, allText(res))
		}
		if gotReq != "" {
			t.Errorf("args %v: RequestedServer = %q, want empty (omitted)", args, gotReq)
		}
		if n := len(res.Content); n != 1 {
			t.Errorf("args %v: %d content blocks, want 1 (no attribution when server is omitted): %v", args, n, allText(res))
		}
	}
}

func TestServerArg_namedCallIsAttributed(t *testing.T) {
	target, mock := schemaChangesTarget(t, "id-stg", "staging")
	expectNoSchemaChanges(mock)
	var gotReq string
	cfg := Config{Resolve: func(ctx context.Context, _ string) (*Target, error) {
		gotReq = RequestedServer(ctx)
		return target, nil
	}}
	cfg.Servers = &ServerRouting{Names: []string{"prod", "staging"}}
	cs := connectRouted(t, cfg)

	res := callSchemaChanges(t, cs, map[string]any{"server": " staging "})
	if res.IsError {
		t.Fatalf("named call failed: %v", allText(res))
	}
	if gotReq != "staging" {
		t.Errorf("RequestedServer = %q, want the trimmed name %q", gotReq, "staging")
	}
	texts := allText(res)
	if len(texts) != 2 {
		t.Fatalf("content blocks = %d, want the tool's own plus one attribution: %v", len(texts), texts)
	}
	if strings.Contains(texts[0], "staging") {
		t.Errorf("block 0 must stay the tool's own output, got %q", texts[0])
	}
	if texts[1] != "Answered by server: staging" {
		t.Errorf("attribution = %q", texts[1])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestServerArg_failedResolveIsNotAttributed(t *testing.T) {
	cfg := Config{Resolve: func(ctx context.Context, _ string) (*Target, error) {
		return nil, errors.New(`unknown server "nope"`)
	}}
	cfg.Servers = &ServerRouting{Names: []string{"prod"}}
	res := callSchemaChanges(t, connectRouted(t, cfg), map[string]any{"server": "nope"})
	if !res.IsError {
		t.Fatal("a failed resolve must be a tool error")
	}
	texts := allText(res)
	if len(texts) != 1 || strings.Contains(texts[0], "Answered by") {
		t.Errorf("a failed resolve must not claim a server answered: %v", texts)
	}
}

// TestServerArg_auditCarriesRoutedServer: the query tool's audit record names
// the server the Target came from, and a Target without one (standalone)
// records no server key at all.
func TestServerArg_auditCarriesRoutedServer(t *testing.T) {
	rec := audittest.Install(t)
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	run := func(t *testing.T, serverID string) map[string]string {
		t.Helper()
		rec.Reset()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(
			sqlmock.NewRows(recoverToolMockCols).AddRow(
				int64(1), "bin.000001", int64(4), int64(40), ts,
				nil, nil, "app", "users", int64(parser.EventInsert), "42",
				nil, nil, []byte(`{"id":42}`), int64(0), nil, nil, nil,
			))
		cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
			return &Target{DB: db, NoArchive: true, ResolverLoaded: true, ServerID: serverID}, nil
		}}
		res, _, _ := MakeQueryTool(cfg)(context.Background(), nil, QueryArgs{Schema: "app", Table: "users"})
		if res.IsError {
			t.Fatalf("query failed: %s", resultText(res))
		}
		evs := rec.Events()
		if len(evs) != 1 {
			t.Fatalf("audit events = %d, want 1", len(evs))
		}
		return evs[0].Detail
	}
	if got := run(t, "id-stg")["server"]; got != "id-stg" {
		t.Errorf("audit server = %q, want the routed id", got)
	}
	if d := run(t, ""); d != nil {
		if _, has := d["server"]; has {
			t.Errorf("a Target without a server must not add a server key, got %v", d)
		}
	}
}
