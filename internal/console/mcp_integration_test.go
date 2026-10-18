//go:build integration

package console

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// bearerTransport injects the console token into every MCP HTTP request.
type bearerTransport struct{ token string }

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(req)
}

// mcpConnect opens a real Streamable HTTP MCP session against endpoint.
func mcpConnect(t *testing.T, endpoint, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "it", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Transport: bearerTransport{token}},
	}, nil)
	if err != nil {
		t.Fatalf("MCP client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func mcpToolText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		return ""
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("expected *mcp.TextContent, got %T", res.Content[0])
	}
	return tc.Text
}

// seedMCPConsole seeds an index with an INSERT+UPDATE on app.users pk=1,
// stamps captured statement text on every event (the redaction subject), and
// returns a token-authenticated console over it.
func seedMCPConsole(t *testing.T) (*Server, *sql.DB, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"name":"alice"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:05:00", nil,
		"app", "users", 2 /*UPDATE*/, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"alicia"}`))

	// Captured statement text — present in the index, redacted by the console
	// read boundary (the events API's eventDTO omits it, #699).
	if _, err := db.Exec(`UPDATE binlog_events SET query_text = 'UPDATE users SET name = ''SECRET_LITERAL''', query_hash = 'cafe0199'`); err != nil {
		t.Fatalf("stamp query_text: %v", err)
	}

	srv, err := New(Config{
		DB:        db,
		DBName:    dbName,
		Listen:    "127.0.0.1:8090",
		Token:     intToken,
		NoArchive: true,
		// Keep the managed-token file inside the test dir: the grant tests
		// mint through the real handler, which must never write to $HOME.
		MCPTokenPath: filepath.Join(t.TempDir(), "mcp-token.yaml"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, db, dbName
}

func TestIntegrationMCPEndpoint(t *testing.T) {
	srv, _, _ := seedMCPConsole(t)
	ts := httptest.NewServer(srv.Handler())
	// Cleanup, not defer: the streamable client holds a standalone SSE GET open
	// for the session lifetime, and httptest Close blocks until every
	// connection drains. Registering ts.Close BEFORE the session connects makes
	// the LIFO cleanup order close the session (registered later, inside
	// mcpConnect) first.
	t.Cleanup(ts.Close)
	ctx := context.Background()

	session := mcpConnect(t, ts.URL+"/mcp", intToken)

	// Tool listing: exactly the read-only tools, including reconstruct (#953 —
	// registered whenever the console serves MCP; the per-server baseline gate
	// is enforced on the CALL, mirroring /api/reconstruct's 404).
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	names := map[string]bool{}
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"query", "recover", "recover_cascade", "status", "list_schema_changes", "reconstruct", "sql"} {
		if !names[want] {
			t.Errorf("tool %q not listed; got %v", want, names)
		}
	}
	if len(tools.Tools) != 7 {
		t.Errorf("expected 7 tools, got %d", len(tools.Tools))
	}

	// This console was seeded with NoArchive (no baseline), so the per-server
	// gate must refuse — and the parameters that would let a client point the
	// console at arbitrary storage must be refused too.
	for name, args := range map[string]map[string]any{
		"ungated": {"schema": "app", "table": "users", "pk": "1"},
		"baseline_dir param": {"schema": "app", "table": "users", "pk": "1",
			"baseline_dir": t.TempDir()},
	} {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "reconstruct", Arguments: args})
		if err != nil {
			t.Fatalf("CallTool reconstruct (%s): %v", name, err)
		}
		if !res.IsError {
			t.Errorf("reconstruct (%s): expected a tool error, got %s", name, mcpToolText(t, res))
		}
	}

	// Query round-trip: both events come back, statement text does not.
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users"},
	})
	if err != nil {
		t.Fatalf("CallTool query: %v", err)
	}
	text := mcpToolText(t, res)
	if res.IsError {
		t.Fatalf("query IsError: %s", text)
	}
	if !strings.Contains(text, "alicia") {
		t.Errorf("query result missing row data: %s", text)
	}
	if strings.Contains(text, "SECRET_LITERAL") || strings.Contains(text, "cafe0199") {
		t.Errorf("query result leaks statement text the console API redacts: %s", text)
	}

	// index_dsn is rejected: the console routes connections itself.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"index_dsn": "u:p@tcp(127.0.0.1:13306)/other"},
	})
	if err != nil {
		t.Fatalf("CallTool query(index_dsn): %v", err)
	}
	if !res.IsError || !strings.Contains(mcpToolText(t, res), "index_dsn is not accepted") {
		t.Errorf("index_dsn must be rejected, got: %s", mcpToolText(t, res))
	}

	// The console events cap (1000) applies as the query ceiling.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users", "limit": 5000},
	})
	if err != nil {
		t.Fatalf("CallTool query(limit): %v", err)
	}
	if !strings.Contains(mcpToolText(t, res), "ceiling of 1000") {
		t.Errorf("oversized limit must be capped at the console events cap: %s", mcpToolText(t, res))
	}

	// Recover round-trip: reversal SQL for the UPDATE restores "alice".
	res, err = session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "recover",
		Arguments: map[string]any{"schema": "app", "table": "users", "pk": "1"},
	})
	if err != nil {
		t.Fatalf("CallTool recover: %v", err)
	}
	text = mcpToolText(t, res)
	if res.IsError {
		t.Fatalf("recover IsError: %s", text)
	}
	if !strings.Contains(text, "alice") {
		t.Errorf("recover script missing reversal data: %s", text)
	}

	// Status round-trip.
	res, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "status", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool status: %v", err)
	}
	if res.IsError {
		t.Fatalf("status IsError: %s", mcpToolText(t, res))
	}
	if mcpToolText(t, res) == "" {
		t.Error("status returned empty text")
	}
}

func TestIntegrationMCPWrongTokenRefused(t *testing.T) {
	srv, _, _ := seedMCPConsole(t)
	ts := httptest.NewServer(srv.Handler())
	// Cleanup, not defer: the streamable client holds a standalone SSE GET open
	// for the session lifetime, and httptest Close blocks until every
	// connection drains. Registering ts.Close BEFORE the session connects makes
	// the LIFO cleanup order close the session (registered later, inside
	// mcpConnect) first.
	t.Cleanup(ts.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "it", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   ts.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearerTransport{"not-the-token"}},
	}, nil)
	if err == nil {
		session.Close()
		t.Fatal("MCP connect with a wrong token must fail")
	}
}

func TestIntegrationMCPRoutesByServerName(t *testing.T) {
	srv, _, dbName := seedMCPConsole(t)
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "reg-target", DSN: testutil.SnapshotDSN(dbName), NoArchive: true}); err != nil {
		t.Fatal(err)
	}
	// Rebuild the console over the registry (same index behind both names).
	srv.cm.reg = reg

	ts := httptest.NewServer(srv.Handler())
	// Cleanup, not defer: the streamable client holds a standalone SSE GET open
	// for the session lifetime, and httptest Close blocks until every
	// connection drains. Registering ts.Close BEFORE the session connects makes
	// the LIFO cleanup order close the session (registered later, inside
	// mcpConnect) first.
	t.Cleanup(ts.Close)
	ctx := context.Background()

	session := mcpConnect(t, ts.URL+"/mcp/reg-target", intToken)
	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users"},
	})
	if err != nil {
		t.Fatalf("CallTool via /mcp/reg-target: %v", err)
	}
	if res.IsError {
		t.Fatalf("query via named server IsError: %s", mcpToolText(t, res))
	}
	if !strings.Contains(mcpToolText(t, res), "alicia") {
		t.Errorf("named-server query missing row data: %s", mcpToolText(t, res))
	}
}

// mintManagedMCPToken POSTs /api/mcp-token over real HTTP with bearer and
// returns the minted plaintext.
func mintManagedMCPToken(t *testing.T, ts *httptest.Server, bearer string) string {
	t.Helper()
	req, err := http.NewRequest("POST", ts.URL+"/api/mcp-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("mint MCP token: %v", err)
	}
	defer resp.Body.Close()
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&minted); err != nil || resp.StatusCode != 200 || minted.Token == "" {
		t.Fatalf("mint MCP token = %d (decode err %v)", resp.StatusCode, err)
	}
	return minted.Token
}

// A trailing slash is the most common hand-edit a pasted address suffers,
// and before the {$} routes in server.go existed, "/mcp/" fell through to the SPA
// catch-all: the bridge received the console's HTML page with a 200 and
// died on "unsupported content type" (observed live, 2026-08-23). The
// discriminating claim is that these paths reach the MCP handler (401 with
// a bad token, JSON-family content) and never the asset handler (200 html).
func TestIntegrationMCPTrailingSlashRoutes(t *testing.T) {
	srv, _, dbName := seedMCPConsole(t)
	reg, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "reg-target", DSN: testutil.SnapshotDSN(dbName), NoArchive: true}); err != nil {
		t.Fatal(err)
	}
	srv.cm.reg = reg

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	for _, path := range []string{"/mcp/", "/mcp/reg-target/"} {
		req, err := http.NewRequest(http.MethodPost, ts.URL+path,
			strings.NewReader(`{"jsonrpc":"2.0","id":0,"method":"initialize","params":{}}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer wrong-token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("POST %s with a bad token: status %d, want 401 (the MCP handler); 200 means the SPA swallowed it", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); strings.Contains(ct, "text/html") {
			t.Errorf("POST %s answered %q — the web page, not the MCP endpoint", path, ct)
		}
	}

	// The positive half: a real session through the slashed default route.
	session := mcpConnect(t, ts.URL+"/mcp/", intToken)
	if _, err := session.ListTools(context.Background(), nil); err != nil {
		t.Fatalf("ListTools via /mcp/: %v", err)
	}
}

// TestIntegrationMCPTokenGrants pins #1124 end to end over the real HTTP /mcp
// endpoint: a managed token is capped at the permission grants of the session
// that minted it, so the MCP door and the /api door to the same data enforce
// the same permission model. A full-access mint (and, byte-identically, a
// token file from before grants were recorded) keeps the full read surface.
func TestIntegrationMCPTokenGrants(t *testing.T) {
	srv, _, _ := seedMCPConsole(t)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	ctx := context.Background()

	// A session holding ONLY settings:read may still mint (the mint gate is
	// unchanged) — but the minted token must be refused every core tool,
	// exactly as /api would refuse the minter itself.
	viewer, _, err := srv.sessions.IssueWithPolicy("settings-viewer",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}})
	if err != nil {
		t.Fatal(err)
	}
	tokA := mintManagedMCPToken(t, ts, viewer)
	sA := mcpConnect(t, ts.URL+"/mcp", tokA)
	for tool, perm := range map[string]string{
		"query":               "query:execute",
		"list_schema_changes": "query:execute",
		"recover":             "recover:execute",
		"reconstruct":         "reconstruct:execute",
		"status":              "status:read",
	} {
		res, err := sA.CallTool(ctx, &mcp.CallToolParams{
			Name:      tool,
			Arguments: map[string]any{"schema": "app", "table": "users", "pk": "1"},
		})
		if err != nil {
			t.Fatalf("CallTool %s (scoped token): %v", tool, err)
		}
		text := mcpToolText(t, res)
		if !res.IsError || !strings.Contains(text, perm) || !strings.Contains(text, "forbidden") {
			t.Errorf("%s with a settings:read-only token: want a forbidden error naming %s, got IsError=%v %q", tool, perm, res.IsError, text)
		}
	}

	// A minter holding query:execute mints a token that CAN query — and still
	// cannot reconstruct or recover (the per-tool cap, not all-or-nothing).
	// This rotate also invalidates tokA, so its assertions ran above.
	analyst, _, err := srv.sessions.IssueWithPolicy("analyst",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead, ext.PermQueryExecute}})
	if err != nil {
		t.Fatal(err)
	}
	tokB := mintManagedMCPToken(t, ts, analyst)
	sB := mcpConnect(t, ts.URL+"/mcp", tokB)
	res, err := sB.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users"},
	})
	if err != nil {
		t.Fatalf("CallTool query (analyst token): %v", err)
	}
	if res.IsError || !strings.Contains(mcpToolText(t, res), "alicia") {
		t.Errorf("analyst token query: want row data, got IsError=%v %q", res.IsError, mcpToolText(t, res))
	}
	res, err = sB.CallTool(ctx, &mcp.CallToolParams{
		Name:      "recover",
		Arguments: map[string]any{"schema": "app", "table": "users", "pk": "1"},
	})
	if err != nil {
		t.Fatalf("CallTool recover (analyst token): %v", err)
	}
	if !res.IsError || !strings.Contains(mcpToolText(t, res), "recover:execute") {
		t.Errorf("analyst token recover: want forbidden recover:execute, got IsError=%v %q", res.IsError, mcpToolText(t, res))
	}

	// A full-access mint (the static token has no session policy) records no
	// permission cap — the file has NO permissions field, which is exactly
	// the shape a pre-grants binary wrote (the legacy-token compatibility
	// contract: absent grants = full read surface).
	tokC := mintManagedMCPToken(t, ts, intToken)
	onDisk, err := os.ReadFile(srv.mcpTokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(onDisk), "permissions") {
		t.Fatalf("full-access mint must not record a permissions field (legacy shape), got: %s", onDisk)
	}
	sC := mcpConnect(t, ts.URL+"/mcp", tokC)
	res, err = sC.CallTool(ctx, &mcp.CallToolParams{
		Name:      "query",
		Arguments: map[string]any{"schema": "app", "table": "users"},
	})
	if err != nil {
		t.Fatalf("CallTool query (full token): %v", err)
	}
	if res.IsError || !strings.Contains(mcpToolText(t, res), "alicia") {
		t.Errorf("full-access token query: want row data, got IsError=%v %q", res.IsError, mcpToolText(t, res))
	}
	res, err = sC.CallTool(ctx, &mcp.CallToolParams{Name: "status", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool status (full token): %v", err)
	}
	if res.IsError {
		t.Errorf("full-access token status IsError: %s", mcpToolText(t, res))
	}
}
