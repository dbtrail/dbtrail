package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/ext"
)

// grantBearerTransport injects a bearer token into every MCP HTTP request.
// (The integration build has its own bearerTransport; both files compile into
// the package under -tags integration, hence the distinct name.)
type grantBearerTransport struct{ token string }

func (b grantBearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(req)
}

// mcpGrantConnect opens a real Streamable HTTP MCP session against endpoint.
func mcpGrantConnect(t *testing.T, endpoint, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "authz-test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Transport: grantBearerTransport{token}},
	}, nil)
	if err != nil {
		t.Fatalf("MCP client Connect: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func mcpGrantToolText(t *testing.T, res *mcp.CallToolResult) string {
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

// TestMCPToolPermsCoverCoreTools pins the tool→permission table against the
// tools the console actually registers: a core tool missing from mcpToolPerms
// would be misclassified into the extension bucket (PermExtViewRead), and a
// stale extra entry would gate a tool that no longer exists. Every mapped
// permission must also be one the core defines, mirroring the apiRoutePerms
// typo guard.
func TestMCPToolPermsCoverCoreTools(t *testing.T) {
	s := newManagedServer(t, "tok")
	// A SQL sandbox, so the opt-in sql tool (#2456) is listed with the rest.
	s.sqlRunner = &fakeSQLRunner{}
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)

	session := mcpGrantConnect(t, ts.URL+"/mcp", "tok")
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools.Tools) != len(mcpToolPerms) {
		t.Errorf("mcpToolPerms has %d entries, console registers %d tools — the two must track each other", len(mcpToolPerms), len(tools.Tools))
	}
	for _, tool := range tools.Tools {
		if _, ok := mcpToolPerms[tool.Name]; !ok {
			t.Errorf("core tool %q missing from mcpToolPerms — it would be misclassified as an extension tool", tool.Name)
		}
	}
	known := map[ext.Permission]bool{}
	for _, p := range ext.AllPermissions() {
		known[p] = true
	}
	for tool, perm := range mcpToolPerms {
		if !known[perm] {
			t.Errorf("mcpToolPerms[%q] = %q, which is not a permission the core defines", tool, perm)
		}
	}
}

// TestMCPManagedTokenGrantEnforcement drives #1124 through the real HTTP
// stack without a database: a settings:read-only session mints through the
// real handler (the mint gate is unchanged), and the minted token is refused
// every core tool BEFORE any index access — which is why no DB is needed for
// the denial path. A grant the minter does hold lets the call through the
// permission gate (it then fails on the fake DB, but never with the
// permission error).
func TestMCPManagedTokenGrantEnforcement(t *testing.T) {
	s := newManagedServer(t, "")
	// The sql tool registered (#2456): without a runner the loop below would
	// pass for sql only because the gate runs before the tool lookup.
	runner := &fakeSQLRunner{}
	s.sqlRunner = runner
	ts := httptest.NewServer(s.mux)
	t.Cleanup(ts.Close)
	ctx := context.Background()

	mint := func(bearer string) string {
		t.Helper()
		rec := doJSON(t, s, "POST", "/api/mcp-token", bearer)
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

	viewer, _, err := s.sessions.IssueWithPolicy("settings-viewer",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}})
	if err != nil {
		t.Fatal(err)
	}
	scopedTok := mint(viewer)

	session := mcpGrantConnect(t, ts.URL+"/mcp", scopedTok)
	for tool, perm := range mcpToolPerms {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{}})
		if err != nil {
			t.Fatalf("CallTool %s: %v", tool, err)
		}
		text := mcpGrantToolText(t, res)
		if !res.IsError || !strings.Contains(text, string(perm)) || !strings.Contains(text, "forbidden") {
			t.Errorf("%s with settings:read-only token: want forbidden naming %s, got IsError=%v %q", tool, perm, res.IsError, text)
		}
	}

	// A minter that holds query:execute mints a token that passes the query
	// grant gate (the call then fails on the fake DB — asserted to NOT be the
	// permission denial) while recover stays forbidden.
	analyst, _, err := s.sessions.IssueWithPolicy("analyst",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead, ext.PermQueryExecute}})
	if err != nil {
		t.Fatal(err)
	}
	analystTok := mint(analyst)
	session2 := mcpGrantConnect(t, ts.URL+"/mcp", analystTok)
	res, err := session2.CallTool(ctx, &mcp.CallToolParams{Name: "query", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool query (analyst): %v", err)
	}
	if strings.Contains(mcpGrantToolText(t, res), "forbidden") {
		t.Errorf("query with a query:execute token must pass the grant gate, got %q", mcpGrantToolText(t, res))
	}
	res, err = session2.CallTool(ctx, &mcp.CallToolParams{Name: "recover", Arguments: map[string]any{}})
	if err != nil {
		t.Fatalf("CallTool recover (analyst): %v", err)
	}
	if !res.IsError || !strings.Contains(mcpGrantToolText(t, res), "recover:execute") {
		t.Errorf("recover with a query-only token: want forbidden recover:execute, got %q", mcpGrantToolText(t, res))
	}
	// Free SQL needs sql:execute, as POST /api/sql does: a query-only token is
	// refused before a slot is taken.
	res, err = session2.CallTool(ctx, &mcp.CallToolParams{Name: "sql", Arguments: map[string]any{"statement": "SELECT 1"}})
	if err != nil {
		t.Fatalf("CallTool sql (analyst): %v", err)
	}
	if !res.IsError || !strings.Contains(mcpGrantToolText(t, res), "sql:execute") {
		t.Errorf("sql with a query-only token: want forbidden sql:execute, got %q", mcpGrantToolText(t, res))
	}
	if len(runner.users) != 0 {
		t.Errorf("a refused sql call reserved a slot: %v", runner.users)
	}
}

// TestManagedTokenGrants_FileRoundTrip pins the storage semantics of #1124 at
// the file layer: a scoped mint records the grant list; a full-access mint
// records NO permissions field (byte-identical to a pre-grants legacy file,
// which therefore keeps the full read surface); a present-but-empty list
// grants nothing (never collapses to "absent"); and matches() pairs the
// digest with the grants from the same refresh.
func TestManagedTokenGrants_FileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	var m managedMCPToken

	// Scoped mint: grants recorded, matches returns the capped policy.
	tok, f, err := GenerateMCPToken(path, []ext.Permission{ext.PermSettingsRead, ext.PermQueryExecute})
	if err != nil {
		t.Fatal(err)
	}
	m.initFromDisk(path, f)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "permissions") || !strings.Contains(string(raw), string(ext.PermQueryExecute)) {
		t.Fatalf("scoped mint must record its grants, got: %s", raw)
	}
	ok, pol := m.matches(tok)
	if !ok || pol == nil {
		t.Fatalf("scoped token: matches = (%v, %v), want a match with a policy", ok, pol)
	}
	if !pol.Allows(ext.PermQueryExecute) || pol.Allows(ext.PermRecoverExecute) {
		t.Errorf("scoped policy: query=%v recover=%v, want true/false", pol.Allows(ext.PermQueryExecute), pol.Allows(ext.PermRecoverExecute))
	}

	// Full-access mint (nil grants): no permissions field on disk — the
	// legacy shape — and a nil (full-access) policy. The per-check re-read
	// picks up the rotation without a reload call.
	tok2, _, err := GenerateMCPToken(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "permissions") {
		t.Fatalf("full-access mint must not record a permissions field, got: %s", raw)
	}
	ok, pol = m.matches(tok2)
	if !ok || pol != nil {
		t.Errorf("full-access token: matches = (%v, %v), want (true, nil)", ok, pol)
	}
	if ok, _ := m.matches(tok); ok {
		t.Error("rotated-away scoped token still matches")
	}

	// Present-but-empty grants nothing — and survives the YAML round trip as
	// EMPTY, never as absent (which would mean full access).
	tok3, _, err := GenerateMCPToken(path, []ext.Permission{})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadMCPTokenFile(path)
	if err != nil || loaded == nil || loaded.Permissions == nil || len(*loaded.Permissions) != 0 {
		t.Fatalf("empty grants must load as present-and-empty, got %+v (err %v)", loaded, err)
	}
	ok, pol = m.matches(tok3)
	if !ok || pol == nil {
		t.Fatalf("empty-grant token: matches = (%v, %v), want a match with a policy", ok, pol)
	}
	for _, p := range ext.AllPermissions() {
		if pol.Allows(p) {
			t.Errorf("empty-grant policy allows %q, want nothing", p)
		}
	}
}

// doMCPRaw posts one raw JSON-RPC message to path, optionally continuing an
// existing MCP session via its Mcp-Session-Id, and returns the recorder. Raw
// on purpose: the session-riding and non-tool-method tests need to send
// exactly the header/body combinations a well-behaved SDK client would not.
func doMCPRaw(t *testing.T, s *Server, path, token, sessionID, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090"+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if sessionID != "" {
		req.Header.Set("Mcp-Session-Id", sessionID)
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	return rec
}

const (
	mcpRawInitialize  = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`
	mcpRawInitialized = `{"jsonrpc":"2.0","method":"notifications/initialized"}`
	mcpRawPing        = `{"jsonrpc":"2.0","id":2,"method":"ping","params":{}}`
)

// TestMCPSessionBoundToCreatingCredential pins the session-continuation
// guard: an MCP session's grants are fixed at creation, so a session must
// only ever be continuable by the credential that created it — otherwise any
// holder of any valid token who learns a stronger session's Mcp-Session-Id
// would inherit its grants. The guard is the SDK's userID check, armed by
// the TokenInfo our verifier now populates.
func TestMCPSessionBoundToCreatingCredential(t *testing.T) {
	s := newManagedServer(t, "static-tok")

	// Managed token (full cap — irrelevant here; identity is what's tested).
	rec := doJSON(t, s, "POST", "/api/mcp-token", "static-tok")
	if rec.Code != 200 {
		t.Fatalf("mint = %d: %s", rec.Code, rec.Body.String())
	}
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil {
		t.Fatal(err)
	}

	// Create a session with the managed token.
	rec = doMCPRaw(t, s, "/mcp", minted.Token, "", mcpRawInitialize)
	if rec.Code != 200 {
		t.Fatalf("initialize with managed token = %d: %s", rec.Code, rec.Body.String())
	}
	sess := rec.Header().Get("Mcp-Session-Id")
	if sess == "" {
		t.Fatal("initialize response carries no Mcp-Session-Id")
	}

	// The static token is a VALID credential — but not the one that created
	// this session: continuation must be refused, not inherited.
	if rec := doMCPRaw(t, s, "/mcp", "static-tok", sess, mcpRawPing); rec.Code != http.StatusForbidden {
		t.Fatalf("session created by managed token continued by static token = %d, want 403 (session riding)", rec.Code)
	}
	// Positive control: the creating credential continues its own session.
	if rec := doMCPRaw(t, s, "/mcp", minted.Token, sess, mcpRawPing); rec.Code != 200 {
		t.Fatalf("creating credential refused its own session: %d %s", rec.Code, rec.Body.String())
	}

	// Rotation mints a new value = a new identity: the old session is
	// orphaned — the old token no longer authenticates at all, and the new
	// token must not be able to ride the session the old one created.
	rec = doJSON(t, s, "POST", "/api/mcp-token", "static-tok")
	if rec.Code != 200 {
		t.Fatalf("rotate = %d", rec.Code)
	}
	var rotated struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &rotated); err != nil {
		t.Fatal(err)
	}
	if rec := doMCPRaw(t, s, "/mcp", minted.Token, sess, mcpRawPing); rec.Code != http.StatusUnauthorized {
		t.Fatalf("rotated-away token still authenticates: %d", rec.Code)
	}
	if rec := doMCPRaw(t, s, "/mcp", rotated.Token, sess, mcpRawPing); rec.Code != http.StatusForbidden {
		t.Fatalf("rotated token continued the pre-rotation session = %d, want 403", rec.Code)
	}
	// And the new token creates its own session normally.
	if rec := doMCPRaw(t, s, "/mcp", rotated.Token, "", mcpRawInitialize); rec.Code != 200 {
		t.Fatalf("rotated token cannot initialize: %d %s", rec.Code, rec.Body.String())
	}
}

// TestMCPScopedTokenNonToolMethodsDenied pins deny-by-default beyond
// tools/call: content-bearing methods (resources/read, prompts/get — only an
// extension provider could register such content) require extview:read from
// a scoped token, while the handshake/listing metadata methods pass. A
// full-access credential is not gated (it fails later, in the SDK, for lack
// of registered content — never with the permission error).
func TestMCPScopedTokenNonToolMethodsDenied(t *testing.T) {
	s := newManagedServer(t, "static-tok")

	viewer, _, err := s.sessions.IssueWithPolicy("settings-viewer",
		&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSettingsRead}})
	if err != nil {
		t.Fatal(err)
	}
	rec := doJSON(t, s, "POST", "/api/mcp-token", viewer)
	if rec.Code != 200 {
		t.Fatalf("mint = %d: %s", rec.Code, rec.Body.String())
	}
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil {
		t.Fatal(err)
	}

	openSession := func(token string) string {
		t.Helper()
		rec := doMCPRaw(t, s, "/mcp", token, "", mcpRawInitialize)
		if rec.Code != 200 {
			t.Fatalf("initialize = %d: %s", rec.Code, rec.Body.String())
		}
		sess := rec.Header().Get("Mcp-Session-Id")
		if rec := doMCPRaw(t, s, "/mcp", token, sess, mcpRawInitialized); rec.Code >= 300 {
			t.Fatalf("notifications/initialized = %d: %s", rec.Code, rec.Body.String())
		}
		return sess
	}

	sess := openSession(minted.Token)
	for name, body := range map[string]string{
		"resources/read": `{"jsonrpc":"2.0","id":3,"method":"resources/read","params":{"uri":"file:///x"}}`,
		"prompts/get":    `{"jsonrpc":"2.0","id":4,"method":"prompts/get","params":{"name":"p"}}`,
	} {
		rec := doMCPRaw(t, s, "/mcp", minted.Token, sess, body)
		if !strings.Contains(rec.Body.String(), string(ext.PermExtViewRead)) {
			t.Errorf("%s with a settings:read-only token: want a denial naming %s, got %d %s", name, ext.PermExtViewRead, rec.Code, rec.Body.String())
		}
	}
	// Metadata listing still passes the grant gate for the scoped token.
	rec = doMCPRaw(t, s, "/mcp", minted.Token, sess,
		`{"jsonrpc":"2.0","id":5,"method":"tools/list","params":{}}`)
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "forbidden") {
		t.Errorf("tools/list with a scoped token = %d %s, want ungated metadata", rec.Code, rec.Body.String())
	}

	// The static (full-access) credential is not gated: whatever the SDK
	// answers for unregistered content, it is not the permission denial.
	staticSess := openSession("static-tok")
	rec = doMCPRaw(t, s, "/mcp", "static-tok", staticSess,
		`{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"file:///x"}}`)
	if strings.Contains(rec.Body.String(), string(ext.PermExtViewRead)) {
		t.Errorf("full-access resources/read must not hit the grant gate, got: %s", rec.Body.String())
	}
}
