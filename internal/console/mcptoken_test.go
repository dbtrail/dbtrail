package console

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

func TestMCPTokenFile_GenerateLoadRotateRevoke(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")

	// Missing file = not configured, no error.
	if f, err := LoadMCPTokenFile(path); err != nil || f != nil {
		t.Fatalf("missing file: got (%v, %v), want (nil, nil)", f, err)
	}

	token, f, err := GenerateMCPToken(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, mcpTokenPrefix) {
		t.Errorf("token %q missing %q prefix", token, mcpTokenPrefix)
	}
	sum := sha256.Sum256([]byte(token))
	if f.TokenSHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("stored sha %q does not match sha256(token)", f.TokenSHA256)
	}
	if strings.Contains(mustReadFile(t, path), token) {
		t.Error("plaintext token must never be persisted")
	}
	if runtime.GOOS != "windows" {
		if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
			t.Errorf("token file mode = %v (err %v), want 0600", info.Mode().Perm(), err)
		}
	}

	loaded, err := LoadMCPTokenFile(path)
	if err != nil || loaded == nil || loaded.TokenSHA256 != f.TokenSHA256 {
		t.Fatalf("reload mismatch: %+v, %v", loaded, err)
	}

	// Rotate replaces the hash.
	token2, f2, err := GenerateMCPToken(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token2 == token || f2.TokenSHA256 == f.TokenSHA256 {
		t.Error("rotation must mint a fresh token")
	}

	if err := RevokeMCPToken(path); err != nil {
		t.Fatal(err)
	}
	if f, err := LoadMCPTokenFile(path); err != nil || f != nil {
		t.Fatalf("after revoke: got (%v, %v), want (nil, nil)", f, err)
	}
	// Revoking again is a no-op.
	if err := RevokeMCPToken(path); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
}

// TestMCPTokenFile_NewerVersionReadOnly pins the forward-compat contract: a
// newer file loads read-only WITHOUT payload validation (a v2 digest this
// binary can't read must degrade, not error), and every write path refuses.
func TestMCPTokenFile_NewerVersionReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	content := "version: 99\ntoken_sha256: some-future-format\ncreated_at: 2030-01-01T00:00:00Z\nfuture_field: kept\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := LoadMCPTokenFile(path)
	if err != nil || !f.ReadOnly() {
		t.Fatalf("newer-version file: f=%+v err=%v, want read-only load with no error", f, err)
	}
	if _, _, err := GenerateMCPToken(path, nil); !errors.Is(err, ErrMCPTokenFileReadOnly) {
		t.Errorf("generate on newer-version file: %v, want ErrMCPTokenFileReadOnly", err)
	}
	if err := RevokeMCPToken(path); !errors.Is(err, ErrMCPTokenFileReadOnly) {
		t.Errorf("revoke on newer-version file: %v, want ErrMCPTokenFileReadOnly", err)
	}
}

func TestMCPTokenFile_MalformedShaFailsLoud(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	if err := os.WriteFile(path, []byte("version: 1\ntoken_sha256: nothex\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMCPTokenFile(path); err == nil {
		t.Fatal("malformed v1 token_sha256 must fail loud, got nil error")
	}
	// The documented self-heal: Generate replaces the corrupt file.
	if _, _, err := GenerateMCPToken(path, nil); err != nil {
		t.Fatalf("generate over corrupt v1 file must self-heal, got %v", err)
	}
	if _, err := LoadMCPTokenFile(path); err != nil {
		t.Fatalf("file after self-heal still unreadable: %v", err)
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// newManagedServer builds a Server like newBootServer but with a managed-token
// path under t.TempDir() and an optional static token.
func newManagedServer(t *testing.T, staticToken string) *Server {
	t.Helper()
	db, _, closer := newSQLMock(t)
	t.Cleanup(closer)
	s := &Server{
		token:        staticToken,
		cm:           newConnManager(nil, false),
		mcpTokenPath: filepath.Join(t.TempDir(), "mcp-token.yaml"),
		sessions:     newSessionStore(),
		loginLimiter: newLoginLimiter(),
	}
	s.managedTok.initFromDisk(s.mcpTokenPath, nil)
	s.cm.boot = &bundle{db: db, engine: query.New(db), noArchive: true}
	s.mux = s.buildHandler()
	return s
}

func doJSON(t *testing.T, s *Server, method, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	return rec
}

// TestManagedToken_APILifecycle drives generate → authenticate → rotate →
// revoke through the HTTP surface, asserting the minted value works
// immediately (no restart) on /mcp and stops working after rotate/revoke.
func TestManagedToken_APILifecycle(t *testing.T) {
	s := newManagedServer(t, "static-tok")

	// Status before: static only.
	rec := doJSON(t, s, "GET", "/api/mcp-token", "static-tok")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"static":true`) || !strings.Contains(rec.Body.String(), `"managed":false`) {
		t.Fatalf("initial status = %d %s", rec.Code, rec.Body.String())
	}

	// Generate via the static token.
	rec = doJSON(t, s, "POST", "/api/mcp-token", "static-tok")
	if rec.Code != 200 {
		t.Fatalf("generate = %d: %s", rec.Code, rec.Body.String())
	}
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil || !strings.HasPrefix(minted.Token, mcpTokenPrefix) {
		t.Fatalf("generate response %q: %v", rec.Body.String(), err)
	}

	// The managed token authenticates /mcp immediately — and the STATIC token
	// keeps working alongside it (generating must never lock out existing
	// static-token MCP clients).
	if rec := doMCP(t, s, "/mcp", minted.Token); rec.Code != 200 {
		t.Fatalf("managed token on /mcp = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doMCP(t, s, "/mcp", "static-tok"); rec.Code != 200 {
		t.Fatalf("static token on /mcp with managed configured = %d", rec.Code)
	}
	// Status now reports the managed credential (the fields the UI card renders).
	rec = doJSON(t, s, "GET", "/api/mcp-token", "static-tok")
	if !strings.Contains(rec.Body.String(), `"managed":true`) || !strings.Contains(rec.Body.String(), `"created_at":"`) {
		t.Fatalf("configured status: %s", rec.Body.String())
	}
	// Capabilities (via the static token) reflect it.
	if rec := doJSON(t, s, "GET", "/api/capabilities", "static-tok"); !strings.Contains(rec.Body.String(), `"mcp":true`) {
		t.Fatalf("capabilities.mcp should be true: %s", rec.Body.String())
	}

	// Rotation invalidates the previous value at once.
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
	if rec := doMCP(t, s, "/mcp", minted.Token); rec.Code != 401 {
		t.Fatalf("pre-rotation token still accepted on /mcp: %d", rec.Code)
	}

	// Revoke drops the managed credential; the static one is untouched.
	if rec := doJSON(t, s, "DELETE", "/api/mcp-token", "static-tok"); rec.Code != 200 {
		t.Fatalf("revoke = %d", rec.Code)
	}
	if rec := doJSON(t, s, "GET", "/api/mcp-token", "static-tok"); !strings.Contains(rec.Body.String(), `"managed":false`) {
		t.Fatalf("post-revoke status: %s", rec.Body.String())
	}
	if rec := doMCP(t, s, "/mcp", rotated.Token); rec.Code != 401 {
		t.Fatalf("revoked token still accepted on /mcp: %d", rec.Code)
	}
	if rec := doMCP(t, s, "/mcp", "static-tok"); rec.Code != 200 {
		t.Fatalf("static token on /mcp after revoke = %d", rec.Code)
	}
}

// TestManagedToken_CapabilitiesManagedOnly pins the zero-config headline:
// with NO static token, capabilities.mcp flips false→true when a managed
// token is generated (a regression to static-only would hide the MCP Server
// ready state for exactly the users the feature exists for).
func TestManagedToken_CapabilitiesManagedOnly(t *testing.T) {
	s := newManagedServer(t, "")
	caps := func() capabilitiesResponse {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleCapabilities(rec, httptest.NewRequest("GET", "/api/capabilities", nil))
		if rec.Code != 200 {
			t.Fatalf("capabilities = %d: %s", rec.Code, rec.Body.String())
		}
		var resp capabilitiesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if caps().MCP {
		t.Fatal("capabilities.mcp true with no token configured")
	}
	_, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.managedTok.reload(f)
	if !caps().MCP {
		t.Fatal("capabilities.mcp false with a managed token configured")
	}
}

// TestManagedToken_ReadOnlyFileOverHTTP pins the newer-version surface end to
// end: status reports read_only (and not managed — the digest is unreadable),
// and generate/rotate/revoke map ErrMCPTokenFileReadOnly to 409.
func TestManagedToken_ReadOnlyFileOverHTTP(t *testing.T) {
	s := newManagedServer(t, "static-tok")
	content := "version: 99\ntoken_sha256: future-format\ncreated_at: 2030-01-01T00:00:00Z\n"
	if err := os.WriteFile(s.mcpTokenPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := doJSON(t, s, "GET", "/api/mcp-token", "static-tok")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"read_only":true`) || !strings.Contains(rec.Body.String(), `"managed":false`) {
		t.Fatalf("read-only status = %d %s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(t, s, "POST", "/api/mcp-token", "static-tok"); rec.Code != 409 {
		t.Fatalf("generate on read-only file = %d, want 409", rec.Code)
	}
	if rec := doJSON(t, s, "DELETE", "/api/mcp-token", "static-tok"); rec.Code != 409 {
		t.Fatalf("revoke on read-only file = %d, want 409", rec.Code)
	}
}

// TestManagedToken_CorruptFileNeverBlocksStartup pins the degrade contract:
// New() must survive a junk token file (this daemon may be the stream
// supervisor — capture must not die over a UI-convenience credential), with
// the managed token simply absent.
func TestManagedToken_CorruptFileNeverBlocksStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp-token.yaml")
	if err := os.WriteFile(path, []byte(":\tnot yaml at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen:       "127.0.0.1:8090",
		Token:        "t",
		AuthPath:     filepath.Join(t.TempDir(), "auth.yaml"),
		MCPTokenPath: path,
	})
	if err != nil {
		t.Fatalf("New with corrupt MCP token file must degrade, got: %v", err)
	}
	rec := doJSON(t, srv, "GET", "/api/mcp-token", "t")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"managed":false`) {
		t.Fatalf("degraded status = %d %s", rec.Code, rec.Body.String())
	}
}

// TestManagedToken_ScopedToMCPOnly pins the credential boundary: the managed
// token is an /mcp-only credential — the browser/API surface (including the
// password-change route and the token-management routes themselves) must
// reject it, because its advertised scope is the read-only MCP tools.
func TestManagedToken_ScopedToMCPOnly(t *testing.T) {
	s := newManagedServer(t, "static-tok")
	rec := doJSON(t, s, "POST", "/api/mcp-token", "static-tok")
	if rec.Code != 200 {
		t.Fatalf("generate = %d", rec.Code)
	}
	var minted struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &minted); err != nil {
		t.Fatal(err)
	}

	for _, probe := range []struct{ method, path string }{
		{"GET", "/api/mcp-token"},
		{"POST", "/api/mcp-token"},
		{"GET", "/api/capabilities"},
		{"GET", "/api/servers"},
		{"POST", "/api/auth/password"},
	} {
		if rec := doJSON(t, s, probe.method, probe.path, minted.Token); rec.Code != 401 {
			t.Errorf("managed token on %s %s = %d, want 401 (scope leak)", probe.method, probe.path, rec.Code)
		}
	}
	if rec := doMCP(t, s, "/mcp", minted.Token); rec.Code != 200 {
		t.Errorf("managed token on /mcp = %d, want 200", rec.Code)
	}
}

// TestManagedToken_MCPWithoutStaticToken pins the zero-config path: no
// --token, only a managed token — the /mcp gate opens and the credential
// authenticates, while a wrong value still 401s and a token-less console
// still 403s.
func TestManagedToken_MCPWithoutStaticToken(t *testing.T) {
	s := newManagedServer(t, "")

	// No credential configured at all: the gate refuses and names the UI path.
	rec := doMCP(t, s, "/mcp", "anything")
	if rec.Code != 403 || !strings.Contains(rec.Body.String(), "MCP Server") {
		t.Fatalf("token-less /mcp = %d %s, want 403 naming Settings → MCP Server", rec.Code, rec.Body.String())
	}

	token, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.managedTok.reload(f)

	if rec := doMCP(t, s, "/mcp", token); rec.Code != 200 {
		t.Fatalf("managed-only /mcp = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doMCP(t, s, "/mcp", "wrong"); rec.Code != 401 {
		t.Fatalf("wrong token = %d, want 401", rec.Code)
	}
	if rec := doMCP(t, s, "/mcp", ""); rec.Code != 401 {
		t.Fatalf("empty bearer = %d, want 401", rec.Code)
	}
}

// TestManagedToken_CrossProcessRevokeAndRotate pins the staleness discipline:
// a rotate or revoke performed by ANOTHER process (simulated by mutating the
// file directly) takes effect on this server's /mcp gate without a restart.
func TestManagedToken_CrossProcessRevokeAndRotate(t *testing.T) {
	s := newManagedServer(t, "")
	token, f, err := GenerateMCPToken(s.mcpTokenPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.managedTok.reload(f)
	if rec := doMCP(t, s, "/mcp", token); rec.Code != 200 {
		t.Fatalf("sanity: managed token = %d", rec.Code)
	}

	// Another process rotates: the file changes on disk.
	token2, _, err := GenerateMCPToken(s.mcpTokenPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rec := doMCP(t, s, "/mcp", token2); rec.Code != 200 {
		t.Fatalf("token rotated on disk not picked up: %d", rec.Code)
	}
	if rec := doMCP(t, s, "/mcp", token); rec.Code != 401 {
		t.Fatalf("stale pre-rotation token still accepted: %d", rec.Code)
	}

	// Another process revokes: the file disappears.
	if err := os.Remove(s.mcpTokenPath); err != nil {
		t.Fatal(err)
	}
	if rec := doMCP(t, s, "/mcp", token2); rec.Code == 200 {
		t.Fatal("revoked-on-disk token still accepted")
	}
}
