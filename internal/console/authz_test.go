package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// registeredAPIPatterns mirrors the authenticated /api routes registered on the
// inner mux in buildHandler (server.go). It is the drift guard for the
// route→permission table: every registered route must be classified, and the
// table must carry no entry for a route that no longer exists. Keep this in sync
// with buildHandler's api.HandleFunc calls. (The /api/ext/ subtree is matched by
// prefix, not a table row, and is covered separately.)
var registeredAPIPatterns = []struct{ method, pattern string }{
	{"GET", "/api/status"},
	{"GET", "/api/coverage"},
	{"GET", "/api/uncaptured-tables"},
	{"GET", "/api/capacity"},
	{"GET", "/api/activity"},
	{"GET", "/api/schemas"},
	{"GET", "/api/events"},
	{"GET", "/api/schema-changes"},
	{"POST", "/api/recover"},
	{"POST", "/api/recover-cascade"},
	{"GET", "/api/capabilities"},
	{"GET", "/api/reconstruct"},
	{"GET", "/api/baselines"},
	{"GET", "/api/baselines/files"},
	{"GET", "/api/baselines/download"},
	{"GET", "/api/views.sql"},
	{"GET", "/api/storage"},
	{"GET", "/api/profiles"},
	{"GET", "/api/access-profiles"},
	{"POST", "/api/access-profiles/flags"},
	{"POST", "/api/access-profiles/flags/remove"},
	{"POST", "/api/access-profiles/profiles"},
	{"POST", "/api/access-profiles/profiles/remove"},
	{"POST", "/api/access-profiles/rules"},
	{"POST", "/api/access-profiles/rules/remove"},
	{"GET", "/api/telemetry"},
	{"POST", "/api/telemetry"},
	{"GET", "/api/servers"},
	{"POST", "/api/servers"},
	{"POST", "/api/servers/test"},
	{"GET", "/api/servers/{}"},
	{"PUT", "/api/servers/{}"},
	{"DELETE", "/api/servers/{}"},
	{"POST", "/api/servers/{}/test"},
	{"POST", "/api/servers/{}/monitor/start"},
	{"POST", "/api/servers/{}/monitor/stop"},
	{"GET", "/api/servers/{}/monitor"},
	{"GET", "/api/servers/{}/first-run"},
	{"POST", "/api/servers/{}/baseline"},
	{"GET", "/api/servers/{}/baseline"},
	{"POST", "/api/servers/{}/baseline/restore"},
	{"GET", "/api/servers/{}/baseline/restore"},
	{"POST", "/api/servers/{}/sql-export"},
	{"GET", "/api/servers/{}/sql-export"},
	{"GET", "/api/servers/{}/sql-export/download"},
	{"POST", "/api/servers/{}/schema-snapshot"},
	{"GET", "/api/servers/{}/schema-snapshot"},
	{"POST", "/api/capture-skips/ack"},
	{"POST", "/api/servers/{}/verify"},
	{"GET", "/api/servers/{}/verify"},
	{"GET", "/api/servers/{}/verify/explain"},
	{"GET", "/api/servers/{}/verify/history"},
	{"GET", "/api/rotation"},
	{"PUT", "/api/rotation"},
	{"GET", "/api/backup-settings"},
	{"PUT", "/api/backup-settings/servers/{}"},
	{"PUT", "/api/backup-settings/daemon/{}"},
	{"GET", "/api/baseline-refresh"},
	{"PUT", "/api/servers/{}/backup-schedule"},
	{"DELETE", "/api/servers/{}/backup-schedule"},
	{"POST", "/api/auth/logout"},
	{"POST", "/api/auth/password"},
	{"GET", "/api/mcp-token"},
	{"POST", "/api/mcp-token"},
	{"DELETE", "/api/mcp-token"},
	{"GET", "/api/flashback"},
}

// concretePath turns a table pattern into a real path by giving every "{}" a
// value, so permForRoute (which matches concrete request paths) can be exercised.
func concretePath(pattern string) string {
	return strings.ReplaceAll(pattern, "{}", "x")
}

// TestRouteTableClassifiesEveryRegisteredRoute pins that every registered /api
// route resolves to a permission — the fail-closed contract: an unclassified
// route must be impossible, not merely refused at runtime.
func TestRouteTableClassifiesEveryRegisteredRoute(t *testing.T) {
	for _, r := range registeredAPIPatterns {
		path := concretePath(r.pattern)
		if _, ok := permForRoute(r.method, path); !ok {
			t.Errorf("registered route %s %s is not classified in apiRoutePerms", r.method, r.pattern)
		}
	}
}

// TestRouteTableHasNoDeadEntries pins the other direction: every apiRoutePerms
// entry corresponds to a registered route, so the table never drifts into
// classifying a route that no longer exists.
func TestRouteTableHasNoDeadEntries(t *testing.T) {
	registered := make(map[string]bool, len(registeredAPIPatterns))
	for _, r := range registeredAPIPatterns {
		registered[r.method+" "+r.pattern] = true
	}
	for _, rp := range apiRoutePerms {
		if !registered[rp.method+" "+rp.pattern] {
			t.Errorf("apiRoutePerms has %s %s, which is not a registered route (dead entry?)", rp.method, rp.pattern)
		}
	}
}

// TestRoutePermsUseKnownPermissions guards against a typo'd permission string in
// the table: every entry must be permAny or one of the core's defined permissions.
func TestRoutePermsUseKnownPermissions(t *testing.T) {
	known := map[ext.Permission]bool{permAny: true}
	for _, p := range ext.AllPermissions() {
		known[p] = true
	}
	for _, rp := range apiRoutePerms {
		if !known[rp.perm] {
			t.Errorf("route %s %s uses unknown permission %q", rp.method, rp.pattern, rp.perm)
		}
	}
}

// TestRoutePermReachableAndOrdered gives each table entry a concrete path and
// asserts permForRoute returns THAT entry's permission. This is the ordering
// invariant: if an earlier, more generic pattern shadowed a later specific one,
// the specific entry's concrete path would resolve to the wrong permission.
func TestRoutePermReachableAndOrdered(t *testing.T) {
	for _, rp := range apiRoutePerms {
		got, ok := permForRoute(rp.method, concretePath(rp.pattern))
		if !ok {
			t.Errorf("%s %s: not classified", rp.method, rp.pattern)
			continue
		}
		if got != rp.perm {
			t.Errorf("%s %s resolved to %q, want %q — an earlier pattern is shadowing it", rp.method, rp.pattern, got, rp.perm)
		}
	}
}

// TestPermForRouteExactDepth pins the exact-segment-count rule: a placeholder
// pattern must not match a path of a different depth. GET /api/servers/{} must
// not swallow GET /api/servers (shallower) or GET /api/servers/x/monitor (deeper).
func TestPermForRouteExactDepth(t *testing.T) {
	// Shallower than /api/servers/{}: resolves to the /api/servers entry, not the
	// placeholder one — different permission is not what we assert; classification
	// and the placeholder-not-swallowing is. Deeper: an unregistered depth is
	// unclassified (fail closed).
	if _, ok := permForRoute("GET", "/api/servers/x/does-not-exist"); ok {
		t.Error("GET /api/servers/x/does-not-exist should be unclassified (fail closed), got classified")
	}
	if p, ok := permForRoute("GET", "/api/servers/x"); !ok || p != ext.PermServersRead {
		t.Errorf("GET /api/servers/x = (%q,%v), want (servers:read,true)", p, ok)
	}
}

// TestPermForRouteExtPrefix pins that the whole /api/ext/ subtree, at any depth,
// requires extview:read.
func TestPermForRouteExtPrefix(t *testing.T) {
	for _, path := range []string{"/api/ext/myview/", "/api/ext/myview/data", "/api/ext/a/b/c"} {
		p, ok := permForRoute("GET", path)
		if !ok || p != ext.PermExtViewRead {
			t.Errorf("permForRoute(GET, %q) = (%q,%v), want (extview:read,true)", path, p, ok)
		}
	}
}

// TestPermForRouteUnknownIsFailClosed pins that an unrecognized /api path is
// reported unclassified so authzMiddleware refuses it for a scoped session.
func TestPermForRouteUnknownIsFailClosed(t *testing.T) {
	if _, ok := permForRoute("GET", "/api/nonexistent"); ok {
		t.Error("an unknown /api route must be unclassified (fail closed)")
	}
	// Right path, wrong method is also unclassified.
	if _, ok := permForRoute("DELETE", "/api/status"); ok {
		t.Error("a known path with the wrong method must be unclassified")
	}
}

// withPolicy returns a request context carrying pol, as tokenMiddleware would.
func withPolicy(pol *ext.AccessPolicy) context.Context {
	return context.WithValue(context.Background(), policyCtxKey{}, pol)
}

// TestAuthzMiddlewarePolicyLessAllowsEverything pins the OSS regression guard: a
// nil policy (static token, password login, every OSS session) reaches the
// handler on every route, including ones no permission would grant.
func TestAuthzMiddlewarePolicyLessAllowsEverything(t *testing.T) {
	srv := &Server{}
	reached := false
	h := srv.authzMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, r := range registeredAPIPatterns {
		reached = false
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(r.method, concretePath(r.pattern), nil)
		// No policy on the context — the policy-less case.
		h.ServeHTTP(rec, req)
		if !reached || rec.Code != http.StatusNoContent {
			t.Errorf("policy-less %s %s: reached=%v code=%d, want reached=true code=204", r.method, r.pattern, reached, rec.Code)
		}
	}
}

// TestAuthzMiddlewareScopedSession pins enforcement for a scoped policy: a viewer
// (status:read + servers:read) reaches allowed routes, is 403'd on routes needing
// a permission it lacks (with the permission named), and always reaches permAny
// routes.
func TestAuthzMiddlewareScopedSession(t *testing.T) {
	srv := &Server{}
	viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}}
	reached := false
	h := srv.authzMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	}))
	serve := func(method, path string) *httptest.ResponseRecorder {
		reached = false
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, nil).WithContext(withPolicy(viewer))
		h.ServeHTTP(rec, req)
		return rec
	}

	// Allowed: a granted permission and a permAny route.
	if rec := serve("GET", "/api/status"); !reached || rec.Code != http.StatusNoContent {
		t.Errorf("viewer GET /api/status: reached=%v code=%d, want allowed", reached, rec.Code)
	}
	if rec := serve("GET", "/api/capabilities"); !reached || rec.Code != http.StatusNoContent {
		t.Errorf("viewer GET /api/capabilities (permAny): reached=%v code=%d, want allowed", reached, rec.Code)
	}

	// Denied: events needs query:execute, which a viewer lacks. 403, handler never
	// reached, and the body names the missing permission.
	rec := serve("GET", "/api/events")
	if reached {
		t.Error("viewer reached the /api/events handler despite lacking query:execute")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("viewer GET /api/events = %d, want 403", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), string(ext.PermQueryExecute)) {
		t.Errorf("403 body %q does not name the missing permission %q", rec.Body.String(), ext.PermQueryExecute)
	}
}

// TestAuthzMiddlewareUnclassifiedFailsClosed pins that a scoped session hitting a
// route with no classification is refused, never granted.
func TestAuthzMiddlewareUnclassifiedFailsClosed(t *testing.T) {
	srv := &Server{}
	pol := &ext.AccessPolicy{Permissions: ext.AllPermissions()} // even an all-powerful policy
	reached := false
	h := srv.authzMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/brand-new-route", nil).WithContext(withPolicy(pol))
	h.ServeHTTP(rec, req)
	if reached || rec.Code != http.StatusForbidden {
		t.Errorf("unclassified route: reached=%v code=%d, want 403 fail-closed", reached, rec.Code)
	}
}

// TestPermissionsForPolicy pins the capabilities grant map: nil → all true; a
// subset policy → exactly those true.
func TestPermissionsForPolicy(t *testing.T) {
	all := permissionsForPolicy(nil)
	for _, p := range ext.AllPermissions() {
		if !all[string(p)] {
			t.Errorf("nil policy: %q reported false, want true (full access)", p)
		}
	}

	viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}}
	m := permissionsForPolicy(viewer)
	if !m[string(ext.PermStatusRead)] || !m[string(ext.PermServersRead)] {
		t.Error("viewer should hold status:read and servers:read")
	}
	if m[string(ext.PermRecoverExecute)] || m[string(ext.PermServersWrite)] {
		t.Error("viewer should NOT hold recover:execute or servers:write")
	}
}

// TestScopedSessionEnforcedEndToEnd wires a real server: a scoped session minted
// via IssueWithPolicy is 403'd on a route its policy denies, while a full-access
// session is not blocked by authz (it may fail later for lack of an index, but
// never with authz's 403).
func TestScopedSessionEnforcedEndToEnd(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}

	viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}}
	scoped, _, err := srv.sessions.IssueWithPolicy("test-viewer", viewer)
	if err != nil {
		t.Fatal(err)
	}
	full, _, err := srv.sessions.Issue()
	if err != nil {
		t.Fatal(err)
	}

	// Scoped viewer: events denied at the authz layer (403), never reaching a DB.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events", scoped); rec.Code != http.StatusForbidden {
		t.Errorf("scoped viewer GET /api/events = %d, want 403", rec.Code)
	}
	// Full-access session: authz does not block events (any non-403 is fine — it
	// may 5xx without an index, but authz must not be the thing that stops it).
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/events", full); rec.Code == http.StatusForbidden {
		t.Errorf("full-access GET /api/events = 403, authz should not block a policy-less session")
	}
	// The static token is also policy-less — never blocked by authz.
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", "static-tok"); rec.Code == http.StatusForbidden {
		t.Errorf("static token GET /api/capabilities = 403, want not-403")
	}
}

// TestCapabilitiesReportsScopedPermissions pins that /api/capabilities reflects a
// scoped session's grants (so the SPA can gate its UI), and full grants for a
// policy-less session.
func TestCapabilitiesReportsScopedPermissions(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	viewer := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermStatusRead, ext.PermServersRead}}
	scoped, _, _ := srv.sessions.IssueWithPolicy("test-viewer", viewer)

	rec := getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", scoped)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/capabilities = %d", rec.Code)
	}
	var caps struct {
		Permissions map[string]bool `json:"permissions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	if !caps.Permissions[string(ext.PermStatusRead)] || caps.Permissions[string(ext.PermQueryExecute)] {
		t.Errorf("scoped capabilities.permissions = %v, want status:read true and query:execute false", caps.Permissions)
	}

	// Policy-less (static token): every permission reported true.
	rec = getPath(t, srv, "127.0.0.1:8090", "/api/capabilities", "static-tok")
	if err := json.Unmarshal(rec.Body.Bytes(), &caps); err != nil {
		t.Fatal(err)
	}
	for _, p := range ext.AllPermissions() {
		if !caps.Permissions[string(p)] {
			t.Errorf("policy-less capabilities missing %q=true", p)
		}
	}
}

// TestFirstRunRouteIsARead: the Getting started list is a read, so a read-only
// role keeps it (#1606); a 403 would stop the page's loop without a word.
func TestFirstRunRouteIsARead(t *testing.T) {
	if p, ok := permForRoute("GET", "/api/servers/x/first-run"); !ok || p != ext.PermServersRead {
		t.Fatalf("permForRoute = %q, %v; want %q", p, ok, ext.PermServersRead)
	}
}
