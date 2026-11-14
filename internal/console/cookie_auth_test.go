package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Session-cookie tests (#1370): every login sets the HttpOnly bintrail_session
// cookie, the middleware accepts it as an alternative credential (Bearer wins
// when present), state-changing cookie-authenticated requests must carry the
// application/json CSRF marker, and logout clears the cookie after revoking
// server-side.

// sessionCookieFrom extracts the bintrail_session cookie from a recorded
// response, failing the test when it is absent.
func sessionCookieFrom(t *testing.T, rec *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName || strings.HasPrefix(c.Name, sessionCookieName+"_") {
			return c
		}
	}
	t.Fatalf("no %s cookie in response (Set-Cookie: %v)", sessionCookieName, rec.Header().Values("Set-Cookie"))
	return nil
}

// cookieRequest fires a request authenticated ONLY by the session cookie — no
// Authorization header. contentType == "" sends no Content-Type at all.
func cookieRequest(t *testing.T, srv *Server, method, path, token, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// loginRec performs a password login and returns the raw response recorder
// plus the issued session token, so callers can inspect Set-Cookie.
func loginRec(t *testing.T, srv *Server) (*httptest.ResponseRecorder, string) {
	t.Helper()
	rec := postJSON(t, srv, "/api/auth/login", "", `{"username":"admin","password":"correct-horse-battery"}`)
	if rec.Code != 200 {
		t.Fatalf("login = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Token == "" {
		t.Fatalf("login response unparseable: %s", rec.Body.String())
	}
	return rec, resp.Token
}

// assertSessionCookieAttrs checks the fixed attribute set every session
// cookie must carry, and that its value is the issued session token.
func assertSessionCookieAttrs(t *testing.T, c *http.Cookie, token string) {
	t.Helper()
	if c.Value != token {
		t.Errorf("cookie value = %q, want the issued session token %q", c.Value, token)
	}
	if !c.HttpOnly {
		t.Error("session cookie is not HttpOnly")
	}
	if !c.Secure {
		t.Error("session cookie is not Secure")
	}
	if c.SameSite != http.SameSiteLaxMode {
		t.Errorf("session cookie SameSite = %v, want Lax", c.SameSite)
	}
	if c.Path != "/" {
		t.Errorf("session cookie Path = %q, want /", c.Path)
	}
	// Max-Age mirrors the session's absolute TTL (allow the test's own
	// wall-clock drift downward, never upward).
	if max := int(sessionAbsoluteTTL / time.Second); c.MaxAge > max || c.MaxAge < max-60 {
		t.Errorf("session cookie Max-Age = %d, want ~%d", c.MaxAge, max)
	}
}

func TestLoginSetsSessionCookie(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	rec, token := loginRec(t, srv)
	assertSessionCookieAttrs(t, sessionCookieFrom(t, rec), token)
}

func TestSetupSetsSessionCookie(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	rec := postJSON(t, srv, "/api/auth/setup", "", `{"username":"admin","password":"correct-horse-battery"}`)
	if rec.Code != 200 {
		t.Fatalf("setup = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token string `json:"token"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	assertSessionCookieAttrs(t, sessionCookieFrom(t, rec), resp.Token)
}

func TestPasswordChangeRefreshesSessionCookie(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	_, tok := loginRec(t, srv)
	rec := postJSON(t, srv, "/api/auth/password", tok, `{"current_password":"correct-horse-battery","new_password":"a-new-password"}`)
	if rec.Code != 200 {
		t.Fatalf("password change = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Token string `json:"token"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	// RevokeAll killed the old session; the cookie must carry the FRESH one.
	assertSessionCookieAttrs(t, sessionCookieFrom(t, rec), resp.Token)
}

func TestSessionCookieAuthenticatesGET(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{} // a resolvable default so capabilities answers
	_, tok := loginRec(t, srv)

	rec := cookieRequest(t, srv, "GET", "/api/capabilities", tok, "", "")
	if rec.Code != 200 {
		t.Fatalf("cookie-only GET /api/capabilities = %d body = %s", rec.Code, rec.Body.String())
	}
	var caps capabilitiesResponse
	json.Unmarshal(rec.Body.Bytes(), &caps)
	if caps.Auth.AuthKind != "session" {
		t.Errorf("auth.auth_kind = %q via cookie, want session", caps.Auth.AuthKind)
	}
}

func TestSessionCookieCSRFBelt(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{}
	_, tok := loginRec(t, srv)

	// A state-changing request with only the cookie and no JSON marker is
	// refused — the shape a cross-site HTML form produces.
	for _, ct := range []string{"", "application/x-www-form-urlencoded", "text/plain"} {
		if rec := cookieRequest(t, srv, "POST", "/api/auth/logout", tok, ct, ""); rec.Code != 403 {
			t.Errorf("cookie-only POST with Content-Type %q = %d, want 403", ct, rec.Code)
		}
	}
	// The belt fired BEFORE the handler: the session must still be alive.
	if rec := cookieRequest(t, srv, "GET", "/api/capabilities", tok, "", ""); rec.Code != 200 {
		t.Fatalf("session died on a refused CSRF probe: GET = %d", rec.Code)
	}

	// The same request with the JSON marker passes (the SPA's shape).
	rec := cookieRequest(t, srv, "POST", "/api/auth/logout", tok, "application/json", "")
	if rec.Code != 204 {
		t.Fatalf("cookie-only JSON logout = %d, want 204", rec.Code)
	}
	// Logout cleared the cookie and revoked server-side: the cookie no longer
	// authenticates anything.
	if c := sessionCookieFrom(t, rec); c.MaxAge >= 0 || c.Value != "" {
		t.Errorf("logout cookie = value %q Max-Age %d, want empty and expiring", c.Value, c.MaxAge)
	}
	if rec := cookieRequest(t, srv, "GET", "/api/capabilities", tok, "", ""); rec.Code != 401 {
		t.Errorf("revoked session cookie still authenticates: %d", rec.Code)
	}
}

func TestBearerExemptFromCSRFMarker(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	_, tok := loginRec(t, srv)

	// Bearer requests never need the JSON marker — byte-identical to the
	// pre-cookie contract (scripted access, curl, CI).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/logout", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 204 {
		t.Fatalf("Bearer logout without Content-Type = %d, want 204", rec.Code)
	}
	// Logout with a Bearer also clears the cookie (all tabs die).
	if c := sessionCookieFrom(t, rec); c.MaxAge >= 0 {
		t.Errorf("Bearer logout cookie Max-Age = %d, want expiring", c.MaxAge)
	}
}

func TestBearerWinsOverCookie(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{}
	_, tok := loginRec(t, srv)

	// An invalid Bearer is judged on its own: no silent fallback to the valid
	// cookie riding the same request.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capabilities", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: tok})
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("invalid Bearer + valid cookie = %d, want 401 (Bearer wins)", rec.Code)
	}
}

func TestGarbageSessionCookieRefused(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{}
	if rec := cookieRequest(t, srv, "GET", "/api/capabilities", "bcs_deadbeef", "", ""); rec.Code != 401 {
		t.Errorf("garbage cookie = %d, want 401", rec.Code)
	}
}

func TestExpiredSessionCookieRefused(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	srv.cm.boot = &bundle{}
	_, tok := loginRec(t, srv)

	// Same store, same expiry as Bearer: past the absolute TTL the cookie is
	// refused like any dead session.
	srv.sessions.now = func() time.Time { return time.Now().Add(sessionAbsoluteTTL + time.Hour) }
	if rec := cookieRequest(t, srv, "GET", "/api/capabilities", tok, "", ""); rec.Code != 401 {
		t.Errorf("expired session cookie = %d, want 401", rec.Code)
	}
}

func TestExtLoginSetsSessionCookie(t *testing.T) {
	installFakeProvider(t)
	srv := newExtServer(t)

	// The provider's finish flow calls the issuer, which sets the cookie on
	// the in-flight (redirect) response — external logins extend to new tabs
	// exactly like password logins.
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/auth/ext/finish", "")
	if rec.Code != http.StatusFound {
		t.Fatalf("finish = %d, want 302", rec.Code)
	}
	c := sessionCookieFrom(t, rec)
	if !strings.HasPrefix(c.Value, sessionPrefix) {
		t.Fatalf("ext-login cookie %q is not a session token", c.Value)
	}
	assertSessionCookieAttrs(t, c, c.Value)
	if rec := cookieRequest(t, srv, "GET", "/api/servers", c.Value, "", ""); rec.Code != 200 {
		t.Errorf("ext-issued session cookie GET /api/servers = %d, want 200", rec.Code)
	}
}
