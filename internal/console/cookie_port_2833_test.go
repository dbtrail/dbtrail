package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The login cookie is named for the port the browser opened (#2833): two web
// interfaces on one host and two ports each keep their own.

func TestSessionCookieNameFor(t *testing.T) {
	for _, tc := range []struct{ host, want string }{
		{"127.0.0.1:8090", "bintrail_session_8090"},
		{"127.0.0.1:8091", "bintrail_session_8091"},
		{"localhost:8090", "bintrail_session_8090"},
		{"[::1]:8090", "bintrail_session_8090"},
		{"dbtrail.example.com", "bintrail_session"}, // behind a proxy: no port in the address
		{"dbtrail.example.com:443", "bintrail_session_443"},
		{"", "bintrail_session"},
		{"127.0.0.1:", "bintrail_session"},
		{"127.0.0.1:http", "bintrail_session"},   // not a number: never part of a cookie's name
		{"127.0.0.1:80;x=y", "bintrail_session"}, // nor anything a client could smuggle in
		{"127.0.0.1:123456", "bintrail_session"}, // longer than a port
		{"127.0.0.1:８０９０", "bintrail_session"},   // digits that are not ASCII
		{"::1", "bintrail_session"},              // an IPv6 address with no brackets: no port to read
		{"[::1]", "bintrail_session"},
		{"a:b:c", "bintrail_session"},
		{"host:80:90", "bintrail_session"},
	} {
		r := httptest.NewRequest("GET", "http://x/", nil)
		r.Host = tc.host
		if got := sessionCookieNameFor(r); got != tc.want {
			t.Errorf("host %q: %q, want %q", tc.host, got, tc.want)
		}
	}
}

// loginOn signs in through the address host and returns the cookie set.
func loginOn(t *testing.T, srv *Server, host string) *http.Cookie {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://"+host+"/api/auth/login", strings.NewReader(`{"username":"admin","password":"correct-horse-battery"}`))
	req.Host = host
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("login on %s = %d %s", host, rec.Code, rec.Body.String())
	}
	cs := rec.Result().Cookies()
	if len(cs) != 1 {
		t.Fatalf("login on %s set %d cookies, want 1: %v", host, len(cs), rec.Header().Values("Set-Cookie"))
	}
	return cs[0]
}

// authInfoWith asks who is signed in, sending the cookies a browser would
// send to host (it sends every cookie of the host, whatever the port).
func authInfoWith(t *testing.T, srv *Server, host string, cookies ...*http.Cookie) int {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://"+host+"/api/servers", nil)
	req.Host = host
	for _, c := range cookies {
		req.AddCookie(&http.Cookie{Name: c.Name, Value: c.Value})
	}
	srv.Handler().ServeHTTP(rec, req)
	return rec.Code
}

// Two installations on one machine: each console's login lives under its own
// name, so the browser sending both cookies to both lets each in on its own,
// and signing out of one leaves the other signed in.
func TestSessionCookie_twoConsolesOnOneHostKeepTheirOwnLogin(t *testing.T) {
	first, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	second, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	a := loginOn(t, first, "127.0.0.1:8090")
	b := loginOn(t, second, "127.0.0.1:8091")
	if a.Name != "bintrail_session_8090" || b.Name != "bintrail_session_8091" {
		t.Fatalf("cookie names %q and %q", a.Name, b.Name)
	}
	if a.Name == b.Name {
		t.Fatal("the two consoles set a cookie of the same name: the second login overwrites the first")
	}
	// The browser holds both and sends both to each port.
	if code := authInfoWith(t, first, "127.0.0.1:8090", a, b); code != 200 {
		t.Errorf("the first console with both cookies: %d", code)
	}
	if code := authInfoWith(t, second, "127.0.0.1:8091", a, b); code != 200 {
		t.Errorf("the second console with both cookies: %d", code)
	}
	// One console's cookie alone does not open the other.
	if code := authInfoWith(t, second, "127.0.0.1:8091", a); code == 200 {
		t.Error("the first console's cookie opened the second")
	}

	// Signing out of the second clears its own name (and the plain one a
	// login from before this change may have left), never the first's.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8091/api/auth/logout", strings.NewReader("{}"))
	req.Host = "127.0.0.1:8091"
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: a.Name, Value: a.Value})
	req.AddCookie(&http.Cookie{Name: b.Name, Value: b.Value})
	second.Handler().ServeHTTP(rec, req)
	cleared := map[string]bool{}
	for _, c := range rec.Result().Cookies() {
		if c.MaxAge < 0 {
			cleared[c.Name] = true
		}
	}
	if !cleared["bintrail_session_8091"] {
		t.Errorf("logout cleared %v, want this port's name", cleared)
	}
	if cleared["bintrail_session"] {
		t.Error("logout cleared the plain name, which held no session of this console: another console on the host may be signed in under it")
	}
	if cleared["bintrail_session_8090"] {
		t.Error("logout on the second console cleared the first console's cookie")
	}
	if code := authInfoWith(t, first, "127.0.0.1:8090", a); code != 200 {
		t.Errorf("the first console after the second signed out: %d", code)
	}
	if code := authInfoWith(t, second, "127.0.0.1:8091", b); code == 200 {
		t.Error("the second console still lets its signed-out session in")
	}
}

// A login made before this change holds the plain name: it still works, and
// the next login moves it to the port's name.
func TestSessionCookie_aLoginFromBeforeStillWorks(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	c := loginOn(t, srv, "127.0.0.1:8090")
	old := &http.Cookie{Name: sessionCookieName, Value: c.Value}
	if code := authInfoWith(t, srv, "127.0.0.1:8090", old); code != 200 {
		t.Fatalf("a session under the plain name: %d", code)
	}
	// A stale plain cookie beside a good port cookie: the port's wins.
	stale := &http.Cookie{Name: sessionCookieName, Value: "not-a-session"}
	if code := authInfoWith(t, srv, "127.0.0.1:8090", stale, c); code != 200 {
		t.Errorf("a good port cookie beside a stale plain one: %d", code)
	}
	// A session that ended under the port's name does not shut out a good
	// one under the plain name (what a login through a provider sets).
	ended := &http.Cookie{Name: "bintrail_session_8090", Value: "not-a-session"}
	if code := authInfoWith(t, srv, "127.0.0.1:8090", ended, old); code != 200 {
		t.Errorf("a good plain cookie beside an ended port cookie: %d", code)
	}
	// Neither is a session: refused, as one bad cookie is.
	if code := authInfoWith(t, srv, "127.0.0.1:8090", ended, stale); code == 200 {
		t.Error("two cookies that are no session were let in")
	}
}

// A login through an installed provider is named for the port too: the
// provider's routes are served through a writer that remembers the request.
// A provider that hands the issuer a writer of its own that hides it gets
// the plain name, which every port reads.
func TestSessionCookie_aProviderLoginIsNamedForThePort(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	issue := srv.extSessionIssuer()
	var token string
	provider := withRequestWriter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var err error
		if token, _, err = issue(w, "someone@example.com", nil); err != nil {
			t.Fatal(err)
		}
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8091/auth/ext/callback", nil)
	req.Host = "127.0.0.1:8091"
	provider.ServeHTTP(rec, req)
	cs := rec.Result().Cookies()
	if len(cs) != 1 || cs[0].Name != "bintrail_session_8091" || cs[0].Value != token {
		t.Fatalf("cookies set through the provider's routes: %v", rec.Header().Values("Set-Cookie"))
	}
	if code := authInfoWith(t, srv, "127.0.0.1:8091", cs[0]); code != 200 {
		t.Errorf("the provider's session: %d", code)
	}

	// Under one more wrapper that can be unwrapped: still found.
	rec = httptest.NewRecorder()
	withRequestWriter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, _, err := issue(unwrappable{w}, "someone@example.com", nil); err != nil {
			t.Fatal(err)
		}
	})).ServeHTTP(rec, req)
	if cs := rec.Result().Cookies(); len(cs) != 1 || cs[0].Name != "bintrail_session_8091" {
		t.Fatalf("through a wrapper: %v", rec.Header().Values("Set-Cookie"))
	}

	// A bare writer, as a provider's own might be: the plain name.
	rec = httptest.NewRecorder()
	if _, _, err := issue(rec, "someone@example.com", nil); err != nil {
		t.Fatal(err)
	}
	if cs := rec.Result().Cookies(); len(cs) != 1 || cs[0].Name != sessionCookieName {
		t.Fatalf("with no request to read: %v", rec.Header().Values("Set-Cookie"))
	}
}

type unwrappable struct{ http.ResponseWriter }

func (u unwrappable) Unwrap() http.ResponseWriter { return u.ResponseWriter }

// Two people in one browser: one signs in with a password, then another
// through the provider. A new tab is the second person, the one who signed
// in last, never the first with the second's cookie ignored.
func TestSessionCookie_theLastLoginIsTheOneANewTabGets(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	first := loginOn(t, srv, "127.0.0.1:8090")
	var second string
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/auth/ext/callback", nil)
	req.Host = "127.0.0.1:8090"
	issue := srv.extSessionIssuer()
	withRequestWriter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var err error
		if second, _, err = issue(w, "second@example.com", nil); err != nil {
			t.Fatal(err)
		}
	})).ServeHTTP(rec, req)
	cs := rec.Result().Cookies()
	if len(cs) != 1 || cs[0].Name != first.Name {
		t.Fatalf("the second login did not replace the first one's cookie (%s): %v", first.Name, rec.Header().Values("Set-Cookie"))
	}
	// The browser now holds ONE cookie of that name, the second login's.
	r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers", nil)
	r.Host = "127.0.0.1:8090"
	r.AddCookie(&http.Cookie{Name: cs[0].Name, Value: cs[0].Value})
	if got := srv.sessionCookieToken(r); got != second {
		t.Fatalf("a new tab is judged by another session than the last login's")
	}
	if identity, _, ok := srv.sessions.Lookup(second); !ok || identity != "second@example.com" {
		t.Fatalf("the session a new tab gets is %q (ok=%v), want the second person's", identity, ok)
	}
}

// Signing out of one console leaves alone a plain-named cookie that is not
// its own session: it may be another console's on the same host.
func TestSessionCookie_logoutLeavesAnotherConsolesPlainCookie(t *testing.T) {
	a, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	b, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	// A login on A from before the change: under the plain name.
	recA := httptest.NewRecorder()
	tokA, _, err := a.extSessionIssuer()(recA, "someone@example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	plainA := &http.Cookie{Name: sessionCookieName, Value: tokA}
	mine := loginOn(t, b, "127.0.0.1:8091")

	out := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8091/api/auth/logout", strings.NewReader("{}"))
	req.Host = "127.0.0.1:8091"
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: mine.Name, Value: mine.Value})
	req.AddCookie(plainA)
	b.Handler().ServeHTTP(out, req)
	for _, c := range out.Result().Cookies() {
		if c.Name == sessionCookieName && c.MaxAge < 0 {
			t.Fatal("signing out of the second console cleared the plain cookie, which is the first console's login")
		}
	}
	if !a.sessions.Validate(tokA) {
		t.Fatal("the first console's session ended when the second signed out")
	}
	if code := authInfoWith(t, a, "127.0.0.1:8090", plainA); code != 200 {
		t.Errorf("the first console after the second signed out: %d", code)
	}

	// The same console's own plain-named session IS cleared by its logout.
	recOwn := httptest.NewRecorder()
	own, _, err := b.extSessionIssuer()(recOwn, "someone@example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	out = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "http://127.0.0.1:8091/api/auth/logout", strings.NewReader("{}"))
	req.Host = "127.0.0.1:8091"
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: own})
	b.Handler().ServeHTTP(out, req)
	clearedPlain := false
	for _, c := range out.Result().Cookies() {
		if c.Name == sessionCookieName && c.MaxAge < 0 {
			clearedPlain = true
		}
	}
	if !clearedPlain || b.sessions.Validate(own) {
		t.Errorf("a console's own plain-named session: cleared=%v alive=%v, want cleared and ended", clearedPlain, b.sessions.Validate(own))
	}
}

// Signing out ends every session the browser holds for this address: the one
// under the port's name (a password login) and the one under the plain name
// (a provider login). Neither token works afterwards, as a cookie or as a
// Bearer.
func TestSessionCookie_logoutEndsBothSessions(t *testing.T) {
	srv, _ := newPasswordServer(t, "", "admin", "correct-horse-battery")
	byPassword := loginOn(t, srv, "127.0.0.1:8090")
	rec := httptest.NewRecorder()
	providerToken, _, err := srv.extSessionIssuer()(rec, "someone@example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	byProvider := &http.Cookie{Name: sessionCookieName, Value: providerToken}
	if !srv.sessions.Validate(byPassword.Value) || !srv.sessions.Validate(providerToken) {
		t.Fatal("the two sessions are not both alive before the logout")
	}

	out := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/auth/logout", strings.NewReader("{}"))
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: byPassword.Name, Value: byPassword.Value})
	req.AddCookie(byProvider)
	srv.Handler().ServeHTTP(out, req)

	if srv.sessions.Validate(byPassword.Value) {
		t.Error("the session under the port's name is still alive after signing out")
	}
	if srv.sessions.Validate(providerToken) {
		t.Error("the session under the plain name is still alive after signing out")
	}
	for _, tok := range []string{byPassword.Value, providerToken} {
		r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/servers", nil)
		r.Host = "127.0.0.1:8090"
		r.Header.Set("Authorization", "Bearer "+tok)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, r)
		if w.Code == 200 {
			t.Error("a token of a signed-out session still opens the console as a Bearer")
		}
	}
}
