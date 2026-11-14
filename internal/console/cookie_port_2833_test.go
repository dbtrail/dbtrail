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
		{"dbtrail.example.com", "bintrail_session"},     // behind a proxy: no port in the address
		{"dbtrail.example.com:443", "bintrail_session_443"},
		{"", "bintrail_session"},
		{"127.0.0.1:", "bintrail_session"},
		{"127.0.0.1:http", "bintrail_session"},          // not a number: never part of a cookie's name
		{"127.0.0.1:80;x=y", "bintrail_session"},        // nor anything a client could smuggle in
		{"127.0.0.1:123456", "bintrail_session"},        // longer than a port
		{"127.0.0.1:８０９０", "bintrail_session"},        // digits that are not ASCII
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
	if !cleared["bintrail_session_8091"] || !cleared["bintrail_session"] {
		t.Errorf("logout cleared %v, want this port's name and the plain one", cleared)
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
	// And a stale port cookie does not hide... nothing: it is the one read.
	bad := &http.Cookie{Name: "bintrail_session_8090", Value: "not-a-session"}
	if code := authInfoWith(t, srv, "127.0.0.1:8090", bad, old); code == 200 {
		t.Error("a bad port cookie fell back to the plain one: the port's name, when present, is the one that counts")
	}
}
