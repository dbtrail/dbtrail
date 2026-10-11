package consoleapp

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
)

// saveMySQLPortFile writes the setting of the MySQL port inside the capture
// process, as its web interface does: the file the router used to follow.
func saveMySQLPortFile(t *testing.T, dir string, enabled bool, password string) {
	t.Helper()
	body := "version: 1\nenabled: false\n"
	if enabled {
		body = "version: 1\nenabled: true\n"
	}
	body += "listen: 127.0.0.1:3309\npassword: " + password + "\n"
	path := filepath.Join(dir, console.FlashbackFileName)
	if err := os.WriteFile(path+".tmp", []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

// connClosed reports whether the server end of conn is gone. An answer from
// the server, an error packet included, means the connection is alive: no
// server is registered here, so a ping is answered with error 1049.
func connClosed(conn *client.Conn) bool {
	err := conn.Ping()
	var me *gomysql.MyError
	return err != nil && !errors.As(err, &me)
}

// The router has a switch and a password of its own. The MySQL port inside
// the capture process has another, and nothing done to that one reaches the
// router: its password does not open the router, and turning it off or
// replacing its password does not close the router or the connections of
// the applications on it. Before, one panel did both, and a settings button
// meant for the other port disconnected every application.
func TestRouterPort_theMySQLPortsSwitchIsNotTheRouters(t *testing.T) {
	f := newRouterFiles(t)
	dir := filepath.Dir(f.servers)
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	addr := freeAddr(t)
	bound, _, said := startRouterSaying(t, srv, addr)

	// The MySQL port is on, with a password: the router still has no
	// credential, and does not listen.
	saveMySQLPortFile(t, dir, true, "the-other-ports-password")
	waitUntil(t, "the router to say it waits for a credential", func() bool { return strings.Contains(said(), "not listening yet") })
	for range 15 {
		if bound() != "" || accepts(addr) {
			t.Fatal("the router opened on the password of the MySQL port inside the capture process")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Its own switch turned on: it opens, for its own password only.
	f.savePortFile(t, true, "the-routers-own")
	waitUntil(t, "the router to open once its own password exists", func() bool { return bound() != "" })
	if code := mysqlLogin(t, addr, "the-routers-own"); code != 0 {
		t.Fatalf("login with the router's own password: MySQL error %d", code)
	}
	if code := mysqlLogin(t, addr, "the-other-ports-password"); code != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("login with the MySQL port's password: %d, want 1045", code)
	}
	app, err := client.Connect(addr, "default", "the-routers-own", "")
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	// Everything the other panel can do: a new password, off, on, gone.
	// The router stays open and the application stays connected. Each step
	// is given several of the router's looks at its files.
	steps := map[string]func(){
		"the MySQL port's password replaced": func() { saveMySQLPortFile(t, dir, true, "replaced") },
		"the MySQL port turned off":          func() { saveMySQLPortFile(t, dir, false, "replaced") },
		"the MySQL port turned on again":     func() { saveMySQLPortFile(t, dir, true, "replaced") },
		"the MySQL port's setting removed":   func() { os.Remove(filepath.Join(dir, console.FlashbackFileName)) },
	}
	for _, name := range []string{"the MySQL port's password replaced", "the MySQL port turned off", "the MySQL port turned on again", "the MySQL port's setting removed"} {
		steps[name]()
		srv.RefreshFollowedFiles()
		srv.RefreshFollowedFiles()
		time.Sleep(60 * time.Millisecond)
		if connClosed(app) {
			t.Fatalf("after %s, the application's connection to the router was closed", name)
		}
		if bound() == "" {
			t.Fatalf("after %s, the router closed its port", name)
		}
		if code := mysqlLoginQuiet(addr, "the-routers-own"); code != 0 {
			t.Fatalf("after %s, the router's own password no longer opens it: MySQL error %d", name, code)
		}
	}

	// Its own switch turned off does close it, with the connection on it.
	f.savePortFile(t, false, "the-routers-own")
	waitUntil(t, "the application's connection to be closed when the router is turned off", func() bool { return connClosed(app) })
	waitUntil(t, "the router's port to close", func() bool { return bound() == "" && !accepts(addr) })
}

// The whole path, with both real ends: the web interface of the capture
// process turns the router on, and a router started on the same servers
// file opens for the password that answer carried; a new password there
// puts out the connection made with the old one; off closes the port.
func TestRouterPort_followsTheSwitchTheConsoleSaves(t *testing.T) {
	f := newRouterFiles(t)
	web, err := console.New(console.Config{Listen: "127.0.0.1:8090", AuthPath: filepath.Join(filepath.Dir(f.servers), "web-auth.yaml"),
		MCPTokenPath: filepath.Join(filepath.Dir(f.servers), "web-mcp.yaml"), RouterSwitchPath: console.RouterSwitchPath(f.servers)})
	if err != nil {
		t.Fatal(err)
	}
	if r := web.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	// No access token: the operator signs in with a password, as in an
	// installation where the web interface manages the router.
	session := ""
	change := func(method, path, body string) (int, string) {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if session != "" {
			req.Header.Set("Authorization", "Bearer "+session)
		}
		web.Handler().ServeHTTP(rec, req)
		var got struct {
			Password string `json:"password"`
			Token    string `json:"token"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &got)
		if rec.Code != 200 {
			t.Fatalf("%s %s = %d %s", method, path, rec.Code, rec.Body)
		}
		if got.Token != "" {
			session = got.Token
		}
		return rec.Code, got.Password
	}
	change("POST", "/api/auth/setup", `{"username":"admin","password":"first-run-pass-1"}`)
	if session == "" {
		t.Fatal("no session after first-run setup")
	}

	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	addr := freeAddr(t)
	bound, _, _ := startRouterSaying(t, srv, addr)

	_, first := change("PUT", "/api/router", `{"enabled":true}`)
	if first == "" {
		t.Fatal("turning the router on answered with no password")
	}
	waitUntil(t, "the router to open after it is turned on in the web interface", func() bool { return bound() != "" })
	if code := mysqlLogin(t, addr, first); code != 0 {
		t.Fatalf("login with the password the web interface handed over: MySQL error %d", code)
	}
	app, err := client.Connect(addr, "default", first, "")
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	_, second := change("POST", "/api/router/password", ``)
	waitUntil(t, "the new password to open the router", func() bool { return mysqlLoginQuiet(addr, second) == 0 })
	waitUntil(t, "the connection made with the old password to be closed", func() bool { return connClosed(app) })
	if code := mysqlLoginSettled(t, addr, first); code != gomysql.ER_ACCESS_DENIED_ERROR {
		t.Fatalf("login with the replaced password: %d, want 1045", code)
	}

	change("PUT", "/api/router", `{"enabled":false}`)
	waitUntil(t, "the router's port to close after it is turned off in the web interface", func() bool { return bound() == "" && !accepts(addr) })
}

// The router says which credential opens its port: the web interface cannot
// tell from its own start, and offers its switch by this.
func TestRouterStatus_saysWhichCredentialItTook(t *testing.T) {
	st := routerStatusSettings()
	st.Token = ""
	if got := routerStatus(st, console.RouterNumbers{}, &routerCopySnapshots{}).Credential; got != console.RouterCredentialSaved {
		t.Errorf("started without a token: credential %q, want %q", got, console.RouterCredentialSaved)
	}
	st.Token = "tok"
	doc := routerStatus(st, console.RouterNumbers{}, &routerCopySnapshots{})
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Credential != console.RouterCredentialToken || strings.Contains(string(raw), `"tok"`) {
		t.Errorf("started with a token: credential %q in %s, want %q and never the token", doc.Credential, raw, console.RouterCredentialToken)
	}
}
