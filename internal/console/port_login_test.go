package console

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A server's application login on the MySQL port (port_login.go): the
// username and password a proxy such as ProxySQL logs in with, the ones the
// application already uses, opening the port for that server alone.

func addLoginTestServer(t *testing.T, reg *Registry, name string) ServerEntry {
	t.Helper()
	e, err := reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/db_" + name})
	if err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	return e
}

func TestPortLoginShape(t *testing.T) {
	ok := []struct{ user, pw string }{
		{"app1", "secret"},
		{"wp_user", `it's a "pass\word" ñ`},
		{"usuario_ñandú", "x"},
		{strings.Repeat("a", 32), "x"},
		{"copy", "x"},      // only the suffix is refused
		{"app@copy2", "x"}, // not at the end
	}
	for _, c := range ok {
		if err := validatePortLoginShape(c.user, c.pw); err != nil {
			t.Errorf("(%q, %q) refused: %v", c.user, c.pw, err)
		}
	}
	bad := []struct{ user, pw, why string }{
		{"", "x", "empty user"},
		{"app", "", "empty password"},
		{" app", "x", "leading space"},
		{"app ", "x", "trailing space"},
		{"app\t", "x", "trailing tab"},
		{strings.Repeat("a", 33), "x", "33 characters"},
		{"ap\x01p", "x", "control character"},
		{"app\x00", "x", "NUL in user"},
		{"app", "p\x00w", "NUL in password"},
		{"app", strings.Repeat("p", 513), "password too long"},
		{"default", "x", "the boot id"},
		{"DEFAULT", "x", "the boot id, other case"},
		{"shop@copy", "x", "the copy-only suffix"},
		{"shop@COPY", "x", "the copy-only suffix, other case"},
		{"\xff\xfe", "x", "invalid UTF-8"},
	}
	for _, c := range bad {
		if err := validatePortLoginShape(c.user, c.pw); err == nil {
			t.Errorf("%s (%q, %q) accepted", c.why, c.user, c.pw)
		}
	}
}

func TestSetPortLogin_conflicts(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	prod := addLoginTestServer(t, reg, "prod")
	staging := addLoginTestServer(t, reg, "staging")

	if _, err := reg.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatalf("first login: %v", err)
	}
	// Saving the same login again on the same server is fine.
	if _, err := reg.SetPortLogin(prod.ID, "app1", "pw1b"); err != nil {
		t.Fatalf("the same server saving its own login again: %v", err)
	}
	// Its own name and id select the same server: allowed.
	if _, err := reg.SetPortLogin(prod.ID, "prod", "pw"); err != nil {
		t.Fatalf("login equal to its own name: %v", err)
	}
	if _, err := reg.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	conflicts := []struct{ user, why string }{
		{"app1", "another server's login"},
		{"APP1", "another server's login, other case"},
		{"prod", "another server's name"},
		{"Prod", "another server's name, other case"},
		{prod.ID, "another server's id"},
		{strings.ToUpper(prod.ID), "another server's id, other case"},
	}
	for _, c := range conflicts {
		_, err := reg.SetPortLogin(staging.ID, c.user, "pw")
		if !errors.Is(err, ErrPortLoginConflict) {
			t.Errorf("%s (%q) on staging: err = %v, want ErrPortLoginConflict", c.why, c.user, err)
		}
	}
	if e, _ := reg.Get(staging.ID); e.PortUser != "" {
		t.Fatalf("a refused login was stored: %+v", e)
	}
	// The other way round: a server named like an existing login.
	if _, err := reg.Add(ServerEntry{Name: "App1", DSN: "u:p@tcp(h:3306)/x"}); !errors.Is(err, ErrPortLoginConflict) {
		t.Errorf("adding a server named like a login: err = %v", err)
	}
	renamed := staging
	renamed.Name = "app1"
	if err := reg.Update(renamed); !errors.Is(err, ErrPortLoginConflict) {
		t.Errorf("renaming a server to a login: err = %v", err)
	}
	// prod may keep a name equal to its own login.
	if _, err := reg.SetPortLogin(prod.ID, "prod", "pw"); err != nil {
		t.Fatal(err)
	}
	if err := reg.Update(prod); err != nil {
		t.Errorf("saving a server whose name is its own login: %v", err)
	}
	// A password with no username, and an unknown server.
	if _, err := reg.SetPortLogin(staging.ID, "", "pw"); !errors.Is(err, errPortLoginInvalid) {
		t.Errorf("password without user: err = %v", err)
	}
	if _, err := reg.SetPortLogin("nope", "x", "y"); !errors.Is(err, ErrUnknownServer) {
		t.Errorf("unknown server: err = %v", err)
	}
}

// The login is registry-owned: Add never stores one, a generic edit keeps the
// stored one, and it survives a reload of the file.
func TestSetPortLogin_registryOwned(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	added, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/db", PortUser: "sneaky", PortPassword: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if added.PortUser != "" || added.PortPassword != "" {
		t.Fatalf("Add stored a login: %+v", added)
	}
	if _, err := reg.SetPortLogin(added.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	edit, _ := reg.Get(added.ID)
	edit.PortUser, edit.PortPassword, edit.Schemas = "", "", "shop"
	if err := reg.Update(edit); err != nil {
		t.Fatal(err)
	}
	if e, _ := reg.Get(added.ID); e.PortUser != "app1" || e.PortPassword != "pw1" || e.Schemas != "shop" {
		t.Fatalf("a generic edit changed the login or lost the edit: %+v", e)
	}
	again, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := again.Get(added.ID); e.PortUser != "app1" || e.PortPassword != "pw1" {
		t.Fatalf("the login did not survive a reload: %+v", e)
	}
	if _, err := reg.SetPortLogin(added.ID, "", ""); err != nil {
		t.Fatal(err)
	}
	if e, _ := reg.Get(added.ID); e.PortUser != "" || e.PortPassword != "" {
		t.Fatalf("removing the login left %+v", e)
	}
}

func TestPortLoginMatch(t *testing.T) {
	entries := []ServerEntry{
		{ID: "aaaa", Name: "prod", PortUser: "app1", PortPassword: "pw1"},
		{ID: "bbbb", Name: "staging", PortUser: "app2", PortPassword: "pw2"},
	}
	if id, ok, amb := portLoginMatch(entries, "app1"); !ok || amb || id != "aaaa" {
		t.Fatalf("app1 = %q %v %v", id, ok, amb)
	}
	// MySQL compares usernames by case: the port matches exactly.
	if _, ok, _ := portLoginMatch(entries, "App1"); ok {
		t.Fatal("App1 matched app1")
	}
	for _, u := range []string{"", "prod", "nope", "aaaa"} {
		if _, ok, amb := portLoginMatch(entries, u); ok || amb {
			t.Fatalf("%q matched a login", u)
		}
	}
	// A registry edited by hand into ambiguity is refused, never resolved
	// by whichever comes first.
	twoLogins := append(entries, ServerEntry{ID: "cccc", Name: "dev", PortUser: "app1"})
	if _, ok, amb := portLoginMatch(twoLogins, "app1"); ok || !amb {
		t.Fatal("two servers with one login were not refused")
	}
	loginIsAName := append(entries, ServerEntry{ID: "cccc", Name: "app2"})
	if _, ok, amb := portLoginMatch(loginIsAName, "app2"); ok || !amb {
		t.Fatal("a login that is another server's name was not refused")
	}
	loginIsAnID := append(entries, ServerEntry{ID: "app2", Name: "dev"})
	if _, ok, amb := portLoginMatch(loginIsAnID, "app2"); ok || !amb {
		t.Fatal("a login that is another server's id was not refused")
	}
	// Its own name: the same server, not ambiguous.
	own := []ServerEntry{{ID: "aaaa", Name: "app1", PortUser: "app1"}}
	if id, ok, amb := portLoginMatch(own, "app1"); !ok || amb || id != "aaaa" {
		t.Fatalf("own name: %q %v %v", id, ok, amb)
	}
}

func TestFlashbackPasswordsFor(t *testing.T) {
	srv := newRegistryServer(t) // token "t"
	prod := addLoginTestServer(t, srv.cm.reg, "prod")
	addLoginTestServer(t, srv.cm.reg, "staging")
	if _, err := srv.cm.reg.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		user string
		want []string
	}{
		{"app1", []string{"t", "pw1"}},
		{"App1", []string{"t"}},    // exact, like MySQL
		{"prod", []string{"t"}},    // the server's name: the port's passwords only
		{"staging", []string{"t"}}, // another server: never pw1
		{"nobody", []string{"t"}},  // unknown: the same answer as a wrong password
		{"", []string{"t"}},
	}
	for _, c := range cases {
		if got := srv.FlashbackPasswordsFor(c.user); strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Errorf("FlashbackPasswordsFor(%q) = %q, want %q", c.user, got, c.want)
		}
	}
	// A login never opens a closed port.
	srv.flashback.mu.Lock()
	srv.flashback.followsSwitch, srv.flashback.saved.Enabled = true, false
	srv.flashback.mu.Unlock()
	if got := srv.FlashbackPasswordsFor("app1"); got != nil {
		t.Fatalf("port off: FlashbackPasswordsFor(app1) = %q", got)
	}
}

// Resolving a username refuses an ambiguous login before anything is opened,
// and a username that is nothing at all is an unknown server.
func TestResolveFlashback_loginRefusals(t *testing.T) {
	srv := newRegistryServer(t)
	prod := addLoginTestServer(t, srv.cm.reg, "prod")
	if _, err := srv.cm.reg.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.ResolveFlashback(t.Context(), "nobody"); !errors.Is(err, ErrUnknownServer) {
		t.Fatalf("unknown: %v", err)
	}
	// Hand-edit the registry into ambiguity: another server named app1.
	srv.cm.reg.mu.Lock()
	srv.cm.reg.file.Servers = append(srv.cm.reg.file.Servers, ServerEntry{ID: "cccc", Name: "app1", DSN: "u:p@tcp(h:3306)/x"})
	srv.cm.reg.mu.Unlock()
	if _, err := srv.ResolveFlashback(t.Context(), "app1"); !errors.Is(err, ErrAmbiguousPortLogin) {
		t.Fatalf("ambiguous: %v", err)
	}
	if got := srv.FlashbackPasswordsFor("app1"); strings.Join(got, ",") != "t" {
		t.Fatalf("an ambiguous login still authenticates with its password: %q", got)
	}
}

func TestLoginConnSet(t *testing.T) {
	var l loginConnSet
	epoch := l.current()
	closed := 0
	untrack, ok := l.track("aaaa", epoch, func() { closed++ })
	if !ok {
		t.Fatal("track at the current epoch refused")
	}
	// A change to another server's login moves the epoch: a connection
	// that read the old one is refused, and aaaa's stays open.
	if n := l.drop("bbbb"); n != 0 || closed != 0 {
		t.Fatalf("drop(bbbb) closed %d", closed)
	}
	if _, ok := l.track("aaaa", epoch, func() {}); ok {
		t.Fatal("a connection that read the epoch before a change was tracked")
	}
	// Its own server's change closes it.
	if n := l.drop("aaaa"); n != 1 || closed != 1 {
		t.Fatalf("drop(aaaa) = %d, closed %d", n, closed)
	}
	untrack() // after the drop: harmless
	epoch = l.current()
	untrack, _ = l.track("aaaa", epoch, func() { closed++ })
	untrack()
	if n := l.drop("aaaa"); n != 0 || closed != 1 {
		t.Fatalf("an untracked connection was closed: %d %d", n, closed)
	}
}

// Another process changing the login in the file (the router follows it)
// closes the connections opened with it.
func TestApplyRegistryChange_loginClosesConns(t *testing.T) {
	srv := newRegistryServer(t)
	prod := addLoginTestServer(t, srv.cm.reg, "prod")
	closed := 0
	if _, ok := srv.TrackLoginConn(prod.ID, srv.loginConns.current(), func() { closed++ }); !ok {
		t.Fatal(err0)
	}
	changed := prod
	changed.PortUser, changed.PortPassword = "app1", "pw1"
	srv.applyRegistryChange(RegistryChange{Old: prod, New: changed})
	if closed != 1 {
		t.Fatalf("a login change closed %d connections, want 1", closed)
	}
	// A change that leaves the login alone closes nothing.
	if _, ok := srv.TrackLoginConn(prod.ID, srv.loginConns.current(), func() { closed++ }); !ok {
		t.Fatal(err0)
	}
	other := changed
	other.Schemas = "shop"
	srv.applyRegistryChange(RegistryChange{Old: changed, New: other})
	if closed != 1 {
		t.Fatalf("an unrelated change closed a login connection")
	}
}

var err0 = errors.New("track refused at the current epoch")

func TestPortLoginAPI(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	other := createNamedTestServer(t, srv, "staging")
	path := "/api/servers/" + id + "/port-login"

	rec, out := doServersReq(t, srv, "PUT", path, `{"user":"app1","password":"s3cr3t-pw"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT = %d %s", rec.Code, out)
	}
	var d portLoginDTO
	if err := json.Unmarshal(out, &d); err != nil || d.PortUser != "app1" || !d.HasPortPassword {
		t.Fatalf("PUT answer %s (%v)", out, err)
	}
	// The list says the login, never the password.
	rec, out = doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 || !strings.Contains(string(out), `"port_user":"app1"`) || !strings.Contains(string(out), `"has_port_password":true`) {
		t.Fatalf("list = %d %s", rec.Code, out)
	}
	if strings.Contains(string(out), "s3cr3t-pw") {
		t.Fatal("the list carries the login's password")
	}
	rec, out = doServersReq(t, srv, "GET", "/api/servers/"+id, "")
	if strings.Contains(string(out), "s3cr3t-pw") {
		t.Fatalf("GET one carries the password: %d", rec.Code)
	}

	refusals := []struct {
		path, method, body string
		code               int
	}{
		{path, "PUT", `{}`, 400},
		{path, "PUT", `{"user":"app1"}`, 400},
		{path, "PUT", `{"user":"","password":"x"}`, 400},
		{path, "PUT", `{"user":"app1","password":"x","extra":1}`, 400},
		{path, "PUT", `{"user":" app1","password":"x"}`, 400},
		{path, "PUT", `{"user":"staging","password":"x"}`, 409},
		{"/api/servers/" + other + "/port-login", "PUT", `{"user":"App1","password":"x"}`, 409},
		{"/api/servers/default/port-login", "PUT", `{"user":"x","password":"y"}`, 409},
		{"/api/servers/nope/port-login", "PUT", `{"user":"x","password":"y"}`, 404},
	}
	for _, c := range refusals {
		rec, out := doServersReq(t, srv, c.method, c.path, c.body)
		if rec.Code != c.code {
			t.Errorf("%s %s %s = %d %s, want %d", c.method, c.path, c.body, rec.Code, out, c.code)
		}
	}
	if e, _ := srv.cm.reg.Get(id); e.PortUser != "app1" || e.PortPassword != "s3cr3t-pw" {
		t.Fatalf("a refused PUT changed the login: %+v", e)
	}

	rec, out = doServersReq(t, srv, "DELETE", path, "")
	if rec.Code != 200 {
		t.Fatalf("DELETE = %d %s", rec.Code, out)
	}
	if e, _ := srv.cm.reg.Get(id); e.PortUser != "" || e.PortPassword != "" {
		t.Fatalf("DELETE left %+v", e)
	}
}

func createNamedTestServer(t *testing.T, srv *Server, name string) string {
	t.Helper()
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"`+name+`","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != 201 {
		t.Fatalf("create %s = %d %s", name, rec.Code, body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	return created.ID
}

// While the followed registry file cannot be read, no application login
// opens the port and the connections opened with one are closed: what this
// process still holds may be a login removed since. Once the file reads
// again, the login works again.
func TestFollowFiles_unreadableRegistryRefusesLogins(t *testing.T) {
	owner, srv, prod, path := followerServerWithPath(t)
	if _, err := owner.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if got := srv.FlashbackPasswordsFor("app1"); strings.Join(got, ",") != "tok,pw1" {
		t.Fatalf("before: %q", got)
	}
	closed := 0
	if _, ok := srv.TrackLoginConn(prod.ID, srv.LoginEpoch(), func() { closed++ }); !ok {
		t.Fatal(err0)
	}
	good := mustRead(t, path)
	if err := os.WriteFile(path, []byte("servers: [this is: not: yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if got := srv.FlashbackPasswordsFor("app1"); strings.Join(got, ",") != "tok" {
		t.Fatalf("unreadable registry: the login still authenticates: %q", got)
	}
	if closed != 1 {
		t.Fatalf("unreadable registry closed %d login connections, want 1", closed)
	}
	// Still unreadable: nothing more happens.
	srv.RefreshFollowedFiles()
	if closed != 1 {
		t.Fatalf("a second look closed again: %d", closed)
	}
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if got := srv.FlashbackPasswordsFor("app1"); strings.Join(got, ",") != "tok,pw1" {
		t.Fatalf("readable again: %q", got)
	}
}

// followerServerWithPath is followerServer with the registry file's path.
func followerServerWithPath(t *testing.T) (*Registry, *Server, ServerEntry, string) {
	t.Helper()
	owner, srv, prod, portFile := followerServer(t)
	return owner, srv, prod, filepath.Join(filepath.Dir(portFile), "console-servers.yaml")
}

// A server created or renamed through the API with the name of another
// server's login is a conflict (409), not a server error.
func TestServersAPI_nameThatIsALoginIs409(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv) // "prod"
	other := createNamedTestServer(t, srv, "staging")
	if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/port-login", `{"user":"app1","password":"pw"}`); rec.Code != 200 {
		t.Fatalf("PUT login = %d %s", rec.Code, out)
	}
	rec, out := doServersReq(t, srv, "POST", "/api/servers", `{"name":"App1","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != 409 {
		t.Errorf("create named like a login = %d %s, want 409", rec.Code, out)
	}
	rec, out = doServersReq(t, srv, "PUT", "/api/servers/"+other, `{"name":"app1","host":"h","user":"u","dbname":"db"}`)
	if rec.Code != 409 {
		t.Errorf("rename to a login = %d %s, want 409", rec.Code, out)
	}
}
