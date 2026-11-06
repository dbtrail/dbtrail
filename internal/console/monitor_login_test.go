package console

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// The MySQL port's monitor login (monitor_login.go): the username and
// password a proxy's health checks log in with, opening no server.

func newMonitorTestServer(t *testing.T) (*Server, *Registry, string) {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s, _, path := newManagedFlashback(t, Config{Registry: reg})
	return s, reg, path
}

func TestMonitorLoginAPI(t *testing.T) {
	s, reg, path := newMonitorTestServer(t)
	prod := addLoginTestServer(t, reg, "prod")
	if _, err := reg.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}

	code, got, body := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-s3cret"}`)
	if code != 200 || got["monitor_user"] != "monitor" || got["has_monitor_password"] != true {
		t.Fatalf("PUT = %d %s", code, body)
	}
	if strings.Contains(body, "mon-s3cret") {
		t.Fatal("the answer carries the monitor password")
	}
	if f, err := LoadFlashbackFile(path); err != nil || f.MonitorUser != "monitor" || f.MonitorPassword != "mon-s3cret" {
		t.Fatalf("file: %+v %v", f, err)
	}
	_, got, body = doFlashback(t, s, "GET", "/api/flashback", "")
	if got["monitor_user"] != "monitor" || strings.Contains(body, "mon-s3cret") {
		t.Fatalf("GET = %s", body)
	}

	refusals := []struct {
		body string
		code int
	}{
		{`{}`, 400},
		{`{"user":"monitor2"}`, 400},
		{`{"user":"","password":"x"}`, 400},
		{`{"user":"m","password":"x","extra":1}`, 400},
		{`{"user":" m","password":"x"}`, 400},
		{`{"user":"m@copy","password":"x"}`, 400},
		{`{"user":"default","password":"x"}`, 409},
		{`{"user":"prod","password":"x"}`, 409},
		{`{"user":"PROD","password":"x"}`, 409},
		{`{"user":"` + prod.ID + `","password":"x"}`, 409},
		{`{"user":"App1","password":"x"}`, 409},
	}
	for _, c := range refusals {
		if code, _, body := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", c.body); code != c.code {
			t.Errorf("PUT %s = %d %s, want %d", c.body, code, body, c.code)
		}
	}
	if f, _ := LoadFlashbackFile(path); f.MonitorUser != "monitor" || f.MonitorPassword != "mon-s3cret" {
		t.Fatalf("a refused PUT changed the monitor login: %+v", f)
	}

	// The other direction: no server and no application login may take the
	// monitor's username.
	if _, err := reg.Add(ServerEntry{Name: "Monitor", DSN: "u:p@tcp(h:3306)/db"}); !errors.Is(err, ErrPortLoginConflict) {
		t.Errorf("a server named like the monitor login: %v", err)
	}
	if err := reg.Update(ServerEntry{ID: prod.ID, Name: "monitor", DSN: "u:p@tcp(h:3306)/db_prod"}); !errors.Is(err, ErrPortLoginConflict) {
		t.Errorf("a server renamed to the monitor login: %v", err)
	}
	if _, err := reg.SetPortLogin(prod.ID, "MONITOR", "x"); !errors.Is(err, ErrPortLoginConflict) {
		t.Errorf("an application login named like the monitor login: %v", err)
	}

	if code, got, body := doFlashback(t, s, "DELETE", "/api/flashback/monitor-login", ""); code != 200 || got["monitor_user"] != nil {
		t.Fatalf("DELETE = %d %s", code, body)
	}
	if f, _ := LoadFlashbackFile(path); f.MonitorUser != "" || f.MonitorPassword != "" {
		t.Fatalf("DELETE left %+v", f)
	}
	// Removed: the name is free again.
	if _, err := reg.Add(ServerEntry{Name: "monitor", DSN: "u:p@tcp(h:3306)/db"}); err != nil {
		t.Errorf("after DELETE a server named monitor: %v", err)
	}
	// And a second DELETE is a no-op.
	if code, _, body := doFlashback(t, s, "DELETE", "/api/flashback/monitor-login", ""); code != 200 {
		t.Fatalf("second DELETE = %d %s", code, body)
	}
}

func TestMonitorPasswordFor(t *testing.T) {
	s, reg, _ := newMonitorTestServer(t)
	addLoginTestServer(t, reg, "prod")
	if code, _, body := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"monpw"}`); code != 200 {
		t.Fatalf("PUT = %d %s", code, body)
	}
	if pw, ok, err := s.MonitorPasswordFor("monitor"); !ok || err != nil || pw != "monpw" {
		t.Fatalf("monitor: %q %v %v", pw, ok, err)
	}
	for _, u := range []string{"Monitor", "MONITOR", "monitor ", "", "prod", "nobody"} {
		if _, ok, err := s.MonitorPasswordFor(u); ok || err != nil {
			t.Errorf("MonitorPasswordFor(%q) = %v %v, want not the monitor", u, ok, err)
		}
	}
	// Its password is never one of the port's passwords for any username.
	for _, u := range []string{"monitor", "prod", "nobody"} {
		if got, _ := s.FlashbackPasswordsFor(u); strings.Contains(strings.Join(got, ","), "monpw") {
			t.Errorf("FlashbackPasswordsFor(%q) offers the monitor password: %q", u, got)
		}
	}
	// A closed port: the monitor login does not open it.
	s.flashback.mu.Lock()
	s.flashback.followsSwitch, s.flashback.saved.Enabled = true, false
	s.flashback.mu.Unlock()
	if _, ok, _ := s.MonitorPasswordFor("monitor"); ok {
		t.Fatal("port off: the monitor login still authenticates")
	}
}

// In a process that follows the port's settings file (the router): the
// monitor login is read from it; a hand-edited collision is refused; a port
// turned off or a file that cannot be read holds no monitor login; and a
// changed or removed login closes the connections opened with it, and no
// others.
func TestFollowFiles_monitorLogin(t *testing.T) {
	owner, srv, prod, portFile := followerServer(t)
	write := func(f FlashbackFile) {
		t.Helper()
		if err := saveFlashbackFile(portFile, &f); err != nil {
			t.Fatal(err)
		}
		srv.RefreshFollowedFiles()
	}
	write(FlashbackFile{Enabled: true, Password: "bfp_x", MonitorUser: "monitor", MonitorPassword: "m1"})
	if pw, ok, err := srv.MonitorPasswordFor("monitor"); !ok || err != nil || pw != "m1" {
		t.Fatalf("followed: %q %v %v", pw, ok, err)
	}

	monitorClosed, appClosed := 0, 0
	track := func() {
		t.Helper()
		if _, ok := srv.TrackLoginConn(MonitorLoginKey, srv.LoginEpoch(), func() { monitorClosed++ }); !ok {
			t.Fatal("track monitor")
		}
	}
	track()
	if _, err := owner.SetPortLogin(prod.ID, "app1", "pw1"); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if _, ok := srv.TrackLoginConn(prod.ID, srv.LoginEpoch(), func() { appClosed++ }); !ok {
		t.Fatal("track app")
	}

	// The same login again: nothing closes.
	write(FlashbackFile{Enabled: true, Password: "bfp_x", MonitorUser: "monitor", MonitorPassword: "m1"})
	if monitorClosed != 0 {
		t.Fatalf("an unchanged monitor login closed %d connections", monitorClosed)
	}
	// A new password: the monitor connection closes, the application's stays.
	write(FlashbackFile{Enabled: true, Password: "bfp_x", MonitorUser: "monitor", MonitorPassword: "m2"})
	if monitorClosed != 1 || appClosed != 0 {
		t.Fatalf("rotation closed monitor %d, app %d; want 1, 0", monitorClosed, appClosed)
	}
	if _, ok, _ := srv.MonitorPasswordFor("monitor"); !ok {
		t.Fatal("rotated: not accepted")
	}

	// A file that cannot be read: no monitor login, its connections closed.
	track()
	if err := os.WriteFile(portFile, []byte("enabled: [not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if _, ok, _ := srv.MonitorPasswordFor("monitor"); ok {
		t.Fatal("unreadable settings: the monitor login still authenticates")
	}
	if monitorClosed != 2 {
		t.Fatalf("unreadable settings closed %d monitor connections in all, want 2", monitorClosed)
	}

	// The port turned off: no monitor login.
	write(FlashbackFile{Enabled: false, Password: "bfp_x", MonitorUser: "monitor", MonitorPassword: "m2"})
	if _, ok, _ := srv.MonitorPasswordFor("monitor"); ok {
		t.Fatal("port off: the monitor login still authenticates")
	}

	// Edited by hand into a server's name: refused as ambiguous.
	write(FlashbackFile{Enabled: true, Password: "bfp_x", MonitorUser: prod.Name, MonitorPassword: "m3"})
	if _, ok, err := srv.MonitorPasswordFor(prod.Name); ok || !errors.Is(err, ErrAmbiguousMonitorLogin) {
		t.Fatalf("monitor login named like a server: %v %v, want ErrAmbiguousMonitorLogin", ok, err)
	}
}

// A monitor login edited by hand in the file of a process that does not
// follow it (watch) still closes the old login's connections when the next
// change from the web interface reads the file back and publishes it.
func TestMonitorLogin_handEditPublishedByTheNextChange(t *testing.T) {
	s, _, path := newMonitorTestServer(t)
	if code, _, body := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"m1"}`); code != 200 {
		t.Fatalf("PUT = %d %s", code, body)
	}
	closed := 0
	if _, ok := s.TrackLoginConn(MonitorLoginKey, s.LoginEpoch(), func() { closed++ }); !ok {
		t.Fatal("track")
	}
	f, err := LoadFlashbackFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f.MonitorPassword = "m2"
	if err := saveFlashbackFile(path, f); err != nil {
		t.Fatal(err)
	}
	if code, _, body := doFlashback(t, s, "POST", "/api/flashback/password", ""); code != 200 {
		t.Fatalf("new port password = %d %s", code, body)
	}
	if closed != 1 {
		t.Fatalf("the hand-edited monitor login closed %d connections, want 1", closed)
	}
	if pw, ok, _ := s.MonitorPasswordFor("monitor"); !ok || pw != "m2" {
		t.Fatalf("after the change: %q %v", pw, ok)
	}
}

// A file edited by hand into a username the API refuses is refused at the
// port too, and GET /api/flashback says why.
func TestMonitorLogin_handEditedShapeRefused(t *testing.T) {
	_, srv, prod, portFile := followerServer(t)
	for _, u := range []string{"default", "x@copy", " monitor", prod.Name} {
		if err := saveFlashbackFile(portFile, &FlashbackFile{Enabled: true, Password: "bfp_x", MonitorUser: u, MonitorPassword: "m"}); err != nil {
			t.Fatal(err)
		}
		srv.RefreshFollowedFiles()
		if _, ok, err := srv.MonitorPasswordFor(u); ok || !errors.Is(err, ErrAmbiguousMonitorLogin) {
			t.Errorf("monitor_user %q: %v %v, want refused", u, ok, err)
		}
		if why := srv.monitorProblem(u, "m"); why == "" {
			t.Errorf("monitor_user %q: no reason given", u)
		}
	}
}

// A server already named like the monitor login (a hand edit) still takes
// edits that keep its name; renaming another server to it is refused.
func TestMonitorLogin_reservationOnlyOnRename(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	mon := addLoginTestServer(t, reg, "monitor")
	other := addLoginTestServer(t, reg, "other")
	reg.setReservedUser(func() string { return "monitor" })
	mon.DSN = "u:p@tcp(h2:3306)/db_monitor"
	if err := reg.Update(mon); err != nil {
		t.Fatalf("an edit that keeps the name: %v", err)
	}
	other.Name = "Monitor"
	if err := reg.Update(other); !errors.Is(err, ErrPortLoginConflict) {
		t.Fatalf("a rename to the monitor login: %v", err)
	}
}
