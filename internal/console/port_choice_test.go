package console

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"go.yaml.in/yaml/v2"
)

// Whether somebody chose that the MySQL port is on or off is written in its
// settings file (port_not_chosen, routing_not_chosen), since the file's
// existing says nothing about it: FlashbackFile.portChosen, routingChosen.

func TestFlashbackFile_whatCountsAsChosen(t *testing.T) {
	for _, tc := range []struct {
		name          string
		f             FlashbackFile
		port, routing bool
	}{
		{"no file", FlashbackFile{PortNotChosen: true, RoutingNotChosen: true}, false, false},
		{"a file from before the marks, off", FlashbackFile{}, true, true},
		{"a file from before the marks, with a monitor login", FlashbackFile{MonitorUser: "m", MonitorPassword: "p"}, true, true},
		{"marked, with a monitor login and a password", FlashbackFile{PortNotChosen: true, RoutingNotChosen: true, MonitorUser: "m", MonitorPassword: "p", Password: "bfp_x"}, false, false},
		{"Engine Routing chosen off, port not", FlashbackFile{PortNotChosen: true}, false, true},
		{"Engine Routing chosen on, port not", FlashbackFile{PortNotChosen: true, Routing: true}, false, true},
		{"Engine Routing on under its mark", FlashbackFile{PortNotChosen: true, RoutingNotChosen: true, Routing: true}, false, true},
		{"on under the marks", FlashbackFile{PortNotChosen: true, RoutingNotChosen: true, Enabled: true}, true, true},
		{"an address under the marks: it was on once", FlashbackFile{PortNotChosen: true, RoutingNotChosen: true, Listen: "127.0.0.1:3309"}, true, true},
		{"turned off", FlashbackFile{Listen: "127.0.0.1:3309", Password: "bfp_x"}, true, true},
	} {
		if got := tc.f.portChosen(); got != tc.port {
			t.Errorf("%s: portChosen %v, want %v", tc.name, got, tc.port)
		}
		if got := tc.f.routingChosen(); got != tc.routing {
			t.Errorf("%s: routingChosen %v, want %v", tc.name, got, tc.routing)
		}
	}
}

func TestLoadFlashbackFile_noFileIsNothingChosen(t *testing.T) {
	f, err := LoadFlashbackFile(t.TempDir() + "/none.yaml")
	if err != nil || f.portChosen() || f.routingChosen() || f.Enabled || f.Routing {
		t.Fatalf("no file: %+v %v", f, err)
	}
}

// Each save writes down what it chose, and only that. The file's own text,
// since that is what a later start, the router and an older version read.
func TestFlashbackSaves_writeDownWhatWasChosen(t *testing.T) {
	const (
		portMark    = "port_not_chosen: true"
		routingMark = "routing_not_chosen: true"
	)
	for _, tc := range []struct {
		name                  string
		seeds                 []string
		wantPort, wantRouting bool // the marks left in the file
	}{
		{"the monitor login", []string{`PUT /api/flashback/monitor-login {"user":"monitor","password":"mon-pw"}`}, true, true},
		{"the monitor login removed", []string{`PUT /api/flashback/monitor-login {"user":"monitor","password":"mon-pw"}`, `DELETE /api/flashback/monitor-login `}, true, true},
		{"a password", []string{`POST /api/flashback/password `}, true, true},
		{"Engine Routing off", []string{`PUT /api/flashback {"routing":false}`}, true, false},
		{"Engine Routing on", []string{`PUT /api/flashback {"routing":true}`}, true, false},
		{"the port off", []string{`PUT /api/flashback {"enabled":false}`}, false, false},
		{"the port on", []string{`PUT /api/flashback {"enabled":true}`}, false, false},
		{"the port on with Engine Routing off", []string{`PUT /api/flashback {"enabled":true,"routing":false}`}, false, false},
		{"the monitor login, then the port off", []string{`PUT /api/flashback/monitor-login {"user":"monitor","password":"mon-pw"}`, `PUT /api/flashback {"enabled":false}`}, false, false},
		{"the port off, then everything else", []string{`PUT /api/flashback {"enabled":false}`, `PUT /api/flashback/monitor-login {"user":"monitor","password":"mon-pw"}`, `POST /api/flashback/password `, `PUT /api/flashback {"routing":true}`}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, path := newManagedFlashback(t, Config{})
			for _, sd := range tc.seeds {
				parts := strings.SplitN(sd, " ", 3)
				if code, _, raw := doFlashback(t, s, parts[0], parts[1], parts[2]); code != 200 {
					t.Fatalf("%s = %d %s", sd, code, raw)
				}
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(string(raw), portMark); got != tc.wantPort {
				t.Errorf("%q in the file: %v, want %v\n%s", portMark, got, tc.wantPort, raw)
			}
			if got := strings.Contains(string(raw), routingMark); got != tc.wantRouting {
				t.Errorf("%q in the file: %v, want %v\n%s", routingMark, got, tc.wantRouting, raw)
			}
		})
	}
}

// The first server leaves no mark behind, and neither does the startup
// setting that saves the port as on.
func TestFlashbackTurnOns_leaveNoMark(t *testing.T) {
	s, _, path := newManagedFlashback(t, Config{})
	doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-pw"}`)
	if got := s.portOnForNewServer(newServerRequest(), true); !got.TurnedOn {
		t.Fatalf("first server: %+v", got)
	}
	if raw, _ := os.ReadFile(path); strings.Contains(string(raw), "not_chosen") {
		t.Fatalf("a mark is left after the first server turned the port on:\n%s", raw)
	}
	s2, _, path2 := newManagedFlashback(t, Config{})
	if changed, on, err := s2.EnsureFlashbackOn(); !changed || !on || err != nil {
		t.Fatalf("EnsureFlashbackOn = %v %v %v", changed, on, err)
	}
	if raw, _ := os.ReadFile(path2); strings.Contains(string(raw), "not_chosen") {
		t.Fatalf("a mark is left after the port was saved as on:\n%s", raw)
	}
}

// EnsureFlashbackOn is once only, by the file: a file that exists is left as
// it is, one made by a save that chose nothing included (see its comment for
// why that is not the first server's rule).
func TestEnsureFlashbackOn_leavesAFileThatExists(t *testing.T) {
	for _, seed := range []string{
		`PUT /api/flashback/monitor-login {"user":"monitor","password":"mon-pw"}`,
		`PUT /api/flashback {"enabled":false}`,
	} {
		s, ctl, path := newManagedFlashback(t, Config{})
		parts := strings.SplitN(seed, " ", 3)
		doFlashback(t, s, parts[0], parts[1], parts[2])
		before, _ := os.ReadFile(path)
		opened := len(ctl.calls)
		if changed, on, err := s.EnsureFlashbackOn(); changed || on || err != nil {
			t.Fatalf("after %s: EnsureFlashbackOn = %v %v %v", seed, changed, on, err)
		}
		if after, _ := os.ReadFile(path); string(after) != string(before) || len(ctl.calls) != opened {
			t.Fatalf("after %s: the file changed or the port was opened", seed)
		}
	}
}

// After a save that chose nothing, the refusals are the ones of an
// installation with nothing saved: not the first server, a session that may
// not change settings, a session with a data access policy. Nothing is
// turned on and the file is not touched.
func TestPortOnForNewServer_afterASaveThatChoosesNeitherTheRefusalsStand(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first bool
		pol   *ext.AccessPolicy
		why   string
	}{
		{"not the first server", false, nil, "already had servers"},
		{"without settings:write", true, &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermServersWrite, ext.PermSettingsRead}}, "may not change settings"},
		{"with a data access policy", true, &ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}, "data access policy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctl, path := newManagedFlashback(t, Config{})
			if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-pw"}`); code != 200 {
				t.Fatalf("seed = %d %s", code, raw)
			}
			before, _ := os.ReadFile(path)
			req := newServerRequest()
			if tc.pol != nil {
				req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, tc.pol))
			}
			got := s.portOnForNewServer(req, tc.first)
			if got.TurnedOn || got.Enabled || got.Password != "" || !strings.Contains(got.NotTurnedOn, tc.why) {
				t.Fatalf("%+v, want it left off because %q", got, tc.why)
			}
			if after, _ := os.ReadFile(path); string(after) != string(before) || len(ctl.calls) != 0 {
				t.Fatalf("the file changed or the port was opened: %v\n%s", ctl.calls, after)
			}
			// The first server of a session that may, afterwards, turns it on
			// and keeps the monitor login.
			next := s.portOnForNewServer(newServerRequest(), true)
			f, _ := LoadFlashbackFile(path)
			if !next.TurnedOn || next.Password == "" || f.MonitorUser != "monitor" || f.MonitorPassword != "mon-pw" {
				t.Fatalf("an unrestricted first server after it: %+v, file %+v", next, f)
			}
		})
	}
}

// flashbackFileBeforeMarks is the settings file as the versions before the
// marks read and write it: the same fields without the two marks, and the
// same Extra that carries what they do not know.
type flashbackFileBeforeMarks struct {
	Version           int            `yaml:"version"`
	Enabled           bool           `yaml:"enabled"`
	Listen            string         `yaml:"listen,omitempty"`
	Password          string         `yaml:"password,omitempty"`
	PasswordCreatedAt string         `yaml:"password_created_at,omitempty"`
	Routing           bool           `yaml:"routing,omitempty"`
	MonitorUser       string         `yaml:"monitor_user,omitempty"`
	MonitorPassword   string         `yaml:"monitor_password,omitempty"`
	Extra             map[string]any `yaml:",inline"`
}

// An older version reads a file with the marks, changes it and writes it
// back. It carries the marks along without reading them. When it turned the
// port on and off, the address it saved says so, and the port stays off for
// this version; when it only saved something else, the marks still hold.
func TestFlashbackFile_anOlderVersionRewritesAFileWithTheMarks(t *testing.T) {
	rewrite := func(t *testing.T, path string, change func(*flashbackFileBeforeMarks)) {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var old flashbackFileBeforeMarks
		if err := yaml.Unmarshal(raw, &old); err != nil {
			t.Fatalf("the older version cannot read the file: %v\n%s", err, raw)
		}
		if old.Version != flashbackFileVersion {
			t.Fatalf("version %d: the older version would refuse to change the file", old.Version)
		}
		change(&old)
		out, err := yaml.Marshal(&old)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, out, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	seed := func(t *testing.T) (*Server, string) {
		s, _, path := newManagedFlashback(t, Config{})
		if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-pw"}`); code != 200 {
			t.Fatalf("seed = %d %s", code, raw)
		}
		return s, path
	}

	t.Run("it turned the port on and then off: it stays off", func(t *testing.T) {
		s, path := seed(t)
		rewrite(t, path, func(f *flashbackFileBeforeMarks) {
			f.Enabled, f.Listen, f.Password = true, "127.0.0.1:3309", "bfp_old"
		})
		rewrite(t, path, func(f *flashbackFileBeforeMarks) { f.Enabled = false })
		raw, _ := os.ReadFile(path)
		if !strings.Contains(string(raw), "port_not_chosen: true") {
			t.Fatalf("the older version dropped the mark, so this case no longer tests anything:\n%s", raw)
		}
		got := s.portOnForNewServer(newServerRequest(), true)
		if got.TurnedOn || got.Enabled {
			t.Fatalf("a port the older version turned off came back on: %+v", got)
		}
		if after, _ := os.ReadFile(path); string(after) != string(raw) {
			t.Fatalf("the file changed:\n%s", after)
		}
	})
	t.Run("it turned the port on: nothing to turn on", func(t *testing.T) {
		s, path := seed(t)
		rewrite(t, path, func(f *flashbackFileBeforeMarks) {
			f.Enabled, f.Listen, f.Password = true, "127.0.0.1:3309", "bfp_old"
		})
		raw, _ := os.ReadFile(path)
		// The status is this process's own picture of the port, which a file
		// rewritten under it does not change: what is asked here is that
		// nothing is turned on, made or saved.
		got := s.portOnForNewServer(newServerRequest(), true)
		if got.TurnedOn || got.Password != "" {
			t.Fatalf("%+v", got)
		}
		if after, _ := os.ReadFile(path); string(after) != string(raw) {
			t.Fatalf("the file changed:\n%s", after)
		}
	})
	t.Run("it saved another monitor login: the first server still turns the port on", func(t *testing.T) {
		s, path := seed(t)
		rewrite(t, path, func(f *flashbackFileBeforeMarks) { f.MonitorUser, f.MonitorPassword = "probe", "probe-pw" })
		got := s.portOnForNewServer(newServerRequest(), true)
		f, _ := LoadFlashbackFile(path)
		if !got.TurnedOn || !got.RoutingOn || f.MonitorUser != "probe" || f.MonitorPassword != "probe-pw" {
			t.Fatalf("%+v, file %+v", got, f)
		}
	})
}
