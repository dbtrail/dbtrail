package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// #2686: adding a server turns the MySQL port on, with Engine Routing on,
// when nothing was ever saved about the port, and that answer alone carries
// the password.

func newServerRequest() *http.Request {
	return httptest.NewRequest("POST", "http://127.0.0.1:8090/api/servers/check", nil)
}

func TestPortOnForNewServer_nothingSavedTurnsItOnWithRoutingAndAPassword(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	got := s.portOnForNewServer(newServerRequest())
	if !got.TurnedOn || !got.Enabled || !got.RoutingOn || got.NotTurnedOn != "" {
		t.Fatalf("first server: %+v", got)
	}
	if !strings.HasPrefix(got.Password, flashbackPasswordPrefix) {
		t.Fatalf("the answer carries no password: %q", got.Password)
	}
	f, err := LoadFlashbackFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Enabled || !f.Routing || f.Password != got.Password || f.Listen == "" {
		t.Fatalf("the file: enabled %v routing %v listen %q, and its password is the one answered: %v",
			f.Enabled, f.Routing, f.Listen, f.Password == got.Password)
	}
	if len(ctl.calls) != 1 || ctl.calls[0] != f.Listen {
		t.Fatalf("the port was asked to open %v, want once at %q", ctl.calls, f.Listen)
	}

	// A second server: the port is on, nothing is turned on, and the
	// password is not answered again.
	again := s.portOnForNewServer(newServerRequest())
	if again.TurnedOn || again.Password != "" || !again.Enabled || !again.RoutingOn || !again.HasPassword {
		t.Fatalf("second server: %+v", again)
	}
	if f2, _ := LoadFlashbackFile(path); f2.Password != got.Password {
		t.Fatal("the second server replaced the port's password")
	}
}

// A setting that exists is the operator's: a port turned off stays off, and
// Engine Routing saved off stays off, however many servers are added.
func TestPortOnForNewServer_aSavedSettingIsLeftAsItIs(t *testing.T) {
	for _, tc := range []struct {
		name, body  string
		on, routing bool
	}{
		{"turned off", `{"enabled":false}`, false, false},
		{"routing saved off, port never on", `{"routing":false}`, false, false},
		{"routing saved on, port never on", `{"routing":true}`, false, true},
		{"on with routing off", `{"enabled":true,"routing":false}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, path := newManagedFlashback(t, Config{})
			if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", tc.body); code != 200 {
				t.Fatalf("seed %s = %d %s", tc.body, code, raw)
			}
			before, _ := os.ReadFile(path)
			got := s.portOnForNewServer(newServerRequest())
			if got.TurnedOn || got.Password != "" || got.Enabled != tc.on || got.RoutingOn != tc.routing || got.NotTurnedOn != "" {
				t.Fatalf("after adding a server: %+v", got)
			}
			if after, _ := os.ReadFile(path); string(after) != string(before) {
				t.Fatalf("the saved setting changed:\n%s\nwas\n%s", after, before)
			}
		})
	}
}

// A session with a data access policy turns nothing on, and is told why.
func TestPortOnForNewServer_aSessionWithADataPolicyTurnsNothingOn(t *testing.T) {
	s, ctl, path := newManagedFlashback(t, Config{})
	req := newServerRequest()
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{},
		&ext.AccessPolicy{Profile: "analyst", Permissions: ext.AllPermissions()}))
	got := s.portOnForNewServer(req)
	if got.TurnedOn || got.Enabled || got.Password != "" || !strings.Contains(got.NotTurnedOn, "data access policy") {
		t.Fatalf("restricted session: %+v", got)
	}
	if _, err := os.Stat(path); err == nil || len(ctl.calls) != 0 {
		t.Fatalf("something was saved or opened: stat err %v, calls %v", err, ctl.calls)
	}
	// The next session without one still gets the port and its password.
	if next := s.portOnForNewServer(newServerRequest()); !next.TurnedOn || next.Password == "" {
		t.Fatalf("an unrestricted session after it: %+v", next)
	}
}

// A console that does not run the port says so instead of failing the add.
func TestPortOnForNewServer_whereThePortIsNotManagedHere(t *testing.T) {
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: fbTok})
	if err != nil {
		t.Fatal(err)
	}
	got := s.portOnForNewServer(newServerRequest())
	if got.TurnedOn || got.Enabled || got.Password != "" || got.NotTurnedOn == "" {
		t.Fatalf("no port to manage: %+v", got)
	}
}

// A setting that cannot be saved leaves the port off and says why.
func TestPortOnForNewServer_aSettingThatCannotBeSaved(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes into a read-only folder")
	}
	s, ctl, path := newManagedFlashback(t, Config{})
	dir := path[:strings.LastIndex(path, "/")]
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	got := s.portOnForNewServer(newServerRequest())
	if got.TurnedOn || got.Enabled || got.Password != "" || !strings.Contains(got.NotTurnedOn, "could not be saved") {
		t.Fatalf("unwritable folder: %+v", got)
	}
	if len(ctl.calls) != 0 {
		t.Fatalf("the port was opened with nothing saved: %v", ctl.calls)
	}
}

// Servers added at the same moment: one password, answered once.
func TestPortOnForNewServer_addedAtOnceMakeOnePassword(t *testing.T) {
	s, _, path := newManagedFlashback(t, Config{})
	const n = 8
	got := make([]*newServerPortDTO, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() { defer wg.Done(); got[i] = s.portOnForNewServer(newServerRequest()) }()
	}
	wg.Wait()
	f, err := LoadFlashbackFile(path)
	if err != nil {
		t.Fatal(err)
	}
	shown := 0
	for _, g := range got {
		if g.Password != "" {
			shown++
			if g.Password != f.Password || !g.TurnedOn {
				t.Fatalf("a password answered that is not the saved one, or without turned_on: %+v", g)
			}
		}
		if !g.Enabled {
			t.Fatalf("an add saw the port off: %+v", g)
		}
	}
	if shown != 1 {
		t.Fatalf("the password was answered %d times, want once", shown)
	}
}

// Turning the port on by hand for the first time also starts with Engine
// Routing on, unless the request says otherwise; a setting that exists keeps
// its routing.
func TestFlashbackPut_theFirstTurnOnStartsWithRoutingOn_2686(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed string
		body string
		want bool
	}{
		{"nothing saved", "", `{"enabled":true}`, true},
		{"nothing saved, routing said off", "", `{"enabled":true,"routing":false}`, false},
		{"routing saved off before", `{"routing":false}`, `{"enabled":true}`, false},
		{"turned off before", `{"enabled":false}`, `{"enabled":true}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, path := newManagedFlashback(t, Config{})
			if tc.seed != "" {
				if code, _, raw := doFlashback(t, s, "PUT", "/api/flashback", tc.seed); code != 200 {
					t.Fatalf("seed = %d %s", code, raw)
				}
			}
			code, got, raw := doFlashback(t, s, "PUT", "/api/flashback", tc.body)
			if code != 200 || got["enabled"] != true || got["routing_on"] != tc.want {
				t.Fatalf("%s = %d %s, want routing_on %v", tc.body, code, raw, tc.want)
			}
			if f, _ := LoadFlashbackFile(path); f.Routing != tc.want {
				t.Fatalf("file routing %v, want %v", f.Routing, tc.want)
			}
		})
	}
}

// Turning the port off as the very first thing saved does not turn routing on.
func TestFlashbackPut_turningOffFirstSavesNoRouting_2686(t *testing.T) {
	s, _, path := newManagedFlashback(t, Config{})
	if code, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":false}`); code != 200 || got["routing_on"] != false {
		t.Fatalf("= %d %s", code, raw)
	}
	if f, _ := LoadFlashbackFile(path); f.Routing || f.Enabled {
		t.Fatalf("file: %+v", f)
	}
}
