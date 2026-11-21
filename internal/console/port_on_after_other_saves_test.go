package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Adding the first server turns the MySQL port on (#2686) unless somebody
// chose that it is on or off. The port's settings file is also written by
// saves that choose neither: the monitor login, a password made ahead, Engine
// Routing alone. These drive the real routes: one of those saves first, then
// POST /api/servers/check for the first server.

// portAnswerForLog is the port part of an added server's answer as the
// client receives it, with the password's random part cut.
func portAnswerForLog(t *testing.T, p *newServerPortDTO) string {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if p.Password != "" {
		s = strings.Replace(s, p.Password, flashbackPasswordPrefix+"...", 1)
	}
	return s
}

func TestCheckRoute_theFirstServerAfterASaveThatChoosesNeitherTurnsThePortOn(t *testing.T) {
	type seed struct{ method, path, body string }
	monitor := seed{"PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-pw"}`}
	password := seed{"POST", "/api/flashback/password", ""}
	for _, tc := range []struct {
		name  string
		seeds []seed
		// routing is Engine Routing after the add; madeAhead: the port's
		// password was made before the server, so the add answers none.
		routing, madeAhead bool
	}{
		{"the monitor login saved first", []seed{monitor}, true, false},
		{"the monitor login saved and removed", []seed{monitor, {"DELETE", "/api/flashback/monitor-login", ""}}, true, false},
		{"a password made ahead", []seed{password}, true, true},
		{"Engine Routing turned off ahead", []seed{{"PUT", "/api/flashback", `{"routing":false}`}}, false, false},
		{"Engine Routing turned on ahead", []seed{{"PUT", "/api/flashback", `{"routing":true}`}}, true, false},
		{"the monitor login, a password and Engine Routing off", []seed{monitor, password, {"PUT", "/api/flashback", `{"routing":false}`}}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, fc, path := newSupervisorWithPort(t)
			for _, sd := range tc.seeds {
				if rec, body := doServersReq(t, srv, sd.method, sd.path, sd.body); rec.Code != 200 {
					t.Fatalf("%s %s %s = %d %s", sd.method, sd.path, sd.body, rec.Code, body)
				}
			}
			before, err := LoadFlashbackFile(path)
			if err != nil || before.Enabled {
				t.Fatalf("before the add: %+v %v", before, err)
			}
			rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
			got := decodeCheck(t, body)
			if rec.Code != 200 || !got.Started || got.Port == nil {
				t.Fatalf("add: %d %s", rec.Code, body)
			}
			t.Logf("port answer: %s", portAnswerForLog(t, got.Port))
			p := got.Port
			if !p.TurnedOn || !p.Enabled || p.RoutingOn != tc.routing || p.NotTurnedOn != "" || !p.HasPassword {
				t.Fatalf("the first server did not turn the port on, or not with Engine Routing %v: %s", tc.routing, body)
			}
			after, err := LoadFlashbackFile(path)
			if err != nil || !after.Enabled || after.Routing != tc.routing || after.Listen == "" || after.Password == "" {
				t.Fatalf("the file after the add: %+v %v", after, err)
			}
			// What was saved before is kept.
			if after.MonitorUser != before.MonitorUser || after.MonitorPassword != before.MonitorPassword {
				t.Fatalf("the monitor login changed: %q/%q, was %q/%q", after.MonitorUser, after.MonitorPassword, before.MonitorUser, before.MonitorPassword)
			}
			if tc.madeAhead {
				if after.Password != before.Password || after.PasswordCreatedAt != before.PasswordCreatedAt {
					t.Fatal("the password made ahead was replaced")
				}
				if p.Password != "" || strings.Contains(string(body), before.Password) {
					t.Fatalf("the add answers a password that was made before it: %s", body)
				}
			} else if p.Password == "" || p.Password != after.Password {
				t.Fatalf("no password before the add, and the add's answer does not carry the one it made: %s", body)
			}
			if len(fc.calls) != 1 || fc.calls[0] != after.Listen {
				t.Fatalf("the port was asked to open %v, want once at %q", fc.calls, after.Listen)
			}
			// The second server finds a port that is on, and changes nothing.
			second := `{"source_host":"db2.example.com","source_user":"dbtrail","source_password":"Ab3-xyz"}`
			_, body = doServersReq(t, srv, "POST", "/api/servers/check", second)
			if g := decodeCheck(t, body); g.Port == nil || g.Port.TurnedOn || g.Port.Password != "" || !g.Port.Enabled {
				t.Fatalf("second add: %s", body)
			}
			if again, _ := LoadFlashbackFile(path); again.Password != after.Password {
				t.Fatal("the second add replaced the password")
			}
		})
	}
}

// A port somebody turned off stays off when the first server is added,
// whatever else was saved before or after it was turned off.
func TestCheckRoute_aPortTurnedOffStaysOffWhateverElseWasSaved(t *testing.T) {
	const (
		off      = `PUT /api/flashback {"enabled":false}`
		on       = `PUT /api/flashback {"enabled":true}`
		monitor  = `PUT /api/flashback/monitor-login {"user":"monitor","password":"mon-pw"}`
		unmon    = `DELETE /api/flashback/monitor-login `
		password = `POST /api/flashback/password `
		routing  = `PUT /api/flashback {"routing":true}`
		norout   = `PUT /api/flashback {"routing":false}`
	)
	for _, seeds := range [][]string{
		{off},
		{off, monitor},
		{monitor, off},
		{off, password},
		{password, off},
		{off, routing},
		{routing, off},
		{off, norout, monitor, unmon, password},
		{monitor, password, norout, off, monitor, routing},
		{on, off},
		{on, off, monitor, password, norout},
	} {
		t.Run(strings.Join(seeds, " then "), func(t *testing.T) {
			srv, _, fc, path := newSupervisorWithPort(t)
			for _, sd := range seeds {
				parts := strings.SplitN(sd, " ", 3)
				if rec, body := doServersReq(t, srv, parts[0], parts[1], parts[2]); rec.Code != 200 {
					t.Fatalf("%s = %d %s", sd, rec.Code, body)
				}
			}
			before, _ := os.ReadFile(path)
			opened := len(fc.calls)
			rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
			got := decodeCheck(t, body)
			if rec.Code != 200 || !got.Started || got.Port == nil {
				t.Fatalf("add: %d %s", rec.Code, body)
			}
			t.Logf("port answer: %s", portAnswerForLog(t, got.Port))
			if got.Port.TurnedOn || got.Port.Enabled || got.Port.Password != "" || got.Port.NotTurnedOn != "" {
				t.Fatalf("a port that was turned off came back on: %s", body)
			}
			if after, _ := os.ReadFile(path); string(after) != string(before) {
				t.Fatalf("the saved setting changed:\n%s\nwas\n%s", after, before)
			}
			for _, addr := range fc.calls[opened:] {
				if addr != "" {
					t.Fatalf("the port was opened at %q", addr)
				}
			}
		})
	}
}

// A file from a version that did not write down whether on or off was chosen
// cannot say, so it is read as chosen: the port stays as the file has it.
func TestCheckRoute_aFileFromBeforeTheChoiceWasWrittenDownIsLeftAsItIs(t *testing.T) {
	for name, yaml := range map[string]string{
		"only the monitor login": "version: 1\nenabled: false\nmonitor_user: monitor\nmonitor_password: mon-pw\n",
		"turned off":             "version: 1\nenabled: false\n",
		"a password, off":        "version: 1\nenabled: false\npassword: bfp_made_before\npassword_created_at: \"2026-11-01T00:00:00Z\"\n",
		"on then off":            "version: 1\nenabled: false\nlisten: 127.0.0.1:3309\npassword: bfp_made_before\nrouting: true\n",
		// The mark, carried along unread by a version from before it while
		// that version turned the port on (which saves its address) and off.
		"the mark under a port that was on once": "version: 1\nenabled: false\nlisten: 127.0.0.1:3309\npassword: bfp_made_before\nport_not_chosen: true\nrouting_not_chosen: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), FlashbackFileName)
			if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}, FlashbackPath: path})
			if err != nil {
				t.Fatal(err)
			}
			fc := &fakeFlashbackControl{fail: map[string]error{}}
			srv.ManageFlashback(fc)
			rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
			got := decodeCheck(t, body)
			if rec.Code != 200 || !got.Started || got.Port == nil {
				t.Fatalf("add: %d %s", rec.Code, body)
			}
			if got.Port.TurnedOn || got.Port.Enabled || got.Port.Password != "" {
				t.Fatalf("the port was turned on over a file that may hold a choice: %s", body)
			}
			if after, _ := os.ReadFile(path); string(after) != yaml {
				t.Fatalf("the file changed:\n%s\nwas\n%s", after, yaml)
			}
			if len(fc.calls) != 0 {
				t.Fatalf("the port was opened: %v", fc.calls)
			}
		})
	}
}

// Turning the port on by hand after a save that chose nothing starts with
// Engine Routing on, as on an installation where nothing was saved.
func TestFlashbackPut_theFirstTurnOnAfterASaveThatChoosesNeitherStartsWithRoutingOn(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body string
		want                     bool
	}{
		{"the monitor login", "PUT", "/api/flashback/monitor-login", `{"user":"monitor","password":"mon-pw"}`, true},
		{"a password made ahead", "POST", "/api/flashback/password", "", true},
		{"Engine Routing turned off", "PUT", "/api/flashback", `{"routing":false}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, path := newManagedFlashback(t, Config{})
			if code, _, raw := doFlashback(t, s, tc.method, tc.path, tc.body); code != 200 {
				t.Fatalf("seed = %d %s", code, raw)
			}
			code, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`)
			if code != 200 || got["enabled"] != true || got["routing_on"] != tc.want {
				t.Fatalf("port on = %d %s, want routing_on %v", code, raw, tc.want)
			}
			if f, _ := LoadFlashbackFile(path); f.Routing != tc.want {
				t.Fatalf("file routing %v, want %v", f.Routing, tc.want)
			}
			// Off and on again: Engine Routing is what the file says by now.
			doFlashback(t, s, "PUT", "/api/flashback", `{"routing":false}`)
			doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":false}`)
			if _, got, raw := doFlashback(t, s, "PUT", "/api/flashback", `{"enabled":true}`); got["routing_on"] != false {
				t.Fatalf("off and on again turned Engine Routing on: %s", raw)
			}
		})
	}
}
