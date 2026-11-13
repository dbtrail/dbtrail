package consoleapp

import (
	"bytes"
	"context"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2783: `port max-behind <server> [<seconds>|off]` says or sets the copy's
// lag limit of a server (#2777) from the terminal.

func TestParseMaxBehind(t *testing.T) {
	for in, want := range map[string]int64{
		"300": 300, " 300 ": 300, "+300": 300, "0": 0, "off": 0, "OFF": 0, " Off ": 0,
		"90s": 90, "5m": 300, "1h": 3600, "1h30m": 5400, "0s": 0, "0.5m": 30,
		"31536000": 31536000, "8760h": 31536000,
	} {
		got, err := parseMaxBehind(in)
		if err != nil || got != want {
			t.Errorf("parseMaxBehind(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for in, want := range map[string]string{
		"":                     "empty",
		"   ":                  "empty",
		"-5":                   "negative",
		"-5m":                  "negative",
		"-0":                   "negative",
		"1.5":                  "not written as whole seconds",
		"300.0":                "not written as whole seconds",
		"1e3":                  "not written as whole seconds",
		"1500ms":               "fraction of a second",
		"90.5s":                "fraction of a second",
		"5 m":                  "not a limit this command reads",
		"5min":                 "not a limit this command reads",
		"1d":                   "not a limit this command reads",
		"5M":                   "not a limit this command reads",
		"abc":                  "not a limit this command reads",
		"on":                   "not a limit this command reads",
		"31536001":             "more than a year",
		"8761h":                "more than a year",
		"99999999999999999999": "more than a year",
		"99999999h":            "more than a year",
	} {
		_, err := parseMaxBehind(in)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("parseMaxBehind(%q) err = %v, want it to say %q", in, err, want)
			continue
		}
		// Every refusal but the cap names the forms that are taken.
		if want != "more than a year" && !strings.Contains(err.Error(), "5m") {
			t.Errorf("parseMaxBehind(%q) err = %v: it does not name the accepted forms", in, err)
		}
	}
}

func TestMaxBehindText(t *testing.T) {
	for secs, want := range map[int64]string{
		0: "off", -1: "off", 45: "45 s", 60: "60 s (1m)", 90: "90 s (1m30s)", 300: "300 s (5m)",
		3600: "3600 s (1h)", 5400: "5400 s (1h30m)", 3601: "3601 s (1h0m1s)", 31536000: "31536000 s (8760h)",
	} {
		if got := maxBehindText(secs); got != want {
			t.Errorf("maxBehindText(%d) = %q, want %q", secs, got, want)
		}
	}
}

func limitServers(d *fakeDaemon, limit any) {
	shop := map[string]any{"id": "id1", "name": "shop", "kind": "registry", "has_source": true, "sql_fresh": false, "sql_fresh_saved": true}
	if limit != nil {
		shop["sql_max_behind_source_seconds"] = limit
	}
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
		{"id": "boot", "name": "boot", "kind": "ephemeral", "has_source": true, "sql_max_behind_source_seconds": 0},
		shop,
		{"id": "id3", "name": "old-index", "kind": "registry", "has_source": false, "sql_max_behind_source_seconds": 0},
	}})
}

func TestPortMaxBehindSays(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   any
		want    []string
		wantNot []string
	}{
		{"none", 0, []string{`No lag limit for "shop": the copy answers whatever its lag.`, "port max-behind shop 5m"}, []string{"Under 90 s"}},
		{"five minutes", 300, []string{`Lag limit for "shop": 300 s (5m).`, "goes to MySQL", "error 1105"}, []string{"Under 90 s"}},
		{"under the margin", 45, []string{`Lag limit for "shop": 45 s.`, "Under 90 s it is not kept to the second", "every 30 s", "a minute after its last stamp"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			limitServers(d, tc.limit)
			var out bytes.Buffer
			if err := runPortMaxBehind(context.Background(), c, "shop", nil, &out); err != nil {
				t.Fatal(err)
			}
			for _, w := range tc.want {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output lacks %q:\n%s", w, out.String())
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(out.String(), w) {
					t.Errorf("output says %q:\n%s", w, out.String())
				}
			}
			if slices.Contains(d.calls, "PUT /api/servers/id1/sql-mode") {
				t.Error("a look saved something")
			}
		})
	}
	t.Run("a daemon older than the limit is not read as off", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		limitServers(d, nil)
		var out bytes.Buffer
		err := runPortMaxBehind(context.Background(), c, "shop", nil, &out)
		if err == nil || !strings.Contains(err.Error(), "older than the limit") {
			t.Fatalf("err = %v, output:\n%s", err, out.String())
		}
		if strings.Contains(out.String(), "No lag limit") {
			t.Errorf("an older daemon read as no limit:\n%s", out.String())
		}
	})
}

func TestPortMaxBehindSets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		want    int64
		answer  map[string]any
		out     []string
		wantNot []string
	}{
		{"five minutes", 300, map[string]any{"fresh": false, "fresh_saved": true, "like_ignores_case": true, "max_behind_source_seconds": 300},
			[]string{`Saved for "shop": a lag limit of 300 s (5m).`, "goes to MySQL", "error 1105", "Open connections follow it"}, []string{"Under 90 s"}},
		{"ninety seconds is kept", 90, map[string]any{"fresh": false, "max_behind_source_seconds": 90},
			[]string{`a lag limit of 90 s (1m30s).`}, []string{"Under 90 s"}},
		{"under the margin is saved and says so", 30, map[string]any{"fresh": false, "max_behind_source_seconds": 30},
			[]string{`a lag limit of 30 s.`, "Under 90 s it is not kept to the second"}, nil},
		{"off", 0, map[string]any{"fresh": false, "max_behind_source_seconds": 0},
			[]string{`Saved for "shop": no lag limit. The copy answers whatever its lag.`}, []string{"Under 90 s", "1105"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			limitServers(d, 0)
			d.on("PUT /api/servers/id1/sql-mode", 200, tc.answer)
			var out bytes.Buffer
			want := tc.want
			if err := runPortMaxBehind(context.Background(), c, "shop", &want, &out); err != nil {
				t.Fatal(err)
			}
			b := d.bodies["PUT /api/servers/id1/sql-mode"]
			if len(b) != 1 || b["max_behind_source_seconds"] != float64(tc.want) {
				t.Fatalf("body = %v: only the limit is sent, so fresh/fast and the LIKE switch are not touched", b)
			}
			for _, w := range tc.out {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output lacks %q:\n%s", w, out.String())
				}
			}
			for _, w := range tc.wantNot {
				if strings.Contains(out.String(), w) {
					t.Errorf("output says %q:\n%s", w, out.String())
				}
			}
		})
	}
	for _, tc := range []struct {
		name   string
		status int
		answer map[string]any
		want   string
	}{
		{"the daemon kept another limit", 200, map[string]any{"max_behind_source_seconds": 600}, "is 600 s (10m), not 300 s (5m)"},
		{"an answer without the limit", 200, map[string]any{"fresh": false}, "older than the limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			limitServers(d, 0)
			d.on("PUT /api/servers/id1/sql-mode", tc.status, tc.answer)
			var out bytes.Buffer
			want := int64(300)
			err := runPortMaxBehind(context.Background(), c, "shop", &want, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(out.String(), "Saved") {
				t.Errorf("a failure said it saved:\n%s", out.String())
			}
		})
	}
	// A daemon older than the limit refuses the field as unknown JSON; the
	// list already said it does not know the limit, so nothing is sent.
	t.Run("a daemon older than the limit", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		limitServers(d, nil)
		d.on("PUT /api/servers/id1/sql-mode", 400, map[string]any{"error": `invalid JSON body: json: unknown field "max_behind_source_seconds"`})
		want := int64(300)
		err := runPortMaxBehind(context.Background(), c, "shop", &want, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "older than the limit") {
			t.Fatalf("err = %v", err)
		}
		if slices.Contains(d.calls, "PUT /api/servers/id1/sql-mode") {
			t.Error("a request the daemon cannot read was sent")
		}
	})
	for _, tc := range []struct {
		name   string
		status int
		answer map[string]any
		want   string
	}{
		{"the daemon refused", 400, map[string]any{"error": "max_behind_source_seconds must be a whole number of seconds"}, "must be a whole number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			limitServers(d, 0)
			d.on("PUT /api/servers/id1/sql-mode", tc.status, tc.answer)
			var out bytes.Buffer
			want := int64(300)
			err := runPortMaxBehind(context.Background(), c, "shop", &want, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(out.String(), "Saved") {
				t.Errorf("a failure said it saved:\n%s", out.String())
			}
		})
	}
}

func TestPortMaxBehindRefuses(t *testing.T) {
	for name, want := range map[string]string{
		"boot":      "given where DBTrail starts",
		"old-index": "has no source database",
		"nope":      "nope",
	} {
		for _, set := range []bool{false, true} {
			d, c := newFakeDaemon(t)
			limitServers(d, 0)
			var v *int64
			if set {
				v = new(int64)
				*v = 60
			}
			err := runPortMaxBehind(context.Background(), c, name, v, &bytes.Buffer{})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("%s (set %v): err = %v, want %q", name, set, err, want)
			}
			if slices.Contains(d.calls, "PUT /api/servers/id1/sql-mode") || slices.Contains(d.calls, "PUT /api/servers/boot/sql-mode") {
				t.Errorf("%s: saved something", name)
			}
		}
	}
}

// The argument is checked before anything is asked of the daemon.
func TestPortMaxBehindArgs(t *testing.T) {
	cmd := newPortMaxBehindCmd()
	for _, ok := range [][]string{{"shop"}, {"shop", "300"}, {"shop", "5m"}, {"shop", "off"}} {
		if err := cmd.Args(cmd, ok); err != nil {
			t.Errorf("%v refused: %v", ok, err)
		}
	}
	for _, bad := range [][]string{{}, {"shop", "5m", "x"}, {"shop", "1.5"}, {"shop", "soon"}} {
		if err := cmd.Args(cmd, bad); err == nil {
			t.Errorf("%v taken", bad)
		}
	}
	found := false
	for _, s := range portCmd.Commands() {
		if s.Name() == "max-behind" {
			found = true
		}
	}
	if !found {
		t.Error("port has no max-behind command")
	}
}

func TestPortStatusSaysTheLagLimits(t *testing.T) {
	t.Run("per server", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/flashback", 200, map[string]any{"enabled": true, "routing_on": true})
		d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
			{"id": "boot", "name": "boot", "kind": "ephemeral", "has_source": true, "sql_max_behind_source_seconds": 0},
			{"id": "id1", "name": "shop", "kind": "registry", "has_source": true, "sql_max_behind_source_seconds": 300},
			{"id": "id2", "name": "billing", "kind": "registry", "has_source": true, "sql_max_behind_source_seconds": 0},
			{"id": "id3", "name": "old-index", "kind": "registry", "has_source": false, "sql_max_behind_source_seconds": 0},
		}})
		var out bytes.Buffer
		if err := runPortStatus(context.Background(), c, &out); err != nil {
			t.Fatal(err)
		}
		o := out.String()
		for _, w := range []string{"Lag limit per server", "  shop     300 s (5m)\n", "  billing  off\n", "  boot     off (set at startup"} {
			if !strings.Contains(o, w) {
				t.Errorf("status lacks %q:\n%s", w, o)
			}
		}
		if strings.Contains(o, "old-index") {
			t.Errorf("a server the port does not answer is listed:\n%s", o)
		}
		if n := strings.Count(strings.Join(d.calls, "\n"), "GET /api/servers"); n != 1 {
			t.Errorf("the list was read %d times, want once", n)
		}
	})
	t.Run("the port off still says them", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/flashback", 200, map[string]any{"enabled": false})
		limitServers(d, 120)
		var out bytes.Buffer
		if err := runPortStatus(context.Background(), c, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "is off") || !strings.Contains(out.String(), "shop  120 s (2m)") {
			t.Errorf("status:\n%s", out.String())
		}
	})
	t.Run("an older daemon is not read as off", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/flashback", 200, map[string]any{"enabled": true})
		limitServers(d, nil)
		var out bytes.Buffer
		if err := runPortStatus(context.Background(), c, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "shop  not said (this DBTrail is older than the lag limit)") {
			t.Errorf("status:\n%s", out.String())
		}
	})
	t.Run("a list that does not load is said", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/flashback", 200, map[string]any{"enabled": true})
		d.on("GET /api/servers", 500, map[string]any{"error": "boom"})
		var out bytes.Buffer
		if err := runPortStatus(context.Background(), c, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "The MySQL port is on") || !strings.Contains(out.String(), "lag limits could not be read") || !strings.Contains(out.String(), "boom") {
			t.Errorf("status:\n%s", out.String())
		}
	})
}

// Through the real console: setting the limit from the terminal leaves the
// server's fast mode and LIKE switch as they were saved, and the list, the
// look and `port status` read back what was saved.
func TestPortMaxBehindThroughTheConsole(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(console.ServerEntry{Name: "shop", DSN: "u:p@tcp(127.0.0.1:1)/idx", SourceDSN: "repl:pw@tcp(127.0.0.1:1)/"})
	if err != nil {
		t.Fatal(err)
	}
	fast, like := false, true
	if _, err := reg.SetSQLMode(e.ID, &fast, &like, nil); err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	c := &ctlClient{base: ts.URL, token: "t", hc: ts.Client()}

	kept := func(want int64) {
		t.Helper()
		got, ok := reg.Get(e.ID)
		if !ok {
			t.Fatal("the server is gone")
		}
		if got.SQLFreshOn() || !got.SQLFreshSaved() || !got.SQLLikeIgnoresCase || got.SQLMaxBehindSource != want {
			t.Fatalf("saved fresh=%v saved=%v like=%v limit=%d; want fast saved, LIKE on, limit %d",
				got.SQLFreshOn(), got.SQLFreshSaved(), got.SQLLikeIgnoresCase, got.SQLMaxBehindSource, want)
		}
	}
	var out bytes.Buffer
	if err := runPortMaxBehind(context.Background(), c, "shop", nil, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `No lag limit for "shop"`) {
		t.Fatalf("look before:\n%s", out.String())
	}
	v, err := parseMaxBehind("5m")
	if err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runPortMaxBehind(context.Background(), c, "shop", &v, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("port max-behind shop 5m:\n%s", out.String())
	kept(300)
	out.Reset()
	if err := runPortMaxBehind(context.Background(), c, "shop", nil, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("port max-behind shop:\n%s", out.String())
	if !strings.Contains(out.String(), `Lag limit for "shop": 300 s (5m).`) {
		t.Fatalf("look after:\n%s", out.String())
	}
	out.Reset()
	if err := runPortStatus(context.Background(), c, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("port status:\n%s", out.String())
	if !regexp.MustCompile(`\n  shop +300 s \(5m\)\n`).MatchString(out.String()) {
		t.Fatalf("status:\n%s", out.String())
	}
	short, _ := parseMaxBehind("45")
	out.Reset()
	if err := runPortMaxBehind(context.Background(), c, "shop", &short, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("port max-behind shop 45:\n%s", out.String())
	kept(45)
	off, _ := parseMaxBehind("off")
	out.Reset()
	if err := runPortMaxBehind(context.Background(), c, "shop", &off, &out); err != nil {
		t.Fatal(err)
	}
	t.Logf("port max-behind shop off:\n%s", out.String())
	kept(0)
	for _, bad := range []string{"1.5", "-5", "soon", "2y"} {
		_, err := parseMaxBehind(bad)
		if err == nil {
			t.Fatalf("%q passed", bad)
		}
		t.Logf("port max-behind shop %s: Error: %v", bad, err)
	}
}
