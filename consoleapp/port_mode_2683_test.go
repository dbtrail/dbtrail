package consoleapp

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// #2683: `port mode <server> [fresh|fast]` says what is saved for a server
// and what the router applies, and saves a choice.

func modeServers(d *fakeDaemon, fresh, saved bool) {
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
		{"id": "boot", "name": "boot", "kind": "ephemeral"},
		{"id": "id1", "name": "shop", "kind": "registry", "has_source": true, "source_user": "dbtrail", "sql_fresh": fresh, "sql_fresh_saved": saved},
		{"id": "id3", "name": "old-index", "kind": "registry", "has_source": false},
	}})
}

func routerSays(servers map[string]any, extra map[string]any) map[string]any {
	st := map[string]any{"fresh": false, "per_server_mode": true, "fresh_by_default": true, "servers": servers}
	for k, v := range extra {
		st[k] = v
	}
	return map[string]any{"configured": true, "status": st}
}

func fastModePoll(t *testing.T) {
	ow, op := portModeWait, portModePoll
	portModeWait, portModePoll = 200*time.Millisecond, time.Millisecond
	t.Cleanup(func() { portModeWait, portModePoll = ow, op })
}

func TestPortModeSays(t *testing.T) {
	fresh := map[string]any{"id1": map[string]any{"sql_mode": "fresh", "sql_mode_from": "default"}}
	fast := map[string]any{"id1": map[string]any{"sql_mode": "fast", "sql_mode_from": "saved"}}
	for _, tc := range []struct {
		name         string
		fresh, saved bool
		router       map[string]any
		want         []string
		wantNot      []string
	}{
		{"nothing saved, fresh applied", true, false, routerSays(fresh, nil),
			[]string{`Nothing is saved for "shop": it is fresh, the default.`, "The router answers it in fresh mode."}, []string{"still"}},
		{"fast saved and applied", false, true, routerSays(fast, nil),
			[]string{`Saved for "shop": fast.`, "The router answers it in fast mode."}, nil},
		{"a router that does not list it answers fast", false, true, routerSays(map[string]any{}, nil),
			[]string{"The router answers it in fast mode."}, nil},
		{"every server fresh by the setting", false, true, routerSays(map[string]any{}, map[string]any{"fresh": true, "sql_mode_forced": "fresh"}),
			[]string{`Saved for "shop": fast.`, "The router answers it in fresh mode. The router answers every server in fresh mode: SQL_FRESH=on"}, nil},
		{"every server fast by the setting", true, true, routerSays(map[string]any{}, map[string]any{"sql_mode_forced": "fast"}),
			[]string{`Saved for "shop": fresh.`, "every server in fast mode: SQL_FRESH=off", "applies once that is removed"}, nil},
		{"no router known", true, false, map[string]any{"configured": false},
			[]string{"does not know where its router runs"}, []string{"answers it in"}},
		{"a router that did not answer", true, false, map[string]any{"configured": true, "error": "connection refused"},
			[]string{"The router could not be read (connection refused)"}, []string{"answers it in"}},
		{"a router older than the per-server choice", true, true, map[string]any{"configured": true, "status": map[string]any{"fresh": false}},
			[]string{"older than the per-server choice"}, nil},
		{"a router older than fresh by default, nothing saved", true, false, map[string]any{"configured": true, "status": map[string]any{"fresh": false, "per_server_mode": true}},
			[]string{"older than fresh being the default"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			modeServers(d, tc.fresh, tc.saved)
			d.on("GET /api/router", 200, tc.router)
			var out bytes.Buffer
			if err := runPortMode(context.Background(), c, "shop", "", time.Second, &out); err != nil {
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
}

func TestPortModeSets(t *testing.T) {
	t.Run("saved, and the router takes it a moment later", func(t *testing.T) {
		fastModePoll(t)
		d, c := newFakeDaemon(t)
		modeServers(d, true, false)
		d.on("PUT /api/servers/id1/sql-mode", 200, map[string]any{"fresh": false, "fresh_saved": true, "like_ignores_case": false})
		d.seq("GET /api/router",
			routerSays(map[string]any{"id1": map[string]any{"sql_mode": "fresh"}}, nil),
			routerSays(map[string]any{"id1": map[string]any{"sql_mode": "fast", "sql_mode_from": "saved"}}, nil))
		var out bytes.Buffer
		if err := runPortMode(context.Background(), c, "shop", "fast", portModeWait, &out); err != nil {
			t.Fatal(err)
		}
		if b := d.bodies["PUT /api/servers/id1/sql-mode"]; len(b) != 1 || b["fresh"] != false {
			t.Fatalf("body = %v: only the mode is sent, so the LIKE switch is not touched", b)
		}
		o := out.String()
		if !strings.Contains(o, `Saved for "shop": fast.`) || !strings.Contains(o, "The router answers it in fast mode.") || strings.Contains(o, "still") {
			t.Fatalf("output:\n%s", o)
		}
	})
	t.Run("the router has not taken it within the wait", func(t *testing.T) {
		fastModePoll(t)
		d, c := newFakeDaemon(t)
		modeServers(d, true, false)
		d.on("PUT /api/servers/id1/sql-mode", 200, map[string]any{"fresh": false, "fresh_saved": true})
		d.on("GET /api/router", 200, routerSays(map[string]any{"id1": map[string]any{"sql_mode": "fresh"}}, nil))
		var out bytes.Buffer
		if err := runPortMode(context.Background(), c, "shop", "fast", portModeWait, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "The router still answers it in fresh mode.") || !strings.Contains(out.String(), "port mode shop") {
			t.Fatalf("output:\n%s", out.String())
		}
	})
	t.Run("the daemon kept the other mode", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		modeServers(d, true, false)
		d.on("PUT /api/servers/id1/sql-mode", 200, map[string]any{"fresh": true, "fresh_saved": true})
		err := runPortMode(context.Background(), c, "shop", "fast", 0, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), "still saved as fresh") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestPortModeRefuses(t *testing.T) {
	for name, want := range map[string]string{
		"boot":      "given where DBTrail starts",
		"old-index": "has no source database",
		"nope":      "nope",
	} {
		d, c := newFakeDaemon(t)
		modeServers(d, true, false)
		err := runPortMode(context.Background(), c, name, "fast", 0, &bytes.Buffer{})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", name, err, want)
		}
		if slices.Contains(d.calls, "PUT /api/servers/id1/sql-mode") {
			t.Errorf("%s: saved something", name)
		}
	}
}

func TestServerListSaysTheMode(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{
		{"id": "a", "name": "a", "kind": "registry", "has_source": true, "sql_fresh": true, "sql_fresh_saved": false},
		{"id": "b", "name": "b", "kind": "registry", "has_source": true, "sql_fresh": true, "sql_fresh_saved": true},
		{"id": "c", "name": "c", "kind": "registry", "has_source": true, "sql_fresh": false, "sql_fresh_saved": true},
	}})
	var out bytes.Buffer
	if err := runServerList(context.Background(), c, "text", &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	for i, want := range []string{"fresh (default)", "fresh", "fast"} {
		if got := strings.TrimSpace(lines[i+1][strings.LastIndex(lines[0], "MODE"):]); got != want {
			t.Errorf("row %d mode %q, want %q\n%s", i, got, want, out.String())
		}
	}
}
