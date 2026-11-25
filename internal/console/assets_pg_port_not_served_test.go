package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The Overview of a PostgreSQL server the PostgreSQL port does not serve
// (0.140.0) has a card that says so, in the words its clients get at login,
// from the field GET /api/servers fills; one the port serves has none.
func TestOverviewShowsPGPortNotServed(t *testing.T) {
	node := nodeOrSkip(t)
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	refused := PGPortNotServed("shop", PGRouteRefusedNoAccount)
	server := func(why string) map[string]any {
		var m map[string]any
		if err := json.Unmarshal([]byte(mustJSON(t, serverDTO{ID: "a", Kind: "registry", HasSource: true, MonitorState: "running", Flavor: FlavorPostgres, PGPortRefused: why})), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	type c = map[string]any
	input := func(why string) c {
		return c{"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "lag_seconds": 3, "delta_to": "2026-09-29 10:00:00"},
			"baselines": c{}, "server": server(why), "schema": c{"state": "idle"}, "uncaptured": c{}}}
	}
	arg, err := json.Marshal(map[string]c{"refused": input(refused), "served": input("")})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "pgport.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+flowHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]flowOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	found := 0
	for _, k := range out["refused"].Cards {
		if k.Kind == "pg-port-not-served" {
			found++
			if k.Title != "The PostgreSQL port does not serve this server" || len(k.Lines) != 1 || k.Lines[0] != refused {
				t.Errorf("card %q / %q", k.Title, k.Lines)
			}
		}
	}
	if found != 1 {
		t.Errorf("%d cards for a server the port does not serve: %+v", found, out["refused"].Cards)
	}
	for _, k := range out["served"].Cards {
		if k.Kind == "pg-port-not-served" {
			t.Errorf("a card for a server the port serves: %+v", k)
		}
	}
}
