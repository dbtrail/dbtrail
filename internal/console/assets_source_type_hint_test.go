package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The Source type saved with a console server is a hint: when the server
// contradicts it, capture follows the server and the Overview shows a warning
// card. The input is marshalled from serverDTO, so the key is the real one.
func TestOverviewShowsSourceTypeWarning(t *testing.T) {
	node := nodeOrSkip(t)
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	warning := "This server is saved with Source type MariaDB, but the server reports MySQL. DBTrail captures it as MySQL. To fix the label, remove the server and add it again with Source type MySQL."
	server := func(w string) map[string]any {
		var m map[string]any
		if err := json.Unmarshal([]byte(mustJSON(t, serverDTO{ID: "a", Kind: "registry", HasSource: true, MonitorState: "running", MonitorWarning: w})), &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	type c = map[string]any
	input := func(w string) c {
		return c{"input": c{
			"coverage":  c{"freshness": "current", "continuity": "ok", "lag_seconds": 3, "delta_to": "2026-09-29 10:00:00"},
			"baselines": c{}, "server": server(w), "schema": c{"state": "idle"}, "uncaptured": c{}}}
	}
	arg, err := json.Marshal(map[string]c{"warned": input(warning), "agree": input("")})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sourcetype.js")
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
	w := out["warned"]
	found := false
	for _, k := range w.Cards {
		if k.Kind == "source-type" {
			found = true
			t.Logf("card: %s / %q", k.Title, k.Lines)
			if len(k.Lines) != 1 || k.Lines[0] != warning {
				t.Errorf("card lines = %q", k.Lines)
			}
		}
	}
	if !found || !strings.Contains(w.Screen, "Source type does not match the server") {
		t.Errorf("no source-type card on screen: cards %+v", w.Cards)
	}
	for _, k := range out["agree"].Cards {
		if k.Kind == "source-type" {
			t.Errorf("a card appeared with no warning: %+v", k)
		}
	}
	// Capture itself stays green: a hint is never a failure.
	for _, p := range w.Pieces {
		if p.Title == "binlog" && p.Tone != "ok" {
			t.Errorf("capture piece = %+v, want ok", p)
		}
	}
}
