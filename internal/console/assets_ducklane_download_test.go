package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestDuckLaneDownloadsTheArchiveAlone drives the real backupDuckLane: one
// button, the archive; both files drawn (the archive carries views.sql); the
// lead says one download; the command to open it is on the lane from the
// start, since the file it names is already inside the archive. With the
// views capability off the lane is the same: the archive's views.sql does
// not depend on it.
func TestDuckLaneDownloadsTheArchiveAlone(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const walk = (n, f) => { if (!n || n.nodeType !== 1) return []; const out = f(n) ? [n] : [];
  for (const c of n.children || []) out.push(...walk(c, f)); return out; };
const text = (n) => walk(n, () => true).map((x) => x._text || "").join(" ");
const out = {};
for (const views of [true, false]) {
  vm.runInContext("capsCache = { views: " + views + " }; capsKnown = true;", ctx);
  const l = vm.runInContext("backupDuckLane({ configured: true, snapshots: [{ time: '2026-06-10 12:00:00' }] })", ctx);
  out[views ? "on" : "off"] = {
    buttons: walk(l, (n) => n.tag === "button").map((b) => b._text),
    text: text(l),
  };
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "ducklane.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		Buttons []string
		Text    string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for _, k := range []string{"on", "off"} {
		g := got[k]
		// The Copy button belongs to the command line; the only download is the archive.
		var downloads []string
		for _, b := range g.Buttons {
			if b != "Copy" {
				downloads = append(downloads, b)
			}
		}
		if len(downloads) != 1 || downloads[0] != "Download the data" {
			t.Errorf("views capability %s: buttons %q, want the archive's alone", k, g.Buttons)
		}
		for _, want := range []string{"Parquet files", "views.sql", "One download.", "Unpack it, go into its folder, and run:", "duckdb -init views.sql lake.db", "No views.sql in the folder?"} {
			if !strings.Contains(g.Text, want) {
				t.Errorf("views capability %s: the lane lacks %q in:\n%s", k, want, g.Text)
			}
		}
	}
}
