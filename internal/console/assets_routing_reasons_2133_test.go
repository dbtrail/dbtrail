package console

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The "Who answered" block names why MySQL answered. copy_columns_differ now
// also counts a statement the copy declined for a column's TYPE (#2133:
// arithmetic on a date column, a TIME or YEAR column named), so its line has
// to say so, drawn here through the real app.js.
func TestRoutingBlockNamesTheColumnTypeRefusal(t *testing.T) {
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
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const block = vm.runInContext("routingBlock", ctx);
const routing = { enabled: true, max_copy_age: "10m0s", cost_threshold: 10000, scan_rows: 100000, since: "2026-10-04T12:00:00Z",
  servers: { s1: { copy: 3, mysql: 4, reasons: { expensive_plan: 3, copy_columns_differ: 4 } } } };
console.log(flat(block({ enabled: true, listen: "127.0.0.1:3308", port: "3308", routing }, { id: "s1", name: "shop", has_source: true, source_user: "repl" })).join(" ").replace(/\s+/g, " "));
`
	path := filepath.Join(t.TempDir(), "reasons.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	text := string(raw)
	t.Logf("drawn: %s", text)
	for _, want := range []string{"4 × the copy declined it", "SELECT *", "NATURAL JOIN", "a column the copy lacks", "+ or -, or AVG, on a date column or one of unknown type", "anywhere beside a subquery over such a table",
		"a TIME or YEAR column named", "no table definition", "take a new full snapshot"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in: %s", want, text)
		}
	}
	if strings.Contains(text, "—") || strings.Contains(text, "undefined") || strings.Contains(text, "copy_columns_differ") {
		t.Errorf("holds an em dash, undefined or the raw reason: %s", text)
	}
}
