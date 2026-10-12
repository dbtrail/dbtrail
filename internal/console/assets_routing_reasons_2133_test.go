package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// "Why each side" names why MySQL answered. copy_columns_differ also counts a
// statement the copy declined for a column's TYPE (#2133: arithmetic on a
// date column, a TIME or YEAR column named), so its line has to say so,
// drawn here through the real app.js. Since #2341 the reasons are the
// router's, drawn by routerNumbers inside the Connect a SQL client panel
// (routingBlock, which drew them from GET /api/flashback, is gone).
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
	view, err := json.Marshal(routerViewDTO{Configured: true, URL: "http://router:9091/status", ReadAt: "2026-10-11T14:31:00Z",
		Status: &RouterStatusDoc{Version: 1, Listen: "127.0.0.1:3309", CostThreshold: 10000, ScanRows: 100000,
			RouterNumbers: RouterNumbers{CountingSince: "2026-10-04T12:00:00Z", Servers: map[string]RouterServerNumbers{
				"s1": {Copy: 3, MySQL: 4, Reasons: map[string]uint64{"expensive_plan": 3, "copy_columns_differ": 4}},
			}}}})
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n, out = []) => { if (n == null) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; } if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const numbers = vm.runInContext("routerNumbers", ctx);
const view = ` + string(view) + `;
const drawn = numbers(view, { id: "s1", name: "shop", has_source: true, has_route: true, route_user: "report_ro", source_user: "repl" }, () => {});
// Only what sits under "Why each side" is read: the reason must be said
// there, not merely somewhere in the router's lines.
let why = null;
const each = (n, f) => { if (!n || typeof n === "string" || n.nodeType === 3) return; f(n); (n.children || []).forEach((c) => each(c, f)); };
each(drawn, (x) => { if (x.tag === "details" && flat((x.children || []).find((c) => c && c.tag === "summary")).join("") === "Why each side") why = x; });
console.log(why ? flat(why).join(" ").replace(/\s+/g, " ") : "<no Why each side>");
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
	for _, want := range []string{"Why each side", "3 × an expensive plan: the copy answered", "4 × the copy declined it", "SELECT *", "NATURAL JOIN", "a column the copy lacks", "+ or -, or AVG, on a date column or one of unknown type", "anywhere beside a subquery over such a table",
		"a TIME or YEAR column named", "a column's name right before a string", "no table definition", "take a new full snapshot"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in: %s", want, text)
		}
	}
	if strings.Contains(text, "—") || strings.Contains(text, "undefined") || strings.Contains(text, "copy_columns_differ") {
		t.Errorf("holds an em dash, undefined or the raw reason: %s", text)
	}
}
