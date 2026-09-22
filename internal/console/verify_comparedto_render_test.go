package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The verify page says which read of the database the compared tables were
// checked against, drawn by the real code over the wire shape: one read in
// one sentence, several reads counted with each on its table, and nothing
// when no table was compared (a "match" there cannot be read as a claim about
// the newest snapshot).
func TestVerificationPage_namesTheReadCompared(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	row := func(table, at string) VerifyTableResult {
		return VerifyTableResult{Schema: "shop", Table: table, Status: "match", ComparedTo: at}
	}
	runs := map[string]VerifyStatus{
		"one": {State: VerifyStateSucceeded, Results: []VerifyTableResult{
			row("a", "2026-09-02T03:00:00Z"), row("b", "2026-09-02T03:00:00Z")}},
		"two": {State: VerifyStateSucceeded, Results: []VerifyTableResult{
			row("a", "2026-09-02T03:00:00Z"), row("b", "2026-09-05T03:00:00Z")}},
		"none": {State: VerifyStateSucceeded, Results: []VerifyTableResult{
			{Schema: "shop", Table: "a", Status: "inconclusive", Reason: "no earlier snapshot"}}},
	}
	var js strings.Builder
	for name, st := range runs {
		b, err := json.Marshal(st.WithVerdict())
		if err != nil {
			t.Fatal(err)
		}
		js.WriteString("  " + name + ": " + string(b) + ",\n")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(flat).join("");
const byClass = (n, cls, out = []) => { if (!n) return out; if (String(n.className || "").includes(cls)) out.push(n); (n.children || []).forEach((c) => byClass(c, cls, out)); return out; };
const runs = {
` + js.String() + `};
const out = {};
for (const [name, st] of Object.entries(runs)) {
  const box = vm.runInContext("el", ctx)("div");
  vm.runInContext("renderVerifyResults", ctx)(box, st, "a");
  const line = byClass(box, "vfy-compared-to")[0];
  out[name] = { line: line ? flat(line) : "", titles: byClass(box, "vfy-tbl").map((n) => (n.attrs && n.attrs.title) || n.title || "") };
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "comparedto.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]struct {
		Line   string
		Titles []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for name, g := range got {
		t.Logf("%s: %q %q", name, g.Line, g.Titles)
	}
	if g := got["one"]; g.Line != "Compared against the last read of your database, at 2026-09-02 03:00:00 UTC." {
		t.Errorf("one read: line %q", g.Line)
	}
	if g := got["two"]; !strings.Contains(g.Line, "at 2 different times") ||
		!strings.Contains(strings.Join(g.Titles, "|"), "Compared against the read of 2026-09-05 03:00:00 UTC") {
		t.Errorf("two reads: line %q, titles %q; want the count, and each table's read on the table", g.Line, g.Titles)
	}
	if g := got["none"]; g.Line != "" || strings.Join(g.Titles, "") != "" {
		t.Errorf("nothing compared: line %q, titles %q; want neither", g.Line, g.Titles)
	}
}
