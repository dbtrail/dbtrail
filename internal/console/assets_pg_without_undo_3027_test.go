package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #3027: on PostgreSQL, REPLICA IDENTITY FULL is needed only for undo. What
// the page says about it, drawn by the real app.js in node: the health card
// for a server with tables below FULL, and the hint of the add-server form.
func TestPGWithoutUndoWording_3027(t *testing.T) {
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
	harness, _, ok := strings.Cut(formHarnessJS, "(async () => {")
	if !ok {
		t.Fatal("formHarnessJS no longer holds its scenario marker")
	}
	// The health snapshot as the daemon persists it (pgstreamrun.sourceHealthJSON).
	script := renderHarnessJS + harness + `
const card = (nf) => { ctx.__h = { exists: true, active: true, wal_status: "reserved", retained_bytes: 16384, safe_wal_size: null,
  replica_identity_not_full: nf, checked_at: new Date().toISOString() }; return vm.runInContext("pgHealthCard(__h)", ctx); };
const classes = (n, out = []) => { walk(n, (x) => { if (/hstat-/.test(x.className)) out.push(x.className.split(" ").filter((c) => /^hstat-/.test(c)).join(" ") + ":" + x.textContent); }); return out; };
const f = show({ monitor: true }, null);
f.elements.flavor.value = "postgres"; f.elements.flavor.fire("change");
const pgNodes = [], myNodes = [];
// The harness's textContent leaves out the plain strings a node was given
// with append(); a hint is built from those and <code> elements.
const txt = (n) => typeof n === "string" ? n : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(txt).join("");
walk(f, (n) => { if (n.attrs["data-flavor"] === "postgres" && (n.tag === "p" || n.tag === "pre")) pgNodes.push({ tag: n.tag, on: n.classList.contains("flavor-on"), text: txt(n) }); });
console.log(JSON.stringify({
  none: { text: card([]).textContent, stats: classes(card([])) },
  one: { text: card(["public.orders (relreplident=d)"]).textContent, stats: classes(card(["public.orders (relreplident=d)"])) },
  two: { text: card(["public.a (relreplident=d)", "public.b (relreplident=i)"]).textContent },
  form: pgNodes,
}));
`
	path := filepath.Join(t.TempDir(), "noundo.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		None, One struct {
			Text  string
			Stats []string
		}
		Two  struct{ Text string }
		Form []struct {
			Tag, Text string
			On        bool
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("health card, one table below FULL: %s", got.One.Text)

	if !strings.Contains(got.None.Text, "replica identityall FULL ✓") {
		t.Errorf("every table at FULL: %q", got.None.Text)
	}
	const explain = "Not at REPLICA IDENTITY FULL: recorded, and everything else works. Undo of an UPDATE or a DELETE is refused; ALTER TABLE ... REPLICA IDENTITY FULL makes later changes undoable."
	for _, want := range []string{"replica identity⚠ 1 table without undo", explain, "public.orders (relreplident=d)"} {
		if !strings.Contains(got.One.Text, want) {
			t.Errorf("the card lacks %q:\n%s", want, got.One.Text)
		}
	}
	if !strings.Contains(got.Two.Text, "⚠ 2 tables without undo") {
		t.Errorf("two tables: %q", got.Two.Text)
	}
	for _, text := range []string{got.One.Text, got.Two.Text} {
		for _, gone := range []string{"not FULL\"", " not FULL", "NOT captured", "not captured"} {
			if strings.Contains(strings.ReplaceAll(text, explain, ""), gone) {
				t.Errorf("the card still says %q: %s", gone, text)
			}
		}
	}
	// Amber, never the red of a fault: the table is recorded.
	amber, red := false, false
	for _, s := range got.One.Stats {
		amber = amber || strings.HasPrefix(s, "hstat-warn:⚠ 1 table without undo")
		red = red || strings.HasPrefix(s, "hstat-err")
	}
	if !amber || red {
		t.Errorf("the count must be amber (hstat-warn) and nothing red: %v", got.One.Stats)
	}

	// The form: FULL is not among what the source needs, and is offered apart.
	var texts []string
	for _, n := range got.Form {
		if !n.On {
			t.Errorf("a PostgreSQL hint is not shown for the postgres flavor: %q", n.Text)
		}
		texts = append(texts, n.Tag+": "+n.Text)
	}
	t.Logf("the form's PostgreSQL hints:\n%s", strings.Join(texts, "\n"))
	want := []string{
		"p: PostgreSQL source needs wal_level=logical, a role with the REPLICATION attribute, and a publication you create; copy and run on the source:",
		"pre: CREATE PUBLICATION bintrail_pub FOR ALL TABLES;",
		"p: REPLICA IDENTITY FULL is optional, only for undo: without it a table is recorded and everything else works, but its UPDATE and DELETE cannot be undone. To make a table's later changes undoable (the table then writes more WAL):",
		"pre: ALTER TABLE your_table REPLICA IDENTITY FULL;",
	}
	for _, w := range want {
		found := false
		for _, got := range texts {
			found = found || got == w
		}
		if !found {
			t.Errorf("the form lacks %q", w)
		}
	}
}
