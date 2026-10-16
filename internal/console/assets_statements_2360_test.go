package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// statementsHarnessJS draws the router's per-statement table (#2360) from
// the payloads GET /api/router answers: the first page of all rows, the copy's
// rows through its filter button, the page kept across a redraw, and the
// detail of the first row.
const statementsHarnessJS = `
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");
const call = (name) => vm.runInContext(name, ctx);
const cases = JSON.parse(process.argv[3]);
const out = {};
const line = (r) => [r.side, r.count, r.avg, r.slowest, r.errors, r.timeouts, Math.round(r.pct), r.text].join("|");
for (const [name, c] of Object.entries(cases)) {
  const m = call("routerStatementsModel")(c.view, c.cur);
  if (!m) { out[name] = { drawn: false }; continue; }
  const node = call("routerStatements")(m, "srv-" + name);
  const pg = call("routerStatementsPage")(m, "all", 0);
  const o = { drawn: true, state: m.state, shown: pg.rows.map(line), total: pg.total, pages: pg.pages,
    notCounted: m.notCounted || 0, screen: text(node), bars: find(node, "st-bar").length,
    titles: find(node, "st-text").map((n) => (n.getAttribute && n.getAttribute("title")) || n.title || "") };
  if (m.state === "rows") {
    // The copy's button, pressed: only the copy's rows, the page back to 1.
    const chip = find(node, "st-chip").find((b) => text(b).startsWith("DBTrail's copy"));
    if (chip) { chip.onclick(); o.copyScreen = text(node); o.copyBars = find(node, "st-bar").length; }
    // Redrawn (Refresh): the filter stays.
    const again = call("routerStatements")(m, "srv-" + name);
    o.keptFilter = (find(again, "st-chip").find((b) => (" " + b.className + " ").includes(" is-on ")) || {}).textContent || "";
    o.clamped = call("routerStatementsPage")(m, "all", 99).page;
    o.detail = call("routerStatementDetailParts")(m, m.rows[0]).map(text).join(" ");
  }
  out[name] = o;
}
process.stdout.write(JSON.stringify(out));
`

type drawStatementsOut struct {
	Drawn      bool
	State      string
	Shown      []string
	Total      int
	Pages      int
	NotCounted int
	Screen     string
	Bars       int
	Titles     []string
	CopyScreen string
	CopyBars   int
	KeptFilter string
	Clamped    int
	Detail     string
}

func TestDrawStatements2360(t *testing.T) {
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
	type c = map[string]any
	cur := c{"id": "a", "name": "orders-db"}
	view := func(top c, server c) c {
		st := c{"version": 1, "counting_since": "2026-10-11T19:00:00Z", "servers": c{"a": server}}
		for k, v := range top {
			st[k] = v
		}
		return c{"configured": true, "read_at": "2026-10-12T01:10:00Z", "status": st}
	}
	row := func(stmt, side string, count int, sec, slowest float64, errors, timeouts int) c {
		return c{"statement": stmt, "side": side, "count": count, "errors": errors, "timeouts": timeouts, "seconds": sec, "slowest_seconds": slowest}
	}
	with := func(r c, reasons c, fresh c) c {
		r["reasons"] = reasons
		if fresh != nil {
			r["fresh"] = fresh
		}
		return r
	}
	table := func(notCounted int, rows ...any) c { return c{"rows": rows, "not_counted": notCounted, "limit": 200} }
	twelve := []any{}
	for i := 0; i < 12; i++ {
		twelve = append(twelve, row("select * from t"+strings.Repeat("x", i), "mysql", 1, float64(12-i), float64(12-i), 0, 0))
	}
	long := "select " + strings.Repeat("a_column, ", 40) + "b from t"

	// One server's numbers as the Go side produces them.
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", StatementShape: readrouter.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	srv.RecordRouteDecision("a", "copy", "expensive_plan")
	srv.RecordRouteAnswer("a", "copy", "expensive_plan", "SELECT s, count(*) FROM orders GROUP BY s", 1500*time.Millisecond, false, false)
	srv.RecordRouteAnswer("a", "mysql", "cheap_plan", "SELECT * FROM orders WHERE id = 7", 2*time.Millisecond, true, false)
	rawDoc, _ := json.Marshal(srv.RouterNumbers())
	var fromGo c
	if err := json.Unmarshal(rawDoc, &fromGo); err != nil {
		t.Fatal(err)
	}
	fromGo["version"] = 1

	cases := c{
		"no router":   c{"view": c{"configured": false}, "cur": cur},
		"router down": c{"view": c{"configured": true, "error": "no answer"}, "cur": cur},
		"no server":   c{"view": view(c{"statement_table": true}, c{"copy": 1}), "cur": nil},
		"off":         c{"view": view(c{}, c{"copy": 3, "mysql": 4}), "cur": cur},
		"withheld": c{"view": func() c {
			v := view(c{"statement_table": true}, c{"copy": 3})
			v["statements_withheld"] = true
			return v
		}(), "cur": cur},
		"none yet":        c{"view": view(c{"statement_table": true}, c{"copy": 0, "statements": table(0)}), "cur": cur},
		"server unknown":  c{"view": view(c{"statement_table": true}, c{"copy": 1, "statements": table(0, row("select 1", "copy", 1, 1, 1, 0, 0))}), "cur": c{"id": "zz"}},
		"two sides":       c{"view": view(c{"statement_table": true}, c{"copy": 2, "mysql": 6, "statements": table(0, row("select s, count(*) from orders group by s", "copy", 2, 3, 2, 0, 0), row("select * from orders where id = ?", "mysql", 6, 0.012, 0.004, 2, 1))}), "cur": cur},
		"twelve":          c{"view": view(c{"statement_table": true}, c{"mysql": 12, "statements": table(0, twelve...)}), "cur": cur},
		"not counted":     c{"view": view(c{"statement_table": true}, c{"mysql": 9, "statements": table(7, row("select 1", "mysql", 2, 1, 1, 0, 0))}), "cur": cur},
		"junk rows":       c{"view": view(c{"statement_table": true}, c{"mysql": 9, "statements": c{"rows": []any{nil, 3, "x", c{"side": "mysql", "count": 1}, c{"statement": "select 2", "side": "elsewhere", "count": 1}, c{"statement": "select 3", "side": "mysql", "count": 0}, row("select 4", "mysql", 1, -5, -1, -1, 9)}}}), "cur": cur},
		"only blanks":     c{"view": view(c{"statement_table": true}, c{"mysql": 3, "statements": table(3)}), "cur": cur},
		"rows not a list": c{"view": view(c{"statement_table": true}, c{"mysql": 9, "statements": c{"rows": "x"}}), "cur": cur},
		"long statement":  c{"view": view(c{"statement_table": true}, c{"mysql": 1, "statements": table(0, row(long, "mysql", 1, 1, 1, 0, 0))}), "cur": cur},
		"unsorted":        c{"view": view(c{"statement_table": true}, c{"mysql": 2, "statements": table(0, row("select small", "mysql", 1, 1, 1, 0, 0), row("select big", "mysql", 1, 4, 4, 0, 0))}), "cur": cur},
		"from go":         c{"view": c{"configured": true, "status": fromGo}, "cur": cur},
		"fresh": c{"view": view(c{"statement_table": true}, c{"copy": 9, "mysql": 1, "statements": table(0,
			with(row("select a from t group by a", "copy", 9, 0.81, 0.21, 0, 0), c{"expensive_plan": 9}, c{"last_age_seconds": 9, "last_as_of": "2026-10-16T10:41:02Z", "min_age_seconds": 4, "max_age_seconds": 22, "known": 9, "tailed": 9, "snapshot": 0}),
			with(row("select a from t group by a", "mysql", 1, 7.9, 7.9, 0, 0), c{"copy_refused": 1}, nil))}), "cur": cur},
		"stale": c{"view": view(c{"statement_table": true}, c{"copy": 6, "statements": table(0,
			with(row("select b from u group by b", "copy", 6, 0.3, 0.09, 0, 0), c{"expensive_plan": 6}, c{"last_age_seconds": 212, "min_age_seconds": 140, "max_age_seconds": 298, "known": 6, "unknown": 0, "tailed": 0, "snapshot": 6}))}), "cur": cur},
		"pending": c{"view": view(c{"statement_table": true}, c{"copy": 1, "statements": table(0,
			with(row("select c from v group by c", "copy", 1, 0.3, 0.3, 0, 0), c{"expensive_plan": 1}, c{"pending": true, "known": 0, "tailed": 0, "snapshot": 1}))}), "cur": cur},
		"mixed": c{"view": view(c{"statement_table": true}, c{"copy": 10, "statements": table(0,
			with(row("select f from y group by f", "copy", 10, 0.3, 0.3, 0, 0), c{"expensive_plan": 10}, c{"last_age_seconds": 3, "min_age_seconds": 3, "max_age_seconds": 3, "known": 1, "unknown": 9, "unknown_reason": "which snapshot the answer read could not be told", "tailed": 1, "snapshot": 0}))}), "cur": cur},
		"capture behind": c{"view": view(c{"statement_table": true}, c{"copy": 2, "statements": table(0,
			with(row("select g from z group by g", "copy", 2, 0.3, 0.3, 0, 0), c{"expensive_plan": 2}, c{"last_age_seconds": 600, "min_age_seconds": 590, "max_age_seconds": 600, "known": 2, "tailed": 2, "snapshot": 0}))}), "cur": cur},
		"no file known": c{"view": view(c{"statement_table": true}, c{"copy": 1, "statements": table(0,
			with(row("select e from x group by e", "copy", 1, 0.3, 0.3, 0, 0), c{"expensive_plan": 1}, c{"known": 0, "unknown": 1, "unknown_reason": "which snapshot the answer read could not be told", "tailed": 0, "snapshot": 0}))}), "cur": cur},
		"old router": c{"view": view(c{"statement_table": true}, c{"copy": 1, "statements": table(0, row("select d from w group by d", "copy", 1, 0.3, 0.3, 0, 0))}), "cur": cur},
	}
	arg, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "draw.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+statementsHarnessJS), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(arg)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var out map[string]drawStatementsOut
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}

	for _, name := range []string{"no router", "router down", "no server", "server unknown"} {
		if out[name].Drawn {
			t.Errorf("%s: drawn %+v", name, out[name])
		}
	}
	for name, want := range map[string]struct{ state, says string }{
		"off":             {"off", "ROUTER_STATEMENT_TABLE=1"},
		"withheld":        {"withheld", "no data profile"},
		"none yet":        {"none", "No statement has finished"},
		"rows not a list": {"none", "No statement has finished"},
	} {
		o := out[name]
		if !o.Drawn || o.State != want.state || !strings.Contains(o.Screen, want.says) || o.Bars != 0 {
			t.Errorf("%s: %+v, want state %s saying %q", name, o, want.state, want.says)
		}
	}
	if o := out["off"]; !strings.Contains(o.Screen, "names your tables and columns") || !strings.Contains(o.Screen, "no password") {
		t.Errorf("off does not say what turning it on shows, and to whom: %q", o.Screen)
	}

	two := out["two sides"]
	wantTwo := []string{
		"copy|2|1.5 s|2 s|0|0|100|select s, count(*) from orders group by s",
		"mysql|6|2 ms|4 ms|2|1|0|select * from orders where id = ?",
	}
	if two.State != "rows" || strings.Join(two.Shown, "\n") != strings.Join(wantTwo, "\n") || two.Bars != 2 {
		t.Errorf("two sides: %+v", two)
	}
	for _, says := range []string{"Which statements take the time", "All2", "Your MySQL1", "DBTrail's copy1", "2 times", "6 times", "1.5 s each", "2 errors", "1 out of time"} {
		if !strings.Contains(two.Screen, says) {
			t.Errorf("two sides: no %q in %q", says, two.Screen)
		}
	}
	// The copy's button shows the copy's rows only.
	if two.CopyBars != 1 || !strings.Contains(two.CopyScreen, "select s, count(*)") || strings.Contains(two.CopyScreen, "where id = ?") {
		t.Errorf("two sides, the copy's filter: %d bars, %q", two.CopyBars, two.CopyScreen)
	}
	// A redraw (Refresh) keeps the filter the reader picked.
	if !strings.HasPrefix(two.KeptFilter, "DBTrail's copy") {
		t.Errorf("two sides: the filter after a redraw is %q", two.KeptFilter)
	}
	if strings.Contains(two.Screen, "NaN") || strings.Contains(two.Screen, "undefined") {
		t.Errorf("two sides: %q", two.Screen)
	}

	// Five to a page, with where the reader is.
	if o := out["twelve"]; len(o.Shown) != 5 || o.Total != 12 || o.Pages != 3 || o.Bars != 5 || !strings.Contains(o.Screen, "1 to 5 of 12") ||
		!strings.Contains(o.Screen, "Previous") || !strings.Contains(o.Screen, "Next") || o.Clamped != 2 {
		t.Errorf("twelve: shown %d of %d in %d pages, bars %d, page 99 is %d\n%q", len(o.Shown), o.Total, o.Pages, o.Bars, o.Clamped, o.Screen)
	}
	// One page: no pager.
	if o := out["two sides"]; o.Pages != 1 || strings.Contains(o.Screen, "Previous") {
		t.Errorf("one page has a pager: %q", o.Screen)
	}
	if o := out["only blanks"]; o.State != "none" || strings.Contains(o.Screen, "No statement has finished") || !strings.Contains(o.Screen, "3 statements are not listed") {
		t.Errorf("only blanks: the two lines contradict each other: %q", o.Screen)
	}
	if o := out["not counted"]; o.NotCounted != 7 || !strings.Contains(o.Screen, "7 statements are not listed") || !strings.Contains(o.Screen, "200 lines for each server, one per statement and side") {
		t.Errorf("not counted: %+v", o)
	}
	if o := out["junk rows"]; len(o.Shown) != 1 || o.Shown[0] != "mysql|1|||0|0|0|select 4" || strings.Contains(o.Screen, "NaN") {
		t.Errorf("junk rows: %+v", o)
	}
	if o := out["long statement"]; len(o.Titles) != 1 || o.Titles[0] != long {
		t.Errorf("long statement: the whole text is not on hover: %+v (%+v)", o.Titles, o)
	}
	if o := out["unsorted"]; len(o.Shown) != 2 || !strings.HasSuffix(o.Shown[0], "select big") {
		t.Errorf("unsorted: most time first: %+v", o.Shown)
	}
	if o := out["from go"]; o.State != "rows" || len(o.Shown) != 2 ||
		o.Shown[0] != "copy|1|1.5 s|1.5 s|0|0|100|select s, count(*) from orders group by s" ||
		o.Shown[1] != "mysql|1|2 ms|2 ms|1|0|0|select * from orders where id = ?" {
		t.Errorf("from go: %+v", o)
	}
	// The detail of a row fed by the JSON the Go side produces: its reason.
	if o := out["from go"]; !strings.Contains(o.Detail, "an expensive plan: the copy answered") {
		t.Errorf("from go, detail: %q", o.Detail)
	}

	// The detail: both sides of one statement, why each, and how fresh.
	for name, says := range map[string][]string{
		"fresh": {"Answered by DBTrail's copy", "Answered by your MySQL", "Fresh: every table it read", "9 seconds", "4 seconds", "22 seconds",
			"9 × an expensive plan: the copy answered", "1 × the copy refused it", "at most 22 seconds behind capture", "Current: your MySQL answered from its own tables, with no lag"},
		"stale":          {"Not fresh: it read at least one table as of its snapshot", "3 minutes 32 seconds", "4 minutes 58 seconds", "at most 4 minutes 58 seconds behind capture", "Your MySQL never answered this statement"},
		"mixed":          {"Mixed: of its answers, 1 read every table up to date, for 9 how fresh it was is not known", "counting only the answers whose lag is known", "9 of 10", "last one: which snapshot the answer read could not be told"},
		"capture behind": {"Fresh: every table it read was brought up to date", "at most 10 minutes behind capture"},
		"pending":        {"being worked out"},
		"old router":     {"The router did not say how fresh these answers were", "The router did not say why"},
	} {
		o := out[name]
		for _, w := range says {
			if !strings.Contains(o.Detail, w) {
				t.Errorf("%s detail: no %q in %q", name, w, o.Detail)
			}
		}
		for _, bad := range []string{"NaN", "undefined", "null", "—", "Invalid Date"} {
			if strings.Contains(o.Detail, bad) {
				t.Errorf("%s detail holds %q: %q", name, bad, o.Detail)
			}
		}
	}
	// Unknown is never said as fresh.
	for _, name := range []string{"pending", "no file known", "mixed"} {
		if o := out[name]; strings.Contains(o.Detail, "Fresh:") {
			t.Errorf("%s is said fresh: %q", name, o.Detail)
		}
	}
	if o := out["no file known"]; !strings.Contains(o.Detail, "How each answer read its tables is not known") || !strings.Contains(o.Detail, "unknown") {
		t.Errorf("no file known: %q", o.Detail)
	}
	// The list shows the copy's lag on its line.
	if o := out["fresh"]; !strings.Contains(o.Screen, "Lag 9 seconds") {
		t.Errorf("fresh: no lag on the line: %q", o.Screen)
	}
}
