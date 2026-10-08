package console

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// sqlUnmergedHarnessJS calls the two pure helpers, then paints the real SQL
// panel over the fake DOM with GET /api/sql answered by arg.info, and reads
// back each table row: its name, the note beside it, the note's class and
// the row's tooltip, plus the lines under the list.
const sqlUnmergedHarnessJS = `
const f = (name) => vm.runInContext(name, ctx);
const arg = JSON.parse(process.argv[3]);
const walk = (n, out = []) => { if (!n || !n.children) return out; out.push(n); for (const c of n.children) walk(c, out); return out; };
const cls = (n) => (n.className || (n.attrs && n.attrs.class) || "").trim();
(async () => {
  const out = {
    notes: arg.notes.map((c) => f("sqlWaitingNote")(c.u, c.max)),
    over: arg.over.map((c) => f("sqlOverNote")(c.unmerged, c.max)),
  };
  vm.runInContext("api = (p) => Promise.resolve(__info);", Object.assign(ctx, { __info: arg.info }));
  const box = f("el")("div");
  Object.defineProperty(box, "isConnected", { get: () => true });
  f("renderSQLPanel")(box);
  await new Promise((r) => setImmediate(r));
  out.rows = walk(box).filter((n) => cls(n).split(" ").includes("sqlp-name")).map((b) => {
    const kids = b.children || [];
    const wait = kids.find((k) => cls(k).includes("sqlp-wait"));
    return { name: kids[0] ? kids[0].textContent : "", wait: wait ? wait.textContent : "", cls: wait ? cls(wait) : "",
      title: b.title || (b.attrs && b.attrs.title) || "" };
  });
  out.lines = walk(box).filter((n) => cls(n).includes("sqlp-note")).map((n) => n.textContent);
  // A run, refused, then the list read again: the merge that happened
  // meanwhile clears the red.
  ctx.__info = arg.after;
  vm.runInContext("MutationObserver = undefined; sqlPost = () => Promise.reject(apiError(422, 'refused'));", ctx);
  const runBtn = walk(box).find((n) => cls(n).split(" ").includes("sqlp-run"));
  await runBtn.onclick();
  for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
  out.after = walk(box).filter((n) => cls(n).split(" ").includes("sqlp-name")).map((b) => {
    const w = (b.children || []).find((k) => cls(k).includes("sqlp-wait"));
    return w ? w.textContent + "|" + cls(w) : "";
  });
  out.afterLines = walk(box).filter((n) => cls(n).includes("sqlp-wait-over")).map((n) => n.textContent);
  process.stdout.write(JSON.stringify(out));
})();
`

type sqlWaitNote struct {
	Text  string `json:"text"`
	Level string `json:"level"`
	Title string `json:"title"`
}

func TestSQLPanel_changesWaitingBesideEachTable(t *testing.T) {
	type c = map[string]any
	arg := c{
		"notes": []c{
			{"u": nil, "max": 48},
			{"u": c{"view": "a.b", "mb": 0}, "max": 48},
			{"u": c{"view": "a.b", "mb": 35}, "max": 48},
			{"u": c{"view": "a.b", "mb": 36, "level": "near"}, "max": 48}, // the server decides, on bytes
			{"u": c{"view": "a.b", "mb": 49, "level": "over"}, "max": 48},
			{"u": c{"view": "a.b", "mb": 40}, "max": 0}, // line not known
			{"u": c{"view": "a.b", "mb": 1, "unknown": true}, "max": 48},
		},
		"over": []c{
			{"unmerged": nil, "max": 48},
			{"unmerged": []c{{"view": "shop.orders", "mb": 40}}, "max": 48},
			{"unmerged": []c{{"view": "shop.orders", "mb": 60, "level": "over"}}, "max": 48},
			{"unmerged": []c{{"view": "shop.a", "mb": 60, "level": "over"}, {"view": "shop.b", "mb": 50, "level": "over"}}, "max": 48},
		},
		"info": c{
			"views":  []string{"shop.customers", "shop.orders", "shop.stock"},
			"limits": c{"timeout_seconds": 60, "max_rows": 1000, "memory": "2 GB", "max_unmerged_mb": 48},
			"unmerged": []c{
				{"view": "shop.stock", "mb": 100, "level": "over"},
				{"view": "shop.orders", "mb": 40, "level": "near"},
				{"view": "shop.gone", "mb": 7}, // not listed: ignored
			},
		},
		"after": c{
			"views":    []string{"shop.customers", "shop.orders", "shop.stock"},
			"limits":   c{"max_unmerged_mb": 48},
			"unmerged": []c{{"view": "shop.orders", "mb": 41, "level": "near"}},
		},
	}
	raw := runSQLPanelHarness(t, sqlUnmergedHarnessJS, arg)
	var out struct {
		Notes []*sqlWaitNote `json:"notes"`
		Over  []string       `json:"over"`
		Rows  []struct {
			Name, Wait, Cls, Title string
		} `json:"rows"`
		Lines      []string `json:"lines"`
		After      []string `json:"after"`
		AfterLines []string `json:"afterLines"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}

	if out.Notes[0] != nil || out.Notes[1] != nil {
		t.Errorf("nothing waiting must draw nothing: %+v %+v", out.Notes[0], out.Notes[1])
	}
	for i, want := range []struct{ text, level string }{{"35 MB waiting", ""}, {"36 MB waiting", "near"}, {"49 MB waiting", "over"}, {"40 MB waiting", ""}, {"size unknown", ""}} {
		n := out.Notes[i+2]
		if n == nil || n.Text != want.text || n.Level != want.level {
			t.Errorf("note %d = %+v, want %q level %q", i+2, n, want.text, want.level)
		}
	}
	if n := out.Notes[4]; n == nil || !strings.Contains(n.Title, "at most 48 MB") || !strings.Contains(n.Title, "is answered from an earlier copy until DBTrail merges them") {
		t.Errorf("over title: %+v", n)
	}
	if n := out.Notes[5]; n == nil || strings.Contains(n.Title, "at most") {
		t.Errorf("unknown line must not print one: %+v", n)
	}

	if out.Over[0] != "" || out.Over[1] != "" {
		t.Errorf("no table over: %q", out.Over[:2])
	}
	if want := "shop.orders has more changes waiting than SQL here merges (48 MB per query, all its tables together), so a query naming it is answered from an earlier copy until DBTrail merges them, which it does on its own. More memory for SQL raises the line."; out.Over[2] != want {
		t.Errorf("one over:\n got %q\nwant %q", out.Over[2], want)
	}
	if !strings.HasPrefix(out.Over[3], "2 tables have more changes waiting than SQL here merges (48 MB per query, all its tables together), so a query naming them is answered from an earlier copy") {
		t.Errorf("two over: %q", out.Over[3])
	}

	type row struct{ Name, Wait, Cls string }
	var rows []row
	for _, r := range out.Rows {
		rows = append(rows, row{r.Name, r.Wait, r.Cls})
	}
	wantRows := []row{
		{"shop.customers", "", ""},
		{"shop.orders", "40 MB waiting", "sqlp-wait sqlp-wait-near"},
		{"shop.stock", "100 MB waiting", "sqlp-wait sqlp-wait-over"},
	}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Errorf("rows = %+v\nwant   %+v", rows, wantRows)
	}
	if len(out.Rows) == 3 && (out.Rows[0].Title != "Insert shop.customers" || !strings.HasPrefix(out.Rows[2].Title, "Insert shop.stock. 100 MB of changes waiting")) {
		t.Errorf("titles: %q / %q", out.Rows[0].Title, out.Rows[2].Title)
	}
	if n := out.Notes[6]; n == nil || !strings.Contains(n.Title, "could not be read") || strings.Contains(n.Title, "1 MB") {
		t.Errorf("unknown size: %+v", n)
	}
	if want := []string{"", "41 MB waiting|sqlp-wait sqlp-wait-near", ""}; !reflect.DeepEqual(out.After, want) || len(out.AfterLines) != 0 {
		t.Errorf("after a run the list is read again: rows %q, red lines %q", out.After, out.AfterLines)
	}
	if len(out.Lines) != 1 || !strings.HasPrefix(out.Lines[0], "shop.stock has more changes waiting") {
		t.Errorf("lines under the list = %q", out.Lines)
	}
}
