package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// sqlPanelHarnessJS calls the SQL panel's pure functions in the real app.js
// with plain values, and paints one result table over the fake DOM. The
// fake element here REFUSES innerHTML: a value painted through markup
// parsing throws, so "text is rendered as text" is a property of this test
// and not an accident of a permissive fake.
const sqlPanelHarnessJS = `
Object.defineProperty(FakeEl.prototype, "innerHTML", { set(v) { throw new Error("markup was parsed from a string: " + v); }, get() { return ""; } });
const f = (name) => vm.runInContext(name, ctx);
const arg = JSON.parse(process.argv[3]);
const walk = (n, tag, out = []) => { if (!n || !n.children) return out; if (tag === "*" || n.tag === tag) out.push(n); for (const c of n.children) walk(c, tag, out); return out; };
const table = f("sqlResultTable")(arg.table, 5);
const max = f("SQL_LIST_MAX");
const out = {
  listMax: max,
  numeric: Object.fromEntries(arg.types.map((t) => [t, f("sqlColumnIsNumeric")(t)])),
  ago: arg.ago.map((c) => f("sqlAgo")(c.iso, c.now)),
  status: arg.status.map((c) => f("sqlStatusLine")(c.info, c.now)),
  filter: arg.filter.map((c) => f("sqlFilterViews")(c.names, c.filter, max)),
  starter: arg.starter.map((n) => f("sqlStarterQuery")(n)),
  cells: arg.cells.map((v) => f("sqlCell")(v)),
  count: arg.count.map((c) => f("sqlCountLine")(c.res, c.ms)),
  notes: arg.notes.map((c) => f("sqlResultNotes")(c.res, c.exact)),
  errors: arg.errors.map((c) => f("sqlErrorView")(c.status, c.message, c.limits)),
  table: {
    caption: walk(table, "caption").map((n) => n.textContent),
    tags: Array.from(new Set(walk(table, "*").map((n) => n.tag))).sort(),
    th: walk(table, "th").map((n) => ({ text: n.textContent, cls: (n.className || "").trim(), title: n.attrs.title || n.title || "" })),
    td: walk(table, "td").map((n) => ({ text: n.textContent, cls: (n.className || "").trim(), nulls: walk(n, "span").length })),
  },
};
process.stdout.write(JSON.stringify(out));
`

// runSQLPanelHarness runs harness (appended to the shared prelude) in node
// with the real app.js and one JSON argument, and returns its stdout.
func runSQLPanelHarness(t *testing.T, harness string, arg any) []byte {
	t.Helper()
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
	payload, err := json.Marshal(arg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "h.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+harness), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(payload)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	return raw
}

// TestSQLPanelPureFunctions pins what a person reads in the SQL panel:
// which columns align right (by type), the status line built from what the
// server reported, the table list's cap, the first query, how NULL and
// nested values show, the notices, and one sentence per way a query can
// fail, fed the server's own messages.
func TestSQLPanelPureFunctions(t *testing.T) {
	type c = map[string]any
	const now = 1790000000000 // a fixed clock, in ms
	iso := func(secAgo int64) string { return time.Unix(now/1000-secAgo, 0).UTC().Format(time.RFC3339) }
	names := make([]string, 0, 120)
	names = append(names, "events")
	for i := 0; i < 119; i++ {
		names = append(names, "shop.t"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	const htmlCell = `<img src=x onerror=alert(1)>`
	arg := c{
		"types": []string{"INTEGER", "BIGINT", "HUGEINT", "UBIGINT", "UTINYINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT",
			"DECIMAL(10,2)", "DECIMAL", "integer", "VARCHAR", "DATE", "TIMESTAMP", "BOOLEAN", "INTEGER[]", "STRUCT(a INTEGER)", "JSON", "BLOB", ""},
		"ago": []c{{"iso": iso(10), "now": now}, {"iso": iso(7 * 60), "now": now}, {"iso": iso(3 * 3600), "now": now},
			{"iso": iso(86400), "now": now}, {"iso": iso(3 * 86400), "now": now}, {"iso": "not a time", "now": now}},
		"status": []c{
			{"info": c{"copy_updated_at": iso(7 * 60), "limits": c{"timeout_seconds": 60, "max_rows": 1000}}, "now": now},
			{"info": c{"copy_updated_at": nil, "limits": c{"timeout_seconds": 45}}, "now": now},
			{"info": nil, "now": now},
		},
		"filter": []c{
			{"names": names, "filter": ""},
			{"names": names, "filter": "EVENTS"},
			{"names": names, "filter": "op.tb"},
			{"names": names, "filter": "nothing-matches"},
			{"names": []string{}, "filter": ""},
		},
		"starter": [][]string{{"events", "shop.orders", "shop.users"}, {"events"}, {}},
		"cells":   []any{nil, "NULL", "", "text", 12, 1.5, true, false, []any{1, 2}, c{"a": 1}, `{"cut": "js`},
		"count": []c{
			{"res": c{"rows": []any{}}, "ms": 3.4},
			{"res": c{"rows": []any{[]any{1}}}, "ms": 184.2},
			{"res": c{"rows": make([]any, 1000)}, "ms": 1234},
			{"res": c{"rows": []any{[]any{1}, []any{2}, []any{3}}}, "ms": 0.3},
		},
		"notes": []c{
			{"res": c{"rows": []any{[]any{1}}, "truncated": false, "truncated_cells": 0, "columns": []c{{"name": "id", "type": "BIGINT"}}}, "exact": true},
			{"res": c{"rows": make([]any, 1000), "truncated": true, "truncated_cells": 0}, "exact": true},
			{"res": c{"rows": []any{[]any{1}}, "truncated": false, "truncated_cells": 1}, "exact": true},
			{"res": c{"rows": make([]any, 1000), "truncated": true, "truncated_cells": 1200}, "exact": true},
			{"res": c{"rows": []any{[]any{1}}, "columns": []c{{"name": "id", "type": "BIGINT"}}}, "exact": false},
			{"res": c{"rows": []any{[]any{1}}, "columns": []c{{"name": "u", "type": "UBIGINT"}}}, "exact": false},
			{"res": c{"rows": []any{[]any{1}}, "columns": []c{{"name": "id", "type": "INTEGER"}, {"name": "h", "type": "HUGEINT"}, {"name": "s", "type": "VARCHAR"}}}, "exact": false},
		},
		// The messages are the SERVER's, by constant, so a reworded refusal
		// on the server cannot leave the page matching the old words.
		"errors": []c{
			{"status": 400, "message": "the \"sql\" field is empty"},
			{"status": 403, "message": "forbidden: your role lacks the sql:execute permission"},
			{"status": 403, "message": "SQL on the copy is unavailable while a data profile is active: the profile withholds tables"},
			{"status": 409, "message": sqlCopyNotLocalMessage},
			{"status": 409, "message": errNoViewSources.Error() + "; there is no copy to run SQL on yet"},
			{"status": 409, "message": "the copy defines no view to query: no snapshot was found to build state views from, and no archived partition"},
			{"status": 409, "message": "archive access is disabled for this server, so its copy cannot be read"},
			{"status": 422, "message": "Permission Error: Cannot access file \"/etc/passwd\""},
			{"status": 422, "message": sqlEventsInS3Message},
			{"status": 429, "message": sqlBusyWaitedText},
			{"status": 504, "message": "the query ran longer", "limits": c{"timeout_seconds": 60}},
			{"status": 504, "message": "the query ran longer"},
			{"status": 500, "message": "the SQL worker failed before it could answer; DBTrail's log has the details"},
			{"status": 502, "message": "list baselines: boom"},
			{"status": 0, "message": "Failed to fetch"},
			{"status": 418, "message": "teapot"},
		},
		"table": c{
			"columns": []c{{"name": "id", "type": "INTEGER"}, {"name": "zip", "type": "VARCHAR"}, {"name": "total", "type": "DECIMAL(10,2)"}, {"name": "<i>x</i>", "type": "VARCHAR[]"}},
			"rows":    []any{[]any{1, "02134", "10.50", []any{"a", "b"}}, []any{2, nil, nil, nil}, []any{3, "NULL", "", strings.Repeat("x", 400)}, []any{4, htmlCell, "1", "<script>window.__sqlx=1</script>"}},
		},
	}
	raw := runSQLPanelHarness(t, sqlPanelHarnessJS, arg)
	var out struct {
		ListMax int `json:"listMax"`
		Numeric map[string]bool
		Ago     []string
		Status  []string
		Filter  []struct {
			Shown   []string
			More    int
			Matched int
		}
		Starter []string
		Cells   []struct {
			Text   string
			IsNull bool `json:"isNull"`
		}
		Count  []string
		Notes  [][]string
		Errors []struct{ Text, Detail string }
		Table  struct {
			Caption []string
			Tags    []string
			Th      []struct{ Text, Cls, Title string }
			Td      []struct {
				Text  string
				Cls   string
				Nulls int
			}
		}
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}

	for typ, want := range map[string]bool{"INTEGER": true, "BIGINT": true, "HUGEINT": true, "UBIGINT": true, "UTINYINT": true,
		"SMALLINT": true, "TINYINT": true, "DOUBLE": true, "FLOAT": true, "DECIMAL(10,2)": true, "DECIMAL": true, "integer": true,
		"VARCHAR": false, "DATE": false, "TIMESTAMP": false, "BOOLEAN": false, "INTEGER[]": false, "STRUCT(a INTEGER)": false,
		"JSON": false, "BLOB": false, "": false} {
		if out.Numeric[typ] != want {
			t.Errorf("sqlColumnIsNumeric(%q) = %v, want %v", typ, out.Numeric[typ], want)
		}
	}
	if want := []string{"just now", "7 min ago", "3 h ago", "1 day ago", "3 days ago", ""}; !reflect.DeepEqual(out.Ago, want) {
		t.Errorf("sqlAgo = %q, want %q", out.Ago, want)
	}
	wantStatus := []string{
		"runs on the copy updated 7 min ago · read-only · 60 s limit",
		"runs on DBTrail's copy · read-only · 45 s limit",
		"runs on DBTrail's copy · read-only",
	}
	if !reflect.DeepEqual(out.Status, wantStatus) {
		t.Errorf("sqlStatusLine = %q, want %q", out.Status, wantStatus)
	}
	// The cap is read from the code, not repeated here as an argument.
	if out.ListMax != 50 {
		t.Fatalf("SQL_LIST_MAX = %d, want 50", out.ListMax)
	}
	if f := out.Filter[0]; len(f.Shown) != 50 || f.More != 70 || f.Matched != 120 || f.Shown[0] != "events" {
		t.Errorf("no filter: shown=%d more=%d matched=%d first=%q", len(f.Shown), f.More, f.Matched, f.Shown[0])
	}
	if f := out.Filter[1]; len(f.Shown) != 1 || f.Shown[0] != "events" || f.More != 0 {
		t.Errorf("filter EVENTS (any case): %+v", f)
	}
	// A filter that matches in the MIDDLE of a name: exactly the five
	// shop.tb* tables.
	if f := out.Filter[2]; f.Matched != 5 || len(f.Shown) != 5 || f.More != 0 {
		t.Errorf("filter op.tb: %+v, want exactly 5 matches", f)
	}
	if f := out.Filter[3]; len(f.Shown) != 0 || f.Matched != 0 || f.More != 0 {
		t.Errorf("filter with no match: %+v", f)
	}
	if f := out.Filter[4]; len(f.Shown) != 0 || f.More != 0 {
		t.Errorf("no names: %+v", f)
	}
	if want := []string{"SELECT * FROM shop.orders LIMIT 100", "SELECT * FROM events LIMIT 100", ""}; !reflect.DeepEqual(out.Starter, want) {
		t.Errorf("sqlStarterQuery = %q, want %q", out.Starter, want)
	}
	wantCells := []struct {
		Text   string
		IsNull bool
	}{{"NULL", true}, {"NULL", false}, {"", false}, {"text", false}, {"12", false}, {"1.5", false}, {"true", false}, {"false", false},
		{"[1,2]", false}, {`{"a":1}`, false}, {`{"cut": "js`, false}}
	for i, w := range wantCells {
		if out.Cells[i].Text != w.Text || out.Cells[i].IsNull != w.IsNull {
			t.Errorf("sqlCell #%d = %+v, want %+v", i, out.Cells[i], w)
		}
	}
	// The time is the round trip measured in the page; a fast one never
	// reads "0 ms".
	if want := []string{"0 rows in 3 ms", "1 row in 184 ms", "1,000 rows in 1.2 s", "3 rows in under 1 ms"}; !reflect.DeepEqual(out.Count, want) {
		t.Errorf("sqlCountLine = %q, want %q", out.Count, want)
	}
	const rounded = "Large whole numbers may be rounded on this browser. Download CSV has every digit."
	if len(out.Notes[0]) != 0 {
		t.Errorf("a whole result on a browser that keeps digits carries a notice: %q", out.Notes[0])
	}
	if len(out.Notes[1]) != 1 || !strings.HasPrefix(out.Notes[1][0], "Showing the first 1,000 rows.") {
		t.Errorf("truncated notice = %q", out.Notes[1])
	}
	if len(out.Notes[2]) != 1 || out.Notes[2][0] != "1 long value was cut." {
		t.Errorf("one cut cell = %q", out.Notes[2])
	}
	if len(out.Notes[3]) != 2 || out.Notes[3][1] != "1,200 long values were cut." {
		t.Errorf("both notices = %q", out.Notes[3])
	}
	// A browser that rounds says so, for BIGINT and UBIGINT columns and for
	// nothing else (HUGEINT arrives as text; INTEGER fits).
	if len(out.Notes[4]) != 1 || out.Notes[4][0] != rounded || len(out.Notes[5]) != 1 || out.Notes[5][0] != rounded {
		t.Errorf("rounding notice for BIGINT/UBIGINT = %q / %q", out.Notes[4], out.Notes[5])
	}
	if len(out.Notes[6]) != 0 {
		t.Errorf("rounding notice without a BIGINT column: %q", out.Notes[6])
	}
	wantErr := []struct{ text, detail string }{
		{"The request was not understood.", `the "sql" field is empty`},
		{"Your session is not allowed to run SQL.", ""},
		{"SQL is off while a data profile is active. The profile filters what the web interface shows, and SQL reads the raw files, which it cannot filter.", ""},
		{"The copy for this server is only on S3. SQL in the web interface needs a local copy.", ""},
		{"There is no copy to run SQL on yet.", ""},
		{"There is no copy to run SQL on yet.", ""},
		{"This copy cannot be queried from the web interface.", "archive access is disabled for this server, so its copy cannot be read"},
		{"The query did not run.", `Permission Error: Cannot access file "/etc/passwd"`},
		{"The query did not run.", sqlEventsInS3Message},
		{"SQL on the copy is busy. Try again in a moment.", sqlBusyWaitedText},
		{"The query ran longer than the 60 s limit and was stopped. Narrow it: a WHERE on a table, or a smaller window on events.", ""},
		{"The query ran longer than the time limit and was stopped. Narrow it: a WHERE on a table, or a smaller window on events.", ""},
		{"The query could not be run. DBTrail's log has the details.", ""},
		{"The copy could not be read.", "list baselines: boom"},
		{"The server did not answer.", "Failed to fetch"},
		{"The query failed.", "teapot"},
	}
	for i, w := range wantErr {
		if out.Errors[i].Text != w.text || out.Errors[i].Detail != w.detail {
			t.Errorf("sqlErrorView #%d = %+v, want %+v", i, out.Errors[i], w)
		}
	}

	// The painted table: a caption, numbers to the right by column type
	// (the digits-only zip stays left), NULL as its own token and only
	// there, nested values as JSON text, a long value clipped, and markup
	// in a name or a value painted as the characters it is: no element of
	// its kind exists in the tree, and nothing went through innerHTML (the
	// fake element throws on it).
	if len(out.Table.Caption) != 1 || out.Table.Caption[0] != "Query result, 4 rows in 5 ms" {
		t.Errorf("caption = %q", out.Table.Caption)
	}
	if want := []string{"caption", "div", "span", "table", "tbody", "td", "th", "thead", "tr"}; !reflect.DeepEqual(out.Table.Tags, want) {
		t.Errorf("element kinds in the painted result = %v, want %v (no img, i or script)", out.Table.Tags, want)
	}
	wantTh := []struct{ text, cls, title string }{{"id", "num", "INTEGER"}, {"zip", "", "VARCHAR"}, {"total", "num", "DECIMAL(10,2)"}, {"<i>x</i>", "", "VARCHAR[]"}}
	for i, w := range wantTh {
		if got := out.Table.Th[i]; got.Text != w.text || got.Cls != w.cls || got.Title != w.title {
			t.Errorf("th #%d = %+v, want %+v", i, got, w)
		}
	}
	td := out.Table.Td
	if len(td) != 16 {
		t.Fatalf("td count = %d, want 16", len(td))
	}
	if td[0].Text != "1" || td[0].Cls != "num" || td[1].Text != "02134" || td[1].Cls != "" || td[2].Text != "10.50" || td[2].Cls != "num" || td[3].Text != `["a","b"]` {
		t.Errorf("row 1 = %+v", td[0:4])
	}
	if td[5].Nulls != 1 || td[6].Nulls != 1 || td[7].Nulls != 1 || td[5].Text != "NULL" {
		t.Errorf("row 2 NULL cells = %+v", td[4:8])
	}
	if td[9].Nulls != 0 || td[9].Text != "NULL" || td[10].Nulls != 0 || td[10].Text != "" {
		t.Errorf("row 3: the text NULL and the empty string must not be the NULL token: %+v", td[8:12])
	}
	if len([]rune(td[11].Text)) != 301 || !strings.HasSuffix(td[11].Text, "…") {
		t.Errorf("a long value is clipped to 300 characters and an ellipsis, got %d runes", len([]rune(td[11].Text)))
	}
	if td[13].Text != htmlCell || td[15].Text != "<script>window.__sqlx=1</script>" {
		t.Errorf("row 4: markup must be painted as its characters: %+v", td[12:16])
	}
}

// sqlBigIntHarnessJS parses a result carrying whole numbers past 2^53.
const sqlBigIntHarnessJS = `
const f = (name) => vm.runInContext(name, ctx);
process.stdout.write(JSON.stringify({
  exact: f("SQL_EXACT_INTS"),
  parsed: f("sqlParseResult")('{"rows":[[9223372036854775807,12,1.5,-9007199254740993,"7",9007199254740993]]}').rows[0].map((v) => typeof v + ":" + v),
}));
`

// Whole numbers past 2^53 keep every digit, and ordinary values are
// untouched. The engine has to hand the reviver the number's source text
// (node 22 and later do): on an older node this test SKIPS, out loud,
// instead of passing on a check it could not make. The real browser's
// behavior is pinned in the e2e (SELECT 9007199254740993).
func TestSQLPanelBigIntegers(t *testing.T) {
	raw := runSQLPanelHarness(t, sqlBigIntHarnessJS, map[string]any{})
	var out struct {
		Exact  bool
		Parsed []string
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	if out.Parsed[1] != "number:12" || out.Parsed[2] != "number:1.5" || out.Parsed[4] != "string:7" {
		t.Errorf("sqlParseResult changed ordinary values: %q", out.Parsed)
	}
	if !out.Exact {
		t.Skipf("this node does not expose the JSON reviver's source text (needs node 22 or later), so the exact-digits check cannot run here; parsed as %q", out.Parsed)
	}
	if out.Parsed[0] != "string:9223372036854775807" || out.Parsed[3] != "string:-9007199254740993" || out.Parsed[5] != "string:9007199254740993" {
		t.Errorf("sqlParseResult lost digits of a big integer: %q", out.Parsed)
	}
}

// The panel's copy is plain and its DOM is built from text: no em dash
// anywhere in the SQL block of app.js, no API that parses markup from a
// string, and the card keeps its sentence, tags and action as the mockup
// has them.
func TestSQLPanelCopy(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("assets", "app.js"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(js)
	start := strings.Index(src, "SQL on the copy (#1952)")
	end := strings.Index(src, "// USE_PANELS: what each card opens under the row.")
	if start < 0 || end < start {
		t.Fatal("the SQL panel block was not found in app.js")
	}
	block := src[start:end]
	if strings.Contains(block, "—") {
		t.Error("the SQL panel block carries an em dash")
	}
	// Every value a query returns is attacker-shaped text. The panel builds
	// its DOM with el() and textContent; none of these may appear in it.
	for _, api := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "svgEl(", "DOMParser", "document.write"} {
		if strings.Contains(block, api) {
			t.Errorf("the SQL panel block uses %s: result values must never be parsed as markup", api)
		}
	}
	for _, want := range []string{
		`{ id: "sql", title: "Ask it here", cap: "sql", action: "Open SQL", primary: true,`,
		`tags: [["now", true], ["always current", true], ["nothing to install", false]],`,
		`"Write SQL in this page. It runs on DBTrail's copy, not on MySQL."`,
	} {
		if !strings.Contains(src, want) {
			t.Errorf("the Ask it here card lost %q", want)
		}
	}
	if strings.Contains(src, "not part of this build yet") {
		t.Error("the stub sentence is still in app.js")
	}
}

// sqlBusyWaitedText is the real 429 message after the wait (#2033), so the
// card is checked against what the server sends, not a paraphrase of it.
var sqlBusyWaitedText = (&sqlsandbox.BusyError{Waited: 30 * time.Second, MaxInFlight: 2}).Error()
