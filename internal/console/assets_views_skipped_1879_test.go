package console

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1879: the Snapshots page says how many views a snapshot left out, and
// names them when its row is opened. These run the page's own functions in
// node, over the documents the real endpoints answer for real folders.

type viewsWords struct {
	Head  string   `json:"head"`
	Names []string `json:"names"`
	More  string   `json:"more"`
}

func viewsPageScript(t *testing.T) string {
	t.Helper()
	js := readAsset(t, "app.js")
	parts := []string{`
const made = (tag) => ({
  tag: tag, nodeType: 1, className: "", kids: [], said: "", dataset: {}, hidden: false,
  set textContent(v) { this.said = String(v); this.kids = []; },
  get textContent() { return this.said; },
  set innerHTML(v) { throw new Error("markup was parsed from a string: " + v); },
  setAttribute() {}, addEventListener() {},
  replaceChildren() { this.kids = []; this.said = ""; },
  append(...ks) { for (const k of ks) this.kids.push(k && k.nodeType ? k : { tag: "#text", nodeType: 3, said: String(k), kids: [] }); },
});
const document = { createElement: made, createTextNode: (s) => ({ tag: "#text", nodeType: 3, said: String(s), kids: [] }) };
const flat = (n) => n && ({ tag: n.tag, class: n.className || "", text: n.said || "", kids: (n.kids || []).map(flat) });
const humanBytes = (n) => n + " B";
const sessionMay = () => false;
function clear(n) { if (n) n.replaceChildren(); }
function backupWhyLine() { return ""; }
function downloadBackup() {}
`}
	for _, decl := range []string{"function el(", "function utcLabel(", "function fmtSeconds(", "function timesText(", "function fmtAge(",
		"const MADE_BY", "function madeByCell(", "function sourceReadLine(", "function snapshotLockKey(", "function snapshotLockPill(",
		"function snapshotLockLine(", "function tableLockMark(", "function newestCopyLine(", "function viewsSkippedCount(", "function viewsSkippedAsOf(",
		"function snapshotViewsText(", "function viewsSkippedWords(", "function viewsSkippedBlock(", "async function loadBackupDetail("} {
		parts = append(parts, functionBody(t, js, decl))
	}
	return strings.Join(parts, "\n")
}

func runViewsScript(t *testing.T, tail string, out any) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "views.js")
	if err := os.WriteFile(path, []byte(viewsPageScript(t)+"\n"+tail), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
}

// zeroViewsWords is every way of saying a count that is not one: a zero that
// is a number of its own ("300 views" is not one), and a value that was not
// a number drawn as if it were.
var zeroViewsWords = regexp.MustCompile(`(?i)\b0 views?\b|\bno views?\b|\bNaN\b|\bundefined\b|\bnull\b`)

// neverZeroViews fails on any way of saying a count of zero.
func neverZeroViews(t *testing.T, what, text string) {
	t.Helper()
	if bad := zeroViewsWords.FindString(text); bad != "" {
		t.Errorf("%s says %q: %q", what, bad, text)
	}
}

// The closing case of the issue, from the folder to the row: a source with
// two tables and one view.
func TestSnapshotRow_saysTheViewsOfTheRealListing(t *testing.T) {
	srv := newBaselineServer(t, newViewsFixture(t), true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got map[string]string
	runViewsScript(t, "const b = "+string(body)+";\n"+
		"console.log(JSON.stringify(Object.fromEntries(b.snapshots.map((sn) => [sn.time, snapshotViewsText(sn)]))));", &got)
	for at, want := range map[string]string{
		viewsFullAt: "2 tables, 1 view skipped",
		// Carried: the date of the read, never the present.
		viewsCarriedAt: "2 tables, 1 view skipped as of the full read of 2026-06-01 00:00:00 UTC",
		// Nothing recorded, or a record that cannot be read: no words.
		viewsOldAt: "",
		viewsBadAt: "",
	} {
		if text, ok := got[at]; !ok || text != want {
			t.Errorf("row %s:\n got %q\nwant %q", at, text, want)
		}
	}
}

func TestSnapshotViewsText_words(t *testing.T) {
	tables := func(n int) string {
		names := make([]string, n)
		for i := range names {
			names[i] = fmt.Sprintf("s.t%d", i)
		}
		raw, _ := json.Marshal(names)
		return string(raw)
	}
	cases := []struct{ name, doc, want string }{
		{"no key", `{"tables":` + tables(2) + `}`, ""},
		{"zero", `{"tables":` + tables(2) + `,"views_skipped":0}`, ""},
		{"negative", `{"tables":` + tables(2) + `,"views_skipped":-3}`, ""},
		{"null", `{"tables":` + tables(2) + `,"views_skipped":null}`, ""},
		{"a string", `{"tables":` + tables(2) + `,"views_skipped":"3"}`, ""},
		{"true", `{"tables":` + tables(2) + `,"views_skipped":true}`, ""},
		{"a fraction under one", `{"tables":` + tables(2) + `,"views_skipped":0.4}`, ""},
		{"carried and no count", `{"tables":` + tables(2) + `,"views_carried":true,"views_read_at":"2026-06-01 00:00:00"}`, ""},
		{"no document", `null`, ""},
		{"one table, one view", `{"tables":` + tables(1) + `,"views_skipped":1}`, "1 table, 1 view skipped"},
		{"hundreds", `{"tables":` + tables(40) + `,"views_skipped":312}`, "40 tables, 312 views skipped"},
		{"no tables listed", `{"views_skipped":2}`, "0 tables, 2 views skipped"},
		{"a date on a full read is not said", `{"tables":` + tables(2) + `,"views_skipped":1,"views_read_at":"2026-06-01 00:00:00"}`, "2 tables, 1 view skipped"},
		{"carried with no date", `{"tables":` + tables(2) + `,"views_skipped":2,"views_carried":true}`, "2 tables, 2 views skipped as of an earlier full read"},
		{"carried said as a string is not carried", `{"tables":` + tables(2) + `,"views_skipped":2,"views_carried":"false"}`, "2 tables, 2 views skipped"},
	}
	docs := make([]string, len(cases))
	for i, c := range cases {
		docs[i] = c.doc
	}
	var got []string
	runViewsScript(t, "console.log(JSON.stringify(["+strings.Join(docs, ",")+"].map(snapshotViewsText)));", &got)
	if len(got) != len(cases) {
		t.Fatalf("%d answers for %d documents", len(got), len(cases))
	}
	for i, c := range cases {
		if got[i] != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.name, got[i], c.want)
		}
		if c.name != "no tables listed" {
			neverZeroViews(t, c.name, strings.ReplaceAll(got[i], "0 tables", ""))
		}
	}
}

func detailDoc(t *testing.T, srv *Server, at string) string {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(at), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	return string(body)
}

func TestViewsSkippedWords_ofTheRealDetail(t *testing.T) {
	root := newViewsFixture(t)
	const manyDir, manyAt = "2026-06-03T00-00-00Z", "2026-06-03 00:00:00"
	const runDir, runAt = "2026-06-04T00-00-00Z", "2026-06-04 00:00:00"
	writeBaselineFixture(t, root, manyDir, "shop", "orders.parquet")
	writeBaselineFixture(t, root, runDir, "shop", "orders.parquet")
	var many []string
	for i := range 300 {
		many = append(many, fmt.Sprintf("reports.v%03d", i))
	}
	writeViewsRecord(t, root, manyDir, false, time.Date(2026, 6, 3, 0, 0, 0, 0, time.UTC), many...)
	srv := newBaselineServer(t, root, true)
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv.baselineHistory = h
	if err := h.Append(BaselineRunRecord{ServerID: bootServerID, Kind: BaselineRunDump, SnapshotTime: "2026-06-04T00:00:00Z",
		StartedAt: "2026-06-04T00:00:00Z", FinishedAt: "2026-06-04T00:00:09Z", Tables: 1, ViewsSkipped: 2}); err != nil {
		t.Fatal(err)
	}

	order := []string{viewsFullAt, viewsCarriedAt, viewsOldAt, viewsBadAt, manyAt, runAt}
	docs := make([]string, len(order))
	for i, at := range order {
		docs[i] = detailDoc(t, srv, at)
	}
	var got []*viewsWords
	runViewsScript(t, "console.log(JSON.stringify(["+strings.Join(docs, ",")+"].map((d) => viewsSkippedWords(d.views_skipped))));", &got)
	if len(got) != len(order) {
		t.Fatalf("%d answers for %d documents", len(got), len(order))
	}
	const carriedTail = " This snapshot was updated from the recorded changes and did not read the views again: a view created or dropped since is not shown."
	want := []*viewsWords{
		{Head: "1 view skipped. A view holds no rows to copy.", Names: []string{"shop.big_orders"}},
		{Head: "1 view skipped as of the full read of 2026-06-01 00:00:00 UTC. A view holds no rows to copy." + carriedTail,
			Names: []string{"shop.big_orders"}},
		nil,
		nil,
		{Head: "300 views skipped. A view holds no rows to copy.", Names: many[:20],
			More: "280 more not listed. The log of the full read names every view."},
		{Head: "2 views skipped. A view holds no rows to copy.", Names: []string{},
			More: "The log of the full read names each one."},
	}
	for i, at := range order {
		switch {
		case want[i] == nil && got[i] != nil:
			t.Errorf("snapshot %s has nothing recorded and the page says %+v", at, got[i])
		case want[i] == nil:
		case got[i] == nil:
			t.Errorf("snapshot %s: the page says nothing, want %+v", at, want[i])
		case got[i].Head != want[i].Head || got[i].More != want[i].More || !slices.Equal(got[i].Names, want[i].Names):
			t.Errorf("snapshot %s:\n got %+v\nwant %+v", at, got[i], want[i])
		}
		if got[i] != nil {
			neverZeroViews(t, at, got[i].Head+" "+got[i].More)
		}
	}
}

func TestViewsSkippedWords_edges(t *testing.T) {
	cases := []struct {
		name, doc string
		want      *viewsWords
	}{
		{"nothing", `undefined`, nil},
		{"null", `null`, nil},
		{"zero", `{"count":0,"names":["a.v"]}`, nil},
		{"names and no count", `{"names":["a.v"]}`, nil},
		{"a count that is a string", `{"count":"2","names":["a.v"]}`, nil},
		{"names that are not a list", `{"count":2,"names":"a.v"}`,
			&viewsWords{Head: "2 views skipped. A view holds no rows to copy.", Names: []string{}, More: "The log of the full read names each one."}},
		{"empty names are not drawn", `{"count":2,"names":["", "  ", null, "a.v"]}`,
			&viewsWords{Head: "2 views skipped. A view holds no rows to copy.", Names: []string{"a.v"}, More: "1 more not listed. The log of the full read names every view."}},
		{"more names than the page lists", `{"count":25,"names":` + numberedNames(25) + `}`,
			&viewsWords{Head: "25 views skipped. A view holds no rows to copy.", Names: numbered(20), More: "5 more not listed. The log of the full read names every view."}},
		{"one view, named in the log only", `{"count":1}`,
			&viewsWords{Head: "1 view skipped. A view holds no rows to copy.", Names: []string{}, More: "The log of the full read names it."}},
		{"carried with no date", `{"count":1,"names":["a.v"],"carried":true}`,
			&viewsWords{Head: "1 view skipped as of an earlier full read. A view holds no rows to copy. This snapshot was updated from the recorded changes and did not read the views again: a view created or dropped since is not shown.", Names: []string{"a.v"}}},
	}
	docs := make([]string, len(cases))
	for i, c := range cases {
		docs[i] = c.doc
	}
	var got []*viewsWords
	runViewsScript(t, "console.log(JSON.stringify(["+strings.Join(docs, ",")+"].map((d) => viewsSkippedWords(d))));", &got)
	if len(got) != len(cases) {
		t.Fatalf("%d answers for %d documents", len(got), len(cases))
	}
	for i, c := range cases {
		switch {
		case c.want == nil && got[i] != nil:
			t.Errorf("%s: the page says %+v, want nothing", c.name, got[i])
		case c.want == nil:
		case got[i] == nil:
			t.Errorf("%s: the page says nothing, want %+v", c.name, c.want)
		case got[i].Head != c.want.Head || got[i].More != c.want.More || !slices.Equal(got[i].Names, c.want.Names):
			t.Errorf("%s:\n got %+v\nwant %+v", c.name, got[i], c.want)
		}
	}
}

func numbered(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("a.v%02d", i)
	}
	return out
}

func numberedNames(n int) string {
	raw, _ := json.Marshal(numbered(n))
	return string(raw)
}

// A name comes from a server. It is cut to a length and drawn as text: a
// name that is markup stays a name.
func TestViewsSkippedBlock_drawsTextNeverMarkup(t *testing.T) {
	const hostile = `shop.<img src=x onerror=alert(1)>`
	root := t.TempDir()
	writeBaselineFixture(t, root, viewsFullDir, "shop", "orders.parquet")
	if err := baseline.WriteViewsSkipped(filepath.Join(root, viewsFullDir), baseline.ViewsSkipped{
		ReadAt: "2026-06-01T00:00:00Z", Count: 4,
		Views: []string{hostile, "shop.`q` 'q' \"q\"", "</code><script>alert(2)</script>", strings.Repeat("\U0001F600", 140)},
	}); err != nil {
		t.Fatal(err)
	}
	srv := newBaselineServer(t, root, true)
	var got []*drawnNode
	runViewsScript(t, "const d = "+detailDoc(t, srv, viewsFullAt)+";\n"+
		"console.log(JSON.stringify([flat(viewsSkippedBlock(d.views_skipped)), flat(viewsSkippedBlock(undefined)), flat(viewsSkippedBlock({count: 0}))]));", &got)
	if len(got) != 3 || got[0] == nil {
		t.Fatalf("drawn = %+v", got)
	}
	if got[1] != nil || got[2] != nil {
		t.Errorf("a snapshot with nothing recorded drew a block: %+v %+v", got[1], got[2])
	}
	tags := map[string]int{}
	var names []string
	got[0].walk(func(n drawnNode) {
		tags[n.Tag]++
		if n.Tag == "code" {
			names = append(names, n.Text)
		}
	})
	for tag := range tags {
		switch tag {
		case "div", "p", "ul", "li", "code":
		default:
			t.Errorf("the block built a <%s>: a value was read as markup", tag)
		}
	}
	slices.Sort(names)
	want := []string{hostile, "shop.`q` 'q' \"q\"", "</code><script>alert(2)</script>", strings.Repeat("\U0001F600", 127) + "..."}
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Errorf("names drawn:\n got %q\nwant %q", names, want)
	}
	js := readAsset(t, "app.js")
	for _, fn := range []string{"viewsSkippedWords", "viewsSkippedBlock", "snapshotViewsText"} {
		span := jsFunctionSpan(t, js, fn)
		for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "svgEl(", "DOMParser"} {
			if strings.Contains(span, sink) {
				t.Errorf("%s uses %s", fn, sink)
			}
		}
	}
}

// The opened row draws the names the detail answered: the page's own loader,
// over the real endpoint's document.
func TestLoadBackupDetail_drawsTheSkippedViews(t *testing.T) {
	srv := newBaselineServer(t, newViewsFixture(t), true)
	var got []string
	runViewsScript(t, "const docs = {"+
		fmt.Sprintf("%q: %s, %q: %s, %q: %s", viewsFullAt, detailDoc(t, srv, viewsFullAt),
			viewsCarriedAt, detailDoc(t, srv, viewsCarriedAt), viewsOldAt, detailDoc(t, srv, viewsOldAt))+"};\n"+
		"let asked = [];\n"+
		"async function api(path) { asked.push(path); return docs[decodeURIComponent(path.split('at=')[1])]; }\n"+
		"const text = (n) => [n.said].concat((n.kids || []).map(text)).filter((s) => s).join('\\n');\n"+
		"(async () => { const out = [];\n"+
		"  for (const at of Object.keys(docs)) { const box = made('div'); await loadBackupDetail(at, box); out.push(text(box)); }\n"+
		"  console.log(JSON.stringify(out)); })();", &got)
	if len(got) != 3 {
		t.Fatalf("drawn = %q", got)
	}
	if !strings.Contains(got[0], "1 view skipped. A view holds no rows to copy.") || !strings.Contains(got[0], "\nshop.big_orders\n") {
		t.Errorf("the full read's row, opened:\n%s", got[0])
	}
	if !strings.Contains(got[1], "as of the full read of 2026-06-01 00:00:00 UTC") || !strings.Contains(got[1], "did not read the views again") {
		t.Errorf("the carried row, opened, does not say of when the list is:\n%s", got[1])
	}
	if low := strings.ToLower(got[2]); strings.Contains(low, "view") {
		t.Errorf("a snapshot with nothing recorded, opened, speaks of views:\n%s", got[2])
	}
}

// Where the count is mounted: on the snapshot's row, from that row's own
// document.
func TestSnapshotViewsText_isMountedOnTheRow(t *testing.T) {
	js := readAsset(t, "app.js")
	panel := functionBody(t, js, "function baselinesPanel(")
	for _, want := range []string{
		"const skippedViews = snapshotViewsText(sn);",
		`text: skippedViews || (sn.tables || []).length + " table(s)"`,
	} {
		if strings.Count(panel, want) != 1 {
			t.Errorf("the snapshot list holds %q %d times, want once", want, strings.Count(panel, want))
		}
	}
	if n := strings.Count(functionBody(t, js, "async function loadBackupDetail("), "viewsSkippedBlock(d.views_skipped)"); n != 1 {
		t.Errorf("the opened row mounts the names %d times, want once", n)
	}
}
