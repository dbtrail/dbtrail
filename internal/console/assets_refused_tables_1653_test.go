package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// #1653: the page says which table stopped an update, and why. These run the
// page's own functions in node, over the documents the server sends.

type refusedRow struct {
	Name   string `json:"name"`
	What   string `json:"what"`
	Fix    string `json:"fix"`
	Reason string `json:"reason"`
}

type refusedWords struct {
	Head string       `json:"head"`
	Rows []refusedRow `json:"rows"`
	More string       `json:"more"`
}

type drawnNode struct {
	Tag   string      `json:"tag"`
	Class string      `json:"class"`
	Text  string      `json:"text"`
	Kids  []drawnNode `json:"kids"`
}

func (n drawnNode) walk(f func(drawnNode)) {
	f(n)
	for _, k := range n.Kids {
		k.walk(f)
	}
}

// allText is every piece of text under n, in order, one per line.
func (n drawnNode) allText() string {
	var out []string
	n.walk(func(k drawnNode) {
		if k.Text != "" {
			out = append(out, k.Text)
		}
	})
	return strings.Join(out, "\n")
}

// refusedPageScript is the page's own code for this feature, with a document
// that records what is built on it and refuses markup.
func refusedPageScript(t *testing.T) string {
	t.Helper()
	js := readAsset(t, "app.js")
	parts := []string{`
const made = (tag) => ({
  tag: tag, nodeType: 1, className: "", kids: [], said: "",
  set textContent(v) { this.said = String(v); this.kids = []; },
  get textContent() { return this.said; },
  set innerHTML(v) { throw new Error("markup was parsed from a string: " + v); },
  setAttribute() {}, addEventListener() {},
  append(...ks) { for (const k of ks) this.kids.push(k && k.nodeType ? k : { tag: "#text", nodeType: 3, said: String(k), kids: [] }); },
});
const document = { createElement: made, createTextNode: (s) => ({ tag: "#text", nodeType: 3, said: String(s), kids: [] }) };
const capsCache = { baseline_trigger: true };
function utcLabel(s) { return s ? s.replace("T", " ").replace("Z", " UTC") : ""; }
const flat = (n) => n && ({ tag: n.tag, class: n.className || "", text: n.said || "", kids: (n.kids || []).map(flat) });
`}
	for _, decl := range []string{"function el(", "function plainWords(", "function backupFoldError(", "function reusedCopiedNote(",
		"function budgetRefusedTail(", "const REFUSED_TABLE_WORDS", "function refusedTableRows(", "function refusedTablesBlock(",
		"function baselineRefreshNote("} {
		parts = append(parts, functionBody(t, js, decl))
	}
	return strings.Join(parts, "\n")
}

func runRefusedScript(t *testing.T, tail string, out any) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "refused.js")
	if err := os.WriteFile(path, []byte(refusedPageScript(t)+"\n"+tail), 0o644); err != nil {
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

func wordsFor(t *testing.T, docs []string) []*refusedWords {
	t.Helper()
	var got []*refusedWords
	runRefusedScript(t, "console.log(JSON.stringify(["+strings.Join(docs, ",")+"].map((d) => refusedTableRows(d[0], d[1]))));", &got)
	if len(got) != len(docs) {
		t.Fatalf("%d answers for %d documents", len(got), len(docs))
	}
	return got
}

// The documents are marshalled from the server's own types, so the names the
// page reads are the ones on the wire.
func statusDoc(t *testing.T, st BaselineStatus) string {
	t.Helper()
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	return "[" + string(raw) + "]"
}

func TestRefusedTableRows_words(t *testing.T) {
	ddl := RefusedTable{"shop.orders", "refused-ddl", "shop.orders: schema changed since the baseline"}
	// The two longest are the engine's own sentences, as it writes them.
	gap := RefusedTable{"crm.orders", "refused-gap", "reconstruct: events were lost at 2026-09-01T10:02:00Z for crm.orders; pass --allow-gaps to proceed with a known-incomplete reconstruction: capture gap in the reconstruction window"}
	shape := RefusedTable{"shop.items", "refused-ddl", "shop.items changed shape since its baseline was taken (added since: sku; gone since: none; type changed since: none) \u2014 " +
		"a snapshot emitted from it would carry the OLD CREATE TABLE forward and project every row onto the old columns and types, " +
		"so every reconstruct anchored on it would be wrong. Take a real snapshot instead: `bintrail dump` + `bintrail baseline`. " +
		"(If the schema snapshot is what is stale, run `bintrail snapshot` first and retry.): schema changed since the baseline"}
	other := RefusedTable{"shop.big", "refused", ""}
	// No fix of its own, so the engine's remedy after the dash is kept.
	fetch := RefusedTable{"shop.logs", "refused", "fetch events: an archive file is missing \u2014 take a full read to replace this snapshot"}
	later := RefusedTable{"shop.Quota.v2", "refused-quota", "over the quota"}
	odd := RefusedTable{"", "", "x"}

	runDoc, err := json.Marshal(backupScheduleRunDTO{Method: "refresh", OK: false, Tables: 3, Refused: 1, RefusedTables: []RefusedTable{ddl}})
	if err != nil {
		t.Fatal(err)
	}
	fbDoc, err := json.Marshal(backupScheduleSkipDTO{At: "2026-09-01T10:00:00Z", Reason: "r", Tables: 12, Refused: 2, RefusedTables: []RefusedTable{ddl, gap}})
	if err != nil {
		t.Fatal(err)
	}
	docs := []string{
		/* 0 */ statusDoc(t, BaselineStatus{State: "failed", Tables: 12, Refused: 1, RefusedTables: []RefusedTable{ddl}}),
		/* 1 */ statusDoc(t, BaselineStatus{State: "failed", Tables: 12, Refused: 7, RefusedTables: []RefusedTable{ddl, gap, other, later, odd, shape, fetch}}),
		/* 2 */ statusDoc(t, BaselineStatus{State: "failed", Tables: 2, Refused: 2, RefusedTables: []RefusedTable{ddl, gap}}),
		/* 3 */ statusDoc(t, BaselineStatus{State: "failed", Tables: 1, Refused: 1, RefusedTables: []RefusedTable{ddl}}),
		/* 4 */ statusDoc(t, BaselineStatus{State: "failed", Tables: 6000, Refused: 5000, RefusedTables: []RefusedTable{ddl, gap}, RefusedTablesOmitted: 4998}),
		// A run from before the list: the count and no list.
		/* 5 */ statusDoc(t, BaselineStatus{State: "failed", Tables: 12, Refused: 3, LastError: "x"}),
		// No table refused: the run failed before reading any.
		/* 6 */ statusDoc(t, BaselineStatus{State: "failed", LastError: "the bucket did not answer"}),
		/* 7 */ statusDoc(t, BaselineStatus{State: "running", Since: "2026-09-01T10:00:00Z", RefusedTables: []RefusedTable{ddl}}),
		/* 8 */ statusDoc(t, BaselineStatus{State: "succeeded", Tables: 12, RefusedTables: []RefusedTable{ddl}}),
		/* 9 */ "[" + string(runDoc) + "]",
		/* 10 */ "[" + string(fbDoc) + `, "A full read was started instead."]`,
		// Counts that contradict each other: the rows are believed.
		/* 11 */ `[{"state":"failed","tables":2,"refused":1,"refused_tables":[{"name":"a.b","verdict":"refused"},{"name":"a.c","verdict":"refused"},{"name":"a.d","verdict":"refused"}]}]`,
		/* 12 */ "[null]", "[undefined]", `[{"state":"failed","refused_tables":"shop.orders"}]`, `[{"state":"failed","refused_tables":[null, 7, "x"]}]`,
	}
	got := wordsFor(t, docs)

	for i, want := range map[int]string{
		0:  "1 table stopped the update of all 12. Nothing was published.",
		1:  "7 tables stopped the update of all 12. Nothing was published.",
		2:  "All 2 tables stopped the update. Nothing was published.",
		3:  "The only table stopped the update. Nothing was published.",
		4:  "5000 tables stopped the update of all 6000. Nothing was published.",
		9:  "1 table stopped the update of all 3. Nothing was published.",
		10: "2 tables stopped the update of all 12. A full read was started instead.",
		11: "3 tables stopped the update. Nothing was published.",
		// Entries that are not tables are rows with no name, not a hidden list.
		15: "3 tables stopped the update. Nothing was published.",
	} {
		if got[i] == nil {
			t.Errorf("document %d drew nothing, want %q", i, want)
			continue
		}
		if got[i].Head != want {
			t.Errorf("document %d head = %q, want %q", i, got[i].Head, want)
		}
	}
	for _, i := range []int{5, 6, 7, 8, 12, 13, 14} {
		if got[i] != nil {
			t.Errorf("document %d (%s) drew a list: %+v", i, docs[i], got[i])
		}
	}
	if t.Failed() {
		return
	}

	wantRows := []refusedRow{
		{"shop.orders", "schema changed", "Needs a new read of this table.", "shop.orders: schema changed since the baseline."},
		// The reason is said in the page's words: no flag a browser cannot type.
		{"crm.orders", "changes missing", "Needs a full snapshot.", "events were lost at 2026-09-01T10:02:00Z for crm.orders."},
		{"shop.big", "refused", "", ""},
		// A verdict this page does not know: the name and the verdict as written.
		{"shop.Quota.v2", "refused-quota", "", "over the quota."},
		{"(no name)", "refused", "", "x."},
		{"shop.items", "schema changed", "Needs a new read of this table.",
			"shop.items changed shape since its baseline was taken (added since: sku; gone since: none; type changed since: none)."},
		{"shop.logs", "refused", "", "fetch events: an archive file is missing - take a full read to replace this snapshot."},
	}
	for _, r := range got[1].Rows {
		if r.Fix == "" {
			continue
		}
		if strings.Contains(r.Reason, "--") || strings.Contains(r.Reason, "`") {
			t.Errorf("a reason names a command or a flag the page cannot run: %q", r.Reason)
		}
	}
	if len(got[1].Rows) != len(wantRows) {
		t.Fatalf("rows = %+v", got[1].Rows)
	}
	for i, want := range wantRows {
		if got[1].Rows[i] != want {
			t.Errorf("row %d = %+v\nwant     %+v", i, got[1].Rows[i], want)
		}
	}
	if got[1].More != "" || got[0].More != "" {
		t.Errorf("a whole list says more are missing: %q", got[1].More)
	}
	if got[4].More != "4998 more not listed." {
		t.Errorf("more = %q", got[4].More)
	}
	for i, w := range got {
		if w == nil {
			continue
		}
		for _, s := range append([]string{w.Head, w.More}, flatten(w.Rows)...) {
			if strings.ContainsAny(s, "\u2014\u2013") {
				t.Errorf("document %d says a dash: %q", i, s)
			}
			if oldVocabulary.MatchString(strings.ReplaceAll(strings.ReplaceAll(s, "schema changed since the baseline", ""), "since its baseline was taken", "")) || strings.Contains(strings.ToLower(s), "console") {
				t.Errorf("document %d uses a retired word: %q", i, s)
			}
		}
	}
}

func flatten(rows []refusedRow) []string {
	var out []string
	for _, r := range rows {
		out = append(out, r.Name, r.What, r.Fix, r.Reason)
	}
	return out
}

// Names and reasons come from a server. They are cut to a length and drawn
// as text: a name that is markup stays a name.
func TestRefusedTablesBlock_drawsTextNeverMarkup(t *testing.T) {
	hostile := `shop.<img src=x onerror=alert(1)>`
	st := BaselineStatus{State: "failed", Tables: 9, Refused: 3, RefusedTables: []RefusedTable{
		{hostile, "refused-ddl", "<script>alert(2)</script> first\nsecond"},
		{strings.Repeat("n", 190), `"><b>x</b>`, strings.Repeat("long reason ", 40)},
		{"shop.`q` 'q' \"q\"", "refused-gap", ""},
		// Cut in the middle of characters that take two code units each.
		{strings.Repeat("\U0001F600", 140), "refused", ""},
	}}
	var got []*drawnNode
	runRefusedScript(t, "console.log(JSON.stringify(["+statusDoc(t, st)+", "+statusDoc(t, BaselineStatus{State: "failed", Refused: 3})+
		"].map((d) => flat(refusedTablesBlock(d[0])))));", &got)
	if len(got) != 2 || got[0] == nil {
		t.Fatalf("drawn = %+v", got)
	}
	if got[1] != nil {
		t.Errorf("a run with no list drew a block: %+v", got[1])
	}
	tags := map[string]int{}
	got[0].walk(func(n drawnNode) { tags[n.Tag]++ })
	for tag := range tags {
		switch tag {
		case "div", "p", "ul", "li", "code", "span":
		default:
			t.Errorf("the block built a <%s>: a value was read as markup", tag)
		}
	}
	if tags["li"] != 4 {
		t.Errorf("%d rows for 4 tables", tags["li"])
	}
	var names, whys []string
	got[0].walk(func(n drawnNode) {
		switch {
		case n.Tag == "code":
			names = append(names, n.Text)
		case n.Class == "refused-why":
			whys = append(whys, n.Text)
		}
	})
	if len(names) != 4 || names[0] != hostile || names[2] != "shop.`q` 'q' \"q\"" {
		t.Errorf("names = %q", names)
	}
	if len(names) == 4 && names[3] != strings.Repeat("\U0001F600", 127)+"..." {
		t.Errorf("a name was cut inside a character: %q", names[3])
	}
	if len(names) == 4 && (len(names[1]) != 130 || !strings.HasSuffix(names[1], "...")) {
		t.Errorf("a 190 character name was drawn at %d: %q", len(names[1]), names[1])
	}
	if len(whys) != 2 || whys[0] != "<script>alert(2)</script> first second." {
		t.Errorf("reasons = %q", whys)
	}
	// 300, not 200: this row has no fix of its own, so it keeps more.
	if len(whys) == 2 && (len(whys[1]) != 300 || !strings.HasSuffix(whys[1], "...")) {
		t.Errorf("a long reason was drawn at %d: %q", len(whys[1]), whys[1])
	}
	text := got[0].allText()
	if !strings.Contains(text, `"><b>x</b>`) {
		t.Errorf("the verdict this page does not know was hidden:\n%s", text)
	}
	js := readAsset(t, "app.js")
	for _, fn := range []string{"refusedTableRows", "refusedTablesBlock"} {
		span := jsFunctionSpan(t, js, fn)
		for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "svgEl(", "DOMParser"} {
			if strings.Contains(span, sink) {
				t.Errorf("%s uses %s", fn, sink)
			}
		}
	}
}

// The line above the rows. With a list, the rows say which tables and why,
// so the line does not repeat the joined error. Without one (a run from
// before the list, a status whose names are withheld) it says the count, as
// it always has, and never "0" or nothing.
func TestBaselineRefreshNote_withAndWithoutTheList(t *testing.T) {
	listed := BaselineStatus{State: "failed", FinishedAt: "2026-09-01T10:00:05Z", Tables: 12, Refused: 1,
		LastError:     "shop.orders: schema changed since the baseline",
		RefusedTables: []RefusedTable{{"shop.orders", "refused-ddl", "shop.orders: schema changed since the baseline"}}}
	old := listed
	old.Refused, old.RefusedTables = 3, nil
	var got []drawnNode
	runRefusedScript(t, "console.log(JSON.stringify(["+statusDoc(t, listed)+","+statusDoc(t, old)+"].map((d) => flat(baselineRefreshNote(d[0])))));", &got)
	if want := "Automatic refresh published nothing at 2026-09-01 10:00:05 UTC. Nothing was overwritten; the next run retries."; got[0].Text != want {
		t.Errorf("with a list:\n got %q\nwant %q", got[0].Text, want)
	}
	if want := "Automatic refresh published nothing at 2026-09-01 10:00:05 UTC; 3 table(s) refused: shop.orders: schema changed since the baseline. Nothing was overwritten; the next run retries."; got[1].Text != want {
		t.Errorf("without a list:\n got %q\nwant %q", got[1].Text, want)
	}
}

// Where the rows are mounted: beside the refresh note, under a failed
// scheduled run, and under the fallback's alarm. Each draws ITS document.
func TestRefusedTablesBlock_isMountedWhereARunIsReported(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, want := range []string{"refusedTablesBlock(b.refresh)", "refusedTablesBlock(run)", `refusedTablesBlock(fb, "A full read was started instead.")`} {
		if strings.Count(js, want) != 1 {
			t.Errorf("app.js mounts %s %d times, want once", want, strings.Count(js, want))
		}
	}
}
