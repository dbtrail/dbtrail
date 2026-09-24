package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The second slice of #1860, driven through the real functions in node: the
// grey activity line over the index's own window (a zero said in words), the
// folds remembered per browser, and the copy's age carrying its stamp so it
// can keep moving between two reads.
const slice2HarnessJS = `
FakeEl.prototype.replaceWith = function () {};
document.importNode = (n) => n;
const stored = { "dbtrail.overview.fold.recent": "open" };
ctx.navigated = [];
const sets = [];
const listeners = [];
FakeEl.prototype.addEventListener = function (ev, fn) { listeners.push({ node: this, ev, fn }); };
ctx.localStorage = { getItem: (k) => (k in stored ? stored[k] : null), setItem: (k, v) => sets.push(k + "=" + v), removeItem() {} };
vm.runInContext("capsKnown = true; capsCache = { monitor: true, permissions: {} }; serversEmpty = false; currentServer = 'a';", ctx);
const fn = (name) => vm.runInContext(name, ctx);
const out = {};
out.since = ["2026-09-24 23:00:00|2026-09-24 14:33:52", "2026-09-23 23:00:00|2026-09-24 14:33:52", "2026-09-20 09:15:00|2026-09-24 14:33:52", "junk|2026-09-24 14:33:52"].map((c) => { const [a, b] = c.split("|"); return fn("ovSinceLabel")(a, b); });
out.line = [
  { since: "2026-09-23 23:00:00", until: "2026-09-24 14:33:52", total: 1240, tables: 5 },
  { since: "2026-09-24 02:14:00", until: "2026-09-24 14:33:52", total: 0, tables: 0 },
  { since: "2026-09-24 02:14:00", until: "2026-09-24 14:33:52", total: 1, tables: 1 },
  null,
].map((a) => fn("ovActivityLine")(a));
out.age = fn("ovAgeText")("2026-09-24 14:30:07", Date.parse("2026-09-24T14:35:05Z"));
out.ageZ = fn("ovAgeText")("2026-09-24T14:30:07Z", Date.parse("2026-09-24T14:35:05Z"));
const f = fn("ovFrame")();
out.recentOpen = !!f.recentFold.open;
out.tablesOpen = !!f.tablesFold.open;
const tog = listeners.find((l) => l.node === f.recentFold && l.ev === "toggle");
f.recentFold.open = false; if (tog) tog.fn();
out.sets = sets.slice();
fn("fillOvActivity")(f, { since: "2026-09-23 23:00:00", until: "2026-09-24 14:33:52", total: 1240, tables: 5, deletes: 3, label: "live retention", complete: true, refreshed_at: "2026-09-24T14:33:52Z", top_tables: [] });
out.actLine = f.actLine.textContent;
// The line is a link into Events, the page that lists what it counted.
vm.runInContext("navigate = (r) => { navigated.push(r); };", ctx);
const lk = listeners.find((l) => l.node === f.actLine.children[0] && l.ev === "click");
if (lk) lk.fn({ preventDefault() {} });
out.navs = ctx.navigated.slice();
fn("fillOvActivity")(f, null);
out.actLineGone = f.actLine.textContent;
const m = fn("ovFlowModel")({ coverage: { freshness: "current", continuity: "ok", lag_seconds: 12, delta_to: "2026-09-24 14:33:52" }, baselines: { configured: true, snapshots: [{ time: "2026-09-24 14:30:07", age_hours: 0.083, tables: ["a.b"], kinds: ["dir"] }], schedule: { every: "5m", runnable: true, next_run: "2026-09-24T14:35:00Z" } }, server: { id: "a", kind: "registry", has_source: true }, schema: { state: "succeeded" }, uncaptured: { tables_captured: 1 }, monitorCap: true, may: () => true });
const sec = fn("flowSection")(m, { serverId: "a", registry: true, monitorCap: true });
const find = (n, cls, acc = []) => { if (!n || !n.children) return acc; if ((" " + n.className + " ").includes(" " + cls + " ")) acc.push(n); for (const c of n.children) find(c, cls, acc); return acc; };
const big = find(sec, "flow-big")[0];
out.bigStamp = big ? big.attrs["data-stamp"] : null;
out.bigText = big ? big.textContent : null;
// The tick re-reads the age off the stamp: ten minutes on, the number moved.
const later = Date.parse("2026-09-24T14:40:07Z");
const realNow = Date.now; Date.now = () => later;
fn("ovTickAges")({ querySelectorAll: (sel) => (sel === ".flow-big[data-stamp]" ? [big] : []) });
Date.now = realNow;
out.ticked = big ? big.textContent : null;
out.tickWant = fn("ovAgeText")("2026-09-24 14:30:07", later);
process.stdout.write("\n@@RESULT@@" + JSON.stringify(out));
`

func TestOverviewSliceTwo(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "slice2.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+slice2HarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	_, result, found := strings.Cut(string(raw), "@@RESULT@@")
	if !found {
		t.Fatalf("no result in node output:\n%s", raw)
	}
	var out struct {
		Since       []string `json:"since"`
		Line        []string `json:"line"`
		Age         string   `json:"age"`
		AgeZ        string   `json:"ageZ"`
		RecentOpen  bool     `json:"recentOpen"`
		TablesOpen  bool     `json:"tablesOpen"`
		Sets        []string `json:"sets"`
		ActLine     string   `json:"actLine"`
		ActLineGone string   `json:"actLineGone"`
		BigStamp    string   `json:"bigStamp"`
		BigText     string   `json:"bigText"`
		Ticked      string   `json:"ticked"`
		Navs        []string `json:"navs"`
		TickWant    string   `json:"tickWant"`
	}
	if err := json.Unmarshal([]byte(result), &out); err != nil {
		t.Fatal(err)
	}
	if want := []string{"Since 23:00", "Since 23:00 yesterday", "Since 2026-09-20 09:15", ""}; !reflect.DeepEqual(out.Since, want) {
		t.Errorf("since labels = %q, want %q", out.Since, want)
	}
	if want := []string{"Since 23:00 yesterday · 1,240 changes in 5 tables", "No changes since 02:14", "Since 02:14 · 1 change in 1 table", ""}; !reflect.DeepEqual(out.Line, want) {
		t.Errorf("activity lines = %q, want %q", out.Line, want)
	}
	if out.Age != "4m 58s ago" || out.AgeZ != "4m 58s ago" {
		t.Errorf("age = %q / %q, want 4m 58s ago from the stamp with or without a zone", out.Age, out.AgeZ)
	}
	// The fold this browser left open opens; the other starts closed; a
	// toggle is remembered.
	if !out.RecentOpen || out.TablesOpen {
		t.Errorf("folds: recent open=%v tables open=%v, want the remembered one open only", out.RecentOpen, out.TablesOpen)
	}
	if !reflect.DeepEqual(out.Sets, []string{"dbtrail.overview.fold.recent=closed"}) {
		t.Errorf("toggle remembered as %v", out.Sets)
	}
	if out.ActLine != "Since 23:00 yesterday · 1,240 changes in 5 tables ›" || out.ActLineGone != "" {
		t.Errorf("activity line = %q then %q (unavailable must render nothing)", out.ActLine, out.ActLineGone)
	}
	if !reflect.DeepEqual(out.Navs, []string{"events"}) {
		t.Errorf("clicking the activity line navigated to %v, want events", out.Navs)
	}
	if out.BigStamp != "2026-09-24 14:30:07" || out.BigText == "" {
		t.Errorf("the large age must carry its stamp for the tick: stamp=%q text=%q", out.BigStamp, out.BigText)
	}
	if out.Ticked != out.TickWant || out.Ticked == out.BigText || out.TickWant != "10m ago" {
		t.Errorf("after the tick the age reads %q (was %q), want %q", out.Ticked, out.BigText, out.TickWant)
	}
}
