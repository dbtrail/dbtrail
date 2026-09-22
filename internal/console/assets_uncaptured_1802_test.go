package console

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/status"
)

// ─── The Overview names every uncaptured table (#1802) ───────────────────────
//
// The card is drawn from views Go actually marshals (status.TableCapture.View,
// the same call the endpoint makes), rendered by the real app.js in node. What
// the page shows is compared with the data it draws, and the text Copy puts on
// the clipboard with the text on screen.

type uncapRow struct {
	Cls       string `json:"cls"`
	Text      string `json:"text"`
	HasFix    bool   `json:"hasFix"`
	FixHidden bool   `json:"fixHidden"`
	FixShown  bool   `json:"fixShown"`
	SQL       string `json:"sql"`
	Copied    string `json:"copied"`
	Folded    bool   `json:"folded"`
}

type uncapDrawn struct {
	Title   string     `json:"title"`
	Text    string     `json:"text"`
	Rows    []uncapRow `json:"rows"`
	Summary string     `json:"summary"`
	Folded  int        `json:"folded"`
}

func renderUncaptured(t *testing.T, views map[string]any) map[string]*uncapDrawn {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	in, err := json.Marshal(views)
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
document.importNode = (n) => n;
FakeEl.prototype.addEventListener = function (ev, fn) { (this.handlers = this.handlers || {})[ev] = fn; };
vm.runInContext("copyText = (text) => { copied.push(text); }; var copied = [];", ctx);
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const find = (n, pred, out = []) => { if (!n || n.nodeType === 3) return out; if (pred(n)) out.push(n); for (const c of n.children || []) find(c, pred, out); return out; };
const has = (n, cls) => (" " + (n.className || "") + " ").includes(" " + cls + " ");
const card = vm.runInContext("uncapturedCard", ctx);
const views = ` + string(in) + `;
const out = {};
for (const [name, v] of Object.entries(views)) {
  const c = card(v);
  if (!c) { out[name] = null; continue; }
  const folds = find(c, (n) => n.tag === "details");
  const inFold = new Set(folds.flatMap((d) => find(d, (n) => has(n, "uncap"))));
  const rows = find(c, (n) => has(n, "uncap")).map((r) => {
    const btn = find(r, (n) => n.tag === "button" && flat(n).join("") === "Show fix")[0];
    const fix = find(r, (n) => has(n, "uncap-fix"))[0];
    const row = { cls: r.className, text: flat(r).join(" "), hasFix: !!btn, folded: inFold.has(r), fixHidden: fix ? !!fix.hidden : false };
    if (btn) {
      btn.handlers.click({ stopPropagation() {} });
      row.fixShown = !fix.hidden && flat(btn).join("") === "Hide fix" && btn.attrs["aria-expanded"] === "true";
      row.sql = flat(find(fix, (n) => n.tag === "pre")[0]).join("");
      const copy = find(fix, (n) => n.tag === "button" && flat(n).join("") === "Copy")[0];
      vm.runInContext("copied.length = 0", ctx);
      copy.handlers.click({ stopPropagation() {} });
      row.copied = vm.runInContext("copied[0]", ctx);
    }
    return row;
  });
  const title = find(c, (n) => has(n, "ov-panel-title"))[0];
  out[name] = { title: title ? flat(title).join("") : "", text: flat(c).join(" "), rows, summary: folds.length ? flat(find(folds[0], (n) => n.tag === "summary")[0]).join("") : "", folded: folds.length };
}
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "uncap.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got map[string]*uncapDrawn
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return got
}

func uncapCapture(n int) *status.TableCapture {
	c := &status.TableCapture{State: status.TableCaptureChecked, SnapshotID: 9, CoverageKnown: true}
	for i := range 14 {
		c.Captured = append(c.Captured, status.TableRef{Schema: "shop", Table: fmt.Sprintf("t%02d", i)})
	}
	for i := range n {
		c.Uncaptured = append(c.Uncaptured, status.UncapturedTable{Schema: "shop", Table: fmt.Sprintf("log_%02d", i), Reason: "no primary key", PKColumn: "id"})
	}
	return c
}

func TestUncapturedCardDrawsTheList(t *testing.T) {
	one := uncapCapture(0)
	one.Uncaptured = []status.UncapturedTable{{Schema: "shop", Table: "audit_log", Reason: "no primary key", PKColumn: "dbtrail_id"}}
	odd := uncapCapture(0)
	odd.Uncaptured = []status.UncapturedTable{
		{Schema: "Shop Data", Table: "odd`Name", Reason: "not InnoDB; no primary key", PKColumn: "id"},
		{Schema: "shop", Table: "mystery", Reason: "partitioned table"},
	}
	withheld := uncapCapture(0)
	withheld.Uncaptured = []status.UncapturedTable{
		{Schema: "shop", Table: "audit_log", Reason: "no primary key", PKColumn: "id"},
		{Schema: "shop", Table: "secret_log", Reason: "no primary key", PKColumn: "id"},
	}
	decided := one.View(nil)
	decided.Uncaptured[0].Decision = "left_out"
	legacy := (&status.TableCapture{State: status.TableCaptureNotChecked, SnapshotID: 2, Captured: uncapCapture(0).Captured}).
		WithFilter(status.CaptureFilter{Known: true})
	legacyUnknown := (&status.TableCapture{State: status.TableCaptureNotChecked, SnapshotID: 2, Captured: uncapCapture(0).Captured}).
		WithFilter(status.CaptureFilter{})
	noScope := uncapCapture(1)
	noScope.CoverageKnown = false
	capped := uncapCapture(status.MaxUncapturedListed + 4)
	noFix := uncapCapture(0)
	noFix.Uncaptured = []status.UncapturedTable{{Schema: "shop", Table: "audit_log", Reason: "no primary key"}}

	got := renderUncaptured(t, map[string]any{
		"one":           one.View(nil),
		"many":          uncapCapture(12).View(nil),
		"odd":           odd.View(nil),
		"withheld":      withheld.View(func(_, table string) bool { return !strings.Contains(table, "secret") }),
		"allCaptured":   uncapCapture(0).View(nil),
		"decided":       decided,
		"legacy":        legacy.View(nil),
		"legacyUnknown": legacyUnknown.View(nil),
		"unavailable":   (&status.TableCapture{State: status.TableCaptureUnavailable, Err: fmt.Errorf("Error 1142: SELECT command denied")}).View(nil),
		"noSnapshot":    (&status.TableCapture{State: status.TableCaptureNoSnapshot}).View(nil),
		"notApplicable": (&status.TableCapture{State: status.TableCaptureNotApplicable}).View(nil),
		"noScope":       noScope.View(nil),
		"capped":        capped.View(nil),
		"noFix":         noFix.View(nil),
		"unknownState":  map[string]any{"state": "something_new", "uncaptured": []any{}},
		"missing":       nil,
	})

	// One table: the design's sentence, a Show fix that reveals exactly the
	// statement Go built, and a Copy that copies exactly what is shown.
	d := got["one"]
	if d == nil || len(d.Rows) != 1 {
		t.Fatalf("one: %+v", d)
	}
	if !strings.Contains(d.Text, "Capturing 14 of 15 tables.") {
		t.Errorf("one: coverage is not stated as a count: %q", d.Text)
	}
	r := d.Rows[0]
	wantSQL := "ALTER TABLE `shop`.`audit_log` ADD COLUMN `dbtrail_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"
	if !strings.Contains(r.Text, "shop.audit_log is not captured: no primary key.") || !r.HasFix {
		t.Errorf("one: row = %+v", r)
	}
	if !strings.Contains(r.Cls, "needs-decision") {
		t.Errorf("one: an undecided table must be drawn as needing a decision, class %q", r.Cls)
	}
	if !r.FixHidden || !r.FixShown {
		t.Errorf("one: the fix must start folded and open on Show fix (hidden=%v shown=%v)", r.FixHidden, r.FixShown)
	}
	if r.SQL != wantSQL || r.Copied != wantSQL {
		t.Errorf("one: shown %q, copied %q, want both %q", r.SQL, r.Copied, wantSQL)
	}
	if !strings.Contains(r.Text, "Its changes are not kept until it has a primary key.") {
		t.Errorf("one: the fix does not say what happens to the data: %q", r.Text)
	}

	// Many tables: a count and a list that folds, never a wall.
	d = got["many"]
	if d == nil || len(d.Rows) != 12 || d.Folded != 1 {
		t.Fatalf("many: %d rows, %d folds: %+v", len(d.Rows), d.Folded, d)
	}
	shown := 0
	for _, r := range d.Rows {
		if !r.Folded {
			shown++
		}
	}
	if shown != 3 || d.Summary != "Show 9 more" {
		t.Errorf("many: %d rows before the fold and summary %q, want 3 and \"Show 9 more\"", shown, d.Summary)
	}
	if !strings.Contains(d.Text, "Capturing 14 of 26 tables.") {
		t.Errorf("many: %q", d.Text)
	}

	// Odd names keep their exact spelling on screen and in the statement; an
	// unknown reason is named, with no fix.
	d = got["odd"]
	if d == nil || len(d.Rows) != 2 {
		t.Fatalf("odd: %+v", d)
	}
	if r := d.Rows[0]; !strings.Contains(r.Text, "Shop Data.odd`Name is not captured: not an InnoDB table, and no primary key.") ||
		r.SQL != "ALTER TABLE `Shop Data`.`odd``Name` ENGINE=InnoDB, ADD COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;" || r.Copied != r.SQL {
		t.Errorf("odd: row = %+v", r)
	}
	if r := d.Rows[1]; !strings.Contains(r.Text, `shop.mystery is not captured: "partitioned table".`) || r.HasFix {
		t.Errorf("odd: an unknown reason must be shown without a fix: %+v", r)
	}

	// Withheld: counted in a row of its own, never named.
	d = got["withheld"]
	if d == nil || strings.Contains(d.Text, "secret") {
		t.Fatalf("withheld: %+v", d)
	}
	if !strings.Contains(d.Text, "1 table outside your access is not captured.") || !strings.Contains(d.Text, "Capturing 14 of 16 tables.") {
		t.Errorf("withheld: %q", d.Text)
	}

	// Everything captured: the count, the same closing line the terminal
	// prints (both read one field, so they cannot answer differently), no rows.
	d = got["allCaptured"]
	if d == nil || len(d.Rows) != 0 || !strings.Contains(d.Text, "Capturing 14 of 14 tables.") {
		t.Errorf("allCaptured: %+v", d)
	}
	if !strings.Contains(d.Text, "Every table in that scope is captured.") {
		t.Errorf("the card stops at the count while the terminal answers: %q", d.Text)
	}
	// And it is claimed nowhere else.
	for _, name := range []string{"one", "many", "noScope", "legacy", "withheld", "capped"} {
		if d := got[name]; d != nil && strings.Contains(d.Text, "Every table in that scope is captured.") {
			t.Errorf("%s: claims everything is captured", name)
		}
	}

	// A decided table is gray, not red, and still named with its fix.
	if d := got["decided"]; d == nil || len(d.Rows) != 1 || !strings.Contains(d.Rows[0].Cls, "decided") ||
		strings.Contains(d.Rows[0].Cls, "needs-decision") || !d.Rows[0].HasFix {
		t.Errorf("decided: %+v", d)
	}

	// Not checked: never "of", never "all captured", and never a cause
	// nobody checked — a current build that has not migrated this server's
	// data leaves the same shape as an old one.
	d = got["legacy"]
	if d == nil || !strings.Contains(d.Text, "Capturing 14 tables.") || strings.Contains(d.Text, " of ") ||
		!strings.Contains(d.Text, "has not recorded") || !strings.Contains(d.Text, "leaves one out") ||
		strings.Contains(d.Text, "The last schema read left no table out") {
		t.Errorf("legacy: %+v", d)
	}
	if strings.Contains(strings.ToLower(d.Text), "older version") || strings.Contains(strings.ToLower(d.Text), "predates") {
		t.Errorf("legacy asserts a cause nobody checked: %q", d.Text)
	}

	// The same state with the scope unknown counts nothing at all, and says
	// neither "left no table out" nor "these tables are left out": whether
	// anything was is exactly what it does not know.
	d = got["legacyUnknown"]
	if d == nil || strings.Contains(d.Text, "Capturing") {
		t.Errorf("legacyUnknown counted with the scope unknown: %+v", d)
	}
	if strings.Contains(d.Text, "left no table out") || strings.Contains(d.Text, "These tables are left out") {
		t.Errorf("legacyUnknown claims something it cannot know: %q", d.Text)
	}
	if !strings.Contains(d.Text, "has not recorded") || !strings.Contains(d.Text, "is set where capture runs") {
		t.Errorf("legacyUnknown = %q", d.Text)
	}

	// Scope unknown: the tables are still named, with no count anywhere.
	d = got["noScope"]
	if d == nil || len(d.Rows) != 1 || strings.Contains(d.Text, "Capturing") {
		t.Errorf("noScope: %+v", d)
	}
	if !strings.Contains(d.Text, "is set where capture runs") {
		t.Errorf("noScope does not say why nothing is counted: %q", d.Text)
	}

	// Capped: the ones past the cap are counted in a line of their own, and
	// that line sits OUTSIDE the fold — the notice saying the list is
	// incomplete must not be one of the things the fold hides.
	d = got["capped"]
	if d == nil || len(d.Rows) != status.MaxUncapturedListed+1 {
		t.Fatalf("capped drew %d rows, want the cap plus the line that counts the rest", len(d.Rows))
	}
	notice := d.Rows[len(d.Rows)-1]
	if !strings.Contains(notice.Text, "4 more tables are not captured, not listed here.") {
		t.Errorf("capped hides what it left out: %q", d.Text)
	}
	if notice.Folded {
		t.Error("the line saying the list is incomplete is itself folded away")
	}

	// Same for the withheld line.
	if w := got["withheld"]; w != nil {
		last := w.Rows[len(w.Rows)-1]
		if !strings.Contains(last.Text, "outside your access") || last.Folded {
			t.Errorf("the withheld notice is not the visible last row: %+v", last)
		}
	}

	// No column recorded: the table is named, with no statement and the step
	// that produces one.
	d = got["noFix"]
	if d == nil || len(d.Rows) != 1 || d.Rows[0].HasFix {
		t.Errorf("noFix: %+v", d)
	}
	if !strings.Contains(d.Rows[0].Text, "read this server's tables again") {
		t.Errorf("noFix leaves no next step: %q", d.Rows[0].Text)
	}

	// Unavailable: says it could not check, with the reason, and claims no count.
	d = got["unavailable"]
	if d == nil || !strings.Contains(d.Text, "Could not check which tables are captured: Error 1142: SELECT command denied") ||
		strings.Contains(d.Text, "Capturing") {
		t.Errorf("unavailable: %+v", d)
	}

	// Nothing to say: no card at all, and `bintrail status` prints no section
	// for the same states (TestSilentStatesAgreeAcrossSurfaces), so the two
	// surfaces agree instead of one going quiet while the other speaks.
	for _, name := range []string{"noSnapshot", "notApplicable", "unknownState", "missing"} {
		if got[name] != nil {
			t.Errorf("%s: drew a card: %+v", name, got[name])
		}
	}

	// The panel title is a noun phrase, like every sibling panel's, and the
	// sentence with the count lives in the body.
	for _, name := range []string{"one", "many", "noScope", "legacy", "legacyUnknown"} {
		if d := got[name]; d != nil && d.Title != "Table coverage" {
			t.Errorf("%s: panel title = %q, want the noun phrase every sibling uses", name, d.Title)
		}
	}

	// Every sentence a reader sees: no em dash, and none of the words the
	// first-run copy keeps out of sentences (design 1.12).
	banned := regexp.MustCompile(`(?i)\b(index|source|preflight|console|baseline|backup|daemon|watch|monitor|protect|track|stream)\b`)
	for name, d := range got {
		if d == nil {
			continue
		}
		if strings.Contains(d.Text, "—") {
			t.Errorf("%s: an em dash on screen: %q", name, d.Text)
		}
		if m := banned.FindString(d.Text); m != "" {
			t.Errorf("%s: %q on screen: %q", name, m, d.Text)
		}
	}
	body := jsFunctionSpan(t, readAsset(t, "app.js"), "uncapturedCard") + jsFunctionSpan(t, readAsset(t, "app.js"), "uncapturedRow")
	for _, m := range regexp.MustCompile(`"([^"\n]*)"`).FindAllStringSubmatch(body, -1) {
		if strings.Contains(m[1], "—") {
			t.Errorf("uncaptured card holds an em dash in %q", m[1])
		}
	}
}

// TestOverviewLoadsUncapturedTables: the Overview asks for the list and draws
// it into its own slot, outside the first-run steps (design 2.3: the steps go
// away after the first snapshot, and the table must stay named).
func TestOverviewLoadsUncapturedTables(t *testing.T) {
	js := readAsset(t, "app.js")
	render := jsFunctionBody(t, js, "renderOverview")
	if !strings.Contains(render, "loadOvUncaptured(f, live)") {
		t.Error("renderOverview does not load the uncaptured tables")
	}
	load := jsFunctionBody(t, js, "loadOvUncaptured")
	for _, want := range []string{"/api/uncaptured-tables", "fillOvUncaptured("} {
		if !strings.Contains(load, want) {
			t.Errorf("loadOvUncaptured does not use %s", want)
		}
	}
	frame := jsFunctionBody(t, js, "ovFrame")
	if !strings.Contains(frame, "f.uncapSlot") {
		t.Error("ovFrame has no slot for the uncaptured tables")
	}
	for _, fn := range []string{"firstRunCard", "watchFirstRun"} {
		if strings.Contains(jsFunctionBody(t, js, fn), "uncap") {
			t.Errorf("%s draws the uncaptured tables: they must stay outside the first-run steps", fn)
		}
	}
}

// TestOvUncapturedSaysWhenItCouldNotAsk: a request that fails outright (the
// server's connection could not be opened, a session whose profile does not
// resolve, a network blip) must leave the same "could not check" line on
// screen as a read that failed INSIDE the endpoint. Drawing nothing would
// rebuild, one layer up, the silence this issue exists to end: a page that
// looks healthy while nothing knows which tables are captured.
func TestOvUncapturedSaysWhenItCouldNotAsk(t *testing.T) {
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
document.importNode = (n) => n;
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
(async () => {
  const flush = async () => { for (let i = 0; i < 10; i++) await Promise.resolve(); };
  // The page logs the failure too; keep it off this script's stdout.
  ctx.console = { log: console.log, error: () => {}, warn: () => {} };
  let rejection;
  ctx.nextApi = () => Promise.reject(rejection);
  vm.runInContext("api = () => nextApi();", ctx);
  const load = vm.runInContext("loadOvUncaptured", ctx);
  const out = {};
  for (const [name, err] of [["http", Object.assign(new Error("HTTP 502: could not open this server"), { status: 502 })],
                             ["plain", "network down"]]) {
    rejection = err;
    const f = { uncapSlot: new FakeEl("div") };
    load(f, () => true); await flush();
    out[name] = flat(f.uncapSlot).join(" ");
  }
  // A request that lands after the page moved on paints nothing.
  rejection = new Error("late");
  const f = { uncapSlot: new FakeEl("div") };
  load(f, () => false); await flush();
  out.stale = flat(f.uncapSlot).join(" ");
  console.log(JSON.stringify(out));
})();
`
	path := filepath.Join(t.TempDir(), "uncapfail.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct{ HTTP, Plain, Stale string }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if !strings.Contains(got.HTTP, "Could not check which tables are captured") ||
		!strings.Contains(got.HTTP, "HTTP 502: could not open this server") {
		t.Errorf("a failed request drew %q, want the could-not-check line with the server's own reason", got.HTTP)
	}
	if !strings.Contains(got.Plain, "Could not check which tables are captured") {
		t.Errorf("a rejection that is not an Error drew %q", got.Plain)
	}
	if got.Stale != "" {
		t.Errorf("a late failure painted over a page that moved on: %q", got.Stale)
	}
}

// TestOverviewShowsTheUncapturedSlotWhileItLoads: an empty slot while the
// request is in flight reads as "nothing to report" on the very page whose
// job is to report it. The slot carries a pending card, like the coverage
// card above it, and buildOverview — the seam the browser suite drives —
// fills it like every other card.
func TestOverviewUncapturedSlotAndSeam(t *testing.T) {
	js := readAsset(t, "app.js")
	frame := jsFunctionBody(t, js, "ovFrame")
	if !strings.Contains(frame, "f.uncapSlot.append(ovPendingCard(") {
		t.Error("ovFrame leaves the uncaptured slot empty while its request is in flight")
	}
	idx := func(needle string) int { return strings.Index(frame, needle) }
	if idx("f.uncapSlot") < idx("f.covSlot") {
		t.Error("the uncaptured card must sit under the restore window it qualifies")
	}
	build := jsFunctionBody(t, js, "buildOverview")
	if !strings.Contains(build, "fillOvUncaptured(") {
		t.Error("buildOverview never fills the uncaptured card, so the browser suite cannot drive it")
	}
	if !strings.Contains(jsFunctionSpan(t, js, "buildOverview"), "uncaptured") {
		t.Error("buildOverview does not take the uncaptured payload")
	}
}

// TestUncapturedSlotSurvivesAFourArgumentBuild: buildOverview grew a fifth
// parameter, and the browser suite calls it with FOUR
// (test/console-e2e/console_e2e.mjs, the overview fixture scenario, which
// drives the real builder three times and then reads every .warn-item on the
// page). So the payload arrives undefined there, and this pins what that must
// do: draw NOTHING, and throw nothing. A card built from an absent payload
// would put a warning on a page that was told nothing, and a throw would take
// the three tile assertions down with it in a suite this change may not edit.
func TestUncapturedSlotSurvivesAFourArgumentBuild(t *testing.T) {
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
document.importNode = (n) => n;
const fill = vm.runInContext("fillOvUncaptured", ctx);
const card = vm.runInContext("uncapturedCard", ctx);
const out = {};
// The slot arrives holding the pending card ovFrame put there, so "drew
// nothing" has to mean the placeholder is gone too, not that nothing changed.
const f = { uncapSlot: new FakeEl("div") };
f.uncapSlot.append(new FakeEl("p"));
try { fill(f, undefined); out.threw = null; } catch (e) { out.threw = String((e && e.message) || e); }
out.left = f.uncapSlot.children.length;
out.undefCard = card(undefined) === null;
out.nullCard = card(null) === null;
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "uncap4arg.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Threw     *string `json:"threw"`
		Left      int     `json:"left"`
		UndefCard bool    `json:"undefCard"`
		NullCard  bool    `json:"nullCard"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Threw != nil {
		t.Errorf("a four-argument build threw %q; the browser suite calls it that way", *got.Threw)
	}
	if got.Left != 0 {
		t.Errorf("a four-argument build left %d node(s) in the slot, want an empty slot", got.Left)
	}
	if !got.UndefCard || !got.NullCard {
		t.Errorf("a missing payload drew a card (undefined empty=%v, null empty=%v)", got.UndefCard, got.NullCard)
	}
}
