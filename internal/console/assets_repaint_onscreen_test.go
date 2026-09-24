package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

// repaintByAddressRE finds a repaint decided by the address: comparing the
// path, or the route read from it, against a page name.
var repaintByAddressRE = regexp.MustCompile(`location\.pathname\s*[!=]==|routeFromLocation\(\)\s*[!=]==\s*"(baselines|verification|backup-settings|snapshots)"`)

// TestRepaintAsksWhetherItsBoxIsOnScreen: a job that finishes, a schedule
// saved or a restore started repaints the backups page only if that page is
// still on screen, and the question is asked of the page's own element, not of
// the address. Ten such checks used to compare the address with "/baselines".
// When the backup pages merge under a new address they would all answer "no"
// without any error, and the page would stop refreshing itself; a constant
// holding the new address would answer "yes" on every section of the merged
// page, repainting all of it over whatever someone is typing.
func TestRepaintAsksWhetherItsBoxIsOnScreen(t *testing.T) {
	raw, err := os.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(raw)
	for _, m := range repaintByAddressRE.FindAllStringIndex(js, -1) {
		line := 1
		for _, c := range js[:m[0]] {
			if c == '\n' {
				line++
			}
		}
		t.Errorf("app.js:%d decides by the address (%q): ask whether the page's element is still on screen instead", line, js[m[0]:m[1]])
	}
}

// TestBackupsOnScreenFollowsThePage drives the real Snapshots painter over a
// fake screen that knows what is attached to it: not on screen before any
// paint, on screen while it loads (so a job ending mid-fetch repaints with the
// newer state) and once painted, and off screen after another page replaces
// it. A paint that fails and shows its error is still the page. And a repaint
// of the page already on screen does NOT blank it while it fetches: those
// repaints come from a job that ended or a setting that saved, and the reader
// is mid-page — on a page this long, "Loading…" in place of everything drops
// them at the top of it.
func TestBackupsOnScreenFollowsThePage(t *testing.T) {
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
const screen = new FakeEl("main");
const attach = (parent, x) => { if (x && typeof x === "object") x.__parent = parent; };
const append = FakeEl.prototype.append;
FakeEl.prototype.append = function (...k) { for (const x of k) attach(this, x); return append.apply(this, k); };
const replaceChildren = FakeEl.prototype.replaceChildren;
FakeEl.prototype.replaceChildren = function (...k) { for (const c of this.children) attach(null, c); for (const x of k) attach(this, x); return replaceChildren.apply(this, k); };
Object.defineProperty(FakeEl.prototype, "lastElementChild", { get() { return this.children.filter((c) => c && c.nodeType === 1).pop() || null; } });
Object.defineProperty(FakeEl.prototype, "isConnected", { get() { for (let n = this; n; n = n.__parent) if (n === screen) return true; return false; } });
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
vm.runInContext("capsCache = { monitor: true }; capsKnown = true;", ctx);
let release;
const gate = new Promise((r) => { release = r; });
ctx.__api = (path) => gate.then(() => (path === "/api/servers" ? { servers: [{ id: "a", name: "a" }] } : {}));
vm.runInContext("api = (path) => __api(path);", ctx);
const on = () => vm.runInContext("backupsOnScreen()", ctx);
(async () => {
  const out = { before: on() };
  const painting = vm.runInContext("renderSnapshots()", ctx);
  out.loading = on();
  out.blanksOnFirstPaint = screen.textContent.includes("Loading");
  release();
  await painting;
  out.painted = on();
  // Repaint the page that is already up, with its fetches held.
  let release2;
  const gate2 = new Promise((r) => { release2 = r; });
  ctx.__api = (path) => gate2.then(() => (path === "/api/servers" ? { servers: [{ id: "a", name: "a" }] } : {}));
  const again = vm.runInContext("renderSnapshots()", ctx);
  out.keptWhileRepainting = !screen.textContent.includes("Loading");
  release2();
  await again;
  out.stillPainted = on();
  // A server SWITCH is not a repaint of the same page: what is on screen is
  // the other server's list, and its buttons carry the other server's id.
  let release3;
  const gate3 = new Promise((r) => { release3 = r; });
  ctx.__api = (path) => gate3.then(() => (path === "/api/servers" ? { servers: [{ id: "b", name: "b" }] } : {}));
  vm.runInContext("serverGen++; currentServer = 'b';", ctx);
  const switched = vm.runInContext("renderSnapshots()", ctx);
  out.blanksOnServerSwitch = screen.textContent.includes("Loading");
  release3();
  await switched;
  ctx.__api = (path) => (path === "/api/servers" ? Promise.resolve({ servers: [{ id: "b", name: "b" }] }) : Promise.resolve({}));
  vm.runInContext("clear(VIEW()); VIEW().append(el('div', { text: 'Events' }));", ctx);
  out.away = on();
  // A paint that fails shows its error in place of the page (renderError
  // clears the view, heading included): still the Backups page, on screen.
  ctx.snapshotHero = () => { throw new Error("boom"); };
  await vm.runInContext("renderSnapshots()", ctx);
  out.failed = on() && screen.textContent.includes("boom");
  console.log(JSON.stringify(out));
})().catch((e) => { console.log(JSON.stringify({ err: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "onscreen.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Before, Loading, Painted, Away, Failed                bool
		BlanksOnFirstPaint, KeptWhileRepainting, StillPainted bool
		BlanksOnServerSwitch                                  bool
		Err                                                   string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	if got.Before || !got.Loading || !got.Painted || got.Away || !got.Failed {
		t.Errorf("on screen: before any paint %v (want false), loading %v (want true), painted %v (want true), "+
			"after another page %v (want false), showing its own error %v (want true)",
			got.Before, got.Loading, got.Painted, got.Away, got.Failed)
	}
	if !got.BlanksOnFirstPaint {
		t.Error("arriving from another page does not blank the view: it would paint over somebody else's page")
	}
	if !got.BlanksOnServerSwitch {
		t.Error("switching servers leaves the previous server's page up while the new one loads: its list, " +
			"location and schedule are another server's, and its buttons would start a job on the server " +
			"the reader just left")
	}
	if !got.KeptWhileRepainting || !got.StillPainted {
		t.Errorf("repainting the page already on screen: kept while fetching %v (want true), still the page after %v (want true) — "+
			"a repaint from a finished job must not drop the reader at the top of the page",
			got.KeptWhileRepainting, got.StillPainted)
	}
}
