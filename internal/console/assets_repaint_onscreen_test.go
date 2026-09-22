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

// TestBackupsOnScreenFollowsThePage drives the real Backups painter over a
// fake screen that knows what is attached to it: not on screen before any
// paint, on screen while it loads (so a job ending mid-fetch repaints with the
// newer state) and once painted, and off screen after another page replaces
// it. A paint that fails and shows its error is still the page.
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
  const painting = vm.runInContext("renderBaselines()", ctx);
  out.loading = on();
  release();
  await painting;
  out.painted = on();
  vm.runInContext("clear(VIEW()); VIEW().append(el('div', { text: 'Events' }));", ctx);
  out.away = on();
  // A paint that fails shows its error in place of the page (renderError
  // clears the view, heading included): still the Backups page, on screen.
  ctx.baselineContextStrip = () => { throw new Error("boom"); };
  await vm.runInContext("renderBaselines()", ctx);
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
		Before, Loading, Painted, Away, Failed bool
		Err                                    string
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
}
