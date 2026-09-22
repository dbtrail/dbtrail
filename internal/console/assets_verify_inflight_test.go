package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// verifyInflightHarnessJS drives the real Verification page over a fake screen
// that knows what is attached to it, a clock that runs every timer at once,
// and an api whose verify status answers come from a per-server queue (the
// last answer repeats). It records every toast and every status request.
const verifyInflightHarnessJS = `
const screen = new FakeEl("main");
const attach = (parent, x) => { if (x && typeof x === "object") x.__parent = parent; };
const append = FakeEl.prototype.append;
FakeEl.prototype.append = function (...k) { for (const x of k) attach(this, x); return append.apply(this, k); };
const replaceChildren = FakeEl.prototype.replaceChildren;
FakeEl.prototype.replaceChildren = function (...k) { for (const c of this.children) attach(null, c); for (const x of k) attach(this, x); return replaceChildren.apply(this, k); };
Object.defineProperty(FakeEl.prototype, "isConnected", { get() { for (let n = this; n; n = n.__parent) if (n === screen) return true; return false; } });
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
const find = (n, cls) => { if (!n || !n.children) return null; if ((" " + n.className + " ").includes(" " + cls + " ")) return n;
  for (const c of n.children) { const f = find(c, cls); if (f) return f; } return null; };
ctx.setTimeout = (fn) => setTimeout(fn, 0);
const toasts = [];
ctx.toast = (m) => toasts.push(m);
ctx.toastError = (m) => toasts.push("ERR " + m);
const queues = {}, gets = {};
let posts = 0;
ctx.__api = async (path, opts) => {
  if (path === "/api/servers") return { servers: [{ id: "a", name: "a" }, { id: "b", name: "b" }] };
  if (path.endsWith("/verify/history")) return { history: [] };
  const m = /^\/api\/servers\/([^/]+)\/verify$/.exec(path);
  if (!m) return {};
  if (opts && opts.method === "POST") { posts++; return { verify: { state: "running", mode: "recover-inputs", results: [] } }; }
  gets[m[1]] = (gets[m[1]] || 0) + 1;
  const q = queues[m[1]] || [{ state: "idle" }];
  return { verify: q.length > 1 ? q.shift() : q[0] };
};
vm.runInContext("api = (path, opts) => __api(path, opts);", ctx);
const settle = () => new Promise((r) => setTimeout(r, 30));
const paint = async (server) => {
  vm.runInContext("currentServer = " + JSON.stringify(server) + ";", ctx);
  await vm.runInContext("renderVerification()", ctx);
};
const away = () => { vm.runInContext("clear(VIEW()); VIEW().append(el('div', { text: 'Events' }));", ctx); };
const box = () => { const r = find(screen, "vfy-results"); return r ? r.textContent : null; };
const btn = () => { const b = find(screen, "vfy-run"); return b ? { disabled: !!b.disabled, text: b.textContent } : null; };
const running = (n) => ({ state: "running", mode: "recover-inputs", results: Array.from({ length: n }, (_, i) => ({ schema: "s", table: "t" + i, status: "match" })), summary: { match: n } });
const finished = { state: "succeeded", verdict: "verified", mode: "recover-inputs", results: [{ schema: "s", table: "t0", status: "match" }], summary: { match: 1 } };
const reset = (caps) => {
  vm.runInContext("capsCache = " + JSON.stringify(caps || { monitor: true, verify_trigger: true, verify: true }) + "; capsKnown = true;", ctx);
  vm.runInContext("if (typeof vfyLive !== 'undefined') { vfyLive.clear(); vfyFollowing.clear(); vfyView = null; }", ctx);
  for (const k of Object.keys(queues)) delete queues[k];
  for (const k of Object.keys(gets)) delete gets[k];
  toasts.length = 0; posts = 0;
};
const out = {};
(async () => {
  // Start a run, leave, come back while it runs, let it end.
  reset();
  queues.a = [{ state: "idle" }, running(1), running(2), running(2), finished];
  await paint("a");
  const oldBox = find(screen, "vfy-results");
  const runBtn = find(screen, "vfy-run");
  find(screen, "vfy-mode").value = "recover-inputs";
  const click = runBtn.onclick();
  await new Promise((r) => setImmediate(r));
  const startedBtn = btn();
  away();
  await paint("a");
  const backBox = box(), backBtn = btn();
  await click;
  await settle();
  out.clickThenReturn = { startedBtn, backBox, backBtn, endBox: box(), endBtn: btn(), oldBoxText: oldBox.textContent, toasts: [...toasts], posts };

  // A run the schedule started: the page finds it and follows it.
  reset();
  queues.a = [running(1), running(1), finished];
  await paint("a");
  const schedBox = box(), schedBtn = btn();
  await settle();
  out.scheduled = { schedBox, schedBtn, endBox: box(), endBtn: btn(), toasts: [...toasts] };

  // Nothing running: one status request, no loop.
  reset();
  await paint("a");
  await settle();
  out.idle = { box: box(), btn: btn(), gets: gets.a || 0, toasts: [...toasts] };

  // A run on a, then the page for b, then back to a.
  reset();
  queues.a = [running(1), running(1), running(1), running(1), finished];
  queues.b = [{ state: "idle" }];
  await paint("a");
  const aBox = box();
  away();
  await paint("b");
  const bBox = box(), bBtn = btn();
  await settle();
  const bLater = box();
  away();
  await paint("a");
  await settle();
  out.switchServers = { aBox, bBox, bBtn, bLater, aEnd: box(), aBtn: btn(), toasts: [...toasts] };

  // The feature off: the page says so and asks nothing.
  reset({ monitor: true, verify_trigger: false });
  await paint("a");
  await settle();
  out.off = { gets: gets.a || 0 };

  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`

// TestVerifyRunSurvivesARepaint: a verification run's state lives per server,
// not in the box that was on screen when it started. The page repaints (Back
// to it, a server switch, and once the backup pages merge, any job that
// finishes), and a run that wrote into the box it saved at the start kept
// writing off screen while the new box said "No run yet", its button offered
// another run, and the button re-enabled at the end was the detached one. A
// run the schedule started never showed at all.
func TestVerifyRunSurvivesARepaint(t *testing.T) {
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
	path := filepath.Join(t.TempDir(), "verify.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+verifyInflightHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type button struct {
		Disabled bool
		Text     string
	}
	var got struct {
		Err             string
		ClickThenReturn struct {
			StartedBtn, BackBtn, EndBtn *button
			BackBox, EndBox, OldBoxText string
			Toasts                      []string
			Posts                       int
		}
		Scheduled struct {
			SchedBox, EndBox string
			SchedBtn, EndBtn *button
			Toasts           []string
		}
		Idle struct {
			Box    string
			Btn    *button
			Gets   int
			Toasts []string
		}
		SwitchServers struct {
			ABox, BBox, BLater, AEnd string
			BBtn, ABtn               *button
			Toasts                   []string
		}
		Off struct{ Gets int }
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	finishToasts := func(ts []string) int {
		n := 0
		for _, m := range ts {
			if strings.Contains(m, "Verification complete") {
				n++
			}
		}
		return n
	}
	busy := func(b *button) bool { return b != nil && b.Disabled && b.Text == "Running…" }
	ready := func(b *button) bool { return b != nil && !b.Disabled && b.Text == "Run verification" }

	c := got.ClickThenReturn
	if !busy(c.StartedBtn) {
		t.Errorf("click: the button on screen reads %+v, want disabled and Running…", c.StartedBtn)
	}
	if !strings.Contains(c.BackBox, "RUNNING") || !busy(c.BackBtn) {
		t.Errorf("click, leave, return: box %q, button %+v; want the run in progress and the button busy", c.BackBox, c.BackBtn)
	}
	if !strings.Contains(c.EndBox, "DONE") || !ready(c.EndBtn) {
		t.Errorf("click, leave, return, run ends: box %q, button %+v; want the finished run and the button on screen ready", c.EndBox, c.EndBtn)
	}
	if strings.Contains(c.OldBoxText, "DONE") {
		t.Errorf("the box left behind kept receiving the run: %q", c.OldBoxText)
	}
	if n := finishToasts(c.Toasts); n != 1 || c.Posts != 1 {
		t.Errorf("click then return: %d finish toasts and %d starts, want 1 and 1 (two loops would say it twice): %q", n, c.Posts, c.Toasts)
	}

	s := got.Scheduled
	if !strings.Contains(s.SchedBox, "RUNNING") || !busy(s.SchedBtn) {
		t.Errorf("a run the schedule started: box %q, button %+v; want it shown in progress and the button busy", s.SchedBox, s.SchedBtn)
	}
	if !strings.Contains(s.EndBox, "DONE") || !ready(s.EndBtn) || finishToasts(s.Toasts) != 1 {
		t.Errorf("the scheduled run ends: box %q, button %+v, toasts %q; want it finished, the button ready, one toast", s.EndBox, s.EndBtn, s.Toasts)
	}

	i := got.Idle
	if !strings.Contains(i.Box, "No run yet") || !ready(i.Btn) || i.Gets != 1 || len(i.Toasts) != 0 {
		t.Errorf("nothing running: box %q, button %+v, %d status requests, toasts %q; want No run yet, ready, exactly 1, none",
			i.Box, i.Btn, i.Gets, i.Toasts)
	}

	w := got.SwitchServers
	if !strings.Contains(w.ABox, "RUNNING") {
		t.Errorf("server a with a run: box %q, want it in progress", w.ABox)
	}
	if !strings.Contains(w.BBox, "No run yet") || !ready(w.BBtn) || strings.Contains(w.BLater, "RUNNING") || strings.Contains(w.BLater, "DONE") {
		t.Errorf("server b while a runs: box %q then %q, button %+v; want b's own empty box, never a's run, and b's button ready", w.BBox, w.BLater, w.BBtn)
	}
	if !strings.Contains(w.AEnd, "DONE") || !ready(w.ABtn) || finishToasts(w.Toasts) != 1 {
		t.Errorf("back on a after its run ended: box %q, button %+v, toasts %q; want it finished, ready, one toast", w.AEnd, w.ABtn, w.Toasts)
	}

	if got.Off.Gets != 0 {
		t.Errorf("verification turned off: %d status requests, want none (the server refuses every one)", got.Off.Gets)
	}
}
