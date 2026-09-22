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
// that knows what is attached to it, a manual clock (the page's timers wait
// for the test's tick), and an api whose verify status answers come from a
// per-server queue (the last answer repeats) or a refusal. It records every
// toast and every status request.
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
// A manual clock: the page's timers wait until the test ticks, so a run on
// one server really is going while the test paints another, and a click
// really races a repaint.
let timers = [];
ctx.setTimeout = (fn) => { timers.push(fn); return timers.length; };
const flush = async () => { for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r)); };
const tick = async () => { const due = timers.splice(0); due.forEach((f) => f()); await flush(); };
const drain = async (n = 20) => { for (let i = 0; i < n && timers.length; i++) await tick(); };
const toasts = [];
ctx.toast = (m) => toasts.push(m);
ctx.toastError = (m) => toasts.push("ERR " + m);
const queues = {}, gets = {}, forbid = {};
let posts = 0, postGate = null, post409 = false, post409Msg = "a verify run is already in progress for this server", holdNextGet = null;
const getConflict = {};
ctx.__api = async (path, opts) => {
  if (path === "/api/servers") return { servers: [{ id: "a", name: "a" }, { id: "b", name: "b" }] };
  if (path.endsWith("/verify/history")) return { history: [] };
  const m = /^\/api\/servers\/([^/]+)\/verify$/.exec(path);
  if (!m) return {};
  if (opts && opts.method === "POST") {
    posts++;
    if (postGate) await postGate.p;
    if (post409) throw Object.assign(new Error(post409Msg), { status: 409 });
    return { verify: { state: "running", since: "2026-09-22 10:00:00", mode: "recover-inputs", results: [] } };
  }
  gets[m[1]] = (gets[m[1]] || 0) + 1;
  if (forbid[m[1]]) throw Object.assign(new Error("verification isn't available while an access-control profile is active"), { status: 403 });
  if (getConflict[m[1]]) throw Object.assign(new Error(post409Msg), { status: 409 });
  const q = queues[m[1]] || [{ state: "idle" }];
  const answer = { verify: q.length > 1 ? q.shift() : q[0] };
  // A held answer is read now and delivered late, after newer ones.
  if (holdNextGet) { const g = holdNextGet; holdNextGet = null; await g; }
  return answer;
};
vm.runInContext("api = (path, opts) => __api(path, opts);", ctx);
const paint = async (server) => {
  vm.runInContext("currentServer = " + JSON.stringify(server) + ";", ctx);
  await vm.runInContext("renderVerification()", ctx);
  await flush();
};
const away = () => { vm.runInContext("clear(VIEW()); VIEW().append(el('div', { text: 'Events' }));", ctx); };
const box = () => { const r = find(screen, "vfy-results"); return r ? r.textContent : null; };
const btn = () => { const b = find(screen, "vfy-run"); return b ? { disabled: !!b.disabled, text: b.textContent } : null; };
const flashed = () => !!find(screen, "vfy-flash");
const click = () => { find(screen, "vfy-mode").value = "recover-inputs"; return find(screen, "vfy-run").onclick(); };
const running = (n) => ({ state: "running", since: "2026-09-22 10:00:00", mode: "recover-inputs", results: Array.from({ length: n }, (_, i) => ({ schema: "s", table: "t" + i, status: "match" })), summary: { match: n } });
const finished = { state: "succeeded", since: "2026-09-22 10:00:00", finished_at: "2026-09-22 10:01:00", verdict: "verified", mode: "recover-inputs",
  results: [{ schema: "s", table: "t0", status: "match" }], summary: { match: 1 } };
const newerMismatch = { state: "succeeded", since: "2026-09-22 11:00:00", finished_at: "2026-09-22 11:02:00", verdict: "mismatch", mode: "recover-inputs",
  results: [{ schema: "s", table: "hidden_orders", status: "mismatch" }], summary: { mismatch: 1 } };
const reset = (caps) => {
  vm.runInContext("capsCache = " + JSON.stringify(caps || { monitor: true, verify_trigger: true, verify: true }) + "; capsKnown = true;", ctx);
  vm.runInContext("if (typeof vfyLive !== 'undefined') { vfyLive.clear(); vfyFollowing.clear(); vfyAnnounce.clear(); vfyView = null; }", ctx);
  for (const k of Object.keys(queues)) delete queues[k];
  for (const k of Object.keys(gets)) delete gets[k];
  for (const k of Object.keys(forbid)) delete forbid[k];
  for (const k of Object.keys(getConflict)) delete getConflict[k];
  post409Msg = "a verify run is already in progress for this server";
  toasts.length = 0; posts = 0; postGate = null; post409 = false; holdNextGet = null; timers = [];
};
const out = {};
(async () => {
  // Start a run, leave, come back while it runs, let it end.
  reset();
  queues.a = [{ state: "idle" }, running(1), running(2), finished];
  await paint("a");
  const oldBox = find(screen, "vfy-results");
  const clicked = click();
  await flush();
  const startedBtn = btn();
  away();
  await paint("a");
  const backBox = box(), backBtn = btn();
  await drain();
  await clicked;
  out.clickThenReturn = { startedBtn, backBox, backBtn, endBox: box(), endBtn: btn(), flashed: flashed(), oldBoxText: oldBox.textContent, toasts: [...toasts], posts };

  // The click's start is still on its way when the page repaints and finds
  // the run going: one loop follows it, not two.
  reset();
  queues.a = [{ state: "idle" }, running(1), running(1), running(1), finished];
  await paint("a");
  let release;
  postGate = { p: new Promise((r) => { release = r; }) };
  const raced = click();
  await flush();
  away();
  await paint("a");
  release();
  await flush();
  await drain();
  await raced;
  out.race = { endBox: box(), endBtn: btn(), toasts: [...toasts] };

  // A run the schedule started: the page finds it and follows it.
  reset();
  queues.a = [running(1), running(1), finished];
  await paint("a");
  const schedBox = box(), schedBtn = btn();
  await drain();
  out.scheduled = { schedBox, schedBtn, endBox: box(), endBtn: btn(), flashed: flashed(), toasts: [...toasts] };

  // Nothing running: one status request, no loop.
  reset();
  await paint("a");
  await drain();
  out.idle = { box: box(), btn: btn(), gets: gets.a || 0, toasts: [...toasts] };

  // A run going on a while the page shows b, then back to a.
  reset();
  queues.a = [running(1), running(1), running(1), finished];
  queues.b = [{ state: "idle" }];
  await paint("a");
  const aBox = box();
  away();
  await paint("b");
  const bBox = box(), bBtn = btn();
  await tick();
  const bLater = box(), bLaterBtn = btn();
  await drain();
  const bEnd = box();
  away();
  await paint("a");
  out.switchServers = { aBox, bBox, bBtn, bLater, bLaterBtn, bEnd, aEnd: box(), aBtn: btn(), toasts: [...toasts] };

  // A run ends green; while the page is away a newer run ends with a
  // mismatch. Coming back shows the newer one, never the older green.
  reset();
  queues.a = [{ state: "idle" }, running(1), finished];
  await paint("a");
  const first = click();
  await flush();
  await drain();
  await first;
  const greenBox = box();
  away();
  queues.a = [newerMismatch];
  await paint("a");
  out.newerWhileAway = { greenBox, backBox: box(), toasts: [...toasts] };

  // Signing out drops what the previous session read: the next session,
  // refused the status by its profile, must not be drawn the old run.
  reset();
  queues.a = [{ state: "idle" }, running(1), finished];
  await paint("a");
  const own = click();
  await flush();
  await drain();
  await own;
  ctx.applyAuthGate = () => {}; // the sign-in screen is not this test's subject
  vm.runInContext("clearAuthState()", ctx);
  vm.runInContext("capsCache = { monitor: true, verify_trigger: true, verify: true }; capsKnown = true;", ctx);
  // Read the box before the server answers: a leak that the answer later
  // erases was on screen all the same.
  let releaseSignOut;
  holdNextGet = new Promise((r) => { releaseSignOut = r; });
  away();
  await paint("a");
  out.signOut = { box: box() };
  releaseSignOut();
  await flush();

  // The same, with the refusal only: what the page held goes too.
  reset();
  queues.a = [{ state: "idle" }, running(1), finished];
  await paint("a");
  const held = click();
  await flush();
  await drain();
  await held;
  forbid.a = true;
  away();
  await paint("a");
  out.refused = { box: box() };

  // A click while a run is already going (the schedule started it): the
  // server answers 409; the page shows that run and says when it ends.
  reset();
  queues.a = [{ state: "idle" }, running(1), running(1), finished];
  await paint("a");
  post409 = true;
  const second = click();
  await flush();
  const runningBox = box(), runningBtn = btn();
  await drain();
  await second;
  out.alreadyRunning = { runningBox, runningBtn, endBox: box(), endBtn: btn(), toasts: [...toasts] };

  // A run longer than the poll's ~20-minute cap, watched on screen: the page
  // follows on, so the end still lands and the button comes back.
  reset();
  queues.a = [{ state: "idle" }, ...Array.from({ length: 640 }, () => running(1)), finished];
  await paint("a");
  const long = click();
  await flush();
  await drain(700);
  await long;
  out.pastTheCap = { endBox: box(), endBtn: btn(), toasts: [...toasts] };

  // A probe whose answer lands after the run ended must not bring RUNNING
  // back or start a second loop.
  reset();
  queues.a = [{ state: "idle" }, running(1), running(1), finished];
  await paint("a");
  const slow = click();
  await flush();
  let releaseProbe;
  holdNextGet = new Promise((r) => { releaseProbe = r; });
  away();
  await paint("a");
  await drain();
  await slow;
  releaseProbe();
  await flush();
  const afterLate = box(), loopsAfterLate = vm.runInContext("vfyFollowing.size", ctx);
  await drain();
  out.lateProbe = { afterLate, loopsAfterLate, box: box(), btn: btn(), toasts: [...toasts] };

  // Changing the mode while a run goes does not offer another run.
  reset();
  queues.a = [{ state: "idle" }, running(1), running(1), finished];
  await paint("a");
  const moded = click();
  await flush();
  const sel = find(screen, "vfy-mode");
  sel.value = "baseline-anchored";
  sel.onchange();
  const modeBtn = btn();
  await drain();
  await moded;
  out.modeChange = { modeBtn };

  // A status in flight when the session signs out lands after the clear:
  // it must not write the old session's run back for the next one.
  reset();
  queues.a = [{ state: "idle" }, finished];
  await paint("a");
  const inflight = click();
  await flush();
  let releaseInflight;
  holdNextGet = new Promise((r) => { releaseInflight = r; });
  await tick();
  ctx.applyAuthGate = () => {};
  vm.runInContext("clearAuthState()", ctx);
  vm.runInContext("capsCache = { monitor: true, verify_trigger: true, verify: true }; capsKnown = true;", ctx);
  releaseInflight();
  await flush();
  await drain();
  await inflight;
  const heldAfter = vm.runInContext("vfyLive.size", ctx), loopsAfter = vm.runInContext("vfyFollowing.size", ctx);
  let releaseNext;
  holdNextGet = new Promise((r) => { releaseNext = r; });
  away();
  await paint("a");
  out.inflightSignOut = { heldAfter, loopsAfter, box: box(), toasts: [...toasts] };
  releaseNext();
  await flush();

  // The same for a paint's probe in flight at sign-out, and for a click
  // whose start is in flight at sign-out.
  reset();
  queues.a = [running(1)];
  let releaseProbeOut;
  holdNextGet = new Promise((r) => { releaseProbeOut = r; });
  await paint("a");
  ctx.applyAuthGate = () => {};
  vm.runInContext("clearAuthState()", ctx);
  vm.runInContext("capsCache = { monitor: true, verify_trigger: true, verify: true }; capsKnown = true;", ctx);
  releaseProbeOut();
  await flush();
  const probeHeld = vm.runInContext("vfyLive.size", ctx), probeLoops = vm.runInContext("vfyFollowing.size", ctx);
  timers = [];
  reset();
  queues.a = [{ state: "idle" }, running(1), finished];
  await paint("a");
  let releasePost;
  postGate = { p: new Promise((r) => { releasePost = r; }) };
  const clickOut = click();
  await flush();
  vm.runInContext("clearAuthState()", ctx);
  vm.runInContext("capsCache = { monitor: true, verify_trigger: true, verify: true }; capsKnown = true;", ctx);
  releasePost();
  await flush();
  await drain();
  await clickOut;
  out.otherInflight = { probeHeld, probeLoops, postHeld: vm.runInContext("vfyLive.size", ctx), postAnnounced: vm.runInContext("vfyAnnounce.has('a')", ctx), toasts: [...toasts] };

  // A 409 that is not a run going: the command-line server refuses monitor
  // verbs, the status too. The page says the server's reason and claims no run.
  reset();
  post409Msg = "the command-line server is already streamed by this process; monitor verbs apply to registry servers";
  post409 = true;
  getConflict.a = true;
  await paint("a");
  const cli = click();
  await flush();
  await drain();
  await cli;
  out.cliServer = { box: box(), btn: btn(), toasts: [...toasts], announced: vm.runInContext("vfyAnnounce.has('a')", ctx) };

  // A 409 whose run ended before the page asked: nothing is going, so the
  // page claims nothing and marks nothing as its own.
  reset();
  post409 = true;
  queues.a = [{ state: "idle" }, finished];
  await paint("a");
  const gone = click();
  await flush();
  await drain();
  await gone;
  out.runGone = { toasts: [...toasts], announced: vm.runInContext("vfyAnnounce.has('a')", ctx) };

  // The feature off: the page says so and asks nothing.
  reset({ monitor: true, verify_trigger: false });
  await paint("a");
  await drain();
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
// run the schedule started showed only in History, once it had ended.
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
			Flashed                     bool
			Toasts                      []string
			Posts                       int
		}
		Race struct {
			EndBox string
			EndBtn *button
			Toasts []string
		}
		Scheduled struct {
			SchedBox, EndBox string
			SchedBtn, EndBtn *button
			Flashed          bool
			Toasts           []string
		}
		Idle struct {
			Box    string
			Btn    *button
			Gets   int
			Toasts []string
		}
		SwitchServers struct {
			ABox, BBox, BLater, BEnd, AEnd string
			BBtn, BLaterBtn, ABtn          *button
			Toasts                         []string
		}
		NewerWhileAway struct {
			GreenBox, BackBox string
			Toasts            []string
		}
		SignOut        struct{ Box string }
		Refused        struct{ Box string }
		AlreadyRunning struct {
			RunningBox, EndBox string
			RunningBtn, EndBtn *button
			Toasts             []string
		}
		PastTheCap struct {
			EndBox string
			EndBtn *button
			Toasts []string
		}
		LateProbe struct {
			AfterLate, Box string
			LoopsAfterLate int
			Btn            *button
			Toasts         []string
		}
		ModeChange      struct{ ModeBtn *button }
		InflightSignOut struct {
			HeldAfter, LoopsAfter int
			Box                   string
			Toasts                []string
		}
		OtherInflight struct {
			ProbeHeld, ProbeLoops, PostHeld int
			PostAnnounced                   bool
			Toasts                          []string
		}
		CliServer struct {
			Box       string
			Btn       *button
			Toasts    []string
			Announced bool
		}
		RunGone struct {
			Toasts    []string
			Announced bool
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
	empty := func(s string) bool { return strings.Contains(s, "No run yet") }

	c := got.ClickThenReturn
	if !busy(c.StartedBtn) {
		t.Errorf("click: the button on screen reads %+v, want disabled and Running…", c.StartedBtn)
	}
	if !strings.Contains(c.BackBox, "RUNNING") || !busy(c.BackBtn) {
		t.Errorf("click, leave, return: box %q, button %+v; want the run in progress and the button busy", c.BackBox, c.BackBtn)
	}
	if !strings.Contains(c.EndBox, "DONE") || !c.Flashed || !ready(c.EndBtn) {
		t.Errorf("click, leave, return, run ends: box %q, highlighted %v, button %+v; want the finished run highlighted and the button on screen ready",
			c.EndBox, c.Flashed, c.EndBtn)
	}
	if strings.Contains(c.OldBoxText, "DONE") {
		t.Errorf("the box left behind kept receiving the run: %q", c.OldBoxText)
	}
	if n := finishToasts(c.Toasts); n != 1 || c.Posts != 1 {
		t.Errorf("click then return: %d finish toasts and %d starts, want 1 and 1: %q", n, c.Posts, c.Toasts)
	}

	r := got.Race
	if n := finishToasts(r.Toasts); n != 1 || !strings.Contains(r.EndBox, "DONE") || !ready(r.EndBtn) {
		t.Errorf("a repaint that finds the run while the click's start is on its way: %d finish toasts (want 1; two loops say it twice), box %q, button %+v",
			n, r.EndBox, r.EndBtn)
	}

	// A run this tab did not start is shown on the page, and its end pops no
	// message on whatever page the operator is on (only runs started here
	// do, as before).
	s := got.Scheduled
	if !strings.Contains(s.SchedBox, "RUNNING") || !busy(s.SchedBtn) {
		t.Errorf("a run the schedule started: box %q, button %+v; want it shown in progress and the button busy", s.SchedBox, s.SchedBtn)
	}
	if !strings.Contains(s.EndBox, "DONE") || !s.Flashed || !ready(s.EndBtn) || len(s.Toasts) != 0 {
		t.Errorf("the scheduled run ends: box %q, highlighted %v, button %+v, toasts %q; want it finished and highlighted, the button ready, no message",
			s.EndBox, s.Flashed, s.EndBtn, s.Toasts)
	}

	i := got.Idle
	if !empty(i.Box) || !ready(i.Btn) || i.Gets != 1 || len(i.Toasts) != 0 {
		t.Errorf("nothing running: box %q, button %+v, %d status requests, toasts %q; want No run yet, ready, exactly 1, none",
			i.Box, i.Btn, i.Gets, i.Toasts)
	}

	w := got.SwitchServers
	if !strings.Contains(w.ABox, "RUNNING") {
		t.Errorf("server a with a run: box %q, want it in progress", w.ABox)
	}
	if !empty(w.BBox) || !ready(w.BBtn) || !empty(w.BLater) || !ready(w.BLaterBtn) || !empty(w.BEnd) {
		t.Errorf("server b while a's run goes and ends: box %q, then %q, then %q, button %+v then %+v; want b's own empty box and a ready button throughout",
			w.BBox, w.BLater, w.BEnd, w.BBtn, w.BLaterBtn)
	}
	if !strings.Contains(w.AEnd, "DONE") || !ready(w.ABtn) || len(w.Toasts) != 0 {
		t.Errorf("back on a after its run ended: box %q, button %+v, toasts %q; want it finished, ready, no message (this tab did not start it)", w.AEnd, w.ABtn, w.Toasts)
	}

	nw := got.NewerWhileAway
	if !strings.Contains(nw.GreenBox, "DONE") || !strings.Contains(nw.BackBox, "MISMATCH") || strings.Contains(nw.BackBox, "DONE") {
		t.Errorf("a newer run ended with a mismatch while the page was away: box before %q, after %q; want the older green run replaced by the mismatch",
			nw.GreenBox, nw.BackBox)
	}

	if strings.Contains(got.SignOut.Box, "DONE") || strings.Contains(got.SignOut.Box, "match") || !empty(got.SignOut.Box) {
		t.Errorf("after sign-out, a session the server refuses the status: box %q; want No run yet, never the previous session's run", got.SignOut.Box)
	}
	if !empty(got.Refused.Box) {
		t.Errorf("the server refuses this session the status: box %q; want the held run dropped (No run yet)", got.Refused.Box)
	}

	ar := got.AlreadyRunning
	arErr := false
	for _, m := range ar.Toasts {
		arErr = arErr || strings.HasPrefix(m, "ERR ")
	}
	if !strings.Contains(ar.RunningBox, "RUNNING") || !busy(ar.RunningBtn) || !strings.Contains(ar.EndBox, "DONE") || !ready(ar.EndBtn) ||
		arErr || finishToasts(ar.Toasts) != 1 {
		t.Errorf("a click while a run is already going: box %q then %q, button %+v then %+v, toasts %q; "+
			"want that run shown and followed, no error, one message at its end", ar.RunningBox, ar.EndBox, ar.RunningBtn, ar.EndBtn, ar.Toasts)
	}

	pc := got.PastTheCap
	if !strings.Contains(pc.EndBox, "DONE") || !ready(pc.EndBtn) || finishToasts(pc.Toasts) != 1 {
		t.Errorf("a run past the poll's cap, watched on screen: box %q, button %+v, toasts %q; want it followed to its end, the button ready, one message",
			pc.EndBox, pc.EndBtn, pc.Toasts)
	}

	lp := got.LateProbe
	if !strings.Contains(lp.AfterLate, "DONE") || lp.LoopsAfterLate != 0 {
		t.Errorf("the moment a late probe answer lands: box %q, %d loops; want the finished run kept and no loop started", lp.AfterLate, lp.LoopsAfterLate)
	}
	if !strings.Contains(lp.Box, "DONE") || !ready(lp.Btn) || finishToasts(lp.Toasts) != 1 {
		t.Errorf("a probe answer that lands after the run ended: box %q, button %+v, toasts %q; want the finished run kept, the button ready, one message",
			lp.Box, lp.Btn, lp.Toasts)
	}

	is := got.InflightSignOut
	if is.HeldAfter != 0 || is.LoopsAfter != 0 || !empty(is.Box) || finishToasts(is.Toasts) != 0 {
		t.Errorf("a status in flight at sign-out: %d runs held, %d loops, box %q, toasts %q; want nothing of the old session kept, drawn or announced",
			is.HeldAfter, is.LoopsAfter, is.Box, is.Toasts)
	}

	oi := got.OtherInflight
	if oi.ProbeHeld != 0 || oi.ProbeLoops != 0 || oi.PostHeld != 0 || oi.PostAnnounced || finishToasts(oi.Toasts) != 0 {
		t.Errorf("a probe or a start in flight at sign-out: probe left %d runs and %d loops, start left %d runs, marked %v, toasts %q; want nothing",
			oi.ProbeHeld, oi.ProbeLoops, oi.PostHeld, oi.PostAnnounced, oi.Toasts)
	}

	claimed := func(ts []string) bool {
		for _, m := range ts {
			if strings.Contains(m, "already running on this server") {
				return true
			}
		}
		return false
	}
	cs := got.CliServer
	said := false
	for _, m := range cs.Toasts {
		said = said || (strings.HasPrefix(m, "ERR ") && strings.Contains(m, "command-line server is already streamed"))
	}
	if !said || claimed(cs.Toasts) || cs.Announced || !empty(cs.Box) || !ready(cs.Btn) {
		t.Errorf("a 409 from the command-line server: toasts %q, marked as this tab's run %v, box %q, button %+v; "+
			"want the server's own reason, no claim of a run, nothing marked, the button ready", cs.Toasts, cs.Announced, cs.Box, cs.Btn)
	}
	if claimed(got.RunGone.Toasts) || got.RunGone.Announced {
		t.Errorf("a 409 whose run ended before the page asked: toasts %q, marked %v; want no claim of a run and nothing marked",
			got.RunGone.Toasts, got.RunGone.Announced)
	}

	if !busy(got.ModeChange.ModeBtn) {
		t.Errorf("changing the mode while a run goes: button %+v, want it still busy", got.ModeChange.ModeBtn)
	}

	if got.Off.Gets != 0 {
		t.Errorf("verification turned off: %d status requests, want none (the server refuses every one)", got.Off.Gets)
	}
}
