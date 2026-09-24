package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// overviewLiveHarnessJS drives the real Overview over a fake screen that knows
// what is attached to it, a fake clock (the page's timers and Date.now() move
// only when the test advances them), and a fake server per selected server id
// whose answers the test changes as "changes" land. It records every request
// with the server it was sent for.
const overviewLiveHarnessJS = `
const screen = new FakeEl("main");
const attach = (parent, x) => { if (x && typeof x === "object") x.__parent = parent; };
const append = FakeEl.prototype.append;
FakeEl.prototype.append = function (...k) { for (const x of k) attach(this, x); return append.apply(this, k); };
const replaceChildren = FakeEl.prototype.replaceChildren;
FakeEl.prototype.replaceChildren = function (...k) { for (const c of this.children) attach(null, c); for (const x of k) attach(this, x); return replaceChildren.apply(this, k); };
FakeEl.prototype.replaceWith = function (n) { const p = this.__parent; if (!p) return; const i = p.children.indexOf(this); if (i >= 0) { p.children[i] = n; attach(p, n); } this.__parent = null; };
FakeEl.prototype.focus = function () { document.activeElement = this; };
Object.defineProperty(FakeEl.prototype, "isConnected", { get() { for (let n = this; n; n = n.__parent) if (n === screen) return true; return false; } });
document.getElementById = (id) => (id === "view" ? screen : new FakeEl("div"));
document.importNode = (n) => n;
// The page logs failed requests; the test reads them from the page instead,
// and stdout carries only the result.
ctx.console = Object.assign(Object.create(console), { error() {}, warn() {} });
const find = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push(n);
  for (const c of n.children) find(c, cls, out); return out; };
const text = (n) => (n ? n.textContent : "");

// The clock: timers are due at now + ms, and advance(ms) runs what falls due.
let now = 1000000;
class FakeDate extends Date { static now() { return now; } }
ctx.Date = FakeDate;
let timers = [];
ctx.setTimeout = (fn, ms) => { timers.push({ due: now + (ms || 0), fn, ms: ms || 0 }); return timers.length; };
const flush = async () => { for (let i = 0; i < 40; i++) await new Promise((r) => setImmediate(r)); };
const advance = async (ms) => {
  const end = now + ms;
  for (;;) {
    timers.sort((a, b) => a.due - b.due);
    const t = timers[0];
    if (!t || t.due > end) break;
    timers.shift();
    now = t.due;
    t.fn();
    await flush();
  }
  now = end;
  await flush();
};

// One fake server per id. The answer is computed when the request arrives
// (what that server held at that moment) and delivered when its hold, if
// any, is released.
const servers = {};
const reqs = [];
// Rebuilt-panel counting: every Activity row carries an identity, so the test
// can tell a redraw from an untouched panel.
ctx.__stampN = 0;
const tableRows = () => find(screen, "ov-tablerow").map((r) => r.__stamp);
const ev = (id, type, pk) => ({ event_id: id, anchor: "2026-09-22T11:00:00." + id + "Z|" + id, event_timestamp: "2026-09-22 11:00:0" + (id % 10),
  schema_name: "shop", table_name: "orders", event_type: type, pk_values: String(pk), changed_columns: type === "UPDATE" ? ["status"] : [] });
const fresh = () => ({ events: [], firstRun: null, coverage: { continuity: "ok", freshness: "idle" }, fail: {}, hold: {}, activityShift: false });
const fail = (status, msg) => Object.assign(new Error(msg || ("HTTP " + status)), { status });
ctx.__api = async (path, opts) => {
  const server = vm.runInContext("currentServer", ctx);
  reqs.push({ server, path, at: now });
  const st = servers[server] || (servers[server] = fresh());
  const key = path.split("?")[0];
  // A queue of failures; the last one repeats, like a fault that has not
  // been fixed.
  const f = st.fail[key];
  if (f && f.length) throw f.length > 1 ? f.shift() : f[0];
  let answer;
  if (key === "/api/events/head") answer = { newest_event_id: st.events.length ? st.events[st.events.length - 1].event_id : 0 };
  else if (key === "/api/events") answer = { events: st.events.filter((e) => !(st.withheld && e.event_id === 99)).reverse().slice(0, 8) };
  else if (key === "/api/status") answer = { total_events_estimate: st.events.length, coverage: {} };
  else if (key === "/api/coverage") answer = st.coverage;
  else if (key === "/api/activity") answer = { deletes: st.events.filter((e) => e.event_type === "DELETE").length, tables: st.events.length ? 1 : 0,
    // Identical bytes unless the test moves them: the real aggregate is a
    // server-side cache with a 30-minute life, so a busy server sends the
    // same payload for many turns.
    top_tables: st.events.length ? [{ schema: "shop", table: st.activityShift ? "customers" : "orders", insert: 1, update: 0, delete: 0, total: 1 }] : [],
    label: "live retention", complete: true, refreshed_at: "2026-09-22 11:00:00", since: "2026-09-22 10:00:00", until: "2026-09-22 11:00:00" };
  else if (/\/first-run$/.test(key)) { const q = st.firstRun || []; answer = q.length > 1 ? q.shift() : q[0]; if (answer instanceof Error) throw answer; }
  else answer = {};
  const h = st.hold[key];
  if (h) {
    // A hold that honours the caller's deadline, like a real request that
    // hangs: it ends only when the test releases it or the signal aborts.
    await new Promise((resolve, reject) => {
      h.then(resolve);
      const sig = opts && opts.signal;
      if (!sig) return;
      if (sig.aborted) return reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
      sig.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
    });
  }
  return JSON.parse(JSON.stringify(answer || {}));
};
vm.runInContext("api = (path, opts) => __api(path, opts); renderRoute = () => { __rendered++; };", ctx);
// Each Activity row gets an identity, so the test can tell a panel that was
// left alone from one torn down and rebuilt with the same bytes.
vm.runInContext("ovTableRow = ((orig) => (s, w) => { const n = orig(s, w); n.__stamp = ++__stampN; return n; })(ovTableRow);", ctx);
ctx.__rendered = 0;
// A request that hangs until the test releases it, for every turn that asks,
// which is what a locked table or a host that went away looks like.
const hold = (server, key) => {
  let release;
  const p = new Promise((r) => { release = r; });
  servers[server].hold[key] = p;
  return () => { delete servers[server].hold[key]; release({}); };
};
const count = (server, key) => reqs.filter((r) => r.server === server && r.path.split("?")[0] === key).length;
const rows = () => find(screen, "ov-ev").map((r) => text(r));
const note = () => find(screen, "ov-live-note").map(text).join(" | ");
const card = () => find(screen, "fr-card").map(text).join(" | ");
const asOfStamps = () => find(screen, "ov-panel-head").map((h) => find(h, "cov-asof").length);
// The tab being shown again, as the browser announces it.
const shown = () => vm.runInContext("typeof ovVisibilityChanged === 'function' && ovVisibilityChanged()", ctx);
const paint = async (server) => {
  servers[server] = servers[server] || fresh();
  vm.runInContext("currentServer = " + JSON.stringify(server) + "; serverGen++; renderOverview();", ctx);
  await flush();
};
const reset = () => {
  for (const k of Object.keys(servers)) delete servers[k];
  reqs.length = 0; timers = []; ctx.__rendered = 0; document.hidden = false; document.activeElement = null;
  vm.runInContext("capsCache = { monitor: true }; capsKnown = true; serversEmpty = false;", ctx);
};
const out = {};
`

// TestOverviewKeepsItselfCurrent is #1801. On main the Overview checked for
// news only while the Getting started list showed: the first INSERT appeared
// after 5.3 s, and the UPDATE and DELETE that followed did not appear within
// 120 s each, because the list ended at the first change and nothing else
// asked again. Each scenario below is one of the issue's edge cases.
func TestOverviewKeepsItselfCurrent(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	yes := true
	marshal := func(in firstRunInput) string {
		b, err := json.Marshal(firstRunSteps(in))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	running := firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true}, IndexExists: &yes, SnapshotTaken: true, StreamStarted: true,
		Backup: &BaselineStatus{State: "idle"}}
	waiting := marshal(running)
	withChange := running
	withChange.EventsIndexed = 1
	changed := marshal(withChange)
	withSnapshot := withChange
	withSnapshot.SnapshotExists = true
	snapshot := marshal(withSnapshot)
	snapshotOnly := running
	snapshotOnly.SnapshotExists = true
	snapshotNoChange := marshal(snapshotOnly)
	withRun := withChange
	withRun.Backup = &BaselineStatus{State: "running"}
	backupRunning := marshal(withRun)

	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + overviewLiveHarnessJS + `
const WAITING = ` + waiting + `, CHANGED = ` + changed + `, SNAPSHOT = ` + snapshot + `, SNAPSHOT_NO_CHANGE = ` + snapshotNoChange + `,
  BACKUP_RUNNING = ` + backupRunning + `;
(async () => {
  // The walk: a first change, then an UPDATE and a DELETE, each on screen
  // after one wait of the Overview's own clock and no reload.
  reset();
  servers.a = fresh();
  servers.a.firstRun = [WAITING];
  await paint("a");
  const w = { before: rows(), card: card(), waits: [] };
  const steps = [[ev(1, "INSERT", 5)], [ev(2, "UPDATE", 2)], [ev(3, "DELETE", 3)]];
  for (const s of steps) {
    servers.a.events.push(...s);
    servers.a.firstRun = [CHANGED];
    const t0 = now;
    let n = 0;
    while (rows().length < servers.a.events.length && n < 50) { await advance(500); n++; }
    w.waits.push(now - t0);
  }
  w.rows = rows();
  w.cardAfterChanges = card();
  w.rendered = ctx.__rendered;
  w.eventsReads = count("a", "/api/events");
  // Idle: the Overview keeps asking whether anything changed, and reads the
  // list again only when something did.
  const heads0 = count("a", "/api/events/head"), reads0 = count("a", "/api/events");
  await advance(60000);
  w.idleHeads = count("a", "/api/events/head") - heads0;
  w.idleReads = count("a", "/api/events") - reads0;
  // The list now waits only for a backup, so it asks on its own long wait
  // rather than on every change (a change cannot produce a backup). The
  // change itself still shows at once.
  servers.a.events.push(ev(4, "UPDATE", 4));
  servers.a.firstRun = [BACKUP_RUNNING];
  let n3 = 0;
  while (rows().length < 4 && n3 < 50) { await advance(500); n3++; }
  w.rowsBeforeList = rows().length;
  await advance(130000);
  w.cardWithChange = card();
  w.asOfStamps = asOfStamps();
  // A snapshot lands: the list leaves, the page is not repainted.
  servers.a.firstRun = [SNAPSHOT];
  await advance(130000);
  w.cardAfterSnapshot = card();
  w.renderedAfterSnapshot = ctx.__rendered;
  // And the Overview still updates after the list has gone.
  servers.a.events.push(ev(5, "INSERT", 9));
  const t4 = now;
  let n4 = 0;
  while (rows().length < 5 && n4 < 50) { await advance(500); n4++; }
  w.afterListWait = now - t4;
  out.walk = w;

  // A list waiting only for a backup nobody is running asks rarely: each ask
  // reads the server's backup locations, over the network for an S3 one. It
  // keeps asking, slowly, because a backup can appear from the schedule or the
  // command line; a change landing and the tab coming back ask at once, and a
  // backup this console is running is followed at full rate.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  servers.a.firstRun = [CHANGED];
  await paint("a");
  await advance(60000);
  const settledAsks0 = count("a", "/api/servers/a/first-run");
  await advance(300000);
  const settledAsks = count("a", "/api/servers/a/first-run") - settledAsks0;
  servers.a.events.push(ev(2, "UPDATE", 2));
  await advance(5000);
  const afterChange = count("a", "/api/servers/a/first-run") - settledAsks0 - settledAsks;
  // The tab coming back is a person returning: it asks at once, and a
  // handful of shows in a row do not multiply the asks (arm() replaces the
  // pending wait rather than adding one).
  const wakeFrom = reqs.length;
  for (let i = 0; i < 5; i++) shown();
  await advance(3000);
  const afterWake = reqs.slice(wakeFrom).filter((r) => /first-run$/.test(r.path)).length;
  const wakeTrace = reqs.slice(wakeFrom).map((r) => r.path.replace("/api/servers/a/", "") + "@" + r.at);
  // A backup this console started: the list follows it to the end.
  servers.a.firstRun = [BACKUP_RUNNING, BACKUP_RUNNING, SNAPSHOT];
  vm.runInContext("ovLive && ovLive.wake()", ctx);
  await flush();
  const runningAt = count("a", "/api/servers/a/first-run");
  // One minute: a settled list asks at most once in it, so following the run
  // to its end can only come from the full-rate wait.
  await advance(60000);
  out.settles = { settledAsks, afterChange, afterWake, wakeTrace, whileRunning: count("a", "/api/servers/a/first-run") - runningAt, cardAtEnd: card() };

  // A backup that appears from somewhere this page cannot see (the schedule,
  // the command line) while nothing else happens: the list still notices.
  reset();
  servers.z = fresh();
  servers.z.events.push(ev(1, "INSERT", 1));
  servers.z.firstRun = [CHANGED];
  await paint("z");
  await advance(60000);
  servers.z.firstRun = [SNAPSHOT];
  await advance(150000);
  out.outsideBackup = { card: card(), asks: count("z", "/api/servers/z/first-run") };

  // Another page replaces the Overview: its loops stop asking.
  reset();
  servers.a = fresh();
  servers.a.firstRun = [WAITING];
  await paint("a");
  await advance(5000);
  vm.runInContext("clear(VIEW()); VIEW().append(el('div', { text: 'Events' }));", ctx);
  const awayAt = reqs.length;
  await advance(60000);
  const awayReqs = reqs.length - awayAt;
  // Signing out stops them too, with the page still under the sign-in form.
  reset();
  servers.a = fresh();
  servers.a.firstRun = [WAITING];
  await paint("a");
  await advance(5000);
  ctx.applyAuthGate = () => {};
  vm.runInContext("clearAuthState(); capsCache = { monitor: true };", ctx);
  const outAt = reqs.length;
  await advance(60000);
  out.stops = { away: awayReqs, signOut: reqs.length - outAt };

  // A server with a backup and no change yet: the backup ends the list, the
  // step left on it being one nobody has to do anything about (a change on
  // the source arrives or it does not). What has to keep working is the page
  // underneath: on main the list leaving was the end of every refresh.
  reset();
  servers.s = fresh();
  servers.s.firstRun = [SNAPSHOT_NO_CHANGE];
  await paint("s");
  const s = { card: card(), rows: rows().length };
  servers.s.events.push(ev(1, "INSERT", 1));
  servers.s.firstRun = [SNAPSHOT];
  await advance(5000);
  s.after = rows();
  s.cardAfter = card();
  out.snapshotNoChange = s;

  // A server switch while a refresh is in flight: the answer for the server
  // left behind is dropped, and that server's loop stops.
  reset();
  servers.a = fresh();
  servers.b = fresh();
  await paint("a");
  servers.a.events.push(ev(7, "DELETE", 70));
  const releaseA = hold("a", "/api/events");
  await advance(5000);
  const inflight = count("a", "/api/events");
  servers.b.events.push(ev(8, "INSERT", 80));
  await paint("b");
  const bRows = rows();
  const beforeRelease = reqs.length;
  releaseA();
  await flush();
  const sentOnRelease = reqs.slice(beforeRelease).map((r) => r.server + " " + r.path);
  const afterRelease = rows();
  const aAsks = count("a", "/api/events/head");
  await advance(30000);
  out.switchMidRefresh = { inflight, bRows, afterRelease, sentOnRelease, aAsksLater: count("a", "/api/events/head") - aAsks, bAsks: count("b", "/api/events/head") };

  // A hidden tab asks nothing, neither for changes nor for the list, and a
  // tab shown again asks at once.
  reset();
  servers.a = fresh();
  servers.a.firstRun = [WAITING];
  await paint("a");
  await advance(5000);
  const before = reqs.length;
  document.hidden = true;
  await advance(120000);
  const hiddenReqs = reqs.length - before;
  servers.a.events.push(ev(9, "UPDATE", 90));
  document.hidden = false;
  const shownAt = now;
  shown();
  await flush();
  out.hidden = { hiddenReqs, rowsOnShow: rows(), askedAt: reqs.filter((r) => r.path === "/api/events/head" && r.at === shownAt).length,
    listAskedAt: reqs.filter((r) => /first-run$/.test(r.path) && r.at === shownAt).length };
  // Two quick shows do not start a second loop.
  const hq = count("a", "/api/events/head");
  shown(); shown();
  await flush();
  out.hidden.quickShows = count("a", "/api/events/head") - hq;
  const h0 = count("a", "/api/events/head");
  await advance(5000);
  out.hidden.oneLoop = count("a", "/api/events/head") - h0;

  // A refresh fills the page in place: the heading is the same element, the
  // address is untouched, and a focused Undo keeps its focus when a newer
  // change lands above it.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 5));
  await paint("a");
  await advance(5000);
  const head = find(screen, "page-head")[0];
  const undo = find(screen, "ov-ev-undo")[0];
  undo.focus();
  const search = ctx.location.search;
  servers.a.events.push(ev(2, "UPDATE", 6));
  await advance(5000);
  const focused = document.activeElement;
  out.inPlace = { sameHead: find(screen, "page-head")[0] === head, rows: rows().length,
    focusedAnchor: focused && focused.getAttribute ? focused.getAttribute("data-anchor") : null, focusedOnScreen: !!(focused && focused.isConnected),
    search: ctx.location.search === search, rendered: ctx.__rendered };

  // A profile-restricted session: every refresh goes through the same
  // endpoints as the first paint, and a refusal stops the refresh and says so.
  reset();
  servers.p = fresh();
  servers.p.coverage = { continuity: "ok", freshness: "current", baseline_configured: true, delta_to: "2026-09-22 11:00:00", lag_seconds: 3 };
  await paint("p");
  const paintPaths = new Set(reqs.map((r) => r.path));
  for (let i = 1; i <= 30; i++) { servers.p.events.push(ev(i, "INSERT", i)); await advance(5000); }
  const refreshPaths = [...new Set(reqs.map((r) => r.path))].filter((p) => !paintPaths.has(p));
  const covReads = count("p", "/api/coverage");
  // A change in a table this session's profile withholds: the id moves, the
  // list the session may see does not, and the rows on screen stay as drawn.
  const firstRow = find(screen, "ov-ev")[0];
  servers.p.withheld = true;
  servers.p.events.push(ev(99, "DELETE", 99));
  await advance(5000);
  const withheld = { sameRow: find(screen, "ov-ev")[0] === firstRow, shows99: rows().some((r) => r.includes("#99")) };
  servers.p.fail["/api/events/head"] = [fail(403, "the data profile \"ghost\" is not defined on this server")];
  await advance(5000);
  const stoppedAt = reqs.length;
  await advance(60000);
  out.restricted = { refreshPaths, covReads, withheld, note: note(), asksAfterRefusal: reqs.length - stoppedAt };

  // A refresh that keeps failing says the page stopped updating, keeps
  // trying, and takes the note back once an answer comes.
  reset();
  servers.a = fresh();
  await paint("a");
  await advance(5000);
  servers.a.fail["/api/events/head"] = [fail(502), fail(502), fail(502), fail(502)];
  let n5 = 0;
  while (!note() && n5 < 100) { await advance(1000); n5++; }
  const failedNote = note();
  delete servers.a.fail["/api/events/head"];
  servers.a.events.push(ev(3, "INSERT", 3));
  await advance(60000);
  out.failing = { note: failedNote, afterRecovery: note(), rows: rows().length };

  // The first ask for the id fails: the list is drawn anyway, as the paint
  // always drew it. A refusal on both draws the reason in the panel and
  // says the page stopped updating.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  servers.a.fail["/api/events/head"] = [fail(502)];
  await paint("a");
  const firstFail = { rows: rows().length };
  reset();
  servers.d = fresh();
  servers.d.fail["/api/events/head"] = [fail(403, "denied by your role")];
  servers.d.fail["/api/events"] = [fail(403, "denied by your role")];
  await paint("d");
  firstFail.panel = find(screen, "error-box").map(text).join(" | ");
  firstFail.note = note();
  out.firstFail = firstFail;

  // A request that neither answers nor fails: the console sets no write
  // timeout, so without a deadline of its own the loop would sit in it
  // forever, never arming another turn, never counting a failure, and the
  // tab coming back would find it "busy" and do nothing. The page must give
  // up, say so, and carry on.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  await advance(5000);
  const hung = { rowsBefore: rows().length };
  const releaseHang = hold("a", "/api/events/head");
  servers.a.events.push(ev(2, "UPDATE", 2));
  await advance(5000);          // the turn that hangs
  hung.duringNote = note();
  hung.duringRows = rows().length;
  await advance(120000);        // two minutes of a real hang
  hung.note = note();
  hung.asks = count("a", "/api/events/head");
  // It recovered: the change that was waiting shows up.
  releaseHang();
  await flush();
  servers.a.events.push(ev(3, "DELETE", 3));
  let nh = 0;
  while (rows().length < 3 && nh < 60) { await advance(5000); nh++; }
  hung.rowsAfter = rows().length;
  hung.noteAfter = note();
  out.hang = hung;

  // A hang while the tab is away, then the tab comes back: the wake must not
  // find the loop stuck.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  await advance(5000);
  const releaseHang2 = hold("a", "/api/events/head");
  await advance(5000);
  document.hidden = true;
  await advance(30000);
  servers.a.events.push(ev(2, "UPDATE", 2));
  // The server comes back at the same moment the operator does: what is
  // under test is that the loop is not still sitting in the hung turn.
  releaseHang2();
  document.hidden = false;
  shown();
  await flush();
  let nw = 0;
  while (rows().length < 2 && nw < 60) { await advance(5000); nw++; }
  out.hangThenWake = { rows: rows().length, waits: nw };

  // The tiles' own reads failing: the figures stay, and the page says they
  // are from before rather than passing an hour-old count off as current.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  await advance(5000);
  // The three tiles the failing reads feed. The fourth ("most recent
  // change") comes from the events list, which is still arriving.
  const sideTiles = () => find(screen, "ov-stat").map(text).filter((t) => !t.includes("most recent change"));
  const tilesBefore = sideTiles();
  servers.a.fail["/api/status"] = [fail(500, "index went away"), fail(500, "index went away")];
  servers.a.fail["/api/activity"] = [fail(500, "index went away"), fail(500, "index went away")];
  for (let i = 2; i < 6; i++) { servers.a.events.push(ev(i, "INSERT", i)); await advance(5000); }
  await advance(120000);
  out.sideFailed = { note: find(screen, "ov-side-note").map(text).join(" | "), tiles: sideTiles(), tilesBefore };
  // And it goes when they answer again.
  delete servers.a.fail["/api/status"];
  delete servers.a.fail["/api/activity"];
  servers.a.events.push(ev(9, "UPDATE", 9));
  await advance(130000);
  out.sideFailed.after = find(screen, "ov-side-note").map(text).join(" | ");

  // A busy server whose list waits only for a backup: the change pokes do
  // not drag the first-run endpoint (and its backup-location listing) back
  // to the fast rate.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  servers.a.firstRun = [CHANGED];
  await paint("a");
  await advance(10000);
  const busyFrom = count("a", "/api/servers/a/first-run");
  for (let i = 2; i < 62; i++) { servers.a.events.push(ev(i, "INSERT", i)); await advance(5000); }
  out.busySettled = { asks: count("a", "/api/servers/a/first-run") - busyFrom, minutes: 5 };

  // An idle page: the expensive reads (a full index count, and a coverage
  // card that lists every backup location) are not run once a minute for a
  // server nothing is writing to.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  const idleFrom = { status: count("a", "/api/status"), cov: count("a", "/api/coverage") };
  await advance(1800000); // half an hour
  out.idleSlow = { status: count("a", "/api/status") - idleFrom.status, cov: count("a", "/api/coverage") - idleFrom.cov };

  // The Activity panel is not torn down and rebuilt for identical data: its
  // rows keep their identity while the aggregate does not change, and a
  // click mid-refresh cannot land on a row that was swapped underneath.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  await advance(5000);
  const stamps0 = tableRows();
  for (let i = 2; i < 8; i++) { servers.a.events.push(ev(i, "INSERT", i)); await advance(5000); }
  const stamps1 = tableRows();
  servers.a.activityShift = true;
  servers.a.events.push(ev(9, "DELETE", 9));
  await advance(5000);
  out.activity = { before: stamps0, same: stamps1, afterChange: tableRows() };

  // The list's own request hangs: its loop must give up and keep asking,
  // not sit in it for as long as the page is open.
  reset();
  servers.a = fresh();
  servers.a.firstRun = [WAITING];
  await paint("a");
  await advance(5000);
  const listHold = hold("a", "/api/servers/a/first-run");
  const listFrom = count("a", "/api/servers/a/first-run");
  await advance(180000);
  out.listHang = { asks: count("a", "/api/servers/a/first-run") - listFrom };
  listHold();
  await flush();
  servers.a.firstRun = [CHANGED];
  await advance(30000);
  out.listHang.cardAfter = card();

  // Only the all-time count's read fails: the page still says the figures
  // are from before, and takes it back when that read answers.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  await advance(5000);
  servers.a.fail["/api/status"] = [fail(500, "index went away")];
  // The all-time count is read on the slow schedule, so the change that
  // triggers it has to land after that wait.
  await advance(70000);
  servers.a.events.push(ev(2, "INSERT", 2));
  await advance(10000);
  const statusOnly = { note: find(screen, "ov-side-note").map(text).join(" | ") };
  delete servers.a.fail["/api/status"];
  await advance(70000);
  servers.a.events.push(ev(9, "UPDATE", 9));
  await advance(10000);
  statusOnly.after = find(screen, "ov-side-note").map(text).join(" | ");
  out.statusOnly = statusOnly;

  // Only the window counts' read fails, and answers again: the note goes
  // with it, rather than waiting for the other read to clear it.
  reset();
  servers.a = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  await paint("a");
  await advance(5000);
  servers.a.fail["/api/activity"] = [fail(500, "aggregate failed")];
  servers.a.events.push(ev(2, "INSERT", 2));
  await advance(5000);
  const activityOnly = { note: find(screen, "ov-side-note").map(text).join(" | ") };
  delete servers.a.fail["/api/activity"];
  servers.a.events.push(ev(3, "INSERT", 3));
  await advance(5000);
  activityOnly.after = find(screen, "ov-side-note").map(text).join(" | ");
  out.activityOnly = activityOnly;

  // A coverage read that fails on THIS server must not re-render the card
  // from the payload another server's paint left in the module-wide slot.
  // The two only differ while this server's own coverage has not landed, so
  // that is the shape: server b's card is still being fetched (held) when
  // the loop's own read of it times out.
  reset();
  servers.a = fresh();
  servers.b = fresh();
  servers.a.events.push(ev(1, "INSERT", 1));
  servers.a.coverage = { continuity: "ok", freshness: "current", delta_from: "2026-09-22 09:00:00", delta_to: "2026-09-22 11:00:00", lag_seconds: 3 };
  await paint("a");
  await advance(5000);
  servers.b.events.push(ev(2, "INSERT", 2));
  servers.b.coverage = { continuity: "unknown", freshness: "unknown", delta_from: "2026-09-22 08:00:00", delta_to: "2026-09-22 10:00:00" };
  const releaseCov = hold("b", "/api/coverage");
  await paint("b");
  await advance(5000);
  for (let i = 3; i < 7; i++) { servers.b.events.push(ev(i, "INSERT", i)); await advance(5000); }
  await advance(300000);
  out.covAfterSwitch = { card: find(screen, "cov-card").map(text).join(" | ") };
  releaseCov();
  await flush();

  // Nothing drawn yet and the reads keep failing: the panel says why, and
  // the page does not also say it "has not updated since" a time when it
  // never showed anything. During setup that would sit over a list whose
  // first step reads "Creating it now".
  reset();
  servers.n = fresh();
  servers.n.firstRun = [WAITING];
  servers.n.fail["/api/events/head"] = [fail(500, "Unknown database 'bintrail_idx_ab'")];
  servers.n.fail["/api/events"] = [fail(500, "Unknown database 'bintrail_idx_ab'")];
  await paint("n");
  await advance(120000);
  out.neverDrawn = { note: note(), panel: find(screen, "error-box").map(text).join(" | "), card: card() };

  console.log(JSON.stringify(out));
})().catch((e) => { console.log(JSON.stringify({ err: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "overview.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Err  string
		Walk struct {
			Before                                               []string
			Card, CardAfterChanges                               string
			Waits                                                []int
			Rows                                                 []string
			Rendered, EventsReads                                int
			IdleHeads, IdleReads                                 int
			CardAfterSnapshot, CardWithChange                    string
			RenderedAfterSnapshot, AfterListWait, RowsBeforeList int
			AsOfStamps                                           []int
		}
		Settles struct {
			SettledAsks, AfterChange, AfterWake, WhileRunning int
			CardAtEnd                                         string
			WakeTrace                                         []string
		}
		OutsideBackup struct {
			Card string
			Asks int
		}
		Hang struct {
			RowsBefore, DuringRows, Asks, RowsAfter int
			DuringNote, Note, NoteAfter             string
		}
		HangThenWake struct{ Rows, Waits int }
		SideFailed   struct {
			Note, After        string
			Tiles, TilesBefore []string
		}
		BusySettled struct{ Asks, Minutes int }
		IdleSlow    struct{ Status, Cov int }
		Activity    struct {
			Before, Same, AfterChange []int
		}
		ListHang struct {
			Asks      int
			CardAfter string
		}
		StatusOnly     struct{ Note, After string }
		ActivityOnly   struct{ Note, After string }
		CovAfterSwitch struct{ Card string }
		NeverDrawn     struct{ Note, Panel, Card string }
		Stops          struct{ Away, SignOut int }
		FirstFail      struct {
			Rows        int
			Panel, Note string
		}
		SnapshotNoChange struct {
			Card, CardAfter string
			Rows            int
			After           []string
		}
		SwitchMidRefresh struct {
			Inflight                           int
			BRows, AfterRelease, SentOnRelease []string
			AAsksLater, BAsks                  int
		}
		Hidden struct {
			HiddenReqs                                int
			RowsOnShow                                []string
			AskedAt, ListAskedAt, QuickShows, OneLoop int
		}
		InPlace struct {
			SameHead, FocusedOnScreen, Search bool
			Rows, Rendered                    int
			FocusedAnchor                     *string
		}
		Restricted struct {
			RefreshPaths     []string
			CovReads         int
			Withheld         struct{ SameRow, Shows99 bool }
			Note             string
			AsksAfterRefusal int
		}
		Failing struct {
			Note, AfterRecovery string
			Rows                int
		}
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}

	w := got.Walk
	if len(w.Waits) != 3 {
		t.Fatalf("waits = %v", w.Waits)
	}
	for i, ms := range w.Waits {
		if ms > 5000 {
			t.Errorf("change %d took %d ms of the page's clock to show; want 5 s or less without a reload", i+1, ms)
		}
	}
	if len(w.Rows) != 3 || !strings.Contains(w.Rows[0], "DELETE") || !strings.Contains(w.Rows[1], "UPDATE") || !strings.Contains(w.Rows[2], "INSERT") {
		t.Errorf("rows after the walk = %q, want the DELETE, UPDATE and INSERT, newest first", w.Rows)
	}
	if w.Rendered != 0 {
		t.Errorf("the page was repainted %d time(s) during the walk; a change must fill it in place", w.Rendered)
	}
	if !strings.Contains(w.Card, "Take the first full DB snapshot") || !strings.Contains(w.CardAfterChanges, "Take the first full DB snapshot") {
		t.Errorf("the Getting started list left before a snapshot existed: before %q, after the changes %q", w.Card, w.CardAfterChanges)
	}
	if w.EventsReads != 4 {
		t.Errorf("the events list was read %d times for a paint and three changes, want 4", w.EventsReads)
	}
	if w.IdleHeads < 10 || w.IdleReads != 0 {
		t.Errorf("a quiet minute asked %d time(s) whether anything changed and re-read the list %d time(s); want at least 10 and 0",
			w.IdleHeads, w.IdleReads)
	}
	if w.RowsBeforeList != 4 {
		t.Errorf("the change did not show before the list asked again: %d rows", w.RowsBeforeList)
	}
	if !strings.Contains(w.CardWithChange, "…Take the first full DB snapshot") {
		t.Errorf("the settled list never asked again on its own wait: %q", w.CardWithChange)
	}
	for i, n := range w.AsOfStamps {
		if n > 1 {
			t.Errorf("panel %d carries %d \"as of\" stamps after the refreshes, want one", i, n)
		}
	}
	if w.CardAfterSnapshot != "" || w.RenderedAfterSnapshot != 0 {
		t.Errorf("once a snapshot exists the list stays (%q) or the page is repainted (%d)", w.CardAfterSnapshot, w.RenderedAfterSnapshot)
	}
	if w.AfterListWait > 5000 {
		t.Errorf("after the list left, a change took %d ms to show", w.AfterListWait)
	}

	st := got.Settles
	// Five quiet minutes: at most three asks (one every two minutes), where
	// the 15 s cap would give twenty.
	if st.SettledAsks == 0 || st.SettledAsks > 3 {
		t.Errorf("a list waiting only for a snapshot asked %d time(s) over five quiet minutes; want it still asking, but rarely (each ask reads the server's snapshot locations)", st.SettledAsks)
	}
	if st.AfterChange != 0 {
		t.Errorf("a change made the settled list ask %d time(s); a change cannot produce a snapshot, and a busy server would ask on every one", st.AfterChange)
	}
	// Bounded, not one per show: the fake clock resolves each pending turn at
	// a different instant, so two of them can each arm a wait where a browser
	// would collapse them; what must not happen is five shows costing five
	// asks, each reading the backup locations.
	if st.AfterWake < 1 || st.AfterWake > 2 {
		t.Errorf("five shows in a row asked %d time(s), want 1 or 2: %v", st.AfterWake, st.WakeTrace)
	}
	if st.WhileRunning < 2 {
		t.Errorf("a snapshot this console is running was followed with %d ask(s); want it followed to the end", st.WhileRunning)
	}
	if st.CardAtEnd != "" {
		t.Errorf("the list stayed after the snapshot finished: %q", st.CardAtEnd)
	}
	if got.OutsideBackup.Card != "" {
		t.Errorf("a snapshot taken outside this page left the list saying one is owed: %q (%d asks)", got.OutsideBackup.Card, got.OutsideBackup.Asks)
	}
	h2 := got.Hang
	if !strings.Contains(h2.Note, "has not updated since") {
		t.Errorf("a request that hangs for two minutes: note %q; want the page to say since when it has not updated", h2.Note)
	}
	if h2.Asks < 3 {
		t.Errorf("a hang left the loop asking %d time(s) in two minutes; want it to give up and try again", h2.Asks)
	}
	if h2.RowsAfter < 3 || h2.NoteAfter != "" {
		t.Errorf("after the hang cleared: %d rows, note %q; want the waiting changes shown and the note gone", h2.RowsAfter, h2.NoteAfter)
	}
	if got.HangThenWake.Rows < 2 {
		t.Errorf("a tab shown again while a request hangs stayed stuck: %d rows after %d waits", got.HangThenWake.Rows, got.HangThenWake.Waits)
	}

	sf := got.SideFailed
	if !strings.Contains(sf.Note, "could not be refreshed") {
		t.Errorf("the tiles' own reads failed and the page said %q; want it to say the figures are from before", sf.Note)
	}
	if len(sf.Tiles) == 0 || strings.Join(sf.Tiles, "|") != strings.Join(sf.TilesBefore, "|") {
		t.Errorf("a failed refresh changed the figures: %q, was %q; want the last good ones kept", sf.Tiles, sf.TilesBefore)
	}
	if sf.After != "" {
		t.Errorf("the note stayed after the reads answered again: %q", sf.After)
	}

	if got.BusySettled.Asks > 5 {
		t.Errorf("a busy server asked the first-run endpoint %d times over five minutes while only a snapshot was left; "+
			"each ask reads the server's snapshot locations", got.BusySettled.Asks)
	}
	if got.IdleSlow.Status > 6 || got.IdleSlow.Cov > 6 {
		t.Errorf("half an hour idle ran %d full index counts and %d coverage reads; want a handful", got.IdleSlow.Status, got.IdleSlow.Cov)
	}
	if got.IdleSlow.Status == 0 || got.IdleSlow.Cov == 0 {
		t.Error("an idle page never re-read coverage: a stalled capture would never surface")
	}

	a := got.Activity
	if len(a.Before) == 0 || len(a.Same) == 0 || a.Before[0] != a.Same[0] {
		t.Errorf("the Activity panel was rebuilt for identical data: rows %v then %v", a.Before, a.Same)
	}
	if len(a.AfterChange) == 0 || a.AfterChange[0] == a.Same[0] {
		t.Errorf("the Activity panel did not redraw when its numbers changed: %v then %v", a.Same, a.AfterChange)
	}

	if got.ListHang.Asks < 3 {
		t.Errorf("the Getting started list asked %d time(s) in three minutes while its request hung; want it to give up and retry", got.ListHang.Asks)
	}
	if !strings.Contains(got.ListHang.CardAfter, "Capture the first change") {
		t.Errorf("the list did not carry on after its request cleared: %q", got.ListHang.CardAfter)
	}
	if !strings.Contains(got.StatusOnly.Note, "could not be refreshed") || got.StatusOnly.After != "" {
		t.Errorf("the all-time count's read failing alone: note %q, then %q; want it said and then taken back", got.StatusOnly.Note, got.StatusOnly.After)
	}
	if !strings.Contains(got.ActivityOnly.Note, "could not be refreshed") || got.ActivityOnly.After != "" {
		t.Errorf("the window counts' read failing alone: note %q, then %q; want it said and then taken back", got.ActivityOnly.Note, got.ActivityOnly.After)
	}
	if strings.Contains(got.CovAfterSwitch.Card, "2026-09-22 11:00:00") || strings.Contains(got.CovAfterSwitch.Card, "2026-09-22 09:00:00") {
		t.Errorf("a failed coverage read painted the previous server's window: %q", got.CovAfterSwitch.Card)
	}
	nd := got.NeverDrawn
	if nd.Note != "" {
		t.Errorf("the page said %q before it had ever drawn a row, over a list that says the index is being created", nd.Note)
	}
	if !strings.Contains(nd.Panel, "Unknown database") || !strings.Contains(nd.Card, "Getting started") {
		t.Errorf("nothing drawn: panel %q, list %q; want the reason in the panel and the list still up", nd.Panel, nd.Card)
	}
	if got.Stops.Away != 0 || got.Stops.SignOut != 0 {
		t.Errorf("after another page replaced the Overview it made %d request(s), after sign-out %d; want none", got.Stops.Away, got.Stops.SignOut)
	}
	ff := got.FirstFail
	if ff.Rows != 1 {
		t.Errorf("a first ask for the id that failed left the list undrawn: %d rows", ff.Rows)
	}
	if !strings.Contains(ff.Panel, "Recent changes unavailable: denied by your role") || !strings.Contains(ff.Note, "stopped updating") {
		t.Errorf("refused on both: panel %q, note %q; want the reason in the panel and the page saying it stopped", ff.Panel, ff.Note)
	}

	// A backup with no change captured yet ends the list: seeing the first
	// change is the one step nobody can do anything about, so a backup
	// finishes the list whatever that step says. The page then has to keep
	// itself current with no list at all, which is the whole of #1801: on
	// main the list leaving was the end of every refresh, and the first
	// change would have sat there unshown until a reload.
	s := got.SnapshotNoChange
	if s.Card != "" || s.Rows != 0 {
		t.Errorf("a server with a snapshot and no change: list %q, rows %d; want the list finished by the snapshot and nothing to show yet", s.Card, s.Rows)
	}
	if len(s.After) != 1 || s.CardAfter != "" {
		t.Errorf("after the first change: rows %q, list %q; want the change shown with no list to carry it", s.After, s.CardAfter)
	}

	sw := got.SwitchMidRefresh
	if sw.Inflight != 2 {
		t.Errorf("the refresh of server a was not in flight when the switch happened (%d reads)", sw.Inflight)
	}
	if len(sw.BRows) != 1 || !strings.Contains(sw.BRows[0], "#80") || len(sw.AfterRelease) != 1 || !strings.Contains(sw.AfterRelease[0], "#80") {
		t.Errorf("server b's Overview shows %q, then %q once a's late answer lands; want only b's change", sw.BRows, sw.AfterRelease)
	}
	if len(sw.SentOnRelease) != 0 {
		t.Errorf("server a's late answer set its loop asking again, under whichever server is selected now: %q", sw.SentOnRelease)
	}
	if sw.AAsksLater != 0 || sw.BAsks < 5 {
		t.Errorf("after the switch server a was asked %d more time(s) and b %d; want 0 and b's own loop", sw.AAsksLater, sw.BAsks)
	}

	h := got.Hidden
	if h.HiddenReqs != 0 {
		t.Errorf("a hidden tab made %d request(s) in two minutes", h.HiddenReqs)
	}
	if h.AskedAt != 1 || len(h.RowsOnShow) != 1 {
		t.Errorf("shown again: %d immediate ask(s), rows %q; want one ask at once and the change on screen", h.AskedAt, h.RowsOnShow)
	}
	if h.ListAskedAt != 1 {
		t.Errorf("shown again, the Getting started list asked %d time(s) at once, want 1", h.ListAskedAt)
	}
	if h.QuickShows != 1 {
		t.Errorf("two quick shows asked %d time(s), want 1", h.QuickShows)
	}
	if h.OneLoop != 1 {
		t.Errorf("after repeated shows, one 5 s wait asked %d time(s); want 1 (one loop)", h.OneLoop)
	}

	ip := got.InPlace
	if !ip.SameHead || !ip.Search || ip.Rendered != 0 || ip.Rows != 2 {
		t.Errorf("a refresh repainted the page or touched the address: %+v", ip)
	}
	if ip.FocusedAnchor == nil || *ip.FocusedAnchor != "2026-09-22T11:00:00.1Z|1" || !ip.FocusedOnScreen {
		t.Errorf("the focused Undo lost its focus when a newer change landed: %+v", ip)
	}

	r := got.Restricted
	for _, p := range r.RefreshPaths {
		if p != "/api/events/head" {
			t.Errorf("a refresh asked %q, which the first paint never asked", p)
		}
	}
	if r.CovReads > 4 {
		t.Errorf("150 s of steady changes read the coverage card %d times; a profile-restricted session records a refusal on each, want at most one a minute", r.CovReads)
	}
	if !strings.Contains(r.Note, "stopped updating") || !strings.Contains(r.Note, "not defined on this server") || r.AsksAfterRefusal != 0 {
		t.Errorf("a refused refresh: note %q, %d request(s) after; want it said and no more asking", r.Note, r.AsksAfterRefusal)
	}

	if !r.Withheld.SameRow || r.Withheld.Shows99 {
		t.Errorf("a change the session may not see repainted the list (%v) or showed (%v)", !r.Withheld.SameRow, r.Withheld.Shows99)
	}

	f := got.Failing
	if !strings.Contains(f.Note, "has not updated since") || !strings.Contains(f.Note, "UTC") || f.AfterRecovery != "" || f.Rows != 1 {
		t.Errorf("a refresh that keeps failing: note %q, after recovery %q, rows %d; want it said, then gone, and the change shown", f.Note, f.AfterRecovery, f.Rows)
	}
}

// TestOverviewListensForTheTabComingBack: the page registers the listener
// that wakes the Overview when its tab is shown again. The behaviour behind
// it is driven in TestOverviewKeepsItselfCurrent.
func TestOverviewListensForTheTabComingBack(t *testing.T) {
	init := jsFunctionBody(t, readAsset(t, "app.js"), "init")
	if !strings.Contains(init, `document.addEventListener("visibilitychange", ovVisibilityChanged)`) {
		t.Error("init does not wake the Overview when the tab is shown again")
	}
}

// TestUndoIsVisibleWithoutHover (#1801): Undo was invisible (opacity 0) until
// the mouse was over its row. It is drawn on every row, and keyboard focus
// still shows it.
func TestUndoIsVisibleWithoutHover(t *testing.T) {
	css := readAsset(t, "style.css")
	for _, m := range regexp.MustCompile(`(?s)([^{}]*\.ov-ev-undo[^{}]*)\{([^}]*)\}`).FindAllStringSubmatch(css, -1) {
		if regexp.MustCompile(`opacity\s*:\s*0(\s*;|\s*$)`).MatchString(m[2]) {
			t.Errorf("%s hides Undo: %s", strings.TrimSpace(m[1]), strings.TrimSpace(m[2]))
		}
		if strings.Contains(m[1], ":hover") {
			t.Errorf("Undo still depends on hover: %s", strings.TrimSpace(m[1]))
		}
	}
	if !strings.Contains(jsFunctionBody(t, readAsset(t, "app.js"), "ovEventRow"), `"ov-ev-undo"`) && !strings.Contains(jsFunctionBody(t, readAsset(t, "app.js"), "ovEventRow"), `btn btn-sm ov-ev-undo`) {
		t.Error("ovEventRow no longer draws the Undo button this test reads the style of")
	}
}

// TestIdleCoverageReadsAsTimeSinceTheLastChange is the text half of #1794:
// a server nobody wrote to showed an amber "capture lag 4445s" and named a
// Prometheus metric the page gives no way to read. The chip now says how long
// ago the last change was, in hours and minutes, in a neutral colour; a
// stalled capture keeps its red and a current one its green.
func TestIdleCoverageReadsAsTimeSinceTheLastChange(t *testing.T) {
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
FakeEl.prototype.replaceWith = function () {};
const flat = (n, cls, out = []) => { if (!n || !n.children) return out; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push({ text: n.textContent, cls: n.className.trim() });
  for (const c of n.children) flat(c, cls, out); return out; };
const read = (c) => { const card = vm.runInContext("covCard", ctx)(c, { at: "11:00:00 UTC" }); return { chips: flat(card, "cov-chip"), lines: flat(card, "cov-line"), all: card.textContent }; };
const base = { delta_from: "2026-09-22 10:00:00", delta_to: "2026-09-22 11:30:27", continuity: "ok" };
const dur = vm.runInContext("plainDuration", ctx);
console.log(JSON.stringify({
  idle: read({ ...base, freshness: "idle", lag_seconds: 4445 }),
  idleEmpty: read({ continuity: "ok", freshness: "idle" }),
  stalled: read({ ...base, freshness: "stalled", lag_seconds: 4445, checkpoint_age_seconds: 3720 }),
  current: read({ ...base, freshness: "current", lag_seconds: 146 }),
  durations: [0, 1, 59, 60, 146, 3599, 3600, 4445, 86399, 86400, 97200, 900000].map((s) => dur(s)),
}));
`
	path := filepath.Join(t.TempDir(), "cov.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type part struct{ Text, Cls string }
	type card struct {
		Chips, Lines []part
		All          string
	}
	var got struct {
		Idle, IdleEmpty, Stalled, Current card
		Durations                         []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	chip := func(c card, prefix string) part {
		for _, p := range c.Chips {
			if strings.HasPrefix(p.Text, prefix) {
				return p
			}
		}
		return part{}
	}

	if c := chip(got.Idle, "last change"); c.Text != "last change 1h 14m ago" || c.Cls != "cov-chip" {
		t.Errorf("idle chip = %+v, want \"last change 1h 14m ago\" with no colour", c)
	}
	if c := chip(got.Idle, "capture idle"); c.Cls != "cov-chip" {
		t.Errorf("idle state chip = %+v, want no colour: a quiet server is not a warning", c)
	}
	for _, c := range []card{got.Idle, got.IdleEmpty} {
		for _, l := range c.Lines {
			if strings.Contains(l.Cls, "warn") || strings.Contains(l.Cls, "bad") {
				t.Errorf("idle line is coloured: %+v", l)
			}
		}
		if strings.Contains(c.All, "bintrail_") || strings.Contains(c.All, "metric") || strings.Contains(c.All, "capture lag") {
			t.Errorf("idle card names a metric or calls the quiet time lag: %q", c.All)
		}
		if !strings.Contains(c.All, "cannot tell which") {
			t.Errorf("idle card no longer says the page cannot tell a quiet server from capture falling behind: %q", c.All)
		}
	}
	if !strings.Contains(got.Idle.All, "Nothing captured for 1h 14m.") || !strings.Contains(got.IdleEmpty.All, "Nothing captured yet.") {
		t.Errorf("idle lines = %q / %q", got.Idle.All, got.IdleEmpty.All)
	}
	if c := chip(got.Stalled, "last change"); c.Text != "last change 1h 14m ago" || !strings.Contains(c.Cls, "bad") {
		t.Errorf("stalled chip = %+v, want the same words in red", c)
	}
	if !strings.Contains(got.Stalled.All, "has not checkpointed for 1h 2m") {
		t.Errorf("stalled line = %q, want its age in hours and minutes", got.Stalled.All)
	}
	if c := chip(got.Current, "last change"); c.Text != "last change 2m 26s ago" || !strings.Contains(c.Cls, "ok") {
		t.Errorf("current chip = %+v, want \"last change 2m 26s ago\" in green", c)
	}
	want := []string{"0s", "1s", "59s", "1m", "2m 26s", "59m 59s", "1h", "1h 14m", "23h 59m", "1d", "1d 3h", "10d 10h"}
	if strings.Join(got.Durations, ",") != strings.Join(want, ",") {
		t.Errorf("plainDuration = %q, want %q", got.Durations, want)
	}
	for _, all := range []string{got.Idle.All, got.IdleEmpty.All, got.Stalled.All, got.Current.All} {
		if strings.Contains(all, "—") {
			t.Errorf("em dash in %q", all)
		}
	}
	// The metric name is gone from the page altogether, not only from this
	// card: the page gives no way to read it.
	if strings.Contains(readAsset(t, "app.js"), "bintrail_stream_index_commit_latency_seconds") {
		t.Error("app.js still names bintrail_stream_index_commit_latency_seconds")
	}
}

// jsStringRE matches one double-quoted JavaScript string literal on a line.
var jsStringRE = regexp.MustCompile(`"([^"\n]*)"`)

// TestCaptureLagIsGoneFromThePage: the words "capture lag" are not on screen
// anywhere, including where a reader hovers rather than reads. The chip was
// renamed for #1794 while the refresh button beside it kept "Re-read capture
// lag and continuity" in its tooltip, and a guard that reads rendered TEXT
// cannot see a title attribute, so it stayed green over it. This one reads
// the source: every string app.js holds, whatever it is used for.
func TestCaptureLagIsGoneFromThePage(t *testing.T) {
	js := readAsset(t, "app.js")
	// Comment LINES are dropped first, so the one recording what the chip
	// used to say does not trip this. A trailing comment could still hide a
	// string from it; cutting those would truncate real code at the "//" in
	// a URL, and a negative check that cuts too much passes for the wrong
	// reason.
	for _, m := range jsStringRE.FindAllStringSubmatch(stripJSCommentLines(js), -1) {
		if strings.Contains(strings.ToLower(m[1]), "capture lag") {
			t.Errorf("app.js holds %q: the number is the time since the last captured change, not lag (#1794)", m[1])
		}
	}
	// And the tooltip that carried it says what the button does.
	if !strings.Contains(jsFunctionBody(t, js, "covRefresh"), "Re-read the restore window and capture state") {
		t.Error(`the coverage refresh button lost the tooltip that replaced its "capture lag" one`)
	}
}
