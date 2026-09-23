package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The one page the three backup pages merged into (#1573): the line a reader
// who followed an old address gets, and the jump to the part of the page that
// address named.
//
// Both are driven through the real functions. They are small, and they are
// the two pieces of the merge a reader meets first: a note that will not go
// away, or one that never appears, is the difference between "the page moved"
// and "the page is gone".
func TestSnapshotsArrivalNoteAndSectionJump(t *testing.T) {
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
// A localStorage that really stores, and can be told to throw the way a
// private window or a blocked-site-data policy does.
let store = {}, throws = false;
ctx.localStorage = {
  getItem: (k) => { if (throws) throw new Error("blocked"); return Object.prototype.hasOwnProperty.call(store, k) ? store[k] : null; },
  setItem: (k, v) => { if (throws) throw new Error("blocked"); store[k] = String(v); },
  removeItem: (k) => { delete store[k]; },
};
const notice = (from, missing) => { vm.runInContext("routeArrivedFrom = " + JSON.stringify(from) + ";", ctx);
  return vm.runInContext("snapshotsMovedNotice(" + JSON.stringify(missing || "") + ")", ctx); };
const textOf = (n) => (n ? n.textContent : null);
// The close button is the only button the note holds.
const closeIt = (n) => { const b = n.children.find((c) => c.tag === "button"); b.onclick(); return b; };
// FakeEl has no remove(); the note calls it on itself. And el() binds a
// handler with addEventListener, which the shared harness drops on the
// floor — keep it, so the close button can really be pressed here.
FakeEl.prototype.remove = function () { this._removed = true; };
FakeEl.prototype.addEventListener = function (ev, fn) { this["on" + ev] = fn; };

const out = {};
// Nobody followed an old address: no note.
out.fromNowhere = textOf(notice(""));
// A route that is not one of the three: no note either.
out.fromElsewhere = textOf(notice("storage"));
// And a name every object carries is not an old address.
out.fromConstructor = textOf(notice("constructor"));
out.fromProto = textOf(notice("__proto__"));
// Each old address names the page it was.
out.baselines = textOf(notice("baselines"));
out.verification = textOf(notice("verification"));
out.backupSettings = textOf(notice("backup-settings"));
// Closing one: gone now, gone on the next visit, and the other two untouched.
const n = notice("verification");
const b = closeIt(n);
out.removedOnClose = !!n._removed;
out.closedKeys = Object.keys(store);
out.verificationAfter = textOf(notice("verification"));
out.baselinesAfter = textOf(notice("baselines"));
// The next page LOAD: this visit's memory is gone, and only what the
// browser kept can answer. Without the write, the note comes back every
// time the reader opens their bookmark.
vm.runInContext("movedClosed.clear()", ctx);
out.verificationNextLoad = textOf(notice("verification"));
out.baselinesNextLoad = textOf(notice("baselines"));
// A browser that refuses storage: the note still renders, still closes for
// this visit, and nothing throws.
store = {}; throws = true;
let threw = "";
try {
  const p = notice("backup-settings");
  out.blockedShows = !!p;
  closeIt(p);
  out.blockedAfter = textOf(notice("backup-settings"));
} catch (e) { threw = String(e && e.message || e); }
out.blockedThrew = threw;
throws = false;

// scrollToSection: the address names a part of the page, and the page has
// just painted it.
let scrolled = [];
const section = { scrollIntoView: () => scrolled.push("checks") };
document.getElementById = (id) => (id === "checks" ? section : null);
// Armed the way renderRoute arms it: one arrival, one jump.
const jump = (hash) => { scrolled = []; ctx.location.hash = hash;
  vm.runInContext("scrollPending = true; scrollToSection();", ctx); return scrolled.slice(); };
out.jumpChecks = jump("#checks");
out.jumpNone = jump("");
out.jumpMissing = jump("#setup");
// An element the browser gives us without that method (an old engine, a
// stubbed node) must not throw on the way out of a paint.
document.getElementById = () => ({});
out.jumpNoMethod = jump("#checks");
// The repaints this page does on its own (a job poll, a saved setting, a
// page of the list) must NOT move the reader again.
document.getElementById = (id) => (id === "checks" ? section : null);
scrolled = []; ctx.location.hash = "#checks";
vm.runInContext("scrollPending = true; scrollToSection(); scrollToSection(); scrollToSection();", ctx);
out.jumpRepaints = scrolled.slice();
// A note placed beside the section is what the jump aims at: scrolling the
// heading to the top edge would leave the note just above the viewport.
scrolled = [];
const noteEl = { isConnected: true, scrollIntoView: () => scrolled.push("note") };
vm.runInContext("scrollPending = true;", ctx);
ctx.__note = noteEl;
vm.runInContext("scrollToSection(__note);", ctx);
out.jumpWithNote = scrolled.slice();
// The line a reader gets when the part they asked for is not here: because
// this console is read-only, and because we could not read what the server
// supports at all — a watch daemon must never be told it is read-only.
out.missingSection = textOf(notice("verification", "daemon"));
out.missingUnknown = textOf(notice("verification", "unknown"));
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "snapshots.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		FromNowhere, FromElsewhere               *string
		FromConstructor, FromProto               *string
		Baselines, Verification, BackupSettings  *string
		RemovedOnClose                           bool
		ClosedKeys                               []string
		VerificationAfter, BaselinesAfter        *string
		VerificationNextLoad, BaselinesNextLoad  *string
		BlockedShows                             bool
		BlockedAfter                             *string
		BlockedThrew                             string
		JumpChecks, JumpNone, JumpMissing        []string
		JumpNoMethod, JumpRepaints, JumpWithNote []string
		MissingSection, MissingUnknown           *string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.FromNowhere != nil || got.FromElsewhere != nil {
		t.Errorf("a reader who did not follow one of the merged addresses is told something: %v / %v",
			got.FromNowhere, got.FromElsewhere)
	}
	if got.FromConstructor != nil || got.FromProto != nil {
		t.Errorf("a name every object carries reads as an old address: %q / %q",
			deref(got.FromConstructor), deref(got.FromProto))
	}
	for name, pair := range map[string]struct {
		got  *string
		want string
	}{
		"baselines":       {got.Baselines, "Backups is part of Snapshots now."},
		"verification":    {got.Verification, "Verification is part of Snapshots now."},
		"backup-settings": {got.BackupSettings, "Backup settings is part of Snapshots now."},
	} {
		if pair.got == nil || *pair.got != pair.want+"×" {
			t.Errorf("arriving from /%s reads %q, want %q followed by the close button", name, deref(pair.got), pair.want)
		}
	}
	if !got.RemovedOnClose || len(got.ClosedKeys) != 1 {
		t.Errorf("closing the note: taken off the page %v, keys written %q — it must go now and stay gone",
			got.RemovedOnClose, got.ClosedKeys)
	}
	if got.VerificationAfter != nil {
		t.Errorf("the closed note is back on the next visit: %q", deref(got.VerificationAfter))
	}
	if got.BaselinesAfter == nil {
		t.Error("closing one note hid another: each old address tells its own reader once")
	}
	if got.VerificationNextLoad != nil {
		t.Errorf("the closed note is back on the next page load (%q): closing it must be written to the "+
			"browser, or a bookmark shows it forever", deref(got.VerificationNextLoad))
	}
	if got.BaselinesNextLoad == nil {
		t.Error("a note nobody closed is gone after a page load: the write is keyed by the address it closed")
	}
	if got.BlockedThrew != "" {
		t.Errorf("a browser that refuses site data broke the page: %s", got.BlockedThrew)
	}
	if !got.BlockedShows || got.BlockedAfter != nil {
		t.Errorf("with storage blocked: note shown %v, shown again after closing %q — it must still render and still close for this visit",
			got.BlockedShows, deref(got.BlockedAfter))
	}
	if len(got.JumpChecks) != 1 {
		t.Errorf("an address naming a section did not bring it into view: %q", got.JumpChecks)
	}
	if len(got.JumpRepaints) != 1 {
		t.Errorf("three paints of the same arrival scrolled %d times, want 1: the page repaints itself every "+
			"two seconds while a job runs, and each of those would drag the reader back down", len(got.JumpRepaints))
	}
	if len(got.JumpWithNote) != 1 || got.JumpWithNote[0] != "note" {
		t.Errorf("the jump aimed at %q, want the arrival note beside the section: the heading at the top edge "+
			"leaves the note just above the viewport, unread", got.JumpWithNote)
	}
	if got.MissingSection == nil || !strings.Contains(*got.MissingSection, "not on this console") {
		t.Errorf("a reader whose section this console does not draw reads %q; it must say so, or the page "+
			"claims to hold something with no trace of it", deref(got.MissingSection))
	}
	if got.MissingUnknown == nil || !strings.Contains(*got.MissingUnknown, "capability check failed") ||
		strings.Contains(*got.MissingUnknown, "read-only") {
		t.Errorf("when the capability check failed the note reads %q; it must not tell a watch daemon it is "+
			"read-only, which is a false statement about their installation", deref(got.MissingUnknown))
	}
	if len(got.JumpNone) != 0 || len(got.JumpMissing) != 0 || len(got.JumpNoMethod) != 0 {
		t.Errorf("scrolled when it should not have: no hash %q, a section this console does not have %q, an element without the method %q",
			got.JumpNone, got.JumpMissing, got.JumpNoMethod)
	}
}

func deref(s *string) string {
	if s == nil {
		return "(none)"
	}
	return *s
}

// TestSnapshotsOnServeDropsTheDaemonParts: the page opens on a standalone
// serve, where two of the three pages it replaces did not exist at all —
// /baselines and /verification rewrote the address to Overview. What serve
// can answer it shows (the copies that exist, and where this server keeps
// them); what needs the watch daemon it leaves out entirely, rather than
// drawing a section whose buttons nothing can answer. It must also not ASK
// for the daemon's own endpoint: a fetch that can only 404 there would paint
// an error on a page that is working.
func TestSnapshotsOnServeDropsTheDaemonParts(t *testing.T) {
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
Object.defineProperty(FakeEl.prototype, "isConnected", { get() { for (let n = this; n; n = n.__parent) if (n === screen) return true; return false; } });
document.getElementById = (id) => (id === "view" ? screen : null);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return []; const out = f(n) ? [n] : [];
  for (const c of n.children || []) out.push(...walk(c, f)); return out; };
const hasClass = (n, c) => (" " + n.className + " ").includes(" " + c + " ");
const sections = () => walk(screen, (n) => hasClass(n, "snap-sect")).map((n) => n.attrs.id);
const titles = () => walk(screen, (n) => hasClass(n, "ov-panel-title")).map((n) => n.textContent);
let asked = [];
ctx.__api = async (p) => {
  asked.push(p);
  if (p === "/api/servers") return { servers: [{ id: "a", name: "a", kind: "registry", has_source: true, baseline_dir: "/tmp/b" }] };
  if (p === "/api/baselines") return { configured: true, source: "/tmp/b", kind: "dir",
    snapshots: [{ time: "2026-06-10 12:00:00", tables: 3, binlog_file: "binlog.000001", binlog_pos: 50 }] };
  if (p === "/api/backup-settings") return { daemon: [{ key: "baseline_dir", value: "/tmp/b", editable: false }],
    servers: [{ id: "a", name: "a", source: "none" }] };
  if (p === "/api/baseline-refresh") return { enabled: true, table_deltas: true };
  return {};
};
vm.runInContext("api = (p) => __api(p);", ctx);
const noteText = () => (walk(screen, (n) => hasClass(n, "snap-moved-text")).map((n) => n.textContent)[0] || "");
// Whether the arrival note sits immediately before the section the reader
// asked for: scrolling that section to the top edge would otherwise leave
// the note just above the viewport, unread.
const noteBefore = (id) => {
  const kids = screen.children;
  const i = kids.findIndex((n) => n && n.attrs && n.attrs.id === id);
  return i > 0 && hasClass(kids[i - 1], "snap-moved");
};
// hash is what renderRoute leaves in the address before it dispatches: the
// section the old address became, or whatever deeper anchor that address
// already carried.
const paint = async (caps, from, hash) => {
  asked = [];
  ctx.location.hash = hash || "";
  vm.runInContext("capsCache = " + JSON.stringify(caps) + "; capsKnown = true; currentServer = 'a'; defaultServerId = 'a';", ctx);
  vm.runInContext("routeArrivedFrom = " + JSON.stringify(from || "") + "; movedClosed.clear(); backupsPaintedFor = '';", ctx);
  await vm.runInContext("renderSnapshots()", ctx);
  return { asked: asked.slice(), sections: sections(), titles: titles(), text: screen.textContent,
    note: noteText(), notes: walk(screen, (n) => hasClass(n, "snap-moved")).length,
    noteBeforeChecks: noteBefore("checks"), noteBeforeSetup: noteBefore("setup") };
};
(async () => {
  const out = {};
  out.serve = await paint({});
  out.watch = await paint({ monitor: true, verify_trigger: true });
  // The combination the two guards used to fall between: a reader who
  // followed /verification, on a console that draws no Checks section.
  out.serveArrival = await paint({}, "verification", "#checks");
  out.watchArrival = await paint({ monitor: true, verify_trigger: true }, "verification", "#checks");
  out.setupArrival = await paint({ monitor: true, verify_trigger: true }, "backup-settings", "#setup");
  // A link INTO a part of the old page keeps its own anchor, so the scroll
  // goes there (or nowhere) and a note left beside Checks would never be
  // read: it has to lead the page instead.
  out.deepArrival = await paint({ monitor: true, verify_trigger: true }, "verification", "#past");
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`
	path := filepath.Join(t.TempDir(), "serve.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type painted struct {
		Asked, Sections, Titles           []string
		Text, Note                        string
		Notes                             int
		NoteBeforeChecks, NoteBeforeSetup bool
	}
	var got struct {
		Err                                                                 string
		Serve, Watch, ServeArrival, WatchArrival, SetupArrival, DeepArrival painted
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	for _, want := range []string{"/api/servers", "/api/baselines", "/api/backup-settings"} {
		if !hasString(got.Serve.Asked, want) {
			t.Errorf("serve does not fetch %s, so that half of the page has nothing to show: asked %q", want, got.Serve.Asked)
		}
	}
	if hasString(got.Serve.Asked, "/api/baseline-refresh") {
		t.Errorf("serve asks the watch daemon's own endpoint: %q", got.Serve.Asked)
	}
	if !strings.Contains(got.Serve.Text, "Snapshots") || !hasString(got.Serve.Titles, "Per server") {
		t.Errorf("serve does not render the page and the backup location: titles %q", got.Serve.Titles)
	}
	if hasString(got.Serve.Sections, "checks") {
		t.Errorf("serve draws the checks section, whose runner only the watch daemon has: %q", got.Serve.Sections)
	}
	if !hasString(got.Serve.Sections, "setup") {
		t.Errorf("serve drops the setup section, which is the only editor of a server's backup location: %q", got.Serve.Sections)
	}
	if !hasString(got.Watch.Sections, "checks") || !hasString(got.Watch.Sections, "setup") {
		t.Errorf("under watch the page is missing a section: %q", got.Watch.Sections)
	}
	if !hasString(got.Watch.Asked, "/api/baseline-refresh") {
		t.Errorf("watch does not fetch /api/baseline-refresh, so the disk-space card can only render its error branch: %q", got.Watch.Asked)
	}
	// Nobody followed an old address in the two paints above.
	if got.Serve.Notes != 0 || got.Watch.Notes != 0 {
		t.Errorf("a note appeared for a reader who arrived from the sidebar: %q / %q", got.Serve.Note, got.Watch.Note)
	}
	// Exactly one, wherever it goes: a note drawn at the top AND beside the
	// section is the same sentence twice on one screen.
	for name, p := range map[string]painted{"serve, from /verification": got.ServeArrival,
		"watch, from /verification": got.WatchArrival, "watch, from /backup-settings": got.SetupArrival} {
		if p.Notes != 1 {
			t.Errorf("%s: %d arrival notes on the page, want exactly 1", name, p.Notes)
		}
	}
	// A reader who followed /verification onto a console with no Checks
	// section must be told that, or the page claims to hold something it
	// shows no trace of — worse than the bounce to Overview it replaced.
	if !strings.Contains(got.ServeArrival.Note, "Verification is part of Snapshots now") ||
		!strings.Contains(got.ServeArrival.Note, "not on this console") {
		t.Errorf("arriving from /verification on serve reads %q; it must name the page AND say its section is not here", got.ServeArrival.Note)
	}
	// On a watch daemon the same arrival lands beside the section, and says
	// nothing about it missing.
	if !got.WatchArrival.NoteBeforeChecks || strings.Contains(got.WatchArrival.Note, "not on this console") {
		t.Errorf("arriving from /verification on watch: note beside the Checks heading %v, text %q",
			got.WatchArrival.NoteBeforeChecks, got.WatchArrival.Note)
	}
	if !got.SetupArrival.NoteBeforeSetup || strings.Contains(got.SetupArrival.Note, "not on this console") {
		t.Errorf("arriving from /backup-settings: note beside the setup heading %v, text %q",
			got.SetupArrival.NoteBeforeSetup, got.SetupArrival.Note)
	}
	if got.DeepArrival.Notes != 1 || got.DeepArrival.NoteBeforeChecks {
		t.Errorf("following a link into a part of the old page (/verification#past): %d notes, beside the "+
			"Checks heading %v — the address sends the reader elsewhere, so the note has to lead the page",
			got.DeepArrival.Notes, got.DeepArrival.NoteBeforeChecks)
	}
}

func hasString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// TestSnapshotsPartsFailSeparately: the three jobs that merged into this page
// keep the three blast radii they had as three pages. A verify record this
// build cannot read, or a settings row that throws, must not take the list of
// copies down with it — an operator left with one red box cannot tell whether
// their backups exist, let alone which of the three broke.
func TestSnapshotsPartsFailSeparately(t *testing.T) {
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
Object.defineProperty(FakeEl.prototype, "isConnected", { get() { for (let n = this; n; n = n.__parent) if (n === screen) return true; return false; } });
document.getElementById = (id) => (id === "view" ? screen : null);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return []; const out = f(n) ? [n] : [];
  for (const c of n.children || []) out.push(...walk(c, f)); return out; };
const hasClass = (n, c) => (" " + n.className + " ").includes(" " + c + " ");
const titles = () => walk(screen, (n) => hasClass(n, "ov-panel-title")).map((n) => n.textContent);
ctx.__api = async (p) => {
  if (p === "/api/servers") return { servers: [{ id: "a", name: "a", kind: "registry", has_source: true, baseline_dir: "/tmp/b" }] };
  if (p === "/api/baselines") return { configured: true, source: "/tmp/b", kind: "dir",
    snapshots: [{ time: "2026-06-10 12:00:00", tables: 3 }] };
  if (p === "/api/backup-settings") return { daemon: [], servers: [{ id: "a", name: "a", source: "none" }] };
  return {};
};
vm.runInContext("api = (p) => __api(p); capsCache = { monitor: true, verify_trigger: true }; capsKnown = true; currentServer = 'a'; defaultServerId = 'a';", ctx);
const paint = async () => { await vm.runInContext("renderSnapshots()", ctx); return { titles: titles(), text: screen.textContent }; };
const real = { verifyRegions: ctx.verifyRegions, setup: ctx.snapshotSetupSections, strip: ctx.baselineContextStrip };
(async () => {
  const out = {};
  out.whole = await paint();
  ctx.verifyRegions = () => { throw new Error("bad verify record"); };
  out.checksBroken = await paint();
  ctx.verifyRegions = real.verifyRegions;
  ctx.snapshotSetupSections = () => { throw new Error("bad settings row"); };
  out.setupBroken = await paint();
  ctx.snapshotSetupSections = real.setup;
  ctx.baselineContextStrip = () => { throw new Error("bad strip"); };
  out.listBroken = await paint();
  ctx.baselineContextStrip = real.strip;
  console.log(JSON.stringify(out));
})().catch((e) => console.log(JSON.stringify({ err: String((e && e.stack) || e) })));
`
	path := filepath.Join(t.TempDir(), "parts.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type paint struct {
		Titles []string
		Text   string
	}
	var got struct {
		Err                                          string
		Whole, ChecksBroken, SetupBroken, ListBroken paint
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Err != "" {
		t.Fatalf("threw: %s", got.Err)
	}
	if !hasString(got.Whole.Titles, "Per server") || !strings.Contains(got.Whole.Text, "Snapshots") {
		t.Fatalf("the page does not render whole, so the broken cases below prove nothing: %q", got.Whole.Titles)
	}
	for _, c := range []struct {
		name  string
		got   paint
		says  string
		keeps []string
	}{
		// Each case keeps a panel from EACH of the other two parts, so a
		// failure that quietly swallowed a neighbour still rings.
		{"a verify record it cannot read", got.ChecksBroken, "Checks could not be drawn", []string{"Take a copy with you", "Per server"}},
		{"a settings row that throws", got.SetupBroken, "Where and how often could not be drawn", []string{"Take a copy with you", "Run a check"}},
		{"the context strip throwing", got.ListBroken, "The list of copies could not be drawn", []string{"Run a check", "Per server"}},
	} {
		if !strings.Contains(c.got.Text, c.says) {
			t.Errorf("with %s the page does not say which part broke (looking for %q): %q", c.name, c.says, c.got.Text)
		}
		if !strings.Contains(c.got.Text, "Snapshots") {
			t.Errorf("with %s the page lost its own heading", c.name)
		}
		for _, keep := range c.keeps {
			if !hasString(c.got.Titles, keep) {
				t.Errorf("with %s the page lost %q, which is a different part: %q", c.name, keep, c.got.Titles)
			}
		}
	}
	// And the list survives a broken verify: the one that matters most, since
	// this is where an operator reads whether their data has a copy at all.
	if !strings.Contains(got.ChecksBroken.Text, "2026-06-10") {
		t.Errorf("a broken Checks section took the list of copies with it: %q", got.ChecksBroken.Text)
	}
}
