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

// TestBackupServerRow_localCopyInEveryMode renders the REAL per-server row
// (#1681) in node, clicks through it the way a reader does, and captures what
// Save sends and what the row says. It exists for the failure a CSS-hidden
// field invites: an input the reader cannot see still carries its value, so a
// save must never send a count from a field that is hidden (a server with S3
// and no folder yet). With an S3 destination the count is shown and sent like
// without (#2658). Every save says local_copy true (#2431).
func TestBackupServerRow_localCopyInEveryMode(t *testing.T) {
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
FakeEl.prototype.addEventListener = function (t, f) { (this._l = this._l || {})[t] = ((this._l || {})[t] || []).concat(f); };
const fire = (n, t) => { for (const f of ((n._l || {})[t] || [])) f({ key: "" }); };
const calls = [];
ctx.__calls = calls;
const toasts = [];
ctx.__toasts = toasts;
ctx.__answer = {};
vm.runInContext("api = (u, o) => { __calls.push({ u, body: o.body }); return Promise.resolve(__answer); }; renderSnapshots = () => Promise.resolve(); toast = (m) => { __toasts.push(m); }; capsCache.monitor = true;", ctx);
const walk = (n, f) => { if (!n || n.nodeType !== 1) return; f(n); for (const c of n.children) walk(c, f); };
const find = (root, pred) => { let hit = null; walk(root, (n) => { if (!hit && pred(n)) hit = n; }); return hit; };
const byName = (root, name, value) => find(root, (n) => n.tag === "input" && n.attrs.name === name && (value == null || n.attrs.value === value));
const saveBtn = (root) => find(root, (n) => n.tag === "button" && n.textContent === "Save");
// Visible = not under a hidden ancestor.
const visible = (root) => { const out = []; const go = (n, hid) => { if (!n) return; if (n.nodeType === 3) return; const h = hid || n.hidden; if (!h && n.tag === "p" && n._text) out.push(n._text); for (const c of n.children) go(c, h); }; go(root, false); return out; };
const reds = (root) => { const out = []; walk(root, (n) => { if (!n.hidden && n.tag === "p" && /\berr\b/.test(n.className) && n._text) out.push(n._text); }); return out; };
const shown = (root, name) => { let ok = null; const go = (n, hid) => { if (!n || n.nodeType !== 1) return; const h = hid || n.hidden; if (n.tag === "input" && n.attrs.name === name) ok = !h; for (const c of n.children) go(c, h); }; go(root, false); return ok; };
const base = { id: "s1", name: "prod", baseline_dir: "", baseline_s3: "", default_dir: "/state/snapshots/s1", keep_newest: 0, local_copy: false, prune_loop: true, source: "none" };
const row = (o, reuse) => ctx.backupServerRow(Object.assign({}, base, o), false, [], "", reuse);
const out = {};
async function step(name, o, act, reuse) {
  calls.length = 0;
  const r = row(o, reuse === undefined ? true : reuse);
  const before = { words: visible(r), reds: reds(r), dirShown: shown(r, "baseline_dir"), keepShown: shown(r, "keep_newest"), dir: byName(r, "baseline_dir").value, saveDisabled: saveBtn(r).disabled };
  if (act) act(r);
  const s = saveBtn(r);
  const after = { words: visible(r), reds: reds(r), dirShown: shown(r, "baseline_dir"), keepShown: shown(r, "keep_newest"), saveDisabled: s.disabled };
  if (!s.disabled) { await s.onclick(); }
  // The save is the PUT of the server's snapshot settings; a row with a
  // bucket also asks about the bucket's rules when it is drawn.
  const puts = calls.filter((c) => /\/api\/backup-settings\/servers\//.test(c.u));
  out[name] = { before, after, body: puts.length ? puts[puts.length - 1].body : null };
}
const type = (r, name, v) => { const i = byName(r, name); i.value = v; fire(i, "input"); };
(async () => {
  // A new server: its own folder, a count, no S3.
  const fresh = { baseline_dir: "/state/snapshots/s1", local_copy: true, keep_newest: 3, source: "server" };
  await step("freshAsIs", fresh, null);
  await step("freshKeep5", fresh, (r) => type(r, "keep_newest", "5"));
  await step("freshKeepAll", fresh, (r) => type(r, "keep_newest", ""));
  await step("freshKeepBad", fresh, (r) => type(r, "keep_newest", "2.5"));
  await step("freshWithS3", fresh, (r) => { type(r, "baseline_s3", "s3://b/p/"); type(r, "keep_newest", "9"); });
  await step("freshNoLoop", Object.assign({}, fresh, { prune_loop: false }), null);
  await step("freshNoReuse", fresh, null, false);
  // A server saved with a bucket and no folder: the default folder shown.
  const s3only = { baseline_s3: "s3://b/p/", source: "server" };
  await step("s3onlyAsIs", s3only, null);
  // An existing local-only server that keeps everything.
  await step("oldLocal", { baseline_dir: "/srv/snaps", local_copy: true, source: "server" }, null);
  await step("s3TypedAfterBadCount", fresh, (r) => { type(r, "keep_newest", "x"); type(r, "baseline_s3", "s3://b/p/"); });
  await step("blocked", Object.assign({}, fresh, { keep_blocked: true }), null);
  await step("held", Object.assign({}, fresh, { keep_blocked: true, keep_held: true }), null);
  // How far back the count reaches, from the real schedule fields the
  // settings API sends (#1681), with the count in force as the listing has it.
  const inForce = Object.assign({}, fresh, { keep_in_force: 3 });
  // snapshot_every_minutes is the server's answer (the schedule and the
  // refresh loop together, where each runs); the page never works
  // it out from the schedule fields, which reachScheduleOnly proves.
  await step("reach5m", Object.assign({}, inForce, { snapshot_every_minutes: 5 }), null);
  await step("reachHourly", Object.assign({}, inForce, { snapshot_every_minutes: 60 }), null);
  await step("reachDaily", Object.assign({}, inForce, { schedule_every: "1d", snapshot_every_minutes: 1440 }), null);
  await step("reachDailyTyped1", Object.assign({}, inForce, { snapshot_every_minutes: 1440 }), (r) => type(r, "keep_newest", "1"));
  await step("reachNone", inForce, null);
  await step("reachNoneOne", Object.assign({}, inForce, { keep_newest: 1, keep_in_force: 1 }), null);
  await step("reachRetain", Object.assign({}, inForce, { snapshot_every_minutes: 5, prune_retain_minutes: 7 * 1440 }), null);
  await step("reachScheduleOnly", Object.assign({}, inForce, { schedule_every: "1h", schedule_every_minutes: 60 }), null);
  await step("reachNotApplied", Object.assign({}, fresh, { snapshot_every_minutes: 60 }), null);
  await step("noLocation", { source: "none" }, null);
  await step("oldLocalBothYes", { baseline_dir: "/srv/snaps", baseline_s3: "s3://b/p/", local_copy: true, source: "server" }, null);
  // A server with S3 and a count in force (#2658), from the fields the
  // settings API sends for it.
  const s3c = { baseline_dir: "/state/snapshots/s1", baseline_s3: "s3://b/p/", local_copy: true, keep_newest: 3, keep_in_force: 3, source: "server", snapshot_every_minutes: 5 };
  await step("s3Counted", s3c, null);
  await step("s3CountedHeld", Object.assign({}, s3c, { schedule_every: "5m", schedule_upload_every: "1d", schedule_upload_every_minutes: 1440 }), null);
  await step("s3CountedAge", Object.assign({}, s3c, { delete_after_minutes: 7 * 1440, prune_retain_minutes: 7 * 1440 }), null);
  await step("s3CountedNotIn", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 2, names: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], resending: true } }), null);
  await step("s3CountedUnasked", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 1, names: ["2026-11-10T03-00-00Z"], resending: true, not_asked: ["2026-11-10T03-00-00Z"] } }), null);
  await step("s3CountedUnaskedTwo", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 2, names: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], resending: true, not_asked: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], next_send: "2026-11-11T04:00:00Z" } }), null);
  await step("s3CountedReadRefusedAll", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 2, names: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], resending: true, not_asked: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], read_refused: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"] } }), null);
  await step("s3CountedReadRefusedOne", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 2, names: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], resending: true, not_asked: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], read_refused: ["2026-11-10T03-05-00Z"] } }), null);
  await step("s3CountedWaitsForAnswer", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 1, names: ["2026-11-10T03-00-00Z"], resending: true, not_asked: ["2026-11-10T03-00-00Z"], next_send: "2026-11-11T04:00:00Z", waits_for_answer: true } }), null);
  await step("s3CountedSomeUnasked", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 2, names: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], resending: true, not_asked: ["2026-11-10T03-05-00Z"] } }), null);
  await step("s3CountedWaits", Object.assign({}, s3c, { not_in_s3: { at: "2026-11-11T03:00:00Z", count: 2, names: ["2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"], resending: true, next_send: "2026-11-11T04:00:00Z" } }), null);
  await step("s3CountedNoLoop", Object.assign({}, s3c, { prune_loop: false, keep_in_force: 0 }), null);
  await step("s3CountedBlocked", Object.assign({}, s3c, { keep_blocked: true, keep_in_force: 0 }), null);
  await step("s3CountedKeep5", s3c, (r) => type(r, "keep_newest", "5"));
  await step("s3CountedKeepAll", s3c, (r) => type(r, "keep_newest", ""));
  // Two servers on one S3 prefix (#2658): the count does not apply.
  await step("s3SharedS3", Object.assign({}, s3c, { keep_blocked: true, keep_shared_s3: true, keep_in_force: 0 }), null);
  // The save that gives a server its first S3 destination: the daemon
  // answers with the S3 interval it started on the schedule.
  toasts.length = 0;
  ctx.__answer = { schedule_upload_set: "1d" };
  await step("firstS3", fresh, (r) => type(r, "baseline_s3", "s3://b/p/"));
  out.firstS3.toast = toasts.slice();
  toasts.length = 0;
  ctx.__answer = {};
  await step("laterSave", fresh, (r) => type(r, "keep_newest", "5"));
  out.laterSave.toast = toasts.slice();
  // The schedule card's rate sentence follows the listing's local_retention.
  vm.runInContext("capsCache.backup_schedule = true;", ctx);
  const text = (n) => { let t = ""; walk(n, (x) => { if (x.tag === "p" && x._text) t += x._text + " | "; }); return t; };
  const cur = { id: "s1", kind: "registry", baseline_s3: "" };
  const sch = { every: "1d", at: "03:00", runnable: true };
  out.rateKeep = { before: { words: [text(ctx.backupScheduleCard(cur, { schedule: sch, local_retention: { keep_newest: 3 } }))] } };
  out.rateAll = { before: { words: [text(ctx.backupScheduleCard(cur, { schedule: sch }))] } };
  // A session without servers:write sees the answers but no control.
  vm.runInContext("capsCache.permissions = { \"servers:write\": false };", ctx);
  const lockedRow = row({ baseline_dir: "/state/snapshots/s1", local_copy: true, keep_newest: 3, source: "server" }, true);
  const ctl = ["baseline_dir", "baseline_s3", "keep_newest"].map((n) => byName(lockedRow, n));
  out.locked = { before: { words: [] }, disabled: ctl.every((c) => c && c.disabled === true), saveDisabled: saveBtn(lockedRow) === null };
  vm.runInContext("capsCache.permissions = {};", ctx);
  console.log(JSON.stringify(out));
})().catch((e) => { console.log(JSON.stringify({ err: String(e && e.stack || e) })); });
`
	path := filepath.Join(t.TempDir(), "row.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type view struct {
		Words        []string `json:"words"`
		Reds         []string `json:"reds"`
		DirShown     *bool    `json:"dirShown"`
		KeepShown    *bool    `json:"keepShown"`
		Dir          string   `json:"dir"`
		SaveDisabled bool     `json:"saveDisabled"`
	}
	type res struct {
		Before view           `json:"before"`
		After  view           `json:"after"`
		Body   map[string]any `json:"body"`
		Toast  []string       `json:"toast"`
		// locked only
		Disabled     bool `json:"disabled"`
		SaveDisabled bool `json:"saveDisabled"`
	}
	var got map[string]res
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if e, ok := got["err"]; ok {
		t.Fatalf("the row threw: %+v", e)
	}
	joined := func(v view) string { return strings.Join(v.Words, " | ") }
	b := func(p *bool) bool { return p != nil && *p }

	// A new server as saved: yes, folder and count shown, the count said.
	f := got["freshAsIs"]
	if !b(f.Before.DirShown) || !b(f.Before.KeepShown) || !f.Before.SaveDisabled {
		t.Errorf("fresh server: folder/count not shown or Save awake with nothing changed: %+v", f.Before)
	}
	if !strings.Contains(joined(f.Before), "Older ones past that number are removed from this folder after each new snapshot, never a table's only copy.") {
		t.Errorf("fresh server does not say when older snapshots go: %q", joined(f.Before))
	}
	// Saving a number over a folder that already holds snapshots is the
	// choice to prune them (only a new or moved folder is refused), so the
	// sentence is there for a count that is only typed, too.
	if !strings.Contains(joined(got["freshKeep5"].After), "Older ones past that number are removed from this folder after each new snapshot") {
		t.Errorf("a typed count does not say older snapshots go after each new snapshot: %q", joined(got["freshKeep5"].After))
	}
	// How far back: the count x the schedule and nothing more (#2678: 15
	// minutes for 3 every 5, where it said an hour), never under the age
	// retention; "once" while the number is
	// not the one in force; no number without a schedule that runs.
	for name, want := range map[string]string{
		"reach5m":           "You can restore back up to about 15 minutes, to the oldest of the 3 kept.",
		"reachHourly":       "You can restore back up to about 3 hours, to the oldest of the 3 kept.",
		"reachDaily":        "You can restore back up to about 3 days",
		"reachDailyTyped1":  "After you save, you can restore back up to about 1 day, to the one snapshot kept.",
		"reachNone":         "You can restore back to the oldest of the 3 kept. Put the copy on a schedule (Update the copy, above) and this becomes a length of time.",
		"reachNoneOne":      "You can restore back to the one snapshot kept.",
		"reachRetain":       "You can restore back up to about 7 days",
		"reachScheduleOnly": "You can restore back to the oldest of the 3 kept.",
		"reachNotApplied":   "After you save, you can restore back up to about 3 hours",
	} {
		r := got[name]
		w := joined(r.Before)
		if name == "reachDailyTyped1" {
			w = joined(r.After)
		}
		if !strings.Contains(w, want) {
			t.Errorf("%s: %q does not say %q", name, w, want)
		}
	}
	if w := joined(got["freshKeepAll"].After); strings.Contains(w, "restore back") {
		t.Errorf("keeping everything gives a reach: %q", w)
	}
	if w := joined(got["held"].Before); !strings.Contains(w, "Another server's snapshots are still in this folder, so nothing here is removed. To keep only the newest, use a new empty folder.") || strings.Contains(w, "restore back") {
		t.Errorf("a held folder: %q", w)
	}
	if !strings.Contains(joined(f.Before), "only costs the tables that changed") {
		t.Errorf("fresh server does not say what the local copy saves: %q", joined(f.Before))
	}
	if strings.Contains(joined(got["freshNoReuse"].Before), "only costs the tables that changed") {
		t.Errorf("with reuse off at startup the saving is still promised: %q", joined(got["freshNoReuse"].Before))
	}
	if !strings.Contains(joined(got["freshNoLoop"].Before), "This copy of DBTrail removes nothing") {
		t.Errorf("where no prune loop runs the row still says snapshots are removed: %q", joined(got["freshNoLoop"].Before))
	}
	// The count, sent as a number; empty means keep them all.
	if k := got["freshKeep5"].Body["keep_newest"]; k != float64(5) || got["freshKeep5"].Body["local_copy"] != true {
		t.Errorf("keep 5 sent %+v", got["freshKeep5"].Body)
	}
	if k := got["freshKeepAll"].Body["keep_newest"]; k != float64(0) {
		t.Errorf("an emptied count sent %+v, want keep_newest 0", got["freshKeepAll"].Body)
	}
	if !strings.Contains(joined(got["freshKeepAll"].After), "Every snapshot stays on this machine") {
		t.Errorf("an emptied count does not say everything is kept: %q", joined(got["freshKeepAll"].After))
	}
	if got["freshKeepBad"].Body != nil {
		t.Errorf("a count that is not a whole number was sent: %+v", got["freshKeepBad"].Body)
	}
	// A bucket typed beside the folder: the folder and the count stay shown
	// (#2658), both are sent with local_copy true, and the row says what the
	// count will mean once saved.
	ws := got["freshWithS3"]
	if !b(ws.After.DirShown) || !b(ws.After.KeepShown) {
		t.Errorf("with S3 the folder or the count is hidden: %+v", ws.After)
	}
	if ws.Body["local_copy"] != true || ws.Body["baseline_s3"] != "s3://b/p/" || ws.Body["baseline_dir"] != "/state/snapshots/s1" || ws.Body["keep_newest"] != float64(9) {
		t.Errorf("folder + S3 + count sent %+v", ws.Body)
	}
	if !strings.Contains(joined(ws.After), "After you save, older ones past that number are removed once S3 has them.") {
		t.Errorf("a bucket typed beside a count does not say what the count will do: %q", joined(ws.After))
	}
	// A server with S3 and a count in force: every sentence it can say,
	// rendered from the fields the API sends, logged as the page shows them.
	for _, name := range []string{"s3Counted", "s3CountedHeld", "s3CountedAge", "s3CountedNotIn", "s3CountedUnasked", "s3CountedUnaskedTwo", "s3CountedSomeUnasked", "s3CountedReadRefusedAll", "s3CountedReadRefusedOne", "s3CountedWaitsForAnswer", "s3CountedNoLoop", "s3CountedBlocked", "s3CountedKeep5", "s3CountedKeepAll"} {
		r := got[name]
		t.Logf("%s (at rest):\n  %s", name, strings.Join(r.Before.Words, "\n  "))
		if name == "s3CountedKeep5" || name == "s3CountedKeepAll" {
			t.Logf("%s (after typing):\n  %s", name, strings.Join(r.After.Words, "\n  "))
		}
		if !b(r.Before.KeepShown) {
			t.Errorf("%s: the count is hidden on a server with S3", name)
		}
	}
	for name, want := range map[string]string{
		"s3Counted":               "Older ones past that number are removed from this folder after each new snapshot, once S3 has them.",
		"s3CountedHeld":           "An update the schedule did not send to S3 goes past that number without waiting for S3.",
		"s3CountedAge":            "Older ones past that number and older than 7 days are removed from this folder after each new snapshot, once S3 has them.",
		"s3CountedUnasked":        "Kept here: the copy of 2026-11-10 03:00:00 UTC. S3 could not be asked whether it has it; DBTrail sends it again at the next cleanup.",
		"s3CountedUnaskedTwo":     "Kept here: 2 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC). S3 could not be asked whether it has them; sending failed, and DBTrail tries again after 2026-11-11 04:00:00 UTC.",
		"s3CountedReadRefusedAll": "Kept here: 2 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC). S3 accepts uploads to this prefix but refuses to read them back (s3:GetObject), so DBTrail cannot confirm they are there, and does not send them again.",
		"s3CountedReadRefusedOne": "Kept here: the copy of 2026-11-10 03:00:00 UTC. S3 could not be asked whether it has it; DBTrail sends it again at the next cleanup. Not sent again: the copy of 2026-11-10 03:05:00 UTC, which S3 took but refuses to read back (s3:GetObject).",
		"s3CountedWaitsForAnswer": "Kept here: the copy of 2026-11-10 03:00:00 UTC. S3 could not be asked whether it has it; the last ones sent could not be confirmed, and DBTrail sends again after 2026-11-11 04:00:00 UTC.",
		"s3CountedSomeUnasked":    "Kept here: 2 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC). S3 does not have the copy of 2026-11-10 03:00:00 UTC complete, and could not be asked about the copy of 2026-11-10 03:05:00 UTC; DBTrail sends them again at the next cleanup.",
		"s3CountedWaits":          "Not in S3 yet, so kept here: 2 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC). S3 does not have them complete; sending failed, and DBTrail tries again after 2026-11-11 04:00:00 UTC.",
		"s3CountedNotIn":          "Not in S3 yet, so kept here: 2 older copies (2026-11-10 03:00:00 UTC, 2026-11-10 03:05:00 UTC). S3 does not have them complete; DBTrail sends them again at the next cleanup.",
		"s3CountedNoLoop":         "Keeps the newest 3 where DBTrail takes the snapshots. This copy of DBTrail removes nothing.",
		"s3CountedBlocked":        "This folder is shared with another server or is DBTrail's startup folder, so the count does not apply to it.",
	} {
		if w := joined(got[name].Before); !strings.Contains(w, want) {
			t.Errorf("%s: %q does not say %q", name, w, want)
		}
	}
	// A copy S3 could not be asked about is never said to be missing (#2689).
	for _, name := range []string{"s3CountedUnasked", "s3CountedUnaskedTwo", "s3CountedSomeUnasked", "s3CountedReadRefusedAll", "s3CountedReadRefusedOne", "s3CountedWaitsForAnswer"} {
		if w := joined(got[name].Before); strings.Contains(w, "Not in S3 yet") || strings.Contains(w, "does not have them") || strings.Contains(w, "does not have it ") {
			t.Errorf("%s: a copy S3 could not be asked about is said to be missing: %q", name, w)
		}
	}
	// What a server with S3 must never be told: that a table's only copy is
	// what bounds the removal (S3 having it is), or that the count is idle.
	for _, name := range []string{"s3Counted", "s3CountedHeld", "s3CountedAge", "s3CountedNotIn"} {
		w := joined(got[name].Before)
		if strings.Contains(w, "never a table's only copy") || strings.Contains(w, "Delete by age applies") || strings.Contains(w, "only costs the tables that changed") {
			t.Errorf("%s: a server with S3 is told %q", name, w)
		}
		// With S3 a restore reads the bucket, so the count is not what
		// bounds it (#2678): the line says so, and how much a moment
		// between two sends replays when the schedule spaces them.
		reach := "A restore reads S3: back to the oldest snapshot sent there."
		if name == "s3CountedHeld" {
			reach = "A restore reads S3: back to the oldest snapshot sent there, replaying up to 1 day of changes."
		}
		if !strings.Contains(w, reach) {
			t.Errorf("%s: does not say %q: %q", name, reach, w)
		}
		if strings.Contains(w, "You can restore back") {
			t.Errorf("%s: a server with S3 is told its reach is the local count's: %q", name, w)
		}
		t.Logf("%s reach line: %s", name, reach)
	}
	if w := joined(got["s3CountedHeld"].Before); strings.Contains(w, "stay here until a newer snapshot") {
		t.Errorf("the row still says held updates wait for a newer snapshot in S3: %q", w)
	}
	if w := joined(got["s3Counted"].Before); strings.Contains(w, "not sent to S3") {
		t.Errorf("a server whose schedule sends every update is told about updates kept here: %q", w)
	}
	if w := joined(got["s3CountedBlocked"].Before); strings.Contains(w, "after each new snapshot") || strings.Contains(w, "nothing in it is removed") || strings.Contains(w, "Set a number") {
		t.Errorf("a blocked folder with S3: %q", w)
	}
	// A destination another server also sends to: said, with the way out,
	// and no removal is promised.
	sh := joined(got["s3SharedS3"].Before)
	t.Logf("s3SharedS3 (at rest):\n  %s", strings.Join(got["s3SharedS3"].Before.Words[:1], "\n  "))
	if !strings.Contains(sh, "Another server sends its snapshots to this S3 location too, so the count does not apply to it. Give each server its own S3 prefix.") ||
		strings.Contains(sh, "after each new snapshot") || strings.Contains(sh, "Set a number") {
		t.Errorf("a destination shared with another server: %q", sh)
	}
	// The first S3 destination: the save's own message says what the
	// schedule now does. A save that started nothing says only that it saved.
	t.Logf("firstS3 toast: %q; laterSave toast: %q", got["firstS3"].Toast, got["laterSave"].Toast)
	if tt := got["firstS3"].Toast; len(tt) != 1 || tt[0] != "Saved for prod. Its schedule now sends a snapshot to S3 every 1d, not at each update. Change it under Update the copy." {
		t.Errorf("the save that set a first S3 destination said %q", tt)
	}
	if tt := got["laterSave"].Toast; len(tt) != 1 || tt[0] != "Saved for prod" {
		t.Errorf("a save that started nothing said %q", tt)
	}
	if k := got["s3CountedKeep5"].Body["keep_newest"]; k != float64(5) {
		t.Errorf("a count typed on a server with S3 sent %+v", got["s3CountedKeep5"].Body)
	}
	if k := got["s3CountedKeepAll"].Body["keep_newest"]; k != float64(0) || !strings.Contains(joined(got["s3CountedKeepAll"].After), "Every snapshot stays on this machine") {
		t.Errorf("an emptied count on a server with S3: sent %+v, says %q", got["s3CountedKeepAll"].Body, joined(got["s3CountedKeepAll"].After))
	}
	// The "no" answer left the page (#2431): no hidden radio carries one.
	if strings.Contains(jsFunctionSpan(t, readAsset(t, "app.js"), "backupServerRow"), "body.local_copy = false") {
		t.Error("backupServerRow can still send local_copy false")
	}
	// An S3-only server reads as yes since the cut (the local copy is always
	// on): its folder shows, prefilled with the default one, and the row says
	// that no copy is on this machine yet (Save stays asleep, so it must not
	// say "after you save"), and nothing about copies kept "here".
	so := got["s3onlyAsIs"]
	if !b(so.Before.DirShown) || so.Before.Dir != "/state/snapshots/s1" || !strings.Contains(joined(so.Before), "No copy on this machine yet.") ||
		strings.Contains(joined(so.Before), "Kept here") {
		t.Errorf("an S3-only server does not read as yes with the default folder: %+v", so.Before)
	}
	if b(so.Before.KeepShown) || so.Before.SaveDisabled != true {
		t.Errorf("an S3-only server at rest: the count is shown or Save is awake: %+v", so.Before)
	}
	// A count that is not a whole number blocks the save with an S3
	// destination typed too: the field is shown, so it is checked (#2658).
	if st := got["s3TypedAfterBadCount"]; st.Body != nil {
		t.Errorf("a count that is not a number was saved beside a bucket: %+v", st.Body)
	}
	// A folder the prune never counts says so instead of promising the count.
	if w := joined(got["blocked"].Before); !strings.Contains(w, "nothing in it is removed, whatever the count says") || strings.Contains(w, "after each new snapshot") {
		t.Errorf("a blocked folder: %q", w)
	}
	// A server with no location is not told about DBTrail's startup folder:
	// that folder backs no server since #1684.
	if w := joined(got["noLocation"].Before); strings.Contains(w, "startup folder") || strings.Contains(w, "Time-travel reads") {
		t.Errorf("a server with no location is told about the startup folder: %q", w)
	}

	// An existing local-only server keeps everything, and the row says so.
	if !strings.Contains(joined(got["oldLocal"].Before), "Every snapshot stays on this machine; nothing removes them.") {
		t.Errorf("an existing local server does not say it keeps everything: %q", joined(got["oldLocal"].Before))
	}
	if strings.Contains(joined(got["oldLocalBothYes"].Before), "only costs the tables that changed") {
		t.Errorf("a server with a folder AND a bucket is promised the saving, which a restore never gets: %q", joined(got["oldLocalBothYes"].Before))
	}

	// The schedule card's rate: a count in force is said, and the "never
	// removed" warning is not, since it would then be false.
	// The count itself is said once, above the snapshot list; the rate only
	// loses the warning that would then be false.
	if w := joined(got["rateKeep"].Before); strings.Contains(w, "never removed automatically") || strings.Contains(w, "keeps the newest") {
		t.Errorf("rate with a count in force: %q", w)
	}

	// A session without servers:write: every control disabled, and no Save
	// at all (hidden by permission, as on the rest of the page).
	if l := got["locked"]; !l.Disabled || !l.SaveDisabled {
		t.Errorf("a session without servers:write can edit the row: %+v", l)
	}

	// Every sentence this row can say, against the first-run walk's closed
	// banned-word list and the em-dash rule.
	banned := regexp.MustCompile(`(?i)\b(backups?|baselines?|index(es)?|sources?|consoles?|monitor(s|ing|ed)?|daemons?|watch(es|ing|ed)?|stream(s|ing|ed)?|track(s|ing|ed)?|protect(s|ing|ed|ion)?|preflight)\b|—`)
	// Only the sentences localCopyWords says (the rest of the row predates
	// #1681), and with folder paths taken out: a path is the operator's own
	// value, not copy.
	own := jsFunctionBody(t, readAsset(t, "app.js"), "localCopyWords")
	pathRe := regexp.MustCompile(`\S*/\S*`)
	checked := 0
	for name, r := range got {
		for _, w := range append(append([]string{}, r.Before.Words...), r.After.Words...) {
			if len(w) < 16 || !strings.Contains(own, w[:16]) {
				continue
			}
			checked++
			if m := banned.FindString(pathRe.ReplaceAllString(w, "")); m != "" {
				t.Errorf("%s: %q uses %q", name, w, m)
			}
		}
	}
	if checked < 8 {
		t.Errorf("only %d of the row's own sentences were checked for banned words; the filter lost them", checked)
	}
}

// TestNavigate_samePageGoesToItsTop pins the scroll reset the first-run walk
// needed once saving a snapshot folder stopped forcing a reload (#1681): a
// click on the page you are on goes to its top, and ONLY that case, because
// moving between pages keeps its behavior. The walk's clean run is the
// behavioral check (primary_below_fold_px measurable again on snapshot-ready);
// this keeps the shape from being simplified into a reset on every navigation.
func TestNavigate_samePageGoesToItsTop(t *testing.T) {
	body := jsFunctionBody(t, readAsset(t, "app.js"), "navigate")
	for _, want := range []string{`const samePage = routeFromLocation() === route;`, `if (push && samePage && !hash) {`, `main.scrollTop = 0;`} {
		if !strings.Contains(body, want) {
			t.Errorf("navigate lost %q", want)
		}
	}
	if strings.Index(body, "const samePage") > strings.Index(body, "history.pushState") {
		t.Error("samePage is read after pushState, when the address is already the new one: every navigation would count as the same page")
	}
}
