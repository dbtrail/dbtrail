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
// "no" must never send the folder or the count sitting in the hidden inputs,
// and a "yes" must never send a count for a server that has an S3
// destination, where the count does nothing.
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
vm.runInContext("api = (u, o) => { __calls.push({ u, body: o.body }); return Promise.resolve({}); }; renderSnapshots = () => Promise.resolve(); toast = () => {}; capsCache.monitor = true;", ctx);
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
  out[name] = { before, after, body: calls.length ? calls[0].body : null };
}
const pick = (r, v) => { const y = byName(r, "bks-local-s1", "yes"), n = byName(r, "bks-local-s1", "no"); y.checked = v === "yes"; n.checked = v === "no"; fire(v === "yes" ? y : n, "change"); };
const type = (r, name, v) => { const i = byName(r, name); i.value = v; fire(i, "input"); };
(async () => {
  // A new server: its own folder, a count, no S3.
  const fresh = { baseline_dir: "/state/snapshots/s1", local_copy: true, keep_newest: 3, source: "server" };
  await step("freshAsIs", fresh, null);
  await step("freshKeep5", fresh, (r) => type(r, "keep_newest", "5"));
  await step("freshKeepAll", fresh, (r) => type(r, "keep_newest", ""));
  await step("freshKeepBad", fresh, (r) => type(r, "keep_newest", "2.5"));
  await step("freshNoWithoutS3", fresh, (r) => pick(r, "no"));
  await step("freshNoWithS3", fresh, (r) => { pick(r, "no"); type(r, "baseline_s3", "s3://b/p/"); type(r, "keep_newest", "9"); });
  await step("freshNoLoop", Object.assign({}, fresh, { prune_loop: false }), null);
  await step("freshNoReuse", fresh, null, false);
  // An existing S3-only server answering yes: the default folder, no count sent.
  const s3only = { baseline_s3: "s3://b/p/", source: "server" };
  await step("s3onlyAsIs", s3only, null);
  await step("s3onlyYes", s3only, (r) => pick(r, "yes"));
  // An existing server with nothing: a toggle elsewhere must not send a no.
  await step("bareArchiveToggle", {}, (r) => { const c = find(r, (n) => n.tag === "input" && n.attrs.name === "no_archive"); c.checked = true; fire(c, "change"); });
  // An existing local-only server that keeps everything.
  await step("oldLocal", { baseline_dir: "/srv/snaps", local_copy: true, source: "server" }, null);
  await step("s3TypedAfterBadCount", fresh, (r) => { type(r, "keep_newest", "x"); type(r, "baseline_s3", "s3://b/p/"); });
  await step("blocked", Object.assign({}, fresh, { keep_blocked: true }), null);
  await step("held", Object.assign({}, fresh, { keep_blocked: true, keep_held: true }), null);
  // How far back the count reaches, from the real schedule fields the
  // settings API sends (#1681), with the count in force as the listing has it.
  const inForce = Object.assign({}, fresh, { keep_in_force: 3 });
  // snapshot_every_minutes is the server's answer (the shorter of the
  // schedule and the refresh loop, where each runs); the page never works
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
  await step("daemonDefault", { source: "default" }, null);
  await step("oldLocalBothYes", { baseline_dir: "/srv/snaps", baseline_s3: "s3://b/p/", local_copy: true, source: "server" }, null);
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
  const ctl = ["baseline_dir", "baseline_s3", "keep_newest", "no_archive"].map((n) => byName(lockedRow, n)).concat([byName(lockedRow, "bks-local-s1", "yes"), byName(lockedRow, "bks-local-s1", "no")]);
  pick(lockedRow, "no");
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
	if !strings.Contains(joined(f.Before), "Keeps the newest 3 here. Older ones in this folder are removed at the next hourly cleanup, never a table's only copy.") {
		t.Errorf("fresh server does not say its count and when older snapshots go: %q", joined(f.Before))
	}
	// Saving a number over a folder that already holds snapshots is the
	// choice to prune them (only a new or moved folder is refused), so the
	// sentence is there for a count that is only typed, too.
	if !strings.Contains(joined(got["freshKeep5"].After), "Keeps the newest 5 here. Older ones in this folder are removed at the next hourly cleanup") {
		t.Errorf("a typed count does not say older snapshots go at the next hourly cleanup: %q", joined(got["freshKeep5"].After))
	}
	// How far back: the count x the schedule, never under the hour the prune
	// leaves alone nor under the age retention; "once" while the number is
	// not the one in force; no number without a schedule that runs.
	for name, want := range map[string]string{
		"reach5m":           "You can go back up to about 1 hour: restores, .sql exports and full-table time travel start from the oldest snapshot kept.",
		"reachHourly":       "You can go back up to about 3 hours:",
		"reachDaily":        "You can go back up to about 3 days:",
		"reachDailyTyped1":  "Once this number applies, you can go back up to about 1 day:",
		"reachNone":         "You can go back as far as the oldest of the 3 kept. Nothing here takes snapshots on a timer, so that depends on when they are taken.",
		"reachNoneOne":      "You can go back as far as the one snapshot kept.",
		"reachRetain":       "You can go back up to about 7 days:",
		"reachScheduleOnly": "You can go back as far as the oldest of the 3 kept.",
		"reachNotApplied":   "Once this number applies, you can go back up to about 3 hours:",
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
	if w := joined(got["freshKeepAll"].After); strings.Contains(w, "go back") {
		t.Errorf("keeping everything gives a reach: %q", w)
	}
	if w := joined(got["held"].Before); !strings.Contains(w, "Another server's snapshots are still in this folder, so nothing here is removed. To keep only the newest, use a new empty folder.") || strings.Contains(w, "go back") {
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
	// No without S3: red, and what is sent is refused by the server, never a silent no-copy.
	nw := got["freshNoWithoutS3"]
	if b(nw.After.DirShown) || b(nw.After.KeepShown) {
		t.Errorf("no still shows the folder or the count: %+v", nw.After)
	}
	if len(nw.After.Reds) == 0 || !strings.Contains(strings.Join(nw.After.Reds, " "), "With neither, this server keeps no snapshots at all") {
		t.Errorf("no without S3 does not say there would be no snapshots: %q", joined(nw.After))
	}
	// No with S3: the issue's own words, the old copies named, and NOTHING of
	// the hidden fields in what is sent.
	ns := got["freshNoWithS3"]
	if !strings.Contains(joined(ns.After), "Snapshots live only in S3, and every run writes every table.") {
		t.Errorf("no does not say it in those words: %q", joined(ns.After))
	}
	if !strings.Contains(joined(ns.After), "The snapshots already in /state/snapshots/s1 stay there") {
		t.Errorf("no does not say what happens to the snapshots already here: %q", joined(ns.After))
	}
	if ns.Body["local_copy"] != false || ns.Body["baseline_s3"] != "s3://b/p/" {
		t.Errorf("no + S3 sent %+v", ns.Body)
	}
	for _, k := range []string{"baseline_dir", "keep_newest"} {
		if _, ok := ns.Body[k]; ok {
			t.Errorf("no sent the hidden %s: %+v", k, ns.Body)
		}
	}
	// An S3-only server: no count field, and answering yes asks for the default folder.
	so := got["s3onlyAsIs"]
	if b(so.Before.DirShown) || !strings.Contains(joined(so.Before), "Snapshots live only in S3") {
		t.Errorf("an S3-only server does not read as no: %+v", so.Before)
	}
	sy := got["s3onlyYes"]
	if sy.Body["local_copy"] != true || sy.Body["baseline_dir"] != "/state/snapshots/s1" {
		t.Errorf("yes on an S3-only server sent %+v, want the default folder", sy.Body)
	}
	if _, ok := sy.Body["keep_newest"]; ok {
		t.Errorf("yes with an S3 destination sent a count: %+v", sy.Body)
	}
	if b(sy.After.KeepShown) || !strings.Contains(joined(sy.After), "each snapshot is also sent to S3") {
		t.Errorf("yes with S3: the count is shown or the S3 copy is not said: %+v", sy.After)
	}
	// A count typed and then hidden by an S3 destination is neither checked
	// nor sent: hidden, it does nothing.
	if st := got["s3TypedAfterBadCount"]; st.Body == nil || st.Body["keep_newest"] != nil {
		t.Errorf("a hidden count blocked the save or was sent: %+v", st.Body)
	}
	// A folder the prune never counts says so instead of promising the count.
	if w := joined(got["blocked"].Before); !strings.Contains(w, "nothing in it is removed, whatever the count says") || strings.Contains(w, "Keeps the newest") {
		t.Errorf("a blocked folder: %q", w)
	}
	// A server read from DBTrail's startup folder is not told it has nothing.
	if d := got["daemonDefault"]; len(d.Before.Reds) != 0 || !strings.Contains(joined(d.Before), "Time-travel reads DBTrail's startup folder") {
		t.Errorf("a server on the startup folder: %+v", d.Before)
	}

	// A server with nothing: an unrelated toggle does not send a no (which
	// the server would refuse for want of S3).
	if _, ok := got["bareArchiveToggle"].Body["local_copy"]; ok {
		t.Errorf("an archive toggle on a server with nothing sent local_copy: %+v", got["bareArchiveToggle"].Body)
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
	if w := joined(got["rateKeep"].Before); !strings.Contains(w, "This machine keeps the newest 3 snapshots and removes older ones.") || strings.Contains(w, "never removed automatically") {
		t.Errorf("rate with a count in force: %q", w)
	}
	if w := joined(got["rateAll"].Before); !strings.Contains(w, "never removed automatically") {
		t.Errorf("rate without a count lost its disk warning: %q", w)
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
