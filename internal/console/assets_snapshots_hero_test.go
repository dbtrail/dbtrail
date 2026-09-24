package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The Snapshots hero (round 3) SHOWS the copy instead of listing facts about
// it: the age of the newest snapshot, coloured by the schedule and by the
// capture, where it lives, and how many copies the week holds. These pin the
// colour rule, which is the one part a reader acts on without reading: a
// young copy of a STOPPED capture is pink, never mint, because the copy
// updates from an index that no longer moves; a failed run is pink whatever
// the age; and with no schedule there is no colour to be wrong about.
func TestSnapshotHeroColoursTheAgeByScheduleAndCapture(t *testing.T) {
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
const tone = vm.runInContext("snapshotHeroTone", ctx);
const hero = vm.runInContext("snapshotHero", ctx);
const flat = (n, out = []) => { if (!n) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const snap = (ageH) => ({ time: "2026-06-10 12:00:00", age_hours: ageH, kinds: ["dir"] });
const sched = (every, extra) => Object.assign({ every: every, runnable: true, next_run: "2026-06-10T13:00:00Z" }, extra || {});
const b = (ageH, schedule, extra) => Object.assign({ configured: true, source: "/tmp/b", kind: "dir", snapshots: [snap(ageH)], schedule: schedule }, extra || {});
const cur = { id: "s1", name: "prod", kind: "registry", has_source: true, baseline_dir: "/tmp/b" };
vm.runInContext("capsCache = { monitor: false, baseline_trigger: true };", ctx);
const words = vm.runInContext("vfyVerdictWords", ctx);
const draw = (bb, cov, acts, who) => { const h = hero(bb, cov, who || cur, acts || {}); return { text: flat(h).join(" "), cls: h.children.map((c) => c.className), tiles: h.children[1] ? h.children[1].children.map((c) => c.className) : [] }; };
const rec = (verdict, sum, state) => ({ state: state || "succeeded", verdict: verdict, finished_at: "2026-06-10T12:00:00Z", summary: sum || {} });
console.log(JSON.stringify({
  fresh: tone(b(0.1, sched("5m")), { freshness: "current" }),
  late: tone(b(0.2, sched("5m")), { freshness: "current" }),
  veryLate: tone(b(2, sched("5m")), { freshness: "current" }),
  hourly: tone(b(1.2, sched("1h")), { freshness: "current" }),
  stopped: tone(b(0.1, sched("5m")), { freshness: "stalled" }),
  gap: tone(b(0.1, sched("5m")), { freshness: "current", continuity: "gap_lost" }),
  failedRun: tone(b(0.1, sched("5m", { last_run: { ok: false, error: "capture gap" } })), { freshness: "current" }),
  failedRefresh: tone(b(0.1, null, { refresh: { state: "failed", last_error: "boom" } }), { freshness: "current" }),
  noSchedule: tone(b(30, null), { freshness: "current" }),
  scheduleOff: tone(b(30, sched("5m", { runnable: false })), { freshness: "current" }),
  noCoverage: tone(b(0.1, sched("5m")), null),
  covUnknown: tone(b(0.1, sched("5m")), { freshness: "unknown" }),
  supervisorStopped: tone(b(0.1, sched("5m")), { freshness: "current" }, { kind: "registry", monitor_state: "stopped" }),
  nextSkips: tone(b(0.1, sched("5m", { next_method_error: "no folder" })), { freshness: "current" }),
  skipped: tone(b(0.1, sched("5m", { last_skipped: { at: "2026-06-10T12:00:00Z", reason: "capture gap" } })), { freshness: "current" }),
  refreshWithSchedule: tone(b(0.1, sched("5m"), { refresh: { state: "failed", last_error: "boom" } }), { freshness: "current" }),
  noAge: tone({ configured: true, snapshots: [{ time: "2026-06-10 12:00:00", kinds: ["dir"] }], schedule: sched("5m") }, { freshness: "current" }),
  empty: tone({ configured: true, snapshots: [] }, { freshness: "current" }),
  drawnScheduleOff: draw(b(30, sched("5m", { runnable: false, reason: "snapshot creation is off" })), { freshness: "current" }, { settings: () => {} }),
  drawnEmpty: draw({ configured: true, source: "/tmp/b", kind: "dir", sources: [{ kind: "dir", source: "/tmp/b" }], snapshots: [] }, { freshness: "current" }),
  drawnNoKinds: draw({ configured: true, source: "/tmp/b", kind: "dir", sources: [{ kind: "dir", source: "/tmp/b" }, { kind: "s3", source: "s3://b/p" }], snapshots: [{ time: "2026-06-10 12:00:00", age_hours: 0.1 }] }, { freshness: "current" }),
  wordsVerified: words(rec("verified", { match: 3 })),
  wordsUnproven: words(rec("unproven", { match: 0, inconclusive: 3 })),
  wordsNoPred: words(rec("no_predecessor", {})),
  wordsMismatch: words(rec("mismatch", { match: 2, mismatch: 1 })),
  wordsFailed: words(rec("", {}, "failed")),
  wordsNone: words(null, "none"),
  drawnFresh: draw(b(0.1, sched("5m")), { freshness: "current" }),
  drawnStopped: draw(b(0.1, sched("5m")), { freshness: "stalled" }, { settings: () => {} }),
  drawnNoSchedule: draw(b(30, null), { freshness: "current" }, { settings: () => {} }),
  drawnUnset: draw({ configured: false }, null, { settings: () => {} }),
  drawnS3: draw(b(0.1, sched("5m"), { sources: [{ kind: "dir", source: "/tmp/b" }, { kind: "s3", source: "s3://b/p" }], snapshots: [{ time: "2026-06-10 12:00:00", age_hours: 0.1, kinds: ["s3"] }] }), { freshness: "current" }),
}));
`
	path := filepath.Join(t.TempDir(), "hero.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type grade struct{ Tone, Why string }
	type drawn struct {
		Text  string
		Cls   []string
		Tiles []string
	}
	type verdictWords struct{ State, Title, Line, When, Mark string }
	var got struct {
		Fresh, Late, VeryLate, Hourly, Stopped, Gap, FailedRun, FailedRefresh    grade
		NoSchedule, ScheduleOff, NoCoverage, CovUnknown, SupervisorStopped, Empty grade
		NextSkips, Skipped, RefreshWithSchedule, NoAge                            grade
		DrawnFresh, DrawnStopped, DrawnNoSchedule, DrawnUnset, DrawnS3            drawn
		DrawnScheduleOff, DrawnEmpty, DrawnNoKinds                                drawn
		WordsVerified, WordsUnproven, WordsNoPred, WordsMismatch, WordsFailed     verdictWords
		WordsNone                                                                 verdictWords
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	for name, c := range map[string]struct {
		got  grade
		tone string
		why  string
	}{
		"fresh (6 min at every 5)":        {got.Fresh, "ok", ""},
		"late (12 min at every 5)":        {got.Late, "warn", "Later"},
		"very late (2 h at every 5)":      {got.VeryLate, "bad", "Much later"},
		"hourly, 72 min":                  {got.Hourly, "ok", ""},
		"capture stalled, young copy":     {got.Stopped, "bad", "Capture is stalled"},
		"capture lost position":           {got.Gap, "bad", "Capture is lost"},
		"last scheduled run failed":       {got.FailedRun, "bad", "capture gap"},
		"refresh loop failed, no schedule": {got.FailedRefresh, "bad", "boom"},
		"no schedule":                     {got.NoSchedule, "none", ""},
		// A saved schedule the daemon cannot run is the silent failure the
		// schedule DTO exists to surface: never "no schedule" in grey.
		"schedule cannot run":              {got.ScheduleOff, "bad", "cannot run"},
		"next slot will not start":         {got.NextSkips, "bad", "next run cannot start"},
		"last slot skipped":                {got.Skipped, "bad", "did not start"},
		"manual read failed, schedule on":  {got.RefreshWithSchedule, "bad", "boom"},
		// Unknown capture is never mint: a young copy of a stopped capture
		// is the case the colour exists for, and unread is not "fine".
		"coverage unreadable":             {got.NoCoverage, "warn", "could not be read"},
		"coverage unknown":                {got.CovUnknown, "warn", "could not be read"},
		// The supervisor knows a stopped stream before the index does.
		"supervisor says stopped":         {got.SupervisorStopped, "bad", "Capture is stopped"},
		"no age on the wire":              {got.NoAge, "none", ""},
		"no snapshot":                     {got.Empty, "none", ""},
	} {
		if c.got.Tone != c.tone || !strings.Contains(c.got.Why, c.why) || (c.why == "" && c.got.Why != "") {
			t.Errorf("%s: tone %q why %q, want %q containing %q", name, c.got.Tone, c.got.Why, c.tone, c.why)
		}
	}
	// What the reader sees: the age, the place, the rhythm, and for a
	// stopped capture the reason with the way to the settings.
	if !strings.Contains(got.DrawnFresh.Text, "ago") || !strings.Contains(got.DrawnFresh.Text, "every 5 min") ||
		!strings.Contains(got.DrawnFresh.Text, "On disk") || !strings.Contains(got.DrawnFresh.Text, "No S3") ||
		!strings.Contains(got.DrawnFresh.Text, "1 in all") || !hasString(got.DrawnFresh.Cls, "hero-card hero-age ok") {
		t.Errorf("fresh hero: %q %q", got.DrawnFresh.Text, got.DrawnFresh.Cls)
	}
	// A stopped capture is fixed on Overview, not under this page's
	// Settings: the link goes where the fix is.
	if !strings.Contains(got.DrawnStopped.Text, "Capture is stalled") || !strings.Contains(got.DrawnStopped.Text, "Overview ›") ||
		strings.Contains(got.DrawnStopped.Text, "Settings ›") || !hasString(got.DrawnStopped.Cls, "hero-card hero-age bad") {
		t.Errorf("stopped hero: %q %q", got.DrawnStopped.Text, got.DrawnStopped.Cls)
	}
	if !strings.Contains(got.DrawnScheduleOff.Text, "The schedule cannot run: snapshot creation is off") || !strings.Contains(got.DrawnScheduleOff.Text, "Settings ›") ||
		strings.Contains(got.DrawnScheduleOff.Text, "Not on a schedule") {
		t.Errorf("a stuck schedule must be named, never \"not on a schedule\": %q", got.DrawnScheduleOff.Text)
	}
	// No copy yet: the places are set but not lit ("on" means the newest
	// copy is there). A listing that does not say where the newest one is:
	// the same.
	for name, d := range map[string]drawn{"no snapshot": got.DrawnEmpty, "no kinds on the wire": got.DrawnNoKinds} {
		for _, c := range d.Tiles {
			if strings.Contains(c, " on") {
				t.Errorf("%s: a tile is lit: %q", name, d.Tiles)
			}
		}
		if len(d.Tiles) != 2 {
			t.Errorf("%s: want two tiles, got %q", name, d.Tiles)
		}
	}
	// The verdict words, shared by the hero tile and the Checks card: a
	// tick only for "verified"; a run that compared nothing is never green.
	for name, c := range map[string]struct {
		got   verdictWords
		state string
		title string
	}{
		"verified":       {got.WordsVerified, "ok", "The copy matches"},
		"unproven":       {got.WordsUnproven, "warn", "Nothing proven"},
		"no predecessor": {got.WordsNoPred, "none", "Nothing to compare yet"},
		"mismatch":       {got.WordsMismatch, "bad", "1 table differs"},
		"failed":         {got.WordsFailed, "bad", "The check failed"},
		"never":          {got.WordsNone, "none", "Never checked"},
	} {
		if c.got.State != c.state || c.got.Title != c.title || (c.state == "ok") != (c.got.Mark == "check" && c.got.State == "ok") {
			t.Errorf("%s: %+v, want state %q title %q", name, c.got, c.state, c.title)
		}
	}
	if !strings.Contains(got.DrawnNoSchedule.Text, "Not on a schedule") || !strings.Contains(got.DrawnNoSchedule.Text, "Settings ›") {
		t.Errorf("no-schedule hero does not say so and point at Settings: %q", got.DrawnNoSchedule.Text)
	}
	if !strings.Contains(got.DrawnUnset.Text, "not set up") || !strings.Contains(got.DrawnUnset.Text, "Settings ›") {
		t.Errorf("unconfigured hero: %q", got.DrawnUnset.Text)
	}
	// Two places set, the newest copy only in S3: the S3 tile is lit, the
	// disk tile is set-but-dim, and neither says "No".
	if !strings.Contains(got.DrawnS3.Text, "In S3") || !strings.Contains(got.DrawnS3.Text, "On disk") || strings.Contains(got.DrawnS3.Text, "No S3") {
		t.Errorf("two locations: %q", got.DrawnS3.Text)
	}
}

// The tab bar: the address names the tab on arrival (the old pages'
// addresses included), a click writes the address, and a panel this daemon
// cannot serve is not offered.
func TestSnapshotTabsFollowTheAddress(t *testing.T) {
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
const tabs = vm.runInContext("snapshotTabs", ctx);
const fromHash = vm.runInContext("snapTabFromHash", ctx);
let written = [];
ctx.history = { replaceState: (st, title, url) => written.push(url), state: null };
const out = {};
const hashes = {};
for (const h of ["", "#checks", "#setup", "#settings", "#versions", "#past"]) { ctx.location.hash = h; hashes[h] = fromHash(); }
out.hashes = hashes;
const all = tabs({ checks: true, settings: true });
const tabsOf = (t) => t.bar.children[0].children.filter((c) => c.tag === "button");
out.offered = tabsOf(all).map((c) => c._text);
all.select("checks");
out.afterClick = { shown: Object.keys(all.panels).filter((k) => !all.panels[k].hidden), on: tabsOf(all).filter((c) => c.attrs && c.attrs["aria-selected"] === "true").map((c) => c._text), written: written.slice() };
written = [];
all.select("settings", true);
out.quiet = { shown: Object.keys(all.panels).filter((k) => !all.panels[k].hidden), written: written.slice() };
const serve = tabs({ checks: false, settings: true });
out.serveOffered = tabsOf(serve).map((c) => c._text);
serve.select("checks");
out.serveFallback = Object.keys(serve.panels).filter((k) => !serve.panels[k].hidden);
console.log(JSON.stringify(out));
`
	path := filepath.Join(t.TempDir(), "tabs.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		Hashes     map[string]string
		Offered    []string
		AfterClick struct {
			Shown, On, Written []string
		}
		Quiet struct{ Shown, Written []string }
		ServeOffered, ServeFallback []string
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	want := map[string]string{"": "", "#checks": "checks", "#setup": "settings", "#settings": "settings", "#versions": "versions", "#past": ""}
	for h, w := range want {
		if got.Hashes[h] != w {
			t.Errorf("hash %q opens tab %q, want %q", h, got.Hashes[h], w)
		}
	}
	if strings.Join(got.Offered, ",") != "Versions,Checks,Settings" {
		t.Errorf("tabs offered: %q", got.Offered)
	}
	if strings.Join(got.AfterClick.Shown, ",") != "checks" || strings.Join(got.AfterClick.On, ",") != "Checks" ||
		len(got.AfterClick.Written) != 1 || !strings.HasSuffix(got.AfterClick.Written[0], "#checks") {
		t.Errorf("after a click on Checks: %+v", got.AfterClick)
	}
	if strings.Join(got.Quiet.Shown, ",") != "settings" || len(got.Quiet.Written) != 0 {
		t.Errorf("a quiet select must not write the address: %+v", got.Quiet)
	}
	if strings.Join(got.ServeOffered, ",") != "Versions,Settings" || strings.Join(got.ServeFallback, ",") != "versions" {
		t.Errorf("serve: offered %q, selecting a missing tab shows %q", got.ServeOffered, got.ServeFallback)
	}
}

// A server whose index database is not created yet (MySQL 1049) is the
// state every new server starts in: the hero says what to set up and where,
// in grey, never the driver's sentence in pink.
func TestSnapshotHeroSaysWhereToStartWhenTheIndexIsMissing(t *testing.T) {
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
const hero = vm.runInContext("snapshotHero", ctx);
const flat = (n, out = []) => { if (!n) return out; if (typeof n === "string") { out.push(n); return out; }
  if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const cur = { id: "s1", name: "wp", kind: "registry", has_source: true };
vm.runInContext("capsCache = { monitor: true, baseline_trigger: true };", ctx);
const missing = hero({ error: "server \"wp\": failed to ping MySQL: Error 1049 (42000): Unknown database 'bintrail_idx_5122'" }, null, cur, {});
const other = hero({ error: "server \"wp\": failed to ping MySQL: dial tcp: connection refused" }, null, cur, {});
console.log(JSON.stringify({ missing: { text: flat(missing).join(" "), cls: missing.children[0].className },
  other: { text: flat(other).join(" "), cls: other.children[0].className } }));
`
	path := filepath.Join(t.TempDir(), "idx.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct{ Missing, Other struct{ Text, Cls string } }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if !strings.Contains(got.Missing.Text, "not indexing yet") || !strings.Contains(got.Missing.Text, "Go to Servers and press Start") ||
		!strings.Contains(got.Missing.Text, "Servers ›") || strings.Contains(got.Missing.Text, "1049") || got.Missing.Cls != "hero-card hero-age none" {
		t.Errorf("missing index: %q %q", got.Missing.Text, got.Missing.Cls)
	}
	if !strings.Contains(got.Other.Text, "could not load") || !strings.Contains(got.Other.Text, "connection refused") || got.Other.Cls != "hero-card hero-age bad" {
		t.Errorf("another failure must stay a failure: %q %q", got.Other.Text, got.Other.Cls)
	}
}
