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
const draw = (bb, cov, acts) => { const h = hero(bb, cov, cur, acts || {}); return { text: flat(h).join(" "), cls: h.children.map((c) => c.className) }; };
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
  empty: tone({ configured: true, snapshots: [] }, { freshness: "current" }),
  drawnFresh: draw(b(0.1, sched("5m")), { freshness: "current" }),
  drawnStopped: draw(b(0.1, sched("5m")), { freshness: "stalled" }, { settings: () => {} }),
  drawnNoSchedule: draw(b(30, null), null, { settings: () => {} }),
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
		Text string
		Cls  []string
	}
	var got struct {
		Fresh, Late, VeryLate, Hourly, Stopped, Gap, FailedRun, FailedRefresh grade
		NoSchedule, ScheduleOff, NoCoverage, Empty                             grade
		DrawnFresh, DrawnStopped, DrawnNoSchedule, DrawnUnset, DrawnS3         drawn
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
		"capture stalled, young copy":     {got.Stopped, "bad", "Capture is stopped"},
		"capture lost position":           {got.Gap, "bad", "Capture is stopped"},
		"last scheduled run failed":       {got.FailedRun, "bad", "capture gap"},
		"refresh loop failed, no schedule": {got.FailedRefresh, "bad", "boom"},
		"no schedule":                     {got.NoSchedule, "none", ""},
		"schedule turned off":             {got.ScheduleOff, "none", ""},
		"coverage unreadable":             {got.NoCoverage, "ok", ""},
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
	if !strings.Contains(got.DrawnStopped.Text, "Capture is stopped") || !strings.Contains(got.DrawnStopped.Text, "Settings ›") ||
		!hasString(got.DrawnStopped.Cls, "hero-card hero-age bad") {
		t.Errorf("stopped hero: %q %q", got.DrawnStopped.Text, got.DrawnStopped.Cls)
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
out.offered = all.bar.children.filter((c) => c.tag === "button").map((c) => c._text);
all.select("checks");
out.afterClick = { shown: Object.keys(all.panels).filter((k) => !all.panels[k].hidden), on: all.bar.children.filter((c) => c.attrs && c.attrs["aria-selected"] === "true").map((c) => c._text), written: written.slice() };
written = [];
all.select("settings", true);
out.quiet = { shown: Object.keys(all.panels).filter((k) => !all.panels[k].hidden), written: written.slice() };
const serve = tabs({ checks: false, settings: true });
out.serveOffered = serve.bar.children.filter((c) => c.tag === "button").map((c) => c._text);
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
