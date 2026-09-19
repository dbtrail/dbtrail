package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestBackupOffIsSaidWhereTheActionWouldBe is #1677, drawn by the real page
// code: when the console cannot create a full backup, the Getting started card
// lists the backup step with the reason (from a report Go actually marshals),
// and the Backups page strip says why where the Create backup button would be:
// creation is off, the server has no backup location of its own (the daemon's
// shared default counts for listing backups, not for writing one), or both.
// A server with no source gets no note, and the command-line server none.
func TestBackupOffIsSaidWhereTheActionWouldBe(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	yes := true
	marshal := func(off, noLoc, pg bool) string {
		b, err := json.Marshal(firstRunSteps(firstRunInput{Monitor: MonitorStatus{State: "running", SourceConnected: true},
			IndexExists: &yes, SnapshotTaken: !pg, StreamStarted: true, Postgres: pg, BackupOff: off, BackupNoLocation: noLoc}))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n, out = []) => { if (!n) return out; if (n.nodeType === 3) { out.push(n.textContent); return out; }
  if (n._text) out.push(n._text); for (const c of n.children || []) flat(c, out); return out; };
const last = (n) => { const rows = []; const walk = (x) => { if (!x) return; if ((x.className || "").includes("fr-step ")) { rows.push(x); return; } (x.children || []).forEach(walk); }; walk(n);
  const r = rows[rows.length - 1]; return r ? { cls: r.className, text: flat(r).join(" ") } : null; };
const card = vm.runInContext("firstRunCard", ctx);
const strip = vm.runInContext("baselineContextStrip", ctx);
const buttons = (n, out = []) => { if (!n) return out; if (n.tag === "button") out.push(n._text); (n.children || []).forEach((c) => buttons(c, out)); return out; };
const drawStrip = (caps, b, cur) => { vm.runInContext("capsCache = " + JSON.stringify(caps) + ";", ctx); const s = strip(b, cur); return { text: flat(s).join(" "), buttons: buttons(s) }; };
const reg = { id: "s1", name: "prod", kind: "registry", has_source: true, baseline_dir: "/var/lib/bintrail/baselines" };
const shared = { id: "s2", name: "shared", kind: "registry", has_source: true };
const nosrc = { id: "s3", name: "byo", kind: "registry", baseline_dir: "/var/lib/bintrail/baselines" };
const s3only = { id: "s4", name: "s3only", kind: "registry", has_source: true, baseline_s3: "s3://b/p" };
const cfg = { configured: true, source: "/var/lib/bintrail/baselines", snapshots: [] };
console.log(JSON.stringify({
  off: last(card(` + marshal(true, false, false) + `)),
  offNoLoc: last(card(` + marshal(true, true, false) + `)),
  offPG: last(card(` + marshal(true, false, true) + `)),
  noLoc: last(card(` + marshal(false, true, false) + `)),
  stripOff: drawStrip({ monitor: true, baseline_trigger: false }, cfg, reg),
  stripOn: drawStrip({ monitor: true, baseline_trigger: true }, cfg, reg),
  stripOffBoot: drawStrip({ monitor: true, baseline_trigger: false }, cfg, { id: "default", name: "cli", kind: "ephemeral", has_source: true, baseline_dir: "/var/lib/bintrail/baselines" }),
  stripOffUnconfigured: drawStrip({ monitor: true, baseline_trigger: false }, { configured: false }, reg),
  stripOnShared: drawStrip({ monitor: true, baseline_trigger: true }, cfg, shared),
  stripOffShared: drawStrip({ monitor: true, baseline_trigger: false }, cfg, shared),
  stripOffNoSource: drawStrip({ monitor: true, baseline_trigger: false }, cfg, nosrc),
  stripOnNoSource: drawStrip({ monitor: true, baseline_trigger: true }, cfg, nosrc),
  stripOnS3Only: drawStrip({ monitor: true, baseline_trigger: true }, cfg, s3only),
  stripOffS3Only: drawStrip({ monitor: true, baseline_trigger: false }, cfg, s3only),
}));
`
	path := filepath.Join(t.TempDir(), "backupoff.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	type row struct{ Cls, Text string }
	type drawnStrip struct {
		Text    string
		Buttons []string
	}
	var got struct {
		Off, OffNoLoc, OffPG, NoLoc                                      *row
		StripOff, StripOn, StripOffBoot, StripOffUnconfigured            drawnStrip
		StripOnShared, StripOffShared, StripOffNoSource, StripOnNoSource drawnStrip
		StripOnS3Only, StripOffS3Only                                    drawnStrip
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	// Printed so a reviewer reads the sentences as the page draws them.
	t.Logf("off:        %s", got.Off.Text)
	t.Logf("off+no loc: %s", got.OffNoLoc.Text)
	t.Logf("off, PG:    %s", got.OffPG.Text)
	t.Logf("no loc:     %s", got.NoLoc.Text)
	t.Logf("strip off:  %s", got.StripOff.Text)
	t.Logf("strip on, shared location:  %s", got.StripOnShared.Text)
	t.Logf("strip off, shared location: %s", got.StripOffShared.Text)

	for name, r := range map[string]*row{"off": got.Off, "off+no loc": got.OffNoLoc, "off, PG": got.OffPG, "no loc": got.NoLoc} {
		if r == nil || !strings.Contains(r.Text, "Take the first backup") || !strings.Contains(r.Cls, "waiting") {
			t.Fatalf("%s: last row is not a waiting backup step: %+v", name, r)
		}
		for _, bad := range []string{"—", "BINTRAIL_", "--", " here", "this page"} {
			if strings.Contains(r.Text, bad) {
				t.Errorf("%s: row holds %q: %s", name, bad, r.Text)
			}
		}
	}
	if !strings.Contains(got.Off.Text, "turned off") || !strings.Contains(got.Off.Text, "mydumper") || strings.Contains(got.Off.Text, "backup location") {
		t.Errorf("off: %s", got.Off.Text)
	}
	if !strings.Contains(got.OffNoLoc.Text, "its own backup location") {
		t.Errorf("off with no location does not name the location: %s", got.OffNoLoc.Text)
	}
	if strings.Contains(got.OffPG.Text, "mydumper") {
		t.Errorf("a PostgreSQL full backup does not run mydumper: %s", got.OffPG.Text)
	}
	if !strings.Contains(got.NoLoc.Text, "no backup location of its own") || strings.Contains(got.NoLoc.Text, "turned off") {
		t.Errorf("no location: %s", got.NoLoc.Text)
	}

	const note = "turned off at startup"
	if !strings.Contains(got.StripOff.Text, "CREATE BACKUP") || !strings.Contains(got.StripOff.Text, note) ||
		!strings.Contains(got.StripOff.Text, "Backup settings page") || len(got.StripOff.Buttons) != 0 {
		t.Errorf("creation off: the strip does not say so where the button would be: %+v", got.StripOff)
	}
	if strings.Contains(got.StripOn.Text, note) || len(got.StripOn.Buttons) != 1 || got.StripOn.Buttons[0] != "Create backup" {
		t.Errorf("creation on: want the button and no note: %+v", got.StripOn)
	}
	// Each strip must have drawn something, or the absence below proves nothing.
	if !strings.Contains(got.StripOffBoot.Text, "SOURCE") || !strings.Contains(got.StripOffUnconfigured.Text, "not configured") {
		t.Fatalf("a strip was not drawn: boot %q, unconfigured %q", got.StripOffBoot.Text, got.StripOffUnconfigured.Text)
	}
	if strings.Contains(got.StripOffBoot.Text, note) || strings.Contains(got.StripOffUnconfigured.Text, note) {
		t.Errorf("the note shows where the button is missing for another reason: boot %q, unconfigured %q",
			got.StripOffBoot.Text, got.StripOffUnconfigured.Text)
	}
	const loc = "needs this server's own backup location"
	if strings.Contains(got.StripOff.Text, loc) {
		t.Errorf("a server with its own location is told it needs one: %s", got.StripOff.Text)
	}
	// The daemon's shared default lists backups, but a backup refuses to write
	// to it: no button that is refused on click, and the reason instead.
	if len(got.StripOnShared.Buttons) != 0 || !strings.Contains(got.StripOnShared.Text, loc) || strings.Contains(got.StripOnShared.Text, note) {
		t.Errorf("creation on, shared location only: want the location reason and no button: %+v", got.StripOnShared)
	}
	if !strings.Contains(got.StripOffShared.Text, note) || !strings.Contains(got.StripOffShared.Text, loc) || len(got.StripOffShared.Buttons) != 0 {
		t.Errorf("creation off, shared location only: want both reasons: %+v", got.StripOffShared)
	}
	// No source: nothing a backup could read, so no note either way. The
	// button stays where it was (the page's live test pins it).
	if !strings.Contains(got.StripOffNoSource.Text, "SOURCE") || strings.Contains(got.StripOffNoSource.Text, "CREATE BACKUP") {
		t.Errorf("a server with no source gets a note: %q", got.StripOffNoSource.Text)
	}
	// A bucket is a location of the server's own, like a directory: the
	// precheck takes either, and so must the strip.
	if len(got.StripOnS3Only.Buttons) != 1 || strings.Contains(got.StripOnS3Only.Text, loc) {
		t.Errorf("creation on, S3 only: want the button and no location reason: %+v", got.StripOnS3Only)
	}
	if !strings.Contains(got.StripOffS3Only.Text, note) || strings.Contains(got.StripOffS3Only.Text, loc) {
		t.Errorf("creation off, S3 only: want only the off reason: %+v", got.StripOffS3Only)
	}
	if len(got.StripOnNoSource.Buttons) != 1 {
		t.Errorf("the button moved for a server with no source and its own location: %+v", got.StripOnNoSource)
	}
	for _, s := range []drawnStrip{got.StripOff, got.StripOnShared, got.StripOffShared} {
		for _, bad := range []string{"—", "BINTRAIL_", "--", " here", "this page"} {
			if strings.Contains(s.Text, bad) {
				t.Errorf("strip holds %q: %s", bad, s.Text)
			}
		}
	}
}
