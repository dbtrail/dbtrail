package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// statusCopyHarnessJS runs the real statusFlowModel with each case's
// /api/status payload and /api/baselines answer, and returns the "Your copy"
// station.
const statusCopyHarnessJS = `
const f = (name) => vm.runInContext(name, ctx);
const cases = JSON.parse(process.argv[3]);
const out = cases.map((c) => {
  const m = f("statusFlowModel")(c.data, null, {}, c.copy === "omit" ? undefined : c.copy);
  const p = m.pieces.find((x) => x.title === "Your copy");
  return p ? { tone: p.tone, line: p.line, sub: p.sub } : null;
});
process.stdout.write(JSON.stringify(out));
`

// TestStatusCopyStationReadsTheSnapshotList pins the Status page's "Your
// copy" station to the /api/baselines answer (#1950). The station used to
// read baselines / baseline_staleness from /api/status, which the console
// never fills, so it said "no snapshot yet" on every server. The model tests
// that existed fed hand-written objects carrying those fields, so they passed
// against a payload the server never sends; the e2e reads the real endpoint.
func TestStatusCopyStationReadsTheSnapshotList(t *testing.T) {
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
	snap := func(tm string) map[string]any { return map[string]any{"time": tm} }
	arch := map[string]any{"archives": map[string]any{"total_files": 3, "total_size_human": "1.2 MB"}}
	type c = map[string]any
	cases := []c{
		// 0: the old shape: snapshot fields on /api/status, no listing read.
		// The station must not believe them, and a missing listing is a
		// failed read, never "no snapshot yet".
		{"data": c{"baselines": []any{c{}}, "baseline_staleness": "ok"}, "copy": "omit"},
		// 1: the listing read failed
		{"data": arch, "copy": c{"unavailable": true}},
		// 2: a listing with no snapshots
		{"data": arch, "copy": c{"configured": true, "snapshots": []any{}}},
		// 3: two snapshots, up to date
		{"data": arch, "copy": c{"snapshots": []any{snap("2026-09-30 17:05:00"), snap("2026-09-29 03:00:00")}, "staleness": "ok"}},
		// 4: up to date, but a location could not be read: a subset
		{"data": arch, "copy": c{"snapshots": []any{snap("2026-09-30 17:05:00")}, "staleness": "ok", "incomplete": true}},
		// 5: one snapshot, aging
		{"data": c{}, "copy": c{"snapshots": []any{snap("2026-09-30T17:05:00Z")}, "staleness": "aging"}},
		// 6: broken
		{"data": arch, "copy": c{"snapshots": []any{snap("2026-09-30 17:05:00")}, "staleness": "broken"}},
		// 7: a capped listing
		{"data": arch, "copy": c{"snapshots": []any{snap("2026-09-30 17:05:00")}, "truncated": true}},
	}
	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "h.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+statusCopyHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(payload)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got []struct{ Tone, Line, Sub string }
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	want := []struct{ Tone, Line, Sub string }{
		{"warn", "snapshots could not be read", "no archives yet"},
		{"warn", "snapshots could not be read", "3 archive files · 1.2 MB"},
		{"off", "no snapshot yet", "3 archive files · 1.2 MB"},
		{"ok", "2 snapshots, up to date", "newest 17:05 · 3 archive files · 1.2 MB"},
		{"warn", "1 snapshot, up to date", "newest 17:05 · 3 archive files · 1.2 MB"},
		{"warn", "1 snapshot, aging", "newest 17:05 · no archives yet"},
		{"bad", "1 snapshot, behind", "newest 17:05 · 3 archive files · 1.2 MB"},
		{"none", "1+ snapshots", "newest 17:05 · 3 archive files · 1.2 MB"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d stations, want %d: %s", len(got), len(want), raw)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("case %d: station = %+v, want %+v", i, got[i], want[i])
		}
	}
}
