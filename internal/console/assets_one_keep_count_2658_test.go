package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSnapshotList_goingRowsWithS3 draws the real snapshot list (#2658) for
// a server keeping its newest 2, without and with an S3 destination. Without
// one, every local snapshot past the count is marked as going at the next
// cleanup. With one, only those the listing also found in S3 are: a snapshot
// that is only on this machine stays until S3 has it, and a mark on it would
// announce a removal that does not happen.
func TestSnapshotList_goingRowsWithS3(t *testing.T) {
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
	at := func(h int) string {
		return time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC).Add(-time.Duration(h) * time.Hour).Format(consoleTSFormat)
	}
	// Newest first, as the API lists them. The third is past the count and
	// in both places; the fourth is past the count and only on this machine
	// (held by the S3 interval, or an upload that failed); the fifth is only
	// in S3, which the cleanup never touches.
	snaps := []map[string]any{
		{"time": at(1), "age_hours": 1, "tables": []string{"shop.orders"}, "kinds": []string{"dir"}},
		{"time": at(2), "age_hours": 2, "tables": []string{"shop.orders"}, "kinds": []string{"dir", "s3"}},
		{"time": at(3), "age_hours": 3, "tables": []string{"shop.orders"}, "kinds": []string{"dir", "s3"}},
		{"time": at(4), "age_hours": 4, "tables": []string{"shop.orders"}, "kinds": []string{"dir"}},
		{"time": at(5), "age_hours": 5, "tables": []string{"shop.orders"}, "kinds": []string{"s3"}},
	}
	raw, _ := json.Marshal(snaps)
	script := renderHarnessJS + `
vm.runInContext("capsCache = { backup_schedule: true };", ctx);
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/dbtrail/snapshots/a" };
const snaps = ` + string(raw) + `;
const all = (n, out = []) => { if (!n || typeof n !== "object") return out; out.push(n); for (const c of n.children || []) all(c, out); return out; };
const draw = (extra) => {
  const panel = vm.runInContext("baselinesPanel", ctx)(Object.assign({ configured: true, snapshots: snaps, local_retention: { keep_newest: 2 } }, extra), [cur], {});
  const nodes = all(panel);
  const chips = nodes.filter((n) => n.textContent === "goes at the next cleanup");
  return {
    going: nodes.filter((n) => / stg-row-going /.test(" " + (n.className || "") + " ")).length,
    chips: chips.length,
    titles: chips.map((n) => (n.attrs && n.attrs.title) || n.title || ""),
    line: nodes.filter((n) => / bk-retention /.test(" " + (n.className || "") + " ")).map((n) => n.textContent),
  };
};
console.log(JSON.stringify({ local: draw({}), s3: draw({ s3_destination: true }) }));
`
	path := filepath.Join(t.TempDir(), "going.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
	type view struct {
		Going, Chips int
		Titles, Line []string
	}
	var got struct{ Local, S3 view }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	t.Logf("no S3:   line %q, %d rows going, hover %q", got.Local.Line, got.Local.Chips, got.Local.Titles)
	t.Logf("with S3: line %q, %d rows going, hover %q", got.S3.Line, got.S3.Chips, got.S3.Titles)

	// No S3: the two local snapshots past the newest two go.
	if got.Local.Chips != 2 || got.Local.Going != 2 {
		t.Errorf("no S3: %d chips on %d rows, want the 2 local snapshots past the count", got.Local.Chips, got.Local.Going)
	}
	for _, title := range got.Local.Titles {
		if title != "Past the newest 2 kept (Settings, Keep by count). Removed from this machine at the next hourly cleanup, never a table's only copy." {
			t.Errorf("no S3: hover text %q", title)
		}
	}
	if len(got.Local.Line) != 1 || got.Local.Line[0] != "Keeps the newest 2 snapshots on this machine." {
		t.Errorf("no S3: retention line %q", got.Local.Line)
	}
	// With S3: only the one that is also in S3. The one only on this
	// machine is not marked.
	if got.S3.Chips != 1 || got.S3.Going != 1 {
		t.Errorf("with S3: %d chips on %d rows, want only the snapshot S3 also has", got.S3.Chips, got.S3.Going)
	}
	for _, title := range got.S3.Titles {
		if title != "Past the newest 2 kept (Settings, Keep by count). Removed from this machine at the next hourly cleanup, now that S3 has it." {
			t.Errorf("with S3: hover text %q", title)
		}
	}
	if len(got.S3.Line) != 1 || got.S3.Line[0] != "Keeps the newest 2 snapshots on this machine; older ones are removed once S3 has them." {
		t.Errorf("with S3: retention line %q", got.S3.Line)
	}
	for _, s := range append(append(got.S3.Line, got.S3.Titles...), got.Local.Line...) {
		if strings.ContainsAny(s, "—–") || strings.Contains(s, "undefined") {
			t.Errorf("drawn text %q", s)
		}
	}
}
