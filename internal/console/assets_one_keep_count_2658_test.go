package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestSnapshotList_goingRowsWithS3 draws the real snapshot list (#2658) for
// a server keeping its newest 2, without and with an S3 destination. Without
// one, every local snapshot past the count is marked as going at the next
// cleanup. With one, the marked ones are those the listing also found in S3
// and those the S3 interval kept here on purpose, which go by the count
// alone. A snapshot that was meant for S3 and is not there (an upload that
// failed) stays until S3 has it, and a mark on it would announce a removal
// that does not happen. The S3 case is drawn from GET /api/baselines itself,
// with the held marks on disk.
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
	now := time.Now().UTC().Truncate(time.Second)
	at := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }
	// Newest first. 1 and 2 are inside the count. 3 is past it and in both
	// places. 4 is past it, only here, held on purpose. 5 is past it, only
	// here, NOT held: its upload failed. 6 is only in S3.
	srv, id, dir := s3CopyServerWith(t, &stubScheduleReporter{full: true}, true)
	srv.localPruneLoop = true
	e, _ := srv.cm.reg.Get(id)
	e.LocalKeepNewest = 2
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	stubListings(map[string]s3CopyListing{
		dir:              {snaps: []time.Time{at(1), at(2), at(3), at(4), at(5)}},
		"s3://bkt/snaps": {snaps: []time.Time{at(2), at(3), at(6)}},
	})
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, h := range []int{1, 4} {
		if err := baseline.MarkUploadHeld(dir, reconstruct.SnapshotDirName(at(h))); err != nil {
			t.Fatal(err)
		}
	}
	rec, withS3 := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if rec.Code != 200 {
		t.Fatalf("GET /api/baselines: %d %s", rec.Code, withS3)
	}
	var listed struct {
		Snapshots []struct {
			Time  string
			Held  bool
			Kinds []string
		}
		LocalRetention *localRetentionDTO `json:"local_retention"`
		S3Destination  bool               `json:"s3_destination"`
	}
	if err := json.Unmarshal(withS3, &listed); err != nil {
		t.Fatal(err)
	}
	var heldRows []string
	for _, sn := range listed.Snapshots {
		if sn.Held {
			heldRows = append(heldRows, sn.Time)
		}
	}
	if want := []string{at(1).Format(consoleTSFormat), at(4).Format(consoleTSFormat)}; !slices.Equal(heldRows, want) {
		t.Fatalf("rows the API says are held: %v, want %v", heldRows, want)
	}
	if listed.LocalRetention == nil || listed.LocalRetention.KeepNewest != 2 || !listed.S3Destination || len(listed.Snapshots) != 6 {
		t.Fatalf("the listing: retention %+v, s3_destination %v, %d snapshots", listed.LocalRetention, listed.S3Destination, len(listed.Snapshots))
	}
	// The same folder with no destination, by hand: every row is local.
	// Three more made in the last hour come first: two are the count, and
	// the third is past the count and goes like any other (#2678: no hour).
	// The oldest is the last restore's result, which the cleanup keeps.
	var local []map[string]any
	for _, m := range []int{10, 20, 30} {
		local = append(local, map[string]any{"time": now.Add(-time.Duration(m) * time.Minute).Format(consoleTSFormat), "age_hours": float64(m) / 60,
			"tables": []string{"shop.orders"}, "kinds": []string{"dir"}})
	}
	for h := 1; h <= 5; h++ {
		local = append(local, map[string]any{"time": at(h).Format(consoleTSFormat), "age_hours": h, "tables": []string{"shop.orders"}, "kinds": []string{"dir"}})
	}
	local[len(local)-1]["restored"] = true
	raw, _ := json.Marshal(local)
	script := renderHarnessJS + `
vm.runInContext("capsCache = { backup_schedule: true };", ctx);
const cur = { id: "a", name: "a", kind: "registry", baseline_dir: "/var/lib/dbtrail/snapshots/a" };
const all = (n, out = []) => { if (!n || typeof n !== "object") return out; out.push(n); for (const c of n.children || []) all(c, out); return out; };
const draw = (b) => {
  const panel = vm.runInContext("baselinesPanel", ctx)(b, [cur], {});
  const nodes = all(panel);
  const rows = nodes.filter((n) => / stg-row-going /.test(" " + (n.className || "") + " "));
  const chips = nodes.filter((n) => n.textContent === "goes at the next cleanup");
  return {
    going: rows.length,
    chips: chips.length,
    titles: chips.map((n) => (n.attrs && n.attrs.title) || n.title || ""),
    kept: nodes.filter((n) => n.textContent === "last restore, kept").map((n) => (n.attrs && n.attrs.title) || n.title || ""),
    line: nodes.filter((n) => / bk-retention /.test(" " + (n.className || "") + " ")).map((n) => n.textContent),
  };
};
console.log(JSON.stringify({
  local: draw({ configured: true, snapshots: ` + string(raw) + `, local_retention: { keep_newest: 2 } }),
  s3: draw(` + string(withS3) + `),
}));
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
		Kept         []string
	}
	var got struct{ Local, S3 view }
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("decode %q: %v", out, err)
	}
	t.Logf("no S3:   line %q, %d rows going, hover %q", got.Local.Line, got.Local.Chips, got.Local.Titles)
	t.Logf("with S3: line %q, %d rows going, hover %q", got.S3.Line, got.S3.Chips, got.S3.Titles)

	// No S3: of the six local snapshots past the newest two, five go: the
	// one made 30 minutes ago included (no hour keeps it), the last
	// restore's result not.
	if got.Local.Chips != 5 || got.Local.Going != 5 {
		t.Errorf("no S3: %d chips on %d rows, want 5: the 30-minute one and the four after it, not the restore's result", got.Local.Chips, got.Local.Going)
	}
	for _, title := range got.Local.Titles {
		if title != "Past the newest 2 kept (Settings, Keep by count). Removed from this machine at the next cleanup, which follows each new snapshot, never a table's only copy." {
			t.Errorf("no S3: hover text %q", title)
		}
	}
	// The restore's result says why it is still there.
	t.Logf("the restore's row: chip %q, hover %q", "last restore, kept", got.Local.Kept)
	if len(got.Local.Kept) != 1 || got.Local.Kept[0] != "The result of the last restore to a moment. Kept until the next restore, on top of the newest kept." {
		t.Errorf("no S3: the restore's result is labelled %q", got.Local.Kept)
	}
	if len(got.S3.Kept) != 0 {
		t.Errorf("with S3: a row that is not a restore's result is labelled %q", got.S3.Kept)
	}
	if len(got.Local.Line) != 1 || got.Local.Line[0] != "Keeps the newest 2 snapshots on this machine." {
		t.Errorf("no S3: retention line %q", got.Local.Line)
	}
	// With S3: the one S3 also has, and the one held on purpose. The one
	// whose upload failed is not marked, and neither is the held one inside
	// the count.
	wantTitles := []string{
		"Past the newest 2 kept (Settings, Keep by count). Removed from this machine at the next cleanup, which follows each new snapshot, once it finds its complete copy in S3.",
		"Past the newest 2 kept (Settings, Keep by count). Removed from this machine at the next cleanup, which follows each new snapshot: the schedule did not send it to S3, so it does not wait for S3.",
	}
	if got.S3.Chips != 2 || got.S3.Going != 2 || !slices.Equal(got.S3.Titles, wantTitles) {
		t.Errorf("with S3: %d chips on %d rows, hover %q\nwant %q", got.S3.Chips, got.S3.Going, got.S3.Titles, wantTitles)
	}
	if len(got.S3.Line) != 1 || got.S3.Line[0] != "Keeps the newest 2 snapshots on this machine; older ones are removed once S3 has them. An update the schedule did not send to S3 is removed without waiting." {
		t.Errorf("with S3: retention line %q", got.S3.Line)
	}
	for _, s := range append(append(got.S3.Line, got.S3.Titles...), got.Local.Line...) {
		if strings.ContainsAny(s, "—–") || strings.Contains(s, "undefined") {
			t.Errorf("drawn text %q", s)
		}
	}
}
