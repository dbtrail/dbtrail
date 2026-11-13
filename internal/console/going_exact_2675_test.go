package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// writeKeepSnapshot writes one complete local snapshot of shop.orders taken at.
func writeKeepSnapshot(t *testing.T, dir string, at time.Time) {
	t.Helper()
	snap := filepath.Join(dir, reconstruct.SnapshotDirName(at))
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snap); err != nil {
		t.Fatal(err)
	}
}

// #2675 point 9: the "goes at the next cleanup" mark is worked out by the
// cleanup's own rule. The page counted positions, so with Delete by age set
// it marked a snapshot past the count that the age keeps, and the next
// cleanup left it.
func TestBaselines_goingFollowsTheCountAndTheAge(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	at := func(h int) time.Time { return now.Add(-time.Duration(h) * time.Hour) }
	srv, id, dir := s3CopyServerWith(t, &stubScheduleReporter{full: true}, true)
	srv.localPruneLoop = true
	e, _ := srv.cm.reg.Get(id)
	e.LocalKeepNewest = 2
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	// Every one in both places; 1 and 2 are the count; 3 is past it and
	// younger than the age; 5 and 6 are past both.
	stubListings(map[string]s3CopyListing{
		dir:              {snaps: []time.Time{at(1), at(2), at(3), at(5), at(6)}},
		"s3://bkt/snaps": {snaps: []time.Time{at(1), at(2), at(3), at(5), at(6)}},
	})
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, h := range []int{1, 2, 3, 5, 6} {
		writeKeepSnapshot(t, dir, at(h))
	}
	going := func() map[string]bool {
		t.Helper()
		rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		if rec.Code != 200 {
			t.Fatalf("GET /api/baselines: %d %s", rec.Code, body)
		}
		var listed struct {
			Snapshots []struct {
				Time  string
				Going bool
			}
		}
		if err := json.Unmarshal(body, &listed); err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, sn := range listed.Snapshots {
			out[sn.Time] = sn.Going
		}
		return out
	}
	label := func(h int) string { return at(h).Format(consoleTSFormat) }
	// The count alone: everything past the newest two goes.
	g := going()
	if g[label(1)] || g[label(2)] || !g[label(3)] || !g[label(5)] || !g[label(6)] {
		t.Fatalf("by the count alone, going = %v: want 3, 5 and 6", g)
	}
	// With Delete by age at 4 hours, 3 stays: past the count, younger than
	// the age.
	if err := srv.cm.reg.SetBackupSetting(BackupSettingBaselineRetain, ptr("4h")); err != nil {
		t.Fatal(err)
	}
	g = going()
	if g[label(1)] || g[label(2)] || g[label(3)] || !g[label(5)] || !g[label(6)] {
		t.Fatalf("with the age, going = %v: want only 5 and 6 (3 is younger than the age)", g)
	}
}
