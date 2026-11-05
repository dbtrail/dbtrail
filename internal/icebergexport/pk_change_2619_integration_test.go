//go:build integration

package icebergexport

import (
	"strconv"
	"testing"
	"time"
)

// #2619 against a real index: windows with UPDATEs that change the primary
// key, folded across two runs, must give the table `reconstruct` gives at the
// same cut. Before the fix, run 1 refused and every later run refused again.
func TestIntegrationExport_pkChange2619_matchesReconstruct(t *testing.T) {
	f := seedFixture(t) // baseline: 1, 2, 3 (status "new")
	row := func(id int, status string) string {
		return `{"id":` + strconv.Itoa(id) + `,"status":"` + status + `","amount":10.00,"updated_at":"2026-08-28 12:00:00"}`
	}
	// Window 1: move 2→7, reinsert 2, move 3→8, delete 8.
	f.event(t, 100, 200, 10*time.Second, 2, "2", row(2, "new"), row(7, "moved"))
	f.event(t, 200, 300, 20*time.Second, 1, "2", "", row(2, "back"))
	f.event(t, 300, 400, 30*time.Second, 2, "3", row(3, "new"), row(8, "new"))
	f.event(t, 400, 500, 40*time.Second, 3, "8", row(8, "new"), "")
	warehouse := t.TempDir()

	o := runOne(t, f.config(warehouse, f.base.Add(20*time.Minute)))
	if o.Verdict != VerdictLoaded || o.Events != 4 || o.Upserts != 2 || o.Deletes != 2 {
		t.Fatalf("run 1 = %+v, want loaded: 4 events, 2 upserts (2, 7), 2 deletes (3, 8) (%s)", o, o.Detail)
	}

	// Window 2: move 1→9, then move 7 onto the freed key 1.
	f.event(t, 500, 600, 25*time.Minute, 2, "1", row(1, "new"), row(9, "one"))
	f.event(t, 600, 700, 26*time.Minute, 2, "7", row(7, "moved"), row(1, "seven"))
	at2 := f.base.Add(40 * time.Minute)
	o = runOne(t, f.config(warehouse, at2))
	if o.Verdict != VerdictExported || o.Events != 2 || o.Upserts != 2 || o.Deletes != 1 {
		t.Fatalf("run 2 = %+v, want exported: 2 events, 2 upserts (1, 9), 1 delete (7) (%s)", o, o.Detail)
	}

	ddb := openDuckDBIceberg(t)
	equalRows(t, "after run 2", duckRows(t, ddb, o.Location), []string{"1=seven", "2=back", "9=one"})
	assertMatchesReconstruct(t, f, ddb, o.Location, at2, 3)
}
