//go:build integration

package icebergexport

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

// #2719 against a real index: the export compacts its own table after each
// delta, and the table still matches `reconstruct` at the same cut, including
// UPDATEs that change the primary key and land on rows a compaction already
// rewrote.
func TestIntegrationExport_compact2719_matchesReconstruct(t *testing.T) {
	f := seedFixture(t) // baseline: 1, 2, 3 (status "new")
	row := func(id int, status string) string {
		return `{"id":` + strconv.Itoa(id) + `,"status":"` + status + `","amount":10.00,"updated_at":"2026-08-28 12:00:00"}`
	}
	f.event(t, 100, 200, 10*time.Second, 2, "2", row(2, "new"), row(7, "moved"))
	f.event(t, 200, 300, 20*time.Second, 1, "2", "", row(2, "back"))
	f.event(t, 300, 400, 30*time.Second, 2, "3", row(3, "new"), row(8, "new"))
	f.event(t, 400, 500, 40*time.Second, 3, "8", row(8, "new"), "")
	warehouse := t.TempDir()
	cfg := func(at time.Time) Config {
		c := f.config(warehouse, at)
		c.CompactAfter = 1
		return c
	}
	var commits []Commit
	withCommits := func(c Config) Config {
		c.OnCommit = func(x Commit) { commits = append(commits, x) }
		return c
	}

	// Run 1: the load, the first window, then a compaction of the one delete
	// file that window wrote.
	at1 := f.base.Add(20 * time.Minute)
	o := runOne(t, withCommits(cfg(at1)))
	if o.Verdict != VerdictLoaded || o.Compaction == nil || o.Compaction.Err != nil {
		t.Fatalf("run 1 = %+v (%s), want loaded and compacted", o, o.Detail)
	}
	if o.Compaction.DeleteFilesBefore != 1 || o.Compaction.Rows != 3 {
		t.Fatalf("run 1 compaction = %+v, want 1 delete file and 3 rows", o.Compaction)
	}
	if len(commits) != 3 || commits[0].Kind != CommitLoad || commits[1].Kind != CommitDelta || commits[2].Kind != CommitCompact {
		t.Fatalf("commits = %+v, want load, delta, compact", commits)
	}
	if commits[2].Cursor != commits[1].Cursor || o.Cursor != commits[1].Cursor {
		t.Fatalf("the compaction moved the cursor: %+v, outcome %s", commits, o.Cursor)
	}
	assertReplaceWithoutDeletes(t, warehouse, f.schema, o.SnapshotID)
	ddb := openDuckDBIceberg(t)
	equalRows(t, "after run 1", duckRows(t, ddb, o.Location), []string{"1=new", "2=back", "7=moved"})
	assertMatchesReconstruct(t, f, ddb, o.Location, at1, 3)

	// Run 2: move 1→9, then 7 onto the freed key 1. Both keys sit in the
	// compacted file only.
	f.event(t, 500, 600, 25*time.Minute, 2, "1", row(1, "new"), row(9, "one"))
	f.event(t, 600, 700, 26*time.Minute, 2, "7", row(7, "moved"), row(1, "seven"))
	at2 := f.base.Add(40 * time.Minute)
	o = runOne(t, cfg(at2))
	if o.Verdict != VerdictExported || o.Compaction == nil || o.Compaction.Err != nil {
		t.Fatalf("run 2 = %+v (%s), want exported and compacted", o, o.Detail)
	}
	assertReplaceWithoutDeletes(t, warehouse, f.schema, o.SnapshotID)
	ddb2 := openDuckDBIceberg(t)
	equalRows(t, "after run 2", duckRows(t, ddb2, o.Location), []string{"1=seven", "2=back", "9=one"})
	assertMatchesReconstruct(t, f, ddb2, o.Location, at2, 3)

	// Run 3: nothing new and no delete file: no compaction, no commit.
	o3 := runOne(t, cfg(at2.Add(10*time.Minute)))
	if o3.Verdict != VerdictUnchanged || o3.Compaction != nil {
		t.Fatalf("run 3 = %+v (%s), want unchanged without a compaction", o3, o3.Detail)
	}
	if *o3.SnapshotID != *o.SnapshotID {
		t.Fatalf("run 3 moved the table to snapshot %d from %d", *o3.SnapshotID, *o.SnapshotID)
	}
}

// assertReplaceWithoutDeletes checks that the table stands at the given
// snapshot, a replace with no delete file left.
func assertReplaceWithoutDeletes(t *testing.T, warehouse, schema string, snapshotID *int64) {
	t.Helper()
	cat, release, err := openWarehouse(context.Background(), warehouse)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	tbl, err := cat.LoadTable(context.Background(), catalog.ToIdentifier(schema, "orders"))
	if err != nil {
		t.Fatal(err)
	}
	snap := tbl.CurrentSnapshot()
	if snapshotID == nil || snap == nil || snap.SnapshotID != *snapshotID {
		t.Fatalf("table at %v, outcome says %v", snap, snapshotID)
	}
	if snap.Summary.Operation != table.OpReplace {
		t.Fatalf("operation = %s, want replace", snap.Summary.Operation)
	}
	if _, del := fileCounts(t, tbl); del != 0 {
		t.Fatalf("%d delete files left after the compaction", del)
	}
}
