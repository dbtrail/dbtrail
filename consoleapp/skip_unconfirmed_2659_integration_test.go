//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2659 against a real index: whether a quiet snapshot can wait is read
// from the index's own partitions, schema_changes and stream_state, with the
// production reads on every seam (the unit tests answer for them). A
// snapshot well inside what the index keeps can wait; one close to the
// oldest hour it keeps, one with a table created since it, and one with a
// capture loss on record since it cannot, and each says which.
func TestIntegrationQuietSnapshotCanWait_2659(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	now := time.Now().UTC()
	// The index keeps 48 hours: its oldest partition is the hour 48 hours ago.
	oldest := now.Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.MustExec(t, db, fmt.Sprintf(
		"ALTER TABLE binlog_events REORGANIZE PARTITION p_future INTO ("+
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s')), "+
			"PARTITION %s VALUES LESS THAN (TO_SECONDS('%s')), "+
			"PARTITION p_future VALUES LESS THAN MAXVALUE)",
		indexer.PartitionName(oldest), oldest.Add(time.Hour).Format("2006-01-02 15:04:05"),
		indexer.PartitionName(now), now.Truncate(time.Hour).Add(time.Hour).Format("2006-01-02 15:04:05")))

	// The production reads, not the stand-ins the unit tests install.
	prevTables, prevLoss, prevCov := tablesCreatedSince, lossOnRecordSince, snapshotStillCovered
	t.Cleanup(func() { tablesCreatedSince, lossOnRecordSince, snapshotStillCovered = prevTables, prevLoss, prevCov })
	tablesCreatedSince, lossOnRecordSince, snapshotStillCovered = realTablesCreatedSince, realLossOnRecordSince, snapshotStillCoveredInDB

	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	e := console.ServerEntry{ID: "srv", Name: "srv", DSN: testutil.IntegrationDSN(name)}
	canWait := func(anchor time.Time) (bool, string) {
		t.Helper()
		ok, why := sup.quietSnapshotCanWait(context.Background(), e, anchor, now, time.Hour)
		t.Logf("a snapshot %s old: can wait = %v %s", now.Sub(anchor).Round(time.Minute), ok, why)
		return ok, why
	}
	recent := now.Add(-10 * time.Hour)

	// Ten hours old, 48 kept: well inside.
	if ok, why := canWait(recent); !ok || why != "" {
		t.Fatalf("a recent snapshot on a quiet index: ok=%v why=%q, want it to wait", ok, why)
	}
	// Forty hours old: inside the hours the index keeps, and past the line
	// (four fifths of them) at which the quiet-cycle gate re-anchors.
	if ok, why := canWait(now.Add(-40 * time.Hour)); ok || why != "the previous snapshot is close to the oldest change the index still keeps, and could not be updated once rotation drops those hours" {
		t.Fatalf("a snapshot near the end of what the index keeps: ok=%v why=%q", ok, why)
	}
	// Already older than everything the index keeps.
	if ok, _ := canWait(now.Add(-60 * time.Hour)); ok {
		t.Fatal("a snapshot older than the index's oldest hour can wait")
	}

	// A table created an hour ago, after the recent snapshot.
	testutil.MustExec(t, db, "INSERT INTO schema_changes (detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query) VALUES (?, 'binlog.000001', 4, 'shop', 'new', 'CREATE TABLE', 'CREATE TABLE new (id INT PRIMARY KEY)')", now.Add(-time.Hour))
	if ok, why := canWait(recent); ok || why != "a table was created since the previous snapshot" {
		t.Fatalf("a table created since: ok=%v why=%q", ok, why)
	}
	// One created before the snapshot does not count.
	if ok, why := canWait(now.Add(-30 * time.Minute)); !ok {
		t.Fatalf("a table created before the snapshot stopped it: %q", why)
	}
	testutil.MustExec(t, db, "DELETE FROM schema_changes")

	// A capture loss recorded an hour ago, after the recent snapshot.
	testutil.MustExec(t, db, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id, capture_skips, gap_lost_at, gap_lost_detail)
		VALUES (1, 'position', 'binlog.000001', 4, NULL, 'mysql', UTC_TIMESTAMP(), 1, '{}', ?, 'purged')`, now.Add(-time.Hour))
	if ok, why := canWait(recent); ok || why != "capture lost changes since the previous snapshot" {
		t.Fatalf("a capture loss since: ok=%v why=%q", ok, why)
	}
	// One from before the snapshot does not count.
	if ok, why := canWait(now.Add(-30 * time.Minute)); !ok {
		t.Fatalf("a capture loss before the snapshot stopped it: %q", why)
	}
	testutil.MustExec(t, db, "DELETE FROM stream_state")

	// How far the probe got, for a probe that then runs out of time: the
	// index's record of the capture is read before the source is tried.
	var read atomic.Bool
	if r := captureFromDBs(context.Background(), e.DSN, testutil.DefaultDSN+"/", config.SSL{}, recent, time.Time{}, &read); !read.Load() {
		t.Fatalf("a probe that read stream_state (%+v) did not say the index's part was done", r)
	}
	read.Store(false)
	if r := captureFromDBs(context.Background(), testutil.IntegrationDSN(name+"_gone"), testutil.DefaultDSN+"/", config.SSL{}, recent, time.Time{}, &read); read.Load() || (r.detail != captureDetailIndexSilent && r.detail != captureDetailCheckpointUnread) {
		t.Fatalf("a probe whose index is not there: %+v, index part done = %v", r, read.Load())
	}

	// An index that cannot be reached: not a verdict, and never "can wait".
	gone := e
	gone.DSN = testutil.IntegrationDSN(name + "_gone")
	ok, why := sup.quietSnapshotCanWait(context.Background(), gone, recent, now, time.Hour)
	t.Logf("an index that is not there: can wait = %v %s", ok, why)
	if ok || why != "how far back the index still keeps changes could not be read" {
		t.Fatalf("an unreachable index: ok=%v why=%q", ok, why)
	}
}
