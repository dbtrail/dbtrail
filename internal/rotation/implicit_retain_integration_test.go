//go:build integration

package rotation

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// implicitRig builds an index with the given partition hours and, when
// recorded is non-empty, the retention record a new index carries (#1709).
// InitIndexTables creates the pre-record table set, which is exactly what an
// older build left behind — so "no record" needs no undoing.
func implicitRig(t *testing.T, recorded string, hours ...time.Time) (db *sql.DB, dsn, dbName string) {
	t.Helper()
	db, dbName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.SetupPartitionedTable(t, db, dbName, hours)
	if recorded != "" {
		testutil.MustExec(t, db, indexer.DDLRotationPolicy)
		testutil.MustExec(t, db, "INSERT INTO rotation_policy (id, initial_retain) VALUES (1, ?)", recorded)
	}
	return db, testutil.IntegrationDSN(dbName), dbName
}

// partitionNames lists the named partitions left after a cycle.
func partitionNames(t *testing.T, db *sql.DB, dbName string) map[string]bool {
	t.Helper()
	parts, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	out := map[string]bool{}
	for _, p := range parts {
		out[p.Name] = true
	}
	return out
}

// implicitSettings is the daemon running on its built-in default: 48h here, to
// make the difference from the legacy 30d visible in every case below.
func implicitSettings() Settings {
	return Settings{
		Enabled: true, Retain: 48 * time.Hour, RetainRaw: "48h",
		Interval: time.Hour, AddFuture: 2, Explicit: false,
	}
}

// An index that records 48h rotates on 48h even while the daemon's own default
// says something else: the record is the policy, not the flag.
func TestRotateOneIndex_recordedRetentionIsUsed(t *testing.T) {
	old := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRig(t, "48h", old, current)

	s := implicitSettings()
	s.Retain, s.RetainRaw = 30*24*time.Hour, "30d" // daemon default says 30d
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, s); err != nil {
		t.Fatalf("rotateOneIndex: %v", err)
	}
	if partitionNames(t, db, dbName)[indexer.PartitionName(old)] {
		t.Errorf("partition %s survived: the index records 48h and this one is 72h old",
			indexer.PartitionName(old))
	}
}

// An index with no record keeps 30d — the window it has been running on —
// even though this daemon's default is now 48h. This is the upgrade case: the
// 72h partition must survive and the 40d one must go.
func TestRotateOneIndex_indexWithNoRecordKeepsTheLegacyWindow(t *testing.T) {
	ancient := time.Now().UTC().Add(-40 * 24 * time.Hour).Truncate(time.Hour)
	recent := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRig(t, "", ancient, recent, current)
	keptRetainNoticed.Delete(dsn)
	t.Cleanup(func() { keptRetainNoticed.Delete(dsn) })
	logs := captureSlog(t)

	for range 2 { // two cycles: the notice is once per index, not per cycle
		if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, implicitSettings()); err != nil {
			t.Fatalf("rotateOneIndex: %v", err)
		}
	}
	parts := partitionNames(t, db, dbName)
	if !parts[indexer.PartitionName(recent)] {
		t.Errorf("partition %s (72h old) was dropped: an index created before the record must keep %s",
			indexer.PartitionName(recent), LegacyRetain)
	}
	if parts[indexer.PartitionName(ancient)] {
		t.Errorf("partition %s (40d old) survived: %s is the window, and the upgrade guard must not trip at 40d",
			indexer.PartitionName(ancient), LegacyRetain)
	}
	if logs.has(slog.LevelError, "refusing to drop it without an explicit choice") {
		t.Error("the upgrade guard tripped at 40 days: it is measured against 2x30d, not 2x the daemon default")
	}
	if n := logs.count(slog.LevelWarn, "keeps the retention it was created under"); n != 1 {
		t.Errorf("the operator was told %d times, want exactly 1", n)
	}
}

// The upgrade guard still protects an unrecorded index whose history is far
// beyond the legacy window, and the daemon's shorter default does not move it.
func TestRotateOneIndex_upgradeGuardStillMeasuredAgainstTheLegacyWindow(t *testing.T) {
	ancient := time.Now().UTC().Add(-100 * 24 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRig(t, "", ancient, current)
	logs := captureSlog(t)

	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, implicitSettings()); err != nil {
		t.Fatalf("rotateOneIndex: %v", err)
	}
	if !partitionNames(t, db, dbName)[indexer.PartitionName(ancient)] {
		t.Errorf("partition %s was dropped: 100 days of history on an unrecorded index must wait for an explicit choice",
			indexer.PartitionName(ancient))
	}
	if !logs.has(slog.LevelError, "refusing to drop it without an explicit choice") {
		t.Error("the guard refused silently")
	}
	if !logs.hasAttr("built-in rotation: existing history extends far beyond the default retention — refusing to drop it without an explicit choice",
		"default_retain", LegacyRetain) {
		t.Errorf("the refusal names the daemon's default instead of %s, the window it actually measured", LegacyRetain)
	}
}

// A recorded index is exempt from the guard: history deeper than its window is
// the daemon having been stopped, not an upgrade changing the rules. Guarding
// it would freeze drops after any outage longer than the window.
func TestRotateOneIndex_recordedIndexIsNotGuardedAfterAnOutage(t *testing.T) {
	stale := time.Now().UTC().Add(-6 * 24 * time.Hour).Truncate(time.Hour) // 6 days > 2x48h
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRig(t, "48h", stale, current)
	logs := captureSlog(t)

	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, implicitSettings()); err != nil {
		t.Fatalf("rotateOneIndex: %v", err)
	}
	if partitionNames(t, db, dbName)[indexer.PartitionName(stale)] {
		t.Errorf("partition %s survived: a recorded index rotates on its own window after an outage",
			indexer.PartitionName(stale))
	}
	if logs.has(slog.LevelError, "refusing to drop it without an explicit choice") {
		t.Error("the upgrade guard tripped on an index that records its window")
	}
}

// An explicit choice wins over the record — and says nothing about it.
func TestRotateOneIndex_explicitChoiceIgnoresTheRecord(t *testing.T) {
	old := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRig(t, "30d", old, current)
	keptRetainNoticed.Delete(dsn)
	t.Cleanup(func() { keptRetainNoticed.Delete(dsn) })
	logs := captureSlog(t)

	s := implicitSettings()
	s.Explicit = true // --rotate-retain 48h
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, s); err != nil {
		t.Fatalf("rotateOneIndex: %v", err)
	}
	if partitionNames(t, db, dbName)[indexer.PartitionName(old)] {
		t.Errorf("partition %s survived: an explicit 48h must drop it even though the index records 30d",
			indexer.PartitionName(old))
	}
	if logs.count(slog.LevelWarn, "keeps the retention it was created under") != 0 {
		t.Error("an operator who chose a retention was told about the record anyway")
	}
}

// A record this build cannot read is not a licence to drop on the shorter
// default: it falls back to the legacy window, says why, and stays guarded.
func TestRotateOneIndex_unreadableRecordKeepsTheLegacyWindow(t *testing.T) {
	ancient := time.Now().UTC().Add(-40 * 24 * time.Hour).Truncate(time.Hour)
	recent := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	db, dsn, dbName := implicitRig(t, "two weeks", ancient, recent, current)
	logs := captureSlog(t)

	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, implicitSettings()); err != nil {
		t.Fatalf("rotateOneIndex: %v", err)
	}
	parts := partitionNames(t, db, dbName)
	if !parts[indexer.PartitionName(recent)] {
		t.Errorf("partition %s (72h old) was dropped on an unreadable record: the fallback is %s",
			indexer.PartitionName(recent), LegacyRetain)
	}
	if parts[indexer.PartitionName(ancient)] {
		t.Errorf("partition %s (40d old) survived: the fallback window is %s, not off",
			indexer.PartitionName(ancient), LegacyRetain)
	}
	if !logs.has(slog.LevelWarn, "could not read which retention this index was created under") {
		t.Error("the fallback was silent")
	}
}
