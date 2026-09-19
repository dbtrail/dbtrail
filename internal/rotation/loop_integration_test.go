//go:build integration

package rotation

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestRotateOneIndex_UpgradeGuard verifies the implicit-default upgrade
// guard: an index holding history far beyond the default window (the
// signature of a pre-existing deployment upgrading into built-in rotation)
// must NOT be dropped until the operator sets --rotate-retain explicitly.
func TestRotateOneIndex_UpgradeGuard(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// 100 days of pre-existing history — far beyond 2× the 30d default —
	// plus the current-hour partition (a live deployment always has recent
	// partitions; add-future top-up appends after the LATEST named one, so a
	// lone ancient partition would make top-up land uselessly in the past).
	old := time.Now().UTC().Add(-100 * 24 * time.Hour).Truncate(time.Hour)
	current := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old, current})

	logs := captureSlog(t)

	// Built-in rotation profile, with add-future headroom so the guarded
	// cycle's top-up promise is asserted. rotateOneIndex derives its Options
	// from this Settings via loopOptions (ProtectUnarchived on, no archiving).
	dsn := testutil.IntegrationDSN(dbName)
	s := Settings{
		Enabled: true, Retain: 30 * 24 * time.Hour, RetainRaw: "30d",
		Interval: time.Hour, AddFuture: 2, Explicit: false,
	}

	// Implicit default: the guard must refuse the drop and say so loudly.
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, s); err != nil {
		t.Fatalf("rotateOneIndex (implicit): %v", err)
	}
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	found, future := false, 0
	nowHour := time.Now().UTC().Truncate(time.Hour)
	for _, p := range partitions {
		if p.Name == indexer.PartitionName(old) {
			found = true
		}
		if d, ok := indexer.PartitionDate(p.Name); ok && d.After(nowHour) {
			future++
		}
	}
	if !found {
		t.Fatalf("partition %s was dropped under the IMPLICIT default — the upgrade guard failed", indexer.PartitionName(old))
	}
	if !logs.has(slog.LevelError, "refusing to drop it without an explicit choice") {
		t.Error("upgrade guard did not log its Error explaining the refusal")
	}
	// The guarded cycle must still top up future partitions (retain=0 skips
	// only the drop branch) — otherwise refusing drops would also starve
	// p_future headroom while the operator decides.
	if future < 2 {
		t.Errorf("guarded cycle added %d future partitions, want >= 2 (top-up must survive the guard)", future)
	}

	// Explicit choice: the same retention now drops the old history.
	s.Explicit = true
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: dsn}, s); err != nil {
		t.Fatalf("rotateOneIndex (explicit): %v", err)
	}
	partitions, err = listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	for _, p := range partitions {
		if p.Name == indexer.PartitionName(old) {
			t.Errorf("partition %s should have been dropped once retention was explicit", indexer.PartitionName(old))
		}
	}
}

// TestStartLoop_escalatesOnPersistentDeferral runs the real loop against a real
// index where the protect-unarchived guard defers the same partition every
// cycle (archiving history exists, partition unarchived) and asserts the loop
// escalates to Error after escalateAfter cycles — the "archiving flow stalled,
// index growing unbounded" detection.
func TestStartLoop_escalatesOnPersistentDeferral(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// One partition past retention (30h > 24h) but NOT archived...
	h1 := time.Now().UTC().Add(-30 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	// ...while archive_state shows archiving history for a different
	// partition → protect active, h1 deferred every cycle.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES ('p_2020010100', 'cron-uuid', '/archives/old.parquet', 1)`)

	logs := captureSlog(t)

	prevN := escalateAfter
	escalateAfter = 2
	t.Cleanup(func() { escalateAfter = prevN })

	s := Settings{
		Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h",
		Interval: 25 * time.Millisecond, AddFuture: 0,
		Explicit: true, // h1 is only 30h old; guard wouldn't trip, but be unambiguous
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := StartLoop(ctx, func() Settings { return s }, func() []RotateTarget {
		return []RotateTarget{{DSN: testutil.IntegrationDSN(dbName)}}
	})

	deadline := time.After(30 * time.Second)
	for !logs.has(slog.LevelError, "made no progress for consecutive cycles") {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("loop never escalated to Error after consecutive all-deferred cycles")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// The deferred partition must still exist — detection, not destruction.
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	found := false
	for _, p := range partitions {
		if p.Name == indexer.PartitionName(h1) {
			found = true
		}
	}
	if !found {
		t.Error("deferred partition was dropped — the guard must never destroy unarchived data")
	}
}

// rotationRig is a fresh index with one partition 72 h old (droppable under
// a 24 h retain) and the current hour.
func rotationRig(t *testing.T) (db *sql.DB, dbName string, old, current time.Time, s Settings) {
	t.Helper()
	db, dbName = testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	old = time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Hour)
	current = time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old, current})
	s = Settings{Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h", Interval: time.Hour, AddFuture: 2, Explicit: true}
	return db, dbName, old, current, s
}

func partitionSet(t *testing.T, db *sql.DB, dbName string) map[string]bool {
	t.Helper()
	ps, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, p := range ps {
		out[p.Name] = true
	}
	return out
}

// TestRotateOneIndex_writerlessEmptyIndexIsHousekeeping (#1715): a target
// marked NoWriter that holds no events is still rotated (the old partition
// dropped, future ones added, so a later stream finds a current layout), but
// its drop lines are Debug and the completion line says the index is empty.
func TestRotateOneIndex_writerlessEmptyIndexIsHousekeeping(t *testing.T) {
	db, dbName, old, current, s := rotationRig(t)
	logs := captureSlog(t)
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: testutil.IntegrationDSN(dbName), NoWriter: true}, s); err != nil {
		t.Fatalf("empty, writerless: %v", err)
	}
	after := partitionSet(t, db, dbName)
	if after[indexer.PartitionName(old)] {
		t.Fatalf("the old partition of an empty writerless index was not dropped: %v", after)
	}
	if !after[indexer.PartitionName(current.Add(2*time.Hour))] {
		t.Fatalf("future partitions were not topped up on an empty writerless index: %v", after)
	}
	if logs.has(slog.LevelInfo, "dropped partition") || !logs.has(slog.LevelDebug, "dropped partition") {
		t.Fatal("an empty writerless index must log its drops at Debug, not Info")
	}
	if !logs.hasAttr("rotation complete", "db", dbName) || !logs.hasAttr("rotation complete", "index_empty", "true") {
		t.Fatal("the completion line does not name the database and say the index is empty")
	}
}

// TestRotateOneIndex_writerlessIndexWithEventsLogsLikeAnyOther: the same
// mark on an index that holds one event (a source-ful era left it): Info
// drop lines naming the database, and no index_empty claim.
func TestRotateOneIndex_writerlessIndexWithEventsLogsLikeAnyOther(t *testing.T) {
	db, dbName, old, current, s := rotationRig(t)
	testutil.InsertEvent(t, db, "binlog.000001", 4, 104, current.Format("2006-01-02 15:04:05"), nil, "s", "t", 1, "1", nil, nil, []byte(`{"id":1}`))
	logs := captureSlog(t)
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: testutil.IntegrationDSN(dbName), NoWriter: true}, s); err != nil {
		t.Fatalf("non-empty, writerless: %v", err)
	}
	if partitionSet(t, db, dbName)[indexer.PartitionName(old)] {
		t.Fatal("the old partition survived")
	}
	if !logs.hasAttr("dropped partition", "db", dbName) || logs.has(slog.LevelDebug, "dropped partition") || logs.hasKey("rotation complete", "index_empty") {
		t.Fatal("a writerless index holding events must log like any other, with no index_empty claim")
	}
}

// TestRotateOneIndex_emptyPerSourceIndexLogsInfo pins the other half: a
// per-source index never carries the mark, logs its drops at Info even
// while empty, and makes no index_empty claim, since nothing measured it.
func TestRotateOneIndex_emptyPerSourceIndexLogsInfo(t *testing.T) {
	db, dbName, old, _, s := rotationRig(t)
	logs := captureSlog(t)
	if _, err := rotateOneIndex(context.Background(), RotateTarget{DSN: testutil.IntegrationDSN(dbName)}, s); err != nil {
		t.Fatal(err)
	}
	if partitionSet(t, db, dbName)[indexer.PartitionName(old)] {
		t.Fatal("the old partition survived")
	}
	if !logs.hasAttr("dropped partition", "db", dbName) || logs.hasKey("rotation complete", "index_empty") {
		t.Fatal("an empty per-source index must be rotated and logged at Info, with no index_empty claim")
	}
}

// TestRotateOneIndex_probeErrorFailsTheCycle: a writerless index whose
// events table cannot be probed counts as a failed target, with the probe's
// own error and its Warn — not the later ALTER's error, which a probe that
// swallowed its failure and rotated on a guess would also produce.
func TestRotateOneIndex_probeErrorFailsTheCycle(t *testing.T) {
	_, dbName := testutil.CreateTestDB(t) // no index tables: the probe fails
	s := Settings{Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h", Interval: time.Hour, AddFuture: 2, Explicit: true}
	logs := captureSlog(t)
	_, err := rotateOneIndex(context.Background(), RotateTarget{DSN: testutil.IntegrationDSN(dbName), NoWriter: true}, s)
	if err == nil || !strings.Contains(err.Error(), "probe binlog_events") {
		t.Fatalf("a failed probe did not fail the target with its own error: %v", err)
	}
	if !logs.has(slog.LevelWarn, "could not tell whether the index holds events") {
		t.Fatal("the probe failure was not warned about")
	}
	// On shutdown the same failure is neither a Warn nor a failed target,
	// as the Perform path reads a cancellation: the cycle is over anyway.
	logs2 := captureSlog(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rotateOneIndex(ctx, RotateTarget{DSN: testutil.IntegrationDSN(dbName), NoWriter: true}, s); err != nil {
		t.Fatalf("a cancelled probe counted as a failed target: %v", err)
	}
	if logs2.has(slog.LevelWarn, "could not tell whether the index holds events") {
		t.Fatal("a cancelled probe raised a Warn on shutdown")
	}
}
