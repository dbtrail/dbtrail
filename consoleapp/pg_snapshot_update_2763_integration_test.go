//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgUpdTable = "pgupd_orders"
	pgUpdSlot  = "bintrail_pgupd_slot"
	pgUpdPub   = "bintrail_pgupd_pub"
)

// pgUpdSnapshots lists the complete snapshot folders under dir, oldest first.
func pgUpdSnapshots(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the snapshot folder: %v", err)
	}
	var names []string
	for _, en := range entries {
		if _, ok := snapshotdir.ParseTime(en.Name()); !ok || !en.IsDir() {
			continue
		}
		if baseline.SnapshotComplete(filepath.Join(dir, en.Name())) {
			names = append(names, en.Name())
		}
	}
	sort.Strings(names)
	return names
}

// pgUpdTableFile is the one Parquet file a snapshot holds for the test table.
func pgUpdTableFile(t *testing.T, snapshotDir string) string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(snapshotDir, "*", pgUpdTable+".parquet"))
	if err != nil || len(found) != 1 {
		all, _ := filepath.Glob(filepath.Join(snapshotDir, "*", "*"))
		t.Fatalf("snapshot %s: want one %s.parquet, found %v (err %v); the folder holds %v", snapshotDir, pgUpdTable, found, err, all)
	}
	return found[0]
}

// pgUpdRows reads the test table out of one snapshot file, every value as text.
func pgUpdRows(t *testing.T, file string) [][]string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT CAST(id AS VARCHAR), status, CAST(amount AS VARCHAR) FROM parquet_scan('` + file + `') ORDER BY CAST(id AS BIGINT)`)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		var id, status, amount sql.NullString
		if err := rows.Scan(&id, &status, &amount); err != nil {
			t.Fatal(err)
		}
		out = append(out, []string{id.String, status.String, amount.String})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// A PostgreSQL server's scheduled snapshot update, end to end (#2763): the
// server is registered and captured the way the console does it, its first
// snapshot is a full read started the way the button starts one, rows change
// on the source, and the schedule's own slot then decides what to run and
// runs it. Nothing between the slot and the Parquet file is stubbed.
//
// What it must show is that the second snapshot was built from the first one
// and the captured changes, not read from the source again: with full reads
// allowed a failed update falls back to a full read, and that copy would hold
// the right rows too.
func TestIntegrationPGSnapshotUpdate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	source, err := pgconn.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	exec1 := func(stmt string) {
		t.Helper()
		if _, _, err := pgText(t, source, stmt); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	dropAll := func() {
		bg := context.Background()
		_, _ = source.Exec(bg, "DROP PUBLICATION IF EXISTS "+pgUpdPub).ReadAll()
		_, _ = source.Exec(bg, "DROP TABLE IF EXISTS "+pgUpdTable).ReadAll()
		// The slot stays active for a moment after capture is told to stop,
		// and an active slot cannot be dropped.
		for range 50 {
			_, _ = source.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '"+pgUpdSlot+"' AND NOT active").ReadAll()
			left, err := source.Exec(bg, "SELECT 1 FROM pg_replication_slots WHERE slot_name = '"+pgUpdSlot+"'").ReadAll()
			if err != nil || len(left) == 0 || len(left[0].Rows) == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	dropAll()
	t.Cleanup(dropAll)
	exec1("CREATE TABLE " + pgUpdTable + " (id int PRIMARY KEY, status text, amount numeric(10,2))")
	exec1("ALTER TABLE " + pgUpdTable + " REPLICA IDENTITY FULL")
	exec1("INSERT INTO " + pgUpdTable + " VALUES (1, 'new', 10.50), (2, 'new', 20.00), (3, 'new', 30.00)")
	exec1("CREATE PUBLICATION " + pgUpdPub + " FOR TABLE " + pgUpdTable)

	// Capture, as the daemon supervises it. The one change is the checkpoint
	// period: 10 s in the daemon, and the update below reads only as far as
	// the last checkpoint.
	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)
	mon := newMonitorSupervisor(ctx, bootDSN, nil, nil)
	mon.pgStreamFn = func(c context.Context, cfg pgstreamrun.Config) error {
		cfg.Checkpoint = 200 * time.Millisecond
		return pgstreamrun.One(c, cfg)
	}

	// The schedule as the daemon builds it: full reads allowed, the window
	// measured (the fixture turns that off for its dated fake snapshots), and
	// per-table deltas on, which is what `watch` runs with.
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-update", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgUpdSlot, SourcePublication: pgUpdPub,
		Schemas: "public", BaselineDir: snapshots,
		BackupSchedule: &console.BackupSchedule{Every: "1h", At: "00:00"},
	})
	if err != nil {
		t.Fatalf("add the server: %v", err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := reg.Update(e); err != nil {
		t.Fatalf("save the index connection: %v", err)
	}
	t.Cleanup(func() {
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + e.ID)
			_ = db.Close()
		}
	})
	if err := mon.Start(ctx, e); err != nil {
		t.Fatalf("start capture: %v", err)
	}
	t.Cleanup(func() { _ = mon.Stop(context.Background(), e.ID) })

	waitFor := func(what string, limit time.Duration, done func() (bool, string)) {
		t.Helper()
		deadline := time.Now().Add(limit)
		for {
			ok, seen := done()
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: not within %s; last seen: %s", what, limit, seen)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	waitFor("capture attached to its slot", 60*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT active::text FROM pg_replication_slots WHERE slot_name = '"+pgUpdSlot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot yet"
		}
		return rows[0][0] == "true", "active=" + rows[0][0]
	})

	// The first snapshot: a full read, started as the Create button does.
	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatalf("start the full read: %v", err)
	}
	waitFor("the full read", 2*time.Minute, func() (bool, string) {
		st := sup.Status(e.ID)
		if st.State == "failed" {
			t.Fatalf("the full read failed: %s", st.LastError)
		}
		return st.State == "succeeded", st.State
	})
	first := pgUpdSnapshots(t, snapshots)
	if len(first) != 1 {
		t.Fatalf("after the full read: want one snapshot, got %v", first)
	}
	firstFile := pgUpdTableFile(t, filepath.Join(snapshots, first[0]))
	firstMeta, err := baseline.ReadParquetMetadata(firstFile)
	if err != nil {
		t.Fatal(err)
	}
	if firstMeta.Producer != baseline.ProducerDump {
		t.Fatalf("the full read's file says it was produced by %q, want %q", firstMeta.Producer, baseline.ProducerDump)
	}
	before := [][]string{{"1", "new", "10.50"}, {"2", "new", "20.00"}, {"3", "new", "30.00"}}
	if got := pgUpdRows(t, firstFile); !reflect.DeepEqual(got, before) {
		t.Fatalf("the full read holds %v, want %v", got, before)
	}

	// Changes on the source after the full read: one of each kind.
	exec1("INSERT INTO " + pgUpdTable + " VALUES (4, 'new', 40.00)")
	exec1("UPDATE " + pgUpdTable + " SET status = 'shipped', amount = 21.25 WHERE id = 2")
	exec1("DELETE FROM " + pgUpdTable + " WHERE id = 3")

	// Indexed AND checkpointed past the last of them: the update stops one
	// short of the newest change it cannot prove complete, so counting rows
	// alone would let it leave the DELETE out and fail for the wrong reason.
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	waitFor("the three changes indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgUpdTable).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 3 && covered == 1, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered)
	})

	// Snapshot folders are named to the second.
	time.Sleep(1100 * time.Millisecond)

	// The schedule's slot. It decides between an update and a full read, and
	// runs what it decided.
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.fire(e, p, time.Now().UTC())
	var st console.BackupScheduleState
	waitFor("the scheduled job", 3*time.Minute, func() (bool, string) {
		st = b.ScheduleState(e.ID)
		if st.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", st.LastSkipReason)
		}
		return st.Last != nil && !st.Running, "running"
	})
	b.watchers.Wait()
	st = b.ScheduleState(e.ID)
	sup.postRefresh.Wait()

	if st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("the schedule ran %q (why: %q), want an update", st.LastMethod, st.LastWhy)
	}
	if st.Last == nil || st.Last.State != "succeeded" {
		t.Fatalf("the update did not succeed: %+v", st.Last)
	}
	if st.LastFallbackAt != "" {
		t.Fatalf("the update fell back to a full read: %s", st.LastFallbackReason)
	}

	all := pgUpdSnapshots(t, snapshots)
	if len(all) != 2 || all[0] != first[0] {
		t.Fatalf("after the update: want the full read %s and one new snapshot, got %v", first[0], all)
	}
	newFile := pgUpdTableFile(t, filepath.Join(snapshots, all[1]))
	newMeta, err := baseline.ReadParquetMetadata(newFile)
	if err != nil {
		t.Fatal(err)
	}
	if newMeta.Producer != baseline.ProducerReconstruct {
		t.Fatalf("the new snapshot's file says it was produced by %q, want %q (built from the previous snapshot, not read from the source)", newMeta.Producer, baseline.ProducerReconstruct)
	}
	firstAt, _ := snapshotdir.ParseTime(first[0])
	if !newMeta.DerivedFrom.Equal(firstAt) {
		t.Fatalf("the new snapshot says it was built from %s, want the full read at %s", newMeta.DerivedFrom, firstAt)
	}

	after := [][]string{{"1", "new", "10.50"}, {"2", "shipped", "21.25"}, {"4", "new", "40.00"}}
	if got := pgUpdRows(t, newFile); !reflect.DeepEqual(got, after) {
		t.Fatalf("the updated copy holds %v, want %v", got, after)
	}
	// And the source agrees with what the test expects of it.
	_, src, err := pgText(t, source, "SELECT id::text, status, amount::text FROM "+pgUpdTable+" ORDER BY id")
	if err != nil || !reflect.DeepEqual(src, after) {
		t.Fatalf("the source holds %v (err %v), want %v", src, err, after)
	}
	// The full read is untouched: it is what a restore to that moment reads.
	if got := pgUpdRows(t, firstFile); !reflect.DeepEqual(got, before) {
		t.Fatalf("the update changed the previous snapshot: %v, want %v", got, before)
	}

	updates := 0
	for _, r := range sup.history.List(e.ID) {
		if r.Kind == console.BaselineRunRefresh && r.Error == "" && r.SkipReason == "" {
			updates++
		}
	}
	if updates != 1 {
		t.Fatalf("the history holds %d successful updates, want 1: %+v", updates, sup.history.List(e.ID))
	}
}
