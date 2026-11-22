//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pg3078Table = "pg3078_orders"
	pg3078Slot  = "bintrail_pg3078_slot"
	pg3078Pub   = "bintrail_pg3078_pub"
)

// pg3078Rows reads the test table out of one snapshot file.
func pg3078Rows(t *testing.T, file string) [][]string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT CAST(id AS VARCHAR), status FROM parquet_scan('` + file + `') ORDER BY CAST(id AS BIGINT)`)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		var id, status sql.NullString
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		out = append(out, []string{id.String, status.String})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// The gap of #3078, end to end, the way it was met: a server is added while
// its source is busy, so capture cannot finish creating its replication slot
// (PostgreSQL ends the creation only once every transaction that was open
// when it began has ended); the first snapshot is asked for in that time; the
// source goes on changing; capture then attaches, and the schedule updates
// the snapshot from the captured changes.
//
// The updated copy must hold what the source holds. Before the fix the
// snapshot was read at once, a change committed before the slot had its
// position was in neither the snapshot nor the captured changes, the full
// read reported success, and every later update carried the wrong row.
func TestIntegrationPGSnapshotBeforeCaptureAttached_3078(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN, source, exec1 := pgTestSource(t, context.Background(), pg3078Table, pg3078Slot, pg3078Pub, "id int PRIMARY KEY, status text")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	exec1("INSERT INTO " + pg3078Table + " VALUES (1, 'new'), (2, 'new'), (3, 'new')")

	// The busy source: one transaction, open since before the server is added.
	busy, err := pgconn.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect (open transaction): %v", err)
	}
	t.Cleanup(func() { busy.Close(context.Background()) })
	for _, stmt := range []string{"BEGIN", "SELECT txid_current()"} {
		if _, _, err := pgText(t, busy, stmt); err != nil {
			t.Fatalf("open transaction: %s: %v", stmt, err)
		}
	}

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-3078", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pg3078Slot, SourcePublication: pg3078Pub,
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

	// Capture starts, and stays in the creation of its slot: listed by
	// PostgreSQL, with no position yet.
	if err := mon.Start(ctx, e); err != nil {
		t.Fatalf("start capture: %v", err)
	}
	t.Cleanup(func() { _ = mon.Stop(context.Background(), e.ID) })
	pgTestWait(t, "capture creating its slot", 25*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT (confirmed_flush_lsn IS NOT NULL)::text FROM pg_replication_slots WHERE slot_name = '"+pg3078Slot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot listed yet"
		}
		if rows[0][0] == "true" {
			t.Fatal("the slot has a position while a transaction that was open before its creation is still open: the test's premise does not hold on this server")
		}
		return true, ""
	})

	// The first snapshot, started as the Create button and `snapshot run` do.
	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatalf("start the full read: %v", err)
	}
	// Time for a full read that does not wait to read its three rows.
	early := time.Now().Add(3 * time.Second)
	for time.Now().Before(early) && sup.Status(e.ID).State == "running" {
		time.Sleep(50 * time.Millisecond)
	}
	if st := sup.Status(e.ID); st.State == "failed" {
		t.Fatalf("the full read failed while capture was creating its slot, want it to wait: %s", st.LastError)
	}

	// The source goes on changing while the slot has no position.
	exec1("UPDATE " + pg3078Table + " SET status = 'in between' WHERE id = 1")

	// The source's transaction ends: capture gets its slot and attaches.
	if _, _, err := pgText(t, busy, "COMMIT"); err != nil {
		t.Fatalf("end the open transaction: %v", err)
	}
	pgTestWait(t, "capture attached to its slot", 60*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT active::text, (confirmed_flush_lsn IS NOT NULL)::text FROM pg_replication_slots WHERE slot_name = '"+pg3078Slot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot"
		}
		return rows[0][0] == "true" && rows[0][1] == "true", "active=" + rows[0][0] + " has_position=" + rows[0][1]
	})
	pgTestWait(t, "the full read", 2*time.Minute, func() (bool, string) {
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

	// A change capture does see, so the update has something to apply.
	exec1("UPDATE " + pg3078Table + " SET status = 'after' WHERE id = 2")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the later change indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND JSON_UNQUOTE(JSON_EXTRACT(row_after, '$.status')) = 'after'),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pg3078Table).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 1 && covered == 1, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered)
	})

	// Snapshot folders are named to the second.
	time.Sleep(1100 * time.Millisecond)
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.fire(e, p, time.Now().UTC())
	var st console.BackupScheduleState
	pgTestWait(t, "the scheduled job", 3*time.Minute, func() (bool, string) {
		st = b.ScheduleState(e.ID)
		if st.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", st.LastSkipReason)
		}
		return st.Last != nil && !st.Running, "running"
	})
	b.watchers.Wait()
	st = b.ScheduleState(e.ID)
	sup.postRefresh.Wait()
	if st.Last == nil || st.Last.State != "succeeded" {
		t.Fatalf("the scheduled job did not succeed: %+v", st.Last)
	}
	if st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("the schedule ran %q (why: %q), want an update: a full read here would hide a gap by reading the source again", st.LastMethod, st.LastWhy)
	}

	all := pgUpdSnapshots(t, snapshots)
	if len(all) != 2 {
		t.Fatalf("after the update: want the full read and one new snapshot, got %v", all)
	}
	found, err := filepath.Glob(filepath.Join(snapshots, all[1], "*", pg3078Table+".parquet"))
	if err != nil || len(found) != 1 {
		t.Fatalf("the updated snapshot: want one %s.parquet, found %v (err %v)", pg3078Table, found, err)
	}
	_, src, err := pgText(t, source, "SELECT id::text, status FROM "+pg3078Table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"1", "in between"}, {"2", "after"}, {"3", "new"}}
	if !reflect.DeepEqual(src, want) {
		t.Fatalf("the source holds %v, want %v", src, want)
	}
	if got := pg3078Rows(t, found[0]); !reflect.DeepEqual(got, src) {
		t.Fatalf("the updated copy differs from the source:\n copy   %v\n source %v\nthe change made while capture was creating its slot is in neither the snapshot nor the captured changes", got, src)
	}
}

// A copy that already has the gap, as a build before the fix left it: the
// index records that capture started reading the source after the snapshot's
// floor. The update that follows must not build on it. It refuses the table,
// and the schedule reads the source again, which is the repair.
func TestIntegrationPGSnapshotOlderThanCaptureIsReadAgain_3078(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN, source, exec1 := pgTestSource(t, context.Background(), pg3078Table, pg3078Slot, pg3078Pub, "id int PRIMARY KEY, status text")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	exec1("INSERT INTO " + pg3078Table + " VALUES (1, 'new'), (2, 'new'), (3, 'new')")

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-3078-old", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pg3078Slot, SourcePublication: pg3078Pub,
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
	pgTestStart(t, ctx, mon, bootDSN, source, e)

	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatalf("start the full read: %v", err)
	}
	pgTestWait(t, "the full read", 2*time.Minute, func() (bool, string) {
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
	firstFiles, err := filepath.Glob(filepath.Join(snapshots, first[0], "*", pg3078Table+".parquet"))
	if err != nil || len(firstFiles) != 1 {
		t.Fatalf("the full read's file: %v (err %v)", firstFiles, err)
	}
	firstMeta, err := baseline.ReadParquetMetadata(firstFiles[0])
	if err != nil || firstMeta.LSN == 0 {
		t.Fatalf("the full read's footer: LSN %d, err %v", firstMeta.LSN, err)
	}

	exec1("UPDATE " + pg3078Table + " SET status = 'after' WHERE id = 2")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the change indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pg3078Table).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 1 && covered == 1, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered)
	})

	// The record an install with the gap holds: capture started after the
	// snapshot's floor, from where its slot stands. The slot's position moves
	// when capture tells the source what it has saved, so the test waits for
	// it to be past the snapshot's floor and takes it as the start. Written
	// with capture stopped, since a first run writes its own start at every
	// save; the run that follows resumes and keeps what is there.
	var startedAt uint64
	pgTestWait(t, "the slot's position past the snapshot's floor", 90*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::text FROM pg_replication_slots WHERE slot_name = '"+pg3078Slot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot"
		}
		if startedAt, err = strconv.ParseUint(rows[0][0], 10, 64); err != nil {
			return false, err.Error()
		}
		return startedAt > firstMeta.LSN, "confirmed_flush_lsn=" + rows[0][0] + " floor=" + strconv.FormatUint(firstMeta.LSN, 10)
	})
	if err := mon.Stop(ctx, e.ID); err != nil {
		t.Fatalf("stop capture: %v", err)
	}
	res, err := idx.ExecContext(ctx, "UPDATE stream_state SET pg_capture_from_lsn = ? WHERE id = 1 AND pg_capture_from_lsn IS NOT NULL", startedAt)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("the index records no capture start to move (%d rows)", n)
	}
	if err := mon.Start(ctx, e); err != nil {
		t.Fatalf("start capture again: %v", err)
	}

	// Snapshot folders are named to the second.
	time.Sleep(1100 * time.Millisecond)
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	// The schedule's loop has seen the server, as it has by the time a slot
	// fires in the daemon: a full read is not taken for a schedule that was
	// removed meanwhile.
	b.mu.Lock()
	b.seen[e.ID] = seenSlot{identity: e.BackupSchedule.Identity()}
	b.mu.Unlock()
	b.fire(e, p, time.Now().UTC())
	var st console.BackupScheduleState
	pgTestWait(t, "the update refused and a full read in its place", 3*time.Minute, func() (bool, string) {
		st = b.ScheduleState(e.ID)
		if st.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", st.LastSkipReason)
		}
		return st.LastFallbackAt != "" && !st.Running, "method=" + string(st.LastMethod) + " fallback=" + st.LastFallbackReason
	})
	b.watchers.Wait()
	sup.postRefresh.Wait()
	if !strings.Contains(st.LastFallbackReason, "never captured") {
		t.Fatalf("the full read stood in for another reason: %s", st.LastFallbackReason)
	}
	pgTestWait(t, "the full read that stands in", 2*time.Minute, func() (bool, string) {
		s := sup.Status(e.ID)
		if s.State == "failed" {
			t.Fatalf("the full read failed: %s", s.LastError)
		}
		return s.State == "succeeded", s.State
	})

	all := pgUpdSnapshots(t, snapshots)
	if len(all) != 2 {
		t.Fatalf("want the first snapshot and the full read that replaced the update, got %v", all)
	}
	found, err := filepath.Glob(filepath.Join(snapshots, all[1], "*", pg3078Table+".parquet"))
	if err != nil || len(found) != 1 {
		t.Fatalf("the new snapshot: %v (err %v)", found, err)
	}
	meta, err := baseline.ReadParquetMetadata(found[0])
	if err != nil {
		t.Fatal(err)
	}
	if meta.Producer != baseline.ProducerDump {
		t.Fatalf("the new snapshot was produced by %q, want a read of the source", meta.Producer)
	}
	if meta.LSN < startedAt {
		t.Fatalf("the new snapshot's floor %d is before where capture started (%d)", meta.LSN, startedAt)
	}
	_, src, err := pgText(t, source, "SELECT id::text, status FROM "+pg3078Table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if got := pg3078Rows(t, found[0]); !reflect.DeepEqual(got, src) {
		t.Fatalf("the new snapshot differs from the source:\n copy   %v\n source %v", got, src)
	}
}
