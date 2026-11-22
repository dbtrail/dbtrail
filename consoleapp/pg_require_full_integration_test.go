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

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/status"
)

const (
	pgRFFull = "pgrf_full"
	pgRFDef  = "pgrf_def"
	pgRFSlot = "bintrail_pgrf_slot"
	pgRFPub  = "bintrail_pgrf_pub"
)

// A PostgreSQL server with one table at REPLICA IDENTITY FULL and one at
// DEFAULT, from the source to the copy, with nothing between them stubbed:
// DBTrail captures a table only at FULL.
//
// What it must show:
//   - the server's checks fail on the table at DEFAULT, with the statement
//     that fixes it (what `server add` and Start stop on);
//   - capture, started all the same (as the daemon resumes a running server
//     at boot), takes the table at FULL and none of the other's changes,
//     which the capture's ledger names under replica_identity_not_full;
//   - the snapshot the schedule's next slot leaves never holds the table at
//     DEFAULT as it was before its changes: whatever the slot did, the file
//     a statement on the copy reads for that table holds the source's rows
//     or is not newer than the full read. A file built from the index, which
//     has none of the table's changes, would hold its old rows and look new.
func TestIntegrationPGRequireFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgRFFull, pgRFSlot, pgRFPub, "id int PRIMARY KEY, status text")
	exec1("DROP TABLE IF EXISTS " + pgRFDef)
	t.Cleanup(func() { _, _ = source.Exec(context.Background(), "DROP TABLE IF EXISTS "+pgRFDef).ReadAll() })
	exec1("CREATE TABLE " + pgRFDef + " (id int PRIMARY KEY, status text)") // DEFAULT
	exec1("ALTER PUBLICATION " + pgRFPub + " ADD TABLE " + pgRFDef)
	for _, tb := range []string{pgRFFull, pgRFDef} {
		exec1("INSERT INTO " + tb + " VALUES (1, 'new'), (2, 'new'), (3, 'new')")
	}

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-require-full", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgRFSlot, SourcePublication: pgRFPub,
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

	// The server's checks, as `server add` and Start run them.
	report, err := mon.Doctor(ctx, e)
	if err != nil {
		t.Fatalf("run the server's checks: %v", err)
	}
	var full *console.DoctorCheck
	for i := range report.Checks {
		if report.Checks[i].Name == "REPLICA IDENTITY FULL" {
			full = &report.Checks[i]
		}
	}
	if full == nil {
		t.Fatalf("no REPLICA IDENTITY FULL check in %+v", report.Checks)
	}
	t.Logf("the server's check, as the add-server screen gets it:\n  %s: %s (%s)\n%s", full.Name, full.Status, full.Detail, full.Remediation)
	if full.Status != "fail" || report.Failed == 0 ||
		full.Detail != `1 table of the publication is not at REPLICA IDENTITY FULL, so its changes are not captured: "public"."`+pgRFDef+`"` ||
		!strings.Contains(full.Remediation, "\n  ALTER TABLE \"public\".\""+pgRFDef+"\" REPLICA IDENTITY FULL;\n") {
		t.Errorf("the check must fail on the table at DEFAULT with its statement: %+v (failed=%d)", full, report.Failed)
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
	tableFile := func(snapshot, table string) string {
		found, err := filepath.Glob(filepath.Join(snapshots, snapshot, "*", table+".parquet"))
		if err != nil || len(found) != 1 {
			t.Fatalf("snapshot %s: want one %s.parquet, found %v (err %v)", snapshot, table, found, err)
		}
		return found[0]
	}
	sourceRows := func(table string) [][]string {
		_, rows, err := pgText(t, source, "SELECT id::text, COALESCE(status, 'NULL'), '', '' FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatalf("read the source: %v", err)
		}
		return rows
	}
	copyRows := func(file string) [][]string {
		return pgRequireFullRows(t, file)
	}
	before := copyRows(tableFile(first[0], pgRFDef))
	if want := sourceRows(pgRFDef); !reflect.DeepEqual(before, want) {
		t.Fatalf("the full read holds %v of %s, the source %v", before, pgRFDef, want)
	}

	for _, tb := range []string{pgRFFull, pgRFDef} {
		exec1("UPDATE " + tb + " SET status = 'shipped' WHERE id = 2")
		exec1("DELETE FROM " + tb + " WHERE id = 3")
		exec1("INSERT INTO " + tb + " VALUES (4, 'new')")
	}

	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	var st *status.StreamStateInfo
	pgTestWait(t, "the table at FULL's three changes indexed, the other's three recorded as skipped", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgRFFull).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		st, err = status.LoadStreamState(ctx, idx)
		if err != nil || st == nil {
			return false, "no capture state"
		}
		skips, _ := st.ParseCaptureSkips()
		n := skips[pgcapture.SkipReplicaIdentityNotFull].Count
		return events >= 3 && covered == 1 && n >= 3, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered) + " skipped=" + strconv.FormatInt(n, 10)
	})
	var defEvents int
	if err := idx.QueryRowContext(ctx, `SELECT COUNT(*) FROM binlog_events WHERE table_name = ?`, pgRFDef).Scan(&defEvents); err != nil || defEvents != 0 {
		t.Fatalf("the table at DEFAULT must have nothing in the index: %d (%v)", defEvents, err)
	}
	skips, _ := st.ParseCaptureSkips()
	if got := skips[pgcapture.SkipReplicaIdentityNotFull]; !reflect.DeepEqual(got.Tables, []string{"public." + pgRFDef}) {
		t.Errorf("the ledger must name the table at DEFAULT alone: %+v", got)
	}
	if len(skips) != 1 {
		t.Errorf("the ledger holds other reasons: %+v", skips)
	}

	// Snapshot folders are named to the second.
	time.Sleep(1100 * time.Millisecond)
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.fire(e, p, time.Now().UTC())
	var sched console.BackupScheduleState
	pgTestWait(t, "the scheduled slot", 3*time.Minute, func() (bool, string) {
		sched = b.ScheduleState(e.ID)
		return sched.LastSkipReason != "" || (sched.Last != nil && !sched.Running), "running"
	})
	b.watchers.Wait()
	sched = b.ScheduleState(e.ID)
	sup.postRefresh.Wait()
	t.Logf("the scheduled slot: method=%q why=%q skipped=%q fallback=%q last=%+v",
		sched.LastMethod, sched.LastWhy, sched.LastSkipReason, sched.LastFallbackReason, sched.Last)

	all := pgUpdSnapshots(t, snapshots)
	newest := all[len(all)-1]
	for _, tb := range []string{pgRFFull, pgRFDef} {
		got, want := copyRows(tableFile(newest, tb)), sourceRows(tb)
		t.Logf("%s in the newest snapshot %s: %v; the source: %v", tb, newest, got, want)
		if newest == first[0] {
			continue // the slot made no snapshot: the copy is the full read, dated as such
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: a snapshot newer than the full read holds rows the source no longer has:\ncopy   %v\nsource %v", tb, got, want)
		}
	}
}

// pgRequireFullRows reads id and status out of one snapshot file, as text,
// in the shape sourceRows gives.
func pgRequireFullRows(t *testing.T, file string) [][]string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT CAST(id AS VARCHAR), COALESCE(status, 'NULL') FROM parquet_scan('` + file + `') ORDER BY CAST(id AS BIGINT)`)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		var id, st sql.NullString
		if err := rows.Scan(&id, &st); err != nil {
			t.Fatal(err)
		}
		out = append(out, []string{id.String, st.String, "", ""})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
