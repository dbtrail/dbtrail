//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgNoFullTable = "pgnofull_docs"
	pgNoFullSlot  = "bintrail_pgnofull_slot"
	pgNoFullPub   = "bintrail_pgnofull_pub"
)

// pgNoFullRows reads the test table out of one snapshot file: the key, the
// status and a digest of the large body, every value as text.
func pgNoFullRows(t *testing.T, file string) [][]string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT CAST(id AS VARCHAR), COALESCE(status, 'NULL'), md5(body), CAST(length(body) AS VARCHAR) FROM parquet_scan('` + file + `') ORDER BY CAST(id AS BIGINT)`)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		var id, st, sum, n sql.NullString
		if err := rows.Scan(&id, &st, &sum, &n); err != nil {
			t.Fatal(err)
		}
		out = append(out, []string{id.String, st.String, sum.String, n.String})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// A PostgreSQL table that is NOT at REPLICA IDENTITY FULL, from the source to
// the copy and to undo (#3027), with nothing between them stubbed: the server
// is registered and captured the way the console does it, its first snapshot
// is a full read, rows change on the source, and the schedule's own slot
// builds the next snapshot from the first one and the captured changes.
//
// What it must show:
//   - the table is captured (once it was skipped whole) and capture is not
//     reported as degraded;
//   - the updated copy holds the source's rows, compared row by row, large
//     values included: one an UPDATE did not touch and PostgreSQL did not
//     send (read from the previous snapshot's file), one sent, one of a row
//     inserted and updated inside the window, and a row whose key changed;
//   - it is an update, not a full read that would hold the right rows too;
//   - undo of an UPDATE or a DELETE of the table is refused with the message
//     that says why and what to run, and undo of an INSERT works.
func TestIntegrationPGWithoutFull_3027(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	_, source, exec1 := pgTestSource(t, ctx, pgNoFullTable, pgNoFullSlot, pgNoFullPub, "id int PRIMARY KEY, status text, body text")
	// The fixture sets FULL; this table is left at PostgreSQL's default.
	exec1("ALTER TABLE " + pgNoFullTable + " REPLICA IDENTITY DEFAULT")
	// Out of line and uncompressed, so an UPDATE that leaves body alone does
	// not send it.
	exec1("ALTER TABLE " + pgNoFullTable + " ALTER COLUMN body SET STORAGE EXTERNAL")
	big := func(seed int) string {
		return "(SELECT string_agg(md5((i + " + strconv.Itoa(seed*1000) + ")::text), '') FROM generate_series(1, 100) i)"
	}
	exec1("INSERT INTO " + pgNoFullTable + " VALUES (1, 'new', " + big(1) + "), (2, 'new', " + big(2) + "), (3, 'new', " + big(3) + "), (5, 'new', " + big(5) + ")")

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-no-full", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgNoFullSlot, SourcePublication: pgNoFullPub,
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
	tableFile := func(snapshot string) string {
		found, err := filepath.Glob(filepath.Join(snapshots, snapshot, "*", pgNoFullTable+".parquet"))
		if err != nil || len(found) != 1 {
			t.Fatalf("snapshot %s: want one %s.parquet, found %v (err %v)", snapshot, pgNoFullTable, found, err)
		}
		return found[0]
	}
	sourceRows := func() [][]string {
		_, rows, err := pgText(t, source, "SELECT id::text, COALESCE(status, 'NULL'), md5(body), length(body)::text FROM "+pgNoFullTable+" ORDER BY "+pgNoFullTable+".id")
		if err != nil {
			t.Fatalf("read the source: %v", err)
		}
		return rows
	}
	if got, want := pgNoFullRows(t, tableFile(first[0])), sourceRows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the full read holds %v, the source %v", got, want)
	}

	// Changes after the full read, each in its own transaction.
	for _, stmt := range []string{
		"INSERT INTO " + pgNoFullTable + " VALUES (4, 'new', " + big(4) + ")",
		// body is not sent: it is in the snapshot's file.
		"UPDATE " + pgNoFullTable + " SET status = 'shipped' WHERE id = 2",
		// body is sent.
		"UPDATE " + pgNoFullTable + " SET body = " + big(11) + " WHERE id = 1",
		// body is not sent: it is in the INSERT above.
		"UPDATE " + pgNoFullTable + " SET status = NULL WHERE id = 4",
		// The key alone.
		"DELETE FROM " + pgNoFullTable + " WHERE id = 3",
		// A change of key.
		"UPDATE " + pgNoFullTable + " SET id = 50, body = " + big(50) + " WHERE id = 5",
		"INSERT INTO " + pgNoFullTable + " VALUES (6, 'only inserted', 'small')",
	} {
		exec1(stmt)
	}

	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the seven changes indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgNoFullTable).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 7 && covered == 1, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered)
	})

	// Captured, and not as a degraded capture: nothing was dropped.
	st, err := status.LoadStreamState(ctx, idx)
	if err != nil || st == nil {
		t.Fatalf("load the capture's state: %+v, %v", st, err)
	}
	if skips, ok := st.ParseCaptureSkips(); !ok || len(skips) != 0 {
		t.Fatalf("capture recorded dropped changes for a table below FULL: %+v (readable=%v)", skips, ok)
	}
	// The UPDATE of row 2 as the index holds it.
	var before, after string
	if err := idx.QueryRowContext(ctx, `SELECT CAST(row_before AS CHAR), LEFT(CAST(row_after AS CHAR), 400) FROM binlog_events
		WHERE table_name = ? AND event_type = 2 AND pk_values = '2'`, pgNoFullTable).Scan(&before, &after); err != nil {
		t.Fatalf("read the UPDATE of row 2: %v", err)
	}
	t.Logf("the UPDATE of row 2 in the index:\n  row_before = %s\n  row_after  = %s", before, after)
	for _, want := range []string{`"id": "2"`, `"body": {"__bintrail_not_captured__": true}`, `"status": {"__bintrail_not_captured__": true}`} {
		if !strings.Contains(before, want) {
			t.Errorf("row_before lacks %s", want)
		}
	}
	if !strings.Contains(after, `"body": {"__bintrail_unchanged_toast__": true}`) || !strings.Contains(after, `"status": "shipped"`) {
		t.Errorf("row_after must hold the new status and the marker of the body PostgreSQL did not send")
	}

	// Snapshot folders are named to the second.
	time.Sleep(1100 * time.Millisecond)

	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.fire(e, p, time.Now().UTC())
	var sched console.BackupScheduleState
	pgTestWait(t, "the scheduled job", 3*time.Minute, func() (bool, string) {
		sched = b.ScheduleState(e.ID)
		if sched.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", sched.LastSkipReason)
		}
		return sched.Last != nil && !sched.Running, "running"
	})
	b.watchers.Wait()
	sched = b.ScheduleState(e.ID)
	sup.postRefresh.Wait()

	if sched.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("the schedule ran %q (why: %q), want an update", sched.LastMethod, sched.LastWhy)
	}
	if sched.Last == nil || sched.Last.State != "succeeded" {
		t.Fatalf("the update did not succeed: %+v", sched.Last)
	}
	if sched.LastFallbackAt != "" {
		t.Fatalf("the update fell back to a full read: %s", sched.LastFallbackReason)
	}
	all := pgUpdSnapshots(t, snapshots)
	if len(all) != 2 || all[0] != first[0] {
		t.Fatalf("after the update: want the full read %s and one new snapshot, got %v", first[0], all)
	}
	newFile := tableFile(all[1])
	meta, err := baseline.ReadParquetMetadata(newFile)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Producer != baseline.ProducerReconstruct {
		t.Fatalf("the new snapshot's file says it was produced by %q, want %q (built from the previous snapshot and the captured changes, not read from the source)", meta.Producer, baseline.ProducerReconstruct)
	}

	// Row by row against the real table.
	want := sourceRows()
	got := pgNoFullRows(t, newFile)
	t.Logf("the updated copy (id, status, md5(body), length(body)):\n  %v\nthe source:\n  %v", got, want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the updated copy differs from the source:\ncopy   %v\nsource %v", got, want)
	}
	if len(want) != 5 {
		t.Fatalf("the source holds %d rows, want 5 (1, 2, 4, 6 and 50): %v", len(want), want)
	}

	// Undo. An INSERT needs only the row it added and its key.
	gen := recovery.NewForDialect(idx, nil, recovery.PostgresDialect)
	var ins bytes.Buffer
	n, err := gen.GenerateSQL(ctx, query.Options{Schema: "public", Table: pgNoFullTable, PKValues: "6", Limit: 100}, &ins)
	if err != nil || n != 1 || !strings.Contains(ins.String(), "DELETE FROM") {
		t.Fatalf("undo of the INSERT of row 6: n=%d err=%v\n%s", n, err, ins.String())
	}
	t.Logf("undo of an INSERT of the table:\n%s", ins.String())

	// An UPDATE, a DELETE, and the whole table's window: refused, with nothing
	// written.
	for _, tc := range []struct{ what, pk string }{{"the UPDATE of row 2", "2"}, {"the DELETE of row 3", "3"}, {"every change of the table", ""}} {
		var buf bytes.Buffer
		n, err := gen.GenerateSQL(ctx, query.Options{Schema: "public", Table: pgNoFullTable, PKValues: tc.pk, Limit: 100}, &buf)
		var nc *event.NotCapturedError
		if !errors.As(err, &nc) {
			t.Fatalf("undo of %s: want the not-captured refusal, got n=%d err=%v\n%s", tc.what, n, err, buf.String())
		}
		t.Logf("undo of %s is refused:\n  %v", tc.what, err)
		for _, w := range []string{"cannot undo", "REPLICA IDENTITY FULL", `ALTER TABLE "public"."` + pgNoFullTable + `" REPLICA IDENTITY FULL`} {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("the refusal of %s lacks %q: %v", tc.what, w, err)
			}
		}
		if buf.Len() != 0 {
			t.Errorf("a refusal must write nothing, got %d bytes for %s", buf.Len(), tc.what)
		}
	}
}
