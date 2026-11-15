//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgKcTable = "pgkc_orders"
	pgKcSlot  = "bintrail_pgkc_slot"
	pgKcPub   = "bintrail_pgkc_pub"
)

// pgKcSourceQuery reads the whole test table from the source as text, under
// the settings a snapshot's values are rendered with.
const pgKcSourceQuery = "SELECT tenant::text, code, uid::text, amount::text, seen::text, status, md5(body) FROM " + pgKcTable +
	" ORDER BY tenant, code, uid, amount, seen"

// pgKcSort orders rows the same way on both sides, whatever collation the
// source sorted with.
func pgKcSort(rows [][]string) [][]string {
	sort.Slice(rows, func(i, j int) bool {
		return strings.Join(rows[i], "\x00") < strings.Join(rows[j], "\x00")
	})
	return rows
}

// pgKcTableFile is the one Parquet file a snapshot holds for the test table.
func pgKcTableFile(t *testing.T, snapshotDir string) string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(snapshotDir, "*", pgKcTable+".parquet"))
	if err != nil || len(found) != 1 {
		t.Fatalf("snapshot %s: want one %s.parquet, found %v (err %v)", snapshotDir, pgKcTable, found, err)
	}
	return found[0]
}

// pgKcRows reads the test table out of one snapshot file, every value as the
// text the file holds; the large column as its md5, as the source query does.
func pgKcRows(t *testing.T, file string) [][]string {
	t.Helper()
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT CAST(tenant AS VARCHAR), CAST(code AS VARCHAR), CAST(uid AS VARCHAR), CAST(amount AS VARCHAR),
		CAST(seen AS VARCHAR), CAST(status AS VARCHAR), md5(CAST(body AS VARCHAR)) FROM parquet_scan('` + file + `')`)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, 7)
		ptrs := make([]any, len(vals))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			row[i] = "NULL"
			if v.Valid {
				row[i] = v.String
			}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return pgKcSort(out)
}

// Key-changing UPDATEs on a PostgreSQL source, end to end (#2760): a real
// PostgreSQL, real capture, a full read, then the schedule's own update,
// which must apply every key change as a delete of the old key and an insert
// of the new one instead of refusing the table and reading the source again.
// The source is the judge: the updated copy must hold exactly what
// `SELECT ... ORDER BY` returns there.
//
// The key is five columns (integer, text, uuid, numeric, timestamptz). One
// column is stored out of line (TOAST) and most statements leave it alone, so
// PostgreSQL sends "unchanged" for it and capture fills the value from the
// old row of the same change: the md5 of that column on both sides is what
// shows the value came through.
func TestIntegrationPGSnapshotUpdateKeyChange(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	_, source, exec1 := pgTestSource(t, ctx, pgKcTable, pgKcSlot, pgKcPub,
		"tenant int, code text, uid uuid, amount numeric(10,2), seen timestamptz, status text, body text, "+
			"PRIMARY KEY (tenant, code, uid, amount, seen)")
	// Out of line and uncompressed, so every body below is TOASTed.
	exec1("ALTER TABLE " + pgKcTable + " ALTER COLUMN body SET STORAGE EXTERNAL")
	// The settings a snapshot's values are rendered under, for what this
	// connection reads back.
	exec1("SET TimeZone = 'UTC'")
	exec1("SET DateStyle = 'ISO'")

	const (
		uid  = "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"
		seen = "'2026-03-01 10:00:00+00'"
	)
	row := func(code, status string) string {
		return "(1, '" + code + "', " + uid + ", 10.50, " + seen + ", " + status + ", repeat(md5('" + code + "'), 200))"
	}
	var initial []string
	for _, code := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "u", "keep"} {
		initial = append(initial, row(code, "'was "+code+"'"))
	}
	initial = append(initial, row("t", "NULL"))
	exec1("INSERT INTO " + pgKcTable + " VALUES " + strings.Join(initial, ", "))

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-key-change", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgKcSlot, SourcePublication: pgKcPub,
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

	// The first snapshot: a full read.
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
	firstFile := pgKcTableFile(t, filepath.Join(snapshots, first[0]))
	_, srcBefore, err := pgText(t, source, pgKcSourceQuery)
	if err != nil {
		t.Fatal(err)
	}
	srcBefore = pgKcSort(srcBefore)
	if got := pgKcRows(t, firstFile); !reflect.DeepEqual(got, srcBefore) {
		t.Fatalf("the full read holds\n%v\nthe source holds\n%v", got, srcBefore)
	}

	set := func(what, where string) string { return "UPDATE " + pgKcTable + " SET " + what + " WHERE " + where }
	for _, stmt := range []string{
		// The key changed twice: a, a2, a3.
		set("code = 'a2'", "code = 'a'"),
		set("code = 'a3', status = 'twice'", "code = 'a2'"),
		// Changed and changed back.
		set("code = 'b2'", "code = 'b'"),
		set("code = 'b', status = 'back'", "code = 'b2'"),
		// The new key is one deleted earlier.
		"DELETE FROM " + pgKcTable + " WHERE code = 'c'",
		set("code = 'c'", "code = 'd'"),
		// The new key is deleted later.
		set("code = 'e2'", "code = 'e'"),
		"DELETE FROM " + pgKcTable + " WHERE code = 'e2'",
		// The old key is inserted again later.
		set("code = 'f2'", "code = 'f'"),
		"INSERT INTO " + pgKcTable + " VALUES " + row("f", "'reborn'"),
		// One column of the key changes, per type.
		set("uid = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12'", "code = 'g'"),
		set("amount = 99.90", "code = 'h'"),
		set("seen = '2026-03-01 10:00:00.5+00'", "code = 'i'"),
		set(`code = 'año|ñ\雪'`, "code = 'j'"),
		set(`status = 'after the move'`, `code = 'año|ñ\雪'`),
		// Two rows swap keys through a third.
		set("code = 'tmp'", "code = 'k'"),
		set("code = 'k'", "code = 'l'"),
		set("code = 'l'", "code = 'tmp'"),
		// One transaction: a key change, a change of the moved row, the key
		// changed again, and another row moved onto a key deleted in it.
		"BEGIN; " + set("code = 'm2'", "code = 'm'") + "; " + set("status = 'touched'", "code = 'm2'") + "; " +
			set("code = 'm', tenant = 2", "code = 'm2'") + "; DELETE FROM " + pgKcTable + " WHERE code = 'n'; " +
			set("code = 'n'", "code = 'o'") + "; COMMIT",
		// A change that keeps the key and leaves the large column alone.
		set("status = 'shipped'", "code = 'p'"),
		// Several rows moved by one statement.
		set("tenant = tenant + 10", "code IN ('q', 'r')"),
		// The key and the large column change together.
		set("code = 's2', body = repeat(md5('s2'), 300)", "code = 's'"),
		// A row with a NULL column moves.
		set("code = 't2'", "code = 't'"),
		// A moved row is deleted and its first key taken by another row.
		set("code = 'u2'", "code = 'u'"),
		"DELETE FROM " + pgKcTable + " WHERE code = 'u2'",
		set("code = 'u'", "code = 'a3'"),
		// The last change, which the wait below looks for.
		"INSERT INTO " + pgKcTable + " VALUES " + row("zz-last", "'last'"),
	} {
		exec1(stmt)
	}

	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the changes indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var last, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values LIKE '%zz-last%'),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgKcTable).Scan(&last, &covered)
		if err != nil {
			return false, err.Error()
		}
		return last == 1 && covered == 1, "last_change_indexed=" + strconv.Itoa(last) + " checkpoint_covers=" + strconv.Itoa(covered)
	})
	// The index holds key changes as UPDATEs whose two images name two keys:
	// what the update is about to fold. (An UPDATE that keeps its key has
	// the same five values on both sides.)
	var keyChanges int
	if err := idx.QueryRowContext(ctx, `SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND event_type = 2
		AND NOT (row_before->>'$.code' <=> row_after->>'$.code' AND row_before->>'$.tenant' <=> row_after->>'$.tenant'
			AND row_before->>'$.uid' <=> row_after->>'$.uid' AND row_before->>'$.amount' <=> row_after->>'$.amount'
			AND row_before->>'$.seen' <=> row_after->>'$.seen')`, pgKcTable).Scan(&keyChanges); err != nil {
		t.Fatal(err)
	}
	if keyChanges < 20 {
		t.Fatalf("the index holds %d key-changing UPDATEs, want at least 20: the test is not exercising what it is for", keyChanges)
	}

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
	newFile := pgKcTableFile(t, filepath.Join(snapshots, all[1]))
	newMeta, err := baseline.ReadParquetMetadata(newFile)
	if err != nil {
		t.Fatal(err)
	}
	if newMeta.Producer != baseline.ProducerReconstruct {
		t.Fatalf("the new snapshot's file says it was produced by %q, want %q (built from the previous snapshot, not read from the source)", newMeta.Producer, baseline.ProducerReconstruct)
	}

	// The source is the judge.
	_, src, err := pgText(t, source, pgKcSourceQuery)
	if err != nil {
		t.Fatal(err)
	}
	src = pgKcSort(src)
	got := pgKcRows(t, newFile)
	if !reflect.DeepEqual(got, src) {
		t.Fatalf("the updated copy and the source differ.\nthe copy holds %d rows:\n%s\nthe source holds %d rows:\n%s",
			len(got), pgKcDump(got), len(src), pgKcDump(src))
	}
	// And the source did change, into the shape the statements describe: a
	// comparison of two unchanged tables would pass too.
	if reflect.DeepEqual(src, srcBefore) {
		t.Fatal("the source holds what it held before the statements")
	}
	codes := map[string]string{}
	for _, r := range src {
		codes[r[0]+"/"+r[1]] = r[5]
	}
	for key, status := range map[string]string{
		"1/u": "twice", "1/b": "back", "1/c": "was d", "1/f": "reborn", "1/f2": "was f", `1/año|ñ\雪`: "after the move",
		"1/k": "was l", "1/l": "was k", "2/m": "touched", "1/n": "was o", "1/p": "shipped", "11/q": "was q", "11/r": "was r",
		"1/s2": "was s", "1/t2": "NULL", "1/zz-last": "last", "1/keep": "was keep",
	} {
		if got, ok := codes[key]; !ok || got != status {
			t.Errorf("the source's row %s has status %q (present: %v), want %q", key, got, ok, status)
		}
	}
	for _, gone := range []string{"1/a", "1/a2", "1/a3", "1/b2", "1/d", "1/e", "1/e2", "1/j", "1/tmp", "1/m", "1/m2", "1/o", "1/q", "1/r", "1/s", "1/t", "1/u2"} {
		if _, ok := codes[gone]; ok {
			t.Errorf("the source still holds row %s", gone)
		}
	}
	// The full read is untouched.
	if got := pgKcRows(t, firstFile); !reflect.DeepEqual(got, srcBefore) {
		t.Fatalf("the update changed the previous snapshot")
	}
}

// pgKcDump prints rows one per line, for a failure message.
func pgKcDump(rows [][]string) string {
	var sb strings.Builder
	for _, r := range rows {
		sb.WriteString("  " + strings.Join(r, " | ") + "\n")
	}
	return sb.String()
}
