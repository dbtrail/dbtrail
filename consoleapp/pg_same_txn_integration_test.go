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
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// pgSameTxnCase is one table on a real PostgreSQL, captured, read in full,
// changed, and then updated by the schedule's own slot.
type pgSameTxnCase struct {
	table, slot, pub string
	columns          string   // between the parentheses of CREATE TABLE
	setup            []string // after CREATE TABLE, before capture
	changes          []string // after the full read; each is one statement or one BEGIN ... COMMIT
	lastPK           string   // pk_values of the last change, which the wait looks for
	sourceExprs      string   // SELECT list on the source, every value as text
	fileExprs        string   // the same list over the snapshot file
	width            int
}

// pgSameTxnOutcome is what the update left.
type pgSameTxnOutcome struct {
	src       [][]string   // the source's rows after the changes
	copies    [][][]string // the rows of every snapshot made after the changes
	names     []string     // their folders
	updated   bool         // the update published a copy and no full read took its place
	refused   bool         // the update failed
	said      string       // what the schedule and the log said
	fallback  bool
	snapshots int
}

// pgSameTxnRun runs c and returns the outcome. It fails only on what is not
// the subject: the fixture, the full read, the wait.
func pgSameTxnRun(t *testing.T, c pgSameTxnCase) pgSameTxnOutcome {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	logs := captureSlogFor(t)

	_, source, exec1 := pgTestSource(t, ctx, c.table, c.slot, c.pub, c.columns)
	exec1("SET TimeZone = 'UTC'")
	exec1("SET DateStyle = 'ISO'")
	for _, stmt := range c.setup {
		exec1(stmt)
	}
	sourceQuery := "SELECT " + c.sourceExprs + " FROM " + c.table

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-" + c.table, Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: c.slot, SourcePublication: c.pub,
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
	if first := pgUpdSnapshots(t, snapshots); len(first) != 1 {
		t.Fatalf("after the full read: want one snapshot, got %v", first)
	}

	for _, stmt := range c.changes {
		exec1(stmt)
	}
	var out pgSameTxnOutcome
	if _, out.src, err = pgText(t, source, sourceQuery); err != nil {
		t.Fatal(err)
	}
	out.src = pgKcSort(out.src)

	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the changes indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var last, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			c.table, c.lastPK).Scan(&last, &covered)
		if err != nil {
			return false, err.Error()
		}
		return last >= 1 && covered == 1, "last_change_indexed=" + strconv.Itoa(last) + " checkpoint_covers=" + strconv.Itoa(covered)
	})

	time.Sleep(1100 * time.Millisecond)
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	b.fire(e, p, time.Now().UTC())
	pgTestWait(t, "the scheduled job", 3*time.Minute, func() (bool, string) {
		st := b.ScheduleState(e.ID)
		if st.LastSkipReason != "" {
			t.Fatalf("the schedule skipped its slot: %s", st.LastSkipReason)
		}
		return st.Last != nil && !st.Running, "running"
	})
	// A refused update may start a full read in its place: let whatever the
	// schedule started come to rest.
	b.watchers.Wait()
	sup.postRefresh.Wait()
	pgTestWait(t, "the schedule at rest", 3*time.Minute, func() (bool, string) {
		st := b.ScheduleState(e.ID)
		full, upd := sup.Status(e.ID).State, sup.RefreshStatus(e.ID).State
		return !st.Running && full != "running" && upd != "running", "full_read=" + full + " update=" + upd
	})
	b.watchers.Wait()
	sup.postRefresh.Wait()

	st := b.ScheduleState(e.ID)
	upd := sup.RefreshStatus(e.ID)
	out.said = upd.LastError + "\n" + st.LastFallbackReason + "\n" + st.LastSkipReason
	for _, rt := range upd.RefusedTables {
		out.said += "\n" + rt.Name + ": " + rt.Reason
	}
	for _, rt := range st.LastFallbackRefusedTables {
		out.said += "\n" + rt.Name + ": " + rt.Reason
	}
	// The page's copy of a reason is cut to a few hundred characters; the
	// log holds the whole text.
	out.said += "\n" + logs.String()

	all := pgUpdSnapshots(t, snapshots)
	out.snapshots = len(all)
	out.fallback = st.LastFallbackAt != ""
	out.refused = upd.State == "failed"
	out.updated = upd.State == "succeeded" && len(all) == 2 && !out.fallback && st.LastMethod == console.BackupMethodRefresh
	for _, name := range all[1:] {
		out.names = append(out.names, name)
		out.copies = append(out.copies, pgSameTxnRows(t, filepath.Join(snapshots, name), c))
	}
	return out
}

// pgSameTxnRows reads c's table out of one snapshot.
func pgSameTxnRows(t *testing.T, snapshotDir string, c pgSameTxnCase) [][]string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(snapshotDir, "*", c.table+".parquet"))
	if err != nil || len(found) != 1 {
		t.Fatalf("snapshot %s: want one %s.parquet, found %v (err %v)", snapshotDir, c.table, found, err)
	}
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	rows, err := duck.Query(`SELECT ` + c.fileExprs + ` FROM parquet_scan('` + found[0] + `')`)
	if err != nil {
		t.Fatalf("read %s: %v", found[0], err)
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals := make([]sql.NullString, c.width)
		ptrs := make([]any, c.width)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, c.width)
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

// pgSameTxnJudge is the rule every case answers to: every copy made after
// the changes holds exactly the source's rows. The source is the judge.
func pgSameTxnJudge(t *testing.T, out pgSameTxnOutcome) {
	t.Helper()
	for i, got := range out.copies {
		if !reflect.DeepEqual(got, out.src) {
			t.Fatalf("the copy %s and the source differ (the update published it: %v).\nthe copy holds %d rows:\n%s\nthe source holds %d rows:\n%s",
				out.names[i], out.updated, len(got), pgKcDump(got), len(out.src), pgKcDump(out.src))
		}
	}
}

// A primary key that is DEFERRABLE INITIALLY DEFERRED, on a real PostgreSQL:
// two rows can hold one key value until the transaction commits, with no key
// change at all.
//
// Transaction one inserts a new row 1 and then deletes the OLD row 1. The
// captured changes are INSERT(1), DELETE(1); the last one is the DELETE, and
// a copy that keeps the last change has no row 1 where the source has the
// new one. Transaction two is the mirror: a new row 2 is inserted, the OLD
// row 2 is updated, and the NEW row 2 is deleted, so the last change is a
// DELETE while the source keeps the updated old row.
//
// Required: the newest copy holds the source's rows, or the update refused
// the table for this reason. Never other rows.
func TestIntegrationPGSnapshotUpdateDeferredKeySameTransaction(t *testing.T) {
	const tbl = "pgst_deferred"
	out := pgSameTxnRun(t, pgSameTxnCase{
		table: tbl, slot: "bintrail_pgstd_slot", pub: "bintrail_pgstd_pub",
		columns: "id int, v text, PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED",
		setup:   []string{"INSERT INTO " + tbl + " VALUES (1, 'old one'), (2, 'old two'), (3, 'keep')"},
		changes: []string{
			"BEGIN; INSERT INTO " + tbl + " VALUES (1, 'new one'); DELETE FROM " + tbl + " WHERE id = 1 AND v = 'old one'; COMMIT",
			"BEGIN; INSERT INTO " + tbl + " VALUES (2, 'new two'); UPDATE " + tbl + " SET v = 'old two, changed' WHERE id = 2 AND v = 'old two'; " +
				"DELETE FROM " + tbl + " WHERE id = 2 AND v = 'new two'; COMMIT",
			"INSERT INTO " + tbl + " VALUES (100, 'last')",
		},
		lastPK:      "100",
		sourceExprs: "id::text, v", fileExprs: "CAST(id AS VARCHAR), CAST(v AS VARCHAR)", width: 2,
	})
	if want := pgKcSort([][]string{{"1", "new one"}, {"2", "old two, changed"}, {"3", "keep"}, {"100", "last"}}); !reflect.DeepEqual(out.src, want) {
		t.Fatalf("the source holds %v, want %v", out.src, want)
	}
	pgSameTxnJudge(t, out)
	if out.updated {
		t.Logf("the update followed the transactions of a deferred key and holds the source's rows")
		return
	}
	if !out.refused {
		t.Fatalf("the update neither published the source's rows nor failed (%d snapshots): %s", out.snapshots, out.said)
	}
	for _, want := range []string{"inside one transaction", "do not describe one row", "DEFERRABLE"} {
		if !strings.Contains(out.said, want) {
			t.Errorf("the update was refused, but not for the deferred-key reason (missing %q): %s", want, out.said)
		}
	}
}

// The ordinary transactions, on a table whose key is NOT deferrable: a row
// inserted and deleted again, inserted and updated, updated twice, deleted
// and replaced, and all of them in a row, each inside one transaction. They
// read, change by change, like the deferred-key cases above, except that
// each change starts from the row the one before it left. They must not be
// refused: the update publishes the copy itself, with the source's rows.
//
// The columns are the kinds whose text has to compare exactly between the
// row one change leaves and the row the next one finds: numeric, a time with
// zone, text outside ASCII, NULL, and a value stored out of line that most
// changes leave alone (PostgreSQL then sends "unchanged" for it and capture
// fills it from the old row).
func TestIntegrationPGSnapshotUpdateSameTransactionOrdinary(t *testing.T) {
	const tbl = "pgst_ordinary"
	row := func(id int, v string) string {
		return "(" + strconv.Itoa(id) + ", " + v + ", 10.50, '2026-03-01 10:00:00.25+00', repeat(md5('" + strconv.Itoa(id) + "'), 200))"
	}
	set := func(what string, id int) string {
		return "UPDATE " + tbl + " SET " + what + " WHERE id = " + strconv.Itoa(id)
	}
	del := func(id int) string { return "DELETE FROM " + tbl + " WHERE id = " + strconv.Itoa(id) }
	ins := func(id int, v string) string { return "INSERT INTO " + tbl + " VALUES " + row(id, v) }
	txn := func(stmts ...string) string { return "BEGIN; " + strings.Join(stmts, "; ") + "; COMMIT" }
	out := pgSameTxnRun(t, pgSameTxnCase{
		table: tbl, slot: "bintrail_pgsto_slot", pub: "bintrail_pgsto_pub",
		columns: "id int PRIMARY KEY, v text, n numeric(10,2), seen timestamptz, body text",
		setup: []string{
			"ALTER TABLE " + tbl + " ALTER COLUMN body SET STORAGE EXTERNAL",
			"INSERT INTO " + tbl + " VALUES " + row(1, "'one'") + ", " + row(2, "'two'") + ", " + row(3, "NULL") + ", " + row(4, "'four'"),
		},
		changes: []string{
			// Inserted and deleted again.
			txn(ins(10, "'año ☃'"), del(10)),
			// Inserted and updated; the large value is left alone.
			txn(ins(11, "'new'"), set("v = 'changed', n = 99.90", 11)),
			// Updated twice, the second time back to NULL.
			txn(set("v = 'once', seen = '2026-03-02 00:00:00+00'", 1), set("v = NULL", 1)),
			// Deleted and replaced under the same key.
			txn(del(2), ins(2, "'replaced'")),
			// A NULL set, then the row deleted.
			txn(set("v = 'set'", 3), del(3)),
			// All of it in a row on one key, ending with the large value changed.
			txn(ins(12, "'a'"), set("v = 'b'", 12), del(12), ins(12, "'c'"), set("body = repeat(md5('other'), 300)", 12)),
			// A change that writes the same values again, twice.
			txn(set("v = v", 4), set("v = v", 4)),
			ins(100, "'last'"),
		},
		lastPK:      "100",
		sourceExprs: "id::text, v, n::text, seen::text, md5(body)",
		fileExprs:   "CAST(id AS VARCHAR), CAST(v AS VARCHAR), CAST(n AS VARCHAR), CAST(seen AS VARCHAR), md5(CAST(body AS VARCHAR))",
		width:       5,
	})
	ids := map[string]string{}
	for _, r := range out.src {
		ids[r[0]] = r[1]
	}
	if want := map[string]string{"1": "NULL", "2": "replaced", "4": "four", "11": "changed", "12": "c", "100": "last"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("the source holds %v, want %v", ids, want)
	}
	pgSameTxnJudge(t, out)
	if !out.updated {
		t.Fatalf("ordinary transactions were not followed by the update (refused: %v, a full read took its place: %v, %d snapshots): %s",
			out.refused, out.fallback, out.snapshots, out.said)
	}
}

// A deferred key and a change that comes back, on a real PostgreSQL. Row 1
// is not there before. One transaction inserts (1, 'a'), updates it to 'b',
// inserts (1, 'a') again (allowed until commit) and deletes the row that
// holds 'a'. The source keeps (1, 'b'). The captured changes are INSERT a,
// UPDATE a to b, INSERT a, DELETE a: the third equals the first, as when
// capture delivers a transaction twice, but a second delivery would repeat
// INSERT, UPDATE, and this goes INSERT, DELETE. Taking it for a second
// delivery ends on the DELETE and the copy has no row 1.
//
// Key 2 is the variant: the second 'a' row is updated to 'c' and then
// deleted.
//
// Required: every copy made after the changes holds the source's rows, or
// the update refused the table for this reason. Never other rows.
func TestIntegrationPGSnapshotUpdateDeferredKeyFirstChangeAgain(t *testing.T) {
	const tbl = "pgst_again"
	out := pgSameTxnRun(t, pgSameTxnCase{
		table: tbl, slot: "bintrail_pgsta_slot", pub: "bintrail_pgsta_pub",
		columns: "id int, v text, PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED",
		setup:   []string{"INSERT INTO " + tbl + " VALUES (3, 'keep')"},
		changes: []string{
			"BEGIN; INSERT INTO " + tbl + " VALUES (1, 'a'); UPDATE " + tbl + " SET v = 'b' WHERE id = 1; " +
				"INSERT INTO " + tbl + " VALUES (1, 'a'); DELETE FROM " + tbl + " WHERE id = 1 AND v = 'a'; COMMIT",
			"BEGIN; INSERT INTO " + tbl + " VALUES (2, 'a'); UPDATE " + tbl + " SET v = 'b' WHERE id = 2; " +
				"INSERT INTO " + tbl + " VALUES (2, 'a'); UPDATE " + tbl + " SET v = 'c' WHERE id = 2 AND v = 'a'; " +
				"DELETE FROM " + tbl + " WHERE id = 2 AND v = 'c'; COMMIT",
			"INSERT INTO " + tbl + " VALUES (100, 'last')",
		},
		lastPK:      "100",
		sourceExprs: "id::text, v", fileExprs: "CAST(id AS VARCHAR), CAST(v AS VARCHAR)", width: 2,
	})
	if want := pgKcSort([][]string{{"1", "b"}, {"2", "b"}, {"3", "keep"}, {"100", "last"}}); !reflect.DeepEqual(out.src, want) {
		t.Fatalf("the source holds %v, want %v", out.src, want)
	}
	pgSameTxnJudge(t, out)
	if out.updated {
		t.Logf("the update followed the transactions of a deferred key and holds the source's rows")
		return
	}
	if !out.refused {
		t.Fatalf("the update neither published the source's rows nor failed (%d snapshots): %s", out.snapshots, out.said)
	}
	for _, want := range []string{"inside one transaction", "do not describe one row", "do not repeat the changes before it", "DEFERRABLE"} {
		if !strings.Contains(out.said, want) {
			t.Errorf("the update was refused, but not for this reason (missing %q): %s", want, out.said)
		}
	}
}
