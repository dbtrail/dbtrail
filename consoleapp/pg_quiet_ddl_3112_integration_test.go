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

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A column change no write followed (#3112). PostgreSQL's change stream
// describes a table when it next carries a change of it, so an ALTER TABLE on
// a table nothing writes to afterwards is recorded nowhere. These tests take a
// full read, change tables that then stay quiet, write to ANOTHER table (an
// update with nothing captured is skipped whole), let the schedule's slot run,
// and compare the newest copy of every table with the source.

const (
	pgQdBusy   = "pgqd_busy"
	pgQdSlot   = "bintrail_pgqd_slot"
	pgQdPub    = "bintrail_pgqd_pub"
	pgQdSchema = "PgQd Other" // a quoted name: capitals and a space
)

// pgQdTable is one source table of these tests: how to write it in SQL and
// where a snapshot keeps it.
type pgQdTable struct{ schema, table string }

func (q pgQdTable) sql() string  { return `"` + q.schema + `"."` + q.table + `"` }
func (q pgQdTable) name() string { return q.schema + "." + q.table }

// pgQdSourceRows reads a table from the source: its columns in table order
// and every row as text, ordered by id.
func pgQdSourceRows(t *testing.T, source *pgconn.PgConn, q pgQdTable) (cols []string, rows [][]string) {
	t.Helper()
	_, names, err := pgText(t, source, `SELECT a.attname FROM pg_attribute a WHERE a.attrelid = '`+q.sql()+`'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`)
	if err != nil {
		t.Fatalf("source columns of %s: %v", q.name(), err)
	}
	var sel []string
	for _, n := range names {
		cols = append(cols, n[0])
		sel = append(sel, `"`+n[0]+`"::text`)
	}
	_, rows, err = pgText(t, source, "SELECT "+strings.Join(sel, ", ")+" FROM "+q.sql()+" ORDER BY id")
	if err != nil {
		t.Fatalf("source rows of %s: %v", q.name(), err)
	}
	return cols, rows
}

// pgQdCopyRows reads a table out of one snapshot: the file's columns in its
// order and every row as text, ordered by id.
func pgQdCopyRows(t *testing.T, snapshotDir string, q pgQdTable) (cols []string, rows [][]string) {
	t.Helper()
	file := filepath.Join(snapshotDir, q.schema, q.table+".parquet")
	duck, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer duck.Close()
	lit := "parquet_scan('" + strings.ReplaceAll(file, "'", "''") + "')"
	// The file's own column order: a result set's columns, not a query over
	// DESCRIBE, which does not keep it.
	head, err := duck.Query("SELECT * FROM " + lit + " LIMIT 0")
	if err != nil {
		t.Fatalf("columns of %s: %v", file, err)
	}
	cols, err = head.Columns()
	head.Close()
	if err != nil {
		t.Fatalf("columns of %s: %v", file, err)
	}
	var sel []string
	for _, c := range cols {
		sel = append(sel, `COALESCE(CAST("`+c+`" AS VARCHAR), 'NULL')`)
	}
	res, err := duck.Query("SELECT " + strings.Join(sel, ", ") + " FROM " + lit + " ORDER BY CAST(id AS BIGINT)")
	if err != nil {
		t.Fatalf("rows of %s: %v", file, err)
	}
	defer res.Close()
	for res.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := res.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = v.String
		}
		rows = append(rows, row)
	}
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	return cols, rows
}

// pgQdRun is one run of the scenario, at rest after the schedule's slot.
type pgQdRun struct {
	source    *pgconn.PgConn
	snapshots string
	first     string // the full read's folder
	all       []string
	upd       console.BaselineStatus
	st        console.BackupScheduleState
	logs      string
}

// pgQdScenario creates the busy table and tables, takes the full read, runs
// alters on the tables (which stay quiet afterwards), writes to the busy
// table, and lets the schedule's slot and whatever it starts come to rest.
func pgQdScenario(t *testing.T, tables []pgQdTable, columns map[string]string, alters []string) pgQdRun {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	logs := captureSlogFor(t)

	_, source, exec1 := pgTestSource(t, ctx, pgQdBusy, pgQdSlot, pgQdPub, "id int PRIMARY KEY, v text")
	dropExtra := func() {
		bg := context.Background()
		for _, q := range tables {
			_, _ = source.Exec(bg, "DROP TABLE IF EXISTS "+q.sql()).ReadAll()
		}
		_, _ = source.Exec(bg, `DROP SCHEMA IF EXISTS "`+pgQdSchema+`" CASCADE`).ReadAll()
	}
	dropExtra()
	t.Cleanup(dropExtra)
	exec1(`CREATE SCHEMA "` + pgQdSchema + `"`)
	exec1("INSERT INTO " + pgQdBusy + " VALUES (1, 'one')")
	for _, q := range tables {
		exec1("CREATE TABLE " + q.sql() + " (" + columns[q.name()] + ")")
		exec1("ALTER TABLE " + q.sql() + " REPLICA IDENTITY FULL")
		exec1("ALTER PUBLICATION " + pgQdPub + " ADD TABLE " + q.sql())
		exec1("INSERT INTO " + q.sql() + " VALUES (1, 'a', 10), (2, 'b', 20)")
	}

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	sup.tableDeltas = true
	t.Cleanup(sup.postRefresh.Wait)

	snapshots := t.TempDir()
	e, err := reg.Add(console.ServerEntry{
		Name: "pg-quiet-ddl", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgQdSlot, SourcePublication: pgQdPub,
		Schemas: "public," + pgQdSchema, BaselineDir: snapshots,
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
	// The full read holds every table as the source has it: what the test
	// compares later is a difference the ALTERs made, not one it started with.
	for _, q := range tables {
		wantCols, wantRows := pgQdSourceRows(t, source, q)
		gotCols, gotRows := pgQdCopyRows(t, filepath.Join(snapshots, first[0]), q)
		if !reflect.DeepEqual(gotCols, wantCols) || !reflect.DeepEqual(gotRows, wantRows) {
			t.Fatalf("the full read of %s holds %v %v, the source %v %v", q.name(), gotCols, gotRows, wantCols, wantRows)
		}
	}

	// The changes, and no write to those tables after them.
	for _, stmt := range alters {
		exec1(stmt)
	}
	// A change of another table: without one the update has nothing to apply
	// and is skipped before it reads anything.
	exec1("INSERT INTO " + pgQdBusy + " VALUES (2, 'two')")

	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the busy table's change indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgQdBusy).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 1 && covered == 1, "events=" + strconv.Itoa(events) + " checkpoint_covers=" + strconv.Itoa(covered)
	})
	// The quiet tables stayed quiet as far as capture knows.
	var quietEvents int
	if err := idx.QueryRowContext(ctx, "SELECT COUNT(*) FROM binlog_events WHERE table_name <> ?", pgQdBusy).Scan(&quietEvents); err != nil || quietEvents != 0 {
		t.Fatalf("capture recorded %d changes of the tables that were to stay quiet (err %v)", quietEvents, err)
	}

	// Snapshot folders are named to the second.
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

	return pgQdRun{source: source, snapshots: snapshots, first: first[0], all: pgUpdSnapshots(t, snapshots),
		upd: sup.RefreshStatus(e.ID), st: b.ScheduleState(e.ID), logs: logs.String()}
}

// said is everything the schedule and the update reported, for a failure
// message.
func (r pgQdRun) said() string {
	s := "update state " + r.upd.State + ": " + r.upd.LastError + "\nfallback: " + r.st.LastFallbackReason
	for _, rt := range r.upd.RefusedTables {
		s += "\n  refused " + rt.Name + " (" + rt.Verdict + "): " + rt.Reason
	}
	return s
}

// The changes of columns: every one of them must be seen without a write to
// its table. The newest copy of every table is the source's, columns and rows,
// and the update names exactly the changed tables as the reason it stopped.
func TestIntegrationPGQuietColumnChange_3112(t *testing.T) {
	std := "id int PRIMARY KEY, status text, amount int"
	tbl := func(name string) pgQdTable { return pgQdTable{"public", name} }
	dropped, added, renamed, retyped, readded := tbl("pgqd_dropped"), tbl("pgqd_added"), tbl("pgqd_renamed"), tbl("pgqd_retyped"), tbl("pgqd_readded")
	// A name with capitals, and the same name in two schemas: only the one in
	// the other schema changes.
	mixedPublic, mixedOther := tbl("PgQd Orders"), pgQdTable{pgQdSchema, "PgQd Orders"}
	untouched := tbl("pgqd_untouched")
	tables := []pgQdTable{dropped, added, renamed, retyped, readded, mixedPublic, mixedOther, untouched}
	columns := map[string]string{}
	for _, q := range tables {
		columns[q.name()] = std
	}
	changed := []pgQdTable{dropped, added, renamed, retyped, readded, mixedOther}

	run := pgQdScenario(t, tables, columns, []string{
		"ALTER TABLE " + dropped.sql() + " DROP COLUMN amount",
		"ALTER TABLE " + added.sql() + " ADD COLUMN region text NOT NULL DEFAULT 'eu'",
		"ALTER TABLE " + renamed.sql() + " RENAME COLUMN status TO state",
		"ALTER TABLE " + retyped.sql() + " ALTER COLUMN amount TYPE bigint",
		// The same name and type again: the table looks as it did, and every
		// row's value is gone.
		"ALTER TABLE " + readded.sql() + " DROP COLUMN amount",
		"ALTER TABLE " + readded.sql() + " ADD COLUMN amount int",
		"ALTER TABLE " + mixedOther.sql() + " DROP COLUMN status",
	})

	if len(run.all) < 2 {
		t.Fatalf("no copy was published after the changes: snapshots %v\n%s", run.all, run.said())
	}
	newest := filepath.Join(run.snapshots, run.all[len(run.all)-1])
	for _, q := range tables {
		wantCols, wantRows := pgQdSourceRows(t, run.source, q)
		gotCols, gotRows := pgQdCopyRows(t, newest, q)
		if !reflect.DeepEqual(gotCols, wantCols) {
			t.Errorf("%s: the newest copy has columns %v, the source has %v", q.name(), gotCols, wantCols)
		} else if !reflect.DeepEqual(gotRows, wantRows) {
			t.Errorf("%s: the newest copy holds %v, the source holds %v", q.name(), gotRows, wantRows)
		}
	}
	if t.Failed() {
		t.Fatalf("the copy published after column changes no write followed is not the source's.\n%s", run.said())
	}

	// How it got there: the update was refused for exactly the changed
	// tables, and the full read that replaced it read them again.
	if run.upd.State != "failed" {
		t.Fatalf("the update was not refused: %s", run.said())
	}
	var refused, want []string
	for _, rt := range run.upd.RefusedTables {
		refused = append(refused, rt.Name)
		if !strings.Contains(rt.Reason, "on the source") {
			t.Errorf("%s was refused for another reason: %s", rt.Name, rt.Reason)
		}
	}
	for _, q := range changed {
		want = append(want, q.name())
	}
	sort.Strings(refused)
	sort.Strings(want)
	if !reflect.DeepEqual(refused, want) {
		t.Fatalf("the update refused %q, want exactly the changed tables %q\n%s", refused, want, run.said())
	}
	if run.st.LastFallbackAt == "" {
		t.Fatalf("no full read replaced the refused update: %s", run.said())
	}
	meta, err := baseline.ReadParquetMetadata(filepath.Join(newest, "public", untouched.table+".parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Producer != baseline.ProducerDump {
		t.Fatalf("the newest copy was produced by %q, want a full read (%q)", meta.Producer, baseline.ProducerDump)
	}
}

// Changes that leave the list of columns alone refuse nothing: the update
// publishes as an update, with no full read in its place.
func TestIntegrationPGQuietChangeNotOfColumns_3112(t *testing.T) {
	q := pgQdTable{"public", "pgqd_benign"}
	run := pgQdScenario(t, []pgQdTable{q}, map[string]string{q.name(): "id int PRIMARY KEY, status text, amount int"}, []string{
		"CREATE INDEX pgqd_benign_status ON " + q.sql() + " (status)",
		"ALTER TABLE " + q.sql() + " ALTER COLUMN amount SET DEFAULT 7",
		"ALTER TABLE " + q.sql() + " ALTER COLUMN status SET NOT NULL",
		"COMMENT ON COLUMN " + q.sql() + ".status IS 'what it says'",
	})
	if run.upd.State != "succeeded" || run.st.LastFallbackAt != "" || len(run.all) != 2 {
		t.Fatalf("an index, a default, NOT NULL and a comment stopped the update: snapshots %v\n%s", run.all, run.said())
	}
	newest := filepath.Join(run.snapshots, run.all[1])
	wantCols, wantRows := pgQdSourceRows(t, run.source, q)
	gotCols, gotRows := pgQdCopyRows(t, newest, q)
	if !reflect.DeepEqual(gotCols, wantCols) || !reflect.DeepEqual(gotRows, wantRows) {
		t.Fatalf("the updated copy holds %v %v, the source %v %v", gotCols, gotRows, wantCols, wantRows)
	}
}
