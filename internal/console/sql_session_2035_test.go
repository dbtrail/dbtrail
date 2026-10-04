package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// writeSQLClockFixture adds shop.visits to the fixture snapshot: a DATETIME
// and a TIMESTAMP holding the same text, CREATE TABLE in the footer (which
// shop.orders, written the old way, does not carry).
func writeSQLClockFixture(t *testing.T, root string) {
	t.Helper()
	createSQL := "CREATE TABLE `visits` (\n  `id` int NOT NULL,\n  `seen_at` datetime DEFAULT NULL,\n" +
		"  `stamped` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(root, sqlSnapshotDirName, "shop", "visits.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "2026-10-04 10:00:00", "2026-10-04 10:00:00"}, make([]bool, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// The port's session (#2035) through runSQL and the real worker: the time
// zone reaches the engine, a DATETIME stays the wall clock MySQL holds while
// a TIMESTAMP stays an instant, and a table whose column types the copy does
// not record is refused by name instead of being read shifted.
func TestSQL_realWorkerSessionTimeZone_2035(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	writeSQLClockFixture(t, f.root)
	ctx := context.Background()
	tokyo := sqlsandbox.Session{TimeZone: "Asia/Tokyo"}
	const q = "SELECT seen_at, stamped, current_setting('TimeZone') AS z FROM shop.visits " +
		"WHERE seen_at >= '2026-10-04 09:30:00' AND seen_at < '2026-10-04 10:30:00' AND stamped = '2026-10-04 19:00:00'"

	out, err := f.s.runSQL(ctx, f.s.cm.boot, "u", q, "", 0, tokyo)
	if err != nil {
		t.Fatalf("under Asia/Tokyo: %v", err)
	}
	res := out.Result
	if len(res.Rows) != 1 {
		t.Fatalf("under Asia/Tokyo: %d rows, want 1: the DATETIME literal is a wall clock, the TIMESTAMP literal is read in the session's zone", len(res.Rows))
	}
	if got := res.Columns[0].Type; got != "TIMESTAMP" {
		t.Errorf("seen_at (DATETIME) type under a session zone = %s, want the zone-less TIMESTAMP", got)
	}
	if got := res.Columns[1].Type; got != "TIMESTAMPTZ" && got != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("stamped (TIMESTAMP) type = %s, want it left an instant", got)
	}
	if row := res.Rows[0]; row[0] != "2026-10-04T10:00:00Z" || row[1] != "2026-10-04T10:00:00Z" || row[2] != "Asia/Tokyo" {
		t.Errorf("row = %v, want the stored wall clock, the stored instant, and the session's zone", row)
	}

	// No session: the columns are what they always were, under UTC.
	out, err = f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT seen_at, current_setting('TimeZone') FROM shop.visits", "", 0, sqlsandbox.Session{})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Result.Columns[0].Type; got == "TIMESTAMP" || out.Result.Rows[0][1] != "UTC" {
		t.Errorf("no session: seen_at type = %s under %v, want the column untouched under UTC", got, out.Result.Rows[0][1])
	}

	// A table with no recorded column types cannot be read under a zone.
	_, err = f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT * FROM shop.orders", "", 0, tokyo)
	var refusal *sqlRefusal
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, "shop.orders") || !strings.Contains(refusal.Message, "time_zone") {
		t.Errorf("shop.orders under a session zone: err = %v, want a refusal naming the table and the setting", err)
	}
	// On the port's wire it is a refusal the client is shown, not a worker failure.
	_, err = (&SQLOnCopy{s: f.s, b: f.s.cm.boot, user: "server:x"}).Run(ctx, "SELECT * FROM shop.orders", "", tokyo)
	var un *sqlsandbox.UnavailableError
	if !errors.As(err, &un) || !strings.Contains(un.Reason, "shop.orders") {
		t.Errorf("port: err = %v (%T), want an UnavailableError naming the table", err, err)
	}
	// A listing may read any table, so it is refused too; without the zone it runs.
	if _, err = f.s.runSQL(ctx, f.s.cm.boot, "u", "SHOW ALL TABLES", "", 0, tokyo); !errors.As(err, &refusal) {
		t.Errorf("SHOW ALL TABLES under a session zone: err = %v, want the refusal", err)
	}
	if _, err = f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT * FROM shop.orders", "", 0, sqlsandbox.Session{}); err != nil {
		t.Errorf("shop.orders with no session: %v", err)
	}
	// A statement that reads no table is not held back by one it does not read.
	out, err = f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT current_setting('TimeZone')", "", 0, tokyo)
	if err != nil || out.Result.Rows[0][0] != "Asia/Tokyo" {
		t.Errorf("a table-less statement under a session zone = (%v, %v), want it to run under the zone", out.Result.Rows, err)
	}

	// sql_select_limit reaches the worker.
	out, err = f.s.runSQL(ctx, f.s.cm.boot, "u", "SELECT * FROM range(9)", "", 0, sqlsandbox.Session{SelectLimit: 2})
	if err != nil || len(out.Result.Rows) != 2 || out.Result.Truncated {
		t.Errorf("sql_select_limit 2: rows=%d truncated=%v err=%v, want 2 rows, not truncated", len(out.Result.Rows), out.Result.Truncated, err)
	}
}

// sqlWallClockDatetimes marks only what the render defines and never the
// caller's own slice.
func TestSQLWallClockDatetimes(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	writeSQLClockFixture(t, f.root)
	in, err := f.s.buildViewsInput(context.Background(), f.s.cm.boot, viewsRequest{PinSnapshot: true, OmitEvents: true, StateOnly: true, ForStatement: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Baselines) != 2 {
		t.Fatalf("fixture has %d tables, want 2", len(in.Baselines))
	}
	if _, unknown := sqlWallClockDatetimes(in); unknown != "shop.orders" {
		t.Errorf("every view: unknown = %q, want shop.orders (no recorded column types)", unknown)
	}
	narrowed := in
	narrowed.OnlyViews = map[string]bool{}
	for _, n := range in.ViewNames() {
		if n.View == "visits" {
			narrowed.OnlyViews[n.Key] = true
		}
	}
	tables, unknown := sqlWallClockDatetimes(narrowed)
	if unknown != "" {
		t.Fatalf("visits alone: unknown = %q, want none", unknown)
	}
	for _, tb := range tables {
		if want := tb.Table == "visits"; tb.WallClockDatetimes != want {
			t.Errorf("%s: WallClockDatetimes = %v, want %v", tb.Table, tb.WallClockDatetimes, want)
		}
		if tb.Table == "visits" && (len(tb.Datetimes) != 1 || tb.Datetimes[0] != "seen_at") {
			t.Errorf("visits Datetimes = %v, want [seen_at]", tb.Datetimes)
		}
	}
	for _, tb := range in.Baselines {
		if tb.WallClockDatetimes {
			t.Errorf("%s: the caller's own slice was marked", tb.Table)
		}
	}
}

// The job the port's session produces: the session itself reaches the
// runner, and the events view is rendered for a zone other than UTC
// (commit_time as an instant). Without a session neither happens.
func TestSQL_sessionReachesTheJobAndTheEventsView_2035(t *testing.T) {
	runner := &fakeSQLRunner{res: oneRowResult(), refs: &sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{{Name: "events"}}}}
	f := newSQLFixture(t, runner, true)
	const instant = `AT TIME ZONE 'UTC' END AS "commit_time"`
	sess := sqlsandbox.Session{TimeZone: "Asia/Tokyo", SelectLimit: 4}

	f.expectArchive()
	if _, err := f.s.runSQL(context.Background(), f.s.cm.boot, "u", "SELECT commit_time FROM events", "", 0, sess); err != nil {
		t.Fatalf("under a session: %v", err)
	}
	job := runner.last(t)
	if job.Session != sess {
		t.Errorf("the job's session = %+v, want %+v", job.Session, sess)
	}
	if !strings.Contains(job.ViewsSQL, instant) {
		t.Errorf("under a session zone the events view does not make commit_time an instant:\n%s", job.ViewsSQL)
	}

	f.expectArchive()
	if _, err := f.s.runSQL(context.Background(), f.s.cm.boot, "u", "SELECT commit_time FROM events", "", 0, sqlsandbox.Session{SelectLimit: 4}); err != nil {
		t.Fatalf("no zone: %v", err)
	}
	if job := runner.last(t); strings.Contains(job.ViewsSQL, instant) || !strings.Contains(job.ViewsSQL, `AS "commit_time"`) {
		t.Errorf("with no session zone the events view changed (or is missing):\n%s", job.ViewsSQL)
	}
}
