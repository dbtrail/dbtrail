package query

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/event"
	drivermysql "github.com/go-sql-driver/mysql"
)

// #1720: the baseline-anchored table-window fetch (schema + table + anchor +
// page) reads its page in INDEX ORDER off idx_row_lookup instead of
// materialising the page's keys and looking each one up on PRIMARY. The
// late-materialisation JOIN stays for every other shape: it exists because a
// filesort over the wide row images can overflow sort_buffer_size (1038), and
// only the index-ordered form is guaranteed to need no filesort.

func streamOpts() Options {
	since := time.Date(2026, 9, 18, 4, 35, 29, 0, time.UTC)
	until := since.Add(5 * time.Minute)
	return Options{
		Schema: "tpcc", Table: "warehouse1",
		Since: &since, SincePos: &BinlogPos{File: "mysql-bin.000985", Pos: 12589304},
		Until: &until, UntilPos: &BinlogPos{File: "mysql-bin.000989", Pos: 12394659},
		Limit: 100000, Order: "ASC",
	}
}

func TestBuildQuery_streamShapeReadsInIndexOrder(t *testing.T) {
	q, args := buildQuery(streamOpts())
	for _, want := range []string{
		"FROM binlog_events FORCE INDEX (idx_row_lookup)",
		"schema_name = ?", "table_name = ?",
		"event_timestamp >= ?", "start_pos >= ?", "event_timestamp <= ?", "end_pos <= ?",
		"ORDER BY event_timestamp ASC, event_id ASC LIMIT ?",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("stream shape lacks %q:\n%s", want, q)
		}
	}
	for _, banned := range []string{" JOIN (", " AS k", "be.event_id", "ROW_NUMBER"} {
		if strings.Contains(q, banned) {
			t.Errorf("stream shape still carries %q:\n%s", banned, q)
		}
	}
	// Everything the WHERE binds, in order, then the LIMIT last.
	if n := strings.Count(q, "?"); n != len(args) {
		t.Fatalf("%d placeholders, %d args\n%s", n, len(args), q)
	}
	if args[len(args)-1] != 100000 {
		t.Fatalf("last arg = %v, want the LIMIT", args[len(args)-1])
	}
	// The SELECT list is the JOIN form's, minus the alias: the shape must
	// not drift to a different column set, and the alias strip must touch
	// nothing but the prefix.
	o := streamOpts()
	o.Order = "DESC"
	joinQ, _ := buildQuery(o)
	sel := func(q string) string { return q[len("SELECT "):strings.Index(q, " FROM ")] }
	if got, want := sel(q), strings.ReplaceAll(sel(joinQ), "be.", ""); got != want {
		t.Fatalf("stream shape SELECT list:\n%s\nwant the join form's minus the alias:\n%s", got, want)
	}
}

// TestBuildQuery_streamShapeIsExact: every condition of the shape is
// load-bearing. Dropping any one falls back to the JOIN form, whose
// correctness does not depend on index order.
func TestBuildQuery_streamShapeIsExact(t *testing.T) {
	ts := time.Date(2026, 9, 18, 4, 40, 0, 0, time.UTC)
	et := event.EventUpdate
	cases := []struct {
		name   string
		mutate func(*Options)
	}{
		{"no table", func(o *Options) { o.Table = "" }},
		{"no schema", func(o *Options) { o.Schema = "" }},
		{"no anchor position", func(o *Options) { o.SincePos = nil }},
		{"no since", func(o *Options) { o.Since = nil }},
		{"no page limit", func(o *Options) { o.Limit = 0 }},
		{"per-pk cap", func(o *Options) { o.LimitPerPK = 1 }},
		{"pk lookup", func(o *Options) { o.PKValues = "7" }},
		{"pk list", func(o *Options) { o.PKValuesIn = []string{"7", "8"} }},
		{"pk range", func(o *Options) { o.PKRange = &PKRange{} }},
		{"newest first", func(o *Options) { o.Order = "DESC" }},
		{"event anchor", func(o *Options) { o.EventAnchor = &EventCursor{Timestamp: ts, EventID: 5} }},
		{"backward cursor", func(o *Options) { o.Order = "DESC"; o.BeforeEvent = &EventCursor{Timestamp: ts, EventID: 5} }},
	}
	for _, c := range cases {
		o := streamOpts()
		c.mutate(&o)
		q, _ := buildQuery(o)
		if strings.Contains(q, "FORCE INDEX") || !strings.Contains(q, " JOIN (") {
			t.Errorf("%s: still the stream shape:\n%s", c.name, q)
		}
	}
	// And conditions that do NOT change the shape: they are plain WHERE
	// clauses in either form.
	for _, keep := range []struct {
		name   string
		mutate func(*Options)
	}{
		{"event type", func(o *Options) { o.EventType = &et }},
		{"forward cursor", func(o *Options) { o.AfterEvent = &EventCursor{Timestamp: ts, EventID: 5} }},
		{"deny table", func(o *Options) { o.DenyTables = []SchemaTable{{Schema: "x", Table: "y"}} }},
		{"event id floor", func(o *Options) { o.SinceEventID = 42 }},
		{"no upper bound", func(o *Options) { o.Until, o.UntilPos = nil, nil }},
	} {
		o := streamOpts()
		keep.mutate(&o)
		q, _ := buildQuery(o)
		if !strings.Contains(q, "FORCE INDEX (idx_row_lookup)") {
			t.Errorf("%s: left the stream shape:\n%s", keep.name, q)
		}
	}
}

// TestBuildQuery_sinceEventIDIsAFloorInEitherShape: the event_id floor is a
// plain predicate the index can evaluate, present whenever set, absent when 0.
func TestBuildQuery_sinceEventIDIsAFloorInEitherShape(t *testing.T) {
	o := streamOpts()
	o.SinceEventID = 4500
	q, args := buildQuery(o)
	if !strings.Contains(q, "event_id > ?") {
		t.Fatalf("no event_id floor in the stream shape:\n%s", q)
	}
	found := false
	for _, a := range args {
		if v, ok := a.(uint64); ok && v == 4500 {
			found = true
		}
	}
	if !found {
		t.Fatalf("floor value not bound: %v", args)
	}
	o.Order = "DESC" // JOIN shape
	q, _ = buildQuery(o)
	if !strings.Contains(q, "event_id > ?") || !strings.Contains(q, " JOIN (") {
		t.Fatalf("no event_id floor in the join shape:\n%s", q)
	}
	o.SinceEventID = 0
	q, _ = buildQuery(o)
	if strings.Contains(q, "event_id > ?") {
		t.Fatalf("a zero floor was emitted:\n%s", q)
	}
}

// TestBuildQuery_streamShapeKeepsTheKeysetCursor: page 2 onwards adds the
// exact (event_timestamp, event_id) cut, in the same statement, so the index
// range starts at the cursor.
func TestBuildQuery_streamShapeKeepsTheKeysetCursor(t *testing.T) {
	o := streamOpts()
	o.AfterEvent = &EventCursor{Timestamp: o.Since.Add(time.Minute), EventID: 9000}
	q, _ := buildQuery(o)
	if !strings.Contains(q, "(event_timestamp > ? OR (event_timestamp = ? AND event_id > ?))") {
		t.Fatalf("keyset cut missing:\n%s", q)
	}
}

// TestFetch_streamShapeNamesTheMissingIndex: FORCE INDEX on a hand-built
// index without idx_row_lookup fails with MySQL 1176; the error says which key
// and how to add it, instead of reading as a bug in the refresh. Any other
// error, and the same error outside the shape, pass through untouched.
func TestFetch_streamShapeNamesTheMissingIndex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	missing := &drivermysql.MySQLError{Number: 1176, Message: "Key 'idx_row_lookup' doesn't exist in table 'binlog_events'"}

	mock.ExpectQuery("FORCE INDEX").WillReturnError(missing)
	_, err = New(db).Fetch(context.Background(), streamOpts())
	if err == nil || !strings.Contains(err.Error(), "ALTER TABLE binlog_events ADD INDEX idx_row_lookup") || !errors.Is(err, missing) {
		t.Fatalf("stream shape on a missing index: %v", err)
	}

	mock.ExpectQuery("FORCE INDEX").WillReturnError(&drivermysql.MySQLError{Number: 1146, Message: "Table doesn't exist"})
	_, err = New(db).Fetch(context.Background(), streamOpts())
	if err == nil || strings.Contains(err.Error(), "ADD INDEX") {
		t.Fatalf("another error got the index hint: %v", err)
	}

	o := streamOpts()
	o.Order = "DESC" // JOIN shape: no FORCE INDEX, so 1176 there is something else
	mock.ExpectQuery("JOIN").WillReturnError(missing)
	_, err = New(db).Fetch(context.Background(), o)
	if err == nil || strings.Contains(err.Error(), "ADD INDEX") {
		t.Fatalf("join shape got the index hint: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestStreamCaptured_reportsErrorsAsErrors: a read failure is an error, never
// "no stream". ReconstructTable refuses on it; a (false, nil) there would
// silently drop the floor on every refresh with nothing in the log.
func TestStreamCaptured_reportsErrorsAsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM stream_state").WillReturnError(&drivermysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
	if ok, err := StreamCaptured(context.Background(), db); err == nil || ok {
		t.Fatalf("denied read: (%v, %v), want (false, error)", ok, err)
	}
	mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}))
	if ok, err := StreamCaptured(context.Background(), db); err != nil || ok {
		t.Fatalf("empty table: (%v, %v), want (false, nil)", ok, err)
	}
	mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	if ok, err := StreamCaptured(context.Background(), db); err != nil || !ok {
		t.Fatalf("one row: (%v, %v), want (true, nil)", ok, err)
	}
}
