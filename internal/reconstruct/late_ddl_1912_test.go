package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The window of #1912, by time and by position. Each row is one statement
// against a snapshot taken at binlog.000009:500 and, where the run has one, a
// cut at binlog.000010:300. afterSince and beforeUntil are what the index
// server answered for the statement's time.
func TestDDLWindow_place(t *testing.T) {
	anchor := &query.BinlogPos{File: "binlog.000009", Pos: 500}
	cut := &query.BinlogPos{File: "binlog.000010", Pos: 300}
	tests := []struct {
		name                    string
		file                    string
		pos                     uint64
		afterSince, beforeUntil bool
		anchor, cut             *query.BinlogPos
		want                    int
	}{
		// By time, as before #1912.
		{"in the window by time and by position", "binlog.000010", 100, true, true, anchor, cut, ddlByTime},
		{"in the window by time, snapshot without a position", "binlog.000010", 100, true, true, nil, nil, ddlByTime},
		{"in the window by time, row without a position", "", 0, true, true, anchor, cut, ddlByTime},
		{"after the target by time and past the cut", "binlog.000010", 900, true, false, anchor, cut, ddlOutside},
		{"after the target by time, a run with no cut", "binlog.000010", 100, true, false, anchor, nil, ddlOutside},

		// Indexed late: its time is before the snapshot's, its position after.
		{"indexed late, inside the cut", "binlog.000010", 100, false, true, anchor, cut, ddlByPosition},
		{"indexed late, a run with no cut", "binlog.000010", 100, false, true, anchor, nil, ddlByPosition},
		{"indexed late, same file as the snapshot", "binlog.000009", 501, false, true, anchor, cut, ddlByPosition},
		// Positions compare as numbers, not as text: 1200 is after 500, and
		// 90 is before it.
		{"indexed late, a position with more digits", "binlog.000009", 1200, false, true, anchor, cut, ddlByPosition},
		{"before the snapshot, a position with fewer digits", "binlog.000009", 90, false, true, anchor, cut, ddlOutside},
		// The last statement on a quiet source: no row change follows it, so
		// the cut never moves past it. Its time still says it ran before the
		// target.
		{"indexed late, past the cut", "binlog.000010", 900, false, true, anchor, cut, ddlByPosition},

		// Case 7: before the snapshot by both. Never refused.
		{"before the snapshot, an earlier file", "binlog.000008", 900, false, true, anchor, cut, ddlOutside},
		{"before the snapshot, same file", "binlog.000009", 499, false, true, anchor, cut, ddlOutside},
		{"ends exactly at the snapshot's position", "binlog.000009", 500, false, true, anchor, cut, ddlOutside},
		{"before the snapshot, a run with no cut", "binlog.000008", 900, false, true, anchor, nil, ddlOutside},

		// File names compare by number, not as text: 000009 is before 000010,
		// and 999999 is before 1000000.
		{"file 9 against a snapshot in file 10", "binlog.000009", 900, false, true,
			&query.BinlogPos{File: "binlog.000010", Pos: 4}, nil, ddlOutside},
		{"file 10 against a snapshot in file 9", "binlog.000010", 4, false, true,
			&query.BinlogPos{File: "binlog.000009", Pos: 900}, nil, ddlByPosition},
		{"file 1000000 against a snapshot in file 999999", "binlog.1000000", 4, false, true,
			&query.BinlogPos{File: "binlog.999999", Pos: 900}, nil, ddlByPosition},
		{"file 999999 against a snapshot in file 1000000", "binlog.999999", 900, false, true,
			&query.BinlogPos{File: "binlog.1000000", Pos: 4}, nil, ddlOutside},

		// Another file name is another sequence, a source the index followed
		// before. Read by position it would refuse for good.
		{"another file name, sorts after the snapshot", "mysql-bin.000812", 100, false, true, anchor, cut, ddlOutside},
		{"another file name, a longer one", "mysql-bin-changelog.000002", 100, false, true, anchor, nil, ddlOutside},
		{"another file name, inside the window by time", "mysql-bin.000812", 100, true, true, anchor, cut, ddlByTime},
		{"the same name in another case", "BINLOG.000010", 100, false, true, anchor, cut, ddlOutside},
		{"a name with a dot in it, same sequence", "db.prod.000010", 100, false, true,
			&query.BinlogPos{File: "db.prod.000009", Pos: 500}, nil, ddlByPosition},
		{"names that share only what is before their first dot", "db.west.000010", 100, false, true,
			&query.BinlogPos{File: "db.east.000009", Pos: 500}, nil, ddlOutside},
		{"a name with no suffix against one with", "binlog", 100, false, true, anchor, nil, ddlOutside},

		// A time past the target with a position inside the cut: the source's
		// clock is ahead, or the statement sits between the last row change
		// at or before the target and the first one past it.
		{"after the target by time, inside the window by position", "binlog.000010", 100, true, false, anchor, cut, ddlByPosition},
		{"after the target by time, exactly at the cut", "binlog.000010", 300, true, false, anchor, cut, ddlByPosition},
		{"after the target by time, before the snapshot by position", "binlog.000008", 100, true, false, anchor, cut, ddlOutside},

		// A row with no position cannot be placed before the snapshot.
		{"no position, ran before the snapshot's time", "", 0, false, true, anchor, cut, ddlUnplaced},
		{"a file and no position", "binlog.000008", 0, false, true, anchor, cut, ddlUnplaced},
		{"a position and no file", "", 100, false, true, anchor, cut, ddlUnplaced},
		{"no position, ran after the target", "", 0, true, false, anchor, cut, ddlOutside},

		// A snapshot with no position places nothing by position: its row
		// changes are fetched by time alone too.
		{"snapshot without a position, ran before its time", "binlog.000010", 100, false, true, nil, nil, ddlOutside},
		{"snapshot without a position, a cut alone", "binlog.000010", 100, false, true, nil, cut, ddlOutside},
		{"snapshot without a position, row without one", "", 0, false, true, nil, nil, ddlOutside},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := DDLWindow{Anchor: tt.anchor, Cut: tt.cut}
			d := destructiveDDL{Type: "TRUNCATE TABLE", File: tt.file, Pos: tt.pos,
				AfterSince: tt.afterSince, AtOrBeforeUntil: tt.beforeUntil}
			if got := w.place(d); got != tt.want {
				t.Errorf("place = %d, want %d", got, tt.want)
			}
		})
	}
}

// A footer with no usable position gives no anchor. One read as a position
// would place every statement ever recorded after it.
func TestAnchorOf(t *testing.T) {
	for _, tt := range []struct {
		name string
		file string
		pos  int64
		want *query.BinlogPos
	}{
		{"file and position", "binlog.000003", 500, &query.BinlogPos{File: "binlog.000003", Pos: 500}},
		{"nothing recorded", "", 0, nil},
		{"a file and no position", "binlog.000003", 0, nil},
		{"a position and no file", "", 500, nil},
		{"a negative position", "binlog.000003", -1, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := AnchorOf(baseline.DumpMetadata{BinlogFile: tt.file, BinlogPos: tt.pos})
			if (got == nil) != (tt.want == nil) || (got != nil && *got != *tt.want) {
				t.Errorf("AnchorOf = %v, want %v", got, tt.want)
			}
		})
	}
}

func lateDDLMock(t *testing.T, since, until time.Time, rows *sqlmock.Rows) (context.Context, func(w DDLWindow) error) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
		WithArgs(since, until, "shop", "orders").WillReturnRows(rows)
	ctx := context.Background()
	return ctx, func(w DDLWindow) error { return CheckDestructiveDDL(ctx, db, "shop", "orders", w) }
}

// The first statement inside the window is the one reported, and an older one
// from before the snapshot does not stand in its way (case 7) nor get reported
// in its place.
func TestCheckDestructiveDDL_reportsTheLateStatementAndNotAnOlderOne(t *testing.T) {
	since := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	until := time.Date(2026, 1, 2, 10, 5, 0, 0, time.UTC)
	old := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	late := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	_, check := lateDDLMock(t, since, until, sqlmock.NewRows(ddlCols).
		AddRow(1, "DROP TABLE", old, "binlog.000002", 77, false, true).
		AddRow(2, "TRUNCATE TABLE", late, "binlog.000009", 640, false, true))

	err := check(DDLWindow{Since: since, Until: until, Anchor: &query.BinlogPos{File: "binlog.000009", Pos: 500}})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
	for _, want := range []string{
		"TRUNCATE TABLE on shop.orders", "run at 2026-01-02T10:00:00Z", "recorded at binlog.000009:640",
		"the snapshot is at binlog.000009:500", "though its time is outside them", "Take a new snapshot",
		"row 2 of schema_changes", "DELETE FROM schema_changes WHERE id IN (2);",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
	for _, not := range []string{"DROP TABLE", "binlog.000002", "--", "\u2014"} {
		if strings.Contains(err.Error(), not) {
			t.Errorf("the refusal says %q:\n%v", not, err)
		}
	}
}

// Only statements from before the snapshot, by time and by position: nothing
// to refuse, however many there are (case 7).
func TestCheckDestructiveDDL_statementsBeforeTheSnapshotNeverRefuse(t *testing.T) {
	since := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	until := time.Date(2026, 1, 2, 10, 5, 0, 0, time.UTC)
	old := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	_, check := lateDDLMock(t, since, until, sqlmock.NewRows(ddlCols).
		AddRow(3, "TRUNCATE TABLE", old, "binlog.000002", 77, false, true).
		AddRow(4, "TRUNCATE TABLE", old.Add(time.Hour), "binlog.000009", 500, false, true))
	if err := check(DDLWindow{Since: since, Until: until,
		Anchor: &query.BinlogPos{File: "binlog.000009", Pos: 500},
		Cut:    &query.BinlogPos{File: "binlog.000010", Pos: 300}}); err != nil {
		t.Fatalf("a statement from before the snapshot refused the run: %v", err)
	}
}

// A row with no position says so, and is counted as after the snapshot.
func TestCheckDestructiveDDL_aRowWithNoPositionRefusesAndSaysWhy(t *testing.T) {
	since := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	until := time.Date(2026, 1, 2, 10, 5, 0, 0, time.UTC)
	_, check := lateDDLMock(t, since, until, sqlmock.NewRows(ddlCols).
		AddRow(5, "TRUNCATE TABLE", since.Add(-time.Minute), "", 0, false, true))
	err := check(DDLWindow{Since: since, Until: until, Anchor: &query.BinlogPos{File: "binlog.000009", Pos: 500}})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
	if strings.Contains(err.Error(), "Take a new snapshot") {
		t.Errorf("the refusal offers a new snapshot, which does not clear a row with no position:\n%v", err)
	}
	for _, want := range []string{"its binlog position is not recorded", "counted as after it", "DELETE FROM schema_changes WHERE id IN (5);"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not say %q:\n%v", want, err)
		}
	}
}

// verify's lookup stays a lookup by time: it names no anchor, so a statement
// outside its two snapshots' times is not found.
func TestFindDestructiveDDL_looksByTimeAlone(t *testing.T) {
	since := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	until := time.Date(2026, 1, 2, 10, 5, 0, 0, time.UTC)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("detected_at >= \\?, detected_at <= \\?").
		WithArgs(since, until, "shop", "orders").
		WillReturnRows(sqlmock.NewRows(ddlCols).
			AddRow(6, "TRUNCATE TABLE", since.Add(-time.Minute), "binlog.000010", 100, false, true).
			AddRow(7, "DROP TABLE", since, "binlog.000010", 200, true, true))
	ddl, at, found, err := FindDestructiveDDL(context.Background(), db, "shop", "orders", since, until)
	if err != nil || !found || ddl != "DROP TABLE" || !at.Equal(since) {
		t.Fatalf("FindDestructiveDDL = %q %s %v %v, want the DROP TABLE in the same second as since", ddl, at, found, err)
	}
}

// The mark (#1912): a row at or below it is not placed by position alone.
func TestDDLWindow_placeWithAMark(t *testing.T) {
	anchor := &query.BinlogPos{File: "binlog.000003", Pos: 500}
	cut := &query.BinlogPos{File: "binlog.000004", Pos: 300}
	mark := &DDLMark{ID: 40}
	tests := []struct {
		name                    string
		id                      uint64
		file                    string
		pos                     uint64
		afterSince, beforeUntil bool
		mark                    *DDLMark
		want                    int
	}{
		// The failover case: an old statement from a numbering that started
		// over under the same name, indexed long before the snapshot.
		{"old numbering, indexed before the mark", 12, "binlog.000412", 900, false, true, mark, ddlOutside},
		{"old numbering, exactly the mark", 40, "binlog.000412", 900, false, true, mark, ddlOutside},
		{"late, indexed after the mark", 41, "binlog.000003", 640, false, true, mark, ddlByPosition},
		{"no position, indexed before the mark", 12, "", 0, false, true, mark, ddlOutside},
		{"no position, indexed after the mark", 41, "", 0, false, true, mark, ddlUnplaced},
		// By time it counts whatever its id.
		{"inside by time, indexed before the mark", 12, "binlog.000003", 640, true, true, mark, ddlByTime},
		{"inside by the cut only, indexed before the mark", 12, "binlog.000004", 100, true, false, mark, ddlOutside},
		{"inside by the cut only, indexed after the mark", 41, "binlog.000004", 100, true, false, mark, ddlByPosition},
		// No mark: as before.
		{"old numbering, no mark", 12, "binlog.000412", 900, false, true, nil, ddlByPosition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := DDLWindow{Anchor: anchor, Cut: cut, Mark: tt.mark}
			d := destructiveDDL{ID: tt.id, Type: "TRUNCATE TABLE", File: tt.file, Pos: tt.pos,
				AfterSince: tt.afterSince, AtOrBeforeUntil: tt.beforeUntil}
			if got := w.place(d); got != tt.want {
				t.Errorf("place = %d, want %d", got, tt.want)
			}
		})
	}
}

// A mark is stamped only by a check that placed statements by position, in
// the same binlog sequence as the new file's anchor.
func TestMarkToStamp(t *testing.T) {
	const run = `{"id":7}`
	a := &query.BinlogPos{File: "binlog.000003", Pos: 500}
	c := &query.BinlogPos{File: "binlog.000004", Pos: 300}
	for _, tt := range []struct {
		name string
		w    DDLWindow
		run  string
		want string
	}{
		{"positional check", DDLWindow{Anchor: a, Cut: c}, run, run},
		{"no run mark", DDLWindow{Anchor: a, Cut: c}, "", ""},
		{"base without a position: the check looked by time alone", DDLWindow{Cut: c}, run, ""},
		{"no cut", DDLWindow{Anchor: a}, run, ""},
		{"base from another binlog sequence than the cut", DDLWindow{Anchor: &query.BinlogPos{File: "mysql-bin.000003", Pos: 500}, Cut: c}, run, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := markToStamp(tt.run, tt.w); got != tt.want {
				t.Errorf("markToStamp = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDDLMark_encodeAndParse(t *testing.T) {
	m := DDLMark{ID: 42, File: "binlog.000412", Pos: 900, DetectedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Type: "TRUNCATE TABLE"}
	got := ParseDDLMark(m.Encode())
	if got == nil || *got != m {
		t.Fatalf("round trip = %+v, want %+v", got, m)
	}
	for _, raw := range []string{"", "not json", `{"id":0}`, `{}`} {
		if ParseDDLMark(raw) != nil {
			t.Errorf("ParseDDLMark(%q) gave a mark", raw)
		}
	}
}

// A mark is used only while its row is still the row it names. Gone or
// different, the index handed its ids out again, and the rows are placed by
// position as if there were no mark.
func TestCheckDestructiveDDL_aMarkIsUsedOnlyWhileItsRowIsTheSame(t *testing.T) {
	since := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	until := time.Date(2026, 1, 2, 10, 5, 0, 0, time.UTC)
	old := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	mark := &DDLMark{ID: 40, File: "binlog.000412", Pos: 900, DetectedAt: old, Type: "TRUNCATE TABLE"}
	w := DDLWindow{Since: since, Until: until, Anchor: &query.BinlogPos{File: "binlog.000003", Pos: 500}, Mark: mark}
	markCols := []string{"binlog_file", "binlog_pos", "detected_at", "ddl_type"}
	for _, tt := range []struct {
		name    string
		markRow *sqlmock.Rows
		refuse  bool
	}{
		{"the same row", sqlmock.NewRows(markCols).AddRow("binlog.000412", 900, old, "TRUNCATE TABLE"), false},
		{"the row is gone", sqlmock.NewRows(markCols), true},
		{"another statement under that id", sqlmock.NewRows(markCols).AddRow("binlog.000002", 77, old, "DROP TABLE"), true},
		{"another time under that id", sqlmock.NewRows(markCols).AddRow("binlog.000412", 900, old.Add(time.Second), "TRUNCATE TABLE"), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
				WithArgs(since, until, "shop", "orders").
				WillReturnRows(sqlmock.NewRows(ddlCols).
					AddRow(12, "TRUNCATE TABLE", old, "binlog.000412", 900, false, true).
					AddRow(13, "TRUNCATE TABLE", old, "binlog.000413", 50, false, true))
			mock.ExpectQuery("FROM schema_changes WHERE id = \\?").WithArgs(uint64(40)).WillReturnRows(tt.markRow)
			err = CheckDestructiveDDL(context.Background(), db, "shop", "orders", w)
			if got := errors.Is(err, ErrDestructiveDDL); got != tt.refuse {
				t.Fatalf("refused = %v (%v), want %v", got, err, tt.refuse)
			}
			if tt.refuse && !strings.Contains(err.Error(), "DELETE FROM schema_changes WHERE id IN (12, 13);") {
				t.Errorf("the refusal does not name both rows in one DELETE:\n%v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// With no row the mark would leave out, the mark's row is not read.
func TestCheckDestructiveDDL_aMarkThatChangesNothingIsNotRead(t *testing.T) {
	since := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	until := time.Date(2026, 1, 2, 10, 5, 0, 0, time.UTC)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT id, ddl_type, detected_at, binlog_file, binlog_pos").
		WithArgs(since, until, "shop", "orders").
		WillReturnRows(sqlmock.NewRows(ddlCols).AddRow(41, "TRUNCATE TABLE", since.Add(-time.Minute), "binlog.000003", 640, false, true))
	err = CheckDestructiveDDL(context.Background(), db, "shop", "orders", DDLWindow{Since: since, Until: until,
		Anchor: &query.BinlogPos{File: "binlog.000003", Pos: 500}, Mark: &DDLMark{ID: 40}})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL: row 41 was indexed after the mark", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
