package reconstruct

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

func lateDDLMock(t *testing.T, since, until time.Time, rows *sqlmock.Rows) (context.Context, func(w DDLWindow) error) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	mock.ExpectQuery("SELECT ddl_type, detected_at, binlog_file, binlog_pos").
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
		AddRow("DROP TABLE", old, "binlog.000002", 77, false, true).
		AddRow("TRUNCATE TABLE", late, "binlog.000009", 640, false, true))

	err := check(DDLWindow{Since: since, Until: until, Anchor: &query.BinlogPos{File: "binlog.000009", Pos: 500}})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
	for _, want := range []string{
		"TRUNCATE TABLE on shop.orders", "run at 2026-01-02T10:00:00Z", "recorded at binlog.000009:640",
		"the snapshot is at binlog.000009:500", "indexed after that snapshot was written", "Take a new snapshot",
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
		AddRow("TRUNCATE TABLE", old, "binlog.000002", 77, false, true).
		AddRow("TRUNCATE TABLE", old.Add(time.Hour), "binlog.000009", 500, false, true))
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
		AddRow("TRUNCATE TABLE", since.Add(-time.Minute), "", 0, false, true))
	err := check(DDLWindow{Since: since, Until: until, Anchor: &query.BinlogPos{File: "binlog.000009", Pos: 500}})
	if !errors.Is(err, ErrDestructiveDDL) {
		t.Fatalf("err = %v, want ErrDestructiveDDL", err)
	}
	for _, want := range []string{"its binlog position is not recorded", "counted as after it"} {
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
			AddRow("TRUNCATE TABLE", since.Add(-time.Minute), "binlog.000010", 100, false, true).
			AddRow("DROP TABLE", since, "binlog.000010", 200, true, true))
	ddl, at, found, err := FindDestructiveDDL(context.Background(), db, "shop", "orders", since, until)
	if err != nil || !found || ddl != "DROP TABLE" || !at.Equal(since) {
		t.Fatalf("FindDestructiveDDL = %q %s %v %v, want the DROP TABLE in the same second as since", ddl, at, found, err)
	}
}
