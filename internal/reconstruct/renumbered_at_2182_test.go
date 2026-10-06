package reconstruct

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/query"
)

// checkNumberingForRead (#2182): the bounded check runs only while the live
// binlog_events holds everything the read reaches, and that must still be
// true when the check returns; otherwise the whole-index check answers.
func TestCheckNumberingForRead_2182(t *testing.T) {
	since := time.Date(2026, 10, 6, 10, 30, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	refused := errors.New("refused")
	oldestQ := `SELECT PARTITION_NAME FROM information_schema.PARTITIONS`
	part := func(name string) *sqlmock.Rows { return sqlmock.NewRows([]string{"n"}).AddRow(name) }
	none := func() *sqlmock.Rows { return sqlmock.NewRows([]string{"n"}) }

	for _, tc := range []struct {
		name string
		// oldest is what each read of the oldest partition answers, in order.
		oldest []*sqlmock.Rows
		// results is what each run of the check returns, in order.
		results     []error
		wantWindows []bool // bounded, per run of the check
		wantBounded bool
		wantErr     error
	}{
		{"oldest partition unchanged: bounded", []*sqlmock.Rows{part("p_2026100609"), part("p_2026100609")}, []error{nil}, []bool{true}, true, nil},
		{"floor at the oldest partition's start: bounded", []*sqlmock.Rows{part("p_2026100610"), part("p_2026100610")}, []error{nil}, []bool{true}, true, nil},
		{"rotation dropped the floor's partition during the check: whole index", []*sqlmock.Rows{part("p_2026100609"), part("p_2026100611")}, []error{nil, nil}, []bool{true, false}, false, nil},
		{"dropped during the check, and the whole index refuses", []*sqlmock.Rows{part("p_2026100609"), part("p_2026100611")}, []error{nil, refused}, []bool{true, false}, false, refused},
		{"bounded check refuses: no second look", []*sqlmock.Rows{part("p_2026100609")}, []error{refused}, []bool{true}, true, refused},
		{"no partition row (no table, or unpartitioned): whole index", []*sqlmock.Rows{none()}, []error{nil}, []bool{false}, false, nil},
		{"only p_future: bounded", []*sqlmock.Rows{part("p_future"), part("p_future")}, []error{nil}, []bool{true}, true, nil},
		{"a name this build cannot place: whole index", []*sqlmock.Rows{part("p_weird")}, []error{nil}, []bool{false}, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, rows := range tc.oldest {
				mock.ExpectQuery(oldestQ).WithArgs("idx").WillReturnRows(rows)
			}
			var got []bool
			check := func(w ReadWindow) error {
				got = append(got, w.bounded())
				if w.bounded() && (w.Since != since || w.Until != until || w.Schema != "s" || w.Table != "t") {
					t.Errorf("bounded window %+v, want s.t from %s to %s", w, since, until)
				}
				if len(got) > len(tc.results) {
					t.Fatalf("check ran %d times, want %d", len(got), len(tc.results))
				}
				return tc.results[len(got)-1]
			}
			s := since
			// The floor without SincePos is Since itself: 10:30.
			bounded, err := checkNumberingForRead(context.Background(), db, "idx", query.Options{Schema: "s", Table: "t", Since: &s}, until, check)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if bounded != tc.wantBounded {
				t.Errorf("bounded = %v, want %v", bounded, tc.wantBounded)
			}
			if len(got) != len(tc.wantWindows) {
				t.Fatalf("check ran with windows %v, want %v", got, tc.wantWindows)
			}
			for i := range got {
				if got[i] != tc.wantWindows[i] {
					t.Fatalf("check ran with windows %v, want %v", got, tc.wantWindows)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

// #2186: the bounded check reads archived hours too, so a read whose reach
// rotation already moved out of the live index is checked bounded as long as
// archive_state records an archive for every rotated hour it reaches; an hour
// rotated without one leaves nothing to check, and the whole-index check
// answers.
func TestCheckNumberingForRead_archivedReach_2186(t *testing.T) {
	since := time.Date(2026, 10, 6, 8, 30, 0, 0, time.UTC) // floor 08:30; hours 08 and 09 rotated
	until := since.Add(4 * time.Hour)
	oldestQ := `SELECT PARTITION_NAME FROM information_schema.PARTITIONS`
	coverQ := `SELECT COUNT\(DISTINCT partition_name\) FROM archive_state WHERE partition_name >= \? AND partition_name < \?`
	part := func(name string) *sqlmock.Rows { return sqlmock.NewRows([]string{"n"}).AddRow(name) }
	for _, tc := range []struct {
		name    string
		cover   func(sqlmock.Sqlmock)
		bounded bool
	}{
		{"every rotated hour archived: bounded", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(coverQ).WithArgs("p_2026100608", "p_2026100610").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(2))
		}, true},
		{"one rotated hour without an archive: whole index", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(coverQ).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))
		}, false},
		{"no archive_state: whole index", func(m sqlmock.Sqlmock) {
			m.ExpectQuery(coverQ).WillReturnError(&mysqldriver.MySQLError{Number: 1146})
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			mock.ExpectQuery(oldestQ).WithArgs("idx").WillReturnRows(part("p_2026100610"))
			tc.cover(mock)
			if tc.bounded {
				mock.ExpectQuery(oldestQ).WithArgs("idx").WillReturnRows(part("p_2026100610"))
			}
			var windows []bool
			check := func(w ReadWindow) error { windows = append(windows, w.bounded()); return nil }
			s := since
			bounded, err := checkNumberingForRead(context.Background(), db, "idx", query.Options{Schema: "s", Table: "t", Since: &s}, until, check)
			if err != nil || bounded != tc.bounded || len(windows) != 1 || windows[0] != tc.bounded {
				t.Fatalf("= %v, %v (windows %v); want bounded %v", bounded, err, windows, tc.bounded)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Another failure reading archive_state is an error.
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery(oldestQ).WithArgs("idx").WillReturnRows(part("p_2026100610"))
	mock.ExpectQuery(coverQ).WillReturnError(errors.New("gone away"))
	s := since
	if _, err := checkNumberingForRead(context.Background(), db, "idx", query.Options{Schema: "s", Table: "t", Since: &s}, until, func(ReadWindow) error { return nil }); err == nil {
		t.Fatal("no error")
	}
}
