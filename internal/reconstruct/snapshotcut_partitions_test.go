package reconstruct

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// The partition bound on the snapshot-cut query (#1692). Without it, on
// MySQL 8.4 the first query walked binlog_events' PRIMARY key upward from the
// oldest row and the plan kept the OLDEST partition whatever the time filter
// said, so every refresh read that whole partition before it could see that
// nothing was past `at`. These tests pin the clause and the fallbacks with
// sqlmock; the real plan is checked in the integration test.

func TestPartitionsAtOrAfter(t *testing.T) {
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	full := []string{"p_2026091614", "p_2026091619", "p_2026091620", "p_2026091621", "p_future"}

	cases := []struct {
		name  string
		names []string
		at    time.Time
		want  []string
	}{
		{"no partitions", nil, at, nil},
		{"only the catch-all", []string{"p_future"}, at, []string{"p_future"}},
		{"keeps at's hour and later, drops the oldest", full, at,
			[]string{"p_2026091620", "p_2026091621", "p_future"}},
		{"at exactly on the hour keeps that hour", full,
			time.Date(2026, 9, 16, 20, 0, 0, 0, time.UTC),
			[]string{"p_2026091620", "p_2026091621", "p_future"}},
		{"at one second before the hour keeps the previous hour", full,
			time.Date(2026, 9, 16, 19, 59, 59, 0, time.UTC),
			[]string{"p_2026091619", "p_2026091620", "p_2026091621", "p_future"}},
		{"at before every partition keeps all of them", full,
			time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC), full},
		{"at past every named partition keeps only the catch-all", full,
			time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC), []string{"p_future"}},
		{"a whole-hour zone is truncated in UTC", full,
			time.Date(2026, 9, 16, 17, 30, 0, 0, time.FixedZone("UTC-3", -3*3600)),
			[]string{"p_2026091620", "p_2026091621", "p_future"}},
		{"a half-hour zone is truncated in UTC too", full,
			// 01:35 at +05:30 is 20:05 UTC; a local-hour truncation would give 20:00+05:30 = 19:30 UTC and keep p_19.
			time.Date(2026, 9, 17, 1, 35, 0, 0, time.FixedZone("UTC+5:30", 5*3600+1800)),
			[]string{"p_2026091620", "p_2026091621", "p_future"}},
		{"a missing catch-all is not invented", []string{"p_2026091619", "p_2026091620"}, at,
			[]string{"p_2026091620"}},
		{"nothing can match and no catch-all", []string{"p_2026091614"}, at, nil},
		{"a gap in the hours keeps the partition that covers at",
			// p_21 holds [20:00, 22:00) when p_20 is missing.
			[]string{"p_2026091619", "p_2026091621", "p_future"}, at,
			[]string{"p_2026091621", "p_future"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := partitionsAtOrAfter(c.names, c.at)
			if err != nil {
				t.Fatalf("partitionsAtOrAfter(%v, %v): %v", c.names, c.at, err)
			}
			if !slices.Equal(got, c.want) {
				t.Errorf("partitionsAtOrAfter(%v, %v) = %v, want %v", c.names, c.at, got, c.want)
			}
		})
	}
}

func TestPartitionsAtOrAfter_unrecognisedNameIsAnErrorNamingIt(t *testing.T) {
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	for _, bad := range []string{"p_custom", "P_2026091620", "p_2026091620 ", "p_20260916", "future"} {
		got, err := partitionsAtOrAfter([]string{"p_2026091620", bad, "p_future"}, at)
		if !errors.Is(err, errUnrecognisedPartition) {
			t.Errorf("%q: err = %v, want errUnrecognisedPartition", bad, err)
		}
		if err != nil && !strings.Contains(err.Error(), bad) {
			t.Errorf("%q: the error should name the partition, got %v", bad, err)
		}
		if got != nil {
			t.Errorf("%q: got %v, want nil alongside the error", bad, got)
		}
	}
}

func TestCutBoundClause(t *testing.T) {
	var none *cutBound
	if got := none.clause(); got != "" {
		t.Errorf("nil bound clause = %q, want empty", got)
	}
	if got := (&cutBound{}).clause(); got != "" {
		t.Errorf("empty keep clause = %q, want empty", got)
	}
	if got, want := (&cutBound{keep: []string{"p_2026091620", "p_future"}}).clause(),
		" PARTITION (p_2026091620, p_future)"; got != want {
		t.Errorf("clause = %q, want %q", got, want)
	}
}

func partitionRows(names ...string) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"PARTITION_NAME"})
	for _, n := range names {
		rows.AddRow(n)
	}
	return rows
}

const listPartitionsRE = `information_schema\.PARTITIONS`

func TestListCutBound(t *testing.T) {
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)

	t.Run("names the partitions that can hold a row past at", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(listPartitionsRE).
			WillReturnRows(partitionRows("p_2026091614", "p_2026091619", "p_2026091620", "p_future"))

		bound, err := listCutBound(context.Background(), db, at)
		if err != nil {
			t.Fatalf("listCutBound: %v", err)
		}
		if want := " PARTITION (p_2026091620, p_future)"; bound.clause() != want {
			t.Errorf("clause = %q, want %q", bound.clause(), want)
		}
	})

	t.Run("an unpartitioned table yields no clause", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows())

		bound, err := listCutBound(context.Background(), db, at)
		if err != nil {
			t.Fatalf("listCutBound: %v", err)
		}
		if bound.clause() != "" {
			t.Errorf("clause = %q, want empty", bound.clause())
		}
	})

	t.Run("a listing error is returned, not swallowed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		boom := errors.New("access denied to information_schema")
		mock.ExpectQuery(listPartitionsRE).WillReturnError(boom)

		if _, err := listCutBound(context.Background(), db, at); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want %v", err, boom)
		}
	})

	t.Run("a row error is returned, not swallowed", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		boom := errors.New("connection reset mid-listing")
		mock.ExpectQuery(listPartitionsRE).
			WillReturnRows(partitionRows("p_2026091620", "p_future").RowError(1, boom))

		if _, err := listCutBound(context.Background(), db, at); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want %v", err, boom)
		}
	})

	t.Run("an unrecognised name is an error naming it", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(listPartitionsRE).
			WillReturnRows(partitionRows("p_2026091620", "p_odd; DROP TABLE x", "p_future"))

		_, err = listCutBound(context.Background(), db, at)
		if !errors.Is(err, errUnrecognisedPartition) || !strings.Contains(err.Error(), "p_odd; DROP TABLE x") {
			t.Fatalf("err = %v, want errUnrecognisedPartition naming the partition", err)
		}
	})
}

func cutRows(file string, pos uint64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"binlog_file", "start_pos"}).AddRow(file, pos)
}

func noCutRows() *sqlmock.Rows { return sqlmock.NewRows([]string{"binlog_file", "start_pos"}) }

// expectNewest expects the newest-event read, which ResolveSnapshotCut runs
// FIRST since #1695, before any partition listing or search. sqlmock matches in
// order, so every test built on it also pins that order.
func expectNewest(mock sqlmock.Sqlmock) { expectNewestAt(mock, "mysql-bin.000203", 99999999) }

func expectNewestAt(mock sqlmock.Sqlmock, file string, pos uint64) {
	mock.ExpectQuery(`ORDER BY event_id DESC LIMIT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "end_pos"}).AddRow(file, pos))
}

// The first query must carry the bound, and the layout must be confirmed
// unchanged afterwards. This is the guard for the bug itself: with the clause
// absent the query is byte-for-byte the pre-#1692 one.
func TestResolveSnapshotCut_firstQueryIsBoundToPartitions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	layout := []string{"p_2026091614", "p_2026091619", "p_2026091620", "p_future"}

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(layout...))
	mock.ExpectQuery(`FROM binlog_events PARTITION \(p_2026091620, p_future\)\s+WHERE TO_SECONDS`).
		WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 37210222))
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(layout...))

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.File != "mysql-bin.000203" || cut.Pos != 37210222 {
		t.Errorf("cut = %+v, want mysql-bin.000203:37210222", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if warns.Len() != 0 {
		t.Errorf("a bounded search on a stable layout must not warn, got: %s", warns.String())
	}
}

// When the partitions cannot be listed, the cut is still resolved, over the
// whole table, exactly as before #1692, and the log says so.
func TestResolveSnapshotCut_listingFailureFallsBackToTheWholeTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)

	expectNewestAt(mock, "mysql-bin.000203", 40000000)
	mock.ExpectQuery(listPartitionsRE).WillReturnError(errors.New("access denied"))
	// No PARTITION clause: the table name is followed directly by WHERE.
	mock.ExpectQuery(`FROM binlog_events\s+WHERE TO_SECONDS`).
		WithArgs(at).
		WillReturnRows(noCutRows())

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.Pos != 40000000 {
		t.Errorf("cut = %+v, want the newest event's end_pos", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if !strings.Contains(warns.String(), "searching the whole table") || !strings.Contains(warns.String(), "access denied") {
		t.Errorf("the fallback must be logged with its cause, got: %s", warns.String())
	}
}

// A partition name this build does not recognise drops the bound and SAYS
// which name did it: the search silently going back to the whole table would
// be the #1692 cost returning with no signal.
func TestResolveSnapshotCut_unknownPartitionNameDisablesTheBoundLoudly(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).
		WillReturnRows(partitionRows("p_2026091620", "p_odd; DROP TABLE x", "p_future"))
	mock.ExpectQuery(`FROM binlog_events\s+WHERE TO_SECONDS`).
		WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 1))

	if _, err := ResolveSnapshotCut(context.Background(), db, at); err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if !strings.Contains(warns.String(), "p_odd; DROP TABLE x") {
		t.Errorf("the warning must name the unrecognised partition, got: %s", warns.String())
	}
}

// Rotation can REORGANIZE p_future between the listing and the bounded
// search, moving rows into partitions the clause did not name. A changed
// listing afterwards means the search may have missed a candidate, so it is
// repeated on the new layout.
func TestResolveSnapshotCut_repeatsTheSearchWhenTheLayoutMoved(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	before := []string{"p_2026091619", "p_2026091620", "p_future"}
	after := []string{"p_2026091619", "p_2026091620", "p_2026091621", "p_future"}

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(before...))
	// The row past at sat in p_future when this ran; the reorganisation moved
	// it into p_2026091621 before the search reached it.
	mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).WillReturnRows(noCutRows())
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(after...))
	mock.ExpectQuery(`PARTITION \(p_2026091620, p_2026091621, p_future\)`).WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 37210222))
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(after...))

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.Pos != 37210222 {
		t.Errorf("cut = %+v, want the row the moved layout exposed", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if warns.Len() != 0 {
		t.Errorf("one repeat is the designed path and must not warn, got: %s", warns.String())
	}
}

// Rotation drops partitions older than at's hour one by one. Those cannot
// hold a row past at, so a listing that lost one is not a moved candidate set
// and the result stands without a second search.
func TestResolveSnapshotCut_anOldPartitionDroppedMidSearchNeedsNoRepeat(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).
		WillReturnRows(partitionRows("p_2026091614", "p_2026091619", "p_2026091620", "p_future"))
	mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 37210222))
	// p_2026091614 was dropped meanwhile; the candidate set is unchanged.
	mock.ExpectQuery(listPartitionsRE).
		WillReturnRows(partitionRows("p_2026091619", "p_2026091620", "p_future"))

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.Pos != 37210222 {
		t.Errorf("cut = %+v, want the bounded search's row", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if warns.Len() != 0 {
		t.Errorf("unexpected warnings: %s", warns.String())
	}
}

// A partition the clause named can be dropped between the listing and the
// search (a restore at a past instant racing rotation): MySQL refuses the
// statement with error 1735. That is a moved layout, not a broken table, so
// the search repeats on the new one instead of failing the whole run.
func TestResolveSnapshotCut_aNamedPartitionDroppedMidSearchRepeats(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 19, 5, 27, 0, time.UTC) // a past instant: p_19 is a candidate

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).
		WillReturnRows(partitionRows("p_2026091619", "p_2026091620", "p_future"))
	mock.ExpectQuery(`PARTITION \(p_2026091619, p_2026091620, p_future\)`).WithArgs(at).
		WillReturnError(&mysql.MySQLError{Number: 1735, Message: "Unknown partition 'p_2026091619'"})
	mock.ExpectQuery(listPartitionsRE).
		WillReturnRows(partitionRows("p_2026091620", "p_future"))
	mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 42))
	mock.ExpectQuery(listPartitionsRE).
		WillReturnRows(partitionRows("p_2026091620", "p_future"))

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.Pos != 42 {
		t.Errorf("cut = %+v, want the repeated search's row", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if warns.Len() != 0 {
		t.Errorf("one repeat is the designed path and must not warn, got: %s", warns.String())
	}
}

// Any other error from the bounded search is returned as is.
func TestResolveSnapshotCut_otherSearchErrorsAreReturned(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	boom := &mysql.MySQLError{Number: 1146, Message: "Table 'x.binlog_events' doesn't exist"}

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows("p_2026091620", "p_future"))
	mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).WillReturnError(boom)

	if _, err := ResolveSnapshotCut(context.Background(), db, at); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// The confirming listing can fail too. A plain error falls back to the
// unbounded search, loudly; a stopping daemon returns as is, with no warning
// and no further statement.
func TestResolveSnapshotCut_confirmingListingFailure(t *testing.T) {
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)

	t.Run("plain error falls back loudly", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		warns := captureWarns(t)

		expectNewest(mock)
		mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows("p_2026091620", "p_future"))
		mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).
			WillReturnRows(cutRows("mysql-bin.000203", 1))
		mock.ExpectQuery(listPartitionsRE).WillReturnError(errors.New("connection reset"))
		mock.ExpectQuery(`FROM binlog_events\s+WHERE TO_SECONDS`).WithArgs(at).
			WillReturnRows(cutRows("mysql-bin.000203", 1))

		cut, err := ResolveSnapshotCut(context.Background(), db, at)
		if err != nil {
			t.Fatalf("ResolveSnapshotCut: %v", err)
		}
		if cut == nil || cut.Pos != 1 {
			t.Errorf("cut = %+v, want the unbounded search's row", cut)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
		if !strings.Contains(warns.String(), "cannot confirm") || !strings.Contains(warns.String(), "connection reset") {
			t.Errorf("the fallback must be logged with its cause, got: %s", warns.String())
		}
	})

	t.Run("cancelled context returns as is", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		warns := captureWarns(t)
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		expectNewest(mock)
		mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows("p_2026091620", "p_future"))
		mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).
			WillReturnRows(cutRows("mysql-bin.000203", 1))
		// The confirming listing outlives the deadline.
		mock.ExpectQuery(listPartitionsRE).WillDelayFor(2 * time.Second).
			WillReturnRows(partitionRows("p_2026091620", "p_future"))

		// sqlmock reports the cancellation with its own error, so only the
		// shape is asserted: an error came back, nothing was logged, and no
		// statement followed.
		if _, err := ResolveSnapshotCut(ctx, db, at); err == nil {
			t.Fatal("expected the deadline to surface as an error, got nil")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
		if warns.Len() != 0 {
			t.Errorf("a cancelled context must not be logged as a fallback, got: %s", warns.String())
		}
	})
}

// A layout that keeps changing is not rotation. After a few attempts the
// search runs unbounded, which is always complete, and the log says so.
func TestResolveSnapshotCut_givesUpOnAShiftingLayoutAndSearchesEverything(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)

	layouts := [][]string{
		{"p_2026091620", "p_future"},
		{"p_2026091620", "p_2026091621", "p_future"},
		{"p_2026091620", "p_2026091621", "p_2026091622", "p_future"},
		{"p_2026091620", "p_2026091621", "p_2026091622", "p_2026091623", "p_future"},
	}
	expectNewest(mock)
	for i := 0; i < maxCutBoundAttempts; i++ {
		mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(layouts[i]...))
		mock.ExpectQuery(`PARTITION \(`).WithArgs(at).WillReturnRows(noCutRows())
	}
	// The confirmation listing after the last bounded attempt differs again.
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(layouts[maxCutBoundAttempts]...))
	mock.ExpectQuery(`FROM binlog_events\s+WHERE TO_SECONDS`).WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 7))

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.Pos != 7 {
		t.Errorf("cut = %+v, want the unbounded search's row", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if !strings.Contains(warns.String(), "could not be confirmed after repeated attempts") {
		t.Errorf("giving up on the bound must be logged, got: %s", warns.String())
	}
}

// A search refused for a partition the listing still shows is an
// inconsistency this code cannot resolve. It must never be read as "nothing
// past at": the empty result of a refused attempt is not a result. After the
// attempts run out the search goes unbounded, loudly.
func TestResolveSnapshotCut_aRefusedSearchIsNeverAcceptedAsEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	layout := []string{"p_2026091620", "p_future"}
	refused := &mysql.MySQLError{Number: 1735, Message: "Unknown partition 'p_2026091620'"}

	expectNewest(mock)
	mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(layout...))
	for i := 0; i < maxCutBoundAttempts; i++ {
		mock.ExpectQuery(`PARTITION \(p_2026091620, p_future\)`).WithArgs(at).WillReturnError(refused)
		mock.ExpectQuery(listPartitionsRE).WillReturnRows(partitionRows(layout...))
	}
	mock.ExpectQuery(`FROM binlog_events\s+WHERE TO_SECONDS`).WithArgs(at).
		WillReturnRows(cutRows("mysql-bin.000203", 9))

	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.Pos != 9 {
		t.Errorf("cut = %+v, want the unbounded search's row, never nil from a refused attempt", cut)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if !strings.Contains(warns.String(), "refused=true") {
		t.Errorf("giving up must be logged and say the search was refused, got: %s", warns.String())
	}
}

// A stopping daemon is not a listing failure: the error goes back as is, no
// "searching the whole table" warning, and no further statement is sent.
func TestResolveSnapshotCut_cancelledContextIsNotAFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	warns := captureWarns(t)
	at := time.Date(2026, 9, 16, 20, 5, 27, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// No expectation on purpose: database/sql refuses a cancelled context
	// before any statement reaches the driver, so the first read (the newest
	// event, #1695) fails with context.Canceled and nothing may follow it.

	if _, err := ResolveSnapshotCut(ctx, db, at); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
	if warns.Len() != 0 {
		t.Errorf("a cancelled context must not be logged as a fallback, got: %s", warns.String())
	}
}

func TestPartitionDateOrFuture(t *testing.T) {
	for _, n := range []string{"p_2026091620", "p_future"} {
		if _, ok := partitionDateOrFuture(n); !ok {
			t.Errorf("%q should be a recognised partition name", n)
		}
	}
	for _, n := range []string{"p_odd", "p_20260916", "P_2026091620", "p_2026091620 ", "future", ""} {
		if _, ok := partitionDateOrFuture(n); ok {
			t.Errorf("%q should NOT be a recognised partition name", n)
		}
	}
}
