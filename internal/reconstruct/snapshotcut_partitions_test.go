package reconstruct

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// The partition bound on the snapshot-cut query (#1692). Without it, the
// first query walks binlog_events' PRIMARY key upward from the oldest row,
// and MySQL keeps the OLDEST partition in the plan whatever the time filter
// says, so every refresh read that whole partition before it could see that
// nothing was past `at`.

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
	if got := (&cutBound{names: []string{"p_2026091614"}}).clause(); got != "" {
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
		if want := []string{"p_2026091614", "p_2026091619", "p_2026091620", "p_future"}; !slices.Equal(bound.names, want) {
			t.Errorf("names = %v, want the full listing %v", bound.names, want)
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

	mock.ExpectQuery(listPartitionsRE).WillReturnError(errors.New("access denied"))
	// No PARTITION clause: the table name is followed directly by WHERE.
	mock.ExpectQuery(`FROM binlog_events\s+WHERE TO_SECONDS`).
		WithArgs(at).
		WillReturnRows(noCutRows())
	mock.ExpectQuery(`ORDER BY event_id DESC LIMIT 1`).
		WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "end_pos"}).
			AddRow("mysql-bin.000203", uint64(40000000)))

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
	if !strings.Contains(warns.String(), "kept changing") {
		t.Errorf("giving up on the bound must be logged, got: %s", warns.String())
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
	// before any statement reaches the driver, so the listing fails with
	// context.Canceled and nothing may follow it.

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
