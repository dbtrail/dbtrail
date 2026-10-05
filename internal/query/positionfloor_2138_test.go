package query

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// #2138, the rule on its own: which partitions a fetch anchored on a position
// must still reach although its time floor leaves them out.

// headsAt builds hourly partitions p0, p1, ... starting at h0. heads[i] is the
// start position of partition i's newest row; a negative value is an empty
// partition.
func headsAt(h0 time.Time, heads ...int64) *PartitionHeads {
	h := &PartitionHeads{}
	for i, pos := range heads {
		p := partitionHead{
			name:  "p" + string(rune('0'+i)),
			lower: h0.Add(time.Duration(i) * time.Hour),
			open:  i == 0,
		}
		if pos < 0 {
			p.empty = true
		} else {
			p.pos = BinlogPos{File: "binlog.000001", Pos: uint64(pos)}
		}
		h.parts = append(h.parts, p)
	}
	return h
}

// The issue's own statement: the previous snapshot is stamped T, the index
// then receives an event that ran at T-3h, after the previous cut. The window
// must reach the partition that event is in.
func TestPartitionHeads_anEventAfterTheCutThatRanHoursBeforeTheStampIsInside(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	T := h0.Add(10*time.Hour + 30*time.Minute)
	cut := BinlogPos{File: "binlog.000001", Pos: 500}
	// Hours 0..10. The event at T-3h (hour 7) starts at 600, after the cut;
	// every other partition's newest row is before the cut.
	h := headsAt(h0, 100, 110, 120, 130, 140, 150, 160, 600, 480, 490, 500-1)
	if floor := CoarseSinceFloor(T); !h0.Add(7 * time.Hour).Before(floor) {
		t.Fatalf("the fixture must put the event below the floor %s the stamp gives", floor)
	}
	if got := h.below(T, cut); !slices.Equal(got, []string{"p7"}) {
		t.Fatalf("partitions the fetch must reach = %v, want [p7]: the event at T-3h is after the cut", got)
	}
}

func TestPartitionHeads_below(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(5*time.Hour + 30*time.Minute) // floor: h0+4h
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	for _, tc := range []struct {
		name string
		h    *PartitionHeads
		at   time.Time
		want []string
	}{
		{"nothing after the anchor below the floor", headsAt(h0, 100, 200, 300, 400, 900, 950), since, nil},
		{"a newest row exactly at the anchor counts: the gate is start_pos >= anchor", headsAt(h0, 100, 500, 300, 400), since, []string{"p1"}},
		{"one position before the anchor does not", headsAt(h0, 100, 499, 300, 400), since, nil},
		{"the first partition has no lower bound and is below any floor", headsAt(h0, 700), h0.Add(-48 * time.Hour), []string{"p0"}},
		{"empty partitions are skipped", headsAt(h0, -1, 700, -1, 400), since, []string{"p1"}},
		// p3 is [h0+3h, h0+4h): entirely below the floor. p4 starts AT the
		// floor: the fetch reads it anyway.
		{"the partition that ends at the floor is in, the one that starts at it is not", headsAt(h0, 100, 200, 300, 800, 900), since, []string{"p3"}},
		{"a later binlog file is after the anchor whatever its offset", func() *PartitionHeads {
			h := headsAt(h0, 100, 200)
			h.parts[1].pos = BinlogPos{File: "binlog.000002", Pos: 4}
			return h
		}(), since, []string{"p1"}},
		{"a newest row with no coordinate cannot be placed, so its partition is reached", func() *PartitionHeads {
			h := headsAt(h0, 100, 200)
			h.parts[0].unknown = true
			return h
		}(), since, []string{"p0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.h.below(tc.at, anchor); !slices.Equal(got, tc.want) {
				t.Fatalf("below = %v, want %v", got, tc.want)
			}
		})
	}
}

// `bintrail index` adding files to an index a stream writes puts old
// positions on top of a partition. A fetch whose snapshot is older than that
// run cannot trust the newest row, so it reaches every partition below its
// floor that holds anything.
func TestPartitionHeads_filesIndexedSinceTheSnapshotVoidTheShortcut(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(5*time.Hour + 30*time.Minute)
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	mk := func(stream bool, lastFile time.Time) *PartitionHeads {
		h := headsAt(h0, 100, -1, 300, 400, 900)
		h.streamCaptured, h.lastFileIndexed = stream, lastFile
		return h
	}
	all := []string{"p0", "p2", "p3"}
	for _, tc := range []struct {
		name string
		h    *PartitionHeads
		want []string
	}{
		{"a stream alone", mk(true, time.Time{}), nil},
		{"files alone, however recent", mk(false, since.Add(time.Hour)), nil},
		{"stream, files indexed after the snapshot", mk(true, since.Add(time.Minute)), all},
		{"stream, files indexed inside the clock margin before it", mk(true, since.Add(-fileIndexingMargin)), all},
		{"stream, files indexed well before it", mk(true, since.Add(-fileIndexingMargin-time.Second)), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.h.below(since, anchor); !slices.Equal(got, tc.want) {
				t.Fatalf("below = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPartitionClause(t *testing.T) {
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{nil, ""},
		{[]string{""}, ""}, // a table that is not partitioned
		{[]string{"p_2026030100", "p_future"}, " PARTITION (`p_2026030100`, `p_future`)"},
		{[]string{"a`b"}, " PARTITION (`a``b`)"},
	} {
		if got := partitionClause(tc.names); got != tc.want {
			t.Errorf("partitionClause(%q) = %q, want %q", tc.names, got, tc.want)
		}
	}
}

// buildQuery and PartitionHeads must agree on the floor: a partition the
// index bound leaves out and the picture does not look at is the loss again.
func TestCoarseSinceFloor_isTheBoundBuildQueryApplies(t *testing.T) {
	since := time.Date(2026, 3, 1, 14, 37, 12, 0, time.UTC)
	want := time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC)
	if got := CoarseSinceFloor(since); !got.Equal(want) {
		t.Fatalf("CoarseSinceFloor = %s, want %s", got, want)
	}
	_, args := buildQuery(Options{Schema: "s", Table: "t", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 4}})
	found := false
	for _, a := range args {
		if ts, ok := a.(time.Time); ok && ts.Equal(want) {
			found = true
		}
	}
	if !found {
		t.Fatalf("buildQuery did not bind %s as its time floor; args = %v", want, args)
	}
}

// Every way the picture can fail to load, or the oldest-row lookup can fail,
// must stop the fetch. Going on with the caller's own time is the silent loss
// this exists to stop, so AllowGaps does not soften it.
func TestFetchMerged_refusesWhenTheFloorCannotBeSettled(t *testing.T) {
	since := time.Date(2026, 3, 1, 12, 30, 0, 0, time.UTC)
	forced := errors.New("forced")
	partCols := []string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}
	secs := func(h int) string {
		return strconv.FormatInt(mysqlToSeconds(time.Date(2026, 3, 1, h, 0, 0, 0, time.UTC)), 10)
	}
	twoParts := func(m sqlmock.Sqlmock) {
		m.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(
			sqlmock.NewRows(partCols).AddRow("p_2026030100", secs(1)).AddRow("p_future", "MAXVALUE"))
	}
	head := func(m sqlmock.Sqlmock, file string, pos uint64) {
		m.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(
			sqlmock.NewRows([]string{"binlog_file", "start_pos"}).AddRow(file, pos))
	}
	noHead := func(m sqlmock.Sqlmock) {
		m.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "start_pos"}))
	}
	for _, tc := range []struct {
		name  string
		setup func(m sqlmock.Sqlmock)
	}{
		{"the partition list does not read", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("information_schema.PARTITIONS").WillReturnError(forced)
		}},
		{"binlog_events is not there", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(sqlmock.NewRows(partCols))
		}},
		{"a partition bound that is not a number of seconds", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(
				sqlmock.NewRows(partCols).AddRow("p_2026030100", "'2026-03-01'"))
		}},
		{"a partition's newest row does not read", func(m sqlmock.Sqlmock) {
			twoParts(m)
			m.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnError(forced)
		}},
		{"stream_state does not read", func(m sqlmock.Sqlmock) {
			twoParts(m)
			head(m, "binlog.000001", 900)
			noHead(m)
			m.ExpectQuery("FROM stream_state").WillReturnError(forced)
		}},
		{"index_state does not read", func(m sqlmock.Sqlmock) {
			twoParts(m)
			head(m, "binlog.000001", 900)
			noHead(m)
			m.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}))
			m.ExpectQuery("FROM index_state").WillReturnError(forced)
		}},
		{"the oldest event of the table does not read", func(m sqlmock.Sqlmock) {
			twoParts(m)
			head(m, "binlog.000001", 900)
			noHead(m)
			m.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}))
			m.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
			m.ExpectQuery("ORDER BY event_timestamp LIMIT 1").WillReturnError(forced)
		}},
	} {
		for _, allowGaps := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, AllowGaps=%v", tc.name, allowGaps), func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				tc.setup(mock)
				// No expectation for the events query: reaching it is the bug.
				rows, _, err := FetchMerged(context.Background(), db, New(db), FetchMergedOptions{
					Opts:      Options{Schema: "shop", Table: "orders", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 800}},
					DBName:    "idx",
					NoArchive: true,
					AllowGaps: allowGaps,
				})
				if err == nil {
					t.Fatalf("FetchMerged returned %d rows and no error; it must refuse", len(rows))
				}
				if strings.Contains(err.Error(), "was not expected") {
					t.Fatalf("the fetch went on to another query: %v", err)
				}
				if merr := mock.ExpectationsWereMet(); merr != nil {
					t.Fatalf("the fetch stopped before the failing step: %v", merr)
				}
			})
		}
	}
}

// With no index_state table at all (an index no file was ever indexed into)
// the picture loads, and a fetch with nothing late keeps its own time.
func TestLoadPartitionHeads_missingIndexStateIsNoFileIndexing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(
		sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}).AddRow(nil, nil))
	mock.ExpectQuery("FROM binlog_events ORDER BY event_id DESC LIMIT 1").WillReturnRows(
		sqlmock.NewRows([]string{"binlog_file", "start_pos"}).AddRow("", 0))
	mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectQuery("FROM index_state").WillReturnError(&mysql.MySQLError{Number: 1146, Message: "no such table"})
	h, err := LoadPartitionHeads(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	if len(h.parts) != 1 || !h.parts[0].unknown || !h.parts[0].open || !h.streamCaptured || !h.lastFileIndexed.IsZero() {
		t.Fatalf("heads = %+v", h)
	}
}

// Rotation drops an old partition between the picture and the lookup that
// names it. The answer is read again from a fresh picture instead of
// refusing the table (and every table after it in the same run); a failure
// that is still there after the re-reads does refuse.
func TestSinceFor_readsAgainWhenAPartitionWasDropped(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(5*time.Hour + 30*time.Minute)
	opts := Options{Schema: "shop", Table: "orders", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 500}}
	gone := &mysql.MySQLError{Number: 1735, Message: "Unknown partition 'p0' in table 'binlog_events'"}
	partCols := []string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}
	headCols := []string{"binlog_file", "start_pos"}
	// The fresh picture: p0 is gone, p1 is now the first partition and holds
	// the late row.
	fresh := func(m sqlmock.Sqlmock) {
		m.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(sqlmock.NewRows(partCols).
			AddRow("p1", strconv.FormatInt(mysqlToSeconds(h0.Add(2*time.Hour)), 10)).AddRow("p_future", "MAXVALUE"))
		m.ExpectQuery("PARTITION \\(`p1`\\) ORDER BY event_id DESC").WillReturnRows(sqlmock.NewRows(headCols).AddRow("binlog.000001", 700))
		m.ExpectQuery("PARTITION \\(`p_future`\\) ORDER BY event_id DESC").WillReturnRows(sqlmock.NewRows(headCols))
		m.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		m.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
	}
	late := h0.Add(90 * time.Minute)

	t.Run("the second read answers", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("PARTITION \\(`p0`, `p1`\\) WHERE event_timestamp").WillReturnError(gone)
		fresh(mock)
		mock.ExpectQuery("PARTITION \\(`p1`\\) WHERE event_timestamp").WillReturnRows(sqlmock.NewRows([]string{"event_timestamp"}).AddRow(late))
		got, err := headsAt(h0, 700, 700).SinceFor(context.Background(), db, opts)
		if err != nil || !got.Equal(late) {
			t.Fatalf("SinceFor = %v, err=%v; want %s from the fresh picture", got, err, late)
		}
		if merr := mock.ExpectationsWereMet(); merr != nil {
			t.Fatal(merr)
		}
	})
	t.Run("a partition that keeps vanishing refuses", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("PARTITION \\(`p0`, `p1`\\) WHERE event_timestamp").WillReturnError(gone)
		for range partitionReadAttempts - 1 {
			fresh(mock)
			mock.ExpectQuery("PARTITION \\(`p1`\\) WHERE event_timestamp").WillReturnError(gone)
		}
		got, err := headsAt(h0, 700, 700).SinceFor(context.Background(), db, opts)
		if err == nil {
			t.Fatalf("SinceFor = %v and no error; it must refuse", got)
		}
		if merr := mock.ExpectationsWereMet(); merr != nil {
			t.Fatal(merr)
		}
	})
	t.Run("a partition dropped while the picture loads", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(sqlmock.NewRows(partCols).
			AddRow("p0", strconv.FormatInt(mysqlToSeconds(h0.Add(time.Hour)), 10)).AddRow("p_future", "MAXVALUE"))
		mock.ExpectQuery("PARTITION \\(`p0`\\) ORDER BY event_id DESC").WillReturnError(gone)
		fresh(mock)
		h, err := LoadPartitionHeads(context.Background(), db)
		if err != nil || len(h.parts) != 2 || h.parts[0].name != "p1" {
			t.Fatalf("LoadPartitionHeads = %+v, err=%v; want the second listing", h, err)
		}
	})
}
