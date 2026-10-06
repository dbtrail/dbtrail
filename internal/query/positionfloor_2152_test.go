package query

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// #2152: a change indexed late lands in an old partition, and rotation can
// archive and drop that partition before the next snapshot update. The
// partitions no longer show it; the archive's record must.

func archiveAt(label time.Time) archiveHead {
	return archiveHead{name: "p_" + label.Format("2006010215"), lower: label}
}

func recorded(a archiveHead, file string, pos uint64) archiveHead {
	a.recorded, a.hasPos, a.pos = true, true, BinlogPos{File: file, Pos: pos}
	return a
}

func writtenAt(a archiveHead, at time.Time) archiveHead {
	a.archivedAt = at
	return a
}

func TestPartitionHeads_archivesBelow_2152(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(10*time.Hour + 30*time.Minute) // floor: h0+9h
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	cut := &BinlogPos{File: "binlog.000001", Pos: 800} // the previous refresh's cut
	old := since.Add(-48 * time.Hour)                  // written long before the snapshot
	after := since.Add(time.Hour)                      // written after it
	for _, tc := range []struct {
		name     string
		checked  *BinlogPos // nil: no cut known, the time-bounded rule
		archives []archiveHead
		want     time.Time // zero: the start does not move
	}{
		{"no archives", cut, nil, time.Time{}},

		// The previous refresh's cut is known (#2152, option b).
		{"the issue's case: the archive's newest change is after the cut", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 900), after)},
			h0.Add(2 * time.Hour)},
		{"a newest change exactly at the cut counts", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 800), after)},
			h0.Add(2 * time.Hour)},
		{"a quiet table: after its own anchor but before the cut, the refresh already looked", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 799), after)},
			time.Time{}},
		{"a later binlog file is after the cut whatever its offset", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000002", 4), old)},
			h0.Add(2 * time.Hour)},
		{"with a cut, a recorded position decides whenever the archive was written", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 900), old)},
			h0.Add(2 * time.Hour)},
		{"a cut before the table's own anchor: the anchor is the bound", &BinlogPos{File: "binlog.000001", Pos: 100},
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 300), after)},
			time.Time{}},
		{"recorded with no coordinate: nothing an anchored fetch can return", cut,
			[]archiveHead{writtenAt(func() archiveHead { a := archiveAt(h0.Add(2 * time.Hour)); a.recorded = true; return a }(), after)},
			time.Time{}},
		{"not recorded, written after the snapshot: looked at", cut,
			[]archiveHead{writtenAt(archiveAt(h0.Add(2*time.Hour)), after)},
			h0.Add(2 * time.Hour)},
		{"not recorded, written inside the clock margin before the snapshot: looked at", cut,
			[]archiveHead{writtenAt(archiveAt(h0.Add(2*time.Hour)), since.Add(-archiveWrittenMargin))},
			h0.Add(2 * time.Hour)},
		{"not recorded, written before the margin: cannot hold a change the snapshot missed", cut,
			[]archiveHead{writtenAt(archiveAt(h0.Add(2*time.Hour)), since.Add(-archiveWrittenMargin-time.Second))},
			time.Time{}},
		{"an archive the floor already reaches is not a reason to move", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(9*time.Hour)), "binlog.000001", 900), after)},
			time.Time{}},
		{"the archive just below the floor is", cut,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(8*time.Hour)), "binlog.000001", 900), after)},
			h0.Add(8 * time.Hour)},
		{"content older than the label (a first-partition archive) moves the start to it", cut,
			[]archiveHead{func() archiveHead {
				a := writtenAt(recorded(archiveAt(h0.Add(9*time.Hour)), "binlog.000001", 900), after)
				a.lower = h0.Add(-20 * time.Hour)
				return a
			}()},
			h0.Add(-20 * time.Hour)},
		{"several: the oldest qualifying one, and only qualifying ones", cut,
			[]archiveHead{
				writtenAt(recorded(archiveAt(h0), "binlog.000001", 700), after), // before the cut
				writtenAt(recorded(archiveAt(h0.Add(1*time.Hour)), "binlog.000001", 900), after),
				writtenAt(archiveAt(h0.Add(3*time.Hour)), since.Add(time.Minute)),
				writtenAt(archiveAt(h0.Add(-5*time.Hour)), old), // not recorded, old
			},
			h0.Add(1 * time.Hour)},

		// No cut known (an index `bintrail index` also wrote, a snapshot with
		// no usable cut): the table's own anchor, bounded to archives written
		// after the snapshot's time (option a).
		{"no cut: after the anchor, written after the snapshot", nil,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 600), after)},
			h0.Add(2 * time.Hour)},
		{"no cut: after the anchor, written before the snapshot", nil,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 600), old)},
			time.Time{}},
		{"no cut: before the anchor", nil,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 499), after)},
			time.Time{}},
		{"no cut: a newest change exactly at the anchor counts", nil,
			[]archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 500), after)},
			h0.Add(2 * time.Hour)},
		{"no cut: not recorded, written after the snapshot", nil,
			[]archiveHead{writtenAt(archiveAt(h0.Add(2*time.Hour)), after)},
			h0.Add(2 * time.Hour)},
		{"no cut: not recorded, written before it", nil,
			[]archiveHead{writtenAt(archiveAt(h0.Add(2*time.Hour)), old)},
			time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := headsAt(h0.Add(9*time.Hour), 100, 200)
			h.archives = tc.archives
			got, n := h.archivesBelow(since, anchor, tc.checked)
			if !got.Equal(tc.want) {
				t.Fatalf("archivesBelow = %v (%d archives), want %v", got, n, tc.want)
			}
			if (n > 0) != !tc.want.IsZero() {
				t.Fatalf("archivesBelow counted %d archives for a start of %v", n, got)
			}
		})
	}
}

// The archive record is folded over every row, so it does not depend on the
// premise the live partitions do: files indexed into a stream's index make
// every partition a "maybe", but a recorded archive stays exact.
func TestPartitionHeads_archivesBelow_doNotFollowTheIDPremise_2152(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(10*time.Hour + 30*time.Minute)
	h := headsAt(h0.Add(9*time.Hour), 100)
	h.streamCaptured, h.fileIndexingUnfinished = true, true
	h.archives = []archiveHead{recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 400)}
	if got, _ := h.archivesBelow(since, BinlogPos{File: "binlog.000001", Pos: 500}, &BinlogPos{File: "binlog.000001", Pos: 450}); !got.IsZero() {
		t.Fatalf("archivesBelow = %v; a recorded archive before the anchor must not move the start", got)
	}
}

// sinceFor joins both answers: the oldest of the live partitions' event and
// the archives' hour. An archive alone asks the index nothing.
func TestSinceFor_archives_2152(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(10*time.Hour + 30*time.Minute)
	opts := Options{Schema: "shop", Table: "orders", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 500},
		ArchivesCheckedThrough: &BinlogPos{File: "binlog.000001", Pos: 550}}
	arch := recorded(archiveAt(h0.Add(2*time.Hour)), "binlog.000001", 600)

	t.Run("archive only: no query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		h := headsAt(h0.Add(9*time.Hour), 100, 200) // nothing live after the anchor
		h.archives = []archiveHead{arch}
		got, err := h.SinceFor(context.Background(), db, opts)
		if err != nil || got == nil || !got.Equal(h0.Add(2*time.Hour)) {
			t.Fatalf("SinceFor = %v, err=%v; want the archive's hour %v", got, err, h0.Add(2*time.Hour))
		}
		if merr := mock.ExpectationsWereMet(); merr != nil {
			t.Fatal(merr)
		}
	})
	for _, tc := range []struct {
		name string
		live time.Time
		want time.Time
	}{
		{"the archive is older than the live event", h0.Add(9*time.Hour + 10*time.Minute), h0.Add(2 * time.Hour)},
		{"the live event is older than the archive", h0.Add(1 * time.Hour), h0.Add(1 * time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("PARTITION \\(`p0`\\) WHERE event_timestamp").WillReturnRows(
				sqlmock.NewRows([]string{"event_timestamp"}).AddRow(tc.live))
			h := headsAt(h0.Add(-time.Hour), 700, 200, 300) // p0 is first and holds a late row
			h.archives = []archiveHead{arch}
			got, err := h.SinceFor(context.Background(), db, opts)
			if err != nil || got == nil || !got.Equal(tc.want) {
				t.Fatalf("SinceFor = %v, err=%v; want %v", got, err, tc.want)
			}
		})
	}
	t.Run("the cut reaches the fetch: a quiet table does not move", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		h := headsAt(h0.Add(9*time.Hour), 100, 200)
		h.archives = []archiveHead{writtenAt(arch, since.Add(time.Hour))}
		quiet := opts
		quiet.ArchivesCheckedThrough = &BinlogPos{File: "binlog.000001", Pos: 601}
		got, err := h.SinceFor(context.Background(), db, quiet)
		if err != nil || got != quiet.Since {
			t.Fatalf("SinceFor = %v, err=%v; the refresh before already looked through 601", got, err)
		}
		// The same archive with no cut known: written after the snapshot and
		// after the anchor, so it is read.
		quiet.ArchivesCheckedThrough = nil
		got, err = h.SinceFor(context.Background(), db, quiet)
		if err != nil || got == nil || !got.Equal(h0.Add(2*time.Hour)) {
			t.Fatalf("SinceFor with no cut = %v, err=%v; want the archive's hour", got, err)
		}
		if merr := mock.ExpectationsWereMet(); merr != nil {
			t.Fatal(merr)
		}
	})
	t.Run("the live partitions hold no row of the table", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("PARTITION \\(`p0`\\) WHERE event_timestamp").WillReturnRows(sqlmock.NewRows([]string{"event_timestamp"}))
		h := headsAt(h0.Add(-time.Hour), 700, 200, 300)
		h.archives = []archiveHead{arch}
		got, err := h.SinceFor(context.Background(), db, opts)
		if err != nil || got == nil || !got.Equal(h0.Add(2*time.Hour)) {
			t.Fatalf("SinceFor = %v, err=%v; want the archive's hour", got, err)
		}
	})
}

var (
	partCols2152 = []string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}
	headCols2152 = []string{"part", "binlog_file", "start_pos"}
	archCols2152 = []string{"partition_name", "min_event_ts", "max_event_id", "max_binlog_file", "max_start_pos", "age", "now"}
)

// expectPictureStart is the part of a picture load before archive_state: one
// unpartitioned table whose newest row is before any anchor here.
func expectPictureStart(m sqlmock.Sqlmock) {
	m.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(sqlmock.NewRows(partCols2152).AddRow(nil, nil))
	m.ExpectQuery("FROM binlog_events ORDER BY event_id DESC LIMIT 1").WillReturnRows(
		sqlmock.NewRows(headCols2152).AddRow(0, "binlog.000001", 10))
}

func expectPictureEnd(m sqlmock.Sqlmock) {
	m.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}))
	m.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "n"}).AddRow(nil, 0))
}

// archive_state is read AFTER the partitions: rotation registers an archive
// before it drops the partition, so a partition dropped between the two
// reads is in the second. Read the other way round, it would be in neither.
// sqlmock holds the order.
func TestLoadPartitionHeads_readsArchivesAfterThePartitions_2152(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	min := time.Date(2026, 2, 28, 22, 0, 0, 0, time.UTC)
	expectPictureStart(mock)
	mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows(archCols2152).
		AddRow("p_2026030102", min, 77, "binlog.000003", 900, 600, now). // recorded, content older than its label
		AddRow("p_2026030103", nil, 0, nil, nil, 3600, now).             // recorded, empty
		AddRow("p_2026030104", nil, nil, nil, nil, 7200, now).           // not recorded
		AddRow("not_a_partition", nil, nil, nil, nil, 0, now))           // skipped, as the planner skips it
	expectPictureEnd(mock)
	h, err := LoadPartitionHeads(context.Background(), db)
	if err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	if merr := mock.ExpectationsWereMet(); merr != nil {
		t.Fatal(merr)
	}
	want := []archiveHead{
		{name: "p_2026030102", lower: min, recorded: true, hasPos: true, pos: BinlogPos{File: "binlog.000003", Pos: 900}, archivedAt: now.Add(-10 * time.Minute)},
		{name: "p_2026030103", lower: time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC), recorded: true, archivedAt: now.Add(-time.Hour)},
		{name: "p_2026030104", lower: time.Date(2026, 3, 1, 4, 0, 0, 0, time.UTC), archivedAt: now.Add(-2 * time.Hour)},
	}
	if len(h.archives) != len(want) {
		t.Fatalf("archives = %+v, want %+v", h.archives, want)
	}
	for i := range want {
		if h.archives[i] != want[i] {
			t.Errorf("archive %d = %+v, want %+v", i, h.archives[i], want[i])
		}
	}
}

func TestLoadPartitionHeads_archiveStateShapes_2152(t *testing.T) {
	now := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	t.Run("no archive_state table: no archives", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		expectPictureStart(mock)
		mock.ExpectQuery("FROM archive_state").WillReturnError(&mysql.MySQLError{Number: 1146, Message: "no such table"})
		expectPictureEnd(mock)
		h, err := LoadPartitionHeads(context.Background(), db)
		if err != nil || len(h.archives) != 0 {
			t.Fatalf("LoadPartitionHeads = %+v, err=%v", h, err)
		}
	})
	t.Run("an archive_state not migrated yet: every archive not recorded", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		expectPictureStart(mock)
		mock.ExpectQuery("SELECT partition_name, min_event_ts, max_event_id").WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'max_event_id'"})
		mock.ExpectQuery("SELECT partition_name, TIMESTAMPDIFF").WillReturnRows(
			sqlmock.NewRows([]string{"partition_name", "age", "now"}).AddRow("p_2026030104", 60, now))
		expectPictureEnd(mock)
		h, err := LoadPartitionHeads(context.Background(), db)
		if err != nil {
			t.Fatalf("LoadPartitionHeads: %v", err)
		}
		want := archiveHead{name: "p_2026030104", lower: time.Date(2026, 3, 1, 4, 0, 0, 0, time.UTC), archivedAt: now.Add(-time.Minute)}
		if len(h.archives) != 1 || h.archives[0] != want {
			t.Fatalf("archives = %+v, want [%+v]", h.archives, want)
		}
	})
	t.Run("any other failure refuses", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		expectPictureStart(mock)
		mock.ExpectQuery("FROM archive_state").WillReturnError(&mysql.MySQLError{Number: 1045, Message: "denied"})
		if _, err := LoadPartitionHeads(context.Background(), db); err == nil {
			t.Fatal("LoadPartitionHeads answered without reading the archives")
		}
	})
}
