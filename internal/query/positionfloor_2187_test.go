package query

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// #2187: the routed port's "unchanged since the snapshot" check asks the same
// question of archive_state as a fetch with no cut (archivesBelow), through
// ArchiveHeads. These cases pin that both read the archives by one rule, and
// the one addition #2187 made to it: a newest position on another binlog base
// name cannot be placed against the anchor, so only the time rule applies.

func TestArchiveHeads_mayHoldAfter_2187(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(10*time.Hour + 30*time.Minute)
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	old := since.Add(-48 * time.Hour)
	after := since.Add(time.Hour)
	below := h0.Add(2 * time.Hour) // under the floor (h0+9h)
	for _, tc := range []struct {
		name     string
		archives []archiveHead
		want     int
	}{
		{"no archives", nil, 0},
		{"a late change archived after the snapshot", []archiveHead{writtenAt(recorded(archiveAt(below), "binlog.000001", 900), after)}, 1},
		{"a newest change exactly at the anchor counts", []archiveHead{writtenAt(recorded(archiveAt(below), "binlog.000001", 500), after)}, 1},
		{"routine archive written after the snapshot, every change before the anchor", []archiveHead{writtenAt(recorded(archiveAt(below), "binlog.000001", 499), after)}, 0},
		{"after the anchor but written before the snapshot", []archiveHead{writtenAt(recorded(archiveAt(below), "binlog.000001", 900), old)}, 0},
		{"recorded with no coordinate", []archiveHead{writtenAt(func() archiveHead { a := archiveAt(below); a.recorded = true; return a }(), after)}, 0},
		{"not recorded, written after the snapshot", []archiveHead{writtenAt(archiveAt(below), after)}, 1},
		{"not recorded, written inside the clock margin", []archiveHead{writtenAt(archiveAt(below), since.Add(-archiveWrittenMargin))}, 1},
		{"not recorded, written before the margin", []archiveHead{writtenAt(archiveAt(below), since.Add(-archiveWrittenMargin-time.Second))}, 0},
		{"another base name, written after the snapshot: cannot be placed, counts", []archiveHead{writtenAt(recorded(archiveAt(below), "mysql-bin.000001", 4), after)}, 1},
		{"another base name, written before the snapshot", []archiveHead{writtenAt(recorded(archiveAt(below), "mysql-bin.999999", 4), old)}, 0},
		{"an hour the time floor still reaches is the live index's to answer", []archiveHead{writtenAt(recorded(archiveAt(h0.Add(9*time.Hour)), "binlog.000001", 900), after)}, 0},
		{"several, counted", []archiveHead{
			writtenAt(recorded(archiveAt(h0), "binlog.000001", 700), after),
			writtenAt(recorded(archiveAt(h0.Add(time.Hour)), "binlog.000001", 100), after),
			writtenAt(archiveAt(h0.Add(3*time.Hour)), after),
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (ArchiveHeads{rows: tc.archives}).MayHoldAfter(since, anchor); got != tc.want {
				t.Fatalf("MayHoldAfter = %d, want %d", got, tc.want)
			}
			// The same rule as a fetch with no cut.
			h := headsAt(h0.Add(9*time.Hour), 100, 200)
			h.archives = tc.archives
			if _, n := h.archivesBelow(since, anchor, nil, nil); n != tc.want {
				t.Fatalf("archivesBelow with no cut counted %d, MayHoldAfter's rule says %d", n, tc.want)
			}
		})
	}
}

// With a cut, a position on another base name is not compared with the cut
// either: the time rule decides.
func TestPartitionHeads_archivesBelow_anotherBaseName_2187(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(10*time.Hour + 30*time.Minute)
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	cut := &BinlogPos{File: "binlog.000001", Pos: 800}
	h := headsAt(h0.Add(9*time.Hour), 100, 200)
	// "a.000001" sorts before "binlog.000001" by length: compared, it would
	// read as already seen.
	h.archives = []archiveHead{writtenAt(recorded(archiveAt(h0.Add(2*time.Hour)), "a.000001", 4), since.Add(time.Hour))}
	if got, _ := h.archivesBelow(since, anchor, cut, nil); !got.Equal(h0.Add(2 * time.Hour)) {
		t.Fatalf("archivesBelow = %v; an archive on another base name written after the snapshot must be reached", got)
	}
	h.archives[0].archivedAt = since.Add(-48 * time.Hour)
	if got, _ := h.archivesBelow(since, anchor, cut, nil); !got.IsZero() {
		t.Fatalf("archivesBelow = %v; written long before the snapshot it cannot hold a change it missed", got)
	}
}

func TestLoadArchivesWrittenSince_2187(t *testing.T) {
	after := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	cols := []string{"partition_name", "min_event_ts", "max_event_id", "max_binlog_file", "max_start_pos", "age", "now"}
	now := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)

	t.Run("filters by when the row was written, as an age on the server", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM archive_state\s+WHERE archived_at >= NOW\(\) - INTERVAL \(UNIX_TIMESTAMP\(\) - \?\) SECOND`).
			WithArgs(after.Add(-archiveWrittenMargin).Unix() - archiveWrittenSlack).
			WillReturnRows(sqlmock.NewRows(cols).AddRow("p_2026030102", nil, 9, "binlog.000001", 900, 3600, now))
		heads, present, err := LoadArchivesWrittenSince(context.Background(), db, after)
		if err != nil || !present {
			t.Fatalf("present=%v err=%v", present, err)
		}
		if len(heads.rows) != 1 || !heads.rows[0].hasPos || heads.rows[0].pos.Pos != 900 || !heads.rows[0].archivedAt.Equal(now.Add(-time.Hour)) {
			t.Fatalf("rows = %+v", heads.rows)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("no archive_state is reported, not read as no archives", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM archive_state`).WillReturnError(&mysql.MySQLError{Number: 1146, Message: "Table 'x.archive_state' doesn't exist"})
		_, present, err := LoadArchivesWrittenSince(context.Background(), db, after)
		if err != nil || present {
			t.Fatalf("present=%v err=%v; want absent and no error", present, err)
		}
	})
	t.Run("a table an older build created is read without the record, filtered the same way", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM archive_state`).WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'max_event_id'"})
		mock.ExpectQuery(`SELECT partition_name, TIMESTAMPDIFF.*FROM archive_state\s+WHERE archived_at >= NOW\(\) - INTERVAL \(UNIX_TIMESTAMP\(\) - \?\) SECOND`).
			WithArgs(after.Add(-archiveWrittenMargin).Unix() - archiveWrittenSlack).
			WillReturnRows(sqlmock.NewRows([]string{"partition_name", "age", "now"}).AddRow("p_2026030102", 60, now))
		heads, present, err := LoadArchivesWrittenSince(context.Background(), db, after)
		if err != nil || !present || len(heads.rows) != 1 || heads.rows[0].recorded {
			t.Fatalf("present=%v err=%v rows=%+v", present, err, heads.rows)
		}
	})
	t.Run("any other failure is an error", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM archive_state`).WillReturnError(errors.New("boom"))
		if _, _, err := LoadArchivesWrittenSince(context.Background(), db, after); err == nil {
			t.Fatal("want an error")
		}
	})
}
