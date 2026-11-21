package reconstruct

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/query"
)

// pgFound is one answer of the search for the first event past the point.
type pgFound struct {
	lsn uint64 // 0: none found
	id  uint64
}

// withFirstEventsPast answers each search for the first event past the point
// with the next of found, and returns how many searches were made.
func withFirstEventsPast(t *testing.T, found ...pgFound) *int {
	t.Helper()
	old := pgFirstEventPast
	calls := 0
	pgFirstEventPast = func(context.Context, *sql.DB, time.Time, uint64) (*query.BinlogPos, uint64, error) {
		if calls >= len(found) {
			t.Fatalf("search %d for the first event past the point: only %d expected", calls+1, len(found))
		}
		f := found[calls]
		calls++
		if f.lsn == 0 {
			return nil, 0, nil
		}
		return lsnPos(f.lsn), f.id, nil
	}
	t.Cleanup(func() { pgFirstEventPast = old })
	return &calls
}

// The newest event and the first one past the point are two reads, and
// capture writes between them (#3044): an event written after the newest one
// was read is later in the WAL than it on one and the same cluster. The
// newest event is read again, and only an event written BEFORE the newest
// one with a higher LSN is another cluster.
func TestResolvePGRunEnd_eventWrittenBetweenTheTwoReads(t *testing.T) {
	at := time.Date(2026, 11, 21, 5, 10, 56, 0, time.UTC)
	type newest struct {
		lsn, id uint64
		ts      time.Time
	}
	cases := []struct {
		name      string
		newest    []newest // one per read; lsn 0 = an empty index
		found     []pgFound
		wantUntil uint64
		wantNone  bool
		wantErr   string
		notErr    string
	}{
		// The log of #3044: the newest change read at F/32FC19D0, then the
		// first one past the point at F/3336CE28, written in between.
		{name: "written after the newest read: read again, not another cluster",
			newest:    []newest{{0xF32FC19D0, 500, at}, {0xF3336CE40, 540, at.Add(time.Second)}},
			found:     []pgFound{{0xF3336CE28, 501}, {0xF3336CE28, 501}},
			wantUntil: 0xF3336CE27},
		{name: "the event found is the newest one of the second read",
			newest:    []newest{{0xF32FC19D0, 500, at}, {0xF3336CE28, 501, at.Add(time.Second)}},
			found:     []pgFound{{0xF3336CE28, 501}, {0xF3336CE28, 501}},
			wantUntil: 0xF3336CE27},
		// The second read is a picture of its own: an LSN going backwards in
		// it is refused as before.
		{name: "written after the newest read, and the second read shows another cluster",
			newest:  []newest{{0xF32FC19D0, 500, at}, {0x1200, 540, at.Add(time.Second)}},
			found:   []pgFound{{0xF3336CE28, 501}, {0xF3336CE28, 501}},
			wantErr: "another PostgreSQL cluster"},
		// Written before the newest event, with a higher LSN: one read is
		// enough, and it is another cluster.
		{name: "written before the newest event with a higher LSN: another cluster",
			newest:  []newest{{0x1200, 500, at.Add(time.Hour)}},
			found:   []pgFound{{0x5000, 499}},
			wantErr: "another PostgreSQL cluster"},
		{name: "the same event is both: one read",
			newest:    []newest{{0x5000, 500, at.Add(time.Second)}},
			found:     []pgFound{{0x5000, 500}},
			wantUntil: 0x4FFF},
		// Capture started again between the reads and removed what it had
		// indexed past its saved position: the event found is gone, and the
		// one found next is again newer than the newest read. Not settled
		// in three reads: an error that says so, and never "another cluster".
		{name: "events past the point written after every read",
			newest:  []newest{{0x1000, 500, at}, {0x1000, 500, at}, {0x1000, 500, at}},
			found:   []pgFound{{0x2000, 501}, {0x2000, 601}, {0x2000, 701}},
			wantErr: "could not be read", notErr: "another PostgreSQL cluster"},
		{name: "settled on the third read",
			newest:    []newest{{0x1000, 500, at}, {0x1000, 500, at}, {0x2010, 702, at.Add(time.Second)}},
			found:     []pgFound{{0x2000, 501}, {0x2000, 601}, {0x2000, 701}},
			wantUntil: 0x1FFF},
		{name: "the index emptied between the reads",
			newest:   []newest{{0x1000, 500, at}, {}},
			found:    []pgFound{{0x2000, 501}},
			wantNone: true},
		{name: "the event found is gone at the second read, none past the point",
			newest:    []newest{{0x1000, 500, at}, {0x1000, 500, at}},
			found:     []pgFound{{0x2000, 501}, {}},
			wantUntil: 0xFFF},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			// Read once, before the first read of the newest event, and
			// kept through every read after it (pgRunEnd.caughtUpAt).
			caught := at.Add(-time.Minute)
			mock.ExpectQuery("SELECT pg_caught_up_source_at FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"pg_caught_up_source_at"}).AddRow(caught))
			for _, n := range c.newest {
				rows := sqlmock.NewRows([]string{"end_pos", "event_timestamp", "event_id"})
				if n.lsn != 0 {
					rows.AddRow(n.lsn, n.ts, n.id)
				}
				mock.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(rows)
			}
			if c.wantNone {
				mock.ExpectQuery("FROM archive_state").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
			}
			if c.wantErr == "" && !c.wantNone && c.found[len(c.found)-1].lsn == 0 {
				mock.ExpectQuery("SELECT binlog_position FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"binlog_position"}))
			}
			calls := withFirstEventsPast(t, c.found...)
			run, err := resolvePGRunEnd(context.Background(), db, at)
			if *calls != len(c.found) {
				t.Errorf("%d searches for the first event past the point, want %d", *calls, len(c.found))
			}
			if merr := mock.ExpectationsWereMet(); merr != nil {
				t.Error(merr)
			}
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want %q", err, c.wantErr)
				}
				if c.notErr != "" && strings.Contains(err.Error(), c.notErr) {
					t.Fatalf("err = %v, must not say %q", err, c.notErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if run.none != c.wantNone {
				t.Fatalf("none = %v, want %v", run.none, c.wantNone)
			}
			if !run.caughtUpAt.Equal(caught) {
				t.Errorf("caughtUpAt = %s, want the one read first, %s", run.caughtUpAt, caught)
			}
			if c.wantNone {
				// Nothing of the read before it is left in the run.
				if run.newestEnd != 0 || !run.newestAt.IsZero() || run.until != 0 || !run.allIn {
					t.Errorf("an emptied index: run = %+v, want no newest change, no end, all in", run)
				}
				return
			}
			if run.until != c.wantUntil {
				t.Errorf("until = %X, want %X", run.until, c.wantUntil)
			}
			// What the run keeps is the newest event of its last read: the
			// one the event found was held against.
			last := c.newest[len(c.newest)-1]
			if run.newestEnd != last.lsn || !run.newestAt.Equal(last.ts) {
				t.Errorf("newest kept = %X at %s, want the last read's %X at %s", run.newestEnd, run.newestAt, last.lsn, last.ts)
			}
			if run.allIn {
				t.Error("allIn with a change left for the next fold")
			}
		})
	}
}
