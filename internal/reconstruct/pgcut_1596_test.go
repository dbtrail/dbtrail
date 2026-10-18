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

func withFirstEventPast(t *testing.T, cut *query.BinlogPos) {
	t.Helper()
	old := pgFirstEventPast
	pgFirstEventPast = func(context.Context, *sql.DB, time.Time, uint64) (*query.BinlogPos, uint64, error) {
		return cut, 7, nil
	}
	t.Cleanup(func() { pgFirstEventPast = old })
}

// Where a PostgreSQL fold stops and the next one starts (#1596). Every row of
// a transaction carries its commit LSN in both start_pos and end_pos, so the
// fold stops one below the next start, and the two never share a
// transaction.
func TestResolvePGFoldBounds(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	baselineAt := at.Add(-6 * time.Hour)
	const floor = 0x1000
	newest := func(m sqlmock.Sqlmock, end uint64, ts time.Time) {
		m.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(sqlmock.NewRows([]string{"end_pos", "event_timestamp"}).AddRow(end, ts))
	}
	checkpoint := func(m sqlmock.Sqlmock, lsn any) {
		r := sqlmock.NewRows([]string{"binlog_position"})
		if lsn != nil {
			r.AddRow(lsn)
		}
		m.ExpectQuery("FROM stream_state").WillReturnRows(r)
	}
	cases := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		cut       *query.BinlogPos
		wantUntil uint64 // 0 = nil
		wantNext  uint64
		wantEmpty bool
		wantErr   string
	}{
		{name: "empty index", setup: func(m sqlmock.Sqlmock) {
			m.ExpectQuery("ORDER BY event_id DESC LIMIT 1").WillReturnRows(sqlmock.NewRows([]string{"end_pos", "event_timestamp"}))
		}, wantNext: floor, wantEmpty: true},
		{name: "an event after the point: stop just before its LSN",
			setup: func(m sqlmock.Sqlmock) { newest(m, 0x9000, at.Add(time.Hour)) },
			cut:   &query.BinlogPos{File: "0/5000", Pos: 0x5000}, wantUntil: 0x4FFF, wantNext: 0x5000},
		// The 0/... to 1/... boundary: by number, never by text.
		{name: "an event after the point across the high word",
			setup: func(m sqlmock.Sqlmock) { newest(m, 1<<32+0x10, at.Add(time.Hour)) },
			cut:   &query.BinlogPos{File: "1/0", Pos: 1 << 32}, wantUntil: 1<<32 - 1, wantNext: 1 << 32},
		{name: "none after the point, newest transaction checkpointed: take it",
			setup:     func(m sqlmock.Sqlmock) { newest(m, 0x8000, at.Add(-time.Hour)); checkpoint(m, int64(0x8000)) },
			wantUntil: 0x8000, wantNext: 0x8001},
		{name: "none after the point, newest transaction not checkpointed: leave it whole",
			setup:     func(m sqlmock.Sqlmock) { newest(m, 0x8000, at.Add(-time.Hour)); checkpoint(m, int64(0x7000)) },
			wantUntil: 0x7FFF, wantNext: 0x8000},
		{name: "no checkpoint row: leave the newest transaction whole",
			setup:     func(m sqlmock.Sqlmock) { newest(m, 0x8000, at.Add(-time.Hour)); checkpoint(m, nil) },
			wantUntil: 0x7FFF, wantNext: 0x8000},
		// Right after a baseline nothing new has committed: everything in the
		// index is before its floor. Nothing to fold, not an error.
		{name: "capture has nothing past the floor yet",
			setup:    func(m sqlmock.Sqlmock) { newest(m, 0x800, baselineAt.Add(-time.Minute)); checkpoint(m, int64(0x800)) },
			wantNext: floor, wantEmpty: true},
		{name: "a cut below the floor: nothing to fold, the floor stays",
			setup: func(m sqlmock.Sqlmock) { newest(m, 0x9000, at.Add(time.Hour)) },
			cut:   &query.BinlogPos{File: "0/800", Pos: 0x800}, wantNext: floor, wantEmpty: true},
		{name: "a cut at the floor: nothing to fold",
			setup: func(m sqlmock.Sqlmock) { newest(m, 0x9000, at.Add(time.Hour)) },
			cut:   &query.BinlogPos{File: "0/1000", Pos: floor}, wantNext: floor, wantEmpty: true},
		{name: "a cut just past the floor folds the floor's transaction",
			setup: func(m sqlmock.Sqlmock) { newest(m, 0x9000, at.Add(time.Hour)) },
			cut:   &query.BinlogPos{File: "0/1001", Pos: floor + 1}, wantUntil: floor, wantNext: floor + 1},
		{name: "a cut with LSN 0", setup: func(m sqlmock.Sqlmock) { newest(m, 0x9000, at.Add(time.Hour)) },
			cut: &query.BinlogPos{File: "0/0", Pos: 0}, wantErr: "damaged"},
		{name: "a newest change with LSN 0", setup: func(m sqlmock.Sqlmock) { newest(m, 0, at.Add(time.Hour)) },
			wantErr: "damaged"},
		// The other cluster's LSNs passed the floor but not the old ones.
		{name: "an LSN going backwards", setup: func(m sqlmock.Sqlmock) { newest(m, 0x1200, at.Add(time.Hour)) },
			cut: &query.BinlogPos{File: "0/5000", Pos: 0x5000}, wantErr: "another PostgreSQL cluster"},
		// A change committed after the baseline with an LSN below its floor:
		// another cluster.
		{name: "another cluster", setup: func(m sqlmock.Sqlmock) { newest(m, 0x800, baselineAt.Add(time.Minute)); checkpoint(m, int64(0x800)) },
			wantErr: "another PostgreSQL cluster"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c.setup(mock)
			withFirstEventPast(t, c.cut)
			b, err := resolvePGFoldBounds(context.Background(), db, at, floor, baselineAt, baselineAt)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) || (strings.Contains(c.wantErr, "cluster") && !strings.Contains(err.Error(), "bintrail-pg baseline")) {
					t.Fatalf("err = %v, want %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (c.wantUntil == 0) != (b.Until == nil) || (b.Until != nil && (b.Until.Pos != c.wantUntil || !b.Until.IsLSN())) || b.Next != c.wantNext {
				t.Fatalf("bounds = %+v until %+v, want until %X next %X", b, b.Until, c.wantUntil, c.wantNext)
			}
			if b.Until != nil && b.Until.File != lsnText(b.Until.Pos) {
				t.Errorf("until text %q for %X", b.Until.File, b.Until.Pos)
			}
			if b.empty(floor) != c.wantEmpty {
				t.Errorf("empty = %v, want %v", b.empty(floor), c.wantEmpty)
			}
			// The seam: the next fold starts right after this one stops.
			if b.Until != nil && b.Next != b.Until.Pos+1 {
				t.Errorf("seam: until %X next %X", b.Until.Pos, b.Next)
			}
			if b.Next < floor {
				t.Errorf("next %X below the floor %X", b.Next, floor)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
	// A point before the baseline: refused before any read.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := resolvePGFoldBounds(context.Background(), db, baselineAt.Add(-time.Second), floor, baselineAt, baselineAt); err == nil || !strings.Contains(err.Error(), "before the snapshot") {
		t.Fatalf("err = %v, want the before-the-snapshot refusal", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestLSNText(t *testing.T) {
	for lsn, want := range map[uint64]string{0x16B3748: "0/16B3748", 1 << 32: "1/0", 0x1A2B3C4D5E: "1A/2B3C4D5E"} {
		if got := lsnText(lsn); got != want {
			t.Errorf("lsnText(%X) = %q, want %q", lsn, got, want)
		}
	}
}
