package console

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

func newMockTailSource(t *testing.T) (*indexTailSource, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return newIndexTailSource(&Server{}, &bundle{db: db}), mock
}

const (
	backfilledQuery = "SELECT 1 FROM index_state LIMIT 1"
	newestQuery     = `SELECT event_id, binlog_file, start_pos, event_timestamp FROM binlog_events FORCE INDEX (PRIMARY) ORDER BY event_id DESC LIMIT 1`
	holdsQuery      = `SELECT binlog_file, start_pos FROM binlog_events WHERE event_id = ?`
)

// The newest event, read first by the keeper. An index `bintrail index` also
// wrote has none: its ids are not its binary log order.
func TestIndexTailSource_newest(t *testing.T) {
	at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	t.Run("found", func(t *testing.T) {
		x, mock := newMockTailSource(t)
		mock.ExpectQuery(backfilledQuery).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(newestQuery).WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c", "d"}).AddRow(42, "binlog.000003", 900, at))
		m, err := x.Newest(context.Background())
		if err != nil || m.ID != 42 || m.Pos != (query.BinlogPos{File: "binlog.000003", Pos: 900}) || !m.At.Equal(at) {
			t.Fatalf("%+v %v", m, err)
		}
	})
	t.Run("an index bintrail index also wrote", func(t *testing.T) {
		x, mock := newMockTailSource(t)
		mock.ExpectQuery(backfilledQuery).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		if m, err := x.Newest(context.Background()); err != nil || m.ID != 0 {
			t.Fatalf("%+v %v", m, err)
		}
	})
	t.Run("an empty index", func(t *testing.T) {
		x, mock := newMockTailSource(t)
		mock.ExpectQuery(backfilledQuery).WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery(newestQuery).WillReturnError(sql.ErrNoRows)
		if m, err := x.Newest(context.Background()); err != nil || m.ID != 0 {
			t.Fatalf("%+v %v", m, err)
		}
	})
}

// The mark's event, read back: there, where it was; or gone, or another
// event under its id (a rebuilt index).
func TestIndexTailSource_holds(t *testing.T) {
	m := tailMark{ID: 42, Pos: query.BinlogPos{File: "binlog.000003", Pos: 900}}
	for _, c := range []struct {
		name string
		rows *sqlmock.Rows
		err  error
		want bool
	}{
		{"there", sqlmock.NewRows([]string{"a", "b"}).AddRow("binlog.000003", 900), nil, true},
		{"gone", nil, sql.ErrNoRows, false},
		{"another event", sqlmock.NewRows([]string{"a", "b"}).AddRow("binlog.000003", 901), nil, false},
		{"another file", sqlmock.NewRows([]string{"a", "b"}).AddRow("binlog.000004", 900), nil, false},
		{"no position", sqlmock.NewRows([]string{"a", "b"}).AddRow(nil, nil), nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			x, mock := newMockTailSource(t)
			q := mock.ExpectQuery(holdsQuery).WithArgs(42)
			if c.err != nil {
				q.WillReturnError(c.err)
			} else {
				q.WillReturnRows(c.rows)
			}
			if got, err := x.Holds(context.Background(), m); err != nil || got != c.want {
				t.Fatalf("%v %v, want %v", got, err, c.want)
			}
		})
	}
}

// Losses on record refuse a tail; GTID mode is not asked for: a tail
// compares nothing with the source.
func TestTailCaptureLoss(t *testing.T) {
	read := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	ok := func() *status.StreamStateInfo {
		return &status.StreamStateInfo{GapColumnsPresent: true, Mode: "position", CaptureSkips: sql.NullString{String: "{}", Valid: true}}
	}
	if why := tailCaptureLoss(ok(), read); why != "" {
		t.Errorf("a position-mode capture with no loss: %q", why)
	}
	gap := ok()
	gap.GapLostAt = sql.NullTime{Time: read.Add(time.Minute), Valid: true}
	old := ok()
	old.GapLostAt = sql.NullTime{Time: read.Add(-time.Minute), Valid: true}
	legacy := ok()
	legacy.GapColumnsPresent = false
	for name, c := range map[string]struct {
		st   *status.StreamStateInfo
		want bool
	}{"no capture": {nil, true}, "a gap since": {gap, true}, "a gap before": {old, false}, "no record of losses": {legacy, true}} {
		if why := tailCaptureLoss(c.st, read); (why != "") != c.want {
			t.Errorf("%s: %q", name, why)
		}
	}
}
