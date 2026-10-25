package streamrun

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// agoArg matches the caught_up_at interval: NULL when no marker is pending,
// otherwise microseconds since the marker was read, within a window.
type agoArg struct{ min, max int64 }

func (a agoArg) Match(v driver.Value) bool {
	if a.max == 0 {
		return v == nil
	}
	us, ok := v.(int64)
	return ok && us >= a.min && us <= a.max
}

// #2556: the checkpoint stamps caught_up_at at the marker's READ time (the
// index clock minus how long ago it was read), keeps a stored stamp when no
// marker is pending, and clears it once when asked (--reset, gap advance).
func TestSaveCheckpoint_caughtUpAt_2556(t *testing.T) {
	for _, c := range []struct {
		name   string
		readAt time.Time
		clear  bool
		ago    agoArg
	}{
		{"marker read 5s ago", time.Now().Add(-5 * time.Second), false, agoArg{5e6, 6e6}},
		{"no marker", time.Time{}, false, agoArg{}},
		{"cleared", time.Time{}, true, agoArg{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			args := make([]driver.Value, 0, 15)
			for i := 0; i < 11; i++ {
				args = append(args, sqlmock.AnyArg())
			}
			args = append(args, c.ago, sqlmock.AnyArg(), sqlmock.AnyArg(), c.clear)
			mock.ExpectExec(`(?s)INSERT INTO stream_state.*capture_skips, caught_up_at, dedup_floor_event_id.*` +
				`UTC_TIMESTAMP\(6\) - INTERVAL \? MICROSECOND.*` +
				`caught_up_at    = IF\(\?, NULL, COALESCE\(VALUES\(caught_up_at\), caught_up_at\)\)`).
				WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
			st := &streamState{mode: "position", binlogFile: "binlog.000003", safeFile: "binlog.000003", safePos: 4,
				caughtUpReadAt: c.readAt, clearCaughtUp: c.clear}
			if err := saveCheckpoint(db, st); err != nil {
				t.Fatalf("saveCheckpoint: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
			if st.clearCaughtUp {
				t.Error("clearCaughtUp still set after a successful save: every later checkpoint would wipe the stamp")
			}
		})
	}
}
