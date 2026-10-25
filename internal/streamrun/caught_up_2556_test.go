package streamrun

import (
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// #2556: the checkpoint stamps caught_up_at with the index clock only when a
// caught-up marker is pending, and never NULLs a stamp a previous checkpoint
// wrote: the column says when capture was LAST caught up.
func TestSaveCheckpoint_caughtUpAt_2556(t *testing.T) {
	for _, pending := range []bool{true, false} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		args := make([]driver.Value, 0, 14)
		for i := 0; i < 11; i++ {
			args = append(args, sqlmock.AnyArg())
		}
		args = append(args, pending, sqlmock.AnyArg(), sqlmock.AnyArg())
		mock.ExpectExec(`(?s)INSERT INTO stream_state.*capture_skips, caught_up_at, dedup_floor_event_id.*` +
			`IF\(\?, UTC_TIMESTAMP\(6\), NULL\).*` +
			`caught_up_at    = COALESCE\(VALUES\(caught_up_at\), caught_up_at\)`).
			WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
		st := &streamState{mode: "position", binlogFile: "binlog.000003", safeFile: "binlog.000003", safePos: 4, caughtUpPending: pending}
		if err := saveCheckpoint(db, st); err != nil {
			t.Fatalf("pending=%v: saveCheckpoint: %v", pending, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("pending=%v: %v", pending, err)
		}
		db.Close()
	}
}
