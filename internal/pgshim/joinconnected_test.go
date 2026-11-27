package pgshim

import (
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement the copy ran with its tables in a connected order is counted
// as such, not as written_order or planned.
func TestNoteJoinCheck_connectedOrder(t *testing.T) {
	s := &session{logger: slog.Default()}
	if got := s.noteJoinCheck(sqlsandbox.Result{JoinOrderConnected: "l1, orders"}, nil, true, ""); got != JoinCheckConnectedOrder {
		t.Errorf("counted %q, want %q", got, JoinCheckConnectedOrder)
	}
	if got := s.noteJoinCheck(sqlsandbox.Result{JoinOrderWritten: true}, nil, true, ""); got != JoinCheckWrittenOrder {
		t.Errorf("the order written: counted %q", got)
	}
}

// A slow statement that ran in a connected order did not run in the copy's
// own order: it marks no shape for a trial in the order written, which
// would only refuse it (the order written alone is what the check refused).
func TestMarkIfSlow_notAfterAConnectedOrder(t *testing.T) {
	// No binding: marking would reach for its memory and panic.
	s := &session{logger: slog.Default()}
	s.markIfSlow("SELECT 1", "shape", RunOptions{}, sqlsandbox.Result{JoinOrderConnected: "a, b", Elapsed: time.Hour}, nil)
	if s.writtenOrder != "" {
		t.Errorf("marked: %q", s.writtenOrder)
	}
}
