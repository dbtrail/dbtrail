package console

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// Neither check is an alarm. An acknowledged capture loss is still a loss: a
// copy whose rows were last read from the source before it cannot be vouched
// for, acknowledged or not, and one read after it can.
func TestCaptureLoss_acknowledgedLossChangesNothing(t *testing.T) {
	read := time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)
	state := func(lostAt time.Time, acked bool) *status.StreamStateInfo {
		st := &status.StreamStateInfo{Mode: "gtid", GapColumnsPresent: true,
			GTIDSet:      sql.NullString{String: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50", Valid: true},
			CaptureSkips: sql.NullString{String: "{}", Valid: true},
			GapLostAt:    sql.NullTime{Time: lostAt, Valid: true}}
		if acked {
			st.GapAckedAt = sql.NullTime{Time: lostAt.Add(time.Hour), Valid: true}
		}
		return st
	}
	for _, c := range []struct {
		name        string
		lostAt      time.Time
		wantRefused bool
	}{
		{"a loss after the read of the source", read.Add(time.Minute), true},
		{"a loss at the read of the source", read, true},
		{"a loss before the read of the source", read.Add(-time.Second), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if !state(c.lostAt, true).GapAcknowledged() {
				t.Fatal("fixture: the loss must read as acknowledged")
			}
			for name, f := range map[string]func(*status.StreamStateInfo, time.Time) string{
				"tailCaptureLoss": tailCaptureLoss, "captureLossSince": captureLossSince,
			} {
				plain, acked := f(state(c.lostAt, false), read), f(state(c.lostAt, true), read)
				if (plain != "") != c.wantRefused {
					t.Errorf("%s unacknowledged = %q, want refused=%v", name, plain, c.wantRefused)
				}
				if acked != plain {
					t.Errorf("%s: acknowledged = %q, unacknowledged = %q; an acknowledgement must not change the verdict", name, acked, plain)
				}
				if acked != "" {
					t.Logf("%s refuses: %s", name, acked)
				}
			}
		})
	}
}
