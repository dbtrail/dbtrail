package consoleapp

import (
	"database/sql"
	"testing"
	"time"
)

// The snapshot schedule's capture check is not an alarm: an operator's
// acknowledgement of a capture loss says they saw it, not that the lost rows
// came back. Each loss case of TestCaptureComparable, acknowledged, must give
// the verdict it gives unacknowledged.
func TestCaptureComparable_acknowledgedLossChangesNothing(t *testing.T) {
	anchor := time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC)
	fullRead := anchor.Add(-2 * time.Hour)
	const gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50"
	for _, c := range []struct {
		name   string
		lostAt time.Time
		wantOK bool
	}{
		{"a loss after the anchor", anchor.Add(time.Minute), false},
		{"a loss exactly at the anchor", anchor, false},
		// Behind a read of the source: a full read was taken after it.
		{"a loss before the anchor", anchor.Add(-time.Minute), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			plain := streamStateFor(gtid)
			plain.GapLostAt = sql.NullTime{Time: c.lostAt, Valid: true}
			acked := streamStateFor(gtid)
			acked.GapLostAt = plain.GapLostAt
			acked.GapAckedAt = sql.NullTime{Time: c.lostAt.Add(time.Hour), Valid: true}
			if !acked.GapAcknowledged() {
				t.Fatal("fixture: the loss must read as acknowledged")
			}

			okPlain, whyPlain := captureComparable(plain, anchor, fullRead)
			okAcked, whyAcked := captureComparable(acked, anchor, fullRead)
			if okPlain != c.wantOK {
				t.Fatalf("unacknowledged: ok=%v (%s), want %v", okPlain, whyPlain, c.wantOK)
			}
			if okAcked != okPlain || whyAcked != whyPlain {
				t.Errorf("acknowledged: ok=%v (%q), unacknowledged: ok=%v (%q); an acknowledgement must not change the verdict",
					okAcked, whyAcked, okPlain, whyPlain)
			}
			if !okAcked {
				t.Logf("update refused, full read kept: %s", whyAcked)
			}
		})
	}
}
