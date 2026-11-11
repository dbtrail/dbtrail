package verify

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// verify --check recover is not an alarm: a recorded capture loss inside the
// window makes the verdict inconclusive whether or not an operator has
// acknowledged it.
func TestClassifyCaptureGap_acknowledgedLossChangesNothing(t *testing.T) {
	since := riBase
	until := riBase.Add(2 * time.Hour)
	state := func(at time.Time, acked bool) *status.StreamStateInfo {
		st := &status.StreamStateInfo{GapColumnsPresent: true,
			GapLostAt:     sql.NullTime{Time: at, Valid: true},
			GapLostDetail: sql.NullString{String: "unfillable binlog gap; auto-advanced", Valid: true}}
		if acked {
			st.GapAckedAt = sql.NullTime{Time: at.Add(time.Minute), Valid: true}
		}
		return st
	}
	for _, c := range []struct {
		name string
		at   time.Time
		want captureGapVerdict
	}{
		{"inside the window", since.Add(time.Hour), captureGapStamped},
		{"before the window", since.Add(-time.Second), captureGapNoneStamped},
		{"after the window", until.Add(time.Second), captureGapNoneStamped},
	} {
		t.Run(c.name, func(t *testing.T) {
			if !state(c.at, true).GapAcknowledged() {
				t.Fatal("fixture: the loss must read as acknowledged")
			}
			plain, whyPlain := classifyCaptureGap(state(c.at, false), since, until)
			acked, whyAcked := classifyCaptureGap(state(c.at, true), since, until)
			if plain != c.want {
				t.Fatalf("unacknowledged = %v, want %v", plain, c.want)
			}
			if acked != plain || whyAcked != whyPlain {
				t.Errorf("acknowledged = %v (%q), unacknowledged = %v (%q); an acknowledgement must not change the verdict",
					acked, whyAcked, plain, whyPlain)
			}
			if whyAcked != "" {
				t.Logf("verify says: %s", whyAcked)
			}
		})
	}
}
