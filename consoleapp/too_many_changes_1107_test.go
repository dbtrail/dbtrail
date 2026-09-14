package consoleapp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// TestApplyFoldStatus_tooManyChanges (#1107): the page's refresh line keys on
// this flag to stop promising a retry that would be refused again. Set from
// the sentinel, through wrapping, and cleared on every other outcome so a
// previous run's value never lingers.
func TestApplyFoldStatus_tooManyChanges(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"budget refusal, wrapped", fmt.Errorf("shop.a: %w", errors.Join(fmt.Errorf("more than 1,000,000 rows: %w", reconstruct.ErrTouchedRowBudget))), true},
		{"another refusal", fmt.Errorf("shop.a: %w", reconstruct.ErrCaptureGap), false},
		{"success", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &console.BaselineStatus{TooManyChanges: !tc.want}
			applyFoldStatus(st, 1, 0, reuseTally{}, tc.err)
			if st.TooManyChanges != tc.want {
				t.Fatalf("TooManyChanges = %v, want %v", st.TooManyChanges, tc.want)
			}
		})
	}
}
