//go:build integration

package console

import (
	"testing"
	"time"
)

// #2160: after the source's binlog numbering starts over, a change sorts
// BELOW the snapshot's position. "No event after the position" then says
// nothing about whether the table changed, so the copy must not be vouched for.
func TestIntegrationCopyUnchanged_numberingStartedOver_2160(t *testing.T) {
	r := newUnchangedRig(t)
	after := r.stamp.Add(time.Hour)

	t.Run("a change in binlog.000001 after a snapshot at binlog.000007", func(t *testing.T) {
		tb := r.table(nil)
		// The old numbering reached the snapshot's position...
		r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-50, r.stamp.Add(-time.Minute))
		// ...then the numbering started over, and the table changed.
		r.event("shop", tb.Table, "binlog.000001", 500, after)
		if why := r.ask(tb); why == "" {
			t.Errorf("answer = %q, want a refusal: the table changed after the numbering started over", why)
		}
	})
	t.Run("a shorter log_bin base name after the snapshot", func(t *testing.T) {
		tb := r.table(nil)
		r.event("shop", tb.Table, "bin.000001", 500, after)
		if why := r.ask(tb); why == "" {
			t.Errorf("answer = %q, want a refusal: the table changed after the numbering started over", why)
		}
	})
}
