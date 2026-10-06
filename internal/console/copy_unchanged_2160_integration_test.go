//go:build integration

package console

import (
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2160: after the source's binlog numbering starts over, a change sorts
// BELOW the snapshot's position. "No event after the position" then says
// nothing about whether the table changed, so the copy must not be vouched for.
// Each case has its own index: the newest event is part of the answer.
func TestIntegrationCopyUnchanged_numberingStartedOver_2160(t *testing.T) {
	markOf := func(r *unchangedRig, table, file string, end uint64) string {
		t.Helper()
		id, err := strconv.ParseUint(r.lastID(table), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return reconstruct.EventMark{ID: id, File: file, End: end}.Encode()
	}
	t.Run("a refresh's snapshot, then a change in binlog.000001", func(t *testing.T) {
		r := newUnchangedRig(t)
		tb := r.table(nil)
		// The old numbering reached the refresh's cut...
		r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-50, r.stamp.Add(-time.Minute))
		r.rewrite(tb, func(map[string]string) {})
		// ...then the numbering started over, and the table changed.
		r.event("shop", tb.Table, "binlog.000001", 500, r.stamp.Add(time.Hour))
		if why := r.ask(tb); why == "" {
			t.Errorf("answer = %q, want a refusal: the table changed after the numbering started over", why)
		}
	})
	t.Run("a refresh's snapshot, then a shorter log_bin base name", func(t *testing.T) {
		r := newUnchangedRig(t)
		tb := r.table(nil)
		r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-50, r.stamp.Add(-time.Minute))
		r.rewrite(tb, func(map[string]string) {})
		r.event("shop", tb.Table, "bin.000001", 500, r.stamp.Add(time.Hour))
		if why := r.ask(tb); why == "" {
			t.Errorf("answer = %q, want a refusal: the table changed after the numbering started over", why)
		}
	})
	t.Run("a full snapshot's event mark, a numbering that started over and grew past it", func(t *testing.T) {
		r := newUnchangedRig(t)
		r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-100, r.stamp.Add(-time.Minute))
		tb := r.table(func(md map[string]string) {
			md[baseline.MetaKeyEventMark] = markOf(r, "elsewhere", r.anchor.File, r.anchor.Pos-50)
		})
		r.event("shop", tb.Table, "binlog.000001", 500, r.stamp.Add(time.Hour))
		// Newest, and past the snapshot's position: only the mark shows it.
		r.event("shop", "elsewhere", "binlog.000009", 100, r.stamp.Add(2*time.Hour))
		if why := r.ask(tb); why == "" {
			t.Errorf("answer = %q, want a refusal: the table changed after the numbering started over", why)
		}
	})
	t.Run("a full snapshot taken in the new numbering is vouched for again", func(t *testing.T) {
		r := newUnchangedRig(t)
		r.event("shop", "elsewhere", r.anchor.File, r.anchor.Pos-50, r.stamp.Add(-time.Minute))
		r.event("shop", "elsewhere", "binlog.000001", 500, r.stamp.Add(-30*time.Second))
		tb := r.table(func(md map[string]string) {
			md[baseline.MetaKeyBinlogFile], md[baseline.MetaKeyBinlogPos] = "binlog.000001", "550"
			md[baseline.MetaKeyEventMark] = markOf(r, "elsewhere", "binlog.000001", 550)
		})
		r.wantUnchanged(tb)
		// And the refresh after it, which anchors at its cut.
		r.rewrite(tb, func(md map[string]string) {
			md[baseline.MetaKeyBinlogFile], md[baseline.MetaKeyBinlogPos] = "binlog.000001", "550"
			md[baseline.MetaKeyEventMark] = markOf(r, "elsewhere", "binlog.000001", 550)
		})
		r.wantUnchanged(tb)
	})
	t.Run("a full snapshot ahead of capture is not a numbering that started over", func(t *testing.T) {
		// The dump's position is where the source stood; capture records the
		// next change of a table it captures, which may be long after.
		r := newUnchangedRig(t)
		r.event("shop", "elsewhere", "binlog.000006", 100, r.stamp.Add(-time.Hour))
		tb := r.table(func(md map[string]string) {
			md[baseline.MetaKeyEventMark] = markOf(r, "elsewhere", "binlog.000006", 150)
		})
		r.wantUnchanged(tb)
	})
}
