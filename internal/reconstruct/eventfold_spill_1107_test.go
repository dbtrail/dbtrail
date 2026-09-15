package reconstruct

import (
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

func pageOf(from, to int, v string) map[string]*query.ResultRow {
	m := map[string]*query.ResultRow{}
	for id := from; id <= to; id++ {
		pk := strconv.Itoa(id)
		m[pk] = &query.ResultRow{EventType: event.EventUpdate, PKValues: pk, RowAfter: map[string]any{"v": v}}
	}
	return m
}

// admitPage keeps the changes in memory up to the limit, moves them to disk
// past it when the fold may, and refuses past it when it may not.
func TestFoldResult_admitPage(t *testing.T) {
	t.Run("no limit never spills", func(t *testing.T) {
		r := &foldResult{Changes: pageOf(1, 500, "a")}
		if err := r.admitPage(0, 1, true); err != nil || r.Spill != nil || len(r.Changes) != 500 {
			t.Fatalf("err=%v spill=%v changes=%d", err, r.Spill != nil, len(r.Changes))
		}
	})
	t.Run("at the limit stays in memory", func(t *testing.T) {
		r := &foldResult{Changes: pageOf(1, 10, "a")}
		if err := r.admitPage(10, 1, true); err != nil || r.Spill != nil || len(r.Changes) != 10 {
			t.Fatalf("err=%v spill=%v changes=%d", err, r.Spill != nil, len(r.Changes))
		}
	})
	t.Run("past the limit without a spill refuses", func(t *testing.T) {
		r := &foldResult{Changes: pageOf(1, 11, "a")}
		err := r.admitPage(10, 2, false)
		if !errors.Is(err, ErrTouchedRowBudget) || r.Spill != nil {
			t.Fatalf("err=%v spill=%v", err, r.Spill != nil)
		}
		if !strings.Contains(err.Error(), "more than 10 distinct rows") || !strings.Contains(err.Error(), "2 tables") {
			t.Errorf("refusal text: %v", err)
		}
	})
	t.Run("past the limit moves every change to disk, and later pages follow", func(t *testing.T) {
		r := &foldResult{Changes: pageOf(1, 11, "a")}
		before := reflect.ValueOf(r.Changes).UnsafePointer()
		if err := r.admitPage(10, 1, true); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.close)
		if r.Spill == nil || len(r.Changes) != 0 {
			t.Fatalf("spill=%v changes=%d", r.Spill != nil, len(r.Changes))
		}
		if reflect.ValueOf(r.Changes).UnsafePointer() == before {
			t.Error("the map was cleared in place, which keeps its memory; it must be replaced")
		}
		// The next page overwrites rows 5..14 and adds none: 10 rows, at the
		// limit, so it reaches disk only because the fold is already spilling.
		r.Changes = pageOf(5, 14, "b")
		if err := r.admitPage(10, 1, true); err != nil || len(r.Changes) != 0 {
			t.Fatalf("err=%v changes=%d", err, len(r.Changes))
		}
		if err := r.Spill.finish(); err != nil {
			t.Fatal(err)
		}
		if got := r.changeCount(); got == 0 {
			t.Error("a spilled fold reports no changes, so its table would be carried forward unchanged")
		}
		all := loadAll(t, r.Spill)
		if len(all) != 14 {
			t.Fatalf("read back %d rows, want 14", len(all))
		}
		for id := 1; id <= 14; id++ {
			want := "a"
			if id >= 5 {
				want = "b"
			}
			if got := all[strconv.Itoa(id)].RowAfter["v"]; got != want {
				t.Errorf("row %d = %v, want %v", id, got, want)
			}
		}
		dir := r.Spill.dir
		r.close()
		if r.Spill != nil {
			t.Error("close left the spill set")
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Error("close left the spill directory on disk")
		}
	})
}
