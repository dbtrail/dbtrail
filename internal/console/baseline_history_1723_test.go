package console

import (
	"path/filepath"
	"testing"
)

// AppendCompact (#1723): a repeating compaction failure is one record whose
// end moves; a success, or another error, is a new record.
func TestAppendCompact(t *testing.T) {
	h, err := OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	rec := func(finished, errText string) BaselineRunRecord {
		return BaselineRunRecord{ServerID: "s", StartedAt: "2026-09-18T08:00:00Z", FinishedAt: finished, Error: errText, Refused: 1}
	}
	if added, err := h.AppendCompact(rec("2026-09-18T08:00:05Z", "out of memory")); err != nil || !added {
		t.Fatalf("first: added=%v err=%v", added, err)
	}
	if added, err := h.AppendCompact(rec("2026-09-18T08:05:05Z", "out of memory")); err != nil || added {
		t.Fatalf("same failure again: added=%v err=%v, want the record's end moved", added, err)
	}
	if runs := h.List("s"); len(runs) != 1 || runs[0].FinishedAt != "2026-09-18T08:05:05Z" || runs[0].StartedAt != "2026-09-18T08:00:00Z" || runs[0].Kind != BaselineRunCompact {
		t.Fatalf("runs = %+v", runs)
	}
	if added, _ := h.AppendCompact(rec("2026-09-18T08:10:05Z", "disk full")); !added {
		t.Fatal("another error was folded into the earlier one")
	}
	ok := rec("2026-09-18T08:15:05Z", "")
	ok.Refused, ok.Tables = 0, 1
	if added, _ := h.AppendCompact(ok); !added {
		t.Fatal("a success was folded into a failure")
	}
	if added, _ := h.AppendCompact(ok); !added {
		t.Fatal("two successes were folded: only a failure repeats")
	}
	if runs := h.List("s"); len(runs) != 4 {
		t.Fatalf("runs = %+v, want 4", runs)
	}
	// Reopened: the coalesced record persisted with its moved end.
	h2, err := OpenBaselineHistory(filepath.Join(filepath.Dir(h.path), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	if runs := h2.List("s"); len(runs) != 4 || runs[0].FinishedAt != "2026-09-18T08:05:05Z" {
		t.Fatalf("reopened runs = %+v", runs)
	}
}
