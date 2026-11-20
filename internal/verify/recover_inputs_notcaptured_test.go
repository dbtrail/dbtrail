package verify

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// A PostgreSQL table below REPLICA IDENTITY FULL stores, for an UPDATE or a
// DELETE, only the key of the earlier row: every other column of row_before is
// the not-captured marker. Such a change cannot be checked (there is no earlier
// row to compare) and cannot be undone. It is counted per table as "not
// checkable", never as a mismatch, and the row's history restarts after it.

func ncMark() map[string]any { return map[string]any{event.NotCapturedKey: true} }

// keyOnly is the earlier row PostgreSQL sends below FULL: the key real, every
// other column the marker.
func keyOnly(id int) map[string]any {
	r := riRow(id, "", 0)
	r["name"], r["qty"] = ncMark(), ncMark()
	return r
}

func TestRecoverChains_notCaptured_isNotAMismatch(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
		riEvent(2, event.EventUpdate, "1", keyOnly(1), riRow(1, "b", 2)),
		// Asserted against the partial change's after-image would pass here;
		// the chain restarts instead, so this change has no predecessor.
		riEvent(3, event.EventUpdate, "1", riRow(1, "zzz", 9), riRow(1, "c", 3)),
		// A second row, fully captured, proves the table is otherwise checked.
		riEvent(4, event.EventInsert, "2", nil, riRow(2, "x", 1)),
		riEvent(5, event.EventUpdate, "2", riRow(2, "x", 1), riRow(2, "y", 1)),
	}))
	t.Logf("detail as shown: %s", out.Detail)
	if out.Status != StatusMatch {
		t.Fatalf("status = %s, want match (a partial change is not a mismatch): %s", out.Status, out.Detail)
	}
	if out.NotCaptured != 1 {
		t.Errorf("NotCaptured = %d, want 1", out.NotCaptured)
	}
	if out.Assertions != 1 {
		t.Errorf("Assertions = %d, want 1 (only row 2's UPDATE)", out.Assertions)
	}
	if out.ChainsNoPredecessor != 1 {
		t.Errorf("ChainsNoPredecessor = %d, want 1 (the chain restarted after the partial change)", out.ChainsNoPredecessor)
	}
	for _, want := range []string{
		"1 change(s) could not be checked: the earlier row was not captured",
		"not at REPLICA IDENTITY FULL",
		"recover cannot undo them either",
		"first: event 2 (pk=1)",
		`ALTER TABLE "shop"."orders" REPLICA IDENTITY FULL`,
	} {
		if !strings.Contains(out.Detail, want) {
			t.Errorf("detail missing %q:\n%s", want, out.Detail)
		}
	}
	if strings.Contains(out.Detail, "cannot undo a change") {
		t.Errorf("verify checks; the undo refusal's own sentence is recover's, not this surface's:\n%s", out.Detail)
	}
}

func TestRecoverChains_notCaptured_deleteThenReinsert(t *testing.T) {
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
		riEvent(2, event.EventDelete, "1", keyOnly(1), nil),
		riEvent(3, event.EventInsert, "1", nil, riRow(1, "b", 1)),
		riEvent(4, event.EventUpdate, "1", riRow(1, "b", 1), riRow(1, "c", 1)),
	}))
	if out.Status != StatusMatch || out.NotCaptured != 1 || out.Assertions != 1 {
		t.Fatalf("status=%s NotCaptured=%d Assertions=%d: %s", out.Status, out.NotCaptured, out.Assertions, out.Detail)
	}
}

// A row whose only later change is partial is NOT append-only: calling it "every
// one a new row" would hide that its changes cannot be undone.
func TestRecoverChains_notCaptured_onlyPartialChangesIsUnproven(t *testing.T) {
	cases := map[string][]query.ResultRow{
		"one partial UPDATE per row": {
			riEvent(1, event.EventUpdate, "1", keyOnly(1), riRow(1, "b", 2)),
			riEvent(2, event.EventDelete, "2", keyOnly(2), nil),
		},
		"insert then partial update": {
			riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
			riEvent(2, event.EventUpdate, "1", keyOnly(1), riRow(1, "b", 2)),
		},
	}
	for name, evs := range cases {
		t.Run(name, func(t *testing.T) {
			out := checkRecoverChains(riInput(evs))
			t.Logf("detail as shown: %s", out.Detail)
			if out.Status != StatusInconclusive || out.InconclusiveKind != InconclusiveUnproven {
				t.Fatalf("status=%s kind=%q, want inconclusive/unproven: %s", out.Status, out.InconclusiveKind, out.Detail)
			}
			if strings.Contains(out.Detail, "every one a new row") {
				t.Errorf("a partial change is not an append-only table:\n%s", out.Detail)
			}
			if !strings.Contains(out.Detail, "could not be checked: the earlier row was not captured") {
				t.Errorf("detail must say why:\n%s", out.Detail)
			}
		})
	}
}

// The partial UPDATE's new row may hold an unchanged large value PostgreSQL did
// not resend: still "not checkable", not the unchanged-TOAST mismatch.
func TestRecoverChains_notCaptured_withUnchangedLargeValue(t *testing.T) {
	after := riRow(1, "b", 2)
	after["name"] = map[string]any{event.UnchangedToastKey: true}
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
		riEvent(2, event.EventUpdate, "1", keyOnly(1), after),
	}))
	if out.Status == StatusMismatch || out.NotCaptured != 1 {
		t.Fatalf("status=%s NotCaptured=%d: %s", out.Status, out.NotCaptured, out.Detail)
	}
}

// Markers where the capture never puts them stay loud mismatches: in a new row
// (INSERT or UPDATE row_after), and the unchanged-TOAST marker in a before-image
// keeps its exact #592 text.
func TestRecoverChains_markersElsewhereStayMismatches(t *testing.T) {
	ins := riRow(1, "a", 1)
	ins["name"] = ncMark()
	out := checkRecoverChains(riInput([]query.ResultRow{riEvent(1, event.EventInsert, "1", nil, ins)}))
	if out.Status != StatusMismatch || out.NotCaptured != 0 {
		t.Fatalf("not-captured in an INSERT: status=%s NotCaptured=%d", out.Status, out.NotCaptured)
	}

	before := riRow(1, "a", 1)
	before["name"] = map[string]any{event.UnchangedToastKey: true}
	out = checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, riRow(1, "a", 1)),
		riEvent(2, event.EventUpdate, "1", before, riRow(1, "b", 1)),
	}))
	want := "event 2 (pk=1): " + event.UnresolvedToastError("shop", "orders", "1", []string{"name"}).Error()
	if out.Status != StatusMismatch || !strings.Contains(out.Detail, want) {
		t.Fatalf("unchanged-TOAST must stay the #592 mismatch\nstatus=%s\ndetail=%s\nwant  %s", out.Status, out.Detail, want)
	}
}

// A MySQL JSON value that only looks like the marker (an extra key) is data and
// is compared as data, exactly as before the marker existed.
func TestRecoverChains_notCaptured_mysqlLookalikeIsData(t *testing.T) {
	look := func() map[string]any { return map[string]any{event.NotCapturedKey: true, "x": "1"} }
	a, b := riRow(1, "a", 1), riRow(1, "a", 1)
	a["name"], b["name"] = look(), look()
	out := checkRecoverChains(riInput([]query.ResultRow{
		riEvent(1, event.EventInsert, "1", nil, a),
		riEvent(2, event.EventUpdate, "1", b, riRow(1, "c", 1)),
	}))
	if out.NotCaptured != 0 || out.Assertions != 1 {
		t.Fatalf("NotCaptured=%d Assertions=%d status=%s: %s", out.NotCaptured, out.Assertions, out.Status, out.Detail)
	}
}
