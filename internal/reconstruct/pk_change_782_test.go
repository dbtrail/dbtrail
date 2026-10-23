package reconstruct

import (
	"context"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// pkColsIntTwo returns a composite PK descriptor (a INT, b INT) for the helper
// test's composite case.
func pkColsIntTwo() []metadata.ColumnMeta {
	return []metadata.ColumnMeta{
		{Name: "a", OrdinalPosition: 1, IsPK: true, DataType: "int"},
		{Name: "b", OrdinalPosition: 2, IsPK: true, DataType: "int"},
	}
}

// TestPKChangingUpdate covers the pure detection helper directly.
func TestPKChangingUpdate(t *testing.T) {
	cases := []struct {
		name       string
		pkCols     []metadata.ColumnMeta
		changes    map[string]*query.ResultRow
		wantFound  bool
		wantBefore string
		wantAfter  string
	}{
		{
			name:    "empty",
			pkCols:  pkColsIntID(),
			changes: map[string]*query.ResultRow{},
		},
		{
			name:   "non-pk-changing update ignored",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(1): {
					EventType: event.EventUpdate,
					RowBefore: map[string]any{"id": float64(1), "status": "new"},
					RowAfter:  map[string]any{"id": float64(1), "status": "paid"},
				},
			},
		},
		{
			name:   "insert ignored (nil before)",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(9): {
					EventType: event.EventInsert,
					RowAfter:  map[string]any{"id": float64(9), "status": "new"},
				},
			},
		},
		{
			name:   "delete ignored (nil after)",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(9): {
					EventType: event.EventDelete,
					RowBefore: map[string]any{"id": float64(9), "status": "gone"},
				},
			},
		},
		{
			name:   "pk-changing update detected",
			pkCols: pkColsIntID(),
			changes: map[string]*query.ResultRow{
				pkStrForInt(1): {
					EventType: event.EventUpdate,
					RowBefore: map[string]any{"id": float64(1), "status": "new"},
					RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
				},
			},
			wantFound:  true,
			wantBefore: "1",
			wantAfter:  "2",
		},
		{
			name:   "composite pk partial change detected",
			pkCols: pkColsIntTwo(),
			changes: map[string]*query.ResultRow{
				"1|5": {
					EventType: event.EventUpdate,
					RowBefore: map[string]any{"a": float64(1), "b": float64(5)},
					RowAfter:  map[string]any{"a": float64(1), "b": float64(6)},
				},
			},
			wantFound:  true,
			wantBefore: "1|5",
			wantAfter:  "1|6",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, a, found := pkChangingUpdate(tc.changes, tc.pkCols)
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v (before=%q after=%q)", found, tc.wantFound, b, a)
			}
			if found && (b != tc.wantBefore || a != tc.wantAfter) {
				t.Errorf("before/after = %q/%q, want %q/%q", b, a, tc.wantBefore, tc.wantAfter)
			}
		})
	}
}

// TestFoldPage_pkChangingUpdate_scenarioA is #782 scenario A (resurrection):
// `UPDATE pk 1→2; DELETE pk=2`. Folded under the before-image key alone, the
// baseline row pk=1 would match the UPDATE and emit pk=2 — a row the DELETE
// actually removed. Since #2501 the UPDATE is folded as DELETE 1 + INSERT 2 at
// its own position, and the later DELETE wins key 2: both keys end dead.
func TestFoldPage_pkChangingUpdate_scenarioA(t *testing.T) {
	assertPKChangeState(t, map[int]string{1: "", 2: ""}, []query.ResultRow{
		{
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(1),
			RowBefore: map[string]any{"id": float64(1), "status": "new"},
			RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
		},
		{
			EventType: event.EventDelete,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "moved"},
		},
	})
}

// TestFoldPage_pkChangingUpdate_scenarioB is #782 scenario B (duplication):
// `UPDATE pk 1→2; UPDATE pk=2`. Folded under the before-image key alone, pk=2
// would be emitted twice (the first UPDATE's after-image under key 1, the
// second UPDATE under key 2), a 1062 that only surfaces at restore time. Split,
// key 2 keeps the second UPDATE's image and key 1 is dead.
func TestFoldPage_pkChangingUpdate_scenarioB(t *testing.T) {
	assertPKChangeState(t, map[int]string{1: "", 2: "final"}, []query.ResultRow{
		{
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(1),
			RowBefore: map[string]any{"id": float64(1), "status": "new"},
			RowAfter:  map[string]any{"id": float64(2), "status": "renamed"},
		},
		{
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(2),
			RowBefore: map[string]any{"id": float64(2), "status": "renamed"},
			RowAfter:  map[string]any{"id": float64(2), "status": "final"},
		},
	})
}

// assertPKChangeState folds the page (in binary log order: the events are
// given at rising positions) and asserts the state each key ends in, with
// wantState's contract: "" is dead, anything else the surviving status.
//
// The guard moved from the merge entry points to foldPage when the window
// became paged (#1097), and since #2501 it is no longer a refusal but a split
// of the UPDATE into its two halves, so this asserts the fold's result where
// it runs — one event at a time, before the event is ever folded into the
// change map. The "no partial output on disk" half of the old assertion is
// structural: the fold completes before mergeBaselineIntoWriter is called.
func assertPKChangeState(t *testing.T, want map[int]string, page []query.ResultRow) {
	t.Helper()
	for i := range page {
		page[i].BinlogFile, page[i].StartPos, page[i].EventID = pkFile, 100*uint64(i+1), uint64(i+1)
	}
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, foldOptions{}, res); err != nil {
		t.Fatalf("foldPage: %v", err)
	}
	wantState(t, res, want)
}

// reinsertEventsAndMap builds the #782 reinsert permutation — `UPDATE pk 1→2`
// followed by `INSERT pk=1` (both stored under before-image key "1") — as a raw
// event slice plus the collapsed change map the entry points fold it into. The
// collapse overwrites the UPDATE with the INSERT, so changes["1"] is the INSERT
// and a MAP-ONLY scan misses the PK change entirely: only the raw-slice scan
// catches it. Returns both, asserting the missed-by-map precondition.
func reinsertEventsAndMap(t *testing.T) ([]query.ResultRow, map[string]*query.ResultRow) {
	t.Helper()
	events := []query.ResultRow{
		{
			EventType: event.EventUpdate,
			PKValues:  pkStrForInt(1),
			RowBefore: map[string]any{"id": float64(1), "status": "orig"},
			RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
		},
		{
			EventType: event.EventInsert,
			PKValues:  pkStrForInt(1),
			RowAfter:  map[string]any{"id": float64(1), "status": "reinserted"},
		},
	}
	changes := make(map[string]*query.ResultRow, len(events))
	for i := range events {
		changes[events[i].PKValues] = &events[i]
	}
	// Precondition: the collapsed map has lost the UPDATE, so the map-only scan
	// does NOT detect the PK change — the whole point of the raw-slice scan.
	if _, _, ok := pkChangingUpdate(changes, pkColsIntID()); ok {
		t.Fatal("precondition failed: map-only scan unexpectedly caught the reinsert case")
	}
	if _, _, ok := pkChangingUpdateInEvents(events, pkColsIntID()); !ok {
		t.Fatal("precondition failed: raw-slice scan must catch the reinsert case")
	}
	return events, changes
}

// TestFoldPage_pkChangingUpdate_reinsertPermutation is the #782 review
// permutation: `UPDATE pk 1→2; INSERT pk=1`. A map-only guard misses it (the
// INSERT overwrites the UPDATE under key "1"), so a guard that ran over the
// collapsed map would let the merge silently DROP the moved row (pk=2).
//
// Since #1097 the window is paged and the UPDATE is handled per event inside
// foldPage — since #2501 by splitting it — so this asserts the state there,
// and, crucially, asserts it BOTH when the two events land on the same page
// and when a page boundary falls between them. The split case is the one that
// a whole-slice scan could never get wrong and a paged fold could: it is the
// regression this test exists to catch. Both keys end alive: 1 with the
// INSERT's image, 2 with the UPDATE's.
func TestFoldPage_pkChangingUpdate_reinsertPermutation(t *testing.T) {
	events, _ := reinsertEventsAndMap(t)
	for i := range events {
		events[i].BinlogFile, events[i].StartPos, events[i].EventID = pkFile, 100*uint64(i+1), uint64(i+1)
	}

	for _, tc := range []struct {
		name  string
		pages [][]query.ResultRow
	}{
		{"single page", [][]query.ResultRow{events}},
		{"split across a page boundary", [][]query.ResultRow{events[:1], events[1:]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := &foldResult{Changes: map[string]*query.ResultRow{}}
			for _, page := range tc.pages {
				if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, foldOptions{}, res); err != nil {
					t.Fatalf("foldPage: %v", err)
				}
			}
			wantState(t, res, map[int]string{1: "reinserted", 2: "moved"})
		})
	}
}

// TestFoldPage_lastWriteWinsAcrossPages pins the property paging could most
// plausibly break: a PK touched on an early page and again on a later one must
// resolve to the LATER image. Pages arrive in ascending (event_timestamp,
// event_id) order and are folded in order, so the same last-write-wins result a
// single-slice fold produced still holds — but nothing else in the suite would
// notice if that ordering assumption were violated.
func TestFoldPage_lastWriteWinsAcrossPages(t *testing.T) {
	pages := [][]query.ResultRow{
		{{EventType: event.EventInsert, PKValues: pkStrForInt(7), RowAfter: map[string]any{"id": float64(7), "status": "first"}}},
		{{EventType: event.EventUpdate, PKValues: pkStrForInt(9), RowBefore: map[string]any{"id": float64(9), "status": "x"}, RowAfter: map[string]any{"id": float64(9), "status": "y"}}},
		{{EventType: event.EventUpdate, PKValues: pkStrForInt(7), RowBefore: map[string]any{"id": float64(7), "status": "first"}, RowAfter: map[string]any{"id": float64(7), "status": "last"}}},
	}

	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	for i, page := range pages {
		if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, foldOptions{}, res); err != nil {
			t.Fatalf("foldPage(page %d): %v", i, err)
		}
	}
	changes := res.Changes

	got := changes[pkStrForInt(7)]
	if got == nil {
		t.Fatal("pk 7 missing from the change map")
	}
	if status := got.RowAfter["status"]; status != "last" {
		t.Errorf("pk 7 resolved to %q, want %q — a later page must win over an earlier one", status, "last")
	}
	// foldPage must store retainEvent's trimmed COPY, not the event itself.
	// Without this assertion, reverting the fold to `changes[pk] = ev` keeps the
	// whole suite green while re-pinning every page's backing array in memory —
	// silently undoing the entire point of paging.
	if got.RowBefore != nil {
		t.Error("change-map entry kept its before-image; foldPage must store retainEvent's trimmed copy")
	}
	if len(changes) != 2 {
		t.Errorf("change map has %d entries, want 2 (one per distinct touched PK)", len(changes))
	}
}

// TestRetainEvent_dropsBeforeImage pins the memory contract retainEvent exists
// for: the retained copy must not alias the page it came from, and must not
// carry the fields no merge stage reads. If someone later restores RowBefore
// here "just in case", the change map silently doubles in size again — and if
// someone moves a before-image guard back onto the map, it reads nil.
func TestRetainEvent_dropsBeforeImage(t *testing.T) {
	qt, qh := "UPDATE orders SET status='x'", "deadbeef"
	page := []query.ResultRow{{
		EventType: event.EventUpdate,
		PKValues:  pkStrForInt(1),
		RowBefore: map[string]any{"id": float64(1), "status": "old"},
		RowAfter:  map[string]any{"id": float64(1), "status": "new"},
		QueryText: &qt,
		QueryHash: &qh,
	}}

	kept := retainEvent(&page[0])

	if kept == &page[0] {
		t.Fatal("retainEvent returned a pointer INTO the page; that pins the whole page in memory")
	}
	if kept.RowBefore != nil {
		t.Error("RowBefore must be dropped: nothing downstream reads it, and the guards that do run before the trim")
	}
	if kept.QueryText != nil || kept.QueryHash != nil {
		t.Error("QueryText/QueryHash must be dropped: no merge stage reads them and they are capped at 16 KiB each")
	}
	if kept.RowAfter == nil || kept.RowAfter["status"] != "new" {
		t.Errorf("RowAfter must survive intact, got %#v", kept.RowAfter)
	}
	if kept.EventType != event.EventUpdate || kept.PKValues != pkStrForInt(1) {
		t.Error("event identity fields must survive the trim")
	}
}

// TestSnapshotFullTableImages_pkChangingUpdate_reinsertPermutation covers the
// same reinsert permutation on the shim/verify _snapshot engine when the caller
// supplies the raw Events slice (a full-window caller). BaselinePath points
// nowhere: the refusal must fire before baseline materialization.
func TestSnapshotFullTableImages_pkChangingUpdate_reinsertPermutation(t *testing.T) {
	events, changes := reinsertEventsAndMap(t)
	emitted := 0
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: "/nonexistent/never-touched/baseline.parquet",
		Schema:       "mydb",
		Table:        "orders",
		PKCols:       pkColsIntID(),
		Changes:      changes,
		Events:       events,
	}, func(map[string]any) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("expected a loud error for the reinsert PK-changing UPDATE")
	}
	if !strings.Contains(err.Error(), "PK-changing UPDATE") {
		t.Errorf("error should name the PK-changing UPDATE, got: %v", err)
	}
	if strings.Contains(err.Error(), "materialize baseline") {
		t.Errorf("refusal must fire BEFORE baseline materialization, got: %v", err)
	}
	if emitted != 0 {
		t.Errorf("refusal must emit no rows, emitted %d", emitted)
	}
}

// TestPKChangingUpdateInEvents covers the raw-slice detection helper directly,
// including the reinsert permutation a collapsed-map scan cannot see and the
// event-order-independence (earliest offender returned).
func TestPKChangingUpdateInEvents(t *testing.T) {
	mk := func(et event.EventType, pk string, before, after map[string]any) query.ResultRow {
		return query.ResultRow{EventType: et, PKValues: pk, RowBefore: before, RowAfter: after}
	}
	cases := []struct {
		name       string
		events     []query.ResultRow
		wantFound  bool
		wantBefore string
		wantAfter  string
	}{
		{name: "empty", events: nil},
		{
			name: "no pk change",
			events: []query.ResultRow{
				mk(event.EventUpdate, pkStrForInt(1), map[string]any{"id": float64(1)}, map[string]any{"id": float64(1)}),
				mk(event.EventInsert, pkStrForInt(2), nil, map[string]any{"id": float64(2)}),
			},
		},
		{
			name: "reinsert permutation (missed by map)",
			events: []query.ResultRow{
				mk(event.EventUpdate, pkStrForInt(1), map[string]any{"id": float64(1)}, map[string]any{"id": float64(2)}),
				mk(event.EventInsert, pkStrForInt(1), nil, map[string]any{"id": float64(1)}),
			},
			wantFound: true, wantBefore: "1", wantAfter: "2",
		},
		{
			name: "earliest offender returned",
			events: []query.ResultRow{
				mk(event.EventInsert, pkStrForInt(9), nil, map[string]any{"id": float64(9)}),
				mk(event.EventUpdate, pkStrForInt(3), map[string]any{"id": float64(3)}, map[string]any{"id": float64(4)}),
				mk(event.EventUpdate, pkStrForInt(7), map[string]any{"id": float64(7)}, map[string]any{"id": float64(8)}),
			},
			wantFound: true, wantBefore: "3", wantAfter: "4",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, a, found := pkChangingUpdateInEvents(tc.events, pkColsIntID())
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v (before=%q after=%q)", found, tc.wantFound, b, a)
			}
			if found && (b != tc.wantBefore || a != tc.wantAfter) {
				t.Errorf("before/after = %q/%q, want %q/%q", b, a, tc.wantBefore, tc.wantAfter)
			}
		})
	}
}

// TestSnapshotFullTableImages_pkChangingUpdateRefused pins the #782 guard on the
// shim/verify full-table _snapshot entry point INDEPENDENTLY of the mydumper
// path. BaselinePath deliberately points nowhere and emit records invocations:
// the refusal must fire BEFORE baseline materialization (in production an S3
// download) and before a single row is emitted.
func TestSnapshotFullTableImages_pkChangingUpdateRefused(t *testing.T) {
	emitted := 0
	err := SnapshotFullTableImages(context.Background(), SnapshotFullTableInput{
		BaselinePath: "/nonexistent/never-touched/baseline.parquet",
		Schema:       "mydb",
		Table:        "orders",
		PKCols:       pkColsIntID(),
		Changes: map[string]*query.ResultRow{
			pkStrForInt(1): {
				EventType: event.EventUpdate,
				PKValues:  pkStrForInt(1),
				RowBefore: map[string]any{"id": float64(1), "status": "new"},
				RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
			},
		},
	}, func(map[string]any) error {
		emitted++
		return nil
	})
	if err == nil {
		t.Fatal("expected a loud error for a PK-changing UPDATE")
	}
	if !strings.Contains(err.Error(), "PK-changing UPDATE") {
		t.Errorf("error should name the PK-changing UPDATE, got: %v", err)
	}
	if strings.Contains(err.Error(), "materialize baseline") {
		t.Errorf("refusal must fire BEFORE baseline materialization, got: %v", err)
	}
	if emitted != 0 {
		t.Errorf("refusal must emit no rows, emitted %d", emitted)
	}
}

// TestFoldPage_pkChangingUpdate_binlogOnlyShape covers the no-baseline
// fallback (#766) for a PK-changing UPDATE. That path streams and folds its
// window through the same foldPage as the baseline path since #1097, so the
// split is asserted there; what makes it worth keeping as a distinct case is
// that the binlog-only path has NO baseline to bound the window, so it is the
// path most likely to see a long history containing one. With no base row to
// skip, the DELETE half of key 1 is a no-op and the INSERT half of key 2 is
// the appended row.
func TestFoldPage_pkChangingUpdate_binlogOnlyShape(t *testing.T) {
	assertPKChangeState(t, map[int]string{1: "", 2: "moved"}, []query.ResultRow{{
		EventType: event.EventUpdate,
		PKValues:  pkStrForInt(1),
		RowBefore: map[string]any{"id": float64(1), "status": "new"},
		RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
	}})
}
