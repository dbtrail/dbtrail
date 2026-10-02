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

// TestFoldPage_pkChangingUpdateRefused_scenarioA is #782 scenario A
// (resurrection): `UPDATE pk 1→2; DELETE pk=2`. The change map is keyed by the
// before-image PK, so without the guard the baseline row pk=1 would match the
// UPDATE and emit pk=2 — a row the DELETE actually removed. The guard must
// refuse loudly, naming both PKs, before the fold ever reaches the writer.
func TestFoldPage_pkChangingUpdateRefused_scenarioA(t *testing.T) {
	assertPKChangeRefusal(t, []query.ResultRow{
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

// TestFoldPage_pkChangingUpdateRefused_scenarioB is #782 scenario B
// (duplication): `UPDATE pk 1→2; UPDATE pk=2`. pk=2 would be emitted twice (as
// the first UPDATE's after-image under key 1, and by the second UPDATE under
// key 2), a 1062 that only surfaces at restore time.
func TestFoldPage_pkChangingUpdateRefused_scenarioB(t *testing.T) {
	assertPKChangeRefusal(t, []query.ResultRow{
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

// assertPKChangeRefusal folds the given event page and asserts the #782
// refusal: a loud error naming the schema.table and the PK transition.
//
// The guard moved from the merge entry points to foldPage when the window
// became paged (#1097), so this asserts it where it now runs — one event at a
// time, before the event is ever folded into the change map. The
// "no partial output on disk" half of the old assertion is now structural
// rather than checked here: the fold completes before mergeBaselineIntoWriter
// is called at all, so a refusal happens before the writer can exist.
func assertPKChangeRefusal(t *testing.T, page []query.ResultRow) {
	t.Helper()
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, res)
	if err == nil {
		t.Fatal("expected a fail-loud error for a PK-changing UPDATE, got nil")
	}
	for _, want := range []string{"PK-changing UPDATE", "mydb.orders", `"1"`, `"2"`, "bintrail baseline"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%s", want, err)
		}
	}
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
// permutation: `UPDATE pk 1→2; INSERT pk=1`. The map-only guard misses it (the
// INSERT overwrites the UPDATE under key "1"), so a guard that ran over the
// collapsed map would let the merge silently DROP the moved row (pk=2).
//
// Since #1097 the window is paged and the guard runs per event inside foldPage,
// so this asserts the refusal there — and, crucially, asserts it BOTH when the
// two events land on the same page and when a page boundary falls between them.
// The split case is the one that a whole-slice scan could never get wrong and a
// paged fold could: it is the regression this test exists to catch.
func TestFoldPage_pkChangingUpdate_reinsertPermutation(t *testing.T) {
	events, _ := reinsertEventsAndMap(t)

	for _, tc := range []struct {
		name  string
		pages [][]query.ResultRow
	}{
		{"single page", [][]query.ResultRow{events}},
		{"split across a page boundary", [][]query.ResultRow{events[:1], events[1:]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := &foldResult{Changes: map[string]*query.ResultRow{}}
			var err error
			for _, page := range tc.pages {
				if err = foldPage(page, "mydb", "orders", pkColsIntID(), nil, res); err != nil {
					break
				}
			}
			if err == nil {
				t.Fatal("expected a fail-loud error for the reinsert PK-changing UPDATE, got nil")
			}
			for _, want := range []string{"PK-changing UPDATE", "mydb.orders", `"1"`, `"2"`} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error missing %q:\n%s", want, err)
				}
			}
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
		if err := foldPage(page, "mydb", "orders", pkColsIntID(), nil, res); err != nil {
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

// TestFoldPage_pkChangingUpdateRefused_binlogOnlyShape covers the no-baseline
// fallback (#766) for a PK-changing UPDATE. That path streams and folds its
// window through the same foldPage as the baseline path since #1097, so the
// refusal is asserted there; what makes it worth keeping as a distinct case is
// that the binlog-only path has NO baseline to bound the window, so it is the
// path most likely to see a long history containing one.
func TestFoldPage_pkChangingUpdateRefused_binlogOnlyShape(t *testing.T) {
	assertPKChangeRefusal(t, []query.ResultRow{{
		EventType: event.EventUpdate,
		PKValues:  pkStrForInt(1),
		RowBefore: map[string]any{"id": float64(1), "status": "new"},
		RowAfter:  map[string]any{"id": float64(2), "status": "moved"},
	}})
}
