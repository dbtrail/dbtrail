package reconstruct

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// TestReconstruct843_columnDroppedAfterBaselineFailsLoud is the regression for
// #843, the symmetric direction of #602. A column DROPped from the source
// table after the baseline snapshot stops appearing in delta events' row_after
// images; rowAfterOrdered used to NULL-fill it (with a per-row warn), so the
// dump mixed two schema epochs: touched rows NULL, never-touched pass-through
// rows keeping the pre-drop value, under a CREATE TABLE still declaring the
// column. The fix refuses the run loudly instead — and must do so BEFORE
// writing any chunk file, so no partial output is left on disk.
func TestReconstruct843_columnDroppedAfterBaselineFailsLoud(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	// id=2 is UPDATEd after the `status` column was DROPped. The event
	// row_after carries only id. status ∈ baseline columns (id, status).
	folded := foldForTest(t, []query.ResultRow{{
		EventType: parser.EventUpdate,
		PKValues:  pkStrForInt(2),
		RowBefore: map[string]any{"id": float64(2)},
		RowAfter:  map[string]any{"id": float64(2)},
	}})

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           folded.Changes,
		ImageColumns:      folded.ImageColumns,
		SawImage:          folded.SawImage,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)

	if err == nil {
		t.Fatalf("expected a fail-loud error for the dropped 'status' column, got nil")
	}
	if !strings.Contains(err.Error(), "status") {
		t.Errorf("error should name the dropped column %q, got: %v", "status", err)
	}
	if !strings.Contains(err.Error(), "bintrail baseline") {
		t.Errorf("error should point at re-running `bintrail baseline`, got: %v", err)
	}

	// No partial output may be left on disk: the guard fires before the writer
	// opens, so the output dir must contain no .sql chunk files.
	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			t.Errorf("partial output left on disk after failed run: %s", e.Name())
		}
	}
}

// TestReconstruct843_columnDroppedDeleteOnlyFailsLoud is the follow-up
// regression: a window where the ONLY post-drop event for a touched PK is a
// DELETE. DELETE events carry no row_after, so the original #843 guard (which
// inspected row_after exclusively) skipped them and let the run through — a
// pass-through row for a different, never-touched PK would then silently
// re-emit the dropped column's stale pre-drop value (the exact PII
// re-exposure scenario the #843 refusal exists to prevent). row_before on a
// DELETE is itself a post-drop image (the row as it existed just before
// deletion), so it carries the same detection signal as row_after.
func TestReconstruct843_columnDroppedDeleteOnlyFailsLoud(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	// id=2 is DELETEd after the `status` column was DROPped. The event
	// row_before (post-drop image) carries only id — no row_after at all.
	folded := foldForTest(t, []query.ResultRow{{
		EventType: parser.EventDelete,
		PKValues:  pkStrForInt(2),
		RowBefore: map[string]any{"id": float64(2)},
	}})

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           folded.Changes,
		ImageColumns:      folded.ImageColumns,
		SawImage:          folded.SawImage,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)

	if err == nil {
		t.Fatalf("expected a fail-loud error for the dropped 'status' column (DELETE-only signal), got nil")
	}
	if !strings.Contains(err.Error(), "status") {
		t.Errorf("error should name the dropped column %q, got: %v", "status", err)
	}

	entries, derr := os.ReadDir(outDir)
	if derr != nil {
		t.Fatalf("read output dir: %v", derr)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			t.Errorf("partial output left on disk after failed run: %s", e.Name())
		}
	}
}

// TestReconstruct843_nullValueIsNotADrop pins the column-absent-from-image vs
// value-null distinction: an event whose row_after carries the column with a
// nil value (a genuine SQL NULL under binlog_row_image=FULL) must NOT trip the
// dropped-column refusal — it merges and emits NULL as before.
func TestReconstruct843_nullValueIsNotADrop(t *testing.T) {
	baselinePath := writeTestBaseline(t, [][]string{
		{"1", "new"},
		{"2", "paid"},
	})
	outDir := t.TempDir()

	folded := foldForTest(t, []query.ResultRow{{
		EventType: parser.EventUpdate,
		PKValues:  pkStrForInt(2),
		RowBefore: map[string]any{"id": float64(2), "status": "paid"},
		RowAfter:  map[string]any{"id": float64(2), "status": nil},
	}})

	rep := &TableReport{}
	err := mergeBaselineIntoWriter(context.Background(), mergeInput{
		LocalBaselinePath: baselinePath,
		CreateTableSQL:    "-- test",
		Schema:            "mydb",
		Table:             "orders",
		PKCols:            pkColsIntID(),
		Changes:           folded.Changes,
		ImageColumns:      folded.ImageColumns,
		SawImage:          folded.SawImage,
		OutputDir:         outDir,
		ChunkSize:         0,
	}, rep)
	if err != nil {
		t.Fatalf("mergeBaselineIntoWriter refused a genuine NULL value: %v", err)
	}
	if rep.UpdatesApplied != 1 {
		t.Errorf("UpdatesApplied = %d, want 1", rep.UpdatesApplied)
	}

	chunk := mustReadOnlyChunk(t, outDir)
	if !strings.Contains(chunk, "(1, 'new')") {
		t.Errorf("chunk missing passthrough row:\n%s", chunk)
	}
	if !strings.Contains(chunk, "(2, NULL)") {
		t.Errorf("chunk missing NULL-status updated row:\n%s", chunk)
	}
}

// TestDroppedBaselineColumns covers the pure detection helper directly.
func TestDroppedBaselineColumns(t *testing.T) {
	baseCols := []string{"id", "status"}

	cases := []struct {
		name   string
		events []query.ResultRow
		want   []string
	}{
		{
			name:   "no events",
			events: nil,
			want:   nil,
		},
		{
			name: "no drift",
			events: []query.ResultRow{
				{EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "status": "x"}},
			},
			want: nil,
		},
		{
			name: "NULL value is not a drop",
			events: []query.ResultRow{
				{EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2, "status": nil}},
			},
			want: nil,
		},
		{
			name: "one dropped column",
			events: []query.ResultRow{
				{EventType: event.EventUpdate, RowAfter: map[string]any{"id": 2}},
			},
			want: []string{"status"},
		},
		{
			name: "multiple dropped columns sorted and deduped across events",
			events: []query.ResultRow{
				{EventType: event.EventUpdate, RowAfter: map[string]any{}},
				{EventType: event.EventInsert, RowAfter: map[string]any{"status": "x"}},
			},
			want: []string{"id", "status"},
		},
		{
			name: "DELETE row_before still detects a drop (#843 follow-up)",
			events: []query.ResultRow{
				{EventType: event.EventDelete, RowBefore: map[string]any{"id": 2}},
			},
			want: []string{"status"},
		},
		{
			name: "DELETE row_before with no drift is not flagged",
			events: []query.ResultRow{
				{EventType: event.EventDelete, RowBefore: map[string]any{"id": 2, "status": "paid"}},
			},
			want: nil,
		},
		{
			// An event carrying no image at all contributes nothing, and if it
			// is the ONLY event the window saw no image — which must read as
			// "no evidence", not "every column dropped".
			name: "event with no images at all is not evidence of a drop",
			events: []query.ResultRow{
				{EventType: event.EventInsert, RowAfter: nil},
			},
			want: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			folded := foldForTest(t, c.events)
			got := droppedBaselineColumns(folded.ImageColumns, folded.SawImage, baseCols)
			if len(got) != len(c.want) {
				t.Fatalf("droppedBaselineColumns = %v, want %v", got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("droppedBaselineColumns = %v, want %v", got, c.want)
				}
			}
		})
	}
}

// foldForTest runs an event page through the REAL fold, so the #843 assertions
// below exercise the map shape production actually produces.
//
// This is the whole point of the helper: before #1097 these tests hand-built
// the change map with before-images intact, and kept passing after retainEvent
// started blanking them — the guard was dead and its own regression test could
// not see it. Anything asserting on the change map must come through here.
func foldForTest(t *testing.T, events []query.ResultRow) *foldResult {
	t.Helper()
	res := &foldResult{Changes: map[string]*query.ResultRow{}}
	if err := foldPage(events, "mydb", "orders", pkColsIntID(), nil, res); err != nil {
		t.Fatalf("foldPage: %v", err)
	}
	return res
}
