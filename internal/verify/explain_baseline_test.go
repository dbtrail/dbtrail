package verify

import (
	"bytes"
	"strings"
	"testing"
)

// TestMismatchExplanation_Write pins the drill-down formatting: a changed row
// shows the column with recovery-vs-baseline values, missing/extra rows are
// labeled by side, and a capped run reports the remaining count.
func TestMismatchExplanation_Write(t *testing.T) {
	ex := &MismatchExplanation{
		Schema: "mydb", Table: "orders", Anchor: "binlog.000001:300",
		Diffs: []RowDiff{
			{PK: "id=2", Kind: diffChanged, Cells: []CellDiff{{Column: "status", Recovery: "wrong", Baseline: "shipped"}}},
			{PK: "id=5", Kind: diffExtra},
			{PK: "id=7", Kind: diffMissing},
		},
		Total: 5, // two more than shown
	}
	var buf bytes.Buffer
	ex.Write(&buf)
	out := buf.String()

	for _, want := range []string{
		"mydb.orders @ binlog.000001:300",
		"id=2", "status", "recovery=wrong", "baseline=shipped",
		"id=5", "absent from the new baseline",
		"id=7", "NOT reproduced by the recovery",
		"and 2 more differing row(s)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("drill-down output missing %q\n--- output ---\n%s", want, out)
		}
	}
}

// TestMismatchExplanation_Write_rowCountOnly covers the edge where a mismatch was
// flagged on row count but every matched PK lines up cell-for-cell (Total == 0):
// the drill-down must say so, not print an empty section.
func TestMismatchExplanation_Write_rowCountOnly(t *testing.T) {
	ex := &MismatchExplanation{Schema: "mydb", Table: "orders", Anchor: "binlog.000001:300"}
	var buf bytes.Buffer
	ex.Write(&buf)
	if !strings.Contains(buf.String(), "row count, not row content") {
		t.Errorf("want the row-count-only note, got:\n%s", buf.String())
	}
}

// TestMismatchExplanation_Write_overflowByKind: rows past the printed cap are
// still counted, and the overflow line breaks them down by kind so the
// missing rows are never hidden behind the ones that filled the cap.
func TestMismatchExplanation_Write_overflowByKind(t *testing.T) {
	ex := &MismatchExplanation{Total: maxExplainRows + 25, byKind: map[string]int{diffExtra: maxExplainRows + 20, diffMissing: 5}}
	for range maxExplainRows {
		ex.Diffs = append(ex.Diffs, RowDiff{PK: "x", Kind: diffExtra})
	}
	var buf bytes.Buffer
	ex.Write(&buf)
	if !strings.Contains(buf.String(), "5 missing (not reproduced), 20 extra") {
		t.Errorf("want the overflow broken down by kind, got:\n%s", buf.String())
	}
}

// TestMismatchExplanation_Write_deferredCaveat: a drill-down that touched a
// deferred-type column surfaces the caveat (its reconstructed value may be an
// event image, not corruption); one that didn't must not show it.
func TestMismatchExplanation_Write_deferredCaveat(t *testing.T) {
	with := &MismatchExplanation{
		Schema: "mydb", Table: "orders", Anchor: "binlog.000001:300",
		Diffs:        []RowDiff{{PK: "id=2", Kind: diffChanged, Cells: []CellDiff{{Column: "kind", Recovery: "2", Baseline: "shipped"}}}},
		Total:        1,
		deferredSeen: true,
	}
	var b1 bytes.Buffer
	with.Write(&b1)
	if !strings.Contains(b1.String(), "deferred-type column") {
		t.Errorf("want the deferred caveat, got:\n%s", b1.String())
	}

	without := &MismatchExplanation{
		Schema: "mydb", Table: "orders", Anchor: "binlog.000001:300",
		Diffs: []RowDiff{{PK: "id=2", Kind: diffChanged, Cells: []CellDiff{{Column: "status", Recovery: "wrong", Baseline: "shipped"}}}},
		Total: 1,
	}
	var b2 bytes.Buffer
	without.Write(&b2)
	if strings.Contains(b2.String(), "deferred-type column") {
		t.Errorf("no deferred column touched — caveat must not appear:\n%s", b2.String())
	}
}

// TestCellEqual locks the NULL-vs-empty distinction the drill-down must honor to
// agree with the content digest: a SQL NULL (nil) and an empty value ([]byte(""))
// are DIFFERENT, where plain bytes.Equal would call them equal and silently miss
// a NULL↔'' divergence the verdict flagged.
func TestCellEqual(t *testing.T) {
	empty := []byte("")
	for _, tc := range []struct {
		name string
		a, b []byte
		want bool
	}{
		{"both NULL", nil, nil, true},
		{"NULL vs empty", nil, empty, false},
		{"empty vs NULL", empty, nil, false},
		{"both empty", empty, empty, true},
		{"equal values", []byte("x"), []byte("x"), true},
		{"different values", []byte("x"), []byte("y"), false},
		{"NULL vs value", nil, []byte("x"), false},
	} {
		if got := cellEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: cellEqual(%q,%q)=%v, want %v", tc.name, tc.a, tc.b, got, tc.want)
		}
	}
}

// add records one differing row the way rowJoin.diff fills an explanation:
// counted in Total and byKind, kept in Diffs up to maxExplainRows. A test
// helper for building explanations by hand.
func (ex *MismatchExplanation) add(d RowDiff) {
	ex.Total++
	if ex.byKind == nil {
		ex.byKind = map[string]int{}
	}
	ex.byKind[d.Kind]++
	if len(ex.Diffs) < maxExplainRows {
		ex.Diffs = append(ex.Diffs, d)
	}
}
