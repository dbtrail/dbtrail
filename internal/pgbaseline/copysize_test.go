package pgbaseline

import (
	"slices"
	"testing"
)

// The sum the disk check reads before a PostgreSQL snapshot (#3029).
func TestCopyTableSum(t *testing.T) {
	var sum copyTableSum
	for _, r := range []copyTableRow{
		{schema: "public", table: "orders", bytes: 100},
		{schema: "public", table: "gone", bytes: 0, unsized: 1},
		{schema: "public", table: "events", bytes: 40, unsized: 2}, // two leaves without a size: one table
		{schema: "ts", table: "small_col", bytes: 5, other: 1},
		{schema: "ts", table: "big_col", bytes: 500, other: 3},
		{schema: "ts", table: "mid_col", bytes: 50, other: 1},
		{schema: "ts", table: "also_mid", bytes: 50, other: 1},
		{schema: "public", table: "negative", bytes: -7},
	} {
		sum.add(r)
	}
	got := sum.estimate()
	if got.Bytes != 745 || got.Tables != 8 || got.Unsized != 2 || got.OtherStorage != 4 {
		t.Errorf("estimate = %+v, want 745 bytes, 8 tables, 2 unsized, 4 on another storage", got)
	}
	// The largest two, and by name between equal sizes.
	if want := []string{"ts.big_col", "ts.also_mid"}; !slices.Equal(got.OtherStorageTop, want) {
		t.Errorf("named = %q, want %q", got.OtherStorageTop, want)
	}
}

func TestCopyTableSumEmpty(t *testing.T) {
	var sum copyTableSum
	if got := sum.estimate(); got.Bytes != 0 || got.Tables != 0 || got.Unsized != 0 || got.OtherStorage != 0 || got.OtherStorageTop != nil {
		t.Errorf("estimate of nothing = %+v", got)
	}
}

func TestCopyOtherStorageSentence(t *testing.T) {
	for _, c := range []struct {
		est  CopyEstimate
		want string
	}{
		{CopyEstimate{}, ""},
		{CopyEstimate{OtherStorage: 1, OtherStorageTop: []string{"ts.metrics"}},
			" 1 table is not stored as a PostgreSQL heap (ts.metrics): the size the server reports for it can be a compressed one, and a snapshot is written from its rows, so it can need more than the sizes say."},
		{CopyEstimate{OtherStorage: 5, OtherStorageTop: []string{"ts.a", "ts.b"}},
			" 5 tables are not stored as a PostgreSQL heap (ts.a, ts.b and 3 more): the size the server reports for them can be a compressed one, and a snapshot is written from their rows, so it can need more than the sizes say."},
	} {
		if got := CopyOtherStorageSentence(c.est); got != c.want {
			t.Errorf("%+v:\n got %q\nwant %q", c.est, got, c.want)
		}
	}
}

// A name with a line break stays on the line it is printed in.
func TestCopyEstimateNamesStayOnOneLine(t *testing.T) {
	var sum copyTableSum
	sum.add(copyTableRow{schema: "s\n1", table: "t\r\nx", bytes: 1, other: 1})
	if got, want := sum.estimate().OtherStorageTop, []string{"s?1.t??x"}; !slices.Equal(got, want) {
		t.Errorf("named = %q, want %q", got, want)
	}
}
