package icebergexport

import "testing"

// #2621: right after a load, the cursor is the baseline's anchor. When no row
// event was indexed after the dump, the run's cut (the newest indexed event)
// is older than the anchor, which is also what a reset or restored index
// looks like. captureReachedAnchor tells the two apart from where capture
// has read to: at or past the anchor, the index simply holds nothing newer.
func TestCaptureReachedAnchor2621(t *testing.T) {
	anchor := cursor{File: "binlog.000007", Pos: 900, FromBaseline: true}
	cases := []struct {
		name string
		file string
		pos  uint64
		want bool
	}{
		{"same file, past the anchor", "binlog.000007", 950, true},
		{"same file, exactly at the anchor", "binlog.000007", 900, true},
		{"same file, before the anchor", "binlog.000007", 899, false},
		{"a later file", "binlog.000008", 4, true},
		{"an earlier file", "binlog.000006", 99999, false},
		{"a later file with a longer number", "binlog.1000000", 4, true},
		// Two base names are two numbering schemes: no order between them.
		{"another base name, same length", "mysqlb.000008", 4, false},
		{"another base name, longer", "mysql-bin.000008", 4, false},
		{"no checkpoint position", "", 0, false},
		{"a file name without a number", "binlog", 950, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := captureReachedAnchor(tc.file, tc.pos, anchor); got != tc.want {
				t.Fatalf("captureReachedAnchor(%q, %d, %s) = %v, want %v", tc.file, tc.pos, anchor.String(), got, tc.want)
			}
		})
	}
	// An anchor without a file proves nothing either.
	if captureReachedAnchor("binlog.000007", 950, cursor{FromBaseline: true}) {
		t.Fatal("an anchor with no file was treated as reached")
	}
}
