package console

import "testing"

// An answer from the snapshot alone after an answer with tails on the same
// connection is told once; a statement that reads no table of the copy, or
// one that read tails, is not told and changes nothing.
func TestTellSnapshotAlone(t *testing.T) {
	cases := []struct {
		name                 string
		seen                 uint64
		told, read, tailsRan bool
		want                 bool
	}{
		{"after a fresh answer, the snapshot alone", 500, false, true, false, true},
		{"told already", 500, true, true, false, false},
		{"no fresh answer on this connection", 0, false, true, false, false},
		{"a probe that reads no table (SELECT 1, a catalog listing)", 500, false, false, false, false},
		{"tails ran (fresh mode, the change log named)", 500, false, true, true, false},
	}
	for _, c := range cases {
		if got := tellSnapshotAlone(c.seen, c.told, c.read, c.tailsRan); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}
