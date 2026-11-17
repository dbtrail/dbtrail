package console

import "testing"

// An answer from the snapshot alone, after an answer with tails on the same
// connection, says when it is behind that earlier answer (a server changed
// from fresh to fast mode while the connection was open).
func TestSnapshotOlderThanSeen(t *testing.T) {
	files := map[string][]string{"/copy/2026-11-17T01-40-00Z": {"/copy/2026-11-17T01-40-00Z/bank.accounts.parquet"}}
	cutAt := func(id uint64, why string) func() (uint64, string) {
		return func() (uint64, string) { return id, why }
	}
	never := func() (uint64, string) {
		t.Fatal("the snapshot's cut was asked for an answer that read none")
		return 0, ""
	}
	cases := []struct {
		name     string
		readFrom map[string][]string
		ok       bool
		cut      func() (uint64, string)
		note     string
		seen     uint64
	}{
		{"snapshot behind the floor", files, true, cutAt(400, ""), snapshotOlderNote, 400},
		{"snapshot at the floor", files, true, cutAt(500, ""), "", 500},
		{"snapshot past the floor", files, true, cutAt(900, ""), "", 500},
		{"cut unknown", files, true, cutAt(0, "the index could not be read"), snapshotMaybeOlderNote, 0},
		{"cut zero with no reason", files, true, cutAt(0, ""), snapshotMaybeOlderNote, 0},
		{"files not told", nil, false, never, snapshotMaybeOlderNote, 0},
		{"no snapshot table read", map[string][]string{}, true, never, "", 500},
		{"nil files, told", nil, true, never, "", 500},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			note, seen := snapshotOlderThanSeen(500, c.readFrom, c.ok, c.cut)
			if note != c.note || seen != c.seen {
				t.Fatalf("got (%q, %d), want (%q, %d)", note, seen, c.note, c.seen)
			}
		})
	}
}

// The next answer from the same snapshot, once the floor was lowered to it,
// says nothing more: the connection was told once.
func TestSnapshotOlderThanSeenTellsOnce(t *testing.T) {
	files := map[string][]string{"/copy/a": {"/copy/a/t.parquet"}}
	cut := func() (uint64, string) { return 400, "" }
	note, seen := snapshotOlderThanSeen(500, files, true, cut)
	if note == "" || seen != 400 {
		t.Fatalf("first answer: got (%q, %d)", note, seen)
	}
	if note, seen = snapshotOlderThanSeen(seen, files, true, cut); note != "" || seen != 400 {
		t.Fatalf("second answer: got (%q, %d), want no note and the floor kept", note, seen)
	}
}
