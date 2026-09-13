package views

import (
	"strings"
	"testing"
)

// TestHeader_unreadableDirectoriesAreNamed pins the #1601 note in both of the
// header's shapes. A views file that pins one snapshot and says nothing reads
// as "this is the newest one here"; one that says "none discoverable" over a
// directory the walk could not open reads as "no backup exists". Both are
// silent subsets, and the note is what turns a subset into a stated one.
func TestHeader_unreadableDirectoriesAreNamed(t *testing.T) {
	const note = "under this location could not be read"

	pinned := goldenInput()
	pinned.BaselineUnreadable = 2
	got := Generate(pinned)
	if !strings.Contains(got, "NOTE: 2 snapshot director") || !strings.Contains(got, note) {
		t.Errorf("a file that pins a snapshot over a partly readable location does not say so:\n%s", firstLines(got, 30))
	}

	none := goldenInput()
	none.Baselines = nil
	none.BaselineUnreadable = 3
	got = Generate(none)
	if !strings.Contains(got, "none discoverable") {
		t.Fatalf("zero-baselines header lost its line:\n%s", firstLines(got, 30))
	}
	if !strings.Contains(got, "NOTE: 3 snapshot director") || !strings.Contains(got, "does not name") {
		t.Errorf("\"none discoverable\" over an unreadable location does not say a snapshot may exist there:\n%s", firstLines(got, 30))
	}

	// The negative half: a fully read location carries no note, or the line
	// becomes noise the reader learns to skip.
	if got := Generate(goldenInput()); strings.Contains(got, note) {
		t.Error("a fully readable location carries the unreadable note")
	}
}

func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}
