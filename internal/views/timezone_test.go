package views

import (
	"strings"
	"testing"
)

// TestGenerate_pinsTheSessionToUTC covers the one statement in the file that is
// neither a view nor a way to reach storage.
//
// It exists because leaving it out is wrong QUIETLY. The archives write
// event_timestamp as TIMESTAMP WITH TIME ZONE, so the session's zone decides
// where date_trunc puts a day boundary: read from Buenos Aires, an event at
// 2026-01-02 01:00 UTC buckets into January 1st, and nothing fails. The guide
// used to carry this as a step performed by hand, which made the silent wrong
// answer the default for anyone who skipped it.
func TestGenerate_pinsTheSessionToUTC(t *testing.T) {
	got := Generate(goldenInput())
	if !strings.Contains(got, "SET TimeZone = 'UTC';") {
		t.Fatal("the generated file does not pin the session zone, so date_trunc buckets on " +
			"whatever zone the reader's machine happens to be in")
	}

	// Ahead of every view. A zone set after them still applies, since the views
	// are not evaluated at creation, but a reader who stops the file early or
	// copies the first half out gets the buckets they expect either way.
	if strings.Index(got, "SET TimeZone") > strings.Index(got, "CREATE OR REPLACE VIEW") {
		t.Error("the zone is pinned after the views are defined")
	}

	// It says what it did. This changes the reader's session rather than
	// describing the layout, which is the one thing the rest of the file never
	// does, so the file has to own it and say how to undo it.
	// The needle avoids the line break: the sentence is wrapped into fixed
	// width comment lines, so a phrase spanning the wrap point pins the
	// WRAPPING rather than the meaning, and re-flowing the paragraph would
	// fail a guard that has no opinion about it.
	if !strings.Contains(got, "you would rather read in your own zone") {
		t.Error("the file sets the reader's session zone without saying they can change it")
	}

	// A local-only file gets it too: the zone question is about the column type,
	// not about where the bytes are.
	local := goldenInput()
	local.ArchiveSources = []string{"/data/archives/bintrail_id=1111"}
	local.BaselineSource = "/data/baselines"
	if !strings.Contains(Generate(local), "SET TimeZone = 'UTC';") {
		t.Error("a file over local paths does not pin the session zone")
	}
}
