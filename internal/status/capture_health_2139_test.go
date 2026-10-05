package status

import (
	"strings"
	"testing"
	"time"
)

// The explanation for row_map_failed (#2139), built from a ledger shaped like
// the one the parser writes.
func rowMapLedger() map[string]CaptureSkipStat {
	return map[string]CaptureSkipStat{
		CaptureSkipReasonRowMapFailed: {
			Count: 3, LastAt: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
			LastFile: "binlog.000007", LastPos: 200, Tables: []string{"shop.notes"},
		},
	}
}

func TestExplainCaptureSkips_rowMapFailedTellsTheTruth(t *testing.T) {
	lines := ExplainCaptureSkips(rowMapLedger(), time.Time{})
	if len(lines) != 5 {
		t.Fatalf("got %d paragraphs, want cause, remedy, scope, acknowledgement, log:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	cause, remedy, scope := lines[0], lines[1], lines[2]

	// The cause covers all three ways a value fails, including a latin1
	// column: "only latin1 is converted" alone would deny the row an operator
	// is looking at.
	for _, want := range []string{
		"shop.notes had rows with text",
		"it converts latin1 only",
		"a latin1 value holds a byte that latin1 assigns no character to",
		"too old to have recorded the column's character set",
		"The other rows of the same event were indexed.",
	} {
		if !strings.Contains(cause, want) {
			t.Errorf("cause missing %q:\n%s", want, cause)
		}
	}
	// A fresh snapshot is offered for the one case it fixes, and ruled out
	// for the others.
	for _, want := range []string{"refresh the schema snapshot", "In the other cases a fresh snapshot changes nothing", "utf8mb4"} {
		if !strings.Contains(remedy, want) {
			t.Errorf("remedy missing %q:\n%s", want, remedy)
		}
	}
	// Reading the binlogs again drops the same rows: do not send anyone there.
	if strings.Contains(scope, "bintrail index") {
		t.Errorf("re-reading the binlogs is offered for rows it would drop again:\n%s", scope)
	}
	if !strings.Contains(scope, "Reading the binlogs again does not bring them back") {
		t.Errorf("scope does not say re-reading fails:\n%s", scope)
	}
	if !strings.Contains(lines[4], `on the lines reading "failed to map"`) {
		t.Errorf("log line does not name the string to look for:\n%s", lines[4])
	}
	for i, l := range []string{cause, remedy, scope} {
		if strings.Contains(l, "—") {
			t.Errorf("paragraph %d has an em dash:\n%s", i, l)
		}
	}
}

// With another reason beside it the re-read advice stays for that reason and
// is explicitly withdrawn for this one.
func TestExplainCaptureSkips_rowMapFailedBesideAnotherReason(t *testing.T) {
	ledger := rowMapLedger()
	ledger[CaptureSkipReasonColumnCountMismatch] = CaptureSkipStat{
		Count: 9, LastAt: time.Date(2026, 10, 5, 11, 0, 0, 0, time.UTC), Tables: []string{"shop.orders"},
	}
	all := strings.Join(ExplainCaptureSkips(ledger, time.Time{}), "\n")
	for _, want := range []string{
		"`bintrail index --binlog-dir <dir> --files <file>` can re-read them.",
		"That does not apply to row_map_failed: reading the binlogs again drops the same rows the same way",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q:\n%s", want, all)
		}
	}
}

// Every other reason keeps the paragraph it had.
func TestExplainCaptureSkips_otherReasonsKeepTheReReadAdvice(t *testing.T) {
	ledger := map[string]CaptureSkipStat{
		CaptureSkipReasonColumnCountMismatch: {Count: 9, LastAt: time.Date(2026, 10, 5, 11, 0, 0, 0, time.UTC)},
	}
	lines := ExplainCaptureSkips(ledger, time.Time{})
	const want = "None of this recovers what was already skipped: those changes are absent from the index for good " +
		"unless the source still has the binlogs covering that window, in which case `bintrail index --binlog-dir " +
		"<dir> --files <file>` can re-read them."
	if lines[2] != want {
		t.Errorf("scope paragraph changed for a reason this did not touch:\n%s", lines[2])
	}
	if strings.Contains(strings.Join(lines, "\n"), "row_map_failed") {
		t.Error("row_map_failed mentioned in a ledger that does not hold it")
	}
}
