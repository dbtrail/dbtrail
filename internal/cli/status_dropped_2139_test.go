package cli

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/status"
)

// The --fail-on-gap message for dropped events. A stale snapshot is the usual
// cause and stays the default text; row_map_failed (#2139) is not that, and a
// fresh snapshot does not fix it.
func TestDroppedEventsError(t *testing.T) {
	t.Run("row_map_failed alone", func(t *testing.T) {
		msg := droppedEventsError(3, []string{status.CaptureSkipReasonRowMapFailed}).Error()
		for _, want := range []string{
			"3 event(s) had rows read from the stream and permanently dropped (row_map_failed)",
			"a text value was not valid UTF-8 and could not be converted",
			"`failed to map` lines in the capture log",
			"--ack-capture-skips",
			"failing closed under --fail-on-gap",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("missing %q:\n%s", want, msg)
			}
		}
		for _, wrong := range []string{"stale or corrupt", "run `bintrail snapshot`", "—"} {
			if strings.Contains(msg, wrong) {
				t.Errorf("message carries %q, which is not true for this reason:\n%s", wrong, msg)
			}
		}
	})
	t.Run("row_map_failed beside a stale-snapshot reason", func(t *testing.T) {
		msg := droppedEventsError(12, []string{status.CaptureSkipReasonRowMapFailed, status.CaptureSkipReasonColumnCountMismatch}).Error()
		for _, want := range []string{
			"12 event(s) read from the stream and permanently dropped (column_count_mismatch, row_map_failed)",
			"most often the schema snapshot is stale or corrupt (row_map_failed is a different cause: a text value that could not be converted, which a fresh snapshot does not fix",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("missing %q:\n%s", want, msg)
			}
		}
	})
	t.Run("other reasons are unchanged", func(t *testing.T) {
		msg := droppedEventsError(41203, []string{status.CaptureSkipReasonColumnCountMismatch}).Error()
		const want = "capture health: 41203 event(s) read from the stream and permanently dropped (column_count_mismatch); most often the schema snapshot is stale or corrupt; run `bintrail snapshot` against the source and restart the stream, then acknowledge this tally with `bintrail status --index-dsn <index> --ack-capture-skips` (it is monotonic and never clears itself; acknowledging erases nothing and a later skip fails this check again); failing closed under --fail-on-gap"
		if msg != want {
			t.Errorf("message changed for a reason this did not touch:\n%s", msg)
		}
	})
}
