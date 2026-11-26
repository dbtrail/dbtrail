package pgstreamrun

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// A capture that failed on its own as the loop stopped with an error of its
// own (#3114: a slot lost just as the lock went) is logged, since One returns
// the loop's error; a capture that only stopped is not.
func TestReportCaptureFailure_3114(t *testing.T) {
	for _, c := range []struct {
		capErr error
		want   bool
	}{
		{fmt.Errorf("stream: %w", pgcapture.ErrSlotLost), true},
		{nil, false},
		{context.Canceled, false},
		{streamrun.ErrStopWithoutFlush, false},
	} {
		var buf bytes.Buffer
		reportCaptureFailure(slog.New(slog.NewTextHandler(&buf, nil)), streamrun.ErrStopWithoutFlush, c.capErr)
		if got := strings.Contains(buf.String(), "stream_error="); got != c.want {
			t.Errorf("capture error %v: logged %v, want %v (%q)", c.capErr, got, c.want, buf.String())
		}
	}
}
