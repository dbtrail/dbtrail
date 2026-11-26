//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"testing"
)

// A worker the kernel stopped at its ceiling leaves no answer: the line it
// wrote on stderr before running says whether the join check had it run in
// the order written. Without that line it ran in the copy's own order and is
// run again (and stopped again here); with it, it is not, and says why.
func TestRunner_writtenOrderRetryAfterTheKernelStoppedTheWorker(t *testing.T) {
	fakeFence(t, "max 12\noom 1\noom_kill 1\n")
	f := newCopyFixture(t)
	for _, tc := range []struct {
		name, script, want string
	}{
		{"no line: run again, and stopped again", "kill -9 $$", WrittenOrderRetryFailed},
		{"the check chose the order written", "echo '" + joinOrderWrittenLine + "' >&2; kill -9 $$", WrittenOrderRetrySkippedCheckWritten},
	} {
		r := New(Config{Exe: "/bin/sh", Args: []string{"-c", tc.script}, Limits: testLimits()})
		job := f.job("SELECT 1")
		job.Session.RetryOutOfMemoryInWrittenOrder = true
		job.Session.JoinNDV = &JoinCounts{NDV: map[string]float64{"a": 1}}
		_, err := r.Run(context.Background(), job)
		var qe *QueryError
		if !errors.As(err, &qe) || qe.WrittenOrderRetry != tc.want {
			t.Errorf("%s: err = %v (%T)", tc.name, err, err)
		}
	}
}
