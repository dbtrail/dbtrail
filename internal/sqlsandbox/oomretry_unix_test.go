//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"testing"
)

// A worker the kernel stopped at its ceiling leaves no answer that says which
// join order it ran in: with the join check on it may have been the order
// written already, so it is not run again; without the check it was the
// copy's own, and it is.
func TestRunner_writtenOrderRetryAfterTheKernelStoppedTheWorker(t *testing.T) {
	fakeFence(t, "max 12\noom 1\noom_kill 1\n")
	f := newCopyFixture(t)
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "kill -9 $$"}, Limits: testLimits()})
	for _, tc := range []struct {
		name  string
		check bool
		want  string
	}{
		{"no join check: run again, and stopped again", false, WrittenOrderRetryFailed},
		{"the join check on: not run again", true, ""},
	} {
		job := f.job("SELECT 1")
		job.Session.RetryOutOfMemoryInWrittenOrder = true
		if tc.check {
			job.Session.JoinNDV = &JoinCounts{NDV: map[string]float64{"a": 1}}
		}
		_, err := r.Run(context.Background(), job)
		var qe *QueryError
		if !errors.As(err, &qe) || qe.WrittenOrderRetry != tc.want {
			t.Errorf("%s: err = %v (%T)", tc.name, err, err)
		}
	}
}
