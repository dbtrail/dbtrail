//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// The child's environment, seen from INSIDE the child: a shell in the
// worker's place prints its environment to stderr and exits 1. The worker
// marker is there; the credentials set in the parent are not. This also
// covers "a non-zero exit with no result is a WorkerError with stderr".
func TestRun_childEnvironmentIsScrubbed(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "leak-aws")
	t.Setenv("BINTRAIL_INDEX_DSN", "leak-dsn")
	f := newCopyFixture(t)
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "env 1>&2; exit 1"}, Limits: testLimits()})
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("err = %v, want WorkerError", err)
	}
	if !strings.Contains(we.Stderr, workerEnv+"=1") {
		t.Errorf("child env lacks the worker marker:\n%s", we.Stderr)
	}
	for _, leak := range []string{"AWS_SECRET_ACCESS_KEY", "BINTRAIL_INDEX_DSN", "leak-aws", "leak-dsn"} {
		if strings.Contains(we.Stderr, leak) {
			t.Errorf("child env carries %s:\n%s", leak, we.Stderr)
		}
	}
	if !strings.Contains(we.Error(), "exit status 1") {
		t.Errorf("error does not carry the exit status: %v", we)
	}
}

// A child that prints garbage and exits 2 is a WorkerError carrying its
// stderr, never a result.
func TestRun_garbageFromTheChildIsAWorkerError(t *testing.T) {
	f := newCopyFixture(t)
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "echo 'not json'; echo oops 1>&2; exit 2"}, Limits: testLimits()})
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("err = %v, want WorkerError", err)
	}
	if !strings.Contains(we.Stderr, "oops") || !strings.Contains(we.Error(), "exit status 2") {
		t.Errorf("WorkerError = %v (stderr %q)", we, we.Stderr)
	}
	// Garbage with a clean exit is still not a result.
	r = New(Config{Exe: "/bin/sh", Args: []string{"-c", "echo 'not json'"}, Limits: testLimits()})
	_, err = r.Run(context.Background(), f.job("SELECT 1"))
	if !errors.As(err, &we) {
		t.Fatalf("clean exit with garbage: err = %v, want WorkerError", err)
	}
}

// The PARENT's own overflow path: a child that writes past MaxResultBytes and
// then hangs is killed by the parent (the capped pipe buffer fires the group
// kill) and reported as ErrResultTooLarge, well before the child would have
// exited on its own. The worker stops at the cap on its side too, so this
// path is the belt for a worker of another version or a result whose
// column header, which the worker does not count, tips it over.
func TestRun_parentKillsAChildThatOverflowsTheResult(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MaxResultBytes = 4096
	l.Timeout = 30 * time.Second
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "head -c 100000 /dev/zero | tr '\\0' x; sleep 30"}, Limits: l})
	start := time.Now()
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	took := time.Since(start)
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("err = %v, want ErrResultTooLarge", err)
	}
	if took > 5*time.Second {
		t.Errorf("Run took %v: the parent waited for the child instead of killing it", took)
	}
}
