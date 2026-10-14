package sqlsandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// A helper process started the way a statement's worker is (#2385): in a
// process group of its own, killed as a group, raised to the kernel's first
// choice when the host runs out (the child does that itself, with
// LowerOOMPriority), and inside a memory cgroup of its own where the host
// gives one (#2226). The console runs the verify drill-down in one, so a
// table that needs more memory than it may stops that process and never the
// one that captures.
//
// What it does NOT share with a statement's worker is the lockdown: the
// child is the console's own code with the console's environment (the index
// DSN, the AWS credential chain, S3), not the user's SQL. Everything that
// makes a worker safe to hand a stranger's statement stays in Runner.

// ChildJob is one run of a helper process.
type ChildJob struct {
	// Args are the arguments after the executable: the hidden command that
	// makes the binary act as the child.
	Args []string
	// Env is the child's whole environment.
	Env []string
	// MemoryLimit is the DuckDB memory the child may use in all, e.g.
	// "1092MiB". Its cgroup's ceiling is this plus the worker headroom
	// (CeilingBytes).
	MemoryLimit string
	// Input is written to the child's stdin once it is inside its cgroup.
	// Stdin stays open until the child exits, so a child that reads it to
	// the end learns that the parent died.
	Input []byte
	// MaxOutput caps what the child may write to stdout. Past it the child
	// is killed and ErrResultTooLarge is returned.
	MaxOutput int64
}

// ChildOOMError is a child the kernel stopped at its own cgroup's ceiling:
// it needed more memory than it may have, and nothing else was touched.
type ChildOOMError struct {
	// CeilingBytes is the ceiling it reached.
	CeilingBytes int64
}

func (e *ChildOOMError) Error() string {
	return fmt.Sprintf("it needed more memory than its ceiling of %d MiB and was stopped; nothing else on the host was affected", e.CeilingBytes>>20)
}

// ChildExitError is a child that ended without an answer: it exited with an
// error, or was killed by something other than its own ceiling.
type ChildExitError struct {
	// Err is what Wait returned.
	Err error
	// Stderr is the end of what the child wrote to stderr.
	Stderr string
}

func (e *ChildExitError) Error() string {
	if s := strings.TrimSpace(e.Stderr); s != "" {
		return fmt.Sprintf("%v: %s", e.Err, s)
	}
	return e.Err.Error()
}

func (e *ChildExitError) Unwrap() error { return e.Err }

// childStderrMax is how much of a child's stderr is kept: its last words are
// what an error report needs, and the rest is dropped, never a reason to
// fail.
const childStderrMax = 64 << 10

// childFence is where children get their cgroups: found once per process,
// with the cgroups a dead daemon's children left swept on the way.
var childFence = sync.OnceValue(func() fence {
	f := fenceFor()
	if f.root != "" {
		sweepFences(f.root)
	}
	return f
})

// childExe is the executable a child runs: this binary. A variable so a
// test can start its own test binary.
var childExe = workerExe

// RunChild starts the child, hands it job.Input, and returns what it wrote
// to stdout once it exits 0. ctx ending kills the child's whole process
// group and returns ctx.Err(). A child that outgrows its cgroup returns a
// *ChildOOMError; one that exits otherwise without success a
// *ChildExitError.
func RunChild(ctx context.Context, job ChildJob) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	exe, err := childExe()
	if err != nil {
		return nil, fmt.Errorf("locate the executable: %w", err)
	}
	var (
		wf      *workerFence
		ceiling int64
	)
	if f := childFence(); f.root != "" {
		if wf, err = f.make(job.MemoryLimit); err != nil {
			slog.Warn("a helper process runs without a memory cgroup of its own", "args", job.Args, "error", err)
			wf = nil
		} else {
			ceiling, _ = fenceBytes(job.MemoryLimit)
		}
	}
	c, err := startChild(exe, job, wf)
	if err != nil && wf != nil {
		// Not started inside its cgroup: start it as a statement's worker
		// would then, without one, rather than not at all.
		slog.Warn("a helper process runs without a memory cgroup of its own", "args", job.Args, "error", err)
		wf, ceiling = nil, 0
		c, err = startChild(exe, job, nil)
	}
	if err != nil {
		return nil, err
	}
	// Inside its cgroup now (or with none): only now does it get its job.
	if _, err := c.stdin.Write(job.Input); err != nil {
		c.kill()
		<-c.done
		if c.oomKilled {
			return nil, &ChildOOMError{CeilingBytes: ceiling}
		}
		return nil, &ChildExitError{Err: fmt.Errorf("hand the job over: %w", err), Stderr: c.stderr.text()}
	}
	select {
	case <-c.done:
	case <-ctx.Done():
		select {
		case <-c.done:
			// Ended on its own as ctx did: never signal a group that may be
			// gone. The caller asked to stop, so it gets that.
		default:
			c.kill()
			<-c.done
		}
		return nil, ctx.Err()
	}
	switch {
	case c.stdout.didOverflow():
		return nil, ErrResultTooLarge
	case c.oomKilled:
		return nil, &ChildOOMError{CeilingBytes: ceiling}
	case c.waitErr != nil:
		return nil, &ChildExitError{Err: c.waitErr, Stderr: c.stderr.text()}
	}
	return c.stdout.take(), nil
}

// LowerOOMPriority makes the calling process the kernel's first choice when
// the host runs out of memory. A child calls it first thing; best-effort,
// and a no-op outside Linux.
func LowerOOMPriority(stderr io.Writer) { lowerOOMPriority(stderr) }

// child is one started helper process.
type child struct {
	stdin io.Writer
	stdout, stderr *cappedBuffer
	kill           func()
	done           chan struct{}
	// Set before done is closed.
	waitErr   error
	oomKilled bool
}

// startChild starts the child and, when wf is given, moves it into that
// cgroup before returning it. wf is removed when the process exits, and also
// when the child cannot be started or moved.
func startChild(exe string, job ChildJob, wf *workerFence) (*child, error) {
	cmd := exec.Command(exe, job.Args...)
	cmd.Env = job.Env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		if wf != nil {
			wf.close()
		}
		return nil, fmt.Errorf("stdin: %w", err)
	}
	setProcessGroup(cmd)
	// If the group kill somehow leaves the pipes open, stop waiting on them.
	cmd.WaitDelay = 5 * time.Second
	c := &child{stdin: stdin, done: make(chan struct{}),
		stderr: &cappedBuffer{max: childStderrMax, drop: true},
	}
	var once sync.Once
	c.kill = func() { once.Do(func() { _ = killProcessGroup(cmd.Process) }) }
	c.stdout = &cappedBuffer{max: job.MaxOutput, onOverflow: func() { go c.kill() }}
	cmd.Stdout, cmd.Stderr = c.stdout, c.stderr
	if err := cmd.Start(); err != nil {
		if wf != nil {
			wf.close()
		}
		return nil, fmt.Errorf("start: %w", err)
	}
	go func() {
		c.waitErr = cmd.Wait()
		if wf != nil {
			// The process is gone: its cgroup's counters are final, and the
			// cgroup can go.
			c.oomKilled = wf.oomKilled()
			wf.close()
		}
		close(c.done)
	}()
	if wf != nil {
		err := intoFence(wf, cmd.Process.Pid)
		if errors.Is(err, syscall.ESRCH) {
			// Already gone: a child that died at its start, which its exit
			// reports. Not a cgroup that failed.
			return c, nil
		}
		if err != nil {
			// Never handed its job outside the ceiling as if it were inside.
			c.kill()
			<-c.done
			return nil, fmt.Errorf("move it into its cgroup: %w", err)
		}
	}
	return c, nil
}
