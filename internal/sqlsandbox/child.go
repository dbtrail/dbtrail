package sqlsandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
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
	// StopGrace is how long a child whose context ended has to stop by
	// itself once its stdin is closed, so its deferred cleanup runs (a
	// working directory the size of a table, say). Past it, or with 0, its
	// process group is killed.
	StopGrace time.Duration
	// LogMsg and LogAttrs label the child's stderr in this process's log:
	// each line it writes is logged here, as it comes, at the level the line
	// names (WARN when it names none).
	LogMsg   string
	LogAttrs []any
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
	// Stderr is the child's own account of why: the line of a Go panic, or
	// else the last line it wrote. Its whole stderr is in the log.
	Stderr string
	// Killed: SIGKILL from outside this process. With no ceiling of its own
	// to reach, that is most often the kernel's out-of-memory killer.
	Killed bool
}

func (e *ChildExitError) Error() string {
	if e.Killed {
		return "the process was killed from outside, most likely by the kernel because the host or its container ran out of memory"
	}
	if s := strings.TrimSpace(e.Stderr); s != "" {
		return fmt.Sprintf("%v: %s", e.Err, s)
	}
	return e.Err.Error()
}

func (e *ChildExitError) Unwrap() error { return e.Err }

// childStderrLines is how many of a child's stderr lines reach this
// process's log; past it one line says how many more were dropped.
const childStderrLines = 200

// childFence is where children get their cgroups, found once per process.
// No sweep here: the console's Runner sweeps the same root once at boot, and
// a sweep while it serves statements could remove a cgroup it has made and
// not yet filled.
var childFence = sync.OnceValue(func() fence { return fenceFor() })

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
		// It died before reading it: its own exit says why, not the pipe.
		c.kill()
		<-c.done
		if c.waitErr == nil {
			c.waitErr = fmt.Errorf("hand the job over: %w", err)
		}
		return nil, c.failure(ceiling)
	}
	select {
	case <-c.done:
	case <-ctx.Done():
		c.stop(job.StopGrace)
		return nil, ctx.Err()
	}
	if c.stdout.didOverflow() {
		return nil, ErrResultTooLarge
	}
	if err := c.failure(ceiling); err != nil {
		return nil, err
	}
	return c.stdout.take(), nil
}

// failure is how the finished child ended, nil for a clean exit.
func (c *child) failure(ceiling int64) error {
	switch {
	case c.oomKilled:
		return &ChildOOMError{CeilingBytes: ceiling}
	case c.waitErr == nil:
		return nil
	}
	var ee *exec.ExitError
	killed := false
	if errors.As(c.waitErr, &ee) && !c.killedByUs.Load() {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL {
			killed = true
		}
	}
	return &ChildExitError{Err: c.waitErr, Stderr: c.stderr.summary(), Killed: killed}
}

// stop ends a child whose caller gave up: its stdin is closed so it can stop
// by itself and run its cleanup, and past grace its group is killed.
func (c *child) stop(grace time.Duration) {
	_ = c.stdinCloser.Close()
	if grace > 0 {
		t := time.NewTimer(grace)
		defer t.Stop()
		select {
		case <-c.done:
			return
		case <-t.C:
		}
	}
	c.kill()
	<-c.done
}

// LowerOOMPriority makes the calling process the kernel's first choice when
// the host runs out of memory. A child calls it first thing; best-effort,
// and a no-op outside Linux.
func LowerOOMPriority(stderr io.Writer) { lowerOOMPriority(stderr) }

// child is one started helper process.
type child struct {
	stdin       io.Writer
	stdinCloser io.Closer
	stdout      *cappedBuffer
	stderr      *stderrLog
	kill        func()
	done        chan struct{}
	// reaped is set as soon as Wait returns: from then on the pid may be
	// another process's, and kill must not signal it.
	reaped atomic.Bool
	// killedByUs: the group kill came from this process (a cancel, the
	// output cap, a cgroup it could not be moved into), not from outside.
	killedByUs atomic.Bool
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
	c := &child{stdin: stdin, stdinCloser: stdin, done: make(chan struct{}),
		stderr: &stderrLog{msg: job.LogMsg, attrs: job.LogAttrs},
	}
	var once sync.Once
	c.kill = func() {
		once.Do(func() {
			if c.reaped.Load() {
				return
			}
			c.killedByUs.Store(true)
			_ = killProcessGroup(cmd.Process)
		})
	}
	c.stdout = &cappedBuffer{max: job.MaxOutput, onOverflow: func() { go c.kill() }}
	cmd.Stdout, cmd.Stderr = c.stdout, c.stderr
	if err := cmd.Start(); err != nil {
		if wf != nil {
			wf.close()
		}
		return nil, fmt.Errorf("start: %w", err)
	}
	go func() {
		err := cmd.Wait()
		c.reaped.Store(true)
		c.waitErr = err
		c.stderr.flush()
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

// stderrLog takes a child's stderr: each line goes to this process's log as
// it comes, so what the child warned about is not lost when it succeeds, and
// the lines an error report needs are kept.
type stderrLog struct {
	msg   string
	attrs []any

	mu      sync.Mutex
	partial []byte
	lines   int
	dropped int
	panic   string
	last    string
}

func (l *stderrLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partial = append(l.partial, p...)
	for {
		i := bytes.IndexByte(l.partial, '\n')
		if i < 0 {
			break
		}
		l.line(string(l.partial[:i]))
		l.partial = l.partial[i+1:]
	}
	// A line with no end in sight is cut, never held without bound.
	if len(l.partial) > 16<<10 {
		l.line(string(l.partial))
		l.partial = nil
	}
	return len(p), nil
}

// flush logs a last line the child did not end, and the count of dropped
// ones. Called once the child has exited.
func (l *stderrLog) flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.partial) > 0 {
		l.line(string(l.partial))
		l.partial = nil
	}
	if l.dropped > 0 {
		slog.Warn(l.label(), append(l.attrs[:len(l.attrs):len(l.attrs)], "dropped_lines", l.dropped)...)
	}
}

func (l *stderrLog) line(text string) {
	text = strings.TrimRight(text, "\r")
	if strings.TrimSpace(text) == "" {
		return
	}
	if l.panic == "" && isFatalLine(text) {
		l.panic = text
	}
	l.last = text
	if l.lines >= childStderrLines {
		l.dropped++
		return
	}
	l.lines++
	slog.Log(context.Background(), childLineLevel(text), l.label(), append(l.attrs[:len(l.attrs):len(l.attrs)], "line", text)...)
}

func (l *stderrLog) label() string {
	if l.msg == "" {
		return "helper process"
	}
	return l.msg
}

// summary is the child's own account of why it failed: the line a Go crash
// starts with (the goroutine dump after it is for the log, not for a page), or
// else the last line it wrote, cut to a readable length.
func (l *stderrLog) summary() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.last
	if l.panic != "" {
		s = l.panic
	}
	if len(s) > 1<<10 {
		s = s[:1<<10] + "..."
	}
	return s
}

// childLineLevel is the level a child's log line names (slog's text format
// writes level=ERROR), WARN when it names none: the child logs at WARN and
// above, and a line with no level is a panic or a library's own output.
func childLineLevel(text string) slog.Level {
	switch {
	case strings.Contains(text, "level=ERROR"):
		return slog.LevelError
	case strings.Contains(text, "level=INFO"):
		return slog.LevelInfo
	case strings.Contains(text, "level=DEBUG"):
		return slog.LevelDebug
	}
	return slog.LevelWarn
}

// isFatalLine reports the line the Go runtime starts a crash with: a panic,
// a fatal error (concurrent map writes, the runtime out of memory), or a
// signal in C code such as DuckDB's.
func isFatalLine(text string) bool {
	for _, p := range []string{"panic: ", "fatal error: ", "SIGSEGV", "SIGBUS", "unexpected signal"} {
		if strings.HasPrefix(text, p) {
			return true
		}
	}
	return false
}
