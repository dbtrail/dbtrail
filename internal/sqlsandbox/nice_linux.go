package sqlsandbox

import (
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"syscall"
)

// threadNice reads the nice value of thread tid. The raw system call
// answers 20 minus the nice value (1 to 40), not the value itself.
var threadNice = func(tid int) (int, error) {
	raw, err := syscall.Getpriority(syscall.PRIO_PROCESS, tid)
	if err != nil {
		return 0, err
	}
	return 20 - raw, nil
}

// setThreadNice sets the nice value of thread tid. A variable so a test can
// make it fail.
var setThreadNice = func(tid, nice int) error {
	return syscall.Setpriority(syscall.PRIO_PROCESS, tid, nice)
}

// onMainThread says whether the calling thread is the process's main thread.
// A variable so a test can take the main thread's path from any thread.
var onMainThread = func() bool { return syscall.Gettid() == os.Getpid() }

// startLowered runs start, which starts one process, step nice values below
// the thread that calls it (#3321).
//
// On Linux the nice value belongs to each thread, and a new thread or process
// takes the one of the thread that made it. Lowering the child from outside
// once it runs would reach only the threads it has by then: the Go runtime's
// own and, in a standby worker, DuckDB's, which it opens before its job comes.
// So the process is started from a thread of its own whose priority is
// lowered first: the child, and every thread it ever makes, is born at the
// lower priority. That thread is locked to this goroutine and never
// unlocked, so the runtime ends it when the goroutine returns and its
// priority never runs anything else of this process. Nothing here raises a
// priority, so it needs no privilege.
//
// niceErr says why the child was started at this process's priority instead;
// startErr is start's own.
func startLowered(step int, start func() error) (niceErr, startErr error) {
	if step <= 0 {
		return nil, start()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.LockOSThread()
		if onMainThread() {
			// The process's main thread, which the runtime parks instead of
			// ending when a goroutine locked to it returns: lowered, it would
			// stay lowered, and it is the thread ps and top show for the whole
			// process. Held here, it cannot be the one a second goroutine
			// lands on, and it goes back to the scheduler untouched.
			niceErr, startErr = lowerAndStart(step, start)
			runtime.UnlockOSThread()
			return
		}
		// No UnlockOSThread, on purpose: see above.
		niceErr, startErr = startOnThisThread(step, start)
	}()
	<-done
	return niceErr, startErr
}

// lowerAndStart runs startOnThisThread in a goroutine of its own, locked to
// a thread that is not the caller's.
func lowerAndStart(step int, start func() error) (niceErr, startErr error) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.LockOSThread()
		niceErr, startErr = startOnThisThread(step, start)
	}()
	<-done
	return niceErr, startErr
}

// startOnThisThread lowers the calling thread, which must be locked to its
// goroutine and never unlocked, then runs start on it. A panic in start comes
// back as its error: nothing above this goroutine could recover it.
func startOnThisThread(step int, start func() error) (niceErr, startErr error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("sql on the copy: starting a worker panicked", "panic", p, "stack", string(debug.Stack()))
			startErr = fmt.Errorf("start the worker: panic: %v", p)
		}
	}()
	niceErr = lowerThisThread(step)
	return niceErr, start()
}

// lowerThisThread lowers the calling thread step nice values, down to the
// lowest there is.
func lowerThisThread(step int) error {
	tid := syscall.Gettid()
	cur, err := threadNice(tid)
	if err != nil {
		return fmt.Errorf("read this thread's CPU priority: %w", err)
	}
	target, lower := niceTarget(cur, step)
	if !lower {
		return fmt.Errorf("this process already runs at nice %d, the lowest there is", cur)
	}
	if err := setThreadNice(tid, target); err != nil {
		return fmt.Errorf("lower the CPU priority from nice %d to %d: %w", cur, target, err)
	}
	return nil
}
