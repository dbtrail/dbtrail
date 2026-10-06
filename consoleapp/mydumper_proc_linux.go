//go:build linux

package consoleapp

import (
	"os/exec"
	"runtime"
	"syscall"
)

// prepareMydumperCmd makes mydumper die with the daemon (#2180). Orphaned by
// a SIGKILL or the OOM killer, it would go on writing into a dump folder that
// the next start removes as a dead job's.
func prepareMydumperCmd(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
}

// runMydumperCmd runs it on a locked OS thread: Linux sends the death signal
// when the THREAD that started the child exits, not the process, and the Go
// runtime may retire an idle thread. Locked, the thread lives until the
// child has been waited for.
func runMydumperCmd(cmd *exec.Cmd) ([]byte, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return cmd.CombinedOutput()
}
