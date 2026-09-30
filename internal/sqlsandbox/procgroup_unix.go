//go:build unix

package sqlsandbox

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// setProcessGroup puts the child in its own process group, so a kill can
// take the whole group without touching the parent's.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killProcessGroup SIGKILLs the child's process group. An already-gone group
// is not an error: the child may have exited on its own between the deadline
// firing and the kill.
func killProcessGroup(p *os.Process) error {
	if p == nil {
		return nil
	}
	err := syscall.Kill(-p.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
