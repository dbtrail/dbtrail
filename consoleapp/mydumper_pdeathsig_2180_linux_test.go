//go:build linux

package consoleapp

import (
	"os/exec"
	"syscall"
	"testing"
)

// mydumper dies with the daemon (#2180): orphaned, it would keep writing into
// a dump folder the next start removes.
func TestPrepareMydumperCmd_diesWithTheDaemon(t *testing.T) {
	cmd := exec.Command("true")
	prepareMydumperCmd(cmd)
	if cmd.SysProcAttr == nil || cmd.SysProcAttr.Pdeathsig != syscall.SIGKILL {
		t.Fatalf("SysProcAttr = %+v, want Pdeathsig SIGKILL", cmd.SysProcAttr)
	}
	if out, err := runMydumperCmd(cmd); err != nil {
		t.Fatalf("running it: %v %s", err, out)
	}
}
