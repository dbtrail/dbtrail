//go:build linux

package sqlsandbox

import (
	"os"
	"os/exec"
	"syscall"
)

// fenceReason is whether a host without a fence says why: on Linux it does.
const fenceReason = true

// procSelfCgroup is the daemon's own cgroup membership.
func procSelfCgroup() ([]byte, error) { return os.ReadFile("/proc/self/cgroup") }

// cgroupDelegated reports whether dir was handed over to this process to
// manage: systemd marks a cgroup it delegates (Delegate=yes), for a unit run
// as root and for one with User= alike (systemd 255, verified). Being able
// to write there is not the test, and neither is owning it: root can write
// into any cgroup, and a user's own systemd owns cgroups it manages itself.
func cgroupDelegated(dir string) bool {
	for _, attr := range []string{"user.delegate", "trusted.delegate"} {
		buf := make([]byte, 8)
		if n, err := syscall.Getxattr(dir, attr, buf); err == nil && string(buf[:n]) == "1" {
			return true
		}
	}
	return false
}

// startInFence makes the kernel start cmd inside the cgroup (clone3 with
// CLONE_INTO_CGROUP, Linux 5.7), so the worker never runs a moment outside
// its ceiling. setProcessGroup has set SysProcAttr already.
func startInFence(cmd *exec.Cmd, wf *workerFence) {
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(wf.handle.Fd())
}
