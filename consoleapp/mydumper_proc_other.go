//go:build !linux

package consoleapp

import "os/exec"

// Pdeathsig is Linux-only; elsewhere an orphaned mydumper is not stopped by
// the kernel (the production image is Linux).
func prepareMydumperCmd(*exec.Cmd) {}

func runMydumperCmd(cmd *exec.Cmd) ([]byte, error) { return cmd.CombinedOutput() }
