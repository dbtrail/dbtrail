//go:build !unix

package sqlsandbox

import (
	"os"
	"os/exec"
)

// No process groups here: the child alone is killed. DuckDB's threads belong
// to that process, so this is enough for the worker itself.
func setProcessGroup(*exec.Cmd) {}

func killProcessGroup(p *os.Process) error {
	if p == nil {
		return nil
	}
	return p.Kill()
}
