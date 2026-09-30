//go:build !linux

package sqlsandbox

import "os"

func workerExe() (string, error) { return os.Executable() }
