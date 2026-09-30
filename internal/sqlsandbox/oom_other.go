//go:build !linux

package sqlsandbox

import "io"

// No per-process out-of-memory priority to set here.
func lowerOOMPriority(io.Writer) {}
