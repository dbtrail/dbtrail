//go:build !linux

package sqlsandbox

// spillFS cannot read the free space here: -1 is "unknown", and the cap is
// SpillFactor times the memory alone. Only Linux hosts run the daemon.
func spillFS(string) (free int64, inMemory bool, err error) { return -1, false, nil }
