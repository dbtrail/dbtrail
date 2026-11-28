//go:build !linux

package sqlsandbox

// startLowered runs start, the worker at this process's priority. Only Linux
// lowers it (nice_linux.go), where the nice value is per thread, which is what
// lets the child be born lower without touching this process. Like the memory
// cgroup (fenceReason), elsewhere it is not there and nothing says so.
func startLowered(_ int, start func() error) (niceErr, startErr error) {
	return nil, start()
}
