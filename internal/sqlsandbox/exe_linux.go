//go:build linux

package sqlsandbox

// workerExe is the binary this process runs, as the kernel sees it. A
// package upgrade mid-run replaces the file at os.Executable's path with a
// newer binary; /proc/self/exe keeps pointing at the one that is running
// (Go strips the " (deleted)" suffix), so parent and child always share one
// protocol.
func workerExe() (string, error) { return "/proc/self/exe", nil }
