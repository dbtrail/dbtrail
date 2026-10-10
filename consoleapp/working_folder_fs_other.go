//go:build !darwin && !linux

package consoleapp

// fsKind cannot tell on this platform, and says a local disk: the write check
// still has to pass.
func fsKind(string) (fsClass, string, error) { return fsLocal, "", nil }
