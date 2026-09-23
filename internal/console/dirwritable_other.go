//go:build !unix

package console

// dirWritable cannot ask without writing here; the daemon that takes the
// snapshots reports a folder it cannot write into on its first run.
func dirWritable(string) error { return nil }
