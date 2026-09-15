//go:build !darwin && !linux

package consoleapp

import "os"

// sharedLink cannot see link counts here, so every file counts in full.
func sharedLink(os.FileInfo) bool { return false }
