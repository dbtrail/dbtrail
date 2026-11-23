//go:build !darwin && !linux

package console

import "context"

// measureFolder cannot tell hard links apart on this platform, so it reports
// no size rather than one that counts a shared file many times.
func measureFolder(context.Context, string, int) (uint64, bool) { return 0, false }

// folderDisk cannot tell on this platform.
func folderDisk(string) (uint64, bool) { return 0, false }
