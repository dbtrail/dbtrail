//go:build linux

package sqlsandbox

import "os"

// residentBytes is the memory this process holds now that is its own: its
// resident set less the pages backed by files, from /proc/self/statm (fields
// 2 and 3, in pages). The pages of the binary and of the Parquet files it
// read are left out because they are not what a statement leaves behind:
// they are shared with every other worker, and the kernel drops them when it
// needs the room. Not the peak. 0 when it cannot be read.
func residentBytes() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	return statmPrivate(string(b), int64(os.Getpagesize()))
}
