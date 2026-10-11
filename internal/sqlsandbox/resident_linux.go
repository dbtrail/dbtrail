//go:build linux

package sqlsandbox

import (
	"os"
	"strconv"
	"strings"
)

// residentBytes is the memory this process holds now (not its peak), from
// /proc/self/statm, whose second field is the resident set in pages. 0 when
// it cannot be read.
func residentBytes() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || pages < 0 {
		return 0
	}
	return pages * int64(os.Getpagesize())
}
