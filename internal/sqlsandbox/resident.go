package sqlsandbox

import (
	"strconv"
	"strings"
)

// statmPrivate reads a /proc/<pid>/statm line: resident pages (field 2) less
// file-backed ones (field 3), in bytes. 0 for a line it cannot read.
func statmPrivate(statm string, pageSize int64) int64 {
	fields := strings.Fields(statm)
	if len(fields) < 3 {
		return 0
	}
	resident, err1 := strconv.ParseInt(fields[1], 10, 64)
	shared, err2 := strconv.ParseInt(fields[2], 10, 64)
	if err1 != nil || err2 != nil || resident < shared || shared < 0 {
		return 0
	}
	return (resident - shared) * pageSize
}
