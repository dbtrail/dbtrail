package duckdbutil

import "strings"

// FileGlob is the pattern that names the one file at path, for read_parquet,
// parquet_scan and the other DuckDB functions that take a path as a glob
// (#2243): each of [ * ? { wrapped in a single-character class. A backslash
// does not escape in DuckDB's glob; a class does. Without this a table named
// "order[st]" beside one named "orders" has every read of its file answered
// from the other's. A path that goes to DuckDB as a file to READ goes
// through this; one that is written (a COPY's target) does not, since that
// one is no pattern.
func FileGlob(path string) string {
	var b strings.Builder
	for _, r := range path {
		switch r {
		case '[', '*', '?', '{':
			b.WriteByte('[')
			b.WriteRune(r)
			b.WriteByte(']')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
