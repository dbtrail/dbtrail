package snapshotdir

import (
	"strings"
	"time"
)

// ParseTime reads the instant a snapshot directory's name encodes,
// which is RFC 3339 with the colons replaced (2026-08-23T14-50-30Z), the form
// reconstruct.SnapshotDirName writes.
//
// It lives in a package of its own, with no dependencies, because the readers
// sit on opposite sides of an import edge: internal/query needs it and
// internal/baseline cannot be imported from there (its own tests reach
// internal/recovery, which reaches internal/query, so the edge closes a cycle).
// A one-function package is a smaller price than two hand-written parsers of
// one format, which is how they drift.
//
// The directory name is the AUTHORITATIVE source for a snapshot's time. Every discovery path already works this
// way: FindBaseline, ListBaselines and the status staleness grading all derive
// the time from the directory and never from the file.
//
// That matters more than it used to. A table whose delta window held no events
// is published by carrying its previous Parquet file forward unchanged, so its
// footer keeps the OLDER bintrail.snapshot_timestamp. Any reader that trusts
// the footer instead of the directory then dates two files in the SAME snapshot
// differently, purely by whether each table happened to be cold.
func ParseTime(name string) (time.Time, bool) {
	idx := strings.IndexByte(name, 'T')
	if idx < 0 {
		return time.Time{}, false
	}
	// Colons are restored only after the 'T': the date half uses '-' as its own
	// separator and must not be touched.
	rfc := name[:idx+1] + strings.ReplaceAll(name[idx+1:], "-", ":")
	t, err := time.Parse(time.RFC3339, rfc)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// Name is the directory name a snapshot taken at t is written under, the form
// ParseTime reads back: RFC 3339 in UTC with the colons replaced.
func Name(t time.Time) string {
	return strings.ReplaceAll(t.UTC().Format(time.RFC3339), ":", "-")
}
