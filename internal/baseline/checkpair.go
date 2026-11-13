package baseline

import (
	"path/filepath"
	"sort"
	"strings"
)

// A check of the snapshots (verify, and its Explain) compares, for each
// table, the snapshot that last read the table from the database with the
// snapshot before it (internal/verify pairLastRead). Without an S3
// destination the local folder is the only place those two are, and the
// count removes them a few snapshots after the read: at the defaults (a
// snapshot every 5 minutes, a count of 3) about 15 minutes after a full read,
// which is usually daily (#2691). So the cleanup of a folder with a count and
// no destination keeps that pair on top of the count, until the next read
// makes another pair the one a check needs. One pair per folder, two
// snapshots at most: the pair of the first table of the newest snapshot. A
// full read reads every table at once, so for a folder whose tables are read
// together it is every table's pair; a table read on its own since then
// (a re-read after a refusal) has a pair of its own that is not kept.
//
// The read is found the way the check finds it, from the newest snapshot's
// file of that table: that file itself when it was read from the database,
// else the snapshot of the time its footer says the table was last read. The
// snapshot before it is the newest complete, readable one dated before it
// that holds the table, the restore's result included (the check lists it).

// CheckPair names the two snapshots of the local folder dir that a check
// compares for the first table of its newest snapshot: read, the one that
// last read that table from the database, and before, the one it is compared
// with. Either is "" when there is none to name: no complete snapshot, a
// footer that will not read, a read whose snapshot is gone, no snapshot
// before the read.
func CheckPair(dir string) (read, before string) {
	if dir == "" || strings.HasPrefix(dir, "s3://") {
		return "", ""
	}
	snaps, err := enumerateLocalSnapshots(dir)
	if err != nil {
		return "", ""
	}
	return checkPairOf(dir, snaps)
}

func checkPairOf(dir string, snaps []localSnapshot) (read, before string) {
	var usable []localSnapshot
	for _, s := range snaps {
		if s.complete && !s.unreadable && len(s.tables) > 0 {
			usable = append(usable, s)
		}
	}
	if len(usable) == 0 {
		return "", ""
	}
	sort.Slice(usable, func(i, j int) bool { return usable[i].ts.After(usable[j].ts) })
	newest := usable[0]
	tables := append([]string(nil), newest.tables...)
	sort.Strings(tables)
	table := tables[0] // "schema/table"
	file := func(s localSnapshot) string {
		return filepath.Join(dir, s.name, filepath.FromSlash(table)+".parquet")
	}
	holds := func(s localSnapshot) bool {
		for _, t := range s.tables {
			if t == table {
				return true
			}
		}
		return false
	}
	md, err := ReadParquetMetadata(file(newest))
	if err != nil {
		return "", ""
	}
	r, found := newest, readFromDatabaseHere(newest, md)
	if !found {
		at := SourceReadOf(md).At
		if at.IsZero() {
			return "", ""
		}
		for _, s := range usable {
			if s.ts.Equal(at) && holds(s) {
				r, found = s, true
				break
			}
		}
		if !found {
			return "", ""
		}
		rmd, err := ReadParquetMetadata(file(r))
		if err != nil || !readFromDatabaseHere(r, rmd) {
			// The check answers with an error or "inconclusive" for this:
			// nothing it would compare.
			return "", ""
		}
	}
	for _, s := range usable {
		if s.ts.Before(r.ts) && holds(s) {
			return r.name, s.name
		}
	}
	return r.name, ""
}

// readFromDatabaseHere is internal/verify's readHere: the file was written
// into snapshot s by a read of the database, not carried there.
func readFromDatabaseHere(s localSnapshot, md DumpMetadata) bool {
	return !md.SnapshotTimestamp.IsZero() && ProvenanceOf(s.ts, md).ProducedBy == ProducedByDump
}

// keepCheckPair adds the pair a check compares to keepers (#2691). Only for
// a folder with a count and no destination: with one, the check finds the
// pair in S3.
func keepCheckPair(dir string, keepers map[string]bool) {
	read, before := CheckPair(dir)
	for _, name := range []string{read, before} {
		if name != "" {
			keepers[name] = true
		}
	}
}
