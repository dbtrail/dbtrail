package baseline

import (
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// A check of the snapshots (verify, and its Explain) compares, for each
// table, the snapshot that last read the table from the database with the
// snapshot before it (internal/verify pairLastRead). Without an S3
// destination the local folder is the only place those two are, and the
// count removes them a few snapshots after the read: at the defaults (a
// snapshot every 5 minutes, a count of 3) about 15 minutes after a full read,
// which is usually daily (#2691). So the cleanup of a folder with a count and
// no destination keeps one pair on top of the count: the read most tables of
// the newest snapshot come from (ties: the oldest of them), and the snapshot
// before it. That is the last full read and the snapshot before it, unless
// most tables were read again since. One pair per folder, two snapshots at
// most: a table read on its own since then (a re-read after a refusal), or
// one with no recorded read (built from the changes alone), has no pair kept.
//
// Each read is found the way the check finds it, from the newest snapshot's
// file of a table: that file itself when it was read from the database, else
// the snapshot of the time its footer says the table was last read. The
// snapshot before it is the newest complete, readable one dated before it
// that holds the table, the restore's result included (the check lists it).

// CheckPair names the two snapshots of the local folder dir that a check
// compares for most tables of its newest snapshot: read, the read of the
// database they come from, and before, the snapshot it is compared with.
// Either is "" when there is none to name: no complete snapshot, no table
// with a recorded read whose snapshot is still here, no snapshot before the
// read.
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
	holds := func(s localSnapshot, table string) bool {
		for _, t := range s.tables {
			if t == table {
				return true
			}
		}
		return false
	}
	file := func(s localSnapshot, table string) string {
		return filepath.Join(dir, s.name, filepath.FromSlash(table)+".parquet")
	}
	// The reads the newest snapshot's tables come from, the one most of them
	// share first (ties: the oldest, the full read a table read on its own
	// since came after). A table whose copy records no read (one built from
	// the changes alone, a footer from before the record) is skipped.
	for _, c := range readsOf(dir, newest, file) {
		var r localSnapshot
		found := false
		for _, s := range usable {
			if s.ts.Equal(c.at) && holds(s, c.table) {
				r, found = s, true
				break
			}
		}
		if !found {
			continue // that read's snapshot is gone: the next most shared
		}
		md, err := footerOf(file(r, c.table))
		if err != nil || !readFromDatabaseHere(r, md) {
			// The check answers with an error or "inconclusive" for this:
			// nothing it would compare.
			continue
		}
		for _, s := range usable {
			if s.ts.Before(r.ts) && holds(s, c.table) {
				return r.name, s.name
			}
		}
		return r.name, ""
	}
	return "", ""
}

// checkRead is one read of the database the newest snapshot's tables come
// from: when, one table that comes from it, and how many do.
type checkRead struct {
	at     time.Time
	table  string
	tables int
}

// readsOf is the reads newest's tables come from, the one most of them share
// first, ties to the oldest. A snapshot's files never change once it is
// published, so the answer is kept for the newest snapshot of each folder:
// the cleanup after every snapshot and every page load of the snapshot list
// ask, and the footers are read once per new snapshot, not on each.
func readsOf(dir string, newest localSnapshot, file func(localSnapshot, string) string) []checkRead {
	key := readKey(dir)
	checkReadsMu.Lock()
	if m, ok := checkReadsMemo[key]; ok && m.newest == newest.name {
		checkReadsMu.Unlock()
		return m.reads
	}
	checkReadsMu.Unlock()
	tables := append([]string(nil), newest.tables...)
	sort.Strings(tables)
	byAt := map[time.Time]*checkRead{}
	for _, table := range tables {
		md, err := footerOf(file(newest, table))
		if err != nil {
			continue
		}
		at := newest.ts
		if !readFromDatabaseHere(newest, md) {
			at = SourceReadOf(md).At
		}
		if at.IsZero() {
			continue
		}
		at = at.UTC()
		if byAt[at] == nil {
			byAt[at] = &checkRead{at: at, table: table}
		}
		byAt[at].tables++
	}
	var reads []checkRead
	for _, r := range byAt {
		reads = append(reads, *r)
	}
	sort.Slice(reads, func(i, j int) bool {
		if reads[i].tables != reads[j].tables {
			return reads[i].tables > reads[j].tables
		}
		return reads[i].at.Before(reads[j].at)
	})
	checkReadsMu.Lock()
	checkReadsMemo[key] = checkReadsEntry{newest: newest.name, reads: reads}
	checkReadsMu.Unlock()
	return reads
}

type checkReadsEntry struct {
	newest string
	reads  []checkRead
}

var (
	checkReadsMu   sync.Mutex
	checkReadsMemo = map[string]checkReadsEntry{} // folder -> its newest snapshot's reads
)

// footerOf is ReadParquetMetadata, counted in tests.
var footerOf = ReadParquetMetadata

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
