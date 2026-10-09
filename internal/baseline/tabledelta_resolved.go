package baseline

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// A resolved pair (#2231) is a table's whole chain merged into one range
// pair, kept OUTSIDE the snapshot as a reader's shortcut: the newest version
// of every key already chosen, so a statement reads the chain through
// TableDeltaOnePairStateSQL instead of the aggregate and join of
// TableDeltaStateSQL. Measured on a 20 M row table with 1.03 M changed rows
// (DuckDB 1.4.5, 2 threads, the same dead rows read both ways): a count and a
// sum took 423 ms through the join and 323 ms through the pair, a lookup by
// key 80 ms and 31 ms; merging the pair took 0.7 s and under 0.5 GB.
//
// Beside the snapshots and not inside them, on purpose. Inside, every
// snapshot would hold its own copy of the whole chain where today it holds
// links to the earlier pairs: say a chain that grows evenly to 24 MiB over 36
// refreshes, that is about 430 MiB across those snapshots against 24, and
// that much more to upload. Here there is at most one copy per table for
// each of the snapshots still being read, and nothing a snapshot, its
// manifest, an upload or retention knows about changes. It is a cache:
// whoever finds none reads the chain as before, and nothing but speed is
// lost by removing it.

// ResolvedDirName is the directory beside a server's snapshots that holds
// their resolved pairs: "<root>/.resolved/<snapshot>/<schema>/". A dot name,
// like ".compact", so no listing reads it as a snapshot.
const ResolvedDirName = ".resolved"

// CompactDirName is the directory beside a server's snapshots where the
// table-delta compaction job stages a range pair until the next refresh
// adopts it (#1723).
const CompactDirName = ".compact"

// isDaemonWorkDir says whether path is one of the directories the watch
// daemon keeps directly under a snapshots root for its own work: staged
// compactions and resolved pairs. Neither is part of a snapshot.
//
// Under a ROOT only. When root is itself a snapshot (the daemon uploads one
// snapshot at a time), what sits directly under it is a schema, and a schema
// may be named anything: that directory is the snapshot's data. A snapshot
// is told by its name or by a completeness marker directly in it, either
// being enough: a root has neither.
func isDaemonWorkDir(root, path string) bool {
	root = filepath.Clean(root)
	if filepath.Dir(path) != root {
		return false
	}
	if _, isSnapshot := snapshotdir.ParseTime(filepath.Base(root)); isSnapshot {
		return false
	}
	for _, marker := range []string{SuccessMarker, IncompleteMarker} {
		if _, err := os.Stat(filepath.Join(root, marker)); err == nil {
			return false
		}
	}
	name := filepath.Base(path)
	return name == CompactDirName || name == ResolvedDirName
}

// resolvedTableDeltaDir is where the resolved pair of the table file at
// basePath ("<root>/<snapshot>/<schema>/<table>.parquet") is kept, or "" for a
// path that is not a local file three levels under a root. The snapshot's own
// name is part of it: a snapshot is never rewritten, so a pair found there
// was merged from the chain that snapshot still holds.
func resolvedTableDeltaDir(basePath string) string {
	if basePath == "" || strings.Contains(basePath, "://") {
		return ""
	}
	schemaDir := filepath.Dir(basePath)
	snapDir := filepath.Dir(schemaDir)
	root := filepath.Dir(snapDir)
	if schemaDir == snapDir || snapDir == root {
		return ""
	}
	return filepath.Join(root, ResolvedDirName, filepath.Base(snapDir), filepath.Base(schemaDir))
}

// ResolvedTableDeltaPaths names the resolved pair of a chain: a range pair
// from the chain's first sequence number to its last, under
// resolvedTableDeltaDir. ok is false where no resolved pair can exist: a path
// with no such directory, or a chain of fewer than two pairs (one pair is
// read as it is, and a range needs two ends).
func ResolvedTableDeltaPaths(basePath string, files []TableDeltaFile) (f TableDeltaFile, ok bool) {
	dir := resolvedTableDeltaDir(basePath)
	if dir == "" || len(files) < 2 {
		return TableDeltaFile{}, false
	}
	lo, hi := files[0].SeqLo, files[len(files)-1].Seq
	if lo >= hi {
		return TableDeltaFile{}, false
	}
	posdel, upserts := TableDeltaRangePaths(filepath.Join(dir, filepath.Base(basePath)), lo, hi)
	return TableDeltaFile{SeqLo: lo, Seq: hi, Posdel: posdel, Upserts: upserts}, true
}

// ResolvedLeavingMark is the file the daemon leaves in a snapshot's
// directory of resolved pairs ("<root>/.resolved/<snapshot>/") when that
// snapshot is no longer one whose pairs are kept: they are removed some time
// after it. Pairs under a mark are not handed out any more
// (FindResolvedTableDelta), so the only statements still reading them named
// them before the mark, and the wait before removal is counted from it.
const ResolvedLeavingMark = ".leaving"

// FindResolvedTableDelta returns the resolved pair of the chain files beside
// basePath when BOTH its files are there and its snapshot's pairs are not
// marked as leaving. The writer renames the upserts into place last, so both
// present is a finished pair; the range in its name is the chain's, so a
// pair merged before the chain grew is not found.
func FindResolvedTableDelta(basePath string, files []TableDeltaFile) (TableDeltaFile, bool) {
	f, ok := ResolvedTableDeltaPaths(basePath, files)
	if !ok {
		return TableDeltaFile{}, false
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(filepath.Dir(f.Upserts)), ResolvedLeavingMark)); err == nil {
		return TableDeltaFile{}, false
	}
	for _, p := range []string{f.Posdel, f.Upserts} {
		if fi, err := os.Stat(p); err != nil || !fi.Mode().IsRegular() {
			return TableDeltaFile{}, false
		}
	}
	return f, true
}
