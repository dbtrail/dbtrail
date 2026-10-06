package reconstruct

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// archiveCuts gives each table's fetch query.Options.ArchivesCheckedThrough
// (#2152): the cut of the refresh that published the snapshot the table is
// read from.
//
// # Why the table's own anchor is not enough
//
// A table with no changes is carried forward with its old anchor
// (carryForward), so its next fetch continues from a position that can be
// days old. Archives are not narrowed to a table, and every archive rotation
// writes after that position holds events after it: measured against the
// table's own anchor, each refresh of a quiet table would read every archive
// written since the table went quiet.
//
// # Why the cut is
//
// Capture indexes in commit order, so a row indexed after a refresh ran sits
// after that refresh's cut, and every row between the table's anchor and the
// cut was in the index when that refresh looked, and was checked for this
// table then. An archive whose newest change is before the cut holds nothing
// this fetch has not already seen.
//
// That premise fails on an index `bintrail index` also writes: files indexed
// late get old positions. There no cut is given (query.IndexBackfilled, as the
// #2160 numbering check does), and PartitionHeads falls back to archives
// written after the snapshot's time.
//
// # Where the cut is read from
//
// From baseline.MetaKeyArchiveCut and nothing else. A refresh writes that key
// on the files it writes only when it checked the archives itself
// (runChecksArchives) and had a cut. The binlog anchor beside it is not that
// promise: a file written by a build that never looked at archives or at late
// changes carries an anchor no refresh searched through archived hours up to,
// and a snapshot of an empty index keeps its source's (perhaps a dump's)
// position. The newest key in the table's OWN snapshot directory is used: a
// carried-forward file keeps an older key, which an earlier refresh wrote the
// same way and which only reads more. The table's own directory, not the
// newest one: a table missing from a newer snapshot is read from an older one,
// which a newer refresh never checked it against. A file or key that does not
// read is left out, which can only lower the cut. Snapshots in S3 are not read
// here and get no cut.
type archiveCuts struct {
	off bool
	mu  sync.Mutex
	dir map[string]*query.BinlogPos
}

// backfilledArchivesWarned says once per process that the cut is not used:
// every refresh of a daemon would repeat it.
var backfilledArchivesWarned sync.Once

func newArchiveCuts(backfilled bool) *archiveCuts {
	return &archiveCuts{off: backfilled, dir: map[string]*query.BinlogPos{}}
}

// forBaseline is the cut for a table read from baselinePath
// (<root>/<snapshot>/<schema>/<table>.parquet), or nil. Each directory is
// read once per run.
func (c *archiveCuts) forBaseline(baselinePath string) *query.BinlogPos {
	if c == nil || c.off || strings.HasPrefix(baselinePath, "s3://") {
		return nil
	}
	dir := filepath.Dir(filepath.Dir(baselinePath))
	c.mu.Lock()
	defer c.mu.Unlock()
	if cut, ok := c.dir[dir]; ok {
		return cut
	}
	cut := snapshotCutOf(dir)
	c.dir[dir] = cut
	return cut
}

// snapshotCutOf is the newest archive cut among the Parquet files under
// snapshotDir, or nil when there is none.
func snapshotCutOf(snapshotDir string) *query.BinlogPos {
	var cut *query.BinlogPos
	_ = filepath.WalkDir(snapshotDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), ".parquet") {
			return nil
		}
		m, err := baseline.ReadParquetMetadata(path)
		if err != nil {
			slog.Debug("the snapshot's cut is read without a file that does not open", "path", path, "error", err)
			return nil
		}
		p, ok := parseArchiveCut(m.ArchiveCut)
		if !ok {
			return nil
		}
		if cut == nil || cut.AtOrBefore(p) {
			cut = &p
		}
		return nil
	})
	return cut
}

// archiveCutJSON is baseline.MetaKeyArchiveCut's value.
type archiveCutJSON struct {
	File string  `json:"binlog_file"`
	Pos  *uint64 `json:"start_pos"`
}

func encodeArchiveCut(p query.BinlogPos) string {
	b, _ := json.Marshal(archiveCutJSON{File: p.File, Pos: &p.Pos}) // a string and a number: cannot fail
	return string(b)
}

// parseArchiveCut reads the key; ok is false for an absent or malformed one.
func parseArchiveCut(v string) (query.BinlogPos, bool) {
	var c archiveCutJSON
	if v == "" || json.Unmarshal([]byte(v), &c) != nil || c.File == "" || c.Pos == nil {
		return query.BinlogPos{}, false
	}
	return query.BinlogPos{File: c.File, Pos: *c.Pos}, true
}

// runChecksArchives reports whether a refresh run may write the archive-cut
// key (#2152): only when its own fetches checked the archives in a way the
// next refresh can build on.
//   - backfilled: `bintrail index` also writes this index, so a row with an
//     old position can be indexed after this run, below its cut.
//   - allowGaps: a fetch may go on past an archive source it could not read.
//   - archErr: discovering the archive sources failed, and under allowGaps
//     the run went on without them.
func runChecksArchives(backfilled, allowGaps bool, archErr error) bool {
	return !backfilled && !allowGaps && archErr == nil
}
