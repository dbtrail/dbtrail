package reconstruct

import (
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
// No snapshot records its run's cut as such. Every file a refresh FOLDS is
// anchored at the cut, and a file it carries forward keeps an older anchor
// that an earlier refresh resolved the same way, so the newest anchor among
// the refresh-written files of the table's own snapshot directory is the cut,
// or an earlier one, which only reads more. A dump's anchor is left out: it is
// the source's position when the dump started, and capture may still have been
// behind it. The table's OWN directory, not the newest one: a table missing
// from a newer snapshot is read from an older one, which a newer refresh never
// checked it against. A file that does not read is left out, which can only
// lower the cut. Snapshots in S3 are not read here and get no cut.
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

// snapshotCutOf is the newest anchor among the refresh-written Parquet files
// under snapshotDir, or nil when there is none.
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
		if m.Producer != baseline.ProducerReconstruct || m.BinlogFile == "" || m.BinlogPos <= 0 {
			return nil
		}
		p := query.BinlogPos{File: m.BinlogFile, Pos: uint64(m.BinlogPos)}
		if cut == nil || cut.AtOrBefore(p) {
			cut = &p
		}
		return nil
	})
	return cut
}
