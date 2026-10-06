package reconstruct

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dbtrail/dbtrail/internal/query"
)

// archiveCuts gives each table's fetch query.Options.ArchivesCheckedThrough
// (#2152), and records what this run's snapshot can promise the next one.
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
// # What "checked through" means
//
// A refresh that checked the archives (runChecksArchives) and had a cut has
// looked at every change of each of its tables up to that cut, archived or
// not: capture indexes in commit order, so every row up to the cut was in the
// index when it looked, and a row indexed later sits after the cut. An archive
// whose newest change is before it holds nothing the next fetch has not seen.
//
// That premise fails on an index `bintrail index` also writes: files indexed
// late get old positions. There no cut is used at all (query.IndexBackfilled,
// as the #2160 numbering check does), and PartitionHeads falls back to
// archives written after the snapshot's time.
//
// # Where it is recorded: one file per snapshot directory, per table
//
// archiveCutFile, written by the run that published the directory and only
// when the run completed. It maps each table to the position up to which that
// table has been checked:
//   - a run that checked the archives and has a cut: its cut;
//   - any other run (`--allow-gaps`, archive discovery failed, an empty
//     index): what the folder the table was read from recorded for it,
//     unchanged, because this run added no check of its own.
//
// Per table, and never a maximum over the directory's files: a directory can
// hold tables carried forward from folders of different ages, and a run that
// did not check archives must not lift a table to a position some other
// table was checked through. Table files carry nothing about it: a footer
// travels with a carried-forward file into folders written by runs that did
// not check, and files written by older builds carry an anchor no refresh
// searched archived hours up to.
//
// Read once per directory per run. Snapshots in S3 are not read here and get
// no value; a record that does not read gives none either. Both only read more.
type archiveCuts struct {
	off bool
	mu  sync.Mutex
	dir map[string]map[string]query.BinlogPos
	// next is what this run's snapshot directory will record, per table.
	next map[string]query.BinlogPos
}

// archiveCutFile is the per-directory record beside _SUCCESS.
const archiveCutFile = "_ARCHIVE_CHECKED"

// backfilledArchivesWarned says once per process that the record is not used:
// every refresh of a daemon would repeat it.
var backfilledArchivesWarned sync.Once

func newArchiveCuts(backfilled bool) *archiveCuts {
	return &archiveCuts{off: backfilled, dir: map[string]map[string]query.BinlogPos{}, next: map[string]query.BinlogPos{}}
}

// forBaseline is how far schema.table has been checked according to the
// directory baselinePath (<root>/<snapshot>/<schema>/<table>.parquet) lives
// in, or nil.
func (c *archiveCuts) forBaseline(baselinePath, schema, table string) *query.BinlogPos {
	if c == nil || c.off || strings.HasPrefix(baselinePath, "s3://") {
		return nil
	}
	dir := filepath.Dir(filepath.Dir(baselinePath))
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.dir[dir]
	if !ok {
		rec = readArchiveCutFile(dir)
		c.dir[dir] = rec
	}
	if p, ok := rec[schema+"."+table]; ok {
		return &p
	}
	return nil
}

// record notes what this run's directory will promise for schema.table:
// checked when this run checked the archives up to cut, otherwise the value
// it was read with (nil: nothing).
func (c *archiveCuts) record(schema, table string, checked bool, cut, inherited *query.BinlogPos) {
	if c == nil {
		return
	}
	v := inherited
	if checked && cut != nil {
		v = cut
	}
	if v == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next[schema+"."+table] = *v
}

// write writes the record of this run into snapshotDir. Nothing to record
// writes nothing. Called only for a run that completed.
func (c *archiveCuts) write(snapshotDir string) error {
	if c == nil || c.off {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.next) == 0 {
		return nil
	}
	f := archiveCutRecord{Version: 1, Tables: map[string]archiveCutJSON{}}
	for k, p := range c.next {
		f.Tables[k] = archiveCutJSON{File: p.File, Pos: &p.Pos}
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	tmp := filepath.Join(snapshotDir, archiveCutFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(snapshotDir, archiveCutFile))
}

type archiveCutRecord struct {
	Version int                       `json:"version"`
	Tables  map[string]archiveCutJSON `json:"tables"`
}

type archiveCutJSON struct {
	File string  `json:"binlog_file"`
	Pos  *uint64 `json:"start_pos"`
}

// readArchiveCutFile reads dir's record; nil when it is absent or does not
// read. An entry with no file or position is left out.
func readArchiveCutFile(dir string) map[string]query.BinlogPos {
	b, err := os.ReadFile(filepath.Join(dir, archiveCutFile))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Debug("the snapshot's archive record does not read; its tables get no value", "dir", dir, "error", err)
		}
		return nil
	}
	var f archiveCutRecord
	if err := json.Unmarshal(b, &f); err != nil || f.Version != 1 {
		slog.Debug("the snapshot's archive record does not parse; its tables get no value", "dir", dir, "error", fmt.Sprint(err))
		return nil
	}
	out := map[string]query.BinlogPos{}
	for k, v := range f.Tables {
		if v.File == "" || v.Pos == nil {
			continue
		}
		out[k] = query.BinlogPos{File: v.File, Pos: *v.Pos}
	}
	return out
}

// runChecksArchives reports whether a refresh run's own fetches checked the
// archives in a way the next refresh can build on (#2152).
//   - backfilled: `bintrail index` also writes this index, so a row with an
//     old position can be indexed after this run, below its cut.
//   - allowGaps: a fetch may go on past an archive source it could not read.
//   - archErr: discovering the archive sources failed, and under allowGaps
//     the run went on without them.
func runChecksArchives(backfilled, allowGaps bool, archErr error) bool {
	return !backfilled && !allowGaps && archErr == nil
}
