package console

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/views"
)

// runCut is where a snapshot directory a refresh published holds every
// table through (#2592): the cut of the refresh that wrote the directory.
//
// A refresh folds every table of the previous snapshot through its cut and
// publishes all of them or none (foldOutcome). A table with no event in the
// window is carried forward with its OLD footer, so its own cut stays where
// it was last written; for a table nobody writes, that is its last full
// read. Its rows are still good through the refresh's cut: the fold read
// its window and found nothing. copyDirMark bounds the copy's lag by the
// same rule (#2369).
type runCut struct {
	at    query.BinlogPos
	mark  reconstruct.EventMark
	stamp time.Time
}

// runCutIn is the cut md records when the refresh that wrote it is the one
// that published its directory (its writer instant is the directory's, dirAt)
// and that refresh cut the index (MetaKeyFoldCut, matching the position).
// Every other footer says nothing about the directory: a carried file's cut
// is an older run's, which a replay of capture can leave ABOVE this run's,
// and a run that found the index empty carries its source file's position,
// which may be a full read's, ahead of what capture indexed.
func runCutIn(md baseline.DumpMetadata, dirAt time.Time) (runCut, bool) {
	if md.Producer != baseline.ProducerReconstruct || md.BinlogFile == "" || md.BinlogPos <= 0 ||
		dirAt.IsZero() || !md.SnapshotTimestamp.Equal(dirAt) ||
		md.FoldCut != md.BinlogFile+":"+strconv.FormatInt(md.BinlogPos, 10) {
		return runCut{}, false
	}
	m, why := markWithin(md)
	if why != "" {
		return runCut{}, false
	}
	return runCut{at: query.BinlogPos{File: md.BinlogFile, Pos: uint64(md.BinlogPos)}, mark: m, stamp: md.SnapshotTimestamp}, true
}

// promoteToRunCut moves the start of cut's lookup to run when the table's
// own cut is before it on the same numbering: the refresh behind run folded
// the table through it and found nothing, so there is nothing for the
// lookup to find between the two. The time the lookup starts from is then
// the run's mark's (sinceMark, see copyLookupSince), on the source's clock.
//
// Only the lookup moves. sourceRead and lastEventID stay the table's own,
// so the guards that ask what happened since its rows were last read from
// the source (a capture loss, the source server changing, a schema change:
// tailSource.Broken) ask over the same span as before.
func promoteToRunCut(cut copyCut, run runCut) copyCut {
	if cut.refusal != "" || run.mark.ID == 0 ||
		reconstruct.BinlogBaseName(cut.anchor.File) != reconstruct.BinlogBaseName(run.at.File) ||
		run.at.AtOrBefore(cut.anchor) {
		return cut
	}
	cut.anchor, cut.mark, cut.stamp = run.at, run.mark, run.stamp
	cut.folded, cut.sinceMark = true, true
	return cut
}

// copyRunCutMemo remembers each directory's refresh cut. A published
// directory is never written again, so a found cut stands, and so does
// "none" unless the directory is marked incomplete; the memo starts over
// past copyCutMemoMax directories.
type copyRunCutMemo struct {
	mu   sync.Mutex
	dirs map[string]copyRunCutEntry
}

type copyRunCutEntry struct {
	cut runCut
	ok  bool
}

// of is dir's refresh cut. dir is a snapshot directory as copySnapshotDir
// gives it. Every file the run wrote carries the same cut, so the first one
// found answers. A file that cannot be read is skipped, which can only miss
// the cut, never invent one; a directory where one was skipped and no cut
// was found is not remembered, and warn hears why.
func (m *copyRunCutMemo) of(dir string, footer func(path string) (baseline.DumpMetadata, error), warn func(path string, err error)) (runCut, bool) {
	m.mu.Lock()
	e, ok := m.dirs[dir]
	m.mu.Unlock()
	if ok {
		return e.cut, e.ok
	}
	dirAt, named := snapshotdir.ParseTime(filepath.Base(filepath.Clean(dir)))
	if !named {
		return runCut{}, false
	}
	cut, found, skipped := scanRunCut(dir, dirAt, footer, warn)
	if !found {
		if skipped {
			return runCut{}, false
		}
		// A directory a run failed or is still writing says so; a complete
		// one has _SUCCESS, or no marker at all if older than them.
		if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err == nil {
			return runCut{}, false
		}
	}
	m.mu.Lock()
	if m.dirs == nil || len(m.dirs) >= copyCutMemoMax {
		m.dirs = map[string]copyRunCutEntry{}
	}
	m.dirs[dir] = copyRunCutEntry{cut: cut, ok: found}
	m.mu.Unlock()
	return cut, found
}

// scanRunCut looks through dir's table files and the newest pair beside
// each (a delta run writes a pair, not the table file) for one the run
// wrote. Each schema directory is listed once. skipped reports a file or
// listing that could not be read.
func scanRunCut(dir string, dirAt time.Time, footer func(path string) (baseline.DumpMetadata, error), warn func(path string, err error)) (cut runCut, found, skipped bool) {
	schemas, err := os.ReadDir(dir)
	if err != nil {
		warn(dir, err)
		return runCut{}, false, true
	}
	try := func(path string) bool {
		md, err := footer(path)
		if err != nil {
			warn(path, err)
			skipped = true
			return false
		}
		cut, found = runCutIn(md, dirAt)
		return found
	}
	for _, sd := range schemas {
		if !sd.IsDir() {
			continue
		}
		sdir := filepath.Join(dir, sd.Name())
		entries, err := os.ReadDir(sdir)
		if err != nil {
			warn(sdir, err)
			skipped = true
			continue
		}
		names := make([]string, 0, len(entries))
		for _, f := range entries {
			if !f.IsDir() {
				names = append(names, f.Name())
			}
		}
		for _, n := range names {
			if !strings.HasSuffix(n, ".parquet") {
				continue
			}
			if try(filepath.Join(sdir, n)) {
				return cut, true, skipped
			}
			// The v0.83.0 layout's legacy pair is not read: copyCutOf
			// refuses that layout, so nothing it would bound has a tail.
			chain, err := baseline.TableDeltaChainIn(sdir, names, strings.TrimSuffix(n, ".parquet"))
			if err != nil {
				warn(filepath.Join(sdir, n), err)
				skipped = true
				continue
			}
			if chain != nil && !chain.Legacy && len(chain.Files) > 0 && try(chain.Last().Upserts) {
				return cut, true, skipped
			}
		}
	}
	return runCut{}, false, skipped
}

// tailLookupCut is copyCutOf for the lookup a tail starts from: the table's
// own cut, moved to its directory's refresh cut when that is later (#2592).
// The pointer to the newest snapshot is never a directory here: it moves,
// and a cut remembered for it would be applied to whatever it points at
// next.
func (s *Server) tailLookupCut(t views.BaselineTable) copyCut {
	cut := copyCutOf(t, s.copyCuts.footer)
	if cut.refusal != "" {
		return cut
	}
	return s.tailLookupCutFrom(t, cut)
}

// tailLookupCutFrom is tailLookupCut over t's own cut, already read.
func (s *Server) tailLookupCutFrom(t views.BaselineTable, cut copyCut) copyCut {
	dir, ok := copySnapshotDir(t)
	if !ok {
		return cut
	}
	if fi, err := os.Lstat(filepath.Clean(dir)); err != nil || fi.Mode()&os.ModeSymlink != 0 ||
		filepath.Base(filepath.Clean(dir)) == baseline.CurrentLinkName {
		return cut
	}
	run, ok := s.copyRunCuts.of(dir, s.copyCuts.footer, func(path string, err error) {
		s.copyFaults.warn(dir, "Engine Routing: a snapshot file could not be read for its directory's refresh cut; tables nobody writes may have no tail until it can",
			"dir", dir, "path", path, "error", err)
	})
	if !ok {
		return cut
	}
	return promoteToRunCut(cut, run)
}
