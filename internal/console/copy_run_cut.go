package console

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/views"
)

// runCut is where a snapshot directory a refresh published holds every
// table through (#2592): the refresh's cut, from the files it wrote.
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

// runCutOf is the newest refresh cut among footers, the files of one
// snapshot directory: every table's file and the newest pair beside it. A
// footer a refresh did not write (a full read's), one with no position, or
// one whose event mark is missing or past its position is not a cut and is
// skipped, which only lowers the bound: every refresh cut in the directory
// is one its tables were all folded through. Two binlog numberings give
// none, as in copyDirMark.
func runCutOf(footers []baseline.DumpMetadata) (runCut, bool) {
	var best runCut
	found := false
	for _, md := range footers {
		if md.Producer != baseline.ProducerReconstruct || md.BinlogFile == "" || md.BinlogPos <= 0 {
			continue
		}
		m, why := markWithin(md)
		if why != "" {
			continue
		}
		at := query.BinlogPos{File: md.BinlogFile, Pos: uint64(md.BinlogPos)}
		if found && reconstruct.BinlogBaseName(at.File) != reconstruct.BinlogBaseName(best.at.File) {
			return runCut{}, false
		}
		if !found || best.at.AtOrBefore(at) {
			best, found = runCut{at: at, mark: m, stamp: md.SnapshotTimestamp}, true
		}
	}
	return best, found
}

// promoteToRunCut moves the start of cut's lookup to run when the table's
// own cut is before it on the same numbering: the refresh behind run folded
// the table through it and found nothing, so there is nothing for the
// lookup to find between the two. The time the lookup starts from is then
// the run's mark's (sinceMark, see copyLookupSince), on the source's clock.
//
// Only the lookup moves. sourceRead and lastEventID stay the table's own,
// so the guards that ask what happened since its rows were last read from
// the source (a capture loss, the source server changing) ask over the
// same span as before.
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
// directory is never written again, so its answer stands; the memo starts
// over past copyCutMemoMax directories.
type copyRunCutMemo struct {
	mu   sync.Mutex
	dirs map[string]copyRunCutEntry
}

type copyRunCutEntry struct {
	cut runCut
	ok  bool
}

// of is dir's refresh cut. dir is a snapshot directory as copySnapshotDir
// gives it. Any file that cannot be listed or read gives no cut, and is not
// remembered, so the next ask tries again.
func (m *copyRunCutMemo) of(dir string, footer func(path string) (baseline.DumpMetadata, error)) (runCut, bool) {
	m.mu.Lock()
	e, ok := m.dirs[dir]
	m.mu.Unlock()
	if ok {
		return e.cut, e.ok
	}
	footers, ok := runCutFooters(dir, footer)
	if !ok {
		return runCut{}, false
	}
	cut, found := runCutOf(footers)
	m.mu.Lock()
	if m.dirs == nil || len(m.dirs) >= copyCutMemoMax {
		m.dirs = map[string]copyRunCutEntry{}
	}
	m.dirs[dir] = copyRunCutEntry{cut: cut, ok: found}
	m.mu.Unlock()
	return cut, found
}

// runCutFooters reads the footer of every table file in dir and of the
// newest pair beside it. ok false when anything could not be read.
func runCutFooters(dir string, footer func(path string) (baseline.DumpMetadata, error)) ([]baseline.DumpMetadata, bool) {
	schemas, err := os.ReadDir(dir)
	if err != nil {
		return nil, false
	}
	var out []baseline.DumpMetadata
	for _, sd := range schemas {
		if !sd.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(dir, sd.Name()))
		if err != nil {
			return nil, false
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}
			base := filepath.Join(dir, sd.Name(), f.Name())
			md, err := footer(base)
			if err != nil {
				return nil, false
			}
			out = append(out, md)
			chain, err := baseline.ListTableDelta(context.Background(), base)
			if err != nil {
				return nil, false
			}
			if chain != nil && len(chain.Files) > 0 {
				pair, err := footer(chain.LastFileUpserts())
				if err != nil {
					return nil, false
				}
				out = append(out, pair)
			}
		}
	}
	return out, true
}

// tailCutOf is copyCutOf for the lookup a tail starts from: the table's own
// cut, moved to its directory's refresh cut when that is later (#2592).
func (s *Server) tailCutOf(t views.BaselineTable) copyCut {
	cut := copyCutOf(t, s.copyCuts.footer)
	if cut.refusal != "" {
		return cut
	}
	dir, ok := copySnapshotDir(t)
	if !ok {
		return cut
	}
	run, ok := s.copyRunCuts.of(dir, s.copyCuts.footer)
	if !ok {
		return cut
	}
	return promoteToRunCut(cut, run)
}
