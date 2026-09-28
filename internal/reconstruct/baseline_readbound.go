package reconstruct

import (
	"context"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/status"
)

// Where a reader starts, for the backup-age verdict (#1707).
//
// FindBaseline hands every reader the START of the chain of deltas beside a
// table file as the time to fetch events from. The verdict that says whether
// a snapshot can still be rebuilt from graded on the snapshot's directory
// time instead, which is later: it read "ok" over a window the readers then
// refused. ReadBounds gives the verdict the readers' instant.
//
// Two halves, so that a listing stays a listing:
//
//   - the listing marks each file with the NAME of its chain's newest pair
//     (BaselineFile.DeltaUpserts), off the file names it had already read. No
//     request is added, and one short string is kept per file, not the chain:
//     the S3 inventory is cached for the life of the process, and a chain of
//     a day of five-minute windows is hundreds of paths per table.
//   - ReadBounds reads ONE footer per file that has a chain, and only for the
//     files it is handed. A caller that grades each table's newest snapshot
//     hands it those.
//
// The footer read is baseline.TableDeltaStartAt, the one FindBaseline's
// lookup ends in, and over S3 the answer is kept in the same per-process
// memo (s3ChainStarts), so the two agree and a snapshot is paid for once.

// deltaMarks is what one folder's file names say about the chains in it.
type deltaMarks struct {
	newest  map[string]string
	damaged map[string]error
}

func deltaNames(dir string, names []string) deltaMarks {
	newest, damaged := baseline.NewestTableDeltaUpserts(dir, names)
	return deltaMarks{newest: newest, damaged: damaged}
}

func deltaNamesOf(dir string, entries []fs.DirEntry) deltaMarks {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return deltaNames(dir, names)
}

// mark returns f with what the folder says about the chain beside it.
func (m deltaMarks) mark(f BaselineFile) BaselineFile {
	if err := m.damaged[f.Table]; err != nil {
		f.DeltaErr = err
		return f
	}
	if p := m.newest[f.Table]; p != "" {
		f.DeltaUpserts = p[strings.LastIndexAny(p, `/\`)+1:]
	}
	return f
}

// deltaUpsertsPath is where the file named by DeltaUpserts sits: beside Path.
func (f BaselineFile) deltaUpsertsPath() string {
	if strings.HasPrefix(f.Path, "s3://") {
		return f.Path[:strings.LastIndexByte(f.Path, '/')+1] + f.DeltaUpserts
	}
	return filepath.Join(filepath.Dir(f.Path), f.DeltaUpserts)
}

// HasDelta reports whether the listing found delta files beside the file,
// whole or damaged.
func (f BaselineFile) HasDelta() bool { return f.DeltaUpserts != "" || f.DeltaErr != nil }

// readBoundConcurrency bounds the footer reads in flight for one call. Over
// S3 each is a request (and a DuckDB session), and the console calls this
// while rendering a page.
const readBoundConcurrency = 8

// chainStartAt is baseline.TableDeltaStartAt, replaced by a test that counts
// the footers read or makes one unreadable.
var chainStartAt = baseline.TableDeltaStartAt

// ReadBounds returns, for each of files in order, where a reader of that
// table file starts its event fetch: what FindBaseline returns for it.
//
// It never fails. A chain whose start cannot be read (damaged delta files, an
// unreadable footer, a store that did not answer, ctx done first) comes back
// Unread, which no verdict grades as covered. Over a local folder that is
// one footer read per file with a chain; over S3 the same, in parallel, and
// none for a file this process already answered for.
func ReadBounds(ctx context.Context, files []BaselineFile) []status.ReadBound {
	out := make([]status.ReadBound, len(files))
	var wg sync.WaitGroup
	slots := make(chan struct{}, readBoundConcurrency)
	for i, f := range files {
		switch {
		case f.DeltaErr != nil:
			slog.Warn("the table delta beside a backup file is damaged, so the age of the backup cannot be graded",
				"path", f.Path, "error", f.DeltaErr)
			out[i].Unread = true
			continue
		case f.DeltaUpserts == "":
			continue // no chain: a reader starts at the snapshot itself
		}
		s3 := strings.HasPrefix(f.Path, "s3://")
		if s3 {
			// A kept zero is "no delta", and the listing saw one: read it.
			if v, ok := s3ChainStarts.Load(f.Path); ok && !v.(time.Time).IsZero() {
				out[i].ChainStart = v.(time.Time)
				continue
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				out[i].Unread = true
				return
			}
			start, err := chainStartAt(ctx, f.deltaUpsertsPath())
			if err != nil {
				slog.Warn("could not read where the table delta beside a backup file starts, so the age of the backup cannot be graded",
					"path", f.Path, "error", err)
				out[i].Unread = true
				return
			}
			if s3 {
				s3ChainStarts.Store(f.Path, start)
			}
			out[i].ChainStart = start
		}()
	}
	wg.Wait()
	return out
}

// NewestPerTable returns the indexes, into files, of each table's newest
// snapshot file: the ones a headline verdict is decided on, and so the ones
// worth a footer read.
func NewestPerTable(files []BaselineFile) []int {
	newest := map[string]int{}
	for i, f := range files {
		k := f.Schema + "." + f.Table
		if j, ok := newest[k]; !ok || files[j].SnapshotTime.Before(f.SnapshotTime) {
			newest[k] = i
		}
	}
	out := make([]int, 0, len(newest))
	for _, i := range newest {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}
