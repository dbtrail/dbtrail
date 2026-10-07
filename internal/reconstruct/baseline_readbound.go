package reconstruct

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"runtime/debug"
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
// S3 each is a request (and a DuckDB session), and the web interface calls this
// while rendering a page.
const readBoundConcurrency = 8

// chainStartAt is baseline.TableDeltaStartAt, replaced by a test that counts
// the footers read or makes one unreadable.
var chainStartAt = baseline.TableDeltaStartAt

// ReadBounds returns, for each of files in order, where a reader of that
// table file starts its event fetch: what FindBaseline returns for it.
//
// It never fails. A chain whose start cannot be read (damaged delta files, an
// unreadable footer, a store that did not answer, ctx done first, a read
// that panicked) comes back
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
			slog.Warn("the table delta beside a snapshot file is damaged, so the age of the snapshot cannot be graded",
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
			defer recoverReadBound(f.Path, &out[i])
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-ctx.Done():
				out[i].Unread = true
				return
			}
			start, err := chainStartAt(ctx, f.deltaUpsertsPath())
			if err != nil {
				slog.Warn("could not read where the table delta beside a snapshot file starts, so the age of the snapshot cannot be graded",
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

// recoverReadBound stops a panic in one file's footer read from taking the
// process down. ReadBounds starts a goroutine per file, which no recover of
// its caller covers, and it runs inside the daemon that also captures: an
// unrecovered panic here is a capture outage over a read that only grades a
// verdict.
//
// The file comes back unread, whole: whatever the read had written before it
// panicked is dropped, so half an answer cannot grade as covered, and nothing
// is kept in the S3 memo. It is deferred after wg.Done and before the slot is
// taken, so the wait and the slot are both released on this path too; a
// recover that only logged would leave the caller waiting for good.
func recoverReadBound(path string, b *status.ReadBound) {
	r := recover()
	if r == nil {
		return
	}
	slog.Error("reading where the table delta beside a snapshot file starts hit an internal error; "+
		"the age of that snapshot cannot be graded and the other tables continue. "+
		"Please report this with the stack recorded here.",
		"path", path, "panic", r, "stack", string(debug.Stack()))
	*b = status.ReadBound{Unread: true}
}

// NewestPerTable returns the indexes, into files, of each table's newest
// snapshot file: the ones a headline verdict is decided on, and so the ones
// worth a footer read.
func NewestPerTable(files []BaselineFile) []int {
	// Keyed on the pair: a table name can hold a dot (#2008), so a dotted
	// string would fold a/b.c into a.b/c.
	type table struct{ schema, name string }
	newest := map[table]int{}
	for i, f := range files {
		k := table{f.Schema, f.Table}
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

// SnapshotReadsFrom returns the earliest instant a reader of any table of the
// LOCAL snapshot at `at` under root fetches events from (#1904): the start of
// the oldest chain of deltas in it, or the snapshot's own time when no table
// has a chain. It is the instant the snapshot stays restorable only while the
// index still holds, which the refresh loop's gate has to watch rather than
// the snapshot's directory time.
//
// It errors whenever that instant is not known: no table file listed at that
// snapshot, or a chain whose start could not be read. Never a guess, since
// the caller acts on a later answer as "still covered".
func SnapshotReadsFrom(ctx context.Context, root string, at time.Time) (time.Time, error) {
	return SnapshotReadsFromWith(ctx, root, at, nil)
}

// SnapshotReadsFromWith is SnapshotReadsFrom for a snapshot some of whose
// tables are not on disk because they are copied inside S3 at upload
// (#2212). copied holds, per copied table, where its readers start (its
// chain's start, or zero for a table copied without a chain, whose readers
// start at the snapshot itself): those tables are usually the oldest, and a
// snapshot whose every table was copied has no file on disk at all.
func SnapshotReadsFromWith(ctx context.Context, root string, at time.Time, copied []time.Time) (time.Time, error) {
	files, unreadable, err := ListBaselinesUnreadable(ctx, root)
	if err != nil {
		return time.Time{}, err
	}
	// By the folder's NAME: at may carry sub-second digits the name drops.
	name := SnapshotDirName(at)
	for _, u := range unreadable {
		// A folder of this snapshot the listing skipped hides its tables,
		// and their chains may be the oldest.
		if SnapshotDirName(u.SnapshotTime) == name {
			return time.Time{}, fmt.Errorf("part of the snapshot %s could not be listed (%s): %w", name, u.Path, u.Err)
		}
	}
	var mine []BaselineFile
	for _, f := range files {
		if SnapshotDirName(f.SnapshotTime) == name {
			mine = append(mine, f)
		}
	}
	if len(mine) == 0 && len(copied) == 0 {
		return time.Time{}, fmt.Errorf("no table file of the snapshot %s is listed under %s", name, root)
	}
	var oldest time.Time
	for _, from := range copied {
		if from.IsZero() {
			from = at
		}
		if oldest.IsZero() || from.Before(oldest) {
			oldest = from
		}
	}
	for i, b := range ReadBounds(ctx, mine) {
		if b.Unread {
			return time.Time{}, fmt.Errorf("where the chain of deltas beside %s starts could not be read", mine[i].Path)
		}
		if from := b.From(mine[i].SnapshotTime); oldest.IsZero() || from.Before(oldest) {
			oldest = from
		}
	}
	return oldest, nil
}
